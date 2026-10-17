use std::time::Duration;

use axum::{
    Router,
    body::{Body, to_bytes},
    http::{HeaderMap, Request, StatusCode, header},
};
use jsonwebtoken::{Algorithm, EncodingKey, Header, encode, get_current_timestamp};
use kameo::actor::{ActorRef, Spawn};
use patchwork_control_plane::{
    actors::{NodeActor, PartitionActor, TableActor},
    api,
    auth::{Auth, JwtVerifier, PrincipalType},
    datamodels::{node, parition_lock, partition, table},
    encryption_client::EncryptionClient,
    workflows::executor::{ExecutorConfig, LongRunningWorkflowExecutor},
};
use sea_orm::{
    ActiveModelTrait, ConnectionTrait, Database, DatabaseConnection, EntityTrait, Schema, Set,
};
use serde_json::{Value, json};
use tower::ServiceExt;

// Test-only signing material; production only reads the issuer's public key.
const PRIVATE: &[u8] = include_bytes!("fixtures/jwt-private.pem");
const PUBLIC: &[u8] = include_bytes!("fixtures/jwt-public.pem");
const INTERNAL: &str = "test-internal-service-token";

fn auth() -> Auth {
    Auth::new(
        INTERNAL.into(),
        Some(JwtVerifier::new(PUBLIC, "atgs-auth", "patchwork").unwrap()),
    )
    .unwrap()
}

fn claims() -> Value {
    json!({"sub": "user-42", "account_id": "account-7", "principal_type": "User",
        "iss": "atgs-auth", "aud": "patchwork", "exp": get_current_timestamp() + 300})
}

fn signed(claims: &Value) -> String {
    encode(
        &Header::new(Algorithm::RS256),
        claims,
        &EncodingKey::from_rsa_pem(PRIVATE).unwrap(),
    )
    .unwrap()
}

fn headers(token: &str) -> HeaderMap {
    let mut headers = HeaderMap::new();
    headers.insert(
        header::AUTHORIZATION,
        format!("Bearer {token}").parse().unwrap(),
    );
    headers
}

#[test]
fn authenticates_internal_token_and_verified_jwt_identities() {
    let auth = auth();
    let principal = auth.authenticate(&headers(INTERNAL)).unwrap();
    assert_eq!(principal.principal_type, PrincipalType::AtgsInternalService);
    let principal = auth.authenticate(&headers(&signed(&claims()))).unwrap();
    assert_eq!(principal.id, "user-42");
    assert_eq!(principal.account_id.as_deref(), Some("account-7"));
    assert_eq!(principal.principal_type, PrincipalType::User);
    for kind in ["Service", "Container", "Function"] {
        let mut claims = claims();
        claims["principal_type"] = json!(kind);
        assert!(auth.authenticate(&headers(&signed(&claims))).is_some());
    }
}

#[test]
fn rejects_invalid_headers_signatures_claims_and_bootstrap_impersonation() {
    let auth = auth();
    assert!(Auth::new(String::new(), None).is_err());
    assert!(auth.authenticate(&HeaderMap::new()).is_none());
    for value in [
        "Basic abc",
        "Bearer ",
        "Bearer bad-token",
        "Bearer test-internal-service-token extra",
    ] {
        let mut headers = HeaderMap::new();
        headers.insert(header::AUTHORIZATION, value.parse().unwrap());
        assert!(auth.authenticate(&headers).is_none());
    }
    let mut duplicate = headers(INTERNAL);
    duplicate.append(
        header::AUTHORIZATION,
        format!("Bearer {INTERNAL}").parse().unwrap(),
    );
    assert!(auth.authenticate(&duplicate).is_none());
    for (field, value) in [
        ("exp", json!(get_current_timestamp() - 120)),
        ("nbf", json!(get_current_timestamp() + 300)),
        ("iss", json!("someone-else")),
        ("aud", json!("another-service")),
        ("sub", json!("")),
        ("principal_type", json!("AtgsInternalService")),
    ] {
        let mut claims = claims();
        claims[field] = value;
        assert!(
            auth.authenticate(&headers(&signed(&claims))).is_none(),
            "accepted {field}"
        );
    }
    for field in ["sub", "exp", "iss", "aud"] {
        let mut claims = claims();
        claims.as_object_mut().unwrap().remove(field);
        assert!(
            auth.authenticate(&headers(&signed(&claims))).is_none(),
            "missing {field}"
        );
    }
    let wrong_algorithm = encode(
        &Header::default(),
        &claims(),
        &EncodingKey::from_secret(b"not-an-rsa-key"),
    )
    .unwrap();
    assert!(auth.authenticate(&headers(&wrong_algorithm)).is_none());
    let mut token = signed(&claims()).into_bytes();
    let signature = token.iter().rposition(|byte| *byte == b'.').unwrap() + 1;
    token[signature] = if token[signature] == b'A' { b'B' } else { b'A' };
    assert!(
        auth.authenticate(&headers(&String::from_utf8(token).unwrap()))
            .is_none()
    );
    assert!(
        Auth::new(INTERNAL.into(), None)
            .unwrap()
            .authenticate(&headers(&signed(&claims())))
            .is_none()
    );
}

struct App {
    router: Router,
    db: DatabaseConnection,
    node: ActorRef<NodeActor>,
    partition: ActorRef<PartitionActor>,
    table: ActorRef<TableActor>,
    executor: ActorRef<LongRunningWorkflowExecutor>,
}

impl App {
    async fn new() -> Self {
        let db = Database::connect("sqlite::memory:").await.unwrap();
        let schema = Schema::new(db.get_database_backend());
        for statement in [
            schema.create_table_from_entity(node::Entity),
            schema.create_table_from_entity(partition::Entity),
            schema.create_table_from_entity(table::Entity),
            schema.create_table_from_entity(parition_lock::Entity),
        ] {
            db.execute(&statement).await.unwrap();
        }
        for index in 0..3 {
            node::ActiveModel {
                url: Set(format!("http://127.0.0.1:{}", 1 + index)),
                ..Default::default()
            }
            .insert(&db)
            .await
            .unwrap();
        }
        let node = NodeActor::spawn(NodeActor::new(db.clone()));
        let partition = PartitionActor::spawn(PartitionActor::new(db.clone()));
        let table = TableActor::spawn(TableActor::new(db.clone()));
        let executor = LongRunningWorkflowExecutor::spawn(
            LongRunningWorkflowExecutor::with_config(
                node.clone(),
                partition.clone(),
                table.clone(),
                ExecutorConfig {
                    min_check_interval: Duration::from_secs(3600),
                    max_check_interval: Duration::from_secs(3600),
                    ..Default::default()
                },
            )
            .unwrap(),
        );
        let router = api::router(
            node.clone(),
            partition.clone(),
            table.clone(),
            executor.clone(),
            auth(),
        );
        Self {
            router,
            db,
            node,
            partition,
            table,
            executor,
        }
    }

    async fn request(
        &self,
        method: &str,
        path: &str,
        token: Option<&str>,
        body: Value,
    ) -> (StatusCode, Value) {
        let mut request = Request::builder()
            .method(method)
            .uri(path)
            .header("content-type", "application/json")
            .header("x-patchwork-timestamp", "1");
        if let Some(token) = token {
            request = request.header(header::AUTHORIZATION, format!("Bearer {token}"));
        }
        let response = self
            .router
            .clone()
            .oneshot(request.body(Body::from(body.to_string())).unwrap())
            .await
            .unwrap();
        let status = response.status();
        let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        (
            status,
            if bytes.is_empty() {
                Value::Null
            } else {
                serde_json::from_slice(&bytes).unwrap()
            },
        )
    }

    async fn stop(self) {
        self.executor.stop_gracefully().await.unwrap();
        self.executor.wait_for_shutdown().await;
        self.node.stop_gracefully().await.unwrap();
        self.node.wait_for_shutdown().await;
        self.partition.stop_gracefully().await.unwrap();
        self.partition.wait_for_shutdown().await;
        self.table.stop_gracefully().await.unwrap();
        self.table.wait_for_shutdown().await;
        self.db.close().await.unwrap();
    }
}

#[tokio::test]
async fn every_public_route_requires_authentication_and_jwts_cannot_skip_permissions() {
    let app = App::new().await;
    let jwt = signed(&claims());
    for (method, path) in [
        ("GET", "/telemetry"),
        ("DELETE", "/telemetry"),
        ("POST", "/tables"),
        ("DELETE", "/tables/1"),
        ("GET", "/tables/1/partition-keys/10/records/42"),
        ("PUT", "/tables/1/partition-keys/10/records/42"),
        ("GET", "/tables/1/range?lower_bound=0&upper_bound=100"),
    ] {
        let body = json!({"table_name": "test", "table_owner": 7, "partition_key_name": "p", "sort_key_name": "s"});
        assert_eq!(
            app.request(method, path, None, body.clone()).await.0,
            StatusCode::UNAUTHORIZED,
            "{method} {path}"
        );
        assert_eq!(
            app.request(method, path, Some("wrong"), body.clone())
                .await
                .0,
            StatusCode::UNAUTHORIZED
        );
        assert_eq!(
            app.request(method, path, Some(&jwt), body).await.0,
            StatusCode::SERVICE_UNAVAILABLE
        );
    }
    assert_eq!(
        app.request("GET", "/telemetry", Some(INTERNAL), Value::Null)
            .await
            .0,
        StatusCode::OK
    );
    assert!(table::Entity::find().all(&app.db).await.unwrap().is_empty());
    app.stop().await;
}

#[tokio::test]
async fn table_key_metadata_is_saved_and_encrypted_writes_fail_without_the_service() {
    let app = App::new().await;
    for key in [None, Some(7)] {
        let table = table::ActiveModel {
            table_name: Set(format!("test-{key:?}")),
            owner: Set(7),
            partition_key_name: Set("p".into()),
            sort_key_name: Set("s".into()),
            secret_key_id: Set(key),
            is_ready: Set(true),
            ..Default::default()
        }
        .insert(&app.db)
        .await
        .unwrap();
        let id = table.table_id;
        assert_eq!(
            table::Entity::find_by_id(id)
                .one(&app.db)
                .await
                .unwrap()
                .unwrap()
                .secret_key_id,
            key
        );
        if key.is_some() {
            let (status, body) = app
                .request(
                    "PUT",
                    &format!("/tables/{id}/partition-keys/10/records/42"),
                    Some(INTERNAL),
                    json!({"hello": "world"}),
                )
                .await;
            assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
            assert_eq!(body["error"], "encryption service is not configured");
        }
    }
    app.stop().await;
}

#[tokio::test]
async fn encryption_stub_only_passes_through_unencrypted_bytes() {
    let client = EncryptionClient;
    let data = b"{\"hello\":\"world\"}".to_vec();
    assert_eq!(client.encrypt(None, data.clone()).await.unwrap(), data);
    assert_eq!(client.decrypt(None, data.clone()).await.unwrap(), data);
    assert!(client.encrypt(Some(7), data.clone()).await.is_err());
    assert!(client.decrypt(Some(7), data).await.is_err());
}
