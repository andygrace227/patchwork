use axum::{
    Router,
    body::{Body, to_bytes},
    http::{Request, StatusCode},
};
use kameo::actor::Spawn;
use patchwork_data_plane::{api, shard_actor::ShardActor};
use serde_json::{Value, json};
use tower::ServiceExt;

async fn request(
    app: &Router,
    method: &str,
    path: &str,
    body: Option<Value>,
) -> (StatusCode, Value) {
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method(method)
                .uri(path)
                .header("content-type", "application/json")
                .body(body.map_or_else(Body::empty, |v| Body::from(v.to_string())))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status();
    let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    (
        status,
        if bytes.is_empty() {
            Value::Null
        } else {
            serde_json::from_slice(&bytes)
                .unwrap_or_else(|_| Value::String(String::from_utf8_lossy(&bytes).into_owned()))
        },
    )
}

#[tokio::test]
async fn operations_are_scoped_and_persist_after_reopening() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir
        .path()
        .join("nested/shard.sqlite")
        .to_str()
        .unwrap()
        .to_owned();
    let shard = ShardActor::spawn(ShardActor::new(path.clone()).await.unwrap());
    let app = api::router(shard.clone());
    for (table_id, partition, hash) in [(1, 10, -1), (1, 10, 2), (1, 20, 3), (2, 10, -1)] {
        assert_eq!(
            request(
                &app,
                "PUT",
                &format!("/tables/{table_id}/partitions/{partition}/records/{hash}"),
                Some(json!({"value": "original"}))
            )
            .await
            .0,
            StatusCode::NO_CONTENT
        );
    }
    assert_eq!(
        request(&app, "GET", "/tables/1/partitions/10/records/-1", None)
            .await
            .1["version"],
        1
    );
    assert_eq!(
        request(
            &app,
            "PUT",
            "/tables/1/partitions/10/records/-1",
            Some(json!({"value": "updated", "version": 99}))
        )
        .await
        .0,
        StatusCode::NO_CONTENT
    );
    let (status, record) = request(&app, "GET", "/tables/1/partitions/10/records/-1", None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(record["version"], 2);
    assert_eq!(record["data"]["value"], "updated");
    assert_eq!(record["data"]["version"], 99); // Payload fields do not set the record version.
    assert_eq!(
        request(&app, "GET", "/tables/1/partitions/10", None)
            .await
            .1
            .as_array()
            .unwrap()
            .len(),
        2
    );
    let range = request(
        &app,
        "GET",
        "/tables/1/range?lower_bound=9&upper_bound=20",
        None,
    )
    .await
    .1;
    assert_eq!(range.as_array().unwrap().len(), 2);
    assert_eq!(
        request(
            &app,
            "GET",
            "/tables/1/range?lower_bound=10&upper_bound=20",
            None
        )
        .await
        .1,
        json!([])
    );
    assert_eq!(
        request(
            &app,
            "GET",
            "/tables/1/range?lower_bound=20&upper_bound=10",
            None
        )
        .await
        .0,
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        request(&app, "PUT", "/tables/1/partitions/10/records/-1", None)
            .await
            .0,
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        request(&app, "DELETE", "/tables/1/partitions/10/records/-1", None)
            .await
            .1,
        json!({"deleted": 1})
    );
    assert_eq!(
        request(&app, "DELETE", "/tables/1/partitions/10/records/-1", None)
            .await
            .1,
        json!({"deleted": 0})
    );
    assert_eq!(
        request(&app, "GET", "/tables/1/partitions/10/records/-1", None)
            .await
            .0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        request(&app, "DELETE", "/tables/1/partitions/10", None)
            .await
            .1,
        json!({"deleted": 1})
    );
    assert_eq!(
        request(&app, "GET", "/tables/1/partitions/10", None)
            .await
            .1,
        json!([])
    );
    drop(app);
    shard.stop_gracefully().await.unwrap();
    shard.wait_for_shutdown().await;

    let shard = ShardActor::spawn(ShardActor::new(path).await.unwrap());
    let app = api::router(shard.clone());
    assert_eq!(
        request(&app, "GET", "/tables/2/partitions/10/records/-1", None)
            .await
            .0,
        StatusCode::OK
    );
    assert_eq!(
        request(&app, "GET", "/tables/1/partitions/20/records/3", None)
            .await
            .0,
        StatusCode::OK
    );
    for expected_version in [2, 3] {
        assert_eq!(
            request(
                &app,
                "PUT",
                "/tables/1/partitions/20/records/3",
                Some(json!({"updated": true}))
            )
            .await
            .0,
            StatusCode::NO_CONTENT
        );
        assert_eq!(
            request(&app, "GET", "/tables/1/partitions/20/records/3", None)
                .await
                .1["version"],
            expected_version
        );
    }
    shard.stop_gracefully().await.unwrap();
    shard.wait_for_shutdown().await;
}
