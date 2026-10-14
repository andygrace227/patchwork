use std::time::Duration;

use kameo::actor::Spawn;
use patchwork_control_plane::{
    actors::{NodeActor, PartitionActor, TableActor, crud::Create},
    api,
    datamodels::{node, partition, table},
    dataplane_client::{Client, Record},
    workflows::executor::{ExecutorConfig, GetExecutorStatus, LongRunningWorkflowExecutor},
};
use patchwork_data_plane::write_shard::WriteShard;
use reqwest::StatusCode;
use sea_orm::{ActiveValue::Set, ConnectionTrait, Database, Schema};
use serde_json::json;

async fn serve(app: axum::Router) -> (String, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    (url, server)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn routes_invoke_record_workflows_and_report_table_job_results() {
    let client = Client::new();
    let db = Database::connect("sqlite::memory:").await.unwrap();
    let schema = Schema::new(db.get_database_backend());
    for statement in [
        schema.create_table_from_entity(node::Entity),
        schema.create_table_from_entity(partition::Entity),
        schema.create_table_from_entity(table::Entity),
    ] {
        db.execute(&statement).await.unwrap();
    }
    let node = NodeActor::spawn(NodeActor::new(db.clone()));
    let partition = PartitionActor::spawn(PartitionActor::new(db.clone()));
    let table = TableActor::spawn(TableActor::new(db));
    let dir = tempfile::tempdir().unwrap();
    let mut shards = Vec::new();
    let mut servers = Vec::new();
    let mut replicas = Vec::new();
    let mut urls = Vec::new();
    for timestamp in [1, 2] {
        let shard = WriteShard::new(
            dir.path()
                .join(format!("{timestamp}.sqlite"))
                .to_str()
                .unwrap()
                .into(),
        )
        .await
        .unwrap();
        let shard_readers = shard.read_pool(2).unwrap();
        let shard = WriteShard::spawn(shard);
        let (url, server) = serve(
            patchwork_data_plane::api::router(
                shard.clone(),
                shard_readers.clone(),
                &dir.path().join(format!("{timestamp}.wal")),
            )
            .unwrap(),
        )
        .await;
        client
            .upsert(&url, 1, 10, 42, &json!({"value": timestamp}), timestamp)
            .await
            .unwrap();
        let replica = node
            .ask(Create::<node::Entity> {
                data: node::ActiveModel {
                    url: Set(url.clone()),
                    ..Default::default()
                },
            })
            .await
            .unwrap();
        replicas.push(replica.node_id);
        shards.push((shard, shard_readers));
        servers.push(server);
        urls.push(url);
    }
    table
        .ask(Create::<table::Entity> {
            data: table::ActiveModel {
                table_id: Set(1),
                table_name: Set("accounts".into()),
                owner: Set(1),
                partition_key_name: Set("account".into()),
                sort_key_name: Set("id".into()),
                is_ready: Set(true),
                ..Default::default()
            },
        })
        .await
        .unwrap();
    partition
        .ask(Create::<partition::Entity> {
            data: partition::ActiveModel {
                table_id: Set(1),
                hash_start: Set(i64::MIN),
                forward_to: Set(None),
                replicas: Set(partition::ReplicaNodes(replicas)),
            },
        })
        .await
        .unwrap();
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
    let (url, server) = serve(api::router(node, partition, table, executor.clone())).await;
    servers.push(server);
    let http = reqwest::Client::new();
    let record_url = format!("{url}/tables/1/partition-keys/10/records/42");
    let record: Record = http
        .get(&record_url)
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(record.timestamp, 2);
    let record: Record = http
        .get(format!("{record_url}?consistent=false"))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!([1, 2].contains(&record.timestamp));
    let range: Vec<Record> = http
        .get(format!(
            "{url}/tables/1/range?lower_bound=10&upper_bound=11"
        ))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(range.len(), 1);
    assert_eq!(range[0].partition_key, 10);
    assert_eq!(
        http.get(format!(
            "{url}/tables/1/range?lower_bound=11&upper_bound=10"
        ))
        .send()
        .await
        .unwrap()
        .status(),
        StatusCode::BAD_REQUEST
    );
    let empty: Vec<Record> = http
        .get(format!(
            "{url}/tables/1/range?lower_bound=10&upper_bound=10"
        ))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(empty.is_empty());
    client.delete(&urls[1], 1, 10, 42, 3).await.unwrap();
    assert_eq!(
        http.get(&record_url).send().await.unwrap().status(),
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        http.get(format!("{record_url}?consistent=bogus"))
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::BAD_REQUEST
    );

    for (query, timestamp) in [("", 4), ("?consistent=false", 5)] {
        let data = json!({"value": timestamp});
        assert_eq!(
            http.put(format!("{record_url}{query}"))
                .header("x-patchwork-timestamp", timestamp.to_string())
                .json(&data)
                .send()
                .await
                .unwrap()
                .status(),
            StatusCode::NO_CONTENT
        );
        for url in &urls {
            let record = client.get(url, 1, 10, 42).await.unwrap();
            assert_eq!(record.data, data);
            assert_eq!(record.timestamp, timestamp);
        }
    }
    for timestamp in [None, Some("bogus"), Some("-1"), Some("9223372036854775808")] {
        let mut request = http.put(&record_url).json(&json!({"value": "invalid"}));
        if let Some(timestamp) = timestamp {
            request = request.header("x-patchwork-timestamp", timestamp);
        }
        assert_eq!(
            request.send().await.unwrap().status(),
            StatusCode::BAD_REQUEST
        );
    }
    // An acknowledged stale write must not replace the newer value on any replica.
    assert_eq!(
        http.put(&record_url)
            .header("x-patchwork-timestamp", "4")
            .json(&json!({"value": "stale"}))
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::NO_CONTENT
    );
    for url in &urls {
        let record = client.get(url, 1, 10, 42).await.unwrap();
        assert_eq!(record.data, json!({"value": 5}));
        assert_eq!(record.timestamp, 5);
    }

    // Default requests do not collect timings or add a timing header.
    let unmeasured = http.get(&record_url).send().await.unwrap();
    assert!(unmeasured.headers().get("server-timing").is_none());
    let before: serde_json::Value = http
        .get(format!("{url}/telemetry"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(before, json!({}));
    for consistent in [true, false] {
        let measured_url = format!("{record_url}?consistent={consistent}&telemetry=true");
        let response = http
            .put(&measured_url)
            .header("x-patchwork-timestamp", "6")
            .json(&json!({"value": 6}))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NO_CONTENT);
        assert!(
            response.headers()["server-timing"]
                .to_str()
                .unwrap()
                .contains("partition_lookup;dur=")
        );
        let response = http.get(&measured_url).send().await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert!(
            response.headers()["server-timing"]
                .to_str()
                .unwrap()
                .contains("replica_grpc;dur=")
        );
    }
    let response = http
        .get(format!(
            "{url}/tables/1/range?lower_bound=10&upper_bound=11&telemetry=true"
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert!(response.headers().get("server-timing").is_some());
    let missing = http
        .get(format!(
            "{url}/tables/1/partition-keys/10/records/999?telemetry=true"
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(missing.status(), StatusCode::NOT_FOUND);
    assert!(missing.headers().get("server-timing").is_some());
    let timings: serde_json::Value = http
        .get(format!("{url}/telemetry"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    for operation in [
        "put_consistent",
        "put_inconsistent",
        "fetch_consistent",
        "fetch_inconsistent",
        "fetch_range",
    ] {
        let stats = &timings[operation];
        assert!(stats["requests"].as_u64().unwrap() > 0);
        for stage in [
            "total",
            "table_lookup",
            "partition_lookup",
            "replica_resolution",
            "replica_lookup",
            "replica_grpc",
        ] {
            assert!(stats["stages"][stage]["calls"].as_u64().unwrap() > 0);
            assert!(stats["stages"][stage]["average_ms"].as_f64().unwrap() >= 0.0);
        }
    }
    assert_eq!(timings["fetch_consistent"]["errors"], 1);
    assert_eq!(
        timings["fetch_consistent"]["stages"]["replica_grpc"]["calls"],
        4
    );
    assert_eq!(
        http.delete(format!("{url}/telemetry"))
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::NO_CONTENT
    );
    let reset: serde_json::Value = http
        .get(format!("{url}/telemetry"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(reset, json!({}));

    let mut request = json!({
        "table_name": " ", "table_owner": 1,
        "partition_key_name": "account", "sort_key_name": "id",
    });
    assert_eq!(
        http.post(format!("{url}/tables"))
            .json(&request)
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::BAD_REQUEST
    );
    request["table_name"] = json!("new-table");
    // Only two data nodes: MakeTable must fail before creating metadata.
    assert_eq!(
        http.post(format!("{url}/tables"))
            .json(&request)
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::INTERNAL_SERVER_ERROR
    );
    assert_eq!(
        http.delete(format!("{url}/tables/999"))
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::NO_CONTENT
    );
    let status = executor.ask(GetExecutorStatus).await.unwrap();
    assert_eq!(status.failed, 1);
    assert_eq!(status.completed, 1);
    executor.stop_gracefully().await.unwrap();
    executor.wait_for_shutdown().await;
    assert_eq!(
        http.delete(format!("{url}/tables/999"))
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::SERVICE_UNAVAILABLE
    );

    for server in servers {
        server.abort();
        let _ = server.await;
    }
    for (shard, shard_readers) in shards {
        shard_readers.stop_gracefully().await.unwrap();
        shard_readers.wait_for_shutdown().await;
        shard.stop_gracefully().await.unwrap();
        shard.wait_for_shutdown().await;
    }
}
