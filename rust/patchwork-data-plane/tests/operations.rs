mod support;

use axum::{body::Body, http::Request};
use kameo::actor::Spawn;
use patchwork_data_plane::telemetry::{
    AccessStatsActor,
    access_stats_actor::{Flush, GetHistory},
};
use serde_json::{Value, json};
use support::Node;
use tonic::Code;
use tower::ServiceExt;

#[tokio::test]
async fn rest_routes_are_absent_and_backlog_does_not_create_access_samples() {
    let dir = tempfile::tempdir().unwrap();
    let node = Node::start(
        &dir.path().join("shard.sqlite"),
        &dir.path().join("wal"),
        None,
    )
    .await;
    for path in [
        "/shard/size",
        "/shard/telemetry",
        "/shard/write-backlog",
        "/tables/1/telemetry",
        "/tables/1",
        "/tables/1/partition-keys/10",
        "/tables/1/partition-keys/10/records/42",
        "/tables/1/partition-keys/10/records/42/with-replicas",
        "/tables/1/partition-keys/10/records/42/with-replicas-inconsistent",
        "/tables/1/range",
        "/tables/1/range/count",
        "/tables/1/range/size",
    ] {
        for method in ["GET", "PUT", "DELETE"] {
            let response = node
                .app
                .clone()
                .oneshot(
                    Request::builder()
                        .method(method)
                        .uri(path)
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.headers()["grpc-status"], "12", "{method} {path}");
            assert_eq!(response.headers()["content-type"], "application/grpc");
        }
    }
    assert_eq!(
        node.client.get_write_backlog(&node.url).await.unwrap(),
        json!({"queued": 0, "in_flight": 0, "total": 0})
    );
    assert!(
        node.client
            .get_telemetry(&node.url)
            .await
            .unwrap()
            .is_empty()
    );
    node.stop().await;
}

#[tokio::test]
async fn telemetry_tracks_accesses_and_preserves_history_after_cleanup_and_restart() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("shard.sqlite");
    let wal = dir.path().join("wal");
    let stats_path = dir.path().join("stats.sqlite");
    let stats = AccessStatsActor::spawn(AccessStatsActor::new(&stats_path).await.unwrap());
    let node = Node::start(&path, &wal, Some(stats.clone())).await;
    let (client, url) = (&node.client, &node.url);
    assert!(client.get_telemetry(url).await.unwrap().is_empty());
    for (table, partition, key) in [
        (1, 10, 10),
        (1, 10, 10),
        (1, 10, 1000),
        (1, 20, 20),
        (2, 10, 30),
    ] {
        client
            .write_partition_record(
                url,
                &patchwork_common::dataplane_client::Record {
                    table_id: table,
                    partition_id: Some(
                        if partition == 20 {
                            "partition-b"
                        } else {
                            "partition-a"
                        }
                        .into(),
                    ),
                    partition_key: partition,
                    secondary_key: key,
                    data: serde_json::to_vec(&json!(key)).unwrap(),
                    timestamp: 1,
                    deleted: false,
                },
                &[],
                true,
            )
            .await
            .unwrap();
    }
    node.flush().await;
    assert_eq!(
        client.get(url, 1, 10, 99).await.unwrap_err().code(),
        Code::NotFound
    );
    client.get_partition_key(url, 1, 10).await.unwrap();
    client.get_range(url, 1, 21, 20).await.unwrap();
    client.delete(url, 2, 10, 30, 3).await.unwrap();
    node.flush().await;
    let entries = client.get_telemetry(url).await.unwrap();
    assert_eq!(entries.len(), 3);
    let table = client.get_telemetry_for_table(url, 1).await.unwrap();
    assert_eq!(table.len(), 2);
    let sample = &table["partition-a"].statistics;
    assert_eq!(sample.contributing_nodes, 1);
    assert_eq!(sample.average_write_position, Some(10));
    assert_eq!(sample.median_write_position, Some(10));
    assert_eq!(sample.read_count, 2);
    assert_eq!(sample.write_count, 3);
    assert!((sample.reads_per_second / sample.writes_per_second - 2.0 / 3.0).abs() < 1e-9);
    assert!(table["partition-b"].statistics.reads_per_second > 0.0);
    assert_eq!(
        entries
            .iter()
            .find(|entry| entry.table_id == 2)
            .unwrap()
            .statistics
            .median_write_position,
        Some(10)
    );
    assert!(
        client
            .get_telemetry_for_table(url, 99)
            .await
            .unwrap()
            .is_empty()
    );
    client.delete_partition_key(url, 1, 20).await.unwrap();
    client.delete_range(url, 1, 11, 10).await.unwrap();
    node.flush().await;
    assert_eq!(client.get_telemetry(url).await.unwrap().len(), 3);
    assert_eq!(
        client.get_telemetry_for_table(url, 1).await.unwrap().len(),
        2
    );
    node.stop().await;
    stats.ask(Flush {}).await.unwrap();
    let history = stats
        .ask(GetHistory {
            table_id: 1,
            partition_id: "partition-a".into(),
            start_ms: 0,
            end_ms: i64::MAX,
        })
        .await
        .unwrap();
    assert_eq!(history[0].write_count, 3);
    assert_eq!(history[0].read_count, 2);
    stats.stop_gracefully().await.unwrap();
    stats.wait_for_shutdown().await;
    let stats = AccessStatsActor::spawn(AccessStatsActor::new(&stats_path).await.unwrap());
    assert_eq!(
        stats
            .ask(GetHistory {
                table_id: 1,
                partition_id: "partition-a".into(),
                start_ms: 0,
                end_ms: i64::MAX
            })
            .await
            .unwrap()
            .len(),
        1
    );
    // Committed log replay would create fresh samples; use a clean log here.
    let node = Node::start(&path, &dir.path().join("reopened-wal"), Some(stats.clone())).await;
    assert!(
        node.client
            .get_telemetry(&node.url)
            .await
            .unwrap()
            .is_empty()
    );
    let _ = node
        .client
        .get_including_deleted(&node.url, 2, 10, 30)
        .await;
    let table = node
        .client
        .get_telemetry_for_table(&node.url, 2)
        .await
        .unwrap();
    assert_eq!(table["partition-a"].statistics.average_write_position, None);
    assert_eq!(table["partition-a"].statistics.median_write_position, None);
    node.client.delete_table(&node.url, 2).await.unwrap();
    node.flush().await;
    assert_eq!(node.client.get_telemetry(&node.url).await.unwrap().len(), 1);
    node.stop().await;
    stats.stop_gracefully().await.unwrap();
    stats.wait_for_shutdown().await;
}

#[tokio::test]
async fn operations_are_scoped_and_persist_after_reopening() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("nested/shard.sqlite");
    let wal = dir.path().join("wal");
    let node = Node::start(&path, &wal, None).await;
    let (client, url) = (&node.client, &node.url);
    for (table, partition, key) in [(1, 10, -1), (1, 10, 2), (1, 20, 3), (2, 10, -1)] {
        client
            .upsert(url, table, partition, key, &json!({"value": "original"}), 1)
            .await
            .unwrap();
    }
    node.flush().await;
    assert_eq!(client.get(url, 1, 10, -1).await.unwrap().timestamp, 1);
    client
        .upsert(
            url,
            1,
            10,
            -1,
            &json!({"value": "updated", "timestamp": 99}),
            2,
        )
        .await
        .unwrap();
    node.flush().await;
    let record = client.get(url, 1, 10, -1).await.unwrap();
    assert_eq!(record.timestamp, 2);
    assert_eq!(record.data, json!({"value": "updated", "timestamp": 99}));
    assert_eq!(client.get_partition_key(url, 1, 10).await.unwrap().len(), 2);
    let range = client.get_range(url, 1, 20, 9).await.unwrap();
    assert_eq!(range.len(), 2);
    assert_eq!(client.get_range(url, 1, 20, 10).await.unwrap(), range);
    assert_eq!(
        client.get_range(url, 1, 10, 20).await.unwrap_err().code(),
        Code::InvalidArgument
    );
    assert_eq!(
        client.get_range(url, 1, 10, 10).await.unwrap_err().code(),
        Code::InvalidArgument
    );
    client.delete(url, 1, 10, -1, 3).await.unwrap();
    node.flush().await;
    assert_eq!(
        client.get(url, 1, 10, -1).await.unwrap_err().code(),
        Code::NotFound
    );
    client.delete_partition_key(url, 1, 10).await.unwrap();
    node.flush().await;
    assert!(
        client
            .get_partition_key(url, 1, 10)
            .await
            .unwrap()
            .is_empty()
    );
    node.stop().await;
    let node = Node::start(&path, &wal, None).await;
    node.client.get(&node.url, 2, 10, -1).await.unwrap();
    node.client.get(&node.url, 1, 20, 3).await.unwrap();
    for _ in 0..2 {
        node.client
            .upsert(&node.url, 1, 20, 3, &json!({"updated": true}), 2)
            .await
            .unwrap();
        node.flush().await;
        assert_eq!(
            node.client
                .get(&node.url, 1, 20, 3)
                .await
                .unwrap()
                .timestamp,
            2
        );
    }
    node.stop().await;
}

#[tokio::test]
async fn timestamp_wins_regardless_of_arrival_order() {
    let dir = tempfile::tempdir().unwrap();
    let node = Node::start(
        &dir.path().join("shard.sqlite"),
        &dir.path().join("wal"),
        None,
    )
    .await;
    for (key, writes) in [
        (1, vec![(20, "new"), (10, "old"), (20, "new")]),
        (2, vec![(10, "old"), (20, "new")]),
        (3, vec![(30, "a"), (30, "z")]),
        (4, vec![(30, "z"), (30, "a")]),
    ] {
        for (timestamp, value) in writes {
            node.client
                .upsert(&node.url, 1, 1, key, &json!(value), timestamp)
                .await
                .unwrap();
        }
        node.flush().await;
        let record = node.client.get(&node.url, 1, 1, key).await.unwrap();
        assert_eq!(
            record.data,
            if key <= 2 { json!("new") } else { json!("z") }
        );
        assert_eq!(record.timestamp, if key <= 2 { 20 } else { 30 });
    }
    node.stop().await;
}

#[tokio::test]
async fn tombstones_win_ties_and_survive_reopening() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("shard.sqlite");
    let wal = dir.path().join("wal");
    let node = Node::start(&path, &wal, None).await;
    let (client, url) = (&node.client, &node.url);
    client.delete(url, 1, 10, 1, 20).await.unwrap();
    for timestamp in [10, 20] {
        client
            .upsert(url, 1, 10, 1, &json!("late"), timestamp)
            .await
            .unwrap();
        node.flush().await;
        assert_eq!(
            client.get(url, 1, 10, 1).await.unwrap_err().code(),
            Code::NotFound
        );
    }
    let stored = client.get_including_deleted(url, 1, 10, 1).await.unwrap();
    assert!(stored.deleted);
    assert_eq!(stored.timestamp, 20);
    assert_eq!(stored.data, Value::Null);
    assert!(
        client
            .get_partition_key(url, 1, 10)
            .await
            .unwrap()
            .is_empty()
    );
    assert!(client.get_range(url, 1, 20, 0).await.unwrap().is_empty());
    assert_eq!(
        client.count_range(url, 1, 20, 0).await.unwrap(),
        json!({"count": 0})
    );
    assert_eq!(
        client.get_range_size(url, 1, 20, 0).await.unwrap(),
        json!({"size_bytes": 0})
    );
    assert_eq!(
        client
            .get_range_including_deleted(url, 1, 20, 0)
            .await
            .unwrap(),
        vec![stored]
    );
    client
        .upsert(url, 1, 10, 1, &json!("new"), 30)
        .await
        .unwrap();
    client.delete(url, 1, 10, 1, 29).await.unwrap();
    node.flush().await;
    assert_eq!(client.get(url, 1, 10, 1).await.unwrap().data, "new");
    client.delete(url, 1, 10, 1, 30).await.unwrap();
    node.flush().await;
    assert_eq!(
        client.get(url, 1, 10, 1).await.unwrap_err().code(),
        Code::NotFound
    );
    node.stop().await;
    let node = Node::start(&path, &wal, None).await;
    node.client
        .upsert(&node.url, 1, 10, 1, &json!("late"), 30)
        .await
        .unwrap();
    node.flush().await;
    assert_eq!(
        node.client
            .get(&node.url, 1, 10, 1)
            .await
            .unwrap_err()
            .code(),
        Code::NotFound
    );
    assert_eq!(
        node.client
            .get_including_deleted(&node.url, 1, 10, 1)
            .await
            .unwrap()
            .timestamp,
        30
    );
    node.stop().await;
}
