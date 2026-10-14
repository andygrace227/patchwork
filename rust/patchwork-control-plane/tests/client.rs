use kameo::actor::Spawn;
use patchwork_control_plane::dataplane_client::Client;
use patchwork_data_plane::{api, write_shard::WriteShard};
use serde_json::json;

#[tokio::test]
async fn client_calls_all_shard_operations() {
    use axum::serve::ListenerExt;
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    let client = Client::new();
    let dir = tempfile::tempdir().unwrap();
    let shard = WriteShard::new(dir.path().join("shard.sqlite").to_str().unwrap().into())
        .await
        .unwrap();
    let shard_readers = shard.read_pool(2).unwrap();
    let shard = WriteShard::spawn(shard);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/", listener.local_addr().unwrap());
    let app = patchwork_data_plane::grpc::router(
        shard.clone(),
        shard_readers.clone(),
        patchwork_data_plane::subordinate_shard_writer::SubordinateShardWriter::spawn(
            patchwork_data_plane::subordinate_shard_writer::SubordinateShardWriter::default(),
        ),
    );
    let connections = Arc::new(AtomicUsize::new(0));
    let accepted = connections.clone();
    let server = tokio::spawn(async move {
        axum::serve(
            listener.tap_io(move |_| {
                accepted.fetch_add(1, Ordering::SeqCst);
            }),
            app,
        )
        .await
        .unwrap()
    });
    let size = client.get_size(&url).await.unwrap();
    let actual = std::fs::metadata(dir.path().join("shard.sqlite"))
        .unwrap()
        .len();
    assert!(actual > 0);
    assert_eq!(size, json!({"size_bytes": actual}));
    assert!(client.get_telemetry(&url).await.unwrap().is_empty());
    let payload = json!({"name": "example", "nested": [1, true]});
    client.upsert(&url, 1, 10, -42, &payload, 1).await.unwrap();
    let record = client.get(&url, 1, 10, -42).await.unwrap();
    assert_eq!(record.data, payload);
    assert_eq!(record.timestamp, 1);
    let telemetry = client
        .get_telemetry(url.trim_end_matches('/'))
        .await
        .unwrap();
    assert_eq!(telemetry.len(), 1);
    assert_eq!(telemetry[0].table_id, 1);
    assert_eq!(telemetry[0].partition_key, 10);
    assert_eq!(telemetry[0].statistics.average_write_position, Some(-42));
    assert_eq!(telemetry[0].statistics.median_write_position, Some(-42));
    assert!(telemetry[0].statistics.reads_per_second > 0.0);
    assert!(telemetry[0].statistics.writes_per_second > 0.0);
    assert_eq!(telemetry[0].statistics.read_count, 1);
    assert_eq!(telemetry[0].statistics.contributing_nodes, 1);
    let table_telemetry = client.get_telemetry_for_table(&url, 1).await.unwrap();
    assert_eq!(table_telemetry.len(), 1);
    assert_eq!(
        table_telemetry[&10].statistics.median_write_position,
        Some(-42)
    );
    assert!(
        client
            .get_telemetry_for_table(&url, 99)
            .await
            .unwrap()
            .is_empty()
    );
    client
        .upsert(&url, 1, 10, -42, &json!({"updated": true}), 2)
        .await
        .unwrap();
    assert_eq!(client.get(&url, 1, 10, -42).await.unwrap().timestamp, 2);
    assert_eq!(
        client.get_partition_key(&url, 1, 10).await.unwrap().len(),
        1
    );
    assert_eq!(client.get_range(&url, 1, 11, 9).await.unwrap().len(), 1);
    assert!(client.get_range(&url, 1, 10, 9).await.unwrap().is_empty());
    assert_eq!(
        client.get_range(&url, 1, 9, 11).await.unwrap_err().code(),
        tonic::Code::InvalidArgument
    );
    assert_eq!(
        client.delete(&url, 1, 10, -42, 3).await.unwrap(),
        json!({"deleted": 1})
    );
    assert_eq!(
        client.get(&url, 1, 10, -42).await.unwrap_err().code(),
        tonic::Code::NotFound
    );
    client.upsert(&url, 1, 10, 2, &payload, 1).await.unwrap();
    assert_eq!(
        client.delete_partition_key(&url, 1, 10).await.unwrap(),
        json!({"deleted": 2})
    );
    assert!(
        client
            .get_partition_key(&url, 1, 10)
            .await
            .unwrap()
            .is_empty()
    );
    // Whole-table cleanup includes both ends of the ring and preserves other tables.
    for key in [i64::MIN, 0, i64::MAX] {
        client.upsert(&url, 7, key, 1, &payload, 1).await.unwrap();
    }
    client
        .upsert(&url, 8, i64::MAX, 1, &payload, 1)
        .await
        .unwrap();
    let table_telemetry = client
        .get_telemetry_for_table(url.trim_end_matches('/'), 7)
        .await
        .unwrap();
    assert_eq!(table_telemetry.len(), 3);
    for key in [i64::MIN, 0, i64::MAX] {
        assert_eq!(table_telemetry[&key].table_id, 7);
        assert_eq!(table_telemetry[&key].partition_key, key);
    }
    assert_eq!(
        client.delete_table(&url, 7).await.unwrap(),
        json!({"deleted": 3})
    );
    assert_eq!(
        client.delete_table(&url, 7).await.unwrap(),
        json!({"deleted": 0})
    );
    for key in [i64::MIN, 0, i64::MAX] {
        assert_eq!(
            client.get(&url, 7, key, 1).await.unwrap_err().code(),
            tonic::Code::NotFound
        );
    }
    assert_eq!(
        client.get(&url, 8, i64::MAX, 1).await.unwrap().data,
        payload
    );
    // Clones and normalized URLs reuse one connection, even for concurrent calls.
    let cloned = client.clone();
    tokio::try_join!(
        cloned.get_size(url.trim_end_matches('/')),
        client.get_size(&url)
    )
    .unwrap();
    assert_eq!(connections.load(Ordering::SeqCst), 1);
    // A separately constructed client owns a separate cache.
    Client::new().get_size(&url).await.unwrap();
    assert_eq!(connections.load(Ordering::SeqCst), 2);
    server.abort();
    let _ = server.await;
    shard_readers.stop_gracefully().await.unwrap();
    shard_readers.wait_for_shutdown().await;
    shard.stop_gracefully().await.unwrap();
    shard.wait_for_shutdown().await;
}

#[tokio::test]
async fn range_totals_are_scoped_and_measure_payload_bytes() {
    let client = Client::new();
    let dir = tempfile::tempdir().unwrap();
    let shard = WriteShard::new(dir.path().join("totals.sqlite").to_str().unwrap().into())
        .await
        .unwrap();
    let shard_readers = shard.read_pool(2).unwrap();
    let shard = WriteShard::spawn(shard);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let app = api::router(shard.clone(), shard_readers.clone());
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let payload = json!({"text": "café"});
    for (table, key, secondary) in [(1, 10, 1), (1, 10, 2), (1, 20, 1), (2, 10, 1)] {
        client
            .upsert(&url, table, key, secondary, &payload, 1)
            .await
            .unwrap();
    }
    assert_eq!(
        client.count_range(&url, 1, 20, 10).await.unwrap(),
        json!({"count": 2})
    );
    assert_eq!(
        client.get_range_size(&url, 1, 20, 10).await.unwrap(),
        json!({"size_bytes": 2 * payload.to_string().len()})
    );
    for empty in [(30, 20), (10, 0)] {
        let expected = if empty == (30, 20) { 1 } else { 0 };
        assert_eq!(
            client.count_range(&url, 1, empty.0, empty.1).await.unwrap(),
            json!({"count": expected})
        );
    }
    assert_eq!(
        client.get_range_size(&url, 1, 10, 0).await.unwrap(),
        json!({"size_bytes": 0})
    );
    for (upper, lower) in [(10, 10), (10, 20)] {
        assert_eq!(
            client
                .count_range(&url, 1, upper, lower)
                .await
                .unwrap_err()
                .code(),
            tonic::Code::InvalidArgument
        );
        assert_eq!(
            client
                .get_range_size(&url, 1, upper, lower)
                .await
                .unwrap_err()
                .code(),
            tonic::Code::InvalidArgument
        );
    }
    client.delete_range(&url, 1, 20, 10).await.unwrap();
    assert_eq!(
        client.count_range(&url, 1, 20, 10).await.unwrap(),
        json!({"count": 0})
    );
    assert_eq!(
        client.get_range_size(&url, 1, 20, 10).await.unwrap(),
        json!({"size_bytes": 0})
    );
    server.abort();
    let _ = server.await;
    shard_readers.stop_gracefully().await.unwrap();
    shard_readers.wait_for_shutdown().await;
    shard.stop_gracefully().await.unwrap();
    shard.wait_for_shutdown().await;
}

#[tokio::test]
async fn put_with_replicas_writes_all_and_reports_partial_failure() {
    let client = Client::new();
    let dir = tempfile::tempdir().unwrap();
    let mut shards = Vec::new();
    let mut servers = Vec::new();
    let mut urls = Vec::new();
    for index in 0..3 {
        let shard = WriteShard::new(
            dir.path()
                .join(format!("{index}.sqlite"))
                .to_str()
                .unwrap()
                .into(),
        )
        .await
        .unwrap();
        let shard_readers = shard.read_pool(2).unwrap();
        let shard = WriteShard::spawn(shard);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        urls.push(format!("http://{}", listener.local_addr().unwrap()));
        let app = api::router(shard.clone(), shard_readers.clone());
        servers.push(tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap()
        }));
        shards.push((shard, shard_readers));
    }
    let payload = json!({"data": "payload", "replicas": "also payload"});
    let replicas = vec![urls[1].clone(), format!("{}/", urls[1]), urls[2].clone()];
    client
        .put_with_replicas(&urls[0], 1, 10, 20, &payload, &replicas, 1)
        .await
        .unwrap();
    for url in &urls {
        let record = client.get(url, 1, 10, 20).await.unwrap();
        assert_eq!(record.data, payload);
        assert_eq!(record.timestamp, 1); // The caller timestamp is preserved on every replica.
        assert!(client.get(url, 2, 10, 20).await.is_err());
    }
    client
        .put_with_replicas(&urls[0], 1, 10, 21, &payload, &[], 1)
        .await
        .unwrap();
    assert_eq!(client.get(&urls[0], 1, 10, 21).await.unwrap().data, payload);
    assert!(client.get(&urls[1], 1, 10, 21).await.is_err());

    // A failed destination must not prevent writes to the remaining replicas.
    let replicas = vec!["not a URL".into(), urls[1].clone(), urls[2].clone()];
    let error = client
        .put_with_replicas(&urls[0], 1, 10, 22, &payload, &replicas, 1)
        .await
        .unwrap_err();
    assert_eq!(error.code(), tonic::Code::Internal);
    for url in &urls {
        assert_eq!(client.get(url, 1, 10, 22).await.unwrap().data, payload);
    }
    client
        .delete_with_replicas(&urls[0], 1, 10, 20, &urls[1..], 2)
        .await
        .unwrap();
    for url in &urls {
        assert!(client.get(url, 1, 10, 20).await.is_err());
        let record = client.get_including_deleted(url, 1, 10, 20).await.unwrap();
        assert!(record.deleted);
        assert_eq!(record.timestamp, 2);
    }
    client
        .delete_with_replicas_inconsistent(&urls[0], 1, 10, 22, &urls[1..], 3)
        .await
        .unwrap();
    // The queued third replica eventually receives the same tombstone.
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            let record = client
                .get_including_deleted(&urls[2], 1, 10, 22)
                .await
                .unwrap();
            if record.deleted {
                assert_eq!(record.timestamp, 3);
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    // A copy transports the flag and timestamp, not a new write time.
    let records = client
        .get_range_including_deleted(&urls[0], 1, 11, 10)
        .await
        .unwrap();
    let tombstone = records
        .iter()
        .find(|record| record.secondary_key == 22)
        .unwrap();
    assert!(tombstone.deleted);
    client
        .write_record(
            &urls[2],
            9,
            10,
            22,
            &tombstone.data,
            tombstone.timestamp,
            tombstone.deleted,
        )
        .await
        .unwrap();
    client
        .upsert(&urls[2], 9, 10, 22, &json!("stale copy"), 2)
        .await
        .unwrap();
    assert!(client.get(&urls[2], 9, 10, 22).await.is_err());
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

#[tokio::test]
async fn inconsistent_writes_ack_early_and_consistent_writes_wait_for_neighbors() {
    use patchwork_data_plane::{
        subordinate_shard_writer::SubordinateShardWriter, write_handler::WriteHandler,
    };
    use std::{sync::Arc, time::Duration};
    use tokio::sync::Semaphore;

    let client = Client::new();
    let entered = Arc::new(Semaphore::new(0));
    let release = Arc::new(Semaphore::new(0));
    let dir = tempfile::tempdir().unwrap();
    let remote = WriteShard::new(dir.path().join("remote.sqlite").to_str().unwrap().into())
        .await
        .unwrap();
    let remote_readers = remote.read_pool(2).unwrap();
    let remote = WriteShard::spawn(remote);
    let replica =
        api::router(remote.clone(), remote_readers.clone()).layer(axum::middleware::from_fn({
            let entered = entered.clone();
            let release = release.clone();
            move |request: axum::extract::Request, next: axum::middleware::Next| {
                let entered = entered.clone();
                let release = release.clone();
                async move {
                    if request.uri().path() == "/patchwork.dataplane.DataPlane/WriteRecord" {
                        entered.add_permits(1);
                        release.acquire().await.unwrap().forget();
                    }
                    next.run(request).await
                }
            }
        }));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let replica_url = format!("http://{}", listener.local_addr().unwrap());
    let replica_server = tokio::spawn(async move { axum::serve(listener, replica).await.unwrap() });
    let shard = WriteShard::new(dir.path().join("local.sqlite").to_str().unwrap().into())
        .await
        .unwrap();
    let readers = shard.read_pool(2).unwrap();
    let shard = WriteShard::spawn(shard);
    let writer = SubordinateShardWriter::spawn(SubordinateShardWriter::default());
    let handler = WriteHandler::spawn(WriteHandler::new(shard.clone(), writer.clone()));
    let app = api::router_with_handler(shard.clone(), readers.clone(), handler.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

    // Inconsistent writes wait for local storage while neighbors run independently.
    client
        .put_with_replicas_inconsistent(&url, 1, 10, 1, &json!(1), &[replica_url.clone()], 10)
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(2), entered.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();
    tokio::time::timeout(
        Duration::from_secs(2),
        client.put_with_replicas_inconsistent(
            &url,
            1,
            10,
            2,
            &json!(2),
            &[replica_url.clone(), format!("{replica_url}/")],
            20,
        ),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(client.get(&url, 1, 10, 2).await.unwrap().timestamp, 20);

    let consistent = tokio::spawn({
        let client = client.clone();
        let url = url.clone();
        let replica_url = replica_url.clone();
        async move {
            client
                .put_with_replicas(&url, 1, 10, 3, &json!(3), &[replica_url], 30)
                .await
        }
    });
    // Wait for both the second queued write and the consistent write to reach the neighbor.
    for _ in 0..2 {
        tokio::time::timeout(Duration::from_secs(2), entered.acquire())
            .await
            .unwrap()
            .unwrap()
            .forget();
    }
    assert!(!consistent.is_finished());
    // Network waits must not block the handler's next admission.
    tokio::time::timeout(
        Duration::from_secs(2),
        client.upsert(&url, 1, 10, 4, &json!(4), 40),
    )
    .await
    .unwrap()
    .unwrap();
    release.add_permits(3);
    consistent.await.unwrap().unwrap();
    for key in 1..=3 {
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if let Ok(record) = client.get(&replica_url, 1, 10, key).await {
                    assert_eq!(record.timestamp, key * 10);
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
    }

    assert_eq!(
        client
            .put_with_replicas_inconsistent(&url, 1, 10, 5, &json!(5), &[], 50)
            .await
            .unwrap_err()
            .code(),
        tonic::Code::InvalidArgument
    );
    handler.stop_gracefully().await.unwrap();
    handler.wait_for_shutdown().await;
    writer.stop_gracefully().await.unwrap();
    writer.wait_for_shutdown().await;
    server.abort();
    replica_server.abort();
    readers.stop_gracefully().await.unwrap();
    remote_readers.stop_gracefully().await.unwrap();
    shard.stop_gracefully().await.unwrap();
    remote.stop_gracefully().await.unwrap();
}
