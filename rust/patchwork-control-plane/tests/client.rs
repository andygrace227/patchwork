use kameo::actor::Spawn;
use patchwork_control_plane::client::Client;
use patchwork_data_plane::{api, shard_actor::ShardActor};
use serde_json::json;

#[tokio::test]
async fn client_calls_all_shard_operations() {
    let dir = tempfile::tempdir().unwrap();
    let shard = ShardActor::spawn(
        ShardActor::new(dir.path().join("shard.sqlite").to_str().unwrap().into())
            .await
            .unwrap(),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/", listener.local_addr().unwrap());
    let app = api::router(shard.clone());
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let size = Client::get_size(&url).await.unwrap();
    let actual = std::fs::metadata(dir.path().join("shard.sqlite"))
        .unwrap()
        .len();
    assert!(actual > 0);
    assert_eq!(size, json!({"size_bytes": actual}));
    let payload = json!({"name": "example", "nested": [1, true]});
    Client::upsert(&url, 1, 10, -42, &payload, 1).await.unwrap();
    let record = Client::get(&url, 1, 10, -42).await.unwrap();
    assert_eq!(record.data, payload);
    assert_eq!(record.timestamp, 1);
    Client::upsert(&url, 1, 10, -42, &json!({"updated": true}), 2)
        .await
        .unwrap();
    assert_eq!(Client::get(&url, 1, 10, -42).await.unwrap().timestamp, 2);
    assert_eq!(
        Client::get_partition_key(&url, 1, 10).await.unwrap().len(),
        1
    );
    assert_eq!(Client::get_range(&url, 1, 11, 9).await.unwrap().len(), 1);
    assert!(Client::get_range(&url, 1, 10, 9).await.unwrap().is_empty());
    assert_eq!(
        Client::get_range(&url, 1, 9, 11)
            .await
            .unwrap_err()
            .status(),
        Some(reqwest::StatusCode::BAD_REQUEST)
    );
    assert_eq!(
        Client::delete(&url, 1, 10, -42, 3).await.unwrap(),
        json!({"deleted": 1})
    );
    assert_eq!(
        Client::get(&url, 1, 10, -42).await.unwrap_err().status(),
        Some(reqwest::StatusCode::NOT_FOUND)
    );
    Client::upsert(&url, 1, 10, 2, &payload, 1).await.unwrap();
    assert_eq!(
        Client::delete_partition_key(&url, 1, 10).await.unwrap(),
        json!({"deleted": 2})
    );
    assert!(
        Client::get_partition_key(&url, 1, 10)
            .await
            .unwrap()
            .is_empty()
    );
    // Whole-table cleanup includes both ends of the ring and preserves other tables.
    for key in [i64::MIN, 0, i64::MAX] {
        Client::upsert(&url, 7, key, 1, &payload, 1).await.unwrap();
    }
    Client::upsert(&url, 8, i64::MAX, 1, &payload, 1)
        .await
        .unwrap();
    assert_eq!(
        Client::delete_table(&url, 7).await.unwrap(),
        json!({"deleted": 3})
    );
    assert_eq!(
        Client::delete_table(&url, 7).await.unwrap(),
        json!({"deleted": 0})
    );
    for key in [i64::MIN, 0, i64::MAX] {
        assert_eq!(
            Client::get(&url, 7, key, 1).await.unwrap_err().status(),
            Some(reqwest::StatusCode::NOT_FOUND)
        );
    }
    assert_eq!(
        Client::get(&url, 8, i64::MAX, 1).await.unwrap().data,
        payload
    );
    server.abort();
    let _ = server.await;
    shard.stop_gracefully().await.unwrap();
    shard.wait_for_shutdown().await;
}

#[tokio::test]
async fn range_totals_are_scoped_and_measure_payload_bytes() {
    let dir = tempfile::tempdir().unwrap();
    let shard = ShardActor::spawn(
        ShardActor::new(dir.path().join("totals.sqlite").to_str().unwrap().into())
            .await
            .unwrap(),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let app = api::router(shard.clone());
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let payload = json!({"text": "café"});
    for (table, key, secondary) in [(1, 10, 1), (1, 10, 2), (1, 20, 1), (2, 10, 1)] {
        Client::upsert(&url, table, key, secondary, &payload, 1)
            .await
            .unwrap();
    }
    assert_eq!(
        Client::count_range(&url, 1, 20, 10).await.unwrap(),
        json!({"count": 2})
    );
    assert_eq!(
        Client::get_range_size(&url, 1, 20, 10).await.unwrap(),
        json!({"size_bytes": 2 * payload.to_string().len()})
    );
    for empty in [(30, 20), (10, 0)] {
        let expected = if empty == (30, 20) { 1 } else { 0 };
        assert_eq!(
            Client::count_range(&url, 1, empty.0, empty.1)
                .await
                .unwrap(),
            json!({"count": expected})
        );
    }
    assert_eq!(
        Client::get_range_size(&url, 1, 10, 0).await.unwrap(),
        json!({"size_bytes": 0})
    );
    for (upper, lower) in [(10, 10), (10, 20)] {
        assert_eq!(
            Client::count_range(&url, 1, upper, lower)
                .await
                .unwrap_err()
                .status(),
            Some(reqwest::StatusCode::BAD_REQUEST)
        );
        assert_eq!(
            Client::get_range_size(&url, 1, upper, lower)
                .await
                .unwrap_err()
                .status(),
            Some(reqwest::StatusCode::BAD_REQUEST)
        );
    }
    Client::delete_range(&url, 1, 20, 10).await.unwrap();
    assert_eq!(
        Client::count_range(&url, 1, 20, 10).await.unwrap(),
        json!({"count": 0})
    );
    assert_eq!(
        Client::get_range_size(&url, 1, 20, 10).await.unwrap(),
        json!({"size_bytes": 0})
    );
    server.abort();
    let _ = server.await;
    shard.stop_gracefully().await.unwrap();
    shard.wait_for_shutdown().await;
}

#[tokio::test]
async fn put_with_replicas_writes_all_and_reports_partial_failure() {
    let dir = tempfile::tempdir().unwrap();
    let mut shards = Vec::new();
    let mut servers = Vec::new();
    let mut urls = Vec::new();
    for index in 0..3 {
        let shard = ShardActor::spawn(
            ShardActor::new(
                dir.path()
                    .join(format!("{index}.sqlite"))
                    .to_str()
                    .unwrap()
                    .into(),
            )
            .await
            .unwrap(),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        urls.push(format!("http://{}", listener.local_addr().unwrap()));
        let app = api::router(shard.clone());
        servers.push(tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap()
        }));
        shards.push(shard);
    }
    let payload = json!({"data": "payload", "replicas": "also payload"});
    let replicas = vec![urls[1].clone(), format!("{}/", urls[1]), urls[2].clone()];
    Client::put_with_replicas(&urls[0], 1, 10, 20, &payload, &replicas, 1)
        .await
        .unwrap();
    for url in &urls {
        let record = Client::get(url, 1, 10, 20).await.unwrap();
        assert_eq!(record.data, payload);
        assert_eq!(record.timestamp, 1); // The caller timestamp is preserved on every replica.
        assert!(Client::get(url, 2, 10, 20).await.is_err());
    }
    Client::put_with_replicas(&urls[0], 1, 10, 21, &payload, &[], 1)
        .await
        .unwrap();
    assert_eq!(
        Client::get(&urls[0], 1, 10, 21).await.unwrap().data,
        payload
    );
    assert!(Client::get(&urls[1], 1, 10, 21).await.is_err());

    // A failed destination must not prevent writes to the remaining replicas.
    let replicas = vec!["not a URL".into(), urls[1].clone(), urls[2].clone()];
    let error = Client::put_with_replicas(&urls[0], 1, 10, 22, &payload, &replicas, 1)
        .await
        .unwrap_err();
    assert_eq!(
        error.status(),
        Some(reqwest::StatusCode::INTERNAL_SERVER_ERROR)
    );
    for url in &urls {
        assert_eq!(Client::get(url, 1, 10, 22).await.unwrap().data, payload);
    }
    Client::delete_with_replicas(&urls[0], 1, 10, 20, &urls[1..], 2)
        .await
        .unwrap();
    for url in &urls {
        assert!(Client::get(url, 1, 10, 20).await.is_err());
        let record = Client::get_including_deleted(url, 1, 10, 20).await.unwrap();
        assert!(record.deleted);
        assert_eq!(record.timestamp, 2);
    }
    Client::delete_with_replicas_inconsistent(&urls[0], 1, 10, 22, &urls[1..], 3)
        .await
        .unwrap();
    // The queued third replica eventually receives the same tombstone.
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            let record = Client::get_including_deleted(&urls[2], 1, 10, 22)
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
    let records = Client::get_range_including_deleted(&urls[0], 1, 11, 10)
        .await
        .unwrap();
    let tombstone = records
        .iter()
        .find(|record| record.secondary_key == 22)
        .unwrap();
    assert!(tombstone.deleted);
    Client::write_record(
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
    Client::upsert(&urls[2], 9, 10, 22, &json!("stale copy"), 2)
        .await
        .unwrap();
    assert!(Client::get(&urls[2], 9, 10, 22).await.is_err());
    for server in servers {
        server.abort();
        let _ = server.await;
    }
    for shard in shards {
        shard.stop_gracefully().await.unwrap();
        shard.wait_for_shutdown().await;
    }
}

#[tokio::test]
async fn inconsistent_writes_queue_while_all_writes_wait_for_ack() {
    use patchwork_data_plane::subordinate_shard_writer::SubordinateShardWriter;
    use std::{sync::Arc, time::Duration};
    use tokio::sync::Semaphore;

    let entered = Arc::new(Semaphore::new(0));
    let release = Arc::new(Semaphore::new(0));
    let replica = axum::Router::new().route(
        "/tables/{table_id}/partition-keys/{partition_key}/records/{secondary_key}",
        axum::routing::put({
            let entered = entered.clone();
            let release = release.clone();
            move |headers: axum::http::HeaderMap| {
                let entered = entered.clone();
                let release = release.clone();
                async move {
                    assert_eq!(headers["x-patchwork-timestamp"], "1");
                    entered.add_permits(1);
                    release.acquire().await.unwrap().forget();
                    axum::http::StatusCode::NO_CONTENT
                }
            }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let replica_url = format!("http://{}", listener.local_addr().unwrap());
    let replica_server = tokio::spawn(async move { axum::serve(listener, replica).await.unwrap() });

    let dir = tempfile::tempdir().unwrap();
    let shard = ShardActor::spawn(
        ShardActor::new(dir.path().join("local.sqlite").to_str().unwrap().into())
            .await
            .unwrap(),
    );
    let writer = SubordinateShardWriter::spawn(SubordinateShardWriter);
    let app = api::router_with_writer(shard.clone(), writer.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

    let failover = ShardActor::spawn(
        ShardActor::new(dir.path().join("failover.sqlite").to_str().unwrap().into())
            .await
            .unwrap(),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let failover_url = format!("http://{}", listener.local_addr().unwrap());
    let failover_app = api::router(failover.clone());
    let failover_server =
        tokio::spawn(async move { axum::serve(listener, failover_app).await.unwrap() });

    // A blocked first replica prevents acknowledgment.
    let pending = tokio::spawn({
        let url = url.clone();
        let replica_url = replica_url.clone();
        async move {
            Client::put_with_replicas_inconsistent(&url, 1, 10, 99, &json!(99), &[replica_url], 1)
                .await
        }
    });
    tokio::time::timeout(Duration::from_secs(5), entered.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();
    assert!(!pending.is_finished());
    release.add_permits(1);
    tokio::time::timeout(Duration::from_secs(5), pending)
        .await
        .unwrap()
        .unwrap()
        .unwrap();

    let error = Client::put_with_replicas_inconsistent(&url, 1, 10, 100, &json!(100), &[], 1)
        .await
        .unwrap_err();
    assert_eq!(error.status(), Some(reqwest::StatusCode::BAD_REQUEST));
    assert!(Client::get(&url, 1, 10, 100).await.is_err());
    let error = Client::put_with_replicas_inconsistent(
        &url,
        1,
        10,
        101,
        &json!(101),
        &["invalid URL".into(), failover_url.clone()],
        1,
    )
    .await
    .unwrap_err();
    assert_eq!(
        error.status(),
        Some(reqwest::StatusCode::INTERNAL_SERVER_ERROR)
    );
    assert!(Client::get(&failover_url, 1, 10, 101).await.is_err());

    // Both requests return after the failover saves, while later replicas remain blocked.

    for key in [1, 2] {
        tokio::time::timeout(
            Duration::from_secs(5),
            Client::put_with_replicas_inconsistent(
                &url,
                1,
                10,
                key,
                &json!(key),
                &[
                    failover_url.clone(),
                    format!("{failover_url}/"),
                    replica_url.clone(),
                ],
                1,
            ),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(
            Client::get(&url, 1, 10, key).await.unwrap().data,
            json!(key)
        );
        let saved = Client::get(&failover_url, 1, 10, key).await.unwrap();
        assert_eq!(saved.data, json!(key));
        assert_eq!(saved.timestamp, 1);
    }
    tokio::time::timeout(Duration::from_secs(5), entered.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();
    assert_eq!(entered.available_permits(), 0); // Second write is still queued.

    // The all-replicas path bypasses the queue but waits for its remote reply.
    let all = tokio::spawn({
        let url = url.clone();
        let replica_url = replica_url.clone();
        async move { Client::put_with_replicas(&url, 1, 10, 3, &json!(3), &[replica_url], 1).await }
    });
    tokio::time::timeout(Duration::from_secs(5), entered.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();
    assert!(!all.is_finished());
    release.add_permits(3);
    tokio::time::timeout(Duration::from_secs(5), all)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    writer.stop_gracefully().await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), writer.wait_for_shutdown())
        .await
        .unwrap();
    assert_eq!(entered.available_permits(), 1); // Queued second write was processed.
    failover_server.abort();
    let _ = failover_server.await;
    failover.stop_gracefully().await.unwrap();
    failover.wait_for_shutdown().await;
    server.abort();
    replica_server.abort();
    let _ = server.await;
    let _ = replica_server.await;
    shard.stop_gracefully().await.unwrap();
    shard.wait_for_shutdown().await;
}
