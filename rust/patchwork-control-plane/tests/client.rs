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
    Client::upsert(&url, 1, 10, -42, &payload).await.unwrap();
    let record = Client::get(&url, 1, 10, -42).await.unwrap();
    assert_eq!(record.data, payload);
    assert_eq!(record.version, 1);
    Client::upsert(&url, 1, 10, -42, &json!({"updated": true}))
        .await
        .unwrap();
    assert_eq!(Client::get(&url, 1, 10, -42).await.unwrap().version, 2);
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
        Client::delete(&url, 1, 10, -42).await.unwrap(),
        json!({"deleted": 1})
    );
    assert_eq!(
        Client::get(&url, 1, 10, -42).await.unwrap_err().status(),
        Some(reqwest::StatusCode::NOT_FOUND)
    );
    Client::upsert(&url, 1, 10, 2, &payload).await.unwrap();
    assert_eq!(
        Client::delete_partition_key(&url, 1, 10).await.unwrap(),
        json!({"deleted": 1})
    );
    assert!(
        Client::get_partition_key(&url, 1, 10)
            .await
            .unwrap()
            .is_empty()
    );
    // Whole-table cleanup includes both ends of the ring and preserves other tables.
    for key in [i64::MIN, 0, i64::MAX] {
        Client::upsert(&url, 7, key, 1, &payload).await.unwrap();
    }
    Client::upsert(&url, 8, i64::MAX, 1, &payload)
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
        Client::upsert(&url, table, key, secondary, &payload)
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
