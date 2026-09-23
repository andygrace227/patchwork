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
    assert_eq!(record["data"], payload);
    assert_eq!(record["version"], 1);
    Client::upsert(&url, 1, 10, -42, &json!({"updated": true}))
        .await
        .unwrap();
    assert_eq!(Client::get(&url, 1, 10, -42).await.unwrap()["version"], 2);
    assert_eq!(
        Client::get_partition(&url, 1, 10)
            .await
            .unwrap()
            .as_array()
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        Client::get_range(&url, 1, 11, 9)
            .await
            .unwrap()
            .as_array()
            .unwrap()
            .len(),
        1
    );
    assert_eq!(Client::get_range(&url, 1, 10, 9).await.unwrap(), json!([]));
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
        Client::delete_partition(&url, 1, 10).await.unwrap(),
        json!({"deleted": 1})
    );
    assert_eq!(Client::get_partition(&url, 1, 10).await.unwrap(), json!([]));
    server.abort();
    let _ = server.await;
    shard.stop_gracefully().await.unwrap();
    shard.wait_for_shutdown().await;
}
