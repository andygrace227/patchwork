mod support;

use patchwork_common::dataplane_client::Client;
use support::Node;
use tower::ServiceExt;

#[tokio::test]
async fn grpc_and_sqlite_preserve_arbitrary_bytes_and_tombstones() {
    let dir = tempfile::tempdir().unwrap();
    let node = Node::start(
        &dir.path().join("shard.sqlite"),
        &dir.path().join("wal"),
        None,
    )
    .await;
    let client = Client::new();
    // Ciphertext does not need to be valid UTF-8 or JSON.
    let payload = vec![0, 255, 128, 1, 0, 13, 10];
    client
        .upsert(&node.url, 1, 10, 42, &payload, 1)
        .await
        .unwrap();
    node.flush().await;
    assert_eq!(
        client.get(&node.url, 1, 10, 42).await.unwrap().data,
        payload
    );
    assert_eq!(
        client.get_range(&node.url, 1, 11, 10).await.unwrap()[0].data,
        payload
    );
    assert_eq!(
        client.get_range_size(&node.url, 1, 11, 10).await.unwrap()["size_bytes"],
        payload.len()
    );
    client.delete(&node.url, 1, 10, 42, 2).await.unwrap();
    node.flush().await;
    assert_eq!(
        client.get(&node.url, 1, 10, 42).await.unwrap_err().code(),
        tonic::Code::NotFound
    );
    let tombstone = client
        .get_including_deleted(&node.url, 1, 10, 42)
        .await
        .unwrap();
    assert!(tombstone.deleted);
    assert!(tombstone.data.is_empty());
    let response = node
        .app
        .clone()
        .oneshot(
            tonic::codegen::http::Request::builder()
                .uri("/shard/size")
                .body(tonic::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.headers()["grpc-status"], "12");
    node.stop().await;
}
