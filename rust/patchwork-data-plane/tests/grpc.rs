use kameo::actor::Spawn;
use patchwork_common::{
    dataplane_client::Client,
    grpc::proto::{self, data_plane_client::DataPlaneClient},
};
use patchwork_data_plane::{
    grpc, shard_actor::ShardActor, subordinate_shard_writer::SubordinateShardWriter,
};
use serde_json::json;
use tonic::Code;

#[tokio::test]
async fn grpc_validates_writes_and_carries_large_payloads_and_exact_telemetry() {
    let dir = tempfile::tempdir().unwrap();
    let shard = ShardActor::spawn(
        ShardActor::new(dir.path().join("shard.sqlite").to_str().unwrap().into())
            .await
            .unwrap(),
    );
    let writer = SubordinateShardWriter::spawn(SubordinateShardWriter);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    // This listener has no HTTP/JSON routes: every shared-client call must be gRPC.
    let app = grpc::router(shard.clone(), writer.clone());
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let mut raw = DataPlaneClient::connect(url.clone()).await.unwrap();
    let mut request = proto::WriteRequest {
        table_id: 1,
        partition_key: 10,
        secondary_key: 42,
        data_json: b"null".to_vec(),
        ..Default::default()
    };
    for timestamp in [None, Some(-1)] {
        request.timestamp = timestamp;
        assert_eq!(
            raw.write_record(request.clone()).await.unwrap_err().code(),
            Code::InvalidArgument
        );
    }
    request.timestamp = Some(0);
    request.data_json = b"invalid json".to_vec();
    assert_eq!(
        raw.write_record(request.clone()).await.unwrap_err().code(),
        Code::InvalidArgument
    );
    assert_eq!(
        Client::get(&url, 1, 10, 42).await.unwrap_err().code(),
        Code::NotFound
    );
    request.data_json = b"null".to_vec();
    raw.write_record(request).await.unwrap();
    assert_eq!(Client::get(&url, 1, 10, 42).await.unwrap().timestamp, 0);
    assert_eq!(
        raw.delete_record(proto::DeleteRequest {
            table_id: 1,
            partition_key: 10,
            secondary_key: 42,
            timestamp: None,
        })
        .await
        .unwrap_err()
        .code(),
        Code::InvalidArgument
    );
    assert_eq!(Client::get(&url, 1, 10, 42).await.unwrap().timestamp, 0);

    // Exceed tonic's default 4 MiB decoding limit in both directions.
    let payload = json!("x".repeat(5 * 1024 * 1024));
    Client::upsert(&url, 2, 10, 1, &payload, i64::MAX)
        .await
        .unwrap();
    let record = Client::get(&url, 2, 10, 1).await.unwrap();
    assert_eq!(record.data, payload);
    assert_eq!(record.timestamp, i64::MAX);
    assert_eq!(
        Client::get_range(&url, 2, 11, 10).await.unwrap(),
        vec![record]
    );
    for key in [i64::MAX - 2, i64::MAX - 1, i64::MAX] {
        Client::upsert(&url, 3, i64::MIN, key, &json!(true), 1)
            .await
            .unwrap();
    }
    let telemetry = Client::get_telemetry_for_table(&url, 3).await.unwrap();
    let stats = &telemetry[&i64::MIN].statistics;
    assert_eq!(stats.write_position_sum, i64::MAX as i128 * 3 - 3);
    assert_eq!(stats.average_write_position, Some(i64::MAX - 1));
    assert_eq!(stats.median_write_position, Some(i64::MAX - 1));

    server.abort();
    let _ = server.await;
    writer.stop_gracefully().await.unwrap();
    writer.wait_for_shutdown().await;
    shard.stop_gracefully().await.unwrap();
    shard.wait_for_shutdown().await;
}
