use kameo::actor::Spawn;
use patchwork_common::{
    dataplane_client::Client,
    grpc::proto::{self, data_plane_client::DataPlaneClient},
};
use patchwork_data_plane::{grpc, replica_writer::ReplicaWriter, write_shard::WriteShard};
use serde_json::json;
use tonic::Code;

#[tokio::test]
async fn grpc_validates_writes_and_carries_large_payloads_and_exact_telemetry() {
    let client = Client::new();
    let dir = tempfile::tempdir().unwrap();
    let shard = patchwork_data_plane::initialize::shard(
        dir.path().join("shard.sqlite").to_str().unwrap().into(),
    )
    .await
    .unwrap();
    let shard_readers = shard.read_pool(2).unwrap();
    let shard = WriteShard::spawn(shard);
    let writer = ReplicaWriter::spawn(ReplicaWriter::default());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    // This listener has no HTTP/JSON routes: every shared-client call must be gRPC.
    let app = grpc::router(
        shard.clone(),
        shard_readers.clone(),
        writer.clone(),
        &dir.path().join("wal"),
    )
    .unwrap();
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
        client.get(&url, 1, 10, 42).await.unwrap_err().code(),
        Code::NotFound
    );
    request.data_json = b"null".to_vec();
    raw.write_record(request).await.unwrap();
    assert_eq!(client.get(&url, 1, 10, 42).await.unwrap().timestamp, 0);
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
    assert_eq!(client.get(&url, 1, 10, 42).await.unwrap().timestamp, 0);

    // Exceed tonic's default 4 MiB decoding limit in both directions.
    let payload = json!("x".repeat(5 * 1024 * 1024));
    client
        .upsert(&url, 2, 10, 1, &payload, i64::MAX)
        .await
        .unwrap();
    let record = client.get(&url, 2, 10, 1).await.unwrap();
    assert_eq!(record.data, payload);
    assert_eq!(record.timestamp, i64::MAX);
    assert_eq!(
        client.get_range(&url, 2, 11, 10).await.unwrap(),
        vec![record]
    );
    for key in [i64::MAX - 2, i64::MAX - 1, i64::MAX] {
        client
            .upsert(&url, 3, i64::MIN, key, &json!(true), 1)
            .await
            .unwrap();
    }
    let telemetry = client.get_telemetry_for_table(&url, 3).await.unwrap();
    let stats = &telemetry[&i64::MIN].statistics;
    assert_eq!(stats.write_position_sum, i64::MAX as i128 * 3 - 3);
    assert_eq!(stats.average_write_position, Some(i64::MAX - 1));
    assert_eq!(stats.median_write_position, Some(i64::MAX - 1));

    server.abort();
    let _ = server.await;
    writer.stop_gracefully().await.unwrap();
    writer.wait_for_shutdown().await;
    shard_readers.stop_gracefully().await.unwrap();
    shard_readers.wait_for_shutdown().await;
    shard.stop_gracefully().await.unwrap();
    shard.wait_for_shutdown().await;
}

#[tokio::test]
async fn consistent_writes_to_each_other_do_not_block_replica_admission() {
    use patchwork_common::dataplane_client::Record;
    use patchwork_data_plane::write_handler::{FlushToShard, WriteHandler, WriteRecord};
    use std::{sync::Arc, time::Duration};
    use tokio::sync::Barrier;

    let dir = tempfile::tempdir().unwrap();
    let gate = Arc::new(Barrier::new(2));
    let mut nodes = Vec::new();
    let mut urls = Vec::new();
    for index in 0..2 {
        let shard = patchwork_data_plane::initialize::shard(
            dir.path()
                .join(format!("{index}.sqlite"))
                .to_str()
                .unwrap()
                .into(),
        )
        .await
        .unwrap();
        let readers = shard.read_pool(1).unwrap();
        let shard = WriteShard::spawn(shard);
        let writer = ReplicaWriter::spawn(ReplicaWriter::default());
        let handler = WriteHandler::spawn(
            WriteHandler::new(
                shard.clone(),
                writer.clone(),
                &dir.path().join(format!("{index}.wal")),
            )
            .unwrap(),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        urls.push(format!("http://{}", listener.local_addr().unwrap()));
        let gate = gate.clone();
        let app = grpc::router_with_handler(shard.clone(), readers.clone(), handler.clone()).layer(
            axum::middleware::from_fn(
                move |request: axum::extract::Request, next: axum::middleware::Next| {
                    let gate = gate.clone();
                    async move {
                        gate.wait().await;
                        next.run(request).await
                    }
                },
            ),
        );
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        nodes.push((handler, writer, readers, shard, server));
    }
    let write = |index: usize| {
        nodes[index].0.ask(WriteRecord {
            record: Record {
                table_id: 1,
                partition_key: index as i64,
                secondary_key: 1,
                timestamp: 1,
                deleted: false,
                data: json!(index),
            },
            replicas: vec![urls[1 - index].clone()],
            consistent: true,
        })
    };
    let (first, second) = tokio::time::timeout(Duration::from_secs(3), async {
        tokio::join!(write(0), write(1))
    })
    .await
    .expect("consistent writes stalled each other");
    first.unwrap();
    second.unwrap();
    for (handler, writer, readers, shard, server) in nodes {
        handler.ask(FlushToShard {}).await.unwrap();
        server.abort();
        let _ = server.await;
        handler.stop_gracefully().await.unwrap();
        handler
            .wait_for_shutdown_with_result(|result| {
                result.unwrap();
            })
            .await;
        writer.stop_gracefully().await.unwrap();
        writer.wait_for_shutdown().await;
        readers.stop_gracefully().await.unwrap();
        readers.wait_for_shutdown().await;
        shard.stop_gracefully().await.unwrap();
        shard.wait_for_shutdown().await;
    }
}
