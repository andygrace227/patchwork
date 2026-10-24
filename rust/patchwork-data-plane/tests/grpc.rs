mod support;

use patchwork_common::{
    dataplane_client::Client,
    grpc::proto::{self, data_plane_client::DataPlaneClient},
};
use support::Node;
use tonic::Code;

#[tokio::test]
async fn grpc_validates_writes_and_carries_large_payloads_and_exact_telemetry() {
    let client = Client::new();
    let dir = tempfile::tempdir().unwrap();
    let node = Node::start(
        &dir.path().join("shard.sqlite"),
        &dir.path().join("wal"),
        None,
    )
    .await;
    let url = node.url.clone();
    let mut raw = DataPlaneClient::connect(url.clone()).await.unwrap();
    let mut request = proto::WriteRequest {
        partition_id: None,
        table_id: 1,
        partition_key: 10,
        secondary_key: 42,
        data_bin: b"null".to_vec(),
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
    assert_eq!(
        client.get(&url, 1, 10, 42).await.unwrap_err().code(),
        Code::NotFound
    );
    request.data_bin = b"null".to_vec();
    raw.write_record(request).await.unwrap();
    node.flush().await;
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
    let payload = vec![0xff; 5 * 1024 * 1024];
    client
        .upsert(&url, 2, 10, 1, &payload, i64::MAX)
        .await
        .unwrap();
    node.flush().await;
    let record = client.get(&url, 2, 10, 1).await.unwrap();
    assert_eq!(record.data, payload);
    assert_eq!(record.timestamp, i64::MAX);
    assert_eq!(
        client.get_range(&url, 2, 11, 10).await.unwrap(),
        vec![record]
    );
    for key in [i64::MAX - 2, i64::MAX - 1, i64::MAX] {
        client
            .write_partition_record(
                &url,
                &patchwork_common::dataplane_client::Record {
                    table_id: 3,
                    partition_id: Some("partition-a".into()),
                    partition_key: key,
                    secondary_key: 1,
                    data: b"true".to_vec(),
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
    let telemetry = client.get_telemetry_for_table(&url, 3).await.unwrap();
    let stats = &telemetry["partition-a"].statistics;
    assert_eq!(stats.write_position_sum, i64::MAX as i128 * 3 - 3);
    assert_eq!(stats.average_write_position, Some(i64::MAX - 1));
    assert_eq!(stats.median_write_position, Some(i64::MAX - 1));

    node.stop().await;
}

#[tokio::test]
async fn consistent_writes_to_each_other_do_not_block_replica_admission() {
    use patchwork_common::dataplane_client::Record;
    use std::{sync::Arc, time::Duration};
    use tokio::sync::Barrier;

    let dir = tempfile::tempdir().unwrap();
    let gate = Arc::new(Barrier::new(2));
    let mut nodes = Vec::new();
    let mut urls = Vec::new();
    for index in 0..2 {
        let node = Node::start(
            &dir.path().join(format!("{index}.sqlite")),
            &dir.path().join(format!("{index}.wal")),
            None,
        )
        .await;
        urls.push(node.url.clone());
        nodes.push(node);
    }
    let write = |index: usize| {
        let client = Client::new();
        let url = urls[index].clone();
        let remote = urls[1 - index].clone();
        let gate = gate.clone();
        async move {
            gate.wait().await;
            client
                .write_partition_record(
                    &url,
                    &Record {
                        table_id: 1,
                        partition_id: None,
                        partition_key: index as i64,
                        secondary_key: 1,
                        timestamp: 1,
                        deleted: false,
                        data: vec![index as u8],
                    },
                    &[remote],
                    true,
                )
                .await
        }
    };
    let (first, second) = tokio::time::timeout(Duration::from_secs(3), async {
        tokio::join!(write(0), write(1))
    })
    .await
    .expect("consistent writes stalled each other");
    first.unwrap();
    second.unwrap();
    for node in nodes {
        node.stop().await;
    }
}
