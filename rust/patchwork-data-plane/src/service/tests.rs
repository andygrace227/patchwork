use super::*;
use sea_orm::ConnectionTrait;
use serde_json::json;
use std::time::Duration;

#[tokio::test]
async fn point_reads_preserve_keys_payload_and_tombstone_filtering() {
    let dir = tempfile::tempdir().unwrap();
    let mut shard = crate::utils::shard(dir.path().join("shard.sqlite").display().to_string())
        .await
        .unwrap();
    let payload = serde_json::to_vec(
        &json!({"text": "quotes: ' and unicode: 🔧", "nested": [true, null, 42]}),
    )
    .unwrap();
    for (table_id, partition_key, secondary_key) in
        [(1, -10, 42), (2, -10, 42), (1, -9, 42), (1, -10, 43)]
    {
        shard
            .upsert(table_id, partition_key, secondary_key, payload.clone(), 123)
            .await
            .unwrap();
    }
    let stats = AccessStatsActor::spawn(
        AccessStatsActor::new(dir.path().join("stats.sqlite"))
            .await
            .unwrap(),
    );
    let service = Service::new(
        &dir.path().join("shard.sqlite"),
        &dir.path().join("wal"),
        stats.clone(),
        2,
    )
    .await
    .unwrap();
    for include_deleted in [false, true] {
        let record = service
            .get_record(Request::new(proto::RecordRequest {
                table_id: 1,
                partition_key: -10,
                secondary_key: 42,
                include_deleted,
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(
            record,
            proto::Record {
                table_id: 1,
                partition_id: None,
                partition_key: -10,
                secondary_key: 42,
                data_bin: payload.clone(),
                timestamp: 123,
                deleted: false,
            }
        );
        assert!(
            service
                .get_record(Request::new(proto::RecordRequest {
                    table_id: 1,
                    partition_key: -10,
                    secondary_key: 99,
                    include_deleted,
                }))
                .await
                .unwrap_err()
                .code()
                == tonic::Code::NotFound
        );
    }
    shard.delete(1, -10, 42, 124).await.unwrap();
    assert!(
        service
            .get_record(Request::new(proto::RecordRequest {
                table_id: 1,
                partition_key: -10,
                secondary_key: 42,
                include_deleted: false,
            }))
            .await
            .unwrap_err()
            .code()
            == tonic::Code::NotFound
    );
    let tombstone = service
        .get_record(Request::new(proto::RecordRequest {
            table_id: 1,
            partition_key: -10,
            secondary_key: 42,
            include_deleted: true,
        }))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(tombstone.timestamp, 124);
    assert!(tombstone.deleted);
    assert!(tombstone.data_bin.is_empty());
    service.shutdown().await.unwrap();
    stats.stop_gracefully().await.unwrap();
    stats.wait_for_shutdown().await;
}

#[tokio::test]
async fn shared_read_pool_allows_reads_while_one_connection_is_held() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("shard.sqlite");
    let stats = AccessStatsActor::spawn(
        AccessStatsActor::new(dir.path().join("stats.sqlite"))
            .await
            .unwrap(),
    );
    assert!(
        Service::new(&path, &dir.path().join("wal"), stats.clone(), 0)
            .await
            .is_err()
    );
    let service = Service::new(&path, &dir.path().join("wal"), stats.clone(), 2)
        .await
        .unwrap();
    let held = service
        .db
        .get_sqlite_connection_pool()
        .acquire()
        .await
        .unwrap();
    service
        .write_shard
        .ask(crate::write_shard::Upsert {
            table_id: 1,
            partition_key: 10,
            secondary_key: 42,
            data: b"new".to_vec(),
            timestamp: 2,
        })
        .await
        .unwrap();
    let record = tokio::time::timeout(
        Duration::from_secs(2),
        service.get_record(Request::new(proto::RecordRequest {
            table_id: 1,
            partition_key: 10,
            secondary_key: 42,
            include_deleted: false,
        })),
    )
    .await
    .unwrap()
    .unwrap()
    .into_inner();
    assert_eq!(record.timestamp, 2);
    assert!(
        service
            .db
            .execute_unprepared("DELETE FROM data")
            .await
            .unwrap_err()
            .to_string()
            .contains("readonly")
    );
    drop(held);
    service.shutdown().await.unwrap();
    stats.stop_gracefully().await.unwrap();
    stats.wait_for_shutdown().await;
}
