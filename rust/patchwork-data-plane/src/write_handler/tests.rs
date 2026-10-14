use super::*;
use crate::data;
use kameo::actor::Spawn;
use sea_orm::{ConnectionTrait, Database, DatabaseConnection, EntityTrait};
use serde_json::json;

async fn setup() -> (tempfile::TempDir, WriteHandler, DatabaseConnection) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("shard.sqlite");
    let shard = WriteShard::spawn(
        WriteShard::new(path.to_str().unwrap().into())
            .await
            .unwrap(),
    );
    let writer = ReplicaWriter::spawn(ReplicaWriter::default());
    let handler = WriteHandler::new(shard, writer, &dir.path().join("wal")).unwrap();
    let db = Database::connect(format!("sqlite://{}", path.display()))
        .await
        .unwrap();
    (dir, handler, db)
}

fn record(table_id: i64, partition_key: i64, timestamp: i64) -> Record {
    Record {
        table_id,
        partition_key,
        secondary_key: 1,
        data: json!({"value": timestamp}),
        timestamp,
        deleted: false,
    }
}

async fn stop(handler: WriteHandler, db: DatabaseConnection) {
    handler.replica_writer.stop_gracefully().await.unwrap();
    handler.replica_writer.wait_for_shutdown().await;
    handler.shard.stop_gracefully().await.unwrap();
    handler.shard.wait_for_shutdown().await;
    db.close().await.unwrap();
}

#[tokio::test]
async fn mixed_mutations_wait_for_flush_and_keep_queue_order() {
    let (_dir, mut handler, db) = setup().await;
    handler.enqueue_record(&record(1, 10, 1)).unwrap();
    handler.delete_partition_key(1, 10).await.unwrap();
    handler.enqueue_record(&record(1, 11, 2)).unwrap();
    handler.delete_range(1, 12, 11).await.unwrap();
    handler.enqueue_record(&record(1, 12, 3)).unwrap();
    handler.delete_table(1).await.unwrap();
    handler.enqueue_record(&record(2, 10, 4)).unwrap();
    handler.delete(2, 10, 1, 4).await.unwrap();
    handler.enqueue_record(&record(2, 10, 3)).unwrap();
    handler.enqueue_record(&record(3, 12, 5)).unwrap();

    assert!(data::Entity::find().all(&db).await.unwrap().is_empty());
    assert_eq!(handler.queue.len(), 10);
    assert_eq!(handler.wal.iter().unwrap().count(), 10);
    handler.flush_to_shard().await.unwrap();
    assert!(handler.queue.is_empty());
    let rows = data::Entity::find().all(&db).await.unwrap();
    assert_eq!(rows.len(), 2);
    let deleted = rows.iter().find(|row| row.table_id == 2).unwrap();
    assert!(deleted.deleted);
    assert_eq!(deleted.timestamp, 4);
    assert_eq!(deleted.data, serde_json::Value::Null);
    assert_eq!(
        rows.iter()
            .find(|row| row.table_id == 3)
            .unwrap()
            .partition_key,
        12
    );
    handler.flush_to_shard().await.unwrap();
    stop(handler, db).await;
}

#[tokio::test]
async fn failed_batch_rolls_back_and_keeps_mutations_for_retry() {
    let (_dir, mut handler, db) = setup().await;
    db.execute_unprepared(
        "CREATE TRIGGER reject_second BEFORE INSERT ON data
         WHEN NEW.partition_key = 2 BEGIN SELECT RAISE(ABORT, 'reject second'); END",
    )
    .await
    .unwrap();
    for key in [1, 2] {
        handler.enqueue_record(&record(1, key, 1)).unwrap();
    }
    let committed = handler.committed_lsn;
    assert!(handler.current_lsn > committed);
    assert!(handler.flush_to_shard().await.is_err());
    assert_eq!(handler.committed_lsn, committed);
    handler.cleanup_wal().unwrap();
    assert_eq!(handler.wal.iter().unwrap().count(), 2);
    assert_eq!(handler.queue.len(), 2);
    assert!(data::Entity::find().all(&db).await.unwrap().is_empty());
    db.execute_unprepared("DROP TRIGGER reject_second")
        .await
        .unwrap();
    handler.flush_to_shard().await.unwrap();
    assert_eq!(handler.committed_lsn, handler.current_lsn);
    assert!(handler.queue.is_empty());
    assert_eq!(data::Entity::find().all(&db).await.unwrap().len(), 2);
    assert!(handler.enqueue_record(&record(1, 3, -1)).is_err());
    assert!(handler.queue.is_empty());
    assert_eq!(handler.wal.iter().unwrap().count(), 2);
    stop(handler, db).await;
}

#[tokio::test]
async fn actor_flushes_periodically_and_on_shutdown() {
    let (_dir, handler, db) = setup().await;
    let shard = handler.shard.clone();
    let writer = handler.replica_writer.clone();
    let actor = WriteHandler::spawn(handler);
    actor
        .ask(WriteRecord {
            record: record(1, 1, 1),
            replicas: vec![],
            consistent: false,
        })
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(3), async {
        while data::Entity::find().all(&db).await.unwrap().is_empty() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    actor
        .ask(WriteRecord {
            record: record(1, 2, 2),
            replicas: vec![],
            consistent: false,
        })
        .await
        .unwrap();
    actor.stop_gracefully().await.unwrap();
    actor
        .wait_for_shutdown_with_result(|result| {
            result.unwrap();
        })
        .await;
    assert_eq!(data::Entity::find().all(&db).await.unwrap().len(), 2);
    writer.stop_gracefully().await.unwrap();
    writer.wait_for_shutdown().await;
    shard.stop_gracefully().await.unwrap();
    shard.wait_for_shutdown().await;
    db.close().await.unwrap();
}

#[tokio::test]
async fn cleanup_keeps_uncommitted_commands_and_startup_replays_them_in_order() {
    let (dir, mut handler, db) = setup().await;
    handler.enqueue_record(&record(99, 1, 1)).unwrap();
    handler.flush_to_shard().await.unwrap();
    let committed = handler.committed_lsn;

    handler.enqueue_record(&record(1, 10, 1)).unwrap();
    handler.delete_partition_key(1, 10).await.unwrap();
    handler.enqueue_record(&record(1, 11, 2)).unwrap();
    handler.delete_range(1, 12, 11).await.unwrap();
    handler.enqueue_record(&record(2, 10, 3)).unwrap();
    handler.delete_table(2).await.unwrap();
    handler.enqueue_record(&record(3, 10, 4)).unwrap();
    handler.delete(3, 10, 1, 5).await.unwrap();
    handler.enqueue_record(&record(4, 12, 6)).unwrap();
    handler.cleanup_wal().unwrap();
    let entries = handler
        .wal
        .iter()
        .unwrap()
        .collect::<std::result::Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(entries.len(), 9);
    assert_eq!(entries[0].lsn(), committed);

    let shard = handler.shard.clone();
    let writer = handler.replica_writer.clone();
    drop(handler); // Simulate stopping before a SQLite flush.
    let handler =
        WriteHandler::new(shard.clone(), writer.clone(), &dir.path().join("wal")).unwrap();
    assert_eq!(handler.queue.len(), 9);
    assert_eq!(handler.committed_lsn, committed);
    let wal_end = handler.current_lsn;
    let actor = WriteHandler::spawn(handler);
    actor
        .wait_for_startup_with_result(|result| {
            result.unwrap();
        })
        .await;

    let rows = data::Entity::find().all(&db).await.unwrap();
    assert_eq!(rows.len(), 3);
    assert!(rows.iter().any(|row| row.table_id == 99));
    let deleted = rows.iter().find(|row| row.table_id == 3).unwrap();
    assert!(deleted.deleted);
    assert_eq!(deleted.timestamp, 5);
    assert_eq!(deleted.data, serde_json::Value::Null);
    assert_eq!(
        rows.iter().find(|row| row.table_id == 4).unwrap().data,
        json!({"value": 6})
    );

    actor.stop_gracefully().await.unwrap();
    actor
        .wait_for_shutdown_with_result(|result| {
            result.unwrap();
        })
        .await;
    // Replay must not append the recovered commands to the WAL again.
    let handler = WriteHandler::new(shard, writer, &dir.path().join("wal")).unwrap();
    assert_eq!(handler.current_lsn, wal_end);
    stop(handler, db).await;
}

#[tokio::test]
async fn timer_cleans_committed_wal_and_restart_continues_from_its_end() {
    let (dir, handler, db) = setup().await;
    let shard = handler.shard.clone();
    let writer = handler.replica_writer.clone();
    let actor = WriteHandler::spawn(handler);
    actor
        .ask(WriteRecord {
            record: record(1, 1, 1),
            replicas: vec![],
            consistent: false,
        })
        .await
        .unwrap();
    let head_path = dir.path().join("wal/head");
    tokio::time::timeout(Duration::from_secs(4), async {
        loop {
            if let Ok(bytes) = std::fs::read(&head_path) {
                if bytes.len() >= 8 && u64::from_le_bytes(bytes[..8].try_into().unwrap()) > 0 {
                    break;
                }
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("WAL cleanup timer did not advance the head");
    actor.stop_gracefully().await.unwrap();
    actor
        .wait_for_shutdown_with_result(|result| {
            result.unwrap();
        })
        .await;
    let mut handler = WriteHandler::new(shard, writer, &dir.path().join("wal")).unwrap();
    assert!(handler.queue.is_empty());
    assert!(handler.current_lsn.get() > 0);
    assert_eq!(handler.committed_lsn, handler.current_lsn);
    let previous_end = handler.current_lsn;
    handler.enqueue_record(&record(1, 2, 2)).unwrap();
    assert!(handler.current_lsn > previous_end);
    assert_eq!(handler.committed_lsn, previous_end);
    handler.flush_to_shard().await.unwrap();
    handler.cleanup_wal().unwrap();
    assert!(handler.wal.iter().unwrap().next().is_none());
    assert_eq!(data::Entity::find().all(&db).await.unwrap().len(), 2);
    stop(handler, db).await;
}
