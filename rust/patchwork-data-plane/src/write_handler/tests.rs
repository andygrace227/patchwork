use super::*;
use crate::data;
use kameo::{actor::Spawn, message::Message};
use sea_orm::{ConnectionTrait, Database, DatabaseConnection, EntityTrait};
use serde_json::json;

struct PauseShard {
    entered: oneshot::Sender<()>,
    release: oneshot::Receiver<()>,
}

impl Message<PauseShard> for WriteShard {
    type Reply = ();

    async fn handle(&mut self, msg: PauseShard, _: &mut Context<Self, ()>) {
        msg.entered.send(()).unwrap();
        msg.release.await.unwrap();
    }
}

struct BatchState;

impl Message<BatchState> for WriteHandler {
    type Reply = (Lsn, Lsn, Option<Lsn>, usize);

    async fn handle(&mut self, _: BatchState, _: &mut Context<Self, Self::Reply>) -> Self::Reply {
        // Drive flushes explicitly so assertions do not depend on the timer.
        if let Some(timer) = self.timer.take() {
            timer.abort();
        }
        (
            self.current_lsn,
            self.committed_lsn,
            self.in_flight.as_ref().map(|batch| batch.end_lsn),
            self.queue.len(),
        )
    }
}

struct WaitForBatch;

struct QueueWrites(usize);

impl Message<QueueWrites> for WriteHandler {
    type Reply = Result<()>;

    async fn handle(
        &mut self,
        msg: QueueWrites,
        _: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        for i in 0..msg.0 {
            self.enqueue_record(&record(1, 1, i as i64 + 1))?;
        }
        Ok(())
    }
}

impl Message<WaitForBatch> for WriteHandler {
    type Reply = Result<()>;

    async fn handle(&mut self, _: WaitForBatch, _: &mut Context<Self, Self::Reply>) -> Self::Reply {
        self.wait_for_batch().await
    }
}

async fn setup() -> (tempfile::TempDir, WriteHandler, DatabaseConnection) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("shard.sqlite");
    let shard = WriteShard::spawn(
        crate::utils::shard(path.to_str().unwrap().into())
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
        partition_id: None,
        partition_key,
        secondary_key: 1,
        data: serde_json::to_vec(&json!({"value": timestamp})).unwrap(),
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
    handler.drain_to_shard().await.unwrap();
    assert!(handler.queue.is_empty());
    let rows = data::Entity::find().all(&db).await.unwrap();
    assert_eq!(rows.len(), 2);
    let deleted = rows.iter().find(|row| row.table_id == 2).unwrap();
    assert!(deleted.deleted);
    assert_eq!(deleted.timestamp, 4);
    assert!(deleted.data.is_empty());
    assert_eq!(
        rows.iter()
            .find(|row| row.table_id == 3)
            .unwrap()
            .partition_key,
        12
    );
    handler.drain_to_shard().await.unwrap();
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
    assert!(handler.drain_to_shard().await.is_err());
    assert_eq!(handler.committed_lsn, committed);
    handler.cleanup_wal().unwrap();
    assert_eq!(handler.wal.iter().unwrap().count(), 2);
    assert_eq!(handler.queue.len(), 2);
    assert!(data::Entity::find().all(&db).await.unwrap().is_empty());
    db.execute_unprepared("DROP TRIGGER reject_second")
        .await
        .unwrap();
    handler.drain_to_shard().await.unwrap();
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
    handler.drain_to_shard().await.unwrap();
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
    assert!(deleted.data.is_empty());
    assert_eq!(
        rows.iter().find(|row| row.table_id == 4).unwrap().data,
        serde_json::to_vec(&json!({"value": 6})).unwrap()
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
    handler.drain_to_shard().await.unwrap();
    handler.cleanup_wal().unwrap();
    assert!(handler.wal.iter().unwrap().next().is_none());
    assert_eq!(data::Entity::find().all(&db).await.unwrap().len(), 2);
    stop(handler, db).await;
}

#[tokio::test]
async fn bounded_batches_keep_pending_wal_entries_replayable() {
    let (dir, handler, db) = setup().await;
    let shard = handler.shard.clone();
    let writer = handler.replica_writer.clone();
    let actor = WriteHandler::spawn(handler);
    let _ = actor.ask(BatchState).await.unwrap();
    actor.ask(QueueWrites(FLUSH_THRESHOLD + 1)).await.unwrap();
    actor.ask(FlushToShard {}).await.unwrap();
    actor.ask(WaitForBatch).await.unwrap();
    let (current, committed, in_flight, pending) = actor.ask(BatchState).await.unwrap();
    assert!(committed < current);
    assert!(in_flight.is_none());
    assert_eq!(pending, 1);
    actor.ask(CleanupWal {}).await.unwrap();

    // Reopen the retained log before the final queued mutation is applied.
    let mut recovered =
        WriteHandler::new(shard.clone(), writer.clone(), &dir.path().join("wal")).unwrap();
    assert_eq!(recovered.queue.len(), 1);
    assert_eq!(recovered.queue.front().unwrap().lsn, committed);
    recovered.drain_to_shard().await.unwrap();
    assert_eq!(recovered.committed_lsn, current);
    assert_eq!(
        data::Entity::find()
            .one(&db)
            .await
            .unwrap()
            .unwrap()
            .timestamp,
        FLUSH_THRESHOLD as i64 + 1
    );
    drop(recovered);
    actor.stop_gracefully().await.unwrap();
    actor.wait_for_shutdown().await;
    stop(
        WriteHandler::new(shard, writer, &dir.path().join("wal")).unwrap(),
        db,
    )
    .await;
}

#[tokio::test]
async fn full_backlog_rejects_writes_and_deletes_before_appending_to_wal() {
    use patchwork_common::grpc::proto::{self, data_plane_client::DataPlaneClient};

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("shard.sqlite");
    let telemetry = crate::telemetry::AccessStatsActor::spawn(
        crate::telemetry::AccessStatsActor::new(dir.path().join("stats.sqlite"))
            .await
            .unwrap(),
    );
    let service =
        crate::service::Service::new(&path, &dir.path().join("wal"), telemetry.clone(), 1)
            .await
            .unwrap();
    let shard = service.write_shard.clone();
    let actor = service.handler.clone();
    let db = Database::connect(format!("sqlite://{}", path.display()))
        .await
        .unwrap();
    let _ = actor.ask(BatchState).await.unwrap();
    let (entered, ready) = oneshot::channel();
    let (release, wait) = oneshot::channel();
    shard
        .tell(PauseShard {
            entered,
            release: wait,
        })
        .await
        .unwrap();
    ready.await.unwrap();
    actor.ask(QueueWrites(FLUSH_THRESHOLD)).await.unwrap();
    actor.ask(FlushToShard {}).await.unwrap();
    actor
        .ask(QueueWrites(MAX_WRITE_BACKLOG - FLUSH_THRESHOLD))
        .await
        .unwrap();
    let before = actor.ask(BatchState).await.unwrap().0;
    let error = actor
        .ask(WriteRecord {
            record: record(1, 1, 99999),
            replicas: vec![],
            consistent: false,
        })
        .await
        .unwrap_err();
    assert_eq!(
        error.status().unwrap().code(),
        tonic::Code::ResourceExhausted
    );
    let error = actor.ask(DeleteTable { table_id: 1 }).await.unwrap_err();
    assert_eq!(
        error.status().unwrap().code(),
        tonic::Code::ResourceExhausted
    );
    let app = service.server();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .serve_with_incoming(
                app,
                tokio_stream::wrappers::TcpListenerStream::new(listener),
            )
            .await
            .unwrap()
    });
    let mut client = DataPlaneClient::connect(url).await.unwrap();
    let error = client
        .write_record(proto::WriteRequest {
            partition_id: None,
            table_id: 1,
            partition_key: 1,
            secondary_key: 1,
            timestamp: Some(99999),
            data_bin: b"{}".to_vec(),
            ..Default::default()
        })
        .await
        .unwrap_err();
    assert_eq!(error.code(), tonic::Code::ResourceExhausted);
    assert_eq!(error.message(), "write queue is full; retry later");
    let error = client
        .delete_record(proto::DeleteRequest {
            table_id: 1,
            partition_key: 1,
            secondary_key: 1,
            timestamp: Some(99999),
        })
        .await
        .unwrap_err();
    assert_eq!(error.code(), tonic::Code::ResourceExhausted);
    assert_eq!(error.message(), "write queue is full; retry later");
    server.abort();
    let _ = server.await;
    assert_eq!(actor.ask(BatchState).await.unwrap().0, before);
    let backlog = actor.ask(GetWriteBacklog {}).await.unwrap();
    assert_eq!(backlog.total, MAX_WRITE_BACKLOG);
    assert_eq!(backlog.in_flight, FLUSH_THRESHOLD);
    release.send(()).unwrap();
    service.shutdown().await.unwrap();
    telemetry.stop_gracefully().await.unwrap();
    telemetry.wait_for_shutdown().await;
    db.close().await.unwrap();
}

#[tokio::test]
async fn queue_threshold_flushes_without_the_timer_or_waiting_for_sqlite() {
    let (dir, handler, db) = setup().await;
    let shard = handler.shard.clone();
    let writer = handler.replica_writer.clone();
    let actor = WriteHandler::spawn(handler);
    let _ = actor.ask(BatchState).await.unwrap(); // Disable the timer.
    let (entered, ready) = oneshot::channel();
    let (release, wait) = oneshot::channel();
    shard
        .tell(PauseShard {
            entered,
            release: wait,
        })
        .await
        .unwrap();
    ready.await.unwrap();

    for timestamp in 1..FLUSH_THRESHOLD as i64 {
        actor
            .ask(WriteRecord {
                record: record(1, 1, timestamp),
                replicas: vec![],
                consistent: false,
            })
            .await
            .unwrap();
    }
    let backlog = actor.ask(GetWriteBacklog {}).await.unwrap();
    assert_eq!(backlog.queued, FLUSH_THRESHOLD - 1);
    assert_eq!(backlog.in_flight, 0);
    tokio::time::timeout(
        Duration::from_secs(1),
        actor.ask(WriteRecord {
            record: record(1, 1, FLUSH_THRESHOLD as i64),
            replicas: vec![],
            consistent: false,
        }),
    )
    .await
    .expect("Threshold flush waited for SQLite")
    .unwrap();
    let backlog = actor.ask(GetWriteBacklog {}).await.unwrap();
    assert_eq!(backlog.queued, 0);
    assert_eq!(backlog.in_flight, FLUSH_THRESHOLD);

    actor
        .ask(WriteRecord {
            record: record(1, 1, FLUSH_THRESHOLD as i64 + 1),
            replicas: vec![],
            consistent: false,
        })
        .await
        .unwrap();
    let backlog = actor.ask(GetWriteBacklog {}).await.unwrap();
    assert_eq!(backlog.queued, 1);
    assert_eq!(backlog.in_flight, FLUSH_THRESHOLD);
    release.send(()).unwrap();
    actor.ask(WaitForBatch).await.unwrap();
    assert_eq!(
        data::Entity::find()
            .one(&db)
            .await
            .unwrap()
            .unwrap()
            .timestamp,
        FLUSH_THRESHOLD as i64
    );
    actor.stop_gracefully().await.unwrap();
    actor
        .wait_for_shutdown_with_result(|result| {
            result.unwrap();
        })
        .await;
    let handler = WriteHandler::new(shard, writer, &dir.path().join("wal")).unwrap();
    stop(handler, db).await;
}

#[tokio::test]
async fn writes_continue_during_batch_and_shutdown_drains_both_queues() {
    let (dir, handler, db) = setup().await;
    let shard = handler.shard.clone();
    let writer = handler.replica_writer.clone();
    let actor = WriteHandler::spawn(handler);
    let _ = actor.ask(BatchState).await.unwrap();

    let (entered, ready) = oneshot::channel();
    let (release, wait) = oneshot::channel();
    shard
        .tell(PauseShard {
            entered,
            release: wait,
        })
        .await
        .unwrap();
    ready.await.unwrap();
    actor
        .ask(WriteRecord {
            record: record(1, 1, 1),
            replicas: vec![],
            consistent: false,
        })
        .await
        .unwrap();
    let first_end = actor.ask(BatchState).await.unwrap().0;
    tokio::time::timeout(Duration::from_secs(1), actor.ask(FlushToShard {}))
        .await
        .unwrap()
        .unwrap();
    tokio::time::timeout(
        Duration::from_secs(1),
        actor.ask(WriteRecord {
            record: record(1, 2, 2),
            replicas: vec![],
            consistent: false,
        }),
    )
    .await
    .expect("PUT waited for SQLite")
    .unwrap();
    let (current, committed, in_flight, pending) = actor.ask(BatchState).await.unwrap();
    assert!(current > first_end);
    assert_eq!(committed, Lsn::new(0));
    assert_eq!(in_flight, Some(first_end));
    assert_eq!(pending, 1);
    let backlog = tokio::time::timeout(Duration::from_secs(1), actor.ask(GetWriteBacklog {}))
        .await
        .expect("Backlog reporting waited for SQLite")
        .unwrap();
    assert_eq!(
        backlog,
        crate::telemetry::WriteBacklog {
            queued: 1,
            in_flight: 1,
            total: 2
        }
    );
    actor.ask(FlushToShard {}).await.unwrap();
    assert_eq!(actor.ask(BatchState).await.unwrap().2, Some(first_end));
    actor.ask(CleanupWal {}).await.unwrap();
    assert_eq!(
        Wal::open_segmented(dir.path().join("wal"), 16 * 1024 * 1024)
            .unwrap()
            .iter()
            .unwrap()
            .count(),
        2
    );

    release.send(()).unwrap();
    actor.ask(WaitForBatch).await.unwrap();
    let (_, committed, in_flight, pending) = actor.ask(BatchState).await.unwrap();
    assert_eq!(committed, first_end);
    assert!(in_flight.is_none());
    assert_eq!(pending, 1);
    assert_eq!(
        actor.ask(GetWriteBacklog {}).await.unwrap(),
        crate::telemetry::WriteBacklog {
            queued: 1,
            in_flight: 0,
            total: 1
        }
    );
    actor.ask(CleanupWal {}).await.unwrap();
    assert_eq!(
        Wal::open_segmented(dir.path().join("wal"), 16 * 1024 * 1024)
            .unwrap()
            .iter()
            .unwrap()
            .count(),
        1
    );

    // Stop with another batch blocked and a newer write still pending.
    let (entered, ready) = oneshot::channel();
    let (release, wait) = oneshot::channel();
    shard
        .tell(PauseShard {
            entered,
            release: wait,
        })
        .await
        .unwrap();
    ready.await.unwrap();
    actor.ask(FlushToShard {}).await.unwrap();
    actor
        .ask(WriteRecord {
            record: record(1, 3, 3),
            replicas: vec![],
            consistent: false,
        })
        .await
        .unwrap();
    actor.stop_gracefully().await.unwrap();
    release.send(()).unwrap();
    tokio::time::timeout(
        Duration::from_secs(2),
        actor.wait_for_shutdown_with_result(|result| {
            result.unwrap();
        }),
    )
    .await
    .expect("Shutdown waited for a commit message");
    assert_eq!(data::Entity::find().all(&db).await.unwrap().len(), 3);
    let handler = WriteHandler::new(shard, writer, &dir.path().join("wal")).unwrap();
    stop(handler, db).await;
}

#[tokio::test]
async fn failed_in_flight_batch_retries_before_newer_delete() {
    let (_dir, handler, db) = setup().await;
    db.execute_unprepared(
        "CREATE TRIGGER reject_second BEFORE INSERT ON data
         WHEN NEW.partition_key = 2 BEGIN SELECT RAISE(ABORT, 'reject second'); END",
    )
    .await
    .unwrap();
    let shard = handler.shard.clone();
    let writer = handler.replica_writer.clone();
    let actor = WriteHandler::spawn(handler);
    let (_, committed, _, _) = actor.ask(BatchState).await.unwrap();
    let (entered, ready) = oneshot::channel();
    let (release, wait) = oneshot::channel();
    shard
        .tell(PauseShard {
            entered,
            release: wait,
        })
        .await
        .unwrap();
    ready.await.unwrap();
    for key in [1, 2] {
        actor
            .ask(WriteRecord {
                record: record(1, key, 1),
                replicas: vec![],
                consistent: false,
            })
            .await
            .unwrap();
    }
    actor.ask(FlushToShard {}).await.unwrap();
    actor
        .ask(DeletePartitionKey {
            table_id: 1,
            partition_key: 1,
        })
        .await
        .unwrap();
    release.send(()).unwrap();
    // The callback may already have reported the failure before this request.
    let _ = actor.ask(WaitForBatch).await;
    let (_, after_failure, in_flight, pending) = actor.ask(BatchState).await.unwrap();
    assert_eq!(after_failure, committed);
    assert!(in_flight.is_none());
    assert_eq!(pending, 3);
    assert_eq!(actor.ask(GetWriteBacklog {}).await.unwrap().total, 3);
    assert!(data::Entity::find().all(&db).await.unwrap().is_empty());
    actor.ask(CleanupWal {}).await.unwrap();
    db.execute_unprepared("DROP TRIGGER reject_second")
        .await
        .unwrap();
    actor.ask(FlushToShard {}).await.unwrap();
    actor.ask(WaitForBatch).await.unwrap();
    let (current, committed, in_flight, pending) = actor.ask(BatchState).await.unwrap();
    assert_eq!(committed, current);
    assert!(in_flight.is_none());
    assert_eq!(pending, 0);
    assert_eq!(actor.ask(GetWriteBacklog {}).await.unwrap().total, 0);
    let rows = data::Entity::find().all(&db).await.unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].partition_key, 2);

    actor.stop_gracefully().await.unwrap();
    actor
        .wait_for_shutdown_with_result(|result| {
            result.unwrap();
        })
        .await;
    writer.stop_gracefully().await.unwrap();
    writer.wait_for_shutdown().await;
    shard.stop_gracefully().await.unwrap();
    shard.wait_for_shutdown().await;
    db.close().await.unwrap();
}
