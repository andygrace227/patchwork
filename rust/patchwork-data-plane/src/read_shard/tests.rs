use super::*;
use crate::write_shard::{Upsert, WriteShard};
use kameo::{
    actor::Spawn,
    message::{Context, Message},
};
use serde_json::json;
use std::time::Duration;
use tokio::sync::oneshot;

struct Pause {
    entered: oneshot::Sender<()>,
    release: oneshot::Receiver<()>,
}

#[tokio::test]
async fn point_reads_preserve_keys_payload_and_tombstone_filtering() {
    let dir = tempfile::tempdir().unwrap();
    let mut shard = crate::initialize::shard(dir.path().join("shard.sqlite").display().to_string())
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
    let readers = shard.read_pool(2).unwrap();
    for include_deleted in [false, true] {
        let record = readers
            .ask(Dispatch(Get {
                table_id: 1,
                partition_key: -10,
                secondary_key: 42,
                include_deleted,
            }))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            record,
            data::Model {
                table_id: 1,
                partition_id: None,
                partition_key: -10,
                secondary_key: 42,
                data: payload.clone(),
                timestamp: 123,
                deleted: false,
            }
        );
        assert!(
            readers
                .ask(Dispatch(Get {
                    table_id: 1,
                    partition_key: -10,
                    secondary_key: 99,
                    include_deleted,
                }))
                .await
                .unwrap()
                .is_none()
        );
    }
    shard.delete(1, -10, 42, 124).await.unwrap();
    assert!(
        readers
            .ask(Dispatch(Get {
                table_id: 1,
                partition_key: -10,
                secondary_key: 42,
                include_deleted: false,
            }))
            .await
            .unwrap()
            .is_none()
    );
    let tombstone = readers
        .ask(Dispatch(Get {
            table_id: 1,
            partition_key: -10,
            secondary_key: 42,
            include_deleted: true,
        }))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(tombstone.timestamp, 124);
    assert!(tombstone.deleted);
    assert!(tombstone.data.is_empty());
    readers.stop_gracefully().await.unwrap();
    readers.wait_for_shutdown().await;
}

impl Message<Pause> for ReadShard {
    type Reply = ();

    async fn handle(&mut self, pause: Pause, _: &mut Context<Self, ()>) {
        pause.entered.send(()).unwrap();
        pause.release.await.unwrap();
    }
}

struct TryWrite;

impl Message<TryWrite> for ReadShard {
    type Reply = Result<(), sea_orm::DbErr>;

    async fn handle(&mut self, _: TryWrite, _: &mut Context<Self, Self::Reply>) -> Self::Reply {
        self.db
            .execute_unprepared("DELETE FROM data")
            .await
            .map(|_| ())
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn kameo_pool_uses_another_reader_while_one_is_busy() {
    let dir = tempfile::tempdir().unwrap();
    let shard = crate::initialize::shard(dir.path().join("shard.sqlite").to_str().unwrap().into())
        .await
        .unwrap();
    assert!(shard.read_pool(0).is_err());
    let readers = shard.read_pool(2).unwrap();
    let shard = WriteShard::spawn(shard);

    let (entered, ready) = oneshot::channel();
    let (release, wait) = oneshot::channel();
    let busy = tokio::spawn({
        let readers = readers.clone();
        async move {
            readers
                .ask(Dispatch(Pause {
                    entered,
                    release: wait,
                }))
                .await
                .unwrap()
        }
    });
    tokio::time::timeout(Duration::from_secs(2), ready)
        .await
        .unwrap()
        .unwrap();
    tokio::time::timeout(
        Duration::from_secs(2),
        shard.ask(Upsert {
            table_id: 1,
            partition_key: 10,
            secondary_key: 42,
            data: b"new".to_vec(),
            timestamp: 2,
        }),
    )
    .await
    .unwrap()
    .unwrap();
    let record = tokio::time::timeout(
        Duration::from_secs(2),
        readers.ask(Dispatch(Get {
            table_id: 1,
            partition_key: 10,
            secondary_key: 42,
            include_deleted: false,
        })),
    )
    .await
    .unwrap()
    .unwrap()
    .unwrap();
    assert_eq!(record.timestamp, 2);
    assert!(
        readers
            .ask(Dispatch(TryWrite))
            .await
            .unwrap_err()
            .to_string()
            .contains("readonly")
    );
    release.send(()).unwrap();
    busy.await.unwrap();
    readers.stop_gracefully().await.unwrap();
    readers.wait_for_shutdown().await;
    shard.stop_gracefully().await.unwrap();
    shard.wait_for_shutdown().await;
}
