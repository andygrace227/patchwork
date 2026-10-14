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
    let shard = WriteShard::new(dir.path().join("shard.sqlite").to_str().unwrap().into())
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
            data: json!("new"),
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
