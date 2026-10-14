use std::{collections::HashMap, path::Path, time::Duration};

use anyhow::Result;
use kameo::{
    Actor,
    actor::{ActorRef, WeakActorRef},
    error::ActorStopReason,
    messages,
};
use patchwork_common::db::access_stats::{self, Entity};
use sea_orm::{
    ColumnTrait, ConnectOptions, ConnectionTrait, Database, DatabaseConnection, EntityTrait,
    IntoActiveModel, NotSet, QueryFilter, QueryOrder, Schema, TransactionTrait,
};
use tokio::task::JoinHandle;

use super::{Model, cache::TelemetryCache};

pub const FLUSH_INTERVAL: Duration = Duration::from_secs(60);
pub const RETENTION: Duration = Duration::from_secs(30 * 24 * 60 * 60);

/// One per host; share its ActorRef with every shard and read worker.
pub struct AccessStatsActor {
    db: DatabaseConnection,
    cache: TelemetryCache,
    flush_interval: Duration,
    retention: Duration,
    timer: Option<JoinHandle<()>>,
}

impl AccessStatsActor {
    pub async fn new(path: impl AsRef<Path>) -> Result<Self> {
        Self::with_config(path, FLUSH_INTERVAL, RETENTION).await
    }

    pub async fn with_config(
        path: impl AsRef<Path>,
        flush_interval: Duration,
        retention: Duration,
    ) -> Result<Self> {
        anyhow::ensure!(!flush_interval.is_zero(), "Flush interval must be positive");
        anyhow::ensure!(!retention.is_zero(), "Retention must be positive");
        anyhow::ensure!(
            retention.as_millis() <= i64::MAX as u128,
            "Retention is too long"
        );
        let path = path.as_ref();
        if let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty()) {
            std::fs::create_dir_all(parent)?;
        }
        let mut options = ConnectOptions::new(format!("sqlite://{}?mode=rwc", path.display()));
        options.max_connections(1).sqlx_logging(false);
        let db = Database::connect(options).await?;
        db.execute_unprepared("PRAGMA journal_mode = WAL").await?;
        let mut schema = Schema::new(db.get_database_backend()).create_table_from_entity(Entity);
        schema.if_not_exists();
        db.execute(&schema).await?;
        db.execute_unprepared(
            "CREATE INDEX IF NOT EXISTS access_statistics_end_ms ON access_statistics (end_ms)",
        )
        .await?;
        db.execute_unprepared(
            "CREATE INDEX IF NOT EXISTS access_statistics_key_time ON access_statistics (table_id, hash_start, start_ms)",
        ).await?;
        Ok(Self {
            db,
            cache: TelemetryCache::new(),
            flush_interval,
            retention,
            timer: None,
        })
    }
}

impl Actor for AccessStatsActor {
    type Args = Self;
    type Error = anyhow::Error;

    async fn on_start(mut state: Self, actor_ref: ActorRef<Self>) -> Result<Self> {
        let weak = actor_ref.downgrade();
        let interval = state.flush_interval;
        state.timer = Some(tokio::spawn(async move {
            loop {
                tokio::time::sleep(interval).await;
                let Some(actor) = weak.upgrade() else { break };
                if let Err(error) = actor.ask(Flush {}).await {
                    eprintln!("Could not flush access statistics: {error}");
                    if !actor.is_alive() {
                        break;
                    }
                }
            }
        }));
        Ok(state)
    }

    async fn on_stop(&mut self, _: WeakActorRef<Self>, _: ActorStopReason) -> Result<()> {
        if let Some(timer) = self.timer.take() {
            timer.abort();
        }
        self.flush().await?;
        self.db.clone().close().await?;
        Ok(())
    }
}

#[messages]
impl AccessStatsActor {
    #[message]
    pub async fn track_access(
        &mut self,
        table_id: i64,
        partition_key: i64,
        secondary_key: i64,
        is_write: bool,
    ) {
        self.cache
            .inc(table_id, partition_key, secondary_key, is_write);
    }

    /// Append this sample and prune expired history in one transaction.
    /// A failed write keeps the cache intact for the next attempt.
    #[message]
    pub async fn flush(&mut self) -> Result<()> {
        let rows = self.cache.stats();
        let txn = self.db.begin().await?;
        // Chunk below SQLite's bind-variable limit; the transaction covers every chunk.
        for chunk in rows.chunks(50) {
            Entity::insert_many(chunk.iter().cloned().map(|row| {
                let mut row = row.into_active_model();
                row.id = NotSet;
                row
            }))
            .exec_without_returning(&txn)
            .await?;
        }
        Entity::delete_many()
            .filter(
                access_stats::Column::EndMs
                    .lt(super::now_ms().saturating_sub(self.retention.as_millis() as i64)),
            )
            .exec(&txn)
            .await?;
        txn.commit().await?;
        self.cache = TelemetryCache::new();
        Ok(())
    }

    /// Current unflushed samples. Reading does not drain the cache.
    #[message]
    pub async fn get_telemetry(&self) -> Result<Vec<Model>> {
        self.cache
            .stats()
            .into_iter()
            .map(|row| Ok(row.into_telemetry()?))
            .collect()
    }

    #[message]
    pub async fn get_telemetry_for_table(&self, table_id: i64) -> Result<HashMap<i64, Model>> {
        Ok(self
            .get_telemetry()
            .await?
            .into_iter()
            .filter(|row| row.table_id == table_id)
            .map(|row| (row.partition_key, row))
            .collect())
    }

    /// Stored samples overlapping [start_ms, end_ms), oldest first.
    #[message]
    pub async fn get_history(
        &self,
        table_id: i64,
        hash_start: i64,
        start_ms: i64,
        end_ms: i64,
    ) -> Result<Vec<access_stats::Model>> {
        anyhow::ensure!(start_ms < end_ms, "start_ms must be less than end_ms");
        Ok(Entity::find()
            .filter(access_stats::Column::TableId.eq(table_id))
            .filter(access_stats::Column::HashStart.eq(hash_start))
            .filter(access_stats::Column::EndMs.gte(start_ms))
            .filter(access_stats::Column::StartMs.lt(end_ms))
            .filter(
                access_stats::Column::EndMs
                    .gte(super::now_ms().saturating_sub(self.retention.as_millis() as i64)),
            )
            .order_by_asc(access_stats::Column::StartMs)
            .all(&self.db)
            .await?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use kameo::actor::Spawn;

    #[tokio::test]
    async fn failed_batch_keeps_cache_and_rolls_back_every_chunk() {
        let dir = tempfile::tempdir().unwrap();
        let mut actor = AccessStatsActor::new(dir.path().join("stats.sqlite"))
            .await
            .unwrap();
        for key in 0..60 {
            actor.track_access(1, key, i64::MAX, true).await;
        }
        actor.db.execute_unprepared(
            "CREATE TRIGGER fail_batch BEFORE INSERT ON access_statistics WHEN NEW.hash_start = 59 BEGIN SELECT RAISE(ABORT, 'test failure'); END",
        ).await.unwrap();
        assert!(actor.flush().await.is_err());
        assert_eq!(actor.cache.stats().len(), 60);
        assert!(Entity::find().all(&actor.db).await.unwrap().is_empty());
        actor
            .db
            .execute_unprepared("DROP TRIGGER fail_batch")
            .await
            .unwrap();
        actor.flush().await.unwrap();
        assert!(actor.cache.stats().is_empty());
        assert_eq!(Entity::find().all(&actor.db).await.unwrap().len(), 60);
        // Two quick flushes of the same key append separate rows, even within one millisecond.
        for _ in 0..2 {
            actor.track_access(1, 0, i64::MAX, true).await;
            actor.flush().await.unwrap();
        }
        assert_eq!(actor.get_history(1, 0, 0, i64::MAX).await.unwrap().len(), 3);
    }

    #[tokio::test]
    async fn idle_flush_prunes_expired_history_and_preserves_new_rows() {
        let dir = tempfile::tempdir().unwrap();
        let mut actor = AccessStatsActor::new(dir.path().join("stats.sqlite"))
            .await
            .unwrap();
        actor.track_access(1, 10, i64::MAX, true).await;
        actor.track_access(1, 10, i64::MAX, true).await;
        actor.flush().await.unwrap();
        let rows = actor.get_history(1, 10, 0, i64::MAX).await.unwrap();
        assert_eq!(
            rows[0].write_position_sum,
            (i64::MAX as i128 * 2).to_string()
        );
        let mut expired = rows[0].clone().into_active_model();
        expired.id = NotSet;
        expired.start_ms = sea_orm::Set(0);
        expired.end_ms = sea_orm::Set(1);
        Entity::insert(expired).exec(&actor.db).await.unwrap();
        actor.flush().await.unwrap();
        assert_eq!(Entity::find().all(&actor.db).await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn timer_and_shutdown_flush_to_a_database_that_survives_restart() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("stats.sqlite");
        let actor = AccessStatsActor::spawn(
            AccessStatsActor::with_config(&path, Duration::from_millis(10), RETENTION)
                .await
                .unwrap(),
        );
        actor
            .ask(TrackAccess {
                table_id: 1,
                partition_key: 10,
                secondary_key: 42,
                is_write: true,
            })
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let rows = actor
                    .ask(GetHistory {
                        table_id: 1,
                        hash_start: 10,
                        start_ms: 0,
                        end_ms: i64::MAX,
                    })
                    .await
                    .unwrap();
                if !rows.is_empty() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
        actor
            .ask(TrackAccess {
                table_id: 2,
                partition_key: 10,
                secondary_key: 99,
                is_write: false,
            })
            .await
            .unwrap();
        actor.stop_gracefully().await.unwrap();
        actor.wait_for_shutdown().await;
        let reopened = AccessStatsActor::new(&path).await.unwrap();
        assert_eq!(
            reopened.get_history(1, 10, 0, i64::MAX).await.unwrap()[0].write_count,
            1
        );
        assert_eq!(
            reopened.get_history(2, 10, 0, i64::MAX).await.unwrap()[0].read_count,
            1
        );
        assert!(reopened.get_telemetry().await.unwrap().is_empty());
    }
}
