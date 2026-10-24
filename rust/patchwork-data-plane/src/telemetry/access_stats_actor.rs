use std::{collections::HashMap, path::Path, sync::Arc, time::Duration};

use anyhow::Result;
use kameo::{
    Actor,
    actor::{ActorRef, WeakActorRef},
    error::ActorStopReason,
    messages,
};
use patchwork_common::datamodels::access_stats::{self, Entity};
use sea_orm::{ColumnTrait, DatabaseConnection, EntityTrait, QueryFilter, QueryOrder};
use tokio::task::JoinHandle;

use super::{Model, QueuedAccess, cache::TelemetryCache};
use wal_db::{SegmentedStore, Wal};

pub const FLUSH_INTERVAL: Duration = Duration::from_secs(60);
pub const RETENTION: Duration = Duration::from_secs(30 * 24 * 60 * 60);

struct InFlightBatch {
    cache: TelemetryCache,
    task: JoinHandle<Result<()>>,
}

/// One per host. Journal accesses, then commit snapshots without blocking its mailbox.
/// Appends survive process restarts; background sync bounds power-loss exposure.
pub struct AccessStatsActor {
    db: DatabaseConnection,
    cache: TelemetryCache,
    flush_interval: Duration,
    retention: Duration,
    timer: Option<JoinHandle<()>>,
    sync_timer: Option<JoinHandle<()>>,
    wal: Arc<Wal<SegmentedStore>>,
    in_flight: Option<InFlightBatch>,
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
        let db = super::store::open(path).await?;
        let committed = super::store::checkpoint(&db).await?;
        let wal = Arc::new(Wal::open_segmented(
            format!("{}.wal", path.display()),
            16 * 1024 * 1024,
        )?);
        anyhow::ensure!(
            committed <= wal.len(),
            "Telemetry journal is older than its database checkpoint"
        );
        let mut cache = TelemetryCache::new();
        let cutoff = super::now_ms().saturating_sub(retention.as_millis() as i64);
        for entry in wal.iter()? {
            let entry = entry?;
            if entry.lsn().get() < committed {
                continue;
            }
            for access in serde_json::from_slice::<Vec<QueuedAccess>>(entry.data())? {
                if access.time_ms >= cutoff {
                    cache.inc_at(
                        access.table_id,
                        access.partition_key,
                        access.is_write,
                        Some(access.partition_id),
                        access.time_ms,
                    );
                }
            }
        }
        Ok(Self {
            db,
            cache,
            flush_interval,
            retention,
            timer: None,
            sync_timer: None,
            wal,
            in_flight: None,
        })
    }
    fn enqueue(&mut self, accesses: Vec<QueuedAccess>) -> Result<()> {
        if accesses.is_empty() {
            return Ok(());
        }
        let _ = self.wal.append(&serde_json::to_vec(&accesses)?)?;
        for access in accesses {
            self.cache.inc_at(
                access.table_id,
                access.partition_key,
                access.is_write,
                Some(access.partition_id),
                access.time_ms,
            );
        }
        Ok(())
    }

    async fn finish_batch(&mut self) -> Result<()> {
        let Some(batch) = self.in_flight.take() else {
            return Ok(());
        };
        let result = batch.task.await.unwrap_or_else(|error| Err(error.into()));
        if result.is_err() {
            let current = std::mem::replace(&mut self.cache, batch.cache);
            self.cache.merge(current);
        }
        result
    }

    async fn start_flush(&mut self) -> Result<()> {
        if self
            .in_flight
            .as_ref()
            .is_some_and(|batch| batch.task.is_finished())
        {
            self.finish_batch().await?;
        }
        if self.in_flight.is_some() {
            return Ok(());
        }
        let cache = std::mem::take(&mut self.cache);
        let rows = cache.stats();
        let end_lsn = self.wal.len();
        let db = self.db.clone();
        let wal = self.wal.clone();
        let retention = self.retention;
        let task = tokio::spawn(super::store::flush(db, wal, rows, end_lsn, retention));

        self.in_flight = Some(InFlightBatch { cache, task });
        Ok(())
    }
}

impl Actor for AccessStatsActor {
    type Args = Self;
    type Error = anyhow::Error;

    async fn on_start(mut state: Self, actor_ref: ActorRef<Self>) -> Result<Self> {
        let weak = actor_ref.downgrade();
        let interval = state.flush_interval;
        let wal = state.wal.clone();
        state.sync_timer = Some(tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_millis(100)).await;
                let wal = wal.clone();
                match tokio::task::spawn_blocking(move || wal.sync()).await {
                    Ok(Ok(())) => {}
                    error => eprintln!("Could not sync telemetry journal: {error:?}"),
                }
            }
        }));
        state.timer = Some(tokio::spawn(async move {
            loop {
                tokio::time::sleep(interval).await;
                let Some(actor) = weak.upgrade() else { break };
                if let Err(error) = actor.ask(StartFlushBatch {}).await {
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
        if let Some(timer) = self.sync_timer.take() {
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
    pub async fn record_batch(&mut self, accesses: Vec<QueuedAccess>) -> Result<()> {
        self.enqueue(accesses)
    }

    #[message]
    pub async fn track_access(
        &mut self,
        table_id: i64,
        partition_key: i64,
        is_write: bool,
        partition_id: Option<String>,
    ) -> Result<()> {
        let Some(partition_id) = partition_id.filter(|id| !id.is_empty()) else {
            return Ok(());
        };
        self.enqueue(vec![QueuedAccess {
            table_id,
            partition_key,
            is_write,
            partition_id,
            time_ms: super::now_ms(),
        }])
    }

    /// Start the disk write and immediately release the mailbox.
    #[message]
    pub async fn start_flush_batch(&mut self) -> Result<()> {
        self.start_flush().await
    }

    /// Explicit flush and shutdown wait until all accesses accepted so far are stored.
    #[message]
    pub async fn flush(&mut self) -> Result<()> {
        self.finish_batch().await?;
        self.start_flush().await?;
        self.finish_batch().await
    }

    /// Current unflushed samples. Reading does not drain the cache.
    #[message]
    pub async fn get_telemetry(&mut self) -> Result<Vec<Model>> {
        if self
            .in_flight
            .as_ref()
            .is_some_and(|batch| batch.task.is_finished())
        {
            self.finish_batch().await?;
        }
        let rows = if let Some(batch) = &self.in_flight {
            let mut cache = batch.cache.clone();
            cache.merge(self.cache.clone());
            cache.stats()
        } else {
            self.cache.stats()
        };
        rows.into_iter()
            .map(|row| Ok(row.into_telemetry()?))
            .collect()
    }

    #[message]
    pub async fn get_telemetry_for_table(
        &mut self,
        table_id: i64,
    ) -> Result<HashMap<String, Model>> {
        Ok(self
            .get_telemetry()
            .await?
            .into_iter()
            .filter(|row| row.table_id == table_id)
            .map(|row| (row.partition_id.clone(), row))
            .collect())
    }

    /// Stored samples overlapping [start_ms, end_ms), oldest first.
    #[message]
    pub async fn get_history(
        &self,
        table_id: i64,
        partition_id: String,
        start_ms: i64,
        end_ms: i64,
    ) -> Result<Vec<access_stats::Model>> {
        anyhow::ensure!(start_ms < end_ms, "start_ms must be less than end_ms");
        Ok(Entity::find()
            .filter(access_stats::Column::TableId.eq(table_id))
            .filter(access_stats::Column::PartitionId.eq(partition_id))
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
mod tests;
