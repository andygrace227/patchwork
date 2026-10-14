use std::{
    collections::VecDeque,
    path::Path,
    time::{Duration, Instant},
};

use anyhow::{Result, ensure};
use kameo::{
    Actor,
    actor::{ActorRef, WeakActorRef},
    error::ActorStopReason,
    message::Context,
    messages,
    reply::{DelegatedReply, ForwardedReply},
};
use patchwork_common::dataplane_client::Record;
use serde::{Deserialize, Serialize};
use tokio::task::JoinHandle;
use wal_db::{Lsn, SegmentedStore, Wal};

use crate::{
    mutation::Mutation,
    replica_writer::{Put, ReplicaWriter},
    write_shard::{ApplyBatch, WriteShard},
};

type ReplicationReply = ForwardedReply<Put, DelegatedReply<Result<()>>>;

pub const FLUSH_INTERVAL: Duration = Duration::from_millis(5);
pub const CLEANUP_INTERVAL: Duration = Duration::from_secs(1);

#[derive(Deserialize, Serialize)]
enum LogEntry {
    Write { mutation: Mutation },
}

pub struct WriteHandler {
    shard: ActorRef<WriteShard>,
    wal: Wal<SegmentedStore>,
    queue: VecDeque<Mutation>,
    // End offsets: the next record starts at current_lsn.
    current_lsn: Lsn,
    committed_lsn: Lsn,
    replica_writer: ActorRef<ReplicaWriter>,
    timer: Option<JoinHandle<()>>,
}

impl WriteHandler {
    pub fn new(
        shard: ActorRef<WriteShard>,
        shard_writer: ActorRef<ReplicaWriter>,
        wal_path: &Path,
    ) -> Result<Self> {
        let wal = Wal::open_segmented(wal_path, 16 * 1024 * 1024)?;

        let current_lsn = Lsn::new(wal.len());
        let mut committed_lsn = current_lsn;
        let mut queue = VecDeque::new();
        for entry in wal.iter()? {
            let entry = entry?;
            if queue.is_empty() {
                committed_lsn = entry.lsn();
            }
            let LogEntry::Write { mutation } = serde_json::from_slice(entry.data())?;
            queue.push_back(mutation);
        }

        Ok(Self {
            shard,
            wal,
            queue,
            current_lsn,
            committed_lsn,
            replica_writer: shard_writer,
            timer: None,
        })
    }

    fn enqueue_mutation(&mut self, mutation: Mutation) -> Result<()> {
        let entry = LogEntry::Write { mutation };
        let _ = self.wal.append(&serde_json::to_vec(&entry)?)?;
        self.current_lsn = Lsn::new(self.wal.len());
        let LogEntry::Write { mutation } = entry;
        self.queue.push_back(mutation);
        Ok(())
    }

    fn enqueue_record(&mut self, record: &Record) -> Result<()> {
        ensure!(record.timestamp >= 0, "timestamp must be nonnegative");
        self.enqueue_mutation(Mutation::Write(record.clone()))
    }

    pub async fn write_internal_logic(
        &mut self,
        record: Record,
        replicas: Vec<String>,
        consistent: bool,
        ctx: &mut Context<Self, ReplicationReply>,
    ) -> ReplicationReply {
        if let Err(error) = self.enqueue_record(&record) {
            return ReplicationReply::from_err(error);
        }
        if replicas.is_empty() {
            return ReplicationReply::from_ok(());
        }
        if consistent {
            return ctx
                .forward(&self.replica_writer, Put { record, replicas })
                .await;
        }
        match self.replica_writer.tell(Put { record, replicas }).await {
            Ok(()) => ReplicationReply::from_ok(()),
            Err(error) => ReplicationReply::from_err(anyhow::anyhow!("{error}")),
        }
    }
}

impl Actor for WriteHandler {
    type Args = Self;
    type Error = anyhow::Error;

    async fn on_start(mut state: Self, actor_ref: ActorRef<Self>) -> Result<Self> {
        // Replay the retained log before accepting new messages.
        state.flush_to_shard().await?;
        let weak = actor_ref.downgrade();
        state.timer = Some(tokio::spawn(async move {
            let mut last_cleanup = Instant::now();
            loop {
                tokio::time::sleep(FLUSH_INTERVAL).await;
                let Some(actor) = weak.upgrade() else { break };
                if let Err(error) = actor.ask(FlushToShard {}).await {
                    eprintln!("Could not flush queued mutations: {error}");
                    if !actor.is_alive() {
                        break;
                    }
                }
                if last_cleanup.elapsed() >= CLEANUP_INTERVAL {
                    if let Err(error) = actor.ask(CleanupWal {}).await {
                        eprintln!("Could not clean up the WAL: {error}");
                        if !actor.is_alive() {
                            break;
                        }
                    }
                    last_cleanup = Instant::now();
                }
            }
        }));
        Ok(state)
    }

    async fn on_stop(&mut self, _: WeakActorRef<Self>, _: ActorStopReason) -> Result<()> {
        if let Some(timer) = self.timer.take() {
            timer.abort();
        }
        self.flush_to_shard().await
    }
}

#[messages]
impl WriteHandler {
    /// Keep the queue intact until the shard confirms the whole batch committed.
    #[message]
    pub async fn flush_to_shard(&mut self) -> Result<()> {
        if self.queue.is_empty() {
            return Ok(());
        }
        self.shard
            .ask(ApplyBatch {
                mutations: self.queue.iter().cloned().collect(),
            })
            .await
            .map_err(|error| anyhow::anyhow!("{error}"))?;
        self.committed_lsn = self.current_lsn;
        self.queue.clear();
        Ok(())
    }

    /// Reclaim only the prefix already applied to SQLite.
    #[message]
    pub fn cleanup_wal(&mut self) -> Result<()> {
        let _ = self.wal.truncate_before(self.committed_lsn)?;
        Ok(())
    }

    #[message(ctx)]
    pub async fn write_record(
        &mut self,
        record: Record,
        replicas: Vec<String>,
        consistent: bool,
        ctx: &mut Context<Self, ReplicationReply>,
    ) -> ReplicationReply {
        self.write_internal_logic(record, replicas, consistent, ctx)
            .await
    }

    #[message(ctx)]
    pub async fn upsert_consistent(
        &mut self,
        mut record: Record,
        replicas: Vec<String>,
        ctx: &mut Context<Self, ReplicationReply>,
    ) -> ReplicationReply {
        record.deleted = false;
        self.write_internal_logic(record, replicas, true, ctx).await
    }

    #[message(ctx)]
    pub async fn upsert_inconsistent(
        &mut self,
        mut record: Record,
        replicas: Vec<String>,
        ctx: &mut Context<Self, ReplicationReply>,
    ) -> ReplicationReply {
        record.deleted = false;
        self.write_internal_logic(record, replicas, false, ctx)
            .await
    }

    // The existing delete reply type is retained; queued deletes have no row count.
    #[message]
    pub async fn delete(
        &mut self,
        table_id: i64,
        partition_key: i64,
        secondary_key: i64,
        timestamp: i64,
    ) -> Result<u64> {
        self.enqueue_record(&Record {
            table_id,
            partition_key,
            secondary_key,
            timestamp,
            deleted: true,
            data: serde_json::Value::Null,
        })?;
        Ok(0)
    }

    #[message]
    pub async fn delete_partition_key(&mut self, table_id: i64, partition_key: i64) -> Result<u64> {
        self.enqueue_mutation(Mutation::DeletePartitionKey {
            table_id,
            partition_key,
        })?;
        Ok(0)
    }

    #[message]
    pub async fn delete_table(&mut self, table_id: i64) -> Result<u64> {
        self.enqueue_mutation(Mutation::DeleteTable { table_id })?;
        Ok(0)
    }

    #[message]
    pub async fn delete_range(
        &mut self,
        table_id: i64,
        upper_bound: i64,
        lower_bound: i64,
    ) -> Result<u64> {
        ensure!(
            lower_bound < upper_bound,
            "lower_bound must be less than upper_bound"
        );
        self.enqueue_mutation(Mutation::DeleteRange {
            table_id,
            upper_bound,
            lower_bound,
        })?;
        Ok(0)
    }
}

#[cfg(test)]
mod tests;
