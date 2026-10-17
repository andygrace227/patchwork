use std::{
    collections::VecDeque,
    path::Path,
    sync::Arc,
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
use tokio::{sync::oneshot, task::JoinHandle};
use wal_db::{Lsn, SegmentedStore, Wal};

use crate::{
    mutation::Mutation,
    replica_writer::{Put, ReplicaWriter},
    write_shard::{ApplyBatch, ApplyQueuedBatch, WriteShard},
};

type ReplicationReply = ForwardedReply<Put, DelegatedReply<Result<()>>>;

pub(crate) trait WriteFailure: std::fmt::Display {
    fn status(&self) -> Option<&tonic::Status>;
}

impl WriteFailure for anyhow::Error {
    fn status(&self) -> Option<&tonic::Status> {
        self.downcast_ref()
    }
}

impl<M, E: WriteFailure> WriteFailure for kameo::error::SendError<M, E> {
    fn status(&self) -> Option<&tonic::Status> {
        match self {
            Self::HandlerError(error) => error.status(),
            _ => None,
        }
    }
}

pub const FLUSH_INTERVAL: Duration = Duration::from_millis(5);
pub const FLUSH_THRESHOLD: usize = 4096;
pub const MAX_WRITE_BACKLOG: usize = 65_536;
pub const CLEANUP_INTERVAL: Duration = Duration::from_secs(1);

#[derive(Deserialize, Serialize)]
enum LogEntry {
    Write { mutation: Mutation },
}

struct InFlightBatch {
    mutations: Arc<Vec<Mutation>>,
    lsns: Vec<Lsn>,
    end_lsn: Lsn,
    completed: oneshot::Receiver<Result<()>>,
}

struct QueuedMutation {
    lsn: Lsn,
    mutation: Mutation,
}

pub struct WriteHandler {
    shard: ActorRef<WriteShard>,
    wal: Wal<SegmentedStore>,
    queue: VecDeque<QueuedMutation>,
    in_flight: Option<InFlightBatch>,
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
            queue.push_back(QueuedMutation {
                lsn: entry.lsn(),
                mutation,
            });
        }

        Ok(Self {
            shard,
            wal,
            queue,
            in_flight: None,
            current_lsn,
            committed_lsn,
            replica_writer: shard_writer,
            timer: None,
        })
    }

    fn enqueue_mutation(&mut self, mutation: Mutation) -> Result<()> {
        if self.get_write_backlog().total >= MAX_WRITE_BACKLOG {
            return Err(
                tonic::Status::resource_exhausted("write queue is full; retry later").into(),
            );
        }
        let entry = LogEntry::Write { mutation };
        let lsn = self.wal.append(&serde_json::to_vec(&entry)?)?;
        self.current_lsn = Lsn::new(self.wal.len());
        let LogEntry::Write { mutation } = entry;
        self.queue.push_back(QueuedMutation { lsn, mutation });
        Ok(())
    }

    fn enqueue_record(&mut self, record: &Record) -> Result<()> {
        ensure!(record.timestamp >= 0, "timestamp must be nonnegative");
        self.enqueue_mutation(Mutation::Write(record.clone()))
    }

    fn complete_batch(&mut self, result: Result<()>) -> Result<()> {
        let batch = self.in_flight.take().expect("No batch in flight");
        match result {
            Ok(()) => self.committed_lsn = batch.end_lsn,
            Err(error) => {
                // Failed mutations must stay ahead of writes received during the batch.
                let mut queue: VecDeque<_> = batch
                    .lsns
                    .into_iter()
                    .zip(Arc::unwrap_or_clone(batch.mutations))
                    .map(|(lsn, mutation)| QueuedMutation { lsn, mutation })
                    .collect();
                queue.append(&mut self.queue);
                self.queue = queue;
                return Err(error);
            }
        }
        Ok(())
    }

    fn check_batch(&mut self) -> Result<()> {
        let Some(batch) = &mut self.in_flight else {
            return Ok(());
        };
        let result = match batch.completed.try_recv() {
            Ok(result) => result,
            Err(oneshot::error::TryRecvError::Empty) => return Ok(()),
            Err(error) => Err(anyhow::anyhow!("Batch completion lost: {error}")),
        };
        self.complete_batch(result)
    }

    fn start_flush(&mut self, handler: WeakActorRef<Self>) -> Result<()> {
        self.check_batch()?;
        if self.in_flight.is_some() || self.queue.is_empty() {
            return Ok(());
        }
        let count = self.queue.len().min(FLUSH_THRESHOLD);
        let (lsns, mutations): (Vec<_>, Vec<_>) = self
            .queue
            .drain(..count)
            .map(|entry| (entry.lsn, entry.mutation))
            .unzip();
        let mutations = Arc::new(mutations);
        let end_lsn = self
            .queue
            .front()
            .map_or(self.current_lsn, |entry| entry.lsn);
        let (completed, receiver) = oneshot::channel();
        self.in_flight = Some(InFlightBatch {
            mutations: mutations.clone(),
            lsns,
            end_lsn,
            completed: receiver,
        });
        if let Err(error) = self
            .shard
            .tell(ApplyQueuedBatch {
                mutations,
                end_lsn,
                handler,
                completed,
            })
            .try_send()
        {
            return self.complete_batch(Err(anyhow::anyhow!("Could not send batch: {error}")));
        }
        Ok(())
    }

    async fn wait_for_batch(&mut self) -> Result<()> {
        if let Some(batch) = &mut self.in_flight {
            let result = (&mut batch.completed)
                .await
                .unwrap_or_else(|error| Err(anyhow::anyhow!("Batch completion lost: {error}")));
            self.complete_batch(result)?;
        }
        Ok(())
    }

    // Startup and shutdown can wait directly, without processing commit messages.
    async fn drain_to_shard(&mut self) -> Result<()> {
        self.wait_for_batch().await?;
        while !self.queue.is_empty() {
            let count = self.queue.len().min(FLUSH_THRESHOLD);
            self.shard
                .ask(ApplyBatch {
                    mutations: Arc::new(
                        self.queue
                            .iter()
                            .take(count)
                            .map(|entry| entry.mutation.clone())
                            .collect(),
                    ),
                })
                .await
                .map_err(|error| anyhow::anyhow!("{error}"))?;
            self.queue.drain(..count);
            self.committed_lsn = self
                .queue
                .front()
                .map_or(self.current_lsn, |entry| entry.lsn);
        }
        Ok(())
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
        if self.queue.len() >= FLUSH_THRESHOLD {
            if let Err(error) = self.start_flush(ctx.actor_ref().downgrade()) {
                eprintln!("Could not flush queued mutations: {error}");
            }
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
        state.drain_to_shard().await?;
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
        self.drain_to_shard().await
    }
}

#[messages]
impl WriteHandler {
    /// Accepted mutations not yet confirmed committed to this node's SQLite shard.
    #[message]
    pub fn get_write_backlog(&self) -> crate::telemetry::WriteBacklog {
        let queued = self.queue.len();
        let in_flight = self
            .in_flight
            .as_ref()
            .map_or(0, |batch| batch.mutations.len());
        crate::telemetry::WriteBacklog {
            queued,
            in_flight,
            total: queued + in_flight,
        }
    }

    /// Move pending mutations aside so new writes can arrive during the commit.
    /// Returns after sending; the completion reply advances committed_lsn separately.
    #[message(ctx)]
    pub async fn flush_to_shard(&mut self, ctx: &mut Context<Self, Result<()>>) -> Result<()> {
        self.start_flush(ctx.actor_ref().downgrade())
    }

    #[message(ctx)]
    pub fn batch_finished(&mut self, end_lsn: Lsn, ctx: &mut Context<Self, ()>) {
        if self
            .in_flight
            .as_ref()
            .is_some_and(|batch| batch.end_lsn == end_lsn)
        {
            if let Err(error) = self.check_batch() {
                eprintln!("Could not flush queued mutations: {error}");
                return;
            }
            if self.queue.len() >= FLUSH_THRESHOLD {
                if let Err(error) = self.start_flush(ctx.actor_ref().downgrade()) {
                    eprintln!("Could not flush queued mutations: {error}");
                }
            }
        }
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
            data: Vec::new(),
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
