//! Runs background workflows. Jobs for the same table run one at a time locally;
//! the workflows' database locks still coordinate different control nodes.
//! Queued jobs are in memory. Already-running workflows continue if this actor stops.
//!
//! ```ignore
//! use kameo::actor::Spawn;
//! use patchwork_control_plane::workflows::executor::LongRunningWorkflowExecutor;
//!
//! let executor = LongRunningWorkflowExecutor::spawn(
//!     LongRunningWorkflowExecutor::new(node, partition, table),
//! ); // Starts random table checks as well.
//! ```

use std::{
    collections::{HashMap, VecDeque},
    time::Duration,
};

use anyhow::{Result, ensure};
use kameo::{
    Actor,
    actor::{ActorRef, WeakActorRef},
    error::{ActorStopReason, Infallible},
    message::{Context, Message},
};
use tokio::task::JoinHandle;

use super::{
    partition::{
        decrease_replication::{DecreaseReplication, DecreaseReplicationContext},
        increase_replication::{IncreaseReplication, IncreaseReplicationContext},
        merge_partition::{MergePartition, MergePartitionContext},
        split_partition::{SplitPartition, SplitPartitionContext},
    },
    table::{
        check_table_scale::{CheckTableScale, CheckTableScaleContext},
        delete_table::{DeleteTable, DeleteTableContext},
        make_table::{MakeTable, MakeTableContext},
    },
};
use crate::{
    db::{NodeActor, PartitionActor, TableActor},
    scale,
};

pub enum Workflow {
    MakeTable(MakeTableContext),
    DeleteTable(DeleteTableContext),
    CheckTableScale(CheckTableScaleContext),
    SplitPartition(SplitPartitionContext),
    MergePartition(MergePartitionContext),
    IncreaseReplication(IncreaseReplicationContext),
    DecreaseReplication(DecreaseReplicationContext),
}

impl Workflow {
    fn table_id(&self) -> Option<i64> {
        match self {
            Self::MakeTable(_) => None,
            Self::DeleteTable(ctx) => Some(ctx.table_id),
            Self::CheckTableScale(ctx) => Some(ctx.table_id),
            Self::SplitPartition(ctx) => Some(ctx.table_id),
            Self::MergePartition(ctx) => Some(ctx.table_id),
            Self::IncreaseReplication(ctx) => Some(ctx.table_id),
            Self::DecreaseReplication(ctx) => Some(ctx.table_id),
        }
    }

    fn key(&self) -> String {
        match self {
            Self::MakeTable(ctx) => format!("make_table:{}:{}", ctx.table_owner, ctx.table_name),
            Self::DeleteTable(ctx) => format!("delete_table:{}", ctx.table_id),
            Self::CheckTableScale(ctx) => format!("check_table_scale:{}", ctx.table_id),
            Self::SplitPartition(ctx) => format!("split:{}:{}", ctx.table_id, ctx.new_partition),
            Self::MergePartition(ctx) => format!("merge:{}:{}", ctx.table_id, ctx.hash_start),
            Self::IncreaseReplication(ctx) => format!(
                "increase:{}:{}:{}",
                ctx.table_id, ctx.hash_start, ctx.replication_factor
            ),
            Self::DecreaseReplication(ctx) => format!(
                "decrease:{}:{}:{}",
                ctx.table_id, ctx.hash_start, ctx.replication_factor
            ),
        }
    }

    async fn run(
        self,
        node: ActorRef<NodeActor>,
        partition: ActorRef<PartitionActor>,
        table: ActorRef<TableActor>,
    ) -> Result<()> {
        match self {
            Self::MakeTable(ctx) => MakeTable {
                node,
                partition,
                table,
            }
            .run(ctx)
            .await
            .map(|_| ()),
            Self::DeleteTable(ctx) => {
                DeleteTable {
                    node,
                    partition,
                    table,
                }
                .run(ctx)
                .await
            }
            Self::CheckTableScale(ctx) => {
                CheckTableScale {
                    node,
                    partition,
                    table,
                }
                .run(ctx)
                .await
            }
            Self::SplitPartition(ctx) => {
                SplitPartition {
                    node,
                    partition,
                    table,
                }
                .run(ctx)
                .await
            }
            Self::MergePartition(ctx) => MergePartition { node, partition }.run(ctx).await,
            Self::IncreaseReplication(ctx) => {
                IncreaseReplication { node, partition }.run(ctx).await
            }
            Self::DecreaseReplication(ctx) => {
                DecreaseReplication { node, partition }.run(ctx).await
            }
        }
    }
}

#[derive(Clone, Copy)]
pub struct ExecutorConfig {
    pub max_running: usize,
    pub max_queued: usize,
    pub min_check_interval: Duration,
    pub max_check_interval: Duration,
}

impl Default for ExecutorConfig {
    fn default() -> Self {
        Self {
            max_running: 4,
            max_queued: 1024,
            min_check_interval: Duration::from_secs(30),
            max_check_interval: Duration::from_secs(90),
        }
    }
}

pub struct LongRunningWorkflowExecutor {
    node: ActorRef<NodeActor>,
    partition: ActorRef<PartitionActor>,
    table: ActorRef<TableActor>,
    config: ExecutorConfig,
    queued: VecDeque<Workflow>,
    running: HashMap<String, Option<i64>>,
    completed: u64,
    failed: u64,
    last_error: Option<String>,
    scale_checks: Option<JoinHandle<()>>,
}

impl LongRunningWorkflowExecutor {
    /// Spawning this actor also starts random background scaling checks.
    pub fn new(
        node: ActorRef<NodeActor>,
        partition: ActorRef<PartitionActor>,
        table: ActorRef<TableActor>,
    ) -> Self {
        Self::with_config(node, partition, table, ExecutorConfig::default())
            .expect("valid default configuration")
    }

    pub fn with_config(
        node: ActorRef<NodeActor>,
        partition: ActorRef<PartitionActor>,
        table: ActorRef<TableActor>,
        config: ExecutorConfig,
    ) -> Result<Self> {
        ensure!(
            config.max_running > 0,
            "At least one workflow must be allowed to run"
        );
        ensure!(
            config.max_queued > 0,
            "The workflow queue must have room for jobs"
        );
        ensure!(
            !config.min_check_interval.is_zero(),
            "The scale check interval must be positive"
        );
        ensure!(
            config.max_check_interval >= config.min_check_interval,
            "The scale check interval bounds are reversed"
        );
        Ok(Self {
            node,
            partition,
            table,
            config,
            queued: VecDeque::new(),
            running: HashMap::new(),
            completed: 0,
            failed: 0,
            last_error: None,
            scale_checks: None,
        })
    }

    fn start_ready_jobs<R: kameo::reply::Reply>(&mut self, ctx: &Context<Self, R>) {
        while self.running.len() < self.config.max_running {
            let Some(index) = self.queued.iter().position(|job| {
                job.table_id()
                    .is_none_or(|table_id| !self.running.values().any(|id| *id == Some(table_id)))
            }) else {
                break;
            };
            let job = self.queued.remove(index).expect("queued job exists");
            let key = job.key();
            self.running.insert(key.clone(), job.table_id());
            let node = self.node.clone();
            let partition = self.partition.clone();
            let table = self.table.clone();
            // Await the task through pipe so failures and panics both release the slot.
            let task = tokio::spawn(job.run(node, partition, table));
            ctx.pipe(async move {
                let result = match task.await {
                    Ok(result) => result,
                    Err(error) => Err(error.into()),
                };
                WorkflowFinished { key, result }
            });
        }
    }
}

impl Actor for LongRunningWorkflowExecutor {
    type Args = Self;
    type Error = Infallible;

    async fn on_start(mut state: Self, actor_ref: ActorRef<Self>) -> Result<Self, Infallible> {
        state.scale_checks = Some(tokio::spawn(scale::run_random_scale_checks(
            state.table.clone(),
            actor_ref.downgrade(),
            state.config.min_check_interval,
            state.config.max_check_interval,
        )));
        Ok(state)
    }

    async fn on_stop(
        &mut self,
        _: WeakActorRef<Self>,
        _: ActorStopReason,
    ) -> Result<(), Infallible> {
        if let Some(task) = self.scale_checks.take() {
            task.abort();
            let _ = task.await;
        }
        Ok(())
    }
}

pub struct SubmitWorkflow {
    pub workflow: Workflow,
}

impl Message<SubmitWorkflow> for LongRunningWorkflowExecutor {
    /// True means accepted; false means this job is already queued or running.
    type Reply = Result<bool>;

    async fn handle(
        &mut self,
        msg: SubmitWorkflow,
        ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        let key = msg.workflow.key();
        if self.running.contains_key(&key) || self.queued.iter().any(|job| job.key() == key) {
            return Ok(false);
        }
        let can_start = self.running.len() < self.config.max_running
            && msg.workflow.table_id().is_none_or(|table_id| {
                !self.running.values().any(|id| *id == Some(table_id))
            });
        ensure!(
            self.queued.len() < self.config.max_queued || can_start,
            "The workflow queue is full"
        );
        self.queued.push_back(msg.workflow);
        self.start_ready_jobs(ctx);
        Ok(true)
    }
}

struct WorkflowFinished {
    key: String,
    result: Result<()>,
}

impl Message<WorkflowFinished> for LongRunningWorkflowExecutor {
    type Reply = ();

    async fn handle(&mut self, msg: WorkflowFinished, ctx: &mut Context<Self, ()>) {
        self.running.remove(&msg.key);
        match msg.result {
            Ok(()) => self.completed += 1,
            Err(error) => {
                self.failed += 1;
                let error = format!("{}: {error:#}", msg.key);
                eprintln!("Workflow failed: {error}");
                self.last_error = Some(error);
            }
        }
        self.start_ready_jobs(ctx);
    }
}

pub struct GetExecutorStatus;

#[derive(Debug, kameo::Reply)]
pub struct ExecutorStatus {
    pub queued: usize,
    pub running: usize,
    pub completed: u64,
    pub failed: u64,
    pub last_error: Option<String>,
}

impl Message<GetExecutorStatus> for LongRunningWorkflowExecutor {
    type Reply = ExecutorStatus;

    async fn handle(
        &mut self,
        _: GetExecutorStatus,
        _: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        ExecutorStatus {
            queued: self.queued.len(),
            running: self.running.len(),
            completed: self.completed,
            failed: self.failed,
            last_error: self.last_error.clone(),
        }
    }
}
