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
    future::Future,
    pin::Pin,
    sync::Arc,
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

use super::job::JobCompletion;
pub use super::{
    Workflow,
    job::{JobHandle, JobStatus},
};
use crate::{
    db::{NodeActor, PartitionActor, TableActor},
    scale,
};

struct QueuedJob {
    key: String,
    table_id: Option<i64>,
    run: Pin<Box<dyn Future<Output = Result<(), Arc<anyhow::Error>>> + Send>>,
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
    queued: VecDeque<QueuedJob>,
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
                job.table_id
                    .is_none_or(|table_id| !self.running.values().any(|id| *id == Some(table_id)))
            }) else {
                break;
            };
            let job = self.queued.remove(index).expect("queued job exists");
            let key = job.key;
            self.running.insert(key.clone(), job.table_id);
            ctx.pipe(async move {
                let result = job.run.await;
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
            state.node.clone(),
            state.partition.clone(),
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

pub struct SubmitWorkflow<W: Workflow> {
    pub workflow: W,
    pub context: W::Context,
}

impl<W: Workflow> SubmitWorkflow<W> {
    pub fn new(workflow: W, context: W::Context) -> Self {
        Self { workflow, context }
    }
}

impl<W: Workflow> Message<SubmitWorkflow<W>> for LongRunningWorkflowExecutor {
    /// Accepted jobs return a handle; duplicates return None.
    type Reply = Result<Option<JobHandle<W::Output>>>;

    async fn handle(
        &mut self,
        msg: SubmitWorkflow<W>,
        ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        let key = msg.workflow.key(&msg.context);
        let table_id = msg.workflow.table_id(&msg.context);
        if self.running.contains_key(&key) || self.queued.iter().any(|job| job.key == key) {
            return Ok(None);
        }
        let can_start = self.running.len() < self.config.max_running
            && table_id.is_none_or(|id| !self.running.values().any(|running| *running == Some(id)));
        ensure!(
            self.queued.len() < self.config.max_queued || can_start,
            "The workflow queue is full"
        );
        let (handle, completion) = JobCompletion::new();
        self.queued.push_back(QueuedJob {
            key,
            table_id,
            run: Box::pin(async move {
                completion.running();
                // Also catch a panic outside Cano's steps, so the queue slot is released.
                let result =
                    match tokio::spawn(async move { msg.workflow.run(msg.context).await }).await {
                        Ok(result) => result,
                        Err(error) => Err(error.into()),
                    };
                completion.finish(result)
            }),
        });
        self.start_ready_jobs(ctx);
        Ok(Some(handle))
    }
}

struct WorkflowFinished {
    key: String,
    result: Result<(), Arc<anyhow::Error>>,
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
