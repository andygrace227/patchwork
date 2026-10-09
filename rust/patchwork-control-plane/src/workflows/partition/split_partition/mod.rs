use super::copy_range::copy_range;
use crate::workflows::data::fetch_range::FetchRangeCtx;
use crate::workflows::stages::{RunState, finish, task_error, unlock};
use crate::{dataplane_client, db::actor::Get};
use crate::{
    db::{
        NodeActor, PartitionActor, TableActor,
        node::GetNodeExcluding,
        parition_lock::AttemptLock,
        partition::{self, CreateLockedPartition, GetTablePartitions, UpdateLockedPartition},
    },
    workflows::partition::circular_get,
};
use anyhow::{Context, Result};
use cano::{
    CancellationToken, CanoError, Resources, TaskConfig, TaskResult, Workflow as CanoWorkflow,
};
use kameo::actor::ActorRef;
use sea_orm::ActiveValue::Set;
use tokio::sync::Mutex;

/// Split the existing partition containing `new_partition` at that hash boundary.
#[derive(Clone)]
pub struct SplitPartitionContext {
    pub table_id: i64,
    pub new_partition: i64,
}

#[derive(Clone)]
pub struct SplitPartition {
    pub node: ActorRef<NodeActor>,
    pub partition: ActorRef<PartitionActor>,
    pub table: ActorRef<TableActor>,
}

mod steps;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum State {
    Prepare,
    Lock,
    SelectNodes,
    Publish,
    Copy,
    Activate,
    Cleanup,
    Unlock,
    Done,
}

struct Run {
    workflow: SplitPartition,
    ctx: SplitPartitionContext,
    locks: Vec<crate::db::parition_lock::Model>,
    boundaries: Option<(partition::Model, partition::Model)>,
    keys: Vec<i64>,
    lease_end: i64,
    replicas: Vec<i64>,
}

#[cano::task]
impl cano::Task<State> for State {
    fn config(&self) -> TaskConfig {
        // Copying and metadata changes must not be retried automatically.
        TaskConfig::minimal()
    }

    async fn run(
        &self,
        resources: &Resources,
    ) -> std::result::Result<TaskResult<State>, CanoError> {
        let run = resources.get::<RunState<Run>, _>("run")?;
        let mut run = run.0.lock().await;
        let next = match self {
            Self::Prepare => run.prepare().await,
            Self::Lock => run.lock().await,
            Self::SelectNodes => run.select_nodes().await,
            Self::Publish => run.publish().await,
            Self::Copy => run.copy().await,
            Self::Activate => run.activate().await,
            Self::Cleanup => run.cleanup().await,
            Self::Unlock => run.unlock().await,
            Self::Done => Ok(State::Done),
        }
        .map_err(task_error)?;
        Ok(TaskResult::Single(next))
    }
}

impl SplitPartition {
    pub async fn run(&self, ctx: SplitPartitionContext) -> Result<()> {
        let resources = Resources::new().insert(
            "run",
            RunState(Mutex::new(Run {
                workflow: self.clone(),
                ctx,
                locks: Vec::new(),
                boundaries: None,
                keys: Vec::new(),
                lease_end: 0,
                replicas: Vec::new(),
            })),
        );
        let run = resources.get::<RunState<Run>, _>("run")?;
        let workflow = CanoWorkflow::new(resources)
            .register(State::Prepare, State::Prepare)
            .register(State::Lock, State::Lock)
            .register(State::SelectNodes, State::SelectNodes)
            .register(State::Publish, State::Publish)
            .register(State::Copy, State::Copy)
            .register(State::Activate, State::Activate)
            .register(State::Cleanup, State::Cleanup)
            .register(State::Unlock, State::Unlock)
            .add_exit_state(State::Done);
        let result = workflow
            .orchestrate(State::Prepare, CancellationToken::disabled())
            .await
            .map(|_| ())
            .map_err(anyhow::Error::from);

        // Cano reports step failures and panics. Always release partially acquired locks.
        let mut run = run.0.lock().await;
        let released = run.unlock().await;
        finish(result, released.map(|_| ()))
    }
}

impl crate::workflows::Workflow for SplitPartition {
    type Context = SplitPartitionContext;
    type Output = ();

    fn key(&self, ctx: &Self::Context) -> String {
        format!("split:{}:{}", ctx.table_id, ctx.new_partition)
    }

    fn table_id(&self, ctx: &Self::Context) -> Option<i64> {
        Some(ctx.table_id)
    }

    async fn run(self, ctx: Self::Context) -> Result<Self::Output> {
        SplitPartition::run(&self, ctx).await
    }
}
