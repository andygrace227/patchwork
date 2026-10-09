use crate::workflows::stages::{RunState, finish, task_error, unlock};
use anyhow::{Context, Result, ensure};
use cano::{
    CancellationToken, CanoError, Resources, TaskConfig, TaskResult, Workflow as CanoWorkflow,
};
use kameo::actor::ActorRef;
use tokio::sync::Mutex;

use crate::{
    dataplane_client::Client,
    db::{
        NodeActor, PartitionActor, TableActor,
        actor::{Delete, Get, Update},
        parition_lock::AttemptLock,
        partition::{DeleteLockedPartition, GetTablePartitions},
        table,
    },
};

#[derive(Clone)]
pub struct DeleteTableContext {
    pub table_id: i64,
}

#[derive(Clone)]
pub struct DeleteTable {
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
    DeleteData,
    Metadata,
    Unlock,
    Done,
}

struct Run {
    workflow: DeleteTable,
    ctx: DeleteTableContext,
    locks: Vec<crate::db::parition_lock::Model>,
    partitions: Vec<crate::db::partition::Model>,
    nodes: Vec<crate::db::node::Model>,
    lease_end: i64,
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
            Self::DeleteData => run.delete_data().await,
            Self::Metadata => run.metadata().await,
            Self::Unlock => run.unlock().await,
            Self::Done => Ok(State::Done),
        }
        .map_err(task_error)?;
        Ok(TaskResult::Single(next))
    }
}

impl DeleteTable {
    pub async fn run(&self, ctx: DeleteTableContext) -> Result<()> {
        let resources = Resources::new().insert(
            "run",
            RunState(Mutex::new(Run {
                workflow: self.clone(),
                ctx,
                locks: Vec::new(),
                partitions: Vec::new(),
                nodes: Vec::new(),
                lease_end: 0,
            })),
        );
        let run = resources.get::<RunState<Run>, _>("run")?;
        let workflow = CanoWorkflow::new(resources)
            .register(State::Prepare, State::Prepare)
            .register(State::Lock, State::Lock)
            .register(State::SelectNodes, State::SelectNodes)
            .register(State::DeleteData, State::DeleteData)
            .register(State::Metadata, State::Metadata)
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

fn now() -> Result<i64> {
    Ok(std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_secs() as i64)
}

impl crate::workflows::Workflow for DeleteTable {
    type Context = DeleteTableContext;
    type Output = ();

    fn key(&self, ctx: &Self::Context) -> String {
        format!("delete_table:{}", ctx.table_id)
    }

    fn table_id(&self, ctx: &Self::Context) -> Option<i64> {
        Some(ctx.table_id)
    }

    async fn run(self, ctx: Self::Context) -> Result<Self::Output> {
        DeleteTable::run(&self, ctx).await
    }
}
