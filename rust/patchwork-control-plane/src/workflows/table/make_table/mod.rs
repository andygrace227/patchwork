use crate::workflows::stages::{RunState, finish, task_error, unlock};
use anyhow::{Context, Result, ensure};
use cano::{
    CancellationToken, CanoError, Resources, TaskConfig, TaskResult, Workflow as CanoWorkflow,
};
use kameo::actor::ActorRef;
use sea_orm::ActiveValue::Set;
use tokio::sync::Mutex;

use crate::db::{
    NodeActor, PartitionActor, TableActor,
    actor::{Create, Update},
    node::GetRandomNodes,
    parition_lock::AttemptLock,
    partition::{self, CreateLockedPartition},
    table,
};

#[derive(Clone)]
pub struct MakeTableContext {
    pub table_name: String,
    pub table_owner: i64,
    pub partition_key_name: String,
    pub sort_key_name: String,
}

#[derive(Clone)]
pub struct MakeTable {
    pub node: ActorRef<NodeActor>,
    pub partition: ActorRef<PartitionActor>,
    pub table: ActorRef<TableActor>,
}

// Divide the full signed 64-bit ring into thirds without overflowing i64.
fn initial_partitions(table_id: i64, nodes: [i64; 3]) -> Vec<partition::ActiveModel> {
    (0..3)
        .map(|idx| {
            let hash_start = (i64::MIN as i128 + (1_i128 << 64) * idx as i128 / 3) as i64;
            let replicas = (0..3).map(|offset| nodes[(idx + offset) % 3]).collect();
            partition::ActiveModel {
                table_id: Set(table_id),
                hash_start: Set(hash_start),
                forward_to: Set(None),
                replicas: Set(partition::ReplicaNodes(replicas)),
            }
        })
        .collect()
}

mod steps;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum State {
    Validate,
    Create,
    Lock,
    Publish,
    Unlock,
    Ready,
    Done,
}

struct Run {
    workflow: MakeTable,
    ctx: MakeTableContext,
    locks: Vec<crate::db::parition_lock::Model>,
    nodes: [i64; 3],
    partitions: Vec<partition::ActiveModel>,
    created: Option<table::Model>,
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
            Self::Validate => run.validate().await,
            Self::Create => run.create().await,
            Self::Lock => run.lock().await,
            Self::Publish => run.publish().await,
            Self::Unlock => run.unlock().await,
            Self::Ready => run.ready().await,
            Self::Done => Ok(State::Done),
        }
        .map_err(task_error)?;
        Ok(TaskResult::Single(next))
    }
}

impl MakeTable {
    pub async fn run(&self, ctx: MakeTableContext) -> Result<table::Model> {
        let resources = Resources::new().insert(
            "run",
            RunState(Mutex::new(Run {
                workflow: self.clone(),
                ctx,
                locks: Vec::new(),
                nodes: [0; 3],
                partitions: Vec::new(),
                created: None,
            })),
        );
        let run = resources.get::<RunState<Run>, _>("run")?;
        let workflow = CanoWorkflow::new(resources)
            .register(State::Validate, State::Validate)
            .register(State::Create, State::Create)
            .register(State::Lock, State::Lock)
            .register(State::Publish, State::Publish)
            .register(State::Unlock, State::Unlock)
            .register(State::Ready, State::Ready)
            .add_exit_state(State::Done);
        let result = workflow
            .orchestrate(State::Validate, CancellationToken::disabled())
            .await
            .map(|_| ())
            .map_err(anyhow::Error::from);

        // Cano reports step failures and panics. Always release partially acquired locks.
        let mut run = run.0.lock().await;
        let released = run.unlock().await;
        finish(result, released.map(|_| ()))?;
        run.created
            .clone()
            .context("Table creation did not produce a table")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn initial_ring_has_even_ranges_and_three_copies() {
        let partitions = initial_partitions(42, [7, 11, 19]);
        let starts: Vec<_> = partitions
            .iter()
            .map(|p| *p.hash_start.as_ref() as i128)
            .collect();
        let widths = [
            starts[1] - starts[0],
            starts[2] - starts[1],
            (1_i128 << 64) + starts[0] - starts[2],
        ];
        assert_eq!(starts[0], i64::MIN as i128);
        assert!(widths.iter().max().unwrap() - widths.iter().min().unwrap() <= 1);
        assert_eq!(widths.iter().sum::<i128>(), 1_i128 << 64);
        for p in &partitions {
            assert_eq!(*p.table_id.as_ref(), 42);
            let mut replicas = p.replicas.as_ref().0.clone();
            replicas.sort_unstable();
            assert_eq!(replicas, vec![7, 11, 19]);
            assert_eq!(*p.forward_to.as_ref(), None);
        }
    }
}

impl crate::workflows::Workflow for MakeTable {
    type Context = MakeTableContext;
    type Output = table::Model;

    fn key(&self, ctx: &Self::Context) -> String {
        format!("make_table:{}:{}", ctx.table_owner, ctx.table_name)
    }

    fn table_id(&self, _ctx: &Self::Context) -> Option<i64> {
        None
    }

    async fn run(self, ctx: Self::Context) -> Result<Self::Output> {
        MakeTable::run(&self, ctx).await
    }
}
