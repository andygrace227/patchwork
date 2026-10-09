use super::copy_range::copy_range;
use crate::workflows::data::fetch_range::FetchRangeCtx;
use crate::workflows::stages::{RunState, finish, task_error, unlock};
use anyhow::{Context, Result, ensure};
use cano::{
    CancellationToken, CanoError, Resources, TaskConfig, TaskResult, Workflow as CanoWorkflow,
};
use kameo::actor::ActorRef;
use sea_orm::ActiveValue::Set;
use tokio::sync::Mutex;

use crate::{
    dataplane_client::Client,
    db::{
        NodeActor, PartitionActor,
        actor::Get,
        parition_lock::AttemptLock,
        partition::{self, GetTablePartitions, UpdateLockedPartition},
    },
};

#[derive(Clone)]
pub struct DecreaseReplicationContext {
    pub table_id: i64,
    pub hash_start: i64,
    /// Desired total number of copies.
    pub replication_factor: usize,
}

#[derive(Clone)]
pub struct DecreaseReplication {
    pub node: ActorRef<NodeActor>,
    pub partition: ActorRef<PartitionActor>,
}

mod steps;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum State {
    Prepare,
    Lock,
    SelectNodes,
    Copy,
    Metadata,
    Cleanup,
    Unlock,
    Done,
}

struct Run {
    workflow: DecreaseReplication,
    ctx: DecreaseReplicationContext,
    locks: Vec<crate::db::parition_lock::Model>,
    boundaries: Option<(partition::Model, partition::Model)>,
    keys: Vec<i64>,
    lease_end: i64,
    replicas: Vec<i64>,
    obsolete: Vec<crate::db::node::Model>,
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
            Self::Copy => run.copy().await,
            Self::Metadata => run.metadata().await,
            Self::Cleanup => run.cleanup().await,
            Self::Unlock => run.unlock().await,
            Self::Done => Ok(State::Done),
        }
        .map_err(task_error)?;
        Ok(TaskResult::Single(next))
    }
}

impl DecreaseReplication {
    pub async fn run(&self, ctx: DecreaseReplicationContext) -> Result<()> {
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
                obsolete: Vec::new(),
            })),
        );
        let run = resources.get::<RunState<Run>, _>("run")?;
        let workflow = CanoWorkflow::new(resources)
            .register(State::Prepare, State::Prepare)
            .register(State::Lock, State::Lock)
            .register(State::SelectNodes, State::SelectNodes)
            .register(State::Copy, State::Copy)
            .register(State::Metadata, State::Metadata)
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

fn boundaries(
    partitions: &[partition::Model],
    hash_start: i64,
) -> Result<(&partition::Model, &partition::Model)> {
    let idx = partitions
        .binary_search_by_key(&hash_start, |p| p.hash_start)
        .map_err(|_| anyhow::anyhow!("Partition does not exist"))?;
    Ok((&partitions[idx], &partitions[(idx + 1) % partitions.len()]))
}

fn now() -> Result<i64> {
    Ok(std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_secs() as i64)
}

fn check_lease(lease_end: i64) -> Result<()> {
    ensure!(now()? < lease_end, "Replication lease expired");
    Ok(())
}

// Preserve replica order; no node has special status.
fn retained_replicas(source: &partition::Model, count: usize) -> Vec<i64> {
    source.replicas.0.iter().copied().take(count).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decrease_keeps_requested_number_of_replicas() {
        let source = partition::Model {
            table_id: 1,
            hash_start: 10,

            forward_to: None,
            replicas: partition::ReplicaNodes(vec![1, 2, 3]),
        };
        assert_eq!(retained_replicas(&source, 1), vec![1]);
        assert_eq!(retained_replicas(&source, 2), vec![1, 2]);
        assert_eq!(retained_replicas(&source, 3), vec![1, 2, 3]);
        assert!(boundaries(&[], 10).is_err());
    }
}

impl crate::workflows::Workflow for DecreaseReplication {
    type Context = DecreaseReplicationContext;
    type Output = ();

    fn key(&self, ctx: &Self::Context) -> String {
        format!(
            "decrease:{}:{}:{}",
            ctx.table_id, ctx.hash_start, ctx.replication_factor
        )
    }

    fn table_id(&self, ctx: &Self::Context) -> Option<i64> {
        Some(ctx.table_id)
    }

    async fn run(self, ctx: Self::Context) -> Result<Self::Output> {
        DecreaseReplication::run(&self, ctx).await
    }
}
