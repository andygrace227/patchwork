use super::copy_range::copy_range;
use crate::workflows::data::fetch_range::FetchRangeCtx;
use crate::workflows::stages::{unlock, workflow_steps};
use anyhow::{Context, Result, ensure};
use kameo::actor::ActorRef;
use kameo_actors::pool::ActorPool;
use sea_orm::ActiveValue::Set;

use crate::{
    actors::{
        NodeActor, PartitionActor,
        parition_lock::AttemptLock,
        partition::{GetTablePartitionsUncached as GetTablePartitions, UpdateLockedPartition},
    },
    datamodels::partition,
    dataplane_client::Client,
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
    pub client: Client,
    pub node: ActorRef<ActorPool<NodeActor>>,
    pub partition: ActorRef<ActorPool<PartitionActor>>,
}

mod steps;

struct Run {
    workflow: DecreaseReplication,
    ctx: DecreaseReplicationContext,
    locks: Vec<crate::datamodels::parition_lock::Model>,
    boundaries: Option<(partition::Model, partition::Model)>,
    keys: Vec<i64>,
    lease_end: i64,
    replicas: Vec<i64>,
    obsolete: Vec<crate::datamodels::node::Model>,
}

workflow_steps! {
    run: Run,
    state: State,
    start: Prepare,
    done: Done,
    cleanup: unlock,
    Prepare => prepare,
    Lock => lock,
    SelectNodes => select_nodes,
    Copy => copy,
    Metadata => metadata,
    Cleanup => cleanup,
    Unlock => unlock,
}

impl DecreaseReplication {
    pub async fn run(&self, ctx: DecreaseReplicationContext) -> Result<()> {
        Run {
            workflow: self.clone(),
            ctx,
            locks: Vec::new(),
            boundaries: None,
            keys: Vec::new(),
            lease_end: 0,
            replicas: Vec::new(),
            obsolete: Vec::new(),
        }
        .execute()
        .await
        .map(|_| ())
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
mod tests;

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
