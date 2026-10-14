use super::copy_range::copy_range;
use crate::workflows::data::fetch_range::FetchRangeCtx;
use crate::workflows::stages::{unlock, workflow_steps};
use anyhow::{Context, Result, ensure};
use kameo::actor::ActorRef;

use crate::{
    actors::{
        NodeActor, PartitionActor,
        crud::Get,
        parition_lock::AttemptLock,
        partition::{DeleteLockedPartition, GetTablePartitionsUncached as GetTablePartitions},
    },
    datamodels::partition::Model,
    dataplane_client::Client,
};

/// Remove this boundary, extending the previous partition through its range.
#[derive(Clone)]
pub struct MergePartitionContext {
    pub table_id: i64,
    pub hash_start: i64,
}

#[derive(Clone)]
pub struct MergePartition {
    pub client: Client,
    pub node: ActorRef<NodeActor>,
    pub partition: ActorRef<PartitionActor>,
}

mod steps;

struct Run {
    workflow: MergePartition,
    ctx: MergePartitionContext,
    locks: Vec<crate::datamodels::parition_lock::Model>,
    boundaries: Option<(Model, Model, Model)>,
    keys: Vec<i64>,
    lease_end: i64,
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

impl MergePartition {
    pub async fn run(&self, ctx: MergePartitionContext) -> Result<()> {
        Run {
            workflow: self.clone(),
            ctx,
            locks: Vec::new(),
            boundaries: None,
            keys: Vec::new(),
            lease_end: 0,
            obsolete: Vec::new(),
        }
        .execute()
        .await
        .map(|_| ())
    }
}

fn neighbors(partitions: &[Model], hash_start: i64) -> Result<(&Model, &Model, &Model)> {
    ensure!(partitions.len() > 1, "Cannot merge the last partition");
    let idx = partitions
        .binary_search_by_key(&hash_start, |p| p.hash_start)
        .map_err(|_| anyhow::anyhow!("Partition does not exist"))?;
    Ok((
        &partitions[(idx + partitions.len() - 1) % partitions.len()],
        &partitions[idx],
        &partitions[(idx + 1) % partitions.len()],
    ))
}

fn check_lease(lease_end: i64) -> Result<()> {
    ensure!(now()? < lease_end, "Merge lease expired");
    Ok(())
}

fn now() -> Result<i64> {
    Ok(std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_secs() as i64)
}

#[cfg(test)]
mod tests;

impl crate::workflows::Workflow for MergePartition {
    type Context = MergePartitionContext;
    type Output = ();

    fn key(&self, ctx: &Self::Context) -> String {
        format!("merge:{}:{}", ctx.table_id, ctx.hash_start)
    }

    fn table_id(&self, ctx: &Self::Context) -> Option<i64> {
        Some(ctx.table_id)
    }

    async fn run(self, ctx: Self::Context) -> Result<Self::Output> {
        MergePartition::run(&self, ctx).await
    }
}
