use super::copy_range::copy_range;
use crate::clients::{NodeClient, PartitionClient};
use crate::dataplane_client::Client;
use crate::workflows::data::fetch_range::FetchRangeCtx;
use crate::workflows::stages::{unlock, workflow_steps};
use anyhow::{Context, Result, ensure};
use sea_orm::ActiveValue::Set;

use crate::datamodels::partition;

#[derive(Clone)]
pub struct IncreaseReplicationContext {
    pub table_id: i64,
    pub hash_start: i64,
    /// Desired total number of copies.
    pub replication_factor: usize,
}

#[derive(Clone)]
pub struct IncreaseReplication {
    pub client: Client,
    pub node: NodeClient,
    pub partition: PartitionClient,
}

mod steps;

struct Run {
    workflow: IncreaseReplication,
    ctx: IncreaseReplicationContext,
    locks: Vec<crate::datamodels::parition_lock::Model>,
    boundaries: Option<(partition::Model, partition::Model)>,
    keys: Vec<i64>,
    lease_end: i64,
    replicas: Vec<i64>,
    targets: Vec<i64>,
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
    Unlock => unlock,
}

impl IncreaseReplication {
    pub async fn run(&self, ctx: IncreaseReplicationContext) -> Result<()> {
        Run {
            workflow: self.clone(),
            ctx,
            locks: Vec::new(),
            boundaries: None,
            keys: Vec::new(),
            lease_end: 0,
            replicas: Vec::new(),
            targets: Vec::new(),
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

impl crate::workflows::Workflow for IncreaseReplication {
    type Context = IncreaseReplicationContext;
    type Output = ();

    fn key(&self, ctx: &Self::Context) -> String {
        format!(
            "increase:{}:{}:{}",
            ctx.table_id, ctx.hash_start, ctx.replication_factor
        )
    }

    fn table_id(&self, ctx: &Self::Context) -> Option<i64> {
        Some(ctx.table_id)
    }

    async fn run(self, ctx: Self::Context) -> Result<Self::Output> {
        IncreaseReplication::run(&self, ctx).await
    }
}
