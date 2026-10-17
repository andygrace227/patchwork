use super::copy_range::copy_range;
use crate::workflows::data::fetch_range::FetchRangeCtx;
use crate::workflows::stages::{unlock, workflow_steps};
use crate::{actors::crud::Get, dataplane_client::Client};
use crate::{
    actors::{
        NodeActor, PartitionActor, TableActor,
        node::GetNodeExcluding,
        parition_lock::AttemptLock,
        partition::{
            CreateLockedPartition, GetTablePartitionsUncached as GetTablePartitions,
            UpdateLockedPartition,
        },
    },
    datamodels::partition,
    workflows::partition::circular_get,
};
use anyhow::{Context, Result};
use kameo::actor::ActorRef;
use sea_orm::ActiveValue::Set;

/// Split the existing partition containing `new_partition` at that hash boundary.
#[derive(Clone)]
pub struct SplitPartitionContext {
    pub table_id: i64,
    pub new_partition: i64,
}

#[derive(Clone)]
pub struct SplitPartition {
    pub client: Client,
    pub node: ActorRef<NodeActor>,
    pub partition: ActorRef<PartitionActor>,
    pub table: ActorRef<TableActor>,
}

mod steps;

struct Run {
    workflow: SplitPartition,
    ctx: SplitPartitionContext,
    locks: Vec<crate::datamodels::parition_lock::Model>,
    boundaries: Option<(partition::Model, partition::Model)>,
    keys: Vec<i64>,
    lease_end: i64,
    replicas: Vec<i64>,
    partition_id: String,
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
    Publish => publish,
    Copy => copy,
    Activate => activate,
    Cleanup => cleanup,
    Unlock => unlock,
}

impl SplitPartition {
    pub async fn run(&self, ctx: SplitPartitionContext) -> Result<()> {
        Run {
            workflow: self.clone(),
            ctx,
            locks: Vec::new(),
            boundaries: None,
            keys: Vec::new(),
            lease_end: 0,
            replicas: Vec::new(),
            partition_id: format!("{:032x}", rand::random::<u128>()),
        }
        .execute()
        .await
        .map(|_| ())
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
