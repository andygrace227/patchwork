use crate::workflows::stages::{unlock, workflow_steps};
use anyhow::{Context, Result, ensure};
use kameo::actor::ActorRef;

use crate::{
    actors::{
        NodeActor, PartitionActor, TableActor,
        crud::{Delete, Get, Update},
        parition_lock::AttemptLock,
        partition::{DeleteLockedPartition, GetTablePartitionsUncached as GetTablePartitions},
    },
    datamodels::table,
    dataplane_client::Client,
};

#[derive(Clone)]
pub struct DeleteTableContext {
    pub table_id: i64,
}

#[derive(Clone)]
pub struct DeleteTable {
    pub client: Client,
    pub node: ActorRef<NodeActor>,
    pub partition: ActorRef<PartitionActor>,
    pub table: ActorRef<TableActor>,
}

mod steps;

struct Run {
    workflow: DeleteTable,
    ctx: DeleteTableContext,
    locks: Vec<crate::datamodels::parition_lock::Model>,
    partitions: Vec<crate::datamodels::partition::Model>,
    nodes: Vec<crate::datamodels::node::Model>,
    lease_end: i64,
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
    DeleteData => delete_data,
    Metadata => metadata,
    Unlock => unlock,
}

impl DeleteTable {
    pub async fn run(&self, ctx: DeleteTableContext) -> Result<()> {
        Run {
            workflow: self.clone(),
            ctx,
            locks: Vec::new(),
            partitions: Vec::new(),
            nodes: Vec::new(),
            lease_end: 0,
        }
        .execute()
        .await
        .map(|_| ())
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
