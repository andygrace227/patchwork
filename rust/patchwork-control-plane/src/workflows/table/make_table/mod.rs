use crate::workflows::stages::{unlock, workflow_steps};
use anyhow::{Context, Result, ensure};
use kameo::actor::ActorRef;
use kameo_actors::pool::ActorPool;
use sea_orm::ActiveValue::Set;

use crate::{
    actors::{
        NodeActor, PartitionActor, TableActor, node::GetRandomNodes, parition_lock::AttemptLock,
        partition::CreateLockedPartition,
    },
    datamodels::{partition, table},
};

#[derive(Clone)]
pub struct MakeTableContext {
    pub table_name: String,
    pub table_owner: i64,
    pub partition_key_name: String,
    pub sort_key_name: String,
    pub secret_key_id: Option<i64>,
}

#[derive(Clone)]
pub struct MakeTable {
    pub node: ActorRef<ActorPool<NodeActor>>,
    pub partition: ActorRef<ActorPool<PartitionActor>>,
    pub table: ActorRef<ActorPool<TableActor>>,
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
                partition_id: Set(format!("{:032x}", rand::random::<u128>())),
                forward_to: Set(None),
                replicas: Set(partition::ReplicaNodes(replicas)),
            }
        })
        .collect()
}

mod steps;

struct Run {
    workflow: MakeTable,
    ctx: MakeTableContext,
    locks: Vec<crate::datamodels::parition_lock::Model>,
    nodes: [i64; 3],
    partitions: Vec<partition::ActiveModel>,
    created: Option<table::Model>,
}

workflow_steps! {
    run: Run,
    state: State,
    start: Validate,
    done: Done,
    cleanup: unlock,
    Validate => validate,
    Create => create,
    Lock => lock,
    Publish => publish,
    Unlock => unlock,
    Ready => ready,
}

impl MakeTable {
    pub async fn run(&self, ctx: MakeTableContext) -> Result<table::Model> {
        let run = Run {
            workflow: self.clone(),
            ctx,
            locks: Vec::new(),
            nodes: [0; 3],
            partitions: Vec::new(),
            created: None,
        }
        .execute()
        .await?;
        let created = run.0.lock().await.created.clone();
        created.context("Table creation did not produce a table")
    }
}

#[cfg(test)]
mod tests;

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
