use anyhow::{Context, Result, ensure};
use kameo::actor::ActorRef;
use patchwork_common::client::Record;
use rand::seq::IndexedRandom;

use crate::{
    client::Client,
    db::{NodeActor, PartitionActor, TableActor, actor::Get, partition::GetTablePartitions},
};

pub struct FetchRange {
    pub partition: ActorRef<PartitionActor>,
    pub node: ActorRef<NodeActor>,
    pub table: ActorRef<TableActor>,
}

pub struct FetchRangeCtx {
    pub table_id: i64,
    pub lower_bound: i64,
    pub upper_bound: i64,
}

impl FetchRange {
    // Scan [lower_bound, upper_bound), using one replica per partition.
    // Results are not a consistent snapshot across partitions or replicas.
    pub async fn run(&self, ctx: FetchRangeCtx) -> Result<Vec<Record>> {
        ensure!(
            ctx.lower_bound <= ctx.upper_bound,
            "Range bounds are reversed"
        );
        let table = self
            .table
            .ask(Get { id: ctx.table_id })
            .await?
            .context("Table is not alive.")?;
        ensure!(table.is_ready, "Table is not ready.");
        if ctx.lower_bound == ctx.upper_bound {
            return Ok(Vec::new());
        }
        let partitions = self
            .partition
            .ask(GetTablePartitions {
                table_id: ctx.table_id,
            })
            .await?;
        ensure!(!partitions.is_empty(), "Table has no partitions");

        let mut records = Vec::new();
        let mut lower_bound = ctx.lower_bound;
        while lower_bound < ctx.upper_bound {
            let next = partitions.partition_point(|partition| partition.hash_start <= lower_bound);
            // Keys before the first boundary belong to the last partition.
            let partition = &partitions[if next == 0 {
                partitions.len() - 1
            } else {
                next - 1
            }];
            let upper_bound = partitions.get(next).map_or(ctx.upper_bound, |partition| {
                partition.hash_start.min(ctx.upper_bound)
            });
            let replicas =
                super::replicas::resolve(&self.partition, partition.clone(), false, false).await?;
            let replica = *replicas
                .choose(&mut rand::rng())
                .context("Partition has no replicas")?;
            let node = self
                .node
                .ask(Get { id: replica })
                .await?
                .context("Replica node no longer exists")?;
            records.extend(
                Client::get_range(&node.url, ctx.table_id, upper_bound, lower_bound).await?,
            );
            lower_bound = upper_bound;
        }
        Ok(records)
    }

    // Copy scans visit every source replica, including tombstones. Duplicate
    // versions are intentional: consistent writes apply timestamp/tie ordering.
    pub(crate) async fn from_replicas(
        node_actor: &ActorRef<NodeActor>,
        ctx: &FetchRangeCtx,
        replicas: &[i64],
    ) -> Result<Vec<Record>> {
        ensure!(
            ctx.lower_bound < ctx.upper_bound,
            "Copy range must be nonempty and not wrap"
        );
        ensure!(!replicas.is_empty(), "Partition has no replicas");
        let mut replicas = replicas.to_vec();
        replicas.sort_unstable();
        replicas.dedup();
        let mut records = Vec::new();
        for replica in replicas {
            let node = node_actor
                .ask(Get { id: replica })
                .await?
                .context("Source replica no longer exists")?;
            records.extend(
                Client::get_range_including_deleted(
                    &node.url,
                    ctx.table_id,
                    ctx.upper_bound,
                    ctx.lower_bound,
                )
                .await?,
            );
        }
        Ok(records)
    }
}
