use crate::clients::{NodeClient, PartitionClient, TableClient};
use crate::telemetry::{measure, time};
use anyhow::{Context, Result, ensure};
use patchwork_common::dataplane_client::Record;
use rand::seq::IndexedRandom;

use crate::dataplane_client::Client;

pub struct FetchRange {
    pub client: Client,
    pub partition: PartitionClient,
    pub node: NodeClient,
    pub table: TableClient,
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
        let table_timing = measure("table_lookup");
        let table = self
            .table
            .get_table(ctx.table_id)
            .await?
            .context("Table is not alive.")?;
        ensure!(table.is_ready, "Table is not ready.");
        drop(table_timing);
        if ctx.lower_bound == ctx.upper_bound {
            return Ok(Vec::new());
        }
        let partition_timing = measure("partition_lookup");
        let partitions = self
            .partition
            .get_table_partitions_cached(ctx.table_id)
            .await?;
        ensure!(!partitions.is_empty(), "Table has no partitions");
        drop(partition_timing);

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
            let replica_timing = measure("replica_resolution");
            // During a split, reads use the source until copying finishes.
            let replicas = if let Some(hash_start) = partition.forward_to {
                let source = self
                    .partition
                    .get_partition((partition.table_id, hash_start))
                    .await?
                    .context("Forwarded partition no longer exists")?;
                source.replicas.0
            } else {
                partition.replicas.0.clone()
            };
            drop(replica_timing);
            let replica = *replicas
                .choose(&mut rand::rng())
                .context("Partition has no replicas")?;
            let node = time("replica_lookup", self.node.get_node(replica))
                .await?
                .context("Replica node no longer exists")?;
            records.extend(
                time(
                    "replica_grpc",
                    self.client
                        .get_range(&node.url, ctx.table_id, upper_bound, lower_bound),
                )
                .await?,
            );
            lower_bound = upper_bound;
        }
        Ok(records)
    }

    // Copy scans visit every source replica, including tombstones. Duplicate
    // versions are intentional: consistent writes apply timestamp/tie ordering.
    pub(crate) async fn from_replicas(
        client: &Client,
        node_actor: &NodeClient,
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
            let node = time("replica_lookup", node_actor.get_node(replica))
                .await?
                .context("Source replica no longer exists")?;
            records.extend(
                client
                    .get_range_including_deleted(
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
