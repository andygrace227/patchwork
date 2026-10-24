use crate::clients::{NodeClient, PartitionClient, TableClient};
use crate::telemetry::{measure, time};
use anyhow::{Context, Result, ensure};
use rand::seq::SliceRandom;

use crate::dataplane_client::Client;

pub struct PutOneInconsistent {
    pub client: Client,
    pub partition: PartitionClient,
    pub node: NodeClient,
    pub table: TableClient,
}

pub struct PutOneInconsistentCtx {
    pub table_id: i64,
    pub primary_key: i64,
    pub secondary_key: i64,
    pub data: Vec<u8>,
    // Caller-supplied Unix microseconds; preserve on retries.
    pub timestamp: i64,
}

impl PutOneInconsistent {
    pub async fn run(&self, ctx: PutOneInconsistentCtx) -> Result<()> {
        ensure!(ctx.timestamp >= 0, "Timestamp must be nonnegative");
        let table_timing = measure("table_lookup");
        let table = self
            .table
            .get_table(ctx.table_id)
            .await?
            .context("Table is not alive.")?;
        ensure!(table.is_ready, "Table is not ready.");
        drop(table_timing);

        let partition_timing = measure("partition_lookup");
        let mut partition = self
            .partition
            .get_partition_at_or_before_cached(ctx.table_id, ctx.primary_key)
            .await?;
        // Keys before the first boundary wrap to the last partition.
        if partition.is_none() {
            partition = self
                .partition
                .get_partition_at_or_before_cached(ctx.table_id, i64::MAX)
                .await?;
        }
        let partition = partition.context("Table has no partitions")?;
        let partition_id = partition.partition_id.clone();
        drop(partition_timing);

        let replica_timing = measure("replica_resolution");
        let mut replicas = partition.replicas.0;
        ensure!(!replicas.is_empty(), "Partition has no replicas");
        // During a split, writes go to both the new partition and its source.
        if let Some(hash_start) = partition.forward_to {
            let source = self
                .partition
                .get_partition_cached((partition.table_id, hash_start))
                .await?
                .context("Forwarded partition no longer exists")?;
            ensure!(!source.replicas.0.is_empty(), "Partition has no replicas");
            for replica in source.replicas.0 {
                if !replicas.contains(&replica) {
                    replicas.push(replica);
                }
            }
        }
        drop(replica_timing);
        replicas.shuffle(&mut rand::rng());

        // Resolve every destination before starting the write.
        let mut urls = Vec::new();
        for replica in replicas {
            let node = time("replica_lookup", self.node.get_node(replica))
                .await?
                .context("Replica node no longer exists")?;
            let url = node.url.trim_end_matches('/').to_owned();
            if !urls.contains(&url) {
                urls.push(url);
            }
        }
        let (url, replicas) = urls.split_first().context("Partition has no replicas")?;
        ensure!(
            !replicas.is_empty(),
            "Inconsistent writes require at least two nodes"
        );
        time(
            "replica_grpc",
            self.client.write_partition_record(
                url,
                &patchwork_common::dataplane_client::Record {
                    table_id: ctx.table_id,
                    partition_id: Some(partition_id),
                    partition_key: ctx.primary_key,
                    secondary_key: ctx.secondary_key,
                    data: ctx.data,
                    timestamp: ctx.timestamp,
                    deleted: false,
                },
                replicas,
                false,
            ),
        )
        .await?;
        Ok(())
    }
}
