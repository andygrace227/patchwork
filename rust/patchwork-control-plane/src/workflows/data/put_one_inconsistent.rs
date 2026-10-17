use crate::telemetry::{measure, time};
use anyhow::{Context, Result, ensure};
use kameo::actor::ActorRef;
use rand::seq::SliceRandom;

use crate::{
    actors::{
        NodeActor, PartitionActor, TableActor, crud::GetCached as Get,
        partition::GetPartitionAtOrBeforeCached as GetPartitionAtOrBefore,
    },
    dataplane_client::Client,
};

pub struct PutOneInconsistent {
    pub client: Client,
    pub partition: ActorRef<PartitionActor>,
    pub node: ActorRef<NodeActor>,
    pub table: ActorRef<TableActor>,
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
            .ask(Get { id: ctx.table_id })
            .await?
            .context("Table is not alive.")?;
        ensure!(table.is_ready, "Table is not ready.");
        drop(table_timing);

        let partition_timing = measure("partition_lookup");
        let mut partition = self
            .partition
            .ask(GetPartitionAtOrBefore {
                table_id: ctx.table_id,
                hash_start: ctx.primary_key,
            })
            .await?;
        // Keys before the first boundary wrap to the last partition.
        if partition.is_none() {
            partition = self
                .partition
                .ask(GetPartitionAtOrBefore {
                    table_id: ctx.table_id,
                    hash_start: i64::MAX,
                })
                .await?;
        }
        let partition = partition.context("Table has no partitions")?;
        let partition_id = partition.partition_id.clone();
        drop(partition_timing);

        let mut replicas = time(
            "replica_resolution",
            super::replicas::resolve(&self.partition, partition, true, true),
        )
        .await?;
        replicas.shuffle(&mut rand::rng());

        // Resolve every destination before starting the write.
        let mut urls = Vec::new();
        for replica in replicas {
            let node = time("replica_lookup", self.node.ask(Get { id: replica }))
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
