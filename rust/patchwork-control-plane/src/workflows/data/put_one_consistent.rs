use crate::telemetry::{measure, time};
use anyhow::{Context, Result, ensure};
use kameo::actor::ActorRef;
use patchwork_common::dataplane_client::Record;
use rand::seq::SliceRandom;

use crate::{
    actors::{
        NodeActor, PartitionActor, TableActor,
        crud::{Get, GetCached},
        partition::GetPartitionAtOrBefore,
    },
    dataplane_client::Client,
};

pub struct PutOneConsistent {
    pub client: Client,
    pub partition: ActorRef<PartitionActor>,
    pub node: ActorRef<NodeActor>,
    pub table: ActorRef<TableActor>,
}

pub struct PutOneConsistentCtx {
    pub table_id: i64,
    pub primary_key: i64,
    pub secondary_key: i64,
    pub data: Vec<u8>,
    // Caller-supplied Unix microseconds; preserve on retries.
    pub timestamp: i64,
}

impl PutOneConsistent {
    pub async fn run(&self, ctx: PutOneConsistentCtx) -> Result<()> {
        ensure!(ctx.timestamp >= 0, "Timestamp must be nonnegative");
        let table_timing = measure("table_lookup");
        let table = self
            .table
            .ask(GetCached { id: ctx.table_id })
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
        drop(partition_timing);

        let replicas = time(
            "replica_resolution",
            super::replicas::resolve(&self.partition, partition, true, false),
        )
        .await?;
        Self::to_replicas(
            &self.client,
            &self.node,
            &Record {
                table_id: ctx.table_id,
                partition_key: ctx.primary_key,
                secondary_key: ctx.secondary_key,
                data: ctx.data,
                timestamp: ctx.timestamp,
                deleted: false,
            },
            &replicas,
        )
        .await
    }

    // Copying supplies destinations explicitly: they may not be published yet.
    // Preserve the source timestamp and deletion marker on every replica.
    pub(crate) async fn to_replicas(
        client: &Client,
        node_actor: &ActorRef<NodeActor>,
        record: &Record,
        replicas: &[i64],
    ) -> Result<()> {
        let mut replicas = replicas.to_vec();
        replicas.sort_unstable();
        replicas.dedup();
        replicas.shuffle(&mut rand::rng());

        let mut urls = Vec::new();
        for replica in replicas {
            let node = time("replica_lookup", node_actor.ask(Get { id: replica }))
                .await?
                .context("Replica node no longer exists")?;
            let url = node.url.trim_end_matches('/').to_owned();
            if !urls.contains(&url) {
                urls.push(url);
            }
        }
        let (url, replicas) = urls.split_first().context("Partition has no replicas")?;
        time(
            "replica_grpc",
            client.write_with_replicas(
                url,
                record.table_id,
                record.partition_key,
                record.secondary_key,
                &record.data,
                replicas,
                record.timestamp,
                record.deleted,
            ),
        )
        .await?;
        Ok(())
    }
}
