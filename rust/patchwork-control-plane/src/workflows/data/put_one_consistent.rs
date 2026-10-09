use anyhow::{Context, Result, ensure};
use kameo::actor::ActorRef;
use patchwork_common::dataplane_client::Record;
use rand::seq::SliceRandom;
use serde_json::Value;

use crate::{
    dataplane_client::Client,
    db::{NodeActor, PartitionActor, TableActor, actor::Get, partition::GetPartitionAtOrBefore},
};

pub struct PutOneConsistent {
    pub partition: ActorRef<PartitionActor>,
    pub node: ActorRef<NodeActor>,
    pub table: ActorRef<TableActor>,
}

pub struct PutOneConsistentCtx {
    pub table_id: i64,
    pub primary_key: i64,
    pub secondary_key: i64,
    pub data: Value,
    // Caller-supplied Unix microseconds; preserve on retries.
    pub timestamp: i64,
}

impl PutOneConsistent {
    pub async fn run(&self, ctx: PutOneConsistentCtx) -> Result<()> {
        ensure!(ctx.timestamp >= 0, "Timestamp must be nonnegative");
        let table = self
            .table
            .ask(Get { id: ctx.table_id })
            .await?
            .context("Table is not alive.")?;
        ensure!(table.is_ready, "Table is not ready.");

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

        let replicas = super::replicas::resolve(&self.partition, partition, true, false).await?;
        Self::to_replicas(
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
            let node = node_actor
                .ask(Get { id: replica })
                .await?
                .context("Replica node no longer exists")?;
            let url = node.url.trim_end_matches('/').to_owned();
            if !urls.contains(&url) {
                urls.push(url);
            }
        }
        let (url, replicas) = urls.split_first().context("Partition has no replicas")?;
        Client::write_with_replicas(
            url,
            record.table_id,
            record.partition_key,
            record.secondary_key,
            &record.data,
            replicas,
            record.timestamp,
            record.deleted,
        )
        .await?;
        Ok(())
    }
}
