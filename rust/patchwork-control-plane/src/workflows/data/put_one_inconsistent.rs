use anyhow::{Context, Result, ensure};
use kameo::actor::ActorRef;
use rand::seq::SliceRandom;
use serde_json::Value;

use crate::{
    dataplane_client::Client,
    db::{
        NodeActor, PartitionActor, TableActor, actor::GetCached as Get,
        partition::GetPartitionAtOrBeforeCached as GetPartitionAtOrBefore,
    },
};

pub struct PutOneInconsistent {
    pub partition: ActorRef<PartitionActor>,
    pub node: ActorRef<NodeActor>,
    pub table: ActorRef<TableActor>,
}

pub struct PutOneInconsistentCtx {
    pub table_id: i64,
    pub primary_key: i64,
    pub secondary_key: i64,
    pub data: Value,
    // Caller-supplied Unix microseconds; preserve on retries.
    pub timestamp: i64,
}

impl PutOneInconsistent {
    pub async fn run(&self, ctx: PutOneInconsistentCtx) -> Result<()> {
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

        let mut replicas = super::replicas::resolve(&self.partition, partition, true, true).await?;
        replicas.shuffle(&mut rand::rng());

        // Resolve every destination before starting the write.
        let mut urls = Vec::new();
        for replica in replicas {
            let node = self
                .node
                .ask(Get { id: replica })
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
        Client::put_with_replicas_inconsistent(
            url,
            ctx.table_id,
            ctx.primary_key,
            ctx.secondary_key,
            &ctx.data,
            replicas,
            ctx.timestamp,
        )
        .await?;
        Ok(())
    }
}
