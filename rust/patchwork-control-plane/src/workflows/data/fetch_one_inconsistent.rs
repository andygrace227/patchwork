use crate::telemetry::{measure, time};
use anyhow::{Context, Result, ensure};
use kameo::actor::ActorRef;
use patchwork_common::dataplane_client::Record;
use rand::seq::SliceRandom;

use crate::{
    dataplane_client::Client,
    db::{
        NodeActor, PartitionActor, TableActor,
        actor::{Get, GetCached},
        partition::GetPartitionAtOrBeforeCached as GetPartitionAtOrBefore,
    },
};
// Fetch the first record found, trying replicas in a random order.
// Inconsistent reads reuse the five-second partition cache.
pub struct GetOneInconsistent {
    pub partition: ActorRef<PartitionActor>,
    pub node: ActorRef<NodeActor>,
    pub table: ActorRef<TableActor>,
}

pub struct GetOneInconsistentCtx {
    pub table_id: i64,
    pub primary_key: i64,
    pub secondary_key: i64,
}

impl GetOneInconsistent {
    pub async fn run(&self, ctx: GetOneInconsistentCtx) -> Result<Option<Record>> {
        let table_timing = measure("table_lookup");
        let table = self
            .table
            .ask(GetCached { id: ctx.table_id })
            .await?
            .context("Table is not alive.")?;
        ensure!(table.is_ready, "Table is not ready.");
        drop(table_timing);

        let partition_timing = measure("partition_lookup");
        let partition = self
            .partition
            .ask(GetPartitionAtOrBefore {
                table_id: ctx.table_id,
                hash_start: ctx.primary_key,
            })
            .await?
            .context("Something is very wrong with partition creation")?;
        drop(partition_timing);

        let mut replicas = time(
            "replica_resolution",
            super::replicas::resolve(&self.partition, partition, false, true),
        )
        .await?;
        replicas.shuffle(&mut rand::rng());
        for replica in replicas {
            let Ok(Some(node)) = time("replica_lookup", self.node.ask(Get { id: replica })).await
            else {
                continue;
            };

            if let Ok(record) = time(
                "replica_grpc",
                Client::get_including_deleted(
                    &node.url,
                    ctx.table_id,
                    ctx.primary_key,
                    ctx.secondary_key,
                ),
            )
            .await
            {
                // A deletion marker is a result, not a reason to try an older copy.
                return Ok((!record.deleted).then_some(record));
            }
        }

        Ok(None)
    }
}
