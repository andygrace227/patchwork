use crate::clients::{NodeClient, PartitionClient, TableClient};
use crate::telemetry::{measure, time};
use anyhow::{Context, Result, ensure};
use patchwork_common::dataplane_client::Record;
use rand::seq::SliceRandom;

use crate::dataplane_client::Client;
// Fetch the first record found, trying replicas in a random order.
pub struct GetOneInconsistent {
    pub client: Client,
    pub partition: PartitionClient,
    pub node: NodeClient,
    pub table: TableClient,
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
            .get_table(ctx.table_id)
            .await?
            .context("Table is not alive.")?;
        ensure!(table.is_ready, "Table is not ready.");
        drop(table_timing);

        let partition_timing = measure("partition_lookup");
        let partition = self
            .partition
            .get_partition_at_or_before_cached(ctx.table_id, ctx.primary_key)
            .await?
            .context("Something is very wrong with partition creation")?;
        drop(partition_timing);

        let mut replicas = time(
            "replica_resolution",
            super::replicas::resolve_cached(&self.partition, partition, false),
        )
        .await?;
        replicas.shuffle(&mut rand::rng());
        for replica in replicas {
            let Ok(Some(node)) = time("replica_lookup", self.node.get_node(replica)).await else {
                continue;
            };

            if let Ok(record) = time(
                "replica_grpc",
                self.client.get_including_deleted(
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
