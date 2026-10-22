use crate::clients::{NodeClient, PartitionClient, TableClient};
use crate::telemetry::{measure, time};
use anyhow::{Context, Result, ensure};
use patchwork_common::dataplane_client::Record;
use rand::seq::SliceRandom;

use crate::dataplane_client::Client;
// Fetch one and take the one with the latest timestamp.
// GetOneConsistent always hits the live partition map.
pub struct GetOneConsistent {
    pub client: Client,
    pub partition: PartitionClient,
    pub node: NodeClient,
    pub table: TableClient,
}

pub struct GetOneConsistentCtx {
    pub table_id: i64,
    pub primary_key: i64,
    pub secondary_key: i64,
}

impl GetOneConsistent {
    pub async fn run(&self, ctx: GetOneConsistentCtx) -> Result<Option<Record>> {
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
            .get_partition_at_or_before(ctx.table_id, ctx.primary_key)
            .await?
            .context("Something is very wrong with partition creation")?;
        drop(partition_timing);

        let mut replicas = time(
            "replica_resolution",
            super::replicas::resolve(&self.partition, partition, false),
        )
        .await?;
        let mut accepted_record: Option<Record> = None;
        replicas.shuffle(&mut rand::rng());
        for replica in replicas {
            let node = time("replica_lookup", self.node.get_node(replica))
                .await?
                .context("Replica node no longer exists; table cleanup is incomplete")?;

            // Include deletion markers so an older live copy cannot win.
            let rep_record = match time(
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
                Ok(record) => record,
                Err(error) if error.code() == tonic::Code::NotFound => continue,
                Err(error) => return Err(error.into()),
            };

            // Use the same timestamp and tie ordering as shard writes.
            if accepted_record.as_ref().is_none_or(|accepted| {
                (rep_record.timestamp, rep_record.deleted) > (accepted.timestamp, accepted.deleted)
                    || ((rep_record.timestamp, rep_record.deleted)
                        == (accepted.timestamp, accepted.deleted)
                        && rep_record.data > accepted.data)
            }) {
                accepted_record = Some(rep_record);
            }
        }

        Ok(accepted_record.filter(|record| !record.deleted))
    }
}
