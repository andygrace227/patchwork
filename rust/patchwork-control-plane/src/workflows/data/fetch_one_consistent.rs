use crate::actors::{node::GetNode, table::GetTableCached};
use crate::telemetry::{measure, time};
use anyhow::{Context, Result, ensure};
use kameo::actor::ActorRef;
use kameo_actors::pool::{ActorPool, Dispatch};
use patchwork_common::dataplane_client::Record;
use rand::seq::SliceRandom;

use crate::{
    actors::{NodeActor, PartitionActor, TableActor, partition::GetPartitionAtOrBefore},
    dataplane_client::Client,
};
// Fetch one and take the one with the latest timestamp.
// GetOneConsistent always hits the live partition map.
pub struct GetOneConsistent {
    pub client: Client,
    pub partition: ActorRef<ActorPool<PartitionActor>>,
    pub node: ActorRef<ActorPool<NodeActor>>,
    pub table: ActorRef<ActorPool<TableActor>>,
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
            .ask(Dispatch(GetTableCached { id: ctx.table_id }))
            .await?
            .context("Table is not alive.")?;
        ensure!(table.is_ready, "Table is not ready.");
        drop(table_timing);

        let partition_timing = measure("partition_lookup");
        let partition = self
            .partition
            .ask(Dispatch(GetPartitionAtOrBefore {
                table_id: ctx.table_id,
                hash_start: ctx.primary_key,
            }))
            .await?
            .context("Something is very wrong with partition creation")?;
        drop(partition_timing);

        let mut replicas = time(
            "replica_resolution",
            super::replicas::resolve(&self.partition, partition, false, false),
        )
        .await?;
        let mut accepted_record: Option<Record> = None;
        replicas.shuffle(&mut rand::rng());
        for replica in replicas {
            let node = time(
                "replica_lookup",
                self.node.ask(Dispatch(GetNode { id: replica })),
            )
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
