use anyhow::{Context, Result, ensure};
use kameo::actor::ActorRef;
use patchwork_common::client::Record;
use rand::seq::SliceRandom;

use crate::{
    client::Client,
    db::{NodeActor, PartitionActor, TableActor, actor::Get, partition::GetPartitionAtOrBefore},
};
// Fetch one and take the one with the latest timestamp.
// GetOneConsistent always hits the live partition map.
pub struct GetOneConsistent {
    pub partition: ActorRef<PartitionActor>,
    pub node: ActorRef<NodeActor>,
    pub table: ActorRef<TableActor>,
}

pub struct GetOneConsistentCtx {
    pub table_id: i64,
    pub primary_key: i64,
    pub secondary_key: i64,
}

impl GetOneConsistent {
    pub async fn run(&self, ctx: GetOneConsistentCtx) -> Result<Option<Record>> {
        let table = self
            .table
            .ask(Get { id: ctx.table_id })
            .await?
            .context("Table is not alive.")?;
        ensure!(table.is_ready, "Table is not ready.");

        let partition = self
            .partition
            .ask(GetPartitionAtOrBefore {
                table_id: ctx.table_id,
                hash_start: ctx.primary_key,
            })
            .await?
            .context("Something is very wrong with partition creation")?;

        let mut replicas =
            super::replicas::resolve(&self.partition, partition, false, false).await?;
        let mut accepted_record: Option<Record> = None;
        replicas.shuffle(&mut rand::rng());
        for replica in replicas {
            let node = self
                .node
                .ask(Get { id: replica })
                .await?
                .context("Replica node no longer exists; table cleanup is incomplete")?;

            // Include deletion markers so an older live copy cannot win.
            let rep_record = match Client::get_including_deleted(
                &node.url,
                ctx.table_id,
                ctx.primary_key,
                ctx.secondary_key,
            )
            .await
            {
                Ok(record) => record,
                Err(error) if error.status() == Some(reqwest::StatusCode::NOT_FOUND) => continue,
                Err(error) => return Err(error.into()),
            };

            // Use the same timestamp and tie ordering as shard writes.
            if accepted_record.as_ref().is_none_or(|accepted| {
                (rep_record.timestamp, rep_record.deleted) > (accepted.timestamp, accepted.deleted)
                    || ((rep_record.timestamp, rep_record.deleted)
                        == (accepted.timestamp, accepted.deleted)
                        && rep_record.data.to_string() > accepted.data.to_string())
            }) {
                accepted_record = Some(rep_record);
            }
        }

        Ok(accepted_record.filter(|record| !record.deleted))
    }
}
