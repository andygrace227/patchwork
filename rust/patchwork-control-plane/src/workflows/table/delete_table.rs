use anyhow::{Context, Result, ensure};
use kameo::actor::ActorRef;

use crate::{
    client::Client,
    db::{
        NodeActor, PartitionActor, TableActor,
        actor::{Delete, Get, Update},
        parition_lock::{AttemptLock, FreeLock},
        partition::{DeleteLockedPartition, GetTablePartitions},
        table,
    },
};

pub struct DeleteTableContext {
    pub table_id: i64,
}

pub struct DeleteTable {
    pub node: ActorRef<NodeActor>,
    pub partition: ActorRef<PartitionActor>,
    pub table: ActorRef<TableActor>,
}

impl DeleteTable {
    pub async fn run(&self, ctx: DeleteTableContext) -> Result<()> {
        // Stage 1: Mark the table unavailable before removing its records.
        // An already absent table needs no further work.
        let Some(mut table) = self.table.ask(Get { id: ctx.table_id }).await? else {
            return Ok(());
        };
        table.is_ready = false;
        self.table
            .ask(Update::<table::Entity> { data: table })
            .await?;

        let partitions = self
            .partition
            .ask(GetTablePartitions {
                table_id: ctx.table_id,
            })
            .await?;
        let mut locks = Vec::new();
        // Keep failures inside the block so every acquired lock reaches the unlocks.
        let result: Result<()> = async {
            // Stage 2: Lock every partition in hash order before deleting any data.
            // Holding all boundaries prevents splits and merges from moving the ranges.
            let lease_end = now()? + 300;
            for partition in &partitions {
                locks.push(
                    self.partition
                        .ask(AttemptLock {
                            table_id: ctx.table_id,
                            hash_start: partition.hash_start,
                            lease_end,
                        })
                        .await?
                        .context("Partition is already locked; retry deleting the table")?,
                );
            }
            let current = self
                .partition
                .ask(GetTablePartitions {
                    table_id: ctx.table_id,
                })
                .await?;
            ensure!(
                current == partitions,
                "Partition ring changed; retry deleting the table"
            );

            // Stage 3: Find every node holding this table, including bootstrap sources.
            // A node may hold several partitions. Delete this table there only once.
            let mut node_ids = Vec::new();
            for partition in &partitions {
                node_ids.push(partition.node_id);
                node_ids.extend(&partition.replicas.0);
                if partition.forward_to != 0 {
                    node_ids.push(partition.forward_to);
                }
            }
            node_ids.sort_unstable();
            node_ids.dedup();
            let mut nodes = Vec::new();
            for id in node_ids {
                nodes.push(
                    self.node
                        .ask(Get { id })
                        .await?
                        .context("Replica node no longer exists; table cleanup is incomplete")?,
                );
            }

            // Stage 4: Remove the table's records from every replica.
            // A table-scoped delete covers the whole ring and leaves other tables alone.
            // Keep all partition rows until every node succeeds, so a retry can find them.
            for node in nodes {
                ensure!(
                    now()? < lease_end,
                    "Table deletion lease expired during cleanup"
                );
                Client::delete_table(&node.url, ctx.table_id).await?;
            }

            // Stage 5: Remove the empty partitions while their locks are still held.
            // Deletion checks every lease. Only then remove the table entry itself.
            for partition in &partitions {
                self.partition
                    .ask(DeleteLockedPartition {
                        table_id: ctx.table_id,
                        hash_start: partition.hash_start,
                        locks: locks.clone(),
                    })
                    .await?;
            }
            self.table.ask(Delete { id: ctx.table_id }).await?;
            Ok(())
        }
        .await;

        // Stage 6: Unlock everything, even if a node failed during cleanup.
        // Release only our own locks, in reverse order, and try every unlock.
        let mut unlock_errors = Vec::new();
        for lock in locks.into_iter().rev() {
            if let Err(error) = self
                .partition
                .ask(FreeLock {
                    table_id: lock.table_id,
                    hash_start: lock.hash_start,
                    lock_token: lock.lock_token,
                })
                .await
            {
                unlock_errors.push(error.to_string());
            }
        }
        if !unlock_errors.is_empty() {
            anyhow::bail!(
                "Table deletion result: {result:?}; unlock failures: {}",
                unlock_errors.join("; ")
            );
        }
        result.with_context(|| {
            format!(
                "Deleting table {} failed; retry to finish cleanup",
                ctx.table_id
            )
        })
    }
}

fn now() -> Result<i64> {
    Ok(std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_secs() as i64)
}
