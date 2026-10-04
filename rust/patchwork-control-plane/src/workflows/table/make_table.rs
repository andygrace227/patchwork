use anyhow::{Context, Result, ensure};
use kameo::actor::ActorRef;
use sea_orm::ActiveValue::Set;

use crate::db::{
    NodeActor, PartitionActor, TableActor,
    actor::{Create, Update},
    node::GetRandomNodes,
    parition_lock::{AttemptLock, FreeLock},
    partition::{self, CreateLockedPartition},
    table,
};

pub struct MakeTableContext {
    pub table_name: String,
    pub table_owner: i64,
    pub partition_key_name: String,
    pub sort_key_name: String,
}

pub struct MakeTable {
    pub node: ActorRef<NodeActor>,
    pub partition: ActorRef<PartitionActor>,
    pub table: ActorRef<TableActor>,
}

// Divide the full signed 64-bit ring into thirds without overflowing i64.
fn initial_partitions(table_id: i64, nodes: [i64; 3]) -> Vec<partition::ActiveModel> {
    (0..3)
        .map(|idx| {
            let hash_start = (i64::MIN as i128 + (1_i128 << 64) * idx as i128 / 3) as i64;
            let replicas = (0..3).map(|offset| nodes[(idx + offset) % 3]).collect();
            partition::ActiveModel {
                table_id: Set(table_id),
                hash_start: Set(hash_start),
                node_id: Set(nodes[idx]),
                forward_to: Set(0),
                replicas: Set(partition::ReplicaNodes(replicas)),
            }
        })
        .collect()
}

impl MakeTable {
    pub async fn run(&self, ctx: MakeTableContext) -> Result<table::Model> {
        // Check capacity before creating anything. Three copies includes the root.
        ensure!(
            !ctx.table_name.trim().is_empty(),
            "Table name must not be empty"
        );
        ensure!(
            !ctx.partition_key_name.trim().is_empty(),
            "Partition key name must not be empty"
        );
        ensure!(
            !ctx.sort_key_name.trim().is_empty(),
            "Sort key name must not be empty"
        );
        let nodes = self.node.ask(GetRandomNodes { count: 3 }).await?;
        ensure!(
            nodes.len() == 3,
            "Creating a table requires three distinct data nodes"
        );

        // Stage 1: Create the table entry, but keep it unavailable until setup finishes.
        let mut table = self
            .table
            .ask(Create::<table::Entity> {
                data: table::ActiveModel {
                    table_name: Set(ctx.table_name),
                    owner: Set(ctx.table_owner),
                    partition_key_name: Set(ctx.partition_key_name),
                    sort_key_name: Set(ctx.sort_key_name),
                    is_ready: Set(false),
                    ..Default::default()
                },
            })
            .await?;
        let table_id = table.table_id;
        let partitions = initial_partitions(
            table_id,
            [nodes[0].node_id, nodes[1].node_id, nodes[2].node_id],
        );
        let mut locks = Vec::new();

        // Keep failures inside the block so we always attempt to release acquired locks.
        let result: Result<()> = async {
            // Stage 2: Lock all three partition keys before publishing any of them.
            // The keys are already sorted. Each partition gets a different root,
            // with the same three nodes rotated through its replica list.
            let lease_end = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)?
                .as_secs() as i64
                + 300;
            for data in &partitions {
                locks.push(
                    self.partition
                        .ask(AttemptLock {
                            table_id,
                            hash_start: *data.hash_start.as_ref(),
                            lease_end,
                        })
                        .await?
                        .context("Initial partition is already locked")?,
                );
            }
            for data in partitions {
                self.partition
                    .ask(CreateLockedPartition {
                        data,
                        locks: locks.clone(),
                    })
                    .await?;
            }
            // This is an empty table. There are no records to copy to the data nodes.
            Ok(())
        }
        .await;

        // Stage 3: Unlock every partition, including when setup failed partway through.
        // Use our tokens so we cannot release another operation's replacement lock.
        let mut unlock_errors = Vec::new();
        for lock in locks.into_iter().rev() {
            match self
                .partition
                .ask(FreeLock {
                    table_id: lock.table_id,
                    hash_start: lock.hash_start,
                    lock_token: lock.lock_token,
                })
                .await
            {
                Ok(true) => {}
                Ok(false) => unlock_errors
                    .push("Partition lock no longer belongs to this workflow".to_owned()),
                Err(error) => unlock_errors.push(error.to_string()),
            }
        }
        if !unlock_errors.is_empty() {
            anyhow::bail!(
                "Table {table_id} is not ready; setup result: {result:?}; unlock failures: {}",
                unlock_errors.join("; ")
            );
        }
        // Leave failed setup marked not ready, with its ID available for inspection.
        result
            .with_context(|| format!("Table {table_id} setup failed; table remains not ready"))?;

        // All three partitions exist and are unlocked. The table can now be used.
        table.is_ready = true;
        self.table
            .ask(Update::<table::Entity> { data: table })
            .await
            .with_context(|| format!("Failed to mark table {table_id} ready"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn initial_ring_has_even_ranges_distinct_roots_and_three_copies() {
        let partitions = initial_partitions(42, [7, 11, 19]);
        let starts: Vec<_> = partitions
            .iter()
            .map(|p| *p.hash_start.as_ref() as i128)
            .collect();
        let widths = [
            starts[1] - starts[0],
            starts[2] - starts[1],
            (1_i128 << 64) + starts[0] - starts[2],
        ];
        assert_eq!(starts[0], i64::MIN as i128);
        assert!(widths.iter().max().unwrap() - widths.iter().min().unwrap() <= 1);
        assert_eq!(widths.iter().sum::<i128>(), 1_i128 << 64);
        for (idx, p) in partitions.iter().enumerate() {
            assert_eq!(*p.table_id.as_ref(), 42);
            assert_eq!(*p.node_id.as_ref(), [7, 11, 19][idx]);
            assert_eq!(p.replicas.as_ref().0[0], *p.node_id.as_ref());
            let mut replicas = p.replicas.as_ref().0.clone();
            replicas.sort_unstable();
            assert_eq!(replicas, vec![7, 11, 19]);
            assert_eq!(*p.forward_to.as_ref(), 0);
        }
    }
}
