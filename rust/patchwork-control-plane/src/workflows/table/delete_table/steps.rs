use super::*;

impl Run {
    pub(super) async fn prepare(&mut self) -> Result<State> {
        let workflow = &self.workflow;
        let ctx = &self.ctx;
        // Stage 1: Mark the table unavailable before removing its records.
        // An already absent table needs no further work.
        let Some(mut table) = workflow.table.ask(Get { id: ctx.table_id }).await? else {
            return Ok(State::Done);
        };
        table.is_ready = false;
        workflow
            .table
            .ask(Update::<table::Entity> { data: table })
            .await?;

        let partitions = workflow
            .partition
            .ask(GetTablePartitions {
                table_id: ctx.table_id,
            })
            .await?;

        self.partitions = partitions;
        Ok(State::Lock)
    }
    pub(super) async fn lock(&mut self) -> Result<State> {
        let workflow = &self.workflow;
        let ctx = &self.ctx;
        let partitions = &self.partitions;
        let locks = &mut self.locks;
        // Stage 2: Lock every partition in hash order before deleting any data.
        // Holding all boundaries prevents splits and merges from moving the ranges.
        let lease_end = now()? + 300;
        for partition in partitions {
            locks.push(
                workflow
                    .partition
                    .ask(AttemptLock {
                        table_id: ctx.table_id,
                        hash_start: partition.hash_start,
                        lease_end,
                    })
                    .await?
                    .context("Partition is already locked; retry deleting the table")?,
            );
        }
        let current = workflow
            .partition
            .ask(GetTablePartitions {
                table_id: ctx.table_id,
            })
            .await?;
        ensure!(
            current == *partitions,
            "Partition ring changed; retry deleting the table"
        );

        self.lease_end = lease_end;
        Ok(State::SelectNodes)
    }
    pub(super) async fn select_nodes(&mut self) -> Result<State> {
        let workflow = &self.workflow;

        let partitions = &self.partitions;

        // Stage 3: Find every node holding this table, including bootstrap sources in this same table.
        // A node may hold several partitions. Delete this table there only once.
        let mut node_ids = Vec::new();
        for partition in partitions {
            node_ids.extend(&partition.replicas.0);
        }
        node_ids.sort_unstable();
        node_ids.dedup();
        let nodes = &mut self.nodes;
        for id in node_ids {
            nodes.push(
                workflow
                    .node
                    .ask(Get { id })
                    .await?
                    .context("Replica node no longer exists; table cleanup is incomplete")?,
            );
        }
        Ok(State::DeleteData)
    }
    pub(super) async fn delete_data(&mut self) -> Result<State> {
        let ctx = &self.ctx;

        let lease_end = self.lease_end;
        // Stage 4: Remove the table's records from every replica.
        // A table-scoped delete covers the whole ring and leaves other tables alone.
        // Keep all partition rows until every node succeeds, so a retry can find them.
        for node in &self.nodes {
            ensure!(
                now()? < lease_end,
                "Table deletion lease expired during cleanup"
            );
            Client::delete_table(&node.url, ctx.table_id).await?;
        }
        Ok(State::Metadata)
    }
    pub(super) async fn metadata(&mut self) -> Result<State> {
        let workflow = &self.workflow;
        let ctx = &self.ctx;
        let partitions = &self.partitions;
        let locks = &mut self.locks;

        // Stage 5: Remove the empty partitions while their locks are still held.
        // Deletion checks every lease. Only then remove the table entry itself.
        for partition in partitions {
            workflow
                .partition
                .ask(DeleteLockedPartition {
                    table_id: ctx.table_id,
                    hash_start: partition.hash_start,
                    locks: locks.clone(),
                })
                .await?;
        }
        workflow.table.ask(Delete { id: ctx.table_id }).await?;
        Ok(State::Unlock)
    }
    pub(super) async fn unlock(&mut self) -> Result<State> {
        // Stage 6: Unlock everything, even if a node failed during cleanup.
        // Release only our own locks, in reverse order, and try every unlock.
        unlock(&self.workflow.partition, &mut self.locks).await?;
        Ok(State::Done)
    }
}
