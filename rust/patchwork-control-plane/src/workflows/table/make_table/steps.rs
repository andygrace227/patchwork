use super::*;

impl Run {
    pub(super) async fn validate(&mut self) -> Result<State> {
        let workflow = &self.workflow;
        let ctx = &self.ctx;
        // Check capacity before creating anything. Use three copies.
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
        let nodes = workflow.node.ask(GetRandomNodes { count: 3 }).await?;
        ensure!(
            nodes.len() == 3,
            "Creating a table requires three distinct data nodes"
        );

        self.nodes = [nodes[0].node_id, nodes[1].node_id, nodes[2].node_id];
        Ok(State::Create)
    }
    pub(super) async fn create(&mut self) -> Result<State> {
        let workflow = &self.workflow;
        let ctx = &self.ctx;
        // Stage 1: Create the table entry, but keep it unavailable until setup finishes.
        let table = workflow
            .table
            .ask(Create::<table::Entity> {
                data: table::ActiveModel {
                    table_name: Set(ctx.table_name.clone()),
                    owner: Set(ctx.table_owner),
                    partition_key_name: Set(ctx.partition_key_name.clone()),
                    sort_key_name: Set(ctx.sort_key_name.clone()),
                    is_ready: Set(false),
                    ..Default::default()
                },
            })
            .await?;
        let table_id = table.table_id;

        self.partitions = initial_partitions(table_id, self.nodes);
        self.created = Some(table);
        Ok(State::Lock)
    }
    pub(super) async fn lock(&mut self) -> Result<State> {
        let workflow = &self.workflow;
        let table_id = self
            .created
            .as_ref()
            .expect("Create must run first")
            .table_id;
        let locks = &mut self.locks;
        // Stage 2: Lock all three partition keys before publishing any of them.
        // The keys are already sorted. Each partition uses the same three nodes,
        // with their replica order rotated.
        let lease_end = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_secs() as i64
            + 300;
        for data in &self.partitions {
            locks.push(
                workflow
                    .partition
                    .ask(AttemptLock {
                        table_id,
                        hash_start: *data.hash_start.as_ref(),
                        lease_end,
                    })
                    .await?
                    .context("Initial partition is already locked")?,
            );
        }
        Ok(State::Publish)
    }
    pub(super) async fn publish(&mut self) -> Result<State> {
        let workflow = &self.workflow;

        let locks = &mut self.locks;
        for data in self.partitions.clone() {
            workflow
                .partition
                .ask(CreateLockedPartition {
                    data,
                    locks: locks.clone(),
                })
                .await?;
        }
        // This is an empty table. There are no records to copy to the data nodes.
        Ok(State::Unlock)
    }
    pub(super) async fn ready(&mut self) -> Result<State> {
        let workflow = &self.workflow;
        let table_id = self
            .created
            .as_ref()
            .expect("Create must run first")
            .table_id;
        // All three partitions exist and are unlocked. The table can now be used.
        let mut table = self.created.clone().context("Table was not created")?;
        table.is_ready = true;
        let table = workflow
            .table
            .ask(Update::<table::Entity> { data: table })
            .await
            .with_context(|| format!("Failed to mark table {table_id} ready"))?;
        self.created = Some(table);
        Ok(State::Done)
    }
    pub(super) async fn unlock(&mut self) -> Result<State> {
        // Stage 3: Unlock every partition, including when setup failed partway through.
        // Use our tokens so we cannot release another operation's replacement lock.
        unlock(&self.workflow.partition, &mut self.locks).await?;
        Ok(State::Ready)
    }
}
