use super::*;

impl Run {
    pub(super) async fn prepare(&mut self) -> Result<State> {
        let workflow = &self.workflow;
        let ctx = &self.ctx;
        // Fetch the hash ring for the table.
        // The source partition is going away. The previous partition takes its range.
        let partitions = workflow
            .partition
            .ask(GetTablePartitions {
                table_id: ctx.table_id,
            })
            .await?;
        let (previous, source, next) = neighbors(&partitions, ctx.hash_start)?;
        ensure!(
            source.forward_to.is_none() && previous.forward_to.is_none(),
            "Cannot merge a bootstrapping partition"
        );
        for partition in [source, previous] {
            ensure!(
                !partition.replicas.0.is_empty(),
                "Partition has no replicas"
            );
            let mut unique = partition.replicas.0.clone();
            unique.sort_unstable();
            unique.dedup();
            ensure!(
                unique.len() == partition.replicas.0.len(),
                "Replica list contains duplicate nodes"
            );
        }
        // Same range restriction as split_partition until ring ranges are supported.
        ensure!(
            source.hash_start < next.hash_start,
            "Wraparound copying is not supported by the range API yet"
        );
        // Stage 0: Lock partitions in the database.
        // Lock the previous partition, the source, and the next boundary.
        // Take locks in a consistent order. With two partitions, previous == next,
        // so only lock that key once.
        let mut keys = vec![previous.hash_start, source.hash_start, next.hash_start];
        keys.sort_unstable();
        keys.dedup();
        let lease_end = now()? + 300;

        self.boundaries = Some((previous.clone(), source.clone(), next.clone()));
        self.keys = keys;
        self.lease_end = lease_end;
        Ok(State::Lock)
    }
    pub(super) async fn lock(&mut self) -> Result<State> {
        let workflow = &self.workflow;
        let ctx = &self.ctx;
        let lease_end = self.lease_end;
        let locks = &mut self.locks;
        let (previous, source, next) = self.boundaries.as_ref().expect("Prepare must run first");
        for hash_start in self.keys.clone() {
            locks.push(
                workflow
                    .partition
                    .ask(AttemptLock {
                        table_id: ctx.table_id,
                        hash_start,
                        lease_end,
                    })
                    .await?
                    .context("Partition is already locked; retry the merge")?,
            );
        }
        // Another split or merge may have finished while we were taking locks.
        // Check that these are still the same partitions before moving any data.
        let current = workflow
            .partition
            .ask(GetTablePartitions {
                table_id: ctx.table_id,
            })
            .await?;
        ensure!(
            neighbors(&current, ctx.hash_start)? == (previous, source, next),
            "Partition ring changed; retry the merge"
        );

        ensure!(
            !current
                .iter()
                .any(|p| p.forward_to == Some(source.hash_start)
                    || p.forward_to == Some(previous.hash_start)),
            "Another partition is still copying from a merge partition"
        );
        Ok(State::SelectNodes)
    }
    pub(super) async fn select_nodes(&mut self) -> Result<State> {
        let workflow = &self.workflow;
        let (previous, source, _) = self.boundaries.as_ref().expect("Prepare must run first");
        // Stage 1: Resolve obsolete replicas before copying or changing the ring.
        let obsolete = &mut self.obsolete;
        for id in &source.replicas.0 {
            if !previous.replicas.0.contains(id) {
                obsolete.push(
                    workflow
                        .node
                        .ask(Get { id: *id })
                        .await?
                        .context("Source replica no longer exists")?,
                );
            }
        }
        Ok(State::Copy)
    }
    pub(super) async fn copy(&mut self) -> Result<State> {
        let workflow = &self.workflow;
        let ctx = &self.ctx;
        let lease_end = self.lease_end;
        let (previous, source, next) = self.boundaries.as_ref().expect("Prepare must run first");
        // Stage 2: Copy every source replica into the previous partition's replicas.
        // Keep the source boundary until every destination acknowledges the copy.
        copy_range(
            &workflow.node,
            FetchRangeCtx {
                table_id: ctx.table_id,
                lower_bound: source.hash_start,
                upper_bound: next.hash_start,
            },
            &source.replicas.0,
            &previous.replicas.0,
            lease_end,
        )
        .await?;
        Ok(State::Metadata)
    }
    pub(super) async fn metadata(&mut self) -> Result<State> {
        let workflow = &self.workflow;
        let ctx = &self.ctx;
        let locks = &mut self.locks;
        // Stage 3: Remove the source partition from the database.
        // The previous partition now owns the combined range; its row stays as-is.
        // Deletion checks that we still hold all the leases.
        workflow
            .partition
            .ask(DeleteLockedPartition {
                table_id: ctx.table_id,
                hash_start: ctx.hash_start,
                locks: locks.clone(),
            })
            .await?;
        Ok(State::Cleanup)
    }
    pub(super) async fn cleanup(&mut self) -> Result<State> {
        let ctx = &self.ctx;
        let lease_end = self.lease_end;
        let (_, source, next) = self.boundaries.as_ref().expect("Prepare must run first");
        // Stage 4: Clean up the source range on its old replicas.
        // Only remove the records we moved, not the node's other partitions.
        // Shared replicas are excluded from obsolete: they must keep their copies.
        for node in &self.obsolete {
            check_lease(lease_end)?;
            Client::delete_range(&node.url, ctx.table_id, next.hash_start, source.hash_start)
                .await
                .context("Merge completed, but old replica cleanup failed")?;
        }
        Ok(State::Unlock)
    }
    pub(super) async fn unlock(&mut self) -> Result<State> {
        // Stage 5: Unlock everything, including when copying or cleanup failed.
        // Release only our own locks, in reverse order, and try every unlock.
        unlock(&self.workflow.partition, &mut self.locks).await?;
        Ok(State::Done)
    }
}
