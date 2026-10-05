use anyhow::{Context, Result};
use kameo::{Actor, actor::ActorRef, messages};
use serde_json::Value;

use crate::{
    client::Client,
    db::{
        NodeActor, PartitionActor,
        actor::Get,
        partition::{GetTablePartitions, Model},
    },
};

// Handles writes to the current replicas without caching partitions or nodes.
// Writes are best effort: an error can leave some replicas updated and others unchanged.
#[derive(Actor)]
pub struct InconsistentDataplaneClient {
    node: ActorRef<NodeActor>,
    partition: ActorRef<PartitionActor>,
}

impl InconsistentDataplaneClient {
    pub fn new(node: ActorRef<NodeActor>, partition: ActorRef<PartitionActor>) -> Self {
        Self { node, partition }
    }
}

fn owning_partition(partitions: &[Model], key: i64) -> Result<&Model> {
    anyhow::ensure!(!partitions.is_empty(), "Table has no partitions");
    let idx = match partitions.binary_search_by_key(&key, |p| p.hash_start) {
        Ok(idx) => idx,
        Err(0) => partitions.len() - 1,
        Err(idx) => idx - 1,
    };
    Ok(&partitions[idx])
}

#[messages]
impl InconsistentDataplaneClient {
    #[message]
    pub async fn put(
        &self,
        table_id: i64,
        partition_key: i64,
        secondary_key: i64,
        data: Value,
    ) -> Result<()> {
        // Resolve the current ring. An exact boundary belongs to that partition;
        // otherwise use the previous boundary, wrapping around when necessary.
        let partitions = self.partition.ask(GetTablePartitions { table_id }).await?;
        let partition = owning_partition(&partitions, partition_key)?;

        // Include the root and bootstrap source, without writing to any node twice.
        let mut node_ids = partition.replicas.0.clone();
        node_ids.push(partition.node_id);
        if partition.forward_to != 0 {
            node_ids.push(partition.forward_to);
        }
        node_ids.sort_unstable();
        node_ids.dedup();

        // Resolve every node before sending writes, so missing metadata fails early.
        let mut nodes = Vec::new();
        for id in node_ids {
            nodes.push(
                self.node
                    .ask(Get { id })
                    .await?
                    .with_context(|| format!("Replica node {id} no longer exists"))?,
            );
        }

        // Await every write. Still try the remaining replicas if one fails.
        let mut failures = Vec::new();
        for node in nodes {
            if let Err(error) =
                Client::upsert(&node.url, table_id, partition_key, secondary_key, &data).await
            {
                failures.push(format!("node {}: {error}", node.node_id));
            }
        }
        anyhow::ensure!(
            failures.is_empty(),
            "Replica writes failed: {}",
            failures.join("; ")
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::partition::ReplicaNodes;

    #[test]
    fn routes_boundaries_gaps_and_wraparound() {
        let ring: Vec<_> = [-10, 10, 30]
            .into_iter()
            .map(|hash_start| Model {
                table_id: 1,
                hash_start,
                node_id: 1,
                forward_to: 0,
                replicas: ReplicaNodes(vec![1]),
            })
            .collect();
        for (key, expected) in [
            (i64::MIN, 30),
            (-11, 30),
            (-10, -10),
            (0, -10),
            (10, 10),
            (29, 10),
            (30, 30),
            (i64::MAX, 30),
        ] {
            assert_eq!(owning_partition(&ring, key).unwrap().hash_start, expected);
        }
        assert!(owning_partition(&[], 0).is_err());
        assert_eq!(
            owning_partition(&ring[..1], i64::MAX).unwrap().hash_start,
            -10
        );
    }
}
