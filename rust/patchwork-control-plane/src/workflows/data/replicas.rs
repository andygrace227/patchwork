use anyhow::{Context, Result, ensure};
use kameo::actor::ActorRef;

use crate::db::{
    PartitionActor,
    actor::{Get, GetCached},
    partition::Model,
};

// Resolve exact partition boundaries, not a second ownership lookup.
// Reads use the final source; writes include each partition along the way.
pub(super) async fn resolve(
    actor: &ActorRef<PartitionActor>,
    mut partition: Model,
    writes: bool,
    cached: bool,
) -> Result<Vec<i64>> {
    let mut visited = Vec::new();
    let mut replicas = Vec::new();
    loop {
        ensure!(
            !visited.contains(&partition.hash_start),
            "Partition forwarding loop"
        );
        visited.push(partition.hash_start);
        ensure!(
            !partition.replicas.0.is_empty(),
            "Partition has no replicas"
        );
        if writes || partition.forward_to.is_none() {
            replicas.extend(partition.replicas.0);
        }
        let Some(hash_start) = partition.forward_to else {
            break;
        };
        let id = (partition.table_id, hash_start);
        partition = if cached {
            actor.ask(GetCached { id }).await?
        } else {
            actor.ask(Get { id }).await?
        }
        .context("Forwarded partition no longer exists")?;
    }
    replicas.sort_unstable();
    replicas.dedup();
    Ok(replicas)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::{
        actor::Create,
        partition::{self, ReplicaNodes},
    };
    use kameo::actor::Spawn;
    use sea_orm::{ActiveValue::Set, ConnectionTrait, Database, Schema};

    #[tokio::test]
    async fn forwarding_uses_exact_same_table_boundaries_and_detects_loops() {
        let db = Database::connect("sqlite::memory:").await.unwrap();
        let schema = Schema::new(db.get_database_backend());
        db.execute(&schema.create_table_from_entity(partition::Entity))
            .await
            .unwrap();
        let actor = PartitionActor::spawn(PartitionActor::new(db.clone()));
        for (table_id, hash_start, forward_to, replicas) in [
            (1, 0, None, vec![2, 3]),
            (2, 0, None, vec![99]),
            (1, 20, Some(30), vec![4]),
            (1, 30, Some(20), vec![5]),
        ] {
            actor
                .ask(Create::<partition::Entity> {
                    data: partition::ActiveModel {
                        table_id: Set(table_id),
                        hash_start: Set(hash_start),
                        forward_to: Set(forward_to),
                        replicas: Set(ReplicaNodes(replicas)),
                    },
                })
                .await
                .unwrap();
        }
        let mut source = Model {
            table_id: 1,
            hash_start: 10,
            forward_to: Some(0),
            replicas: ReplicaNodes(vec![1, 2]),
        };
        for cached in [false, true] {
            assert_eq!(
                resolve(&actor, source.clone(), false, cached)
                    .await
                    .unwrap(),
                vec![2, 3]
            );
            assert_eq!(
                resolve(&actor, source.clone(), true, cached).await.unwrap(),
                vec![1, 2, 3]
            );
        }
        source.forward_to = Some(1); // Must not fall back to boundary zero.
        assert!(resolve(&actor, source.clone(), false, false).await.is_err());
        source.forward_to = Some(20);
        assert!(
            resolve(&actor, source, true, false)
                .await
                .unwrap_err()
                .to_string()
                .contains("loop")
        );
        actor.stop_gracefully().await.unwrap();
        actor.wait_for_shutdown().await;
        db.close().await.unwrap();
    }
}
