use super::*;
use crate::actors::partition::CreatePartition;
use crate::datamodels::partition::{self, ReplicaNodes};
use kameo::actor::Spawn;
use kameo_actors::pool::{ActorPool, Dispatch};
use sea_orm::{ActiveValue::Set, ConnectionTrait, Database, Schema};

#[tokio::test]
async fn forwarding_uses_exact_same_table_boundaries_and_detects_loops() {
    let db = Database::connect("sqlite::memory:").await.unwrap();
    let schema = Schema::new(db.get_database_backend());
    db.execute(&schema.create_table_from_entity(partition::Entity))
        .await
        .unwrap();
    let actor = ActorPool::spawn(ActorPool::new(2, {
        let db = db.clone();
        move || PartitionActor::spawn(PartitionActor::new(db.clone()))
    }));
    for (table_id, hash_start, forward_to, replicas) in [
        (1, 0, None, vec![2, 3]),
        (2, 0, None, vec![99]),
        (1, 20, Some(30), vec![4]),
        (1, 30, Some(20), vec![5]),
    ] {
        actor
            .ask(Dispatch(CreatePartition {
                data: partition::ActiveModel {
                    table_id: Set(table_id),
                    hash_start: Set(hash_start),
                    partition_id: Set(format!("{}:{}", table_id, hash_start)),
                    forward_to: Set(forward_to),
                    replicas: Set(ReplicaNodes(replicas)),
                },
            }))
            .await
            .unwrap();
    }
    let mut source = Model {
        table_id: 1,
        hash_start: 10,
        partition_id: "test-partition".into(),
        forward_to: Some(0),
        replicas: ReplicaNodes(vec![1, 2]),
    };
    assert_eq!(
        resolve(&actor, source.clone(), false).await.unwrap(),
        vec![2, 3]
    );
    assert_eq!(
        resolve(&actor, source.clone(), true).await.unwrap(),
        vec![1, 2, 3]
    );
    source.forward_to = Some(1); // Must not fall back to boundary zero.
    assert!(resolve(&actor, source.clone(), false).await.is_err());
    source.forward_to = Some(20);
    assert!(
        resolve(&actor, source, true)
            .await
            .unwrap_err()
            .to_string()
            .contains("loop")
    );
    actor.stop_gracefully().await.unwrap();
    actor.wait_for_shutdown().await;
    db.close().await.unwrap();
}
