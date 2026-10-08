use kameo::actor::Spawn;
use patchwork_control_plane::db::{
    NodeActor, PartitionActor, TableActor,
    actor::{Create, Delete, Get, Update},
    node, partition, table,
};
use sea_orm::{ActiveValue::Set, ConnectOptions, ConnectionTrait, Database, Schema};

#[tokio::test]
async fn all_models_support_crud_and_overlapping_requests() {
    let dir = tempfile::tempdir().unwrap();
    let mut options = ConnectOptions::new(format!(
        "sqlite://{}?mode=rwc",
        dir.path().join("control.sqlite").display()
    ));
    options.max_connections(4);
    let db = Database::connect(options).await.unwrap();
    let schema = Schema::new(db.get_database_backend());
    db.execute(&schema.create_table_from_entity(node::Entity))
        .await
        .unwrap();
    db.execute(&schema.create_table_from_entity(partition::Entity))
        .await
        .unwrap();
    db.execute(&schema.create_table_from_entity(table::Entity))
        .await
        .unwrap();

    let nodes = NodeActor::spawn(NodeActor::new(db.clone()));
    let partitions = PartitionActor::spawn(PartitionActor::new(db.clone()));
    let tables = TableActor::spawn(TableActor::new(db.clone()));

    // Nodes and tables use generated keys; partitions use explicit composite keys.
    let mut node = nodes
        .ask(Create::<node::Entity> {
            data: node::ActiveModel {
                url: Set("http://localhost:3000".into()),
                ..Default::default()
            },
        })
        .await
        .unwrap();
    let mut partition = partitions
        .ask(Create::<partition::Entity> {
            data: partition::ActiveModel {
                table_id: Set(1),
                hash_start: Set(0),
                forward_to: Set(None),
                replicas: Set(partition::ReplicaNodes(vec![node.node_id])),
                ..Default::default()
            },
        })
        .await
        .unwrap();
    let mut table = tables
        .ask(Create::<table::Entity> {
            data: table::ActiveModel {
                table_name: Set("accounts".into()),
                is_ready: Set(false),
                owner: Set(2),
                partition_key_name: Set("account".into()),
                sort_key_name: Set("id".into()),
                ..Default::default()
            },
        })
        .await
        .unwrap();
    for (table_id, hash_start) in [(1, 200), (2, 50), (1, 100)] {
        partitions
            .ask(Create::<partition::Entity> {
                data: partition::ActiveModel {
                    table_id: Set(table_id),
                    hash_start: Set(hash_start),
                    replicas: Set(partition::ReplicaNodes(vec![node.node_id])),
                    forward_to: Set(None),
                    ..Default::default()
                },
            })
            .await
            .unwrap();
    }
    let ring = partitions
        .ask(partition::GetTablePartitions { table_id: 1 })
        .await
        .unwrap();
    assert_eq!(
        ring.iter().map(|p| p.hash_start).collect::<Vec<_>>(),
        vec![0, 100, 200]
    );
    assert!(ring.iter().all(|p| p.table_id == 1));
    assert!(
        partitions
            .ask(partition::GetTablePartitions { table_id: 99 })
            .await
            .unwrap()
            .is_empty()
    );
    node.url = "http://localhost:3001".into();
    partition.forward_to = Some(100);
    assert_eq!(partition.replication_factor(), 1);
    table.owner = 3;
    assert_eq!(
        nodes
            .ask(Update::<node::Entity> { data: node.clone() })
            .await
            .unwrap(),
        node
    );
    assert_eq!(
        partitions
            .ask(Update::<partition::Entity> {
                data: partition.clone()
            })
            .await
            .unwrap(),
        partition
    );
    assert_eq!(
        tables
            .ask(Update::<table::Entity> {
                data: table.clone()
            })
            .await
            .unwrap(),
        table
    );
    let (a, b, c, d) = tokio::join!(
        nodes.ask(Get { id: node.node_id }),
        nodes.ask(Get { id: node.node_id }),
        partitions.ask(Get {
            id: (partition.table_id, partition.hash_start)
        }),
        tables.ask(Get { id: table.table_id }),
    );
    assert_eq!(a.unwrap(), Some(node.clone()));
    assert_eq!(b.unwrap(), Some(node.clone()));
    assert_eq!(c.unwrap(), Some(partition.clone()));
    assert_eq!(d.unwrap(), Some(table.clone()));

    assert_eq!(
        nodes
            .ask(node::GetRandomNodes { count: 3 })
            .await
            .unwrap()
            .len(),
        1
    );
    for idx in 0..4 {
        nodes
            .ask(Create::<node::Entity> {
                data: node::ActiveModel {
                    url: Set(format!("http://localhost:{}", 4000 + idx)),
                    ..Default::default()
                },
            })
            .await
            .unwrap();
    }
    let selected = nodes.ask(node::GetRandomNodes { count: 3 }).await.unwrap();
    assert_eq!(selected.len(), 3);
    let unique: std::collections::HashSet<_> = selected.iter().map(|node| node.node_id).collect();
    assert_eq!(unique.len(), 3);

    macro_rules! delete_and_check {
        ($actor:expr, $entity:ty, $model:expr, $id:expr) => {
            assert_eq!($actor.ask(Delete { id: $id }).await.unwrap(), 1);
            assert_eq!($actor.ask(Get { id: $id }).await.unwrap(), None);
            assert_eq!($actor.ask(Delete { id: $id }).await.unwrap(), 0);
            assert!(
                $actor
                    .ask(Update::<$entity> { data: $model })
                    .await
                    .is_err()
            );
        };
    }
    delete_and_check!(nodes, node::Entity, node.clone(), node.node_id);
    delete_and_check!(
        partitions,
        partition::Entity,
        partition.clone(),
        (partition.table_id, partition.hash_start)
    );
    delete_and_check!(tables, table::Entity, table.clone(), table.table_id);
    nodes.stop_gracefully().await.unwrap();
    partitions.stop_gracefully().await.unwrap();
    tables.stop_gracefully().await.unwrap();
    nodes.wait_for_shutdown().await;
    partitions.wait_for_shutdown().await;
    tables.wait_for_shutdown().await;
    db.close().await.unwrap();
}
