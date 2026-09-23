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

    // Auto-generated keys, with no explicit key provided by the caller.
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
                hash_start: Set(0),
                node_id: Set(node.node_id),
                ..Default::default()
            },
        })
        .await
        .unwrap();
    let mut table = tables
        .ask(Create::<table::Entity> {
            data: table::ActiveModel {
                table_name: Set(1),
                owner: Set(2),
                partition_key_name: Set("account".into()),
                sort_key_name: Set("id".into()),
                ..Default::default()
            },
        })
        .await
        .unwrap();
    node.url = "http://localhost:3001".into();
    partition.hash_start = 100;
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
            id: partition.table_id
        }),
        tables.ask(Get { id: table.table_id }),
    );
    assert_eq!(a.unwrap(), Some(node.clone()));
    assert_eq!(b.unwrap(), Some(node.clone()));
    assert_eq!(c.unwrap(), Some(partition.clone()));
    assert_eq!(d.unwrap(), Some(table.clone()));

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
        partition.table_id
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
