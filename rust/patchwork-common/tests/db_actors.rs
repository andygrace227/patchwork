use kameo::actor::Spawn;
use patchwork_common::actors::{
    node::{CreateNode, DeleteNode, GetNode, UpdateNode},
    partition::{CreatePartition, DeletePartition, GetPartition, UpdatePartition},
    table::{CreateTable, DeleteTable, GetTable, UpdateTable},
};
use patchwork_common::{
    actors::{
        NodeActor, PartitionActor, TableActor,
        node::GetRandomNodes,
        partition::{GetPartitionAtOrBefore, GetTablePartitions},
    },
    datamodels::{node, partition, table},
};
use sea_orm::{ActiveValue::Set, ConnectOptions, ConnectionTrait, Database, Schema};

#[tokio::test]
async fn node_reads_reflect_database_changes() {
    let db = Database::connect("sqlite::memory:").await.unwrap();
    db.execute(&Schema::new(db.get_database_backend()).create_table_from_entity(node::Entity))
        .await
        .unwrap();
    let nodes = NodeActor::spawn(NodeActor::new(db.clone()));
    let mut row = nodes
        .ask(CreateNode {
            data: node::ActiveModel {
                url: Set("http://original".into()),
                ..Default::default()
            },
        })
        .await
        .unwrap();
    assert_eq!(
        nodes.ask(GetNode { id: row.node_id }).await.unwrap(),
        Some(row.clone())
    );
    db.execute_unprepared("UPDATE node SET url = 'http://external'")
        .await
        .unwrap();
    assert_eq!(
        nodes
            .ask(GetNode { id: row.node_id })
            .await
            .unwrap()
            .unwrap()
            .url,
        "http://external"
    );

    row.url = "http://actor-update".into();
    nodes.ask(UpdateNode { data: row.clone() }).await.unwrap();
    assert_eq!(
        nodes.ask(GetNode { id: row.node_id }).await.unwrap(),
        Some(row.clone())
    );
    nodes.ask(DeleteNode { id: row.node_id }).await.unwrap();
    assert_eq!(nodes.ask(GetNode { id: row.node_id }).await.unwrap(), None);
    // Registering a previously missing ID is immediately visible.
    nodes
        .ask(CreateNode {
            data: node::ActiveModel {
                node_id: Set(row.node_id),
                url: Set(row.url.clone()),
            },
        })
        .await
        .unwrap();
    assert_eq!(
        nodes.ask(GetNode { id: row.node_id }).await.unwrap(),
        Some(row)
    );
    nodes.stop_gracefully().await.unwrap();
    nodes.wait_for_shutdown().await;
    db.close().await.unwrap();
}

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
        .ask(CreateNode {
            data: node::ActiveModel {
                url: Set("http://localhost:3000".into()),
                ..Default::default()
            },
        })
        .await
        .unwrap();
    let mut partition = partitions
        .ask(CreatePartition {
            data: partition::ActiveModel {
                table_id: Set(1),
                hash_start: Set(0),
                partition_id: Set(format!("{}:{}", 1, 0)),
                forward_to: Set(None),
                replicas: Set(partition::ReplicaNodes(vec![node.node_id])),
                ..Default::default()
            },
        })
        .await
        .unwrap();
    let mut table = tables
        .ask(CreateTable {
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
            .ask(CreatePartition {
                data: partition::ActiveModel {
                    table_id: Set(table_id),
                    hash_start: Set(hash_start),
                    partition_id: Set(format!("{}:{}", table_id, hash_start)),
                    replicas: Set(partition::ReplicaNodes(vec![node.node_id])),
                    forward_to: Set(None),
                    ..Default::default()
                },
            })
            .await
            .unwrap();
    }
    let ring = partitions
        .ask(GetTablePartitions { table_id: 1 })
        .await
        .unwrap();
    assert_eq!(
        ring.iter().map(|p| p.hash_start).collect::<Vec<_>>(),
        vec![0, 100, 200]
    );
    assert!(ring.iter().all(|p| p.table_id == 1));
    assert!(
        partitions
            .ask(GetTablePartitions { table_id: 99 })
            .await
            .unwrap()
            .is_empty()
    );
    node.url = "http://localhost:3001".into();
    partition.forward_to = Some(100);
    assert_eq!(partition.replication_factor(), 1);
    table.owner = 3;
    assert_eq!(
        nodes.ask(UpdateNode { data: node.clone() }).await.unwrap(),
        node
    );
    assert_eq!(
        partitions
            .ask(UpdatePartition {
                data: partition.clone()
            })
            .await
            .unwrap(),
        partition
    );
    assert_eq!(
        tables
            .ask(UpdateTable {
                data: table.clone()
            })
            .await
            .unwrap(),
        table
    );
    let (a, b, c, d) = tokio::join!(
        nodes.ask(GetNode { id: node.node_id }),
        nodes.ask(GetNode { id: node.node_id }),
        partitions.ask(GetPartition {
            id: (partition.table_id, partition.hash_start)
        }),
        tables.ask(GetTable { id: table.table_id }),
    );
    assert_eq!(a.unwrap(), Some(node.clone()));
    assert_eq!(b.unwrap(), Some(node.clone()));
    assert_eq!(c.unwrap(), Some(partition.clone()));
    assert_eq!(d.unwrap(), Some(table.clone()));

    assert_eq!(
        nodes.ask(GetRandomNodes { count: 3 }).await.unwrap().len(),
        1
    );
    for idx in 0..4 {
        nodes
            .ask(CreateNode {
                data: node::ActiveModel {
                    url: Set(format!("http://localhost:{}", 4000 + idx)),
                    ..Default::default()
                },
            })
            .await
            .unwrap();
    }
    let selected = nodes.ask(GetRandomNodes { count: 3 }).await.unwrap();
    assert_eq!(selected.len(), 3);
    let unique: std::collections::HashSet<_> = selected.iter().map(|node| node.node_id).collect();
    assert_eq!(unique.len(), 3);

    macro_rules! delete_and_check {
        ($actor:expr, $delete:ident, $get:ident, $update:ident, $model:expr, $id:expr) => {
            assert_eq!($actor.ask($delete { id: $id }).await.unwrap(), 1);
            assert_eq!($actor.ask($get { id: $id }).await.unwrap(), None);
            assert_eq!($actor.ask($delete { id: $id }).await.unwrap(), 0);
            assert!($actor.ask($update { data: $model }).await.is_err());
        };
    }
    delete_and_check!(
        nodes,
        DeleteNode,
        GetNode,
        UpdateNode,
        node.clone(),
        node.node_id
    );
    delete_and_check!(
        partitions,
        DeletePartition,
        GetPartition,
        UpdatePartition,
        partition.clone(),
        (partition.table_id, partition.hash_start)
    );
    delete_and_check!(
        tables,
        DeleteTable,
        GetTable,
        UpdateTable,
        table.clone(),
        table.table_id
    );
    nodes.stop_gracefully().await.unwrap();
    partitions.stop_gracefully().await.unwrap();
    tables.stop_gracefully().await.unwrap();
    nodes.wait_for_shutdown().await;
    partitions.wait_for_shutdown().await;
    tables.wait_for_shutdown().await;
    db.close().await.unwrap();
}

#[tokio::test]
async fn metadata_reads_reflect_database_changes() {
    let db = Database::connect("sqlite::memory:").await.unwrap();
    let schema = Schema::new(db.get_database_backend());
    db.execute(&schema.create_table_from_entity(table::Entity))
        .await
        .unwrap();
    db.execute(&schema.create_table_from_entity(partition::Entity))
        .await
        .unwrap();
    let tables = TableActor::spawn(TableActor::new(db.clone()));
    let partitions = PartitionActor::spawn(PartitionActor::new(db.clone()));
    let mut row = tables
        .ask(CreateTable {
            data: table::ActiveModel {
                table_id: Set(1),
                table_name: Set("original".into()),
                owner: Set(1),
                partition_key_name: Set("pk".into()),
                sort_key_name: Set("sk".into()),
                is_ready: Set(true),
                ..Default::default()
            },
        })
        .await
        .unwrap();
    assert_eq!(
        tables.ask(GetTable { id: 1 }).await.unwrap(),
        Some(row.clone())
    );
    db.execute_unprepared("UPDATE `table` SET table_name = 'external' WHERE table_id = 1")
        .await
        .unwrap();
    assert_eq!(
        tables
            .ask(GetTable { id: 1 })
            .await
            .unwrap()
            .unwrap()
            .table_name,
        "external"
    );
    row.table_name = "actor update".into();
    tables.ask(UpdateTable { data: row.clone() }).await.unwrap();
    assert_eq!(tables.ask(GetTable { id: 1 }).await.unwrap(), Some(row));
    tables.ask(DeleteTable { id: 1 }).await.unwrap();
    assert!(tables.ask(GetTable { id: 1 }).await.unwrap().is_none());

    assert!(
        partitions
            .ask(GetTablePartitions { table_id: 1 })
            .await
            .unwrap()
            .is_empty()
    );
    let mut boundary = partitions
        .ask(CreatePartition {
            data: partition::ActiveModel {
                table_id: Set(1),
                hash_start: Set(i64::MIN),
                partition_id: Set(format!("{}:{}", 1, i64::MIN)),
                forward_to: Set(None),
                replicas: Set(partition::ReplicaNodes(vec![1, 2])),
            },
        })
        .await
        .unwrap();
    assert_eq!(
        partitions
            .ask(GetTablePartitions { table_id: 1 })
            .await
            .unwrap(),
        vec![boundary.clone()]
    );
    db.execute_unprepared("UPDATE `partition` SET forward_to = 0 WHERE table_id = 1")
        .await
        .unwrap();
    boundary.forward_to = Some(0);
    assert_eq!(
        partitions
            .ask(GetTablePartitions { table_id: 1 })
            .await
            .unwrap(),
        vec![boundary.clone()]
    );
    assert_eq!(
        partitions
            .ask(GetPartitionAtOrBefore {
                table_id: 1,
                hash_start: 10
            })
            .await
            .unwrap(),
        Some(boundary.clone())
    );
    boundary.replicas = partition::ReplicaNodes(vec![2, 3]);
    partitions
        .ask(UpdatePartition {
            data: boundary.clone(),
        })
        .await
        .unwrap();
    assert_eq!(
        partitions
            .ask(GetTablePartitions { table_id: 1 })
            .await
            .unwrap(),
        vec![boundary]
    );
    partitions
        .ask(DeletePartition {
            id: (1i64, i64::MIN),
        })
        .await
        .unwrap();
    assert!(
        partitions
            .ask(GetTablePartitions { table_id: 1 })
            .await
            .unwrap()
            .is_empty()
    );

    db.execute_unprepared(
        "INSERT INTO `partition` (table_id, hash_start, partition_id, replicas) VALUES (1, 0, 'new-partition', '[1]')",
    )
    .await
    .unwrap();
    assert_eq!(
        partitions
            .ask(GetTablePartitions { table_id: 1 })
            .await
            .unwrap()
            .len(),
        1
    );
    let created = tables
        .ask(CreateTable {
            data: table::ActiveModel {
                table_id: Set(1),
                table_name: Set("before".into()),
                owner: Set(1),
                partition_key_name: Set("pk".into()),
                sort_key_name: Set("sk".into()),
                is_ready: Set(true),
                ..Default::default()
            },
        })
        .await
        .unwrap();
    assert_eq!(tables.ask(GetTable { id: 1 }).await.unwrap(), Some(created));
    db.execute_unprepared("UPDATE `table` SET table_name = 'after' WHERE table_id = 1")
        .await
        .unwrap();
    assert_eq!(
        tables
            .ask(GetTable { id: 1 })
            .await
            .unwrap()
            .unwrap()
            .table_name,
        "after"
    );
    partitions.stop_gracefully().await.unwrap();
    partitions.wait_for_shutdown().await;
    tables.stop_gracefully().await.unwrap();
    tables.wait_for_shutdown().await;
    db.close().await.unwrap();
}
