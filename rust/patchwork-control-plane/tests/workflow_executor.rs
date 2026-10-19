use kameo_actors::pool::{ActorPool, Dispatch};
use patchwork_control_plane::actors::{
    node::CreateNode, partition::CreatePartition, table::CreateTable,
};
use std::{sync::Arc, time::Duration};

use axum::{
    body::{Body, to_bytes},
    extract::Request,
    middleware::{self, Next},
};
use kameo::actor::{ActorRef, Spawn};
use patchwork_control_plane::{
    actors::{NodeActor, PartitionActor, TableActor, table::GetRandomReadyTable},
    datamodels::{node, partition, table},
    workflows::{
        executor::{
            ExecutorConfig, GetExecutorStatus, JobStatus, LongRunningWorkflowExecutor,
            SubmitWorkflow,
        },
        partition::split_partition::{SplitPartition, SplitPartitionContext},
        table::check_table_scale::{CheckTableScale, CheckTableScaleContext},
    },
};
use prost::Message;
use sea_orm::{ActiveValue::Set, ConnectionTrait, Database, Schema};
use tokio::sync::{Semaphore, mpsc};

async fn actors() -> (
    ActorRef<ActorPool<NodeActor>>,
    ActorRef<ActorPool<PartitionActor>>,
    ActorRef<ActorPool<TableActor>>,
) {
    let db = Database::connect("sqlite::memory:").await.unwrap();
    let schema = Schema::new(db.get_database_backend());
    for statement in [
        schema.create_table_from_entity(node::Entity),
        schema.create_table_from_entity(partition::Entity),
        schema.create_table_from_entity(table::Entity),
    ] {
        db.execute(&statement).await.unwrap();
    }
    (
        ActorPool::spawn(ActorPool::new(2, {
            let db = db.clone();
            move || NodeActor::spawn(NodeActor::new(db.clone()))
        })),
        ActorPool::spawn(ActorPool::new(2, {
            let db = db.clone();
            move || PartitionActor::spawn(PartitionActor::new(db.clone()))
        })),
        ActorPool::spawn(ActorPool::new(2, {
            let db = db.clone();
            move || TableActor::spawn(TableActor::new(db.clone()))
        })),
    )
}

fn check(
    table_id: i64,
    node: &ActorRef<ActorPool<NodeActor>>,
    partition: &ActorRef<ActorPool<PartitionActor>>,
    table: &ActorRef<ActorPool<TableActor>>,
    executor: &ActorRef<LongRunningWorkflowExecutor>,
) -> SubmitWorkflow<CheckTableScale> {
    SubmitWorkflow::new(
        CheckTableScale {
            client: patchwork_common::dataplane_client::Client::new(),
            node: node.clone(),
            partition: partition.clone(),
            table: table.clone(),
            orchestrator: executor.clone(),
        },
        CheckTableScaleContext { table_id },
    )
}

#[tokio::test]
async fn executor_limits_jobs_keeps_tables_separate_and_recovers_after_failure() {
    let (node, partition, table) = actors().await;
    let gates = [Arc::new(Semaphore::new(0)), Arc::new(Semaphore::new(0))];
    let handler_gates = gates.clone();
    let (started, mut requests) = mpsc::unbounded_channel();
    let dir = tempfile::tempdir().unwrap();
    let shard = patchwork_data_plane::initialize::shard(
        dir.path().join("shard.sqlite").to_str().unwrap().into(),
    )
    .await
    .unwrap();
    let shard_readers = shard.read_pool(2).unwrap();
    let shard = patchwork_data_plane::write_shard::WriteShard::spawn(shard);
    let app = patchwork_data_plane::grpc::router(
        shard.clone(),
        shard_readers.clone(),
        &dir.path().join("wal"),
    )
    .unwrap()
    .layer(middleware::from_fn(move |request: Request, next: Next| {
        let handler_gates = handler_gates.clone();
        let started = started.clone();
        async move {
            assert_eq!(
                request.uri().path(),
                "/patchwork.dataplane.DataPlane/GetTelemetryForTable"
            );
            let (parts, body) = request.into_parts();
            let bytes = to_bytes(body, 1024).await.unwrap();
            let id = patchwork_common::grpc::proto::TableRequest::decode(&bytes[5..])
                .unwrap()
                .table_id;
            started.send(id).unwrap();
            handler_gates[(id - 1) as usize]
                .acquire()
                .await
                .unwrap()
                .forget();
            next.run(Request::from_parts(parts, Body::from(bytes)))
                .await
        }
    }));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let replica = node
        .ask(Dispatch(CreateNode {
            data: node::ActiveModel {
                url: Set(url),
                ..Default::default()
            },
        }))
        .await
        .unwrap();
    for table_id in [1, 2] {
        partition
            .ask(Dispatch(CreatePartition {
                data: partition::ActiveModel {
                    table_id: Set(table_id),
                    hash_start: Set(0),
                    partition_id: Set(format!("{}:{}", table_id, 0)),
                    forward_to: Set(None),
                    replicas: Set(partition::ReplicaNodes(vec![replica.node_id])),
                },
            }))
            .await
            .unwrap();
    }
    let executor = LongRunningWorkflowExecutor::spawn(
        LongRunningWorkflowExecutor::with_config(
            node.clone(),
            partition.clone(),
            table.clone(),
            ExecutorConfig {
                max_running: 2,
                max_queued: 1,
                min_check_interval: Duration::from_secs(3600),
                max_check_interval: Duration::from_secs(3600),
            },
        )
        .unwrap(),
    );

    let check_job = executor
        .ask(check(1, &node, &partition, &table, &executor))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(2), requests.recv())
            .await
            .unwrap(),
        Some(1)
    );
    assert_eq!(check_job.status(), JobStatus::Running);
    assert!(!check_job.is_finished());
    assert!(
        executor
            .ask(check(1, &node, &partition, &table, &executor))
            .await
            .unwrap()
            .is_none()
    );
    let split_job = executor
        .ask(SubmitWorkflow::new(
            // Existing boundary fails before acquiring any MySQL locks.
            SplitPartition {
                client: patchwork_common::dataplane_client::Client::new(),
                node: node.clone(),
                partition: partition.clone(),
                table: table.clone(),
            },
            SplitPartitionContext {
                table_id: 1,
                new_partition: 0,
            },
        ))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(split_job.status(), JobStatus::Queued);
    let status = executor.ask(GetExecutorStatus).await.unwrap();
    assert_eq!((status.running, status.queued), (1, 1));
    // A full pending queue must not block a job that can run immediately.
    assert!(
        executor
            .ask(check(2, &node, &partition, &table, &executor))
            .await
            .unwrap()
            .is_some()
    );
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(2), requests.recv())
            .await
            .unwrap(),
        Some(2)
    );
    assert!(
        executor
            .ask(check(3, &node, &partition, &table, &executor))
            .await
            .is_err()
    );
    gates[0].add_permits(1);
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let status = executor.ask(GetExecutorStatus).await.unwrap();
            if status.failed == 1 {
                assert_eq!(status.completed, 1);
                assert!(
                    status
                        .last_error
                        .unwrap()
                        .contains("Partition already exists")
                );
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    check_job.result().await.unwrap();
    assert!(check_job.is_finished());
    assert!(!check_job.is_error());
    let result = check_job.result().await.unwrap();
    assert!(Arc::ptr_eq(
        &result,
        &check_job.clone().result().await.unwrap()
    ));
    assert!(split_job.is_finished());
    assert!(split_job.is_error());
    let error = split_job.result().await.unwrap_err();
    assert!(format!("{error:#}").contains("Prepare"));
    assert!(
        executor
            .ask(check(1, &node, &partition, &table, &executor))
            .await
            .unwrap()
            .is_some()
    );
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(2), requests.recv())
            .await
            .unwrap(),
        Some(1)
    );
    assert_eq!(executor.ask(GetExecutorStatus).await.unwrap().running, 2);
    gates[0].add_permits(1);
    gates[1].add_permits(1);
    tokio::time::timeout(Duration::from_secs(2), async {
        while executor.ask(GetExecutorStatus).await.unwrap().completed != 3 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    executor.stop_gracefully().await.unwrap();
    executor.wait_for_shutdown().await;
    server.abort();
    let _ = server.await;
    shard_readers.stop_gracefully().await.unwrap();
    shard_readers.wait_for_shutdown().await;
    shard.stop_gracefully().await.unwrap();
    shard.wait_for_shutdown().await;
    node.stop_gracefully().await.unwrap();
    partition.stop_gracefully().await.unwrap();
    table.stop_gracefully().await.unwrap();
}

#[tokio::test]
async fn random_checker_handles_empty_tables_and_submits_ready_tables() {
    let (node, partition, table) = actors().await;
    assert!(
        table
            .ask(Dispatch(GetRandomReadyTable))
            .await
            .unwrap()
            .is_none()
    );
    for (table_id, ready) in [(1, false), (2, true)] {
        table
            .ask(Dispatch(CreateTable {
                data: table::ActiveModel {
                    table_id: Set(table_id),
                    table_name: Set(format!("table-{table_id}")),
                    owner: Set(1),
                    owner_control_node: Set(0),
                    backup_control_node: Set(0),
                    partition_key_name: Set("key".into()),
                    sort_key_name: Set("sort".into()),
                    secret_key_id: Set(None),
                    is_ready: Set(ready),
                },
            }))
            .await
            .unwrap();
    }
    for _ in 0..3 {
        assert_eq!(
            table
                .ask(Dispatch(GetRandomReadyTable))
                .await
                .unwrap()
                .unwrap()
                .table_id,
            2
        );
    }
    let executor = LongRunningWorkflowExecutor::spawn(
        LongRunningWorkflowExecutor::with_config(
            node.clone(),
            partition.clone(),
            table.clone(),
            ExecutorConfig {
                min_check_interval: Duration::from_millis(5),
                max_check_interval: Duration::from_millis(15),
                ..Default::default()
            },
        )
        .unwrap(),
    );
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let status = executor.ask(GetExecutorStatus).await.unwrap();
            assert_eq!(status.failed, 0);
            if status.completed > 0 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    executor.stop_gracefully().await.unwrap();
    executor.wait_for_shutdown().await;
    node.stop_gracefully().await.unwrap();
    partition.stop_gracefully().await.unwrap();
    table.stop_gracefully().await.unwrap();
}
