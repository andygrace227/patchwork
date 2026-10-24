use super::*;
use crate::actors::node::CreateNode;
use crate::{datamodels::node, dataplane_client::Client};
use kameo::actor::Spawn;
use kameo_actors::pool::{ActorPool, Dispatch};
use sea_orm::{ActiveValue::Set, ConnectionTrait, Database, Schema};

#[tokio::test]
async fn copy_reads_all_sources_preserves_deletes_and_newer_destination_writes() {
    let client = Client::new();
    let dir = tempfile::tempdir().unwrap();
    let db = Database::connect("sqlite::memory:").await.unwrap();
    let schema = Schema::new(db.get_database_backend());
    db.execute(&schema.create_table_from_entity(node::Entity))
        .await
        .unwrap();
    let nodes = ActorPool::spawn(ActorPool::new(2, {
        let db = db.clone();
        move || NodeActor::spawn(NodeActor::new(db.clone()))
    }));
    let mut servers = Vec::new();
    let mut shards = Vec::new();
    let mut urls = Vec::new();
    let mut ids = Vec::new();
    for i in 0..4 {
        let shard_path = dir.path().join(format!("{i}.sqlite"));
        let shard_stats = patchwork_data_plane::telemetry::AccessStatsActor::spawn(
            patchwork_data_plane::telemetry::AccessStatsActor::new(
                shard_path.with_extension("stats.sqlite"),
            )
            .await
            .unwrap(),
        );
        let shard_wal = shard_path.with_extension("wal");
        let shard = patchwork_data_plane::service::Service::new(
            &shard_path,
            &shard_wal,
            shard_stats.clone(),
            2,
        )
        .await
        .unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let app = tonic::service::Routes::new(shard.server()).into_axum_router();
        servers.push(tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap()
        }));
        shards.push((shard, shard_stats));
        ids.push(
            nodes
                .ask(Dispatch(CreateNode {
                    data: node::ActiveModel {
                        url: Set(url.clone()),
                        ..Default::default()
                    },
                }))
                .await
                .unwrap()
                .node_id,
        );
        urls.push(url);
    }
    client
        .upsert(&urls[0], 1, 10, 1, &b"old".to_vec(), 1)
        .await
        .unwrap();
    client
        .upsert(&urls[1], 1, 10, 1, &b"new".to_vec(), 2)
        .await
        .unwrap();
    client
        .upsert(&urls[0], 1, 10, 2, &b"live".to_vec(), 3)
        .await
        .unwrap();
    client.delete(&urls[1], 1, 10, 2, 3).await.unwrap();
    client
        .upsert(&urls[1], 1, 10, 3, &b"only second source".to_vec(), 4)
        .await
        .unwrap();
    client
        .upsert(&urls[2], 1, 10, 1, &b"newer destination".to_vec(), 10)
        .await
        .unwrap();
    client
        .upsert(&urls[1], 2, 10, 1, &b"other table".to_vec(), 5)
        .await
        .unwrap();
    client
        .upsert(&urls[1], 1, 20, 1, &b"outside range".to_vec(), 5)
        .await
        .unwrap();
    wait_for_writes(&client, &urls).await;
    let lease_end = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
        + 60;
    copy_range(
        &client,
        &nodes,
        FetchRangeCtx {
            table_id: 1,
            lower_bound: 10,
            upper_bound: 20,
        },
        &ids[..2],
        &ids[1..], // Include an existing replica: its rows also need relabelling.
        lease_end,
        "destination-partition",
    )
    .await
    .unwrap();
    wait_for_writes(&client, &urls).await;
    assert_eq!(client.get(&urls[2], 1, 10, 1).await.unwrap().timestamp, 10);
    assert_eq!(client.get(&urls[3], 1, 10, 1).await.unwrap().timestamp, 2);
    for url in &urls[1..] {
        let deleted = client.get_including_deleted(url, 1, 10, 2).await.unwrap();
        assert!(deleted.deleted);
        assert_eq!(deleted.timestamp, 3);
        assert_eq!(
            deleted.partition_id.as_deref(),
            Some("destination-partition")
        );
        assert_eq!(
            client
                .get(url, 1, 10, 1)
                .await
                .unwrap()
                .partition_id
                .as_deref(),
            Some("destination-partition")
        );
        assert_eq!(client.get(url, 1, 10, 3).await.unwrap().timestamp, 4);
    }
    for url in &urls[2..] {
        assert_eq!(
            client
                .get_range_including_deleted(url, 1, 21, 10)
                .await
                .unwrap()
                .len(),
            3
        );
        assert!(client.get_range(url, 2, 20, 10).await.unwrap().is_empty());
    }
    for server in servers {
        server.abort();
        let _ = server.await;
    }
    for (shard, stats) in shards {
        shard.shutdown().await.unwrap();
        stats.stop_gracefully().await.unwrap();
        stats.wait_for_shutdown().await;
    }
    nodes.stop_gracefully().await.unwrap();
    nodes.wait_for_shutdown().await;
    db.close().await.unwrap();
}

async fn wait_for_writes(client: &Client, urls: &[String]) {
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        for url in urls {
            while client.get_write_backlog(url).await.unwrap()["total"] != 0 {
                tokio::time::sleep(std::time::Duration::from_millis(1)).await;
            }
        }
    })
    .await
    .unwrap();
}
