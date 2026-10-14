use super::*;
use crate::{actors::crud::Create, datamodels::node, dataplane_client::Client};
use kameo::actor::Spawn;
use patchwork_data_plane::{api, write_shard::WriteShard};
use sea_orm::{ActiveValue::Set, ConnectionTrait, Database, Schema};
use serde_json::json;

#[tokio::test]
async fn copy_reads_all_sources_preserves_deletes_and_newer_destination_writes() {
    let client = Client::new();
    let dir = tempfile::tempdir().unwrap();
    let db = Database::connect("sqlite::memory:").await.unwrap();
    let schema = Schema::new(db.get_database_backend());
    db.execute(&schema.create_table_from_entity(node::Entity))
        .await
        .unwrap();
    let nodes = NodeActor::spawn(NodeActor::new(db.clone()));
    let mut servers = Vec::new();
    let mut shards = Vec::new();
    let mut urls = Vec::new();
    let mut ids = Vec::new();
    for i in 0..4 {
        let shard = patchwork_data_plane::initialize::shard(
            dir.path()
                .join(format!("{i}.sqlite"))
                .to_str()
                .unwrap()
                .into(),
        )
        .await
        .unwrap();
        let shard_readers = shard.read_pool(2).unwrap();
        let shard = WriteShard::spawn(shard);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let app = api::router(
            shard.clone(),
            shard_readers.clone(),
            &dir.path().join(format!("{i}.wal")),
        )
        .unwrap();
        servers.push(tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap()
        }));
        shards.push((shard, shard_readers));
        ids.push(
            nodes
                .ask(Create::<node::Entity> {
                    data: node::ActiveModel {
                        url: Set(url.clone()),
                        ..Default::default()
                    },
                })
                .await
                .unwrap()
                .node_id,
        );
        urls.push(url);
    }
    client
        .upsert(&urls[0], 1, 10, 1, &json!("old"), 1)
        .await
        .unwrap();
    client
        .upsert(&urls[1], 1, 10, 1, &json!("new"), 2)
        .await
        .unwrap();
    client
        .upsert(&urls[0], 1, 10, 2, &json!("live"), 3)
        .await
        .unwrap();
    client.delete(&urls[1], 1, 10, 2, 3).await.unwrap();
    client
        .upsert(&urls[1], 1, 10, 3, &json!("only second source"), 4)
        .await
        .unwrap();
    client
        .upsert(&urls[2], 1, 10, 1, &json!("newer destination"), 10)
        .await
        .unwrap();
    client
        .upsert(&urls[1], 2, 10, 1, &json!("other table"), 5)
        .await
        .unwrap();
    client
        .upsert(&urls[1], 1, 20, 1, &json!("outside range"), 5)
        .await
        .unwrap();
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
        &ids[2..],
        lease_end,
    )
    .await
    .unwrap();
    assert_eq!(client.get(&urls[2], 1, 10, 1).await.unwrap().timestamp, 10);
    assert_eq!(client.get(&urls[3], 1, 10, 1).await.unwrap().timestamp, 2);
    for url in &urls[2..] {
        let deleted = client.get_including_deleted(url, 1, 10, 2).await.unwrap();
        assert!(deleted.deleted);
        assert_eq!(deleted.timestamp, 3);
        assert_eq!(client.get(url, 1, 10, 3).await.unwrap().timestamp, 4);
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
    for (shard, shard_readers) in shards {
        shard_readers.stop_gracefully().await.unwrap();
        shard_readers.wait_for_shutdown().await;
        shard.stop_gracefully().await.unwrap();
        shard.wait_for_shutdown().await;
    }
    nodes.stop_gracefully().await.unwrap();
    nodes.wait_for_shutdown().await;
    db.close().await.unwrap();
}
