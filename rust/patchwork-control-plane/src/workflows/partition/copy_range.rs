use anyhow::{Result, ensure};
use kameo::actor::ActorRef;

use crate::{
    db::NodeActor,
    workflows::data::{
        fetch_range::{FetchRange, FetchRangeCtx},
        put_one_consistent::PutOneConsistent,
    },
};

// Control-plane copy: read every source before writing, retaining timestamps
// and tombstones. Destinations need not be published in the partition map yet.
pub(super) async fn copy_range(
    node: &ActorRef<NodeActor>,
    ctx: FetchRangeCtx,
    sources: &[i64],
    targets: &[i64],
    lease_end: i64,
) -> Result<()> {
    check_lease(lease_end)?;
    ensure!(!targets.is_empty(), "Copy has no destinations");
    let records = FetchRange::from_replicas(node, &ctx, sources).await?;
    for record in records {
        check_lease(lease_end)?;
        PutOneConsistent::to_replicas(node, &record, targets).await?;
    }
    check_lease(lease_end)
}

fn check_lease(lease_end: i64) -> Result<()> {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_secs() as i64;
    ensure!(now < lease_end, "Partition copy lease expired");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        dataplane_client::Client,
        db::{actor::Create, node},
    };
    use kameo::actor::Spawn;
    use patchwork_data_plane::{api, shard_actor::ShardActor};
    use sea_orm::{ActiveValue::Set, ConnectionTrait, Database, Schema};
    use serde_json::json;

    #[tokio::test]
    async fn copy_reads_all_sources_preserves_deletes_and_newer_destination_writes() {
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
            let shard = ShardActor::spawn(
                ShardActor::new(
                    dir.path()
                        .join(format!("{i}.sqlite"))
                        .to_str()
                        .unwrap()
                        .into(),
                )
                .await
                .unwrap(),
            );
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let url = format!("http://{}", listener.local_addr().unwrap());
            let app = api::router(shard.clone());
            servers.push(tokio::spawn(async move {
                axum::serve(listener, app).await.unwrap()
            }));
            shards.push(shard);
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
        Client::upsert(&urls[0], 1, 10, 1, &json!("old"), 1)
            .await
            .unwrap();
        Client::upsert(&urls[1], 1, 10, 1, &json!("new"), 2)
            .await
            .unwrap();
        Client::upsert(&urls[0], 1, 10, 2, &json!("live"), 3)
            .await
            .unwrap();
        Client::delete(&urls[1], 1, 10, 2, 3).await.unwrap();
        Client::upsert(&urls[1], 1, 10, 3, &json!("only second source"), 4)
            .await
            .unwrap();
        Client::upsert(&urls[2], 1, 10, 1, &json!("newer destination"), 10)
            .await
            .unwrap();
        Client::upsert(&urls[1], 2, 10, 1, &json!("other table"), 5)
            .await
            .unwrap();
        Client::upsert(&urls[1], 1, 20, 1, &json!("outside range"), 5)
            .await
            .unwrap();
        let lease_end = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64
            + 60;
        copy_range(
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
        assert_eq!(Client::get(&urls[2], 1, 10, 1).await.unwrap().timestamp, 10);
        assert_eq!(Client::get(&urls[3], 1, 10, 1).await.unwrap().timestamp, 2);
        for url in &urls[2..] {
            let deleted = Client::get_including_deleted(url, 1, 10, 2).await.unwrap();
            assert!(deleted.deleted);
            assert_eq!(deleted.timestamp, 3);
            assert_eq!(Client::get(url, 1, 10, 3).await.unwrap().timestamp, 4);
            assert_eq!(
                Client::get_range_including_deleted(url, 1, 21, 10)
                    .await
                    .unwrap()
                    .len(),
                3
            );
            assert!(Client::get_range(url, 2, 20, 10).await.unwrap().is_empty());
        }
        for server in servers {
            server.abort();
            let _ = server.await;
        }
        for shard in shards {
            shard.stop_gracefully().await.unwrap();
            shard.wait_for_shutdown().await;
        }
        nodes.stop_gracefully().await.unwrap();
        nodes.wait_for_shutdown().await;
        db.close().await.unwrap();
    }
}
