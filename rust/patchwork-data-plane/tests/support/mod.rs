use std::{path::Path, time::Duration};

use kameo::actor::{ActorRef, Spawn};
use patchwork_common::dataplane_client::Client;
use patchwork_data_plane::{
    grpc, initialize,
    read_shard::ReadShardPool,
    replica_writer::ReplicaWriter,
    telemetry::AccessStatsActor,
    write_handler::{FlushToShard, WriteHandler},
    write_shard::WriteShard,
};

pub struct Node {
    pub url: String,
    pub app: axum::Router,
    pub client: Client,
    handler: ActorRef<WriteHandler>,
    writer: ActorRef<ReplicaWriter>,
    readers: ReadShardPool,
    shard: ActorRef<WriteShard>,
    server: tokio::task::JoinHandle<()>,
}

impl Node {
    pub async fn start(path: &Path, wal: &Path, stats: Option<ActorRef<AccessStatsActor>>) -> Self {
        let path = path.display().to_string();
        let shard = match stats {
            Some(stats) => initialize::shard_with_access_stats(path, stats)
                .await
                .unwrap(),
            None => initialize::shard(path).await.unwrap(),
        };
        let readers = shard.read_pool(2).unwrap();
        let shard = WriteShard::spawn(shard);
        let writer = ReplicaWriter::spawn(ReplicaWriter::default());
        let handler =
            WriteHandler::spawn(WriteHandler::new(shard.clone(), writer.clone(), wal).unwrap());
        let app = grpc::router_with_handler(shard.clone(), readers.clone(), handler.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let served = app.clone();
        let server = tokio::spawn(async move { axum::serve(listener, served).await.unwrap() });
        Self {
            url,
            app,
            client: Client::new(),
            handler,
            writer,
            readers,
            shard,
            server,
        }
    }

    pub async fn flush(&self) {
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                self.handler.ask(FlushToShard {}).await.unwrap();
                if self.client.get_write_backlog(&self.url).await.unwrap()["total"] == 0 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .expect("writes did not reach SQLite");
    }

    pub async fn stop(self) {
        self.flush().await;
        self.server.abort();
        let _ = self.server.await;
        self.handler.stop_gracefully().await.unwrap();
        self.handler
            .wait_for_shutdown_with_result(|result| {
                result.unwrap();
            })
            .await;
        self.writer.stop_gracefully().await.unwrap();
        self.writer.wait_for_shutdown().await;
        self.readers.stop_gracefully().await.unwrap();
        self.readers.wait_for_shutdown().await;
        self.shard.stop_gracefully().await.unwrap();
        self.shard.wait_for_shutdown().await;
    }
}
