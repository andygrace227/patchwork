use kameo::actor::{ActorRef, Spawn};
use patchwork_common::dataplane_client::Client;
use patchwork_data_plane::{service::Service, telemetry::AccessStatsActor};
use std::{path::Path, time::Duration};
use tokio_stream::wrappers::TcpListenerStream;

pub struct Node {
    pub url: String,
    pub client: Client,
    #[allow(dead_code)]
    pub app: patchwork_common::grpc::proto::data_plane_server::DataPlaneServer<Service>,
    service: Service,
    stats: ActorRef<AccessStatsActor>,
    owns_stats: bool,
    server: tokio::task::JoinHandle<()>,
}

impl Node {
    pub async fn start(path: &Path, wal: &Path, stats: Option<ActorRef<AccessStatsActor>>) -> Self {
        let owns_stats = stats.is_none();
        let stats = match stats {
            Some(stats) => stats,
            None => AccessStatsActor::spawn(
                AccessStatsActor::new(format!("{}.access-stats.sqlite", path.display()))
                    .await
                    .unwrap(),
            ),
        };
        let service = Service::new(path, wal, stats.clone(), 2).await.unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let served = service.server();
        let app = service.server();
        let server = tokio::spawn(async move {
            tonic::transport::Server::builder()
                .serve_with_incoming(served, TcpListenerStream::new(listener))
                .await
                .unwrap();
        });
        Self {
            url,
            client: Client::new(),
            app,
            service,
            stats,
            owns_stats,
            server,
        }
    }

    pub async fn flush(&self) {
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
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
        self.service.shutdown().await.unwrap();
        if self.owns_stats {
            self.stats.stop_gracefully().await.unwrap();
            self.stats
                .wait_for_shutdown_with_result(|result| {
                    result.unwrap();
                })
                .await;
        }
    }
}
