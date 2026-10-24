use anyhow::Result;
use kameo::actor::Spawn;
use patchwork_data_plane::{config::Config, service::Service, telemetry::AccessStatsActor};
use tonic::transport::Server;

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    let config = Config::from_env()?;
    let stats = AccessStatsActor::spawn(
        AccessStatsActor::with_config(
            &config.stats_path,
            config.stats_flush_interval,
            config.stats_retention,
        )
        .await?,
    );
    let service = Service::new(
        &config.shard_path,
        &config.wal_path,
        stats.clone(),
        config.read_connections,
    )
    .await?;
    eprintln!(
        "Data plane gRPC listening on {} with {} read connections",
        config.bind_addr, config.read_connections
    );
    let result = Server::builder()
        .tcp_nodelay(true)
        .serve_with_shutdown(config.bind_addr, service.server(), async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await;
    service.shutdown().await?;
    stats.stop_gracefully().await?;
    stats
        .wait_for_shutdown_with_result(|result| {
            result
                .map(|_| ())
                .map_err(|error| anyhow::anyhow!("{error}"))
        })
        .await?;
    result?;
    Ok(())
}
