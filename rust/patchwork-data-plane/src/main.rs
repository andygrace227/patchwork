use anyhow::{Context, Result};
use axum::serve::ListenerExt;
use kameo::actor::Spawn;
use patchwork_data_plane::{
    api,
    replica_writer::ReplicaWriter,
    telemetry::{
        AccessStatsActor,
        access_stats_actor::{FLUSH_INTERVAL, Flush, RETENTION},
    },
    write_handler::WriteHandler,
    write_shard::WriteShard,
};

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    match dotenvy::dotenv() {
        Ok(_) => {}
        Err(dotenvy::Error::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error).context("Could not load .env"),
    }
    let bind_addr = std::env::var("BIND_ADDR").unwrap_or_else(|_| "127.0.0.1:3000".into());
    let shard_path =
        std::env::var("SHARD_PATH").context("Set SHARD_PATH in .env or the environment")?;
    let pool_size = std::env::var("READ_SHARD_POOL_SIZE")
        .unwrap_or_else(|_| "4".into())
        .parse::<usize>()
        .context("READ_SHARD_POOL_SIZE must be a positive integer")?;
    let stats_path = std::env::var("ACCESS_STATS_PATH")
        .unwrap_or_else(|_| format!("{shard_path}.access-stats.sqlite"));
    let flush_interval = std::env::var("ACCESS_STATS_FLUSH_SECONDS")
        .unwrap_or_else(|_| FLUSH_INTERVAL.as_secs().to_string())
        .parse::<u64>()
        .context("ACCESS_STATS_FLUSH_SECONDS must be a positive integer")?;
    let retention = std::env::var("ACCESS_STATS_RETENTION_SECONDS")
        .unwrap_or_else(|_| RETENTION.as_secs().to_string())
        .parse::<u64>()
        .context("ACCESS_STATS_RETENTION_SECONDS must be a positive integer")?;
    let access_stats = AccessStatsActor::spawn(
        AccessStatsActor::with_config(
            stats_path,
            std::time::Duration::from_secs(flush_interval),
            std::time::Duration::from_secs(retention),
        )
        .await?,
    );
    let wal_path = std::path::PathBuf::from(format!("{shard_path}.wal"));
    let shard = WriteShard::with_access_stats(shard_path, access_stats.clone()).await?;
    let readers = shard.read_pool(pool_size)?;
    let shard = WriteShard::spawn(shard);
    let writer = ReplicaWriter::spawn(ReplicaWriter::default());
    let handler = WriteHandler::spawn(WriteHandler::new(shard.clone(), writer.clone(), &wal_path)?);
    handler
        .wait_for_startup_with_result(|result| {
            result
                .map(|_| ())
                .map_err(|error| anyhow::anyhow!("{error}"))
        })
        .await?;
    let listener = tokio::net::TcpListener::bind(&bind_addr).await?;
    eprintln!(
        "Data plane gRPC and HTTP listening on {} with {} Tokio worker threads and {} read actors",
        listener.local_addr()?,
        tokio::runtime::Handle::current().metrics().num_workers(),
        pool_size,
    );
    let result = axum::serve(
        listener.tap_io(|stream| {
            if let Err(error) = stream.set_nodelay(true) {
                eprintln!("Could not enable TCP_NODELAY: {error}");
            }
        }),
        api::router_with_handler(shard.clone(), readers.clone(), handler.clone()),
    )
    .with_graceful_shutdown(async {
        let _ = tokio::signal::ctrl_c().await;
    })
    .await;
    handler.stop_gracefully().await?;
    handler
        .wait_for_shutdown_with_result(|result| {
            result
                .map(|_| ())
                .map_err(|error| anyhow::anyhow!("{error}"))
        })
        .await?;
    writer.stop_gracefully().await?;
    writer.wait_for_shutdown().await;
    readers.stop_gracefully().await?;
    readers.wait_for_shutdown().await;
    shard.stop_gracefully().await?;
    shard.wait_for_shutdown().await;
    access_stats
        .ask(Flush {})
        .await
        .map_err(|error| anyhow::anyhow!("{error}"))?;
    access_stats.stop_gracefully().await?;
    access_stats.wait_for_shutdown().await;
    result?;
    Ok(())
}
