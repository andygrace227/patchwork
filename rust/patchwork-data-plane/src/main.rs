use anyhow::{Context, Result};
use axum::serve::ListenerExt;
use kameo::actor::Spawn;
use patchwork_data_plane::{
    api, shard_actor::ShardActor, subordinate_shard_writer::SubordinateShardWriter,
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
    let shard = ShardActor::new(shard_path).await?;
    let readers = shard.read_pool(pool_size)?;
    let shard = ShardActor::spawn(shard);
    let writer = SubordinateShardWriter::spawn(SubordinateShardWriter);
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
        api::router_with_writer(shard.clone(), readers.clone(), writer.clone()),
    )
    .with_graceful_shutdown(async {
        let _ = tokio::signal::ctrl_c().await;
    })
    .await;
    writer.stop_gracefully().await?;
    writer.wait_for_shutdown().await;
    readers.stop_gracefully().await?;
    readers.wait_for_shutdown().await;
    shard.stop_gracefully().await?;
    shard.wait_for_shutdown().await;
    result?;
    Ok(())
}
