use anyhow::{Context, Result};
use kameo::actor::Spawn;
use patchwork_data_plane::{api, shard_actor::ShardActor};

#[tokio::main]
async fn main() -> Result<()> {
    match dotenvy::dotenv() {
        Ok(_) => {}
        Err(dotenvy::Error::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error).context("Could not load .env"),
    }
    let bind_addr = std::env::var("BIND_ADDR").unwrap_or_else(|_| "127.0.0.1:3000".into());
    let shard_path =
        std::env::var("SHARD_PATH").context("Set SHARD_PATH in .env or the environment")?;
    let shard = ShardActor::spawn(ShardActor::new(shard_path).await?);
    let listener = tokio::net::TcpListener::bind(&bind_addr).await?;
    eprintln!("Data plane listening on {}", listener.local_addr()?);
    let result = axum::serve(listener, api::router(shard.clone()))
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await;
    shard.stop_gracefully().await?;
    shard.wait_for_shutdown().await;
    result?;
    Ok(())
}
