use anyhow::{Context, Result};
use kameo::actor::Spawn;
use patchwork_control_plane::{
    api,
    db::{NodeActor, PartitionActor, TableActor},
    workflows::executor::LongRunningWorkflowExecutor,
};
use sea_orm::Database;

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    match dotenvy::dotenv() {
        Ok(_) => {}
        Err(dotenvy::Error::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error).context("Could not load .env"),
    }
    let bind_addr = std::env::var("BIND_ADDR").unwrap_or_else(|_| "127.0.0.1:3001".into());
    let database_url =
        std::env::var("DATABASE_URL").context("Set DATABASE_URL in .env or the environment")?;
    let db = Database::connect(database_url).await?;
    let listener = tokio::net::TcpListener::bind(&bind_addr).await?;
    let node = NodeActor::spawn(NodeActor::new(db.clone()));
    let partition = PartitionActor::spawn(PartitionActor::new(db.clone()));
    let table = TableActor::spawn(TableActor::new(db));
    let executor = LongRunningWorkflowExecutor::spawn(LongRunningWorkflowExecutor::new(
        node.clone(),
        partition.clone(),
        table.clone(),
    ));
    eprintln!(
        "Control plane listening on {} with {} Tokio worker threads",
        listener.local_addr()?,
        tokio::runtime::Handle::current().metrics().num_workers(),
    );
    let result = axum::serve(
        listener,
        api::router(node, partition, table, executor.clone()),
    )
    .with_graceful_shutdown(async {
        let _ = tokio::signal::ctrl_c().await;
    })
    .await;
    executor.stop_gracefully().await?;
    executor.wait_for_shutdown().await;
    result?;
    Ok(())
}
