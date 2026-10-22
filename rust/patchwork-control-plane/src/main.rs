use anyhow::{Context, Result, ensure};
use kameo::actor::Spawn;
use kameo_actors::pool::ActorPool;
use patchwork_control_plane::{
    actors::{NodeActor, PartitionActor, TableActor},
    api,
    auth::Auth,
    clients::{NodeClient, PartitionClient, TableClient},
    workflows::executor::LongRunningWorkflowExecutor,
};
use sea_orm::{ConnectOptions, Database};

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    match dotenvy::dotenv() {
        Ok(_) => {}
        Err(dotenvy::Error::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error).context("Could not load .env"),
    }
    let auth = Auth::from_env()?;
    let bind_addr = std::env::var("BIND_ADDR").unwrap_or_else(|_| "127.0.0.1:3001".into());
    let database_url =
        std::env::var("DATABASE_URL").context("Set DATABASE_URL in .env or the environment")?;

    let pool_size = std::env::var("METADATA_ACTOR_POOL_SIZE")
        .unwrap_or_else(|_| "2".into())
        .parse::<usize>()
        .context("METADATA_ACTOR_POOL_SIZE must be a positive integer")?;
    ensure!(pool_size > 0, "METADATA_ACTOR_POOL_SIZE must be positive");
    let db_max_connections = std::env::var("METADATA_DB_MAX_CONNECTIONS")
        .unwrap_or_else(|_| "16".into())
        .parse::<u32>()
        .context("METADATA_DB_MAX_CONNECTIONS must be a positive integer")?;
    ensure!(
        db_max_connections > 0,
        "METADATA_DB_MAX_CONNECTIONS must be positive"
    );
    let mut options = ConnectOptions::new(database_url);
    options
        .min_connections(1)
        .max_connections(db_max_connections);
    // DatabaseConnection clones share this one SQLx pool across all workers.
    let db = Database::connect(options).await?;

    let partition = ActorPool::spawn(ActorPool::new(pool_size, {
        let db = db.clone();
        move || PartitionActor::spawn(PartitionActor::new(db.clone()))
    }));

    let node = ActorPool::spawn(ActorPool::new(pool_size, {
        let db = db.clone();
        move || NodeActor::spawn(NodeActor::new(db.clone()))
    }));

    let table = ActorPool::spawn(ActorPool::new(pool_size, {
        move || TableActor::spawn(TableActor::new(db.clone()))
    }));

    let node = NodeClient::new(node);
    let partition = PartitionClient::new(partition);
    let table = TableClient::new(table);

    let executor = LongRunningWorkflowExecutor::spawn(LongRunningWorkflowExecutor::new(
        node.clone(),
        partition.clone(),
        table.clone(),
    ));

    let listener = tokio::net::TcpListener::bind(&bind_addr).await?;
    eprintln!(
        "Control plane listening on {} with {} Tokio worker threads, {pool_size} workers per metadata actor, and up to {db_max_connections} metadata DB connections",
        listener.local_addr()?,
        tokio::runtime::Handle::current().metrics().num_workers(),
    );
    let result = axum::serve(
        listener,
        api::router(node, partition, table, executor.clone(), auth),
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
