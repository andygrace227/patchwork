//! Prepare a fresh local test database using the same models as the servers.
use anyhow::Result;
use patchwork_control_plane::datamodels::{node, parition_lock, partition, table};
use sea_orm::{ActiveModelTrait, ActiveValue::Set, ConnectionTrait, Database, Schema};

#[tokio::main]
async fn main() -> Result<()> {
    let db = Database::connect(std::env::var("DATABASE_URL")?).await?;
    let schema = Schema::new(db.get_database_backend());
    for statement in [
        schema.create_table_from_entity(node::Entity),
        schema.create_table_from_entity(table::Entity),
        schema.create_table_from_entity(partition::Entity),
        schema.create_table_from_entity(parition_lock::Entity),
    ] {
        db.execute(&statement).await?;
    }
    let urls: Vec<String> = serde_json::from_str(&std::env::var("DATA_NODE_URLS")?)?;
    for url in urls {
        node::ActiveModel {
            url: Set(url),
            ..Default::default()
        }
        .insert(&db)
        .await?;
    }
    db.close().await?;
    Ok(())
}
