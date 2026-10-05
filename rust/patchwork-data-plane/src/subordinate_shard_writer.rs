use kameo::{Actor, messages};
use serde_json::Value;

/// In-memory queue of best-effort replica writes. Pending writes are lost on a crash.
#[derive(Actor)]
pub struct SubordinateShardWriter;

#[messages]
impl SubordinateShardWriter {
    #[message]
    pub async fn put(
        &mut self,
        table_id: i64,
        partition_key: i64,
        secondary_key: i64,
        data: Value,
        replicas: Vec<String>,
    ) {
        if let Err(error) =
            crate::replicas::put(table_id, partition_key, secondary_key, data, replicas).await
        {
            eprintln!(
                "Queued replica write failed for {table_id}/{partition_key}/{secondary_key}: {error}"
            );
        }
    }
}
