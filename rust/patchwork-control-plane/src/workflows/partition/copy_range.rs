use anyhow::{Result, ensure};
use kameo::actor::ActorRef;

use crate::{
    actors::NodeActor,
    dataplane_client::Client,
    workflows::data::{
        fetch_range::{FetchRange, FetchRangeCtx},
        put_one_consistent::PutOneConsistent,
    },
};

// Control-plane copy: read every source before writing, retaining timestamps
// and tombstones. Destinations need not be published in the partition map yet.
pub(super) async fn copy_range(
    client: &Client,
    node: &ActorRef<NodeActor>,
    ctx: FetchRangeCtx,
    sources: &[i64],
    targets: &[i64],
    lease_end: i64,
    partition_id: &str,
) -> Result<()> {
    check_lease(lease_end)?;
    ensure!(!targets.is_empty(), "Copy has no destinations");
    let records = FetchRange::from_replicas(client, node, &ctx, sources).await?;
    for mut record in records {
        record.partition_id = Some(partition_id.to_owned());
        check_lease(lease_end)?;
        PutOneConsistent::to_replicas(client, node, &record, targets).await?;
    }
    check_lease(lease_end)
}

fn check_lease(lease_end: i64) -> Result<()> {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_secs() as i64;
    ensure!(now < lease_end, "Partition copy lease expired");
    Ok(())
}

#[cfg(test)]
mod tests;
