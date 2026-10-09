use anyhow::{Context, Result};
use cano::CanoError;
use kameo::actor::ActorRef;
use tokio::sync::Mutex;

use crate::db::{
    PartitionActor,
    parition_lock::{FreeLock, Model},
};

/// Every execution gets its own state. Only that workflow's steps share it.
#[derive(cano::Resource)]
pub(crate) struct RunState<T: Send + 'static>(pub Mutex<T>);

pub(crate) fn task_error(error: anyhow::Error) -> CanoError {
    CanoError::TaskExecution(format!("{error:#}"))
}

/// Try every token in reverse order, including after a failed step.
pub(crate) async fn unlock(
    partition: &ActorRef<PartitionActor>,
    locks: &mut Vec<Model>,
) -> Result<()> {
    let mut errors = Vec::new();
    for lock in locks.drain(..).rev() {
        match partition
            .ask(FreeLock {
                table_id: lock.table_id,
                hash_start: lock.hash_start,
                lock_token: lock.lock_token,
            })
            .await
        {
            Ok(true) => {}
            Ok(false) => {
                errors.push("Partition lock no longer belongs to this workflow".to_owned())
            }
            Err(error) => errors.push(error.to_string()),
        }
    }
    anyhow::ensure!(
        errors.is_empty(),
        "Failed to release leases: {}",
        errors.join("; ")
    );
    Ok(())
}

pub(crate) fn finish<T>(result: Result<T>, unlock: Result<()>) -> Result<T> {
    unlock.with_context(|| match &result {
        Ok(_) => "Workflow completed, but unlocking failed".to_owned(),
        Err(error) => format!("Workflow failed: {error:#}; unlocking also failed"),
    })?;
    result
}
