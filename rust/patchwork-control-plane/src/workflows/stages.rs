use anyhow::{Context, Result};
use cano::{CancellationToken, CanoError, Workflow};
use kameo::actor::ActorRef;
use kameo_actors::pool::{ActorPool, Dispatch};
use std::{fmt::Debug, future::Future, hash::Hash};
use tokio::sync::Mutex;

use crate::{
    actors::{PartitionActor, parition_lock::FreeLock},
    datamodels::parition_lock::Model,
};

/// Every execution gets its own state. Only that workflow's steps share it.
#[derive(cano::Resource)]
pub(crate) struct RunState<T: Send + 'static>(pub Mutex<T>);

pub(crate) fn task_error(error: anyhow::Error) -> CanoError {
    CanoError::TaskExecution(format!("{error:#}"))
}

pub(crate) trait Cleanup: Send + 'static {
    fn cleanup(&mut self) -> impl Future<Output = Result<()>> + Send;
}

/// Cano catches step failures and panics; both still reach the final unlock attempt.
pub(crate) async fn execute<S, R>(workflow: Workflow<S>, start: S, run: &RunState<R>) -> Result<()>
where
    S: Clone + Debug + Eq + Hash + Send + Sync + 'static,
    R: Cleanup,
{
    let result = workflow
        .orchestrate(start, CancellationToken::disabled())
        .await
        .map(|_| ())
        .map_err(anyhow::Error::from);
    let released = run.0.lock().await.cleanup().await;
    finish(result, released)
}

/// List each step once. Step methods still choose their next state explicitly.
/// Cleanup runs after success, failure, or a caught panic. Use "none" for no cleanup.
macro_rules! workflow_steps {
    (
        run: $run:ident,
        state: $state:ident,
        start: $start:ident,
        done: $done:ident,
        cleanup: $cleanup:ident,
        $($step:ident => $method:ident),+ $(,)?
    ) => {
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
        pub enum $state { $($step,)+ $done }

        #[cano::task]
        impl ::cano::Task<$state> for $state {
            fn config(&self) -> ::cano::TaskConfig {
                // Copying and metadata changes must not be retried automatically.
                ::cano::TaskConfig::minimal()
            }

            async fn run(
                &self,
                resources: &::cano::Resources,
            ) -> ::std::result::Result<::cano::TaskResult<$state>, ::cano::CanoError> {
                let run = resources.get::<$crate::workflows::stages::RunState<$run>, _>("run")?;
                let mut run = run.0.lock().await;
                let next = match self {
                    $(Self::$step => run.$method().await,)+
                    Self::$done => Ok(Self::$done),
                }.map_err($crate::workflows::stages::task_error)?;
                Ok(::cano::TaskResult::Single(next))
            }
        }

        impl $crate::workflows::stages::Cleanup for $run {
            async fn cleanup(&mut self) -> ::anyhow::Result<()> {
                workflow_steps!(@cleanup self, $cleanup)
            }
        }

        impl $run {
            async fn execute(self) -> ::anyhow::Result<
                ::std::sync::Arc<$crate::workflows::stages::RunState<Self>>
            > {
                let resources = ::cano::Resources::new().insert(
                    "run",
                    $crate::workflows::stages::RunState(::tokio::sync::Mutex::new(self)),
                );
                let run = resources.get::<$crate::workflows::stages::RunState<Self>, _>("run")?;
                let workflow = ::cano::Workflow::new(resources)
                    $(.register($state::$step, $state::$step))+
                    .add_exit_state($state::$done);
                $crate::workflows::stages::execute(workflow, $state::$start, &run).await?;
                Ok(run)
            }
        }
    };
    (@cleanup $run:ident, none) => { Ok(()) };
    (@cleanup $run:ident, $method:ident) => {{
        $run.$method().await?;
        Ok(())
    }};
}

pub(crate) use workflow_steps;

/// Try every token in reverse order, including after a failed step.
pub(crate) async fn unlock(
    partition: &ActorRef<ActorPool<PartitionActor>>,
    locks: &mut Vec<Model>,
) -> Result<()> {
    let mut errors = Vec::new();
    for lock in locks.drain(..).rev() {
        match partition
            .ask(Dispatch(FreeLock {
                table_id: lock.table_id,
                hash_start: lock.hash_start,
                lock_token: lock.lock_token,
            }))
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
