use std::time::Duration;

use kameo::actor::{ActorRef, WeakActorRef};

use crate::{
    db::{TableActor, table::GetRandomReadyTable},
    workflows::{
        executor::{LongRunningWorkflowExecutor, SubmitWorkflow, Workflow},
        table::check_table_scale::CheckTableScaleContext,
    },
};

/// Each control node samples independently; partition workflows retain their DB locks.
pub(crate) async fn run_random_scale_checks(
    table: ActorRef<TableActor>,
    executor: WeakActorRef<LongRunningWorkflowExecutor>,
    min_interval: Duration,
    max_interval: Duration,
) {
    loop {
        let nanos = rand::random_range(min_interval.as_nanos()..=max_interval.as_nanos());
        let delay = Duration::new(
            (nanos / 1_000_000_000) as u64,
            (nanos % 1_000_000_000) as u32,
        );
        tokio::time::sleep(delay).await;
        let Some(executor) = executor.upgrade() else {
            break;
        };
        match table.ask(GetRandomReadyTable).await {
            Ok(Some(table)) => {
                if let Err(error) = executor
                    .ask(SubmitWorkflow {
                        workflow: Workflow::CheckTableScale(CheckTableScaleContext {
                            table_id: table.table_id,
                        }),
                    })
                    .await
                {
                    eprintln!("Could not queue scaling check: {error}");
                }
            }
            Ok(None) => {}
            Err(error) => eprintln!("Could not select a table for scaling: {error}"),
        }
    }
}
