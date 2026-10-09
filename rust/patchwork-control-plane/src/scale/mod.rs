use std::time::Duration;

use cano::{
    CancellationToken, CanoError, Resources, TaskConfig, TaskResult, TimerOutcome, Workflow,
};
use kameo::actor::{ActorRef, WeakActorRef};

use crate::{
    db::{NodeActor, PartitionActor, TableActor, table::GetRandomReadyTable},
    workflows::{
        executor::{LongRunningWorkflowExecutor, SubmitWorkflow},
        table::check_table_scale::{CheckTableScale, CheckTableScaleContext},
    },
};

/// Each control node samples independently; partition workflows retain their DB locks.
pub(crate) async fn run_random_scale_checks(
    node: ActorRef<NodeActor>,
    partition: ActorRef<PartitionActor>,
    table: ActorRef<TableActor>,
    executor: WeakActorRef<LongRunningWorkflowExecutor>,
    min_interval: Duration,
    max_interval: Duration,
) {
    let workflow = Workflow::bare()
        .register(
            State::Wait,
            RandomDelay {
                min_interval,
                max_interval,
            },
        )
        .register(
            State::Submit,
            SubmitRandomCheck {
                node,
                partition,
                table,
                executor,
            },
        )
        .add_exit_state(State::Done);
    if let Err(error) = workflow
        .orchestrate(State::Wait, CancellationToken::disabled())
        .await
    {
        eprintln!("Random scale checker stopped: {error}");
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum State {
    Wait,
    Submit,
    Done,
}

struct RandomDelay {
    min_interval: Duration,
    max_interval: Duration,
}

#[cano::task::timer(state = State)]
impl RandomDelay {
    async fn wait(&self, _: &Resources) -> Result<TimerOutcome, CanoError> {
        let nanos = rand::random_range(self.min_interval.as_nanos()..=self.max_interval.as_nanos());
        Ok(TimerOutcome::Duration(Duration::new(
            (nanos / 1_000_000_000) as u64,
            (nanos % 1_000_000_000) as u32,
        )))
    }

    async fn after_wait(&self, _: &Resources) -> Result<TaskResult<State>, CanoError> {
        Ok(TaskResult::Single(State::Submit))
    }
}

struct SubmitRandomCheck {
    node: ActorRef<NodeActor>,
    partition: ActorRef<PartitionActor>,
    table: ActorRef<TableActor>,
    executor: WeakActorRef<LongRunningWorkflowExecutor>,
}

#[cano::task(state = State)]
impl SubmitRandomCheck {
    fn config(&self) -> TaskConfig {
        TaskConfig::minimal()
    }

    async fn run_bare(&self) -> Result<TaskResult<State>, CanoError> {
        let Some(executor) = self.executor.upgrade() else {
            return Ok(TaskResult::Single(State::Done));
        };
        match self.table.ask(GetRandomReadyTable).await {
            Ok(Some(picked)) => {
                if let Err(error) = executor
                    .ask(SubmitWorkflow::new(
                        CheckTableScale {
                            node: self.node.clone(),
                            partition: self.partition.clone(),
                            table: self.table.clone(),
                            orchestrator: executor.clone(),
                        },
                        CheckTableScaleContext {
                            table_id: picked.table_id,
                        },
                    ))
                    .await
                {
                    eprintln!("Could not queue scaling check: {error}");
                }
            }
            Ok(None) => {}
            Err(error) => eprintln!("Could not select a table for scaling: {error}"),
        }
        Ok(TaskResult::Single(State::Wait))
    }
}
