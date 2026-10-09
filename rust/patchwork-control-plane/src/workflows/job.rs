use std::sync::Arc;

use anyhow::anyhow;
use tokio::sync::watch;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum JobStatus {
    Queued,
    Running,
    Succeeded,
    Failed,
}

enum JobState<T> {
    Queued,
    Running,
    Succeeded(Arc<T>),
    Failed(Arc<anyhow::Error>),
}

impl<T> JobState<T> {
    fn status(&self) -> JobStatus {
        match self {
            Self::Queued => JobStatus::Queued,
            Self::Running => JobStatus::Running,
            Self::Succeeded(_) => JobStatus::Succeeded,
            Self::Failed(_) => JobStatus::Failed,
        }
    }

    fn result(&self) -> Option<Result<Arc<T>, Arc<anyhow::Error>>> {
        match self {
            Self::Succeeded(value) => Some(Ok(value.clone())),
            Self::Failed(error) => Some(Err(error.clone())),
            _ => None,
        }
    }
}

/// Cloning a handle shares the same status and result; it does not rerun the job.
pub struct JobHandle<T> {
    state: watch::Receiver<JobState<T>>,
}

impl<T> Clone for JobHandle<T> {
    fn clone(&self) -> Self {
        Self {
            state: self.state.clone(),
        }
    }
}

impl<T> JobHandle<T> {
    pub fn status(&self) -> JobStatus {
        self.state.borrow().status()
    }

    /// True for both successful and failed jobs.
    pub fn is_finished(&self) -> bool {
        matches!(self.status(), JobStatus::Succeeded | JobStatus::Failed)
    }

    pub fn is_error(&self) -> bool {
        self.status() == JobStatus::Failed
    }

    pub fn try_result(&self) -> Option<Result<Arc<T>, Arc<anyhow::Error>>> {
        self.state.borrow().result()
    }

    /// Wait for completion. The stored result can be read by multiple callers.
    pub async fn result(&self) -> Result<Arc<T>, Arc<anyhow::Error>> {
        let mut state = self.state.clone();
        loop {
            if let Some(result) = state.borrow_and_update().result() {
                return result;
            }
            if state.changed().await.is_err() {
                return Err(Arc::new(anyhow!(
                    "Workflow stopped before reporting its result"
                )));
            }
        }
    }
}

pub(crate) struct JobCompletion<T> {
    state: watch::Sender<JobState<T>>,
}

impl<T> JobCompletion<T> {
    pub fn new() -> (JobHandle<T>, Self) {
        let (state, receiver) = watch::channel(JobState::Queued);
        (JobHandle { state: receiver }, Self { state })
    }

    pub fn running(&self) {
        self.state.send_replace(JobState::Running);
    }

    pub fn finish(&self, result: anyhow::Result<T>) -> Result<(), Arc<anyhow::Error>> {
        match result {
            Ok(value) => {
                self.state
                    .send_replace(JobState::Succeeded(Arc::new(value)));
                Ok(())
            }
            Err(error) => {
                let error = Arc::new(error);
                self.state.send_replace(JobState::Failed(error.clone()));
                Err(error)
            }
        }
    }
}

impl<T> Drop for JobCompletion<T> {
    fn drop(&mut self) {
        let finished = self.state.borrow().result().is_some();
        if !finished {
            self.state.send_replace(JobState::Failed(Arc::new(anyhow!(
                "Workflow was dropped before completion"
            ))));
        }
    }
}
