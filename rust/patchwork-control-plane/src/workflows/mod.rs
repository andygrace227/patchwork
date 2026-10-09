pub mod data;
pub mod executor;
pub mod job;
pub mod partition;
mod stages;
pub mod table;

use std::future::Future;

use anyhow::Result;

/// Adapter for queue keys and typed results; Cano runs each workflow's steps.
pub trait Workflow: Send + 'static {
    type Context: Send + 'static;
    type Output: Send + Sync + 'static;

    fn key(&self, ctx: &Self::Context) -> String;
    fn table_id(&self, ctx: &Self::Context) -> Option<i64>;
    fn run(self, ctx: Self::Context) -> impl Future<Output = Result<Self::Output>> + Send;
}
