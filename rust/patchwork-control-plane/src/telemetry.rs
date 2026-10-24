//! Opt-in HTTP workflow timings. No timers or shared writes on unmeasured requests.
use std::{
    cell::RefCell,
    collections::BTreeMap,
    future::{Future, IntoFuture},
    sync::Mutex,
    time::Instant,
};

use axum::{
    http::HeaderValue,
    response::{IntoResponse, Response},
};
use serde::Serialize;


#[derive(Default, Serialize)]
struct Operation {
    requests: u64,
    errors: u64,
    stages: BTreeMap<&'static str, Sample>,
}

#[derive(Default, Serialize)]
struct Sample {
    calls: u64,
    total_ms: f64,
    average_ms: f64,
}

impl Sample {
    fn add(&mut self, calls: u64, ms: f64) {
        self.calls += calls;
        self.total_ms += ms;
        self.average_ms = self.total_ms / self.calls as f64;
    }
}

tokio::task_local! {
    static REQUEST: RefCell<BTreeMap<&'static str, Sample>>;
}

pub(crate) struct Timer(Option<(&'static str, Instant)>);

pub(crate) fn measure(stage: &'static str) -> Timer {
    Timer(REQUEST.try_with(|_| (stage, Instant::now())).ok())
}

impl Drop for Timer {
    fn drop(&mut self) {
        if let Some((stage, started)) = self.0 {
            let _ = REQUEST.try_with(|request| {
                request
                    .borrow_mut()
                    .entry(stage)
                    .or_default()
                    .add(1, started.elapsed().as_secs_f64() * 1000.0);
            });
        }
    }
}

pub(crate) async fn time<F: IntoFuture>(stage: &'static str, future: F) -> F::Output {
    let _timer = measure(stage);
    future.await
}
