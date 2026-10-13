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
pub struct Timings {
    operations: Mutex<BTreeMap<&'static str, Operation>>,
}

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

impl Timings {
    pub fn snapshot(&self) -> serde_json::Value {
        serde_json::to_value(&*self.operations.lock().unwrap()).unwrap()
    }

    pub fn reset(&self) {
        self.operations.lock().unwrap().clear();
    }

    pub(crate) async fn run<F, R>(
        &self,
        operation: &'static str,
        enabled: bool,
        future: F,
    ) -> Response
    where
        F: Future<Output = R>,
        R: IntoResponse,
    {
        if !enabled {
            return future.await.into_response();
        }
        REQUEST
            .scope(RefCell::new(BTreeMap::new()), async {
                let mut response = {
                    let _total = measure("total");
                    future.await.into_response()
                };
                REQUEST.with(|request| {
                    let stages = request.borrow();
                    let header = stages
                        .iter()
                        .map(|(stage, sample)| format!("{stage};dur={:.3}", sample.total_ms))
                        .collect::<Vec<_>>()
                        .join(", ");
                    response
                        .headers_mut()
                        .insert("server-timing", HeaderValue::from_str(&header).unwrap());
                    let mut operations = self.operations.lock().unwrap();
                    let aggregate = operations.entry(operation).or_default();
                    aggregate.requests += 1;
                    aggregate.errors += u64::from(
                        response.status().is_client_error() || response.status().is_server_error(),
                    );
                    for (stage, sample) in stages.iter() {
                        aggregate
                            .stages
                            .entry(stage)
                            .or_default()
                            .add(sample.calls, sample.total_ms);
                    }
                });
                response
            })
            .await
    }
}
