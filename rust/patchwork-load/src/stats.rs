use crate::{config::Config, request::Completion};
use anyhow::Result;
use serde_json::{Value, json};
use std::{
    borrow::Cow,
    collections::{BTreeMap, VecDeque},
    io::Write,
    path::PathBuf,
    time::Duration,
};
use tokio::time::Instant;

#[derive(Default)]
pub(crate) struct Stats {
    pub phase: &'static str,
    pub started: Option<Instant>,
    pub finished: Option<Instant>,
    counts: BTreeMap<&'static str, u64>,
    statuses: BTreeMap<Cow<'static, str>, u64>,
    latencies: VecDeque<f64>,
    recent: VecDeque<(Instant, u64)>,
    pub last_error: String,
}

impl Stats {
    pub fn inc(&mut self, key: &'static str, count: u64) {
        *self.counts.entry(key).or_default() += count;
    }
    pub fn count(&self, key: &str) -> u64 {
        self.counts.get(key).copied().unwrap_or(0)
    }

    pub fn record(&mut self, result: Completion) -> bool {
        if !result.error.is_empty() {
            self.last_error = result.error.clone();
        }
        if result.seed {
            self.inc("seeded", 1);
            if !["204", "OK"].contains(&result.status.as_ref()) {
                self.inc("seed_errors", 1);
                return false;
            }
        } else {
            self.inc("completed", 1);
            self.inc(if result.read { "reads" } else { "writes" }, 1);
            self.inc(
                if !result.error.is_empty() {
                    "errors"
                } else if ["404", "NotFound"].contains(&result.status.as_ref()) {
                    "misses"
                } else {
                    "successes"
                },
                1,
            );
            *self.statuses.entry(result.status).or_default() += 1;
            if self.latencies.len() == 10_000 {
                self.latencies.pop_front();
            }
            self.latencies.push_back(result.latency);
        }
        true
    }

    /// Only batches cross between request loops and the reporter.
    pub fn merge(&mut self, mut batch: Self) {
        let completed = batch.count("completed");
        if completed > 0 {
            self.recent.push_back((Instant::now(), completed));
        }
        for (key, count) in batch.counts {
            self.inc(key, count);
        }
        for (status, count) in batch.statuses {
            *self.statuses.entry(status).or_default() += count;
        }
        self.latencies.append(&mut batch.latencies);
        let discard = self.latencies.len().saturating_sub(10_000);
        self.latencies.drain(..discard);
        if !batch.last_error.is_empty() {
            self.last_error = batch.last_error;
        }
        self.trim_recent();
    }

    pub fn in_flight(&self) -> usize {
        (self.count("started") + self.count("seed_started"))
            .saturating_sub(self.count("completed") + self.count("seeded")) as usize
    }

    pub fn trim_recent(&mut self) {
        let cutoff = Instant::now() - Duration::from_secs(1);
        while self.recent.front().is_some_and(|(time, _)| *time < cutoff) {
            self.recent.pop_front();
        }
    }

    pub fn snapshot(
        &mut self,
        config: &Config,
        in_flight: usize,
        report: Option<&PathBuf>,
    ) -> Value {
        self.trim_recent();
        let elapsed = self.started.map_or(0.0, |start| {
            self.finished
                .unwrap_or_else(Instant::now)
                .duration_since(start)
                .as_secs_f64()
        });
        let mut samples: Vec<_> = self.latencies.iter().copied().collect();
        let p95 = if samples.is_empty() {
            0.0
        } else {
            let index = (samples.len() - 1) * 95 / 100;
            *samples.select_nth_unstable_by(index, f64::total_cmp).1
        };
        let mut counts = self.counts.clone();
        counts.insert("in_flight", in_flight as u64);
        json!({"engine": "rust", "phase": self.phase, "running": (["seeding", "running", "draining", "stopping"].contains(&self.phase)),
            "config": config.values, "counts": counts, "statuses": self.statuses,
            "elapsed": (elapsed * 100.0).round() / 100.0,
            "current_tps": if elapsed > 0.0 { (self.recent.iter().map(|(_, count)| count).sum::<u64>() as f64 / elapsed.min(1.0) * 10.0).round() / 10.0 } else { 0.0 },
            "average_tps": if elapsed > 0.0 { (self.count("completed") as f64 / elapsed * 10.0).round() / 10.0 } else { 0.0 },
            "p95_ms": (p95 * 100.0).round() / 100.0, "last_error": self.last_error, "report": report})
    }
}

pub(crate) fn emit(value: &Value) -> Result<()> {
    let mut stdout = std::io::stdout().lock();
    serde_json::to_writer(&mut stdout, value)?;
    writeln!(stdout)?;
    stdout.flush()?;
    Ok(())
}
