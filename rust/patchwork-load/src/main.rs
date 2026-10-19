//! Standalone load worker. JSON configuration in, progress JSON out.
use std::{
    collections::{BTreeMap, VecDeque},
    io::{BufRead, Write},
    path::PathBuf,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result, bail, ensure};
use bytes::Bytes;
use rand::Rng;
use reqwest::{Client, Url};
use serde_json::{Value, json};
use tokio::{
    sync::watch,
    task::JoinSet,
    time::{Instant, MissedTickBehavior},
};

struct Config {
    values: Value,
    urls: Vec<Url>,
    workers: usize,
    tps: u64,
    duration: Duration,
    reads: u64,
    records: usize,
    keys: usize,
    hot: Option<i64>,
    record_base: i64,
    seed: bool,
    telemetry: bool,
    consistent: bool,
    run_id: String,
    payload: Bytes,
}

fn number(values: &Value, key: &str, default: i64, min: i64, max: i64) -> Result<i64> {
    let value = match values.get(key) {
        None => default,
        Some(Value::String(value)) => value.parse().with_context(|| format!("Invalid {key}"))?,
        Some(value) => value.as_i64().with_context(|| format!("Invalid {key}"))?,
    };
    ensure!(
        (min..=max).contains(&value),
        "{key} must be between {min} and {max}"
    );
    Ok(value)
}

fn boolean(values: &Value, key: &str, default: bool) -> Result<bool> {
    values.get(key).map_or(Ok(default), |value| {
        value
            .as_bool()
            .with_context(|| format!("{key} must be true or false"))
    })
}

impl Config {
    fn parse(values: Value, urls: &[String]) -> Result<Self> {
        // A downloaded dashboard report can also be used as a configuration file.
        let values = values.get("config").cloned().unwrap_or(values);
        ensure!(values.is_object(), "Send a JSON load configuration");
        ensure!(!urls.is_empty(), "Supply at least one --control-url");
        let table = number(&values, "table_id", 0, 1, i64::MAX)?;
        let tps = number(&values, "tps", 100, 1, 100_000)? as u64;
        let duration = number(&values, "duration", 30, 1, 3600)?;
        let workers = number(&values, "workers", 64, 1, 1024)? as usize;
        let reads = number(&values, "read_percent", 0, 0, 100)? as u64;
        let records = number(&values, "records", 1000, 1, 100_000)? as usize;
        let mut keys = number(&values, "keys", 100, 1, 10_000)? as usize;
        let padding = number(&values, "payload_bytes", 256, 0, 1_048_576)? as usize;
        let hot_key = number(&values, "hot_key", 10, i64::MIN, i64::MAX)?;
        let consistent = boolean(&values, "consistent", true)?;
        let seed = boolean(&values, "seed", true)?;
        let telemetry = boolean(&values, "telemetry", false)?;
        let distribution = values
            .get("distribution")
            .and_then(Value::as_str)
            .unwrap_or("spread");
        ensure!(
            ["spread", "hot"].contains(&distribution),
            "Choose spread or hot key distribution"
        );
        let hot = (distribution == "hot").then_some(hot_key);
        if hot.is_some() {
            keys = 1;
        }
        ensure!(
            keys <= records,
            "Partition keys cannot exceed the number of records"
        );
        let node = values.get("node").and_then(Value::as_str).unwrap_or("all");
        let mut selected = Vec::new();
        for (index, url) in urls.iter().enumerate() {
            let url = Url::parse(url)?;
            ensure!(
                ["http", "https"].contains(&url.scheme()) && url.host_str().is_some(),
                "Control URLs must use HTTP or HTTPS"
            );
            ensure!(
                url.username().is_empty() && url.password().is_none(),
                "Control URLs cannot contain credentials"
            );
            if node == "all" || node == format!("control-{}", index + 1) {
                selected.push(url);
            }
        }
        ensure!(
            !selected.is_empty(),
            "Choose a control node from this cluster"
        );
        let record_base = rand::rng().random_range((1i64 << 61)..(1i64 << 62));
        let run_id = format!("{:016x}", rand::rng().random::<u64>());
        let payload = Bytes::from(serde_json::to_vec(
            &json!({"load_run": run_id, "padding": "x".repeat(padding)}),
        )?);
        let values = json!({"table_id": table.to_string(), "tps": tps, "duration": duration, "workers": workers,
            "read_percent": reads, "records": records, "keys": keys, "payload_bytes": padding,
            "hot_key": hot_key.to_string(), "distribution": distribution, "node": node,
            "consistent": consistent, "seed": seed, "telemetry": telemetry,
            "record_base": record_base.to_string(), "run_id": run_id});
        Ok(Self {
            values,
            urls: selected,
            workers,
            tps,
            duration: Duration::from_secs(duration as u64),
            reads,
            records,
            keys,
            hot,
            record_base,
            seed,
            telemetry,
            consistent,
            run_id,
            payload,
        })
    }

    fn partition_key(&self, index: usize) -> i64 {
        self.hot.unwrap_or_else(|| {
            (i64::MIN as i128
                + (u64::MAX as i128 * (index % self.keys) as i128)
                    / self.keys.saturating_sub(1).max(1) as i128) as i64
        })
    }

    fn record_urls(&self) -> Result<Vec<Vec<Url>>> {
        let table = self.values["table_id"].as_str().unwrap();
        (0..self.records).map(|index| self.urls.iter().map(|base| {
            Ok(base.join(&format!("/tables/{table}/partition-keys/{}/records/{}?consistent={}&telemetry={}",
                self.partition_key(index), self.record_base + index as i64, self.consistent, self.telemetry))?)
        }).collect()).collect()
    }
}

struct Completion {
    status: String,
    read: bool,
    seed: bool,
    latency: f64,
    error: String,
}

async fn request(
    client: Client,
    mut url: Url,
    payload: Bytes,
    read: bool,
    seed: bool,
) -> Completion {
    let started = Instant::now();
    let result = async {
        if seed {
            url.query_pairs_mut()
                .clear()
                .append_pair("consistent", "true")
                .append_pair("telemetry", "false");
        }
        let request = if read {
            client.get(url)
        } else {
            let timestamp = SystemTime::now().duration_since(UNIX_EPOCH)?.as_micros();
            client
                .put(url)
                .header("content-type", "application/json")
                .header("x-patchwork-timestamp", timestamp.to_string())
                .body(payload)
        };
        let response = request.send().await?;
        let status = response.status();
        // Drain the body so the connection can be reused, including read misses.
        let body = response.bytes().await?;
        let error = if status.is_success() || (read && status.as_u16() == 404) {
            String::new()
        } else {
            format!(
                "HTTP {status}: {}",
                String::from_utf8_lossy(&body)
                    .chars()
                    .take(400)
                    .collect::<String>()
            )
        };
        Ok::<_, anyhow::Error>((status.as_u16().to_string(), error))
    }
    .await;
    let (status, error) = result.unwrap_or_else(|error| ("network".into(), format!("{error:#}")));
    Completion {
        status,
        read,
        seed,
        latency: started.elapsed().as_secs_f64() * 1000.0,
        error,
    }
}

#[derive(Default)]
struct Stats {
    phase: &'static str,
    started: Option<Instant>,
    finished: Option<Instant>,
    counts: BTreeMap<&'static str, u64>,
    statuses: BTreeMap<String, u64>,
    latencies: VecDeque<f64>,
    recent: VecDeque<Instant>,
    last_error: String,
}

impl Stats {
    fn inc(&mut self, key: &'static str, count: u64) {
        *self.counts.entry(key).or_default() += count;
    }
    fn count(&self, key: &str) -> u64 {
        self.counts.get(key).copied().unwrap_or(0)
    }

    fn record(&mut self, result: std::result::Result<Completion, tokio::task::JoinError>) -> bool {
        let result = match result {
            Ok(result) => result,
            Err(error) => {
                self.last_error = error.to_string();
                self.phase = "failed";
                return false;
            }
        };
        if !result.error.is_empty() {
            self.last_error = result.error.clone();
        }
        if result.seed {
            self.inc("seeded", 1);
            if result.status != "204" {
                self.inc("seed_errors", 1);
                return false;
            }
        } else {
            self.inc("completed", 1);
            self.inc(if result.read { "reads" } else { "writes" }, 1);
            self.inc(
                if !result.error.is_empty() {
                    "errors"
                } else if result.status == "404" {
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
            self.recent.push_back(Instant::now());
            self.trim_recent();
        }
        true
    }

    fn trim_recent(&mut self) {
        let cutoff = Instant::now() - Duration::from_secs(1);
        while self.recent.front().is_some_and(|time| *time < cutoff) {
            self.recent.pop_front();
        }
    }

    fn snapshot(&mut self, config: &Config, in_flight: usize, report: Option<&PathBuf>) -> Value {
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
            "current_tps": if elapsed > 0.0 { (self.recent.len() as f64 / elapsed.min(1.0) * 10.0).round() / 10.0 } else { 0.0 },
            "average_tps": if elapsed > 0.0 { (self.count("completed") as f64 / elapsed * 10.0).round() / 10.0 } else { 0.0 },
            "p95_ms": (p95 * 100.0).round() / 100.0, "last_error": self.last_error, "report": report})
    }
}

fn emit(value: &Value) -> Result<()> {
    let mut stdout = std::io::stdout().lock();
    serde_json::to_writer(&mut stdout, value)?;
    writeln!(stdout)?;
    stdout.flush()?;
    Ok(())
}

async fn run(config: Config, mut cancel: watch::Receiver<bool>, report_dir: PathBuf) -> Result<()> {
    eprintln!(
        "Load generator using {} Tokio worker threads",
        tokio::runtime::Handle::current().metrics().num_workers(),
    );
    let mut headers = reqwest::header::HeaderMap::new();
    if let Ok(token) = std::env::var("ATGS_INTERNAL_SERVICE_TOKEN") {
        let mut value = reqwest::header::HeaderValue::from_str(&format!("Bearer {token}"))
            .context("Invalid ATGS_INTERNAL_SERVICE_TOKEN")?;
        value.set_sensitive(true);
        headers.insert(reqwest::header::AUTHORIZATION, value);
    }
    let client = Client::builder()
        .default_headers(headers)
        .no_proxy()
        .tcp_nodelay(true)
        .pool_max_idle_per_host(config.workers)
        .timeout(Duration::from_secs(15))
        .build()?;
    let mut stats = Stats {
        phase: if config.seed && config.reads > 0 {
            "seeding"
        } else {
            "running"
        },
        ..Default::default()
    };
    emit(&stats.snapshot(&config, 0, None))?;
    let urls = Arc::new(config.record_urls()?);
    let mut tasks = JoinSet::new();
    let mut progress = tokio::time::interval(Duration::from_millis(250));
    progress.set_missed_tick_behavior(MissedTickBehavior::Skip);
    if stats.phase == "seeding" {
        let mut index = 0;
        loop {
            while index < config.records && tasks.len() < config.workers && !*cancel.borrow() {
                tasks.spawn(request(
                    client.clone(),
                    urls[index][index % config.urls.len()].clone(),
                    config.payload.clone(),
                    false,
                    true,
                ));
                index += 1;
            }
            if tasks.is_empty() || *cancel.borrow() {
                break;
            }
            tokio::select! {
                _ = cancel.changed() => break,
                Some(result) = tasks.join_next() => { if !stats.record(result) { stats.phase = "failed"; break; } },
                _ = progress.tick() => emit(&stats.snapshot(&config, tasks.len(), None))?,
            }
        }
    }
    if stats.phase != "failed" && !*cancel.borrow() {
        stats.phase = "running";
        let started = Instant::now();
        stats.started = Some(started);
        let deadline = started + config.duration;
        let mut tick = tokio::time::interval(Duration::from_millis(1));
        tick.set_missed_tick_behavior(MissedTickBehavior::Skip);
        let mut slots = 0u64;
        let mut sequence = 0usize;
        loop {
            if *cancel.borrow() {
                break;
            }
            tokio::select! {
                biased;
                _ = cancel.changed() => break,
                _ = tokio::time::sleep_until(deadline) => break,
                Some(result) = tasks.join_next(), if !tasks.is_empty() => { if !stats.record(result) { stats.phase = "failed"; break; } },
                _ = tick.tick() => {
                    let due = (started.elapsed().as_secs_f64() * config.tps as f64) as u64 + 1;
                    let available = due.saturating_sub(slots);
                    slots = due;
                    // Millisecond batches avoid per-request sleeps. Discard missed slots;
                    // never queue a catch-up burst behind a busy worker pool.
                    let count = available.min(config.tps.div_ceil(200)).min((config.workers - tasks.len()) as u64);
                    stats.inc("skipped", available - count);
                    for _ in 0..count {
                        let (index, read) = {
                            let mut random = rand::rng();
                            (random.random_range(0..config.records), random.random_range(0..100) < config.reads)
                        };
                        tasks.spawn(request(client.clone(), urls[index][sequence % config.urls.len()].clone(), config.payload.clone(), read, false));
                        stats.inc("started", 1);
                        sequence += 1;
                    }
                },
                _ = progress.tick() => emit(&stats.snapshot(&config, tasks.len(), None))?,
            }
        }
    }
    let failed = stats.phase == "failed";
    stats.phase = if failed {
        "failed"
    } else if *cancel.borrow() {
        "stopping"
    } else {
        "draining"
    };
    while !tasks.is_empty() {
        tokio::select! {
            Some(result) = tasks.join_next() => { if !stats.record(result) { stats.phase = "failed"; } },
            _ = progress.tick() => emit(&stats.snapshot(&config, tasks.len(), None))?,
        }
    }
    stats.finished = Some(Instant::now());
    stats.phase = if failed || stats.phase == "failed" || stats.count("seed_errors") > 0 {
        "failed"
    } else if *cancel.borrow() {
        "stopped"
    } else {
        "finished"
    };
    let report = report_dir.join(format!("load-{}.json", config.run_id));
    let value = stats.snapshot(&config, 0, Some(&report));
    std::fs::write(&report, serde_json::to_vec_pretty(&value)?)?;
    emit(&value)?;
    Ok(())
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    if let Err(error) = main_result().await {
        let _ = emit(&json!({"error": format!("{error:#}")}));
        std::process::exit(1);
    }
}

async fn main_result() -> Result<()> {
    match dotenvy::dotenv() {
        Ok(_) => {}
        Err(dotenvy::Error::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error).context("Could not load .env"),
    }

    let mut args = std::env::args().skip(1);
    let mut urls = Vec::new();
    let mut file = None;
    let mut directory = PathBuf::from(".");
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--control-url" => urls.push(args.next().context("Missing control URL")?),
            "--config" => file = Some(args.next().context("Missing config path")?),
            "--report-dir" => {
                directory = PathBuf::from(args.next().context("Missing report directory")?)
            }
            "--help" | "-h" => {
                println!(
                    "patchwork-load --control-url URL [--control-url URL ...] [--config JSON_FILE] [--report-dir DIR]\nWithout --config, reads config JSON then stop commands from stdin. --config also accepts dashboard reports. Progress and final results are JSON lines. Ctrl-C stops scheduling and drains requests."
                );
                return Ok(());
            }
            _ => bail!("Unknown argument: {arg}"),
        }
    }
    ensure!(directory.is_dir(), "Report directory does not exist");
    let stdio = file.is_none();
    let input = if let Some(file) = file {
        std::fs::read_to_string(file)?
    } else {
        let mut input = String::new();
        std::io::stdin().read_line(&mut input)?;
        input
    };
    let config = Config::parse(serde_json::from_str(&input)?, &urls)?;
    let (sender, receiver) = watch::channel(false);
    let signal = sender.clone();
    tokio::spawn(async move {
        if tokio::signal::ctrl_c().await.is_ok() {
            let _ = signal.send(true);
        }
    });
    if stdio {
        std::thread::spawn(move || {
            for line in std::io::stdin().lock().lines() {
                if matches!(line.as_deref(), Ok("stop")) || line.is_err() {
                    break;
                }
            }
            let _ = sender.send(true);
        });
    }
    run(config, receiver, directory).await
}

#[cfg(test)]
mod tests;
