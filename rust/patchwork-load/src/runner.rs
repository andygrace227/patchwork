use crate::{
    config::{Config, Target},
    request::RequestClient,
    stats::{Stats, emit},
};
use anyhow::{Result, ensure};
use futures_util::{StreamExt, stream::FuturesUnordered};
use rand::Rng;
use std::{path::PathBuf, sync::Arc, time::Duration};
use tokio::{
    sync::{mpsc, watch},
    task::JoinSet,
    time::{Instant, MissedTickBehavior},
};

const REPORT_INTERVAL: Duration = Duration::from_millis(250);

/// Distribute remainders too: totals never grow with the number of request loops.
pub(crate) fn share(total: u64, index: usize, loops: usize) -> u64 {
    total / loops as u64 + u64::from((index as u64) < total % loops as u64)
}

async fn seed(
    config: &Config,
    targets: &[Vec<Target>],
    stats: &mut Stats,
    cancel: &mut watch::Receiver<bool>,
) -> Result<()> {
    let mut client = RequestClient::new(config)?;
    let mut requests = FuturesUnordered::new();
    let mut progress = tokio::time::interval(REPORT_INTERVAL);
    progress.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let mut index = 0;
    loop {
        while stats.phase != "failed"
            && index < config.records
            && requests.len() < config.workers
            && !*cancel.borrow()
        {
            requests.push(client.clone().request(
                &targets[index][index % config.urls.len()],
                config.payload.clone(),
                false,
                true,
            ));
            stats.inc("seed_started", 1);
            index += 1;
        }
        if requests.is_empty() {
            break;
        }
        tokio::select! {
            _ = cancel.changed(), if !*cancel.borrow() => {},
            Some(result) = requests.next() => {
                if !stats.record(result) { stats.phase = "failed"; }
            },
            _ = progress.tick() => emit(&stats.snapshot(config, requests.len(), None))?,
        }
    }
    if stats.phase == "failed" || *cancel.borrow() {
        return Ok(());
    }
    // Direct writes acknowledge admission. Wait for seeded rows before measuring reads.
    let deadline = Instant::now() + Duration::from_secs(30);
    while client.backlog().await? != 0 {
        ensure!(
            Instant::now() < deadline,
            "Timed out waiting for seeded writes to commit"
        );
        tokio::select! {
            _ = cancel.changed() => return Ok(()),
            _ = tokio::time::sleep(Duration::from_millis(20)) => {},
        }
    }
    Ok(())
}

/// One persistent task per load thread. Requests stay inside this task rather than
/// being spawned and sent back to a central collector one at a time.
async fn request_loop(
    config: Arc<Config>,
    targets: Arc<Vec<Vec<Target>>>,
    client: RequestClient,
    index: usize,
    loops: usize,
    started: Instant,
    mut stop: watch::Receiver<bool>,
    updates: mpsc::Sender<Stats>,
) -> Stats {
    let rate = share(config.tps, index, loops);
    let workers = share(config.workers as u64, index, loops) as usize;
    let deadline = started + config.duration;
    let mut stats = Stats::default();
    let mut requests = FuturesUnordered::new();
    let mut tick = tokio::time::interval(Duration::from_millis(1));
    tick.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let mut progress = tokio::time::interval_at(started + REPORT_INTERVAL, REPORT_INTERVAL);
    progress.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let mut slots = 0;
    let mut sequence = index;
    loop {
        if *stop.borrow() {
            break;
        }
        tokio::select! {
            biased;
            _ = stop.changed() => break,
            _ = tokio::time::sleep_until(deadline) => break,
            _ = progress.tick() => {
                if updates.send(std::mem::take(&mut stats)).await.is_err() { break; }
            },
            _ = tick.tick() => {
                let due = (started.elapsed().as_secs_f64() * rate as f64) as u64;
                let available = due.saturating_sub(slots);
                slots = due;
                // Discard missed slots rather than building a catch-up queue.
                let count = available.min(rate.div_ceil(200)).min((workers - requests.len()) as u64);
                stats.inc("skipped", available - count);
                for _ in 0..count {
                    let (record, read) = {
                        let mut random = rand::rng();
                        (random.random_range(0..config.records), random.random_range(0..100) < config.reads)
                    };
                    requests.push(client.clone().request(&targets[record][sequence % config.urls.len()], config.payload.clone(), read, false));
                    stats.inc("started", 1);
                    sequence += loops;
                }
            },
            Some(result) = requests.next(), if !requests.is_empty() => { stats.record(result); },
        }
    }
    // Cancellation stops scheduling; every admitted request still finishes.
    while !requests.is_empty() {
        tokio::select! {
            Some(result) = requests.next() => { stats.record(result); },
            _ = progress.tick() => {
                if updates.send(std::mem::take(&mut stats)).await.is_err() { break; }
            },
        }
    }
    stats
}

pub(crate) async fn run(
    mut config: Config,
    mut cancel: watch::Receiver<bool>,
    report_dir: PathBuf,
) -> Result<()> {
    let loops = tokio::runtime::Handle::current()
        .metrics()
        .num_workers()
        .min(config.workers)
        .min(config.tps as usize)
        .max(1);
    config.values["request_loops"] = loops.into();
    eprintln!("Load generator using {loops} independent request loops");
    let targets = Arc::new(config.record_targets()?);
    let mut stats = Stats::default();
    stats.phase = if config.seed && config.reads > 0 {
        "seeding"
    } else {
        "running"
    };
    emit(&stats.snapshot(&config, 0, None))?;
    if stats.phase == "seeding" {
        seed(&config, &targets, &mut stats, &mut cancel).await?;
    }
    let config = Arc::new(config);
    if stats.phase != "failed" && !*cancel.borrow() {
        let clients = (0..loops)
            .map(|_| RequestClient::new(&config))
            .collect::<Result<Vec<_>>>()?;
        let started = Instant::now();
        stats.phase = "running";
        stats.started = Some(started);
        let (stop, stopped) = watch::channel(false);
        let (updates, mut batches) = mpsc::channel(loops * 2);
        let mut tasks = JoinSet::new();
        for (index, client) in clients.into_iter().enumerate() {
            tasks.spawn(request_loop(
                config.clone(),
                targets.clone(),
                client,
                index,
                loops,
                started,
                stopped.clone(),
                updates.clone(),
            ));
        }
        drop(updates);
        let mut progress = tokio::time::interval(REPORT_INTERVAL);
        progress.set_missed_tick_behavior(MissedTickBehavior::Skip);
        let mut stopping = false;
        let mut updates_open = true;
        while !tasks.is_empty() || updates_open {
            tokio::select! {
                _ = cancel.changed(), if !stopping => {
                    stopping = true;
                    stats.phase = "stopping";
                    let _ = stop.send(true);
                },
                batch = batches.recv(), if updates_open => {
                    match batch { Some(batch) => stats.merge(batch), None => updates_open = false }
                },
                Some(result) = tasks.join_next(), if !tasks.is_empty() => {
                    match result {
                        Ok(batch) => stats.merge(batch),
                        Err(error) => {
                            stats.phase = "failed";
                            stats.last_error = error.to_string();
                            let _ = stop.send(true);
                        }
                    }
                },
                _ = progress.tick() => {
                    if stats.phase == "running" && started.elapsed() >= config.duration { stats.phase = "draining"; }
                    let in_flight = stats.in_flight();
                    emit(&stats.snapshot(&config, in_flight, None))?;
                },
            }
        }
    }
    stats.finished = Some(Instant::now());
    stats.phase = if stats.phase == "failed" || stats.count("seed_errors") > 0 {
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
