use anyhow::{Context, Result, ensure};
use bytes::Bytes;
use rand::Rng;
use reqwest::Url;
use serde_json::{Value, json};
use std::time::Duration;

pub(crate) struct Config {
    pub values: Value,
    pub urls: Vec<Url>,
    pub direct: bool,
    pub workers: usize,
    pub tps: u64,
    pub duration: Duration,
    pub reads: u64,
    pub records: usize,
    pub keys: usize,
    pub hot: Option<i64>,
    pub record_base: i64,
    pub seed: bool,
    pub telemetry: bool,
    pub consistent: bool,
    pub run_id: String,
    pub payload: Bytes,
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
    pub fn parse(values: Value, urls: &[String]) -> Result<Self> {
        // A downloaded dashboard report can also be used as a configuration file.
        let values = values.get("config").cloned().unwrap_or(values);
        ensure!(values.is_object(), "Send a JSON load configuration");
        ensure!(!urls.is_empty(), "Supply at least one --control-url");
        let table = number(&values, "table_id", 0, 1, i64::MAX)?;
        let tps = number(&values, "tps", 100, 1, 1_000_000)? as u64;
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
                "Node URLs must use HTTP or HTTPS"
            );
            ensure!(
                url.username().is_empty() && url.password().is_none(),
                "Node URLs cannot contain credentials"
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
            direct: false,
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

    pub fn partition_key(&self, index: usize) -> i64 {
        self.hot.unwrap_or_else(|| {
            (i64::MIN as i128
                + (u64::MAX as i128 * (index % self.keys) as i128)
                    / self.keys.saturating_sub(1).max(1) as i128) as i64
        })
    }

    pub fn record_targets(&self) -> Result<Vec<Vec<Target>>> {
        let table_id = self.values["table_id"].as_str().unwrap().parse::<i64>()?;
        (0..self.records).map(|index| self.urls.iter().map(|base| {
            let partition_key = self.partition_key(index);
            let secondary_key = self.record_base + index as i64;
            let url = if self.direct {
                base.clone()
            } else {
                base.join(&format!("/tables/{table_id}/partition-keys/{partition_key}/records/{secondary_key}?consistent={}&telemetry={}", self.consistent, self.telemetry))?
            };
            Ok(Target { url, table_id, partition_key, secondary_key })
        }).collect()).collect()
    }
}

#[derive(Clone)]
pub(crate) struct Target {
    pub url: Url,
    pub table_id: i64,
    pub partition_key: i64,
    pub secondary_key: i64,
}
