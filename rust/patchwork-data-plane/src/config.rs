use crate::{
    telemetry::access_stats_actor::{FLUSH_INTERVAL, RETENTION},
    utils::positive_env,
};
use anyhow::{Context, Result};
use std::{net::SocketAddr, path::PathBuf, time::Duration};

pub struct Config {
    pub bind_addr: SocketAddr,
    pub shard_path: PathBuf,
    pub wal_path: PathBuf,
    pub read_connections: u32,
    pub stats_path: PathBuf,
    pub stats_flush_interval: Duration,
    pub stats_retention: Duration,
}

impl Config {
    pub fn from_env() -> Result<Self> {
        match dotenvy::dotenv() {
            Ok(_) => {}
            Err(dotenvy::Error::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error).context("Could not load .env"),
        }
        let path =
            std::env::var("SHARD_PATH").context("Set SHARD_PATH in .env or the environment")?;
        Ok(Self {
            bind_addr: std::env::var("BIND_ADDR")
                .unwrap_or_else(|_| "127.0.0.1:3000".into())
                .parse()?,
            shard_path: PathBuf::from(&path),
            wal_path: PathBuf::from(format!("{path}.wal")),
            read_connections: u32::try_from(positive_env("READ_DB_MAX_CONNECTIONS", 4)?)?,
            stats_path: std::env::var("ACCESS_STATS_PATH")
                .unwrap_or_else(|_| format!("{path}.access-stats.sqlite"))
                .into(),
            stats_flush_interval: Duration::from_secs(positive_env(
                "ACCESS_STATS_FLUSH_SECONDS",
                FLUSH_INTERVAL.as_secs(),
            )?),
            stats_retention: Duration::from_secs(positive_env(
                "ACCESS_STATS_RETENTION_SECONDS",
                RETENTION.as_secs(),
            )?),
        })
    }
}
