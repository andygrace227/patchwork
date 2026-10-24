//! Standalone load worker. JSON configuration in, progress JSON out.
use anyhow::{Context, Result, bail, ensure};
use serde_json::json;
use std::{io::BufRead, path::PathBuf};
use tokio::sync::watch;

mod config;
mod request;
mod runner;
mod stats;

use config::Config;
use runner::run;
use stats::emit;

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
    let mut data_url = None;
    let mut file = None;
    let mut directory = PathBuf::from(".");
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--control-url" => urls.push(args.next().context("Missing control URL")?),
            "--data-url" => {
                ensure!(data_url.is_none(), "Supply only one --data-url");
                data_url = Some(args.next().context("Missing data URL")?);
            }
            "--config" => file = Some(args.next().context("Missing config path")?),
            "--report-dir" => {
                directory = PathBuf::from(args.next().context("Missing report directory")?)
            }
            "--help" | "-h" => {
                println!(
                    "patchwork-load (--control-url URL [--control-url URL ...] | --data-url URL) [--config JSON_FILE] [--report-dir DIR]\nWithout --config, reads config JSON then stop commands from stdin. --config also accepts dashboard reports. Use --data-url to load one data node directly over gRPC. Progress and final results are JSON lines. Ctrl-C stops scheduling and drains requests."
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
    let direct = data_url.is_some();
    if let Some(url) = data_url {
        ensure!(
            urls.is_empty(),
            "Choose --data-url or --control-url, not both"
        );
        urls.push(url);
    }
    let mut config = Config::parse(serde_json::from_str(&input)?, &urls)?;
    config.direct = direct;
    config.values["transport"] = json!(if direct { "grpc" } else { "http" });
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
