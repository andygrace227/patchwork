use crate::config::{Config, Target};
use anyhow::{Context, Result};
use bytes::Bytes;
use patchwork_common::grpc::{
    MAX_MESSAGE_SIZE,
    proto::{self, data_plane_client::DataPlaneClient},
};
use std::{
    borrow::Cow,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::time::Instant;
use tonic::transport::{Channel, Endpoint};

#[derive(Clone)]
pub(crate) enum RequestClient {
    Http(reqwest::Client),
    Grpc {
        client: DataPlaneClient<Channel>,
        partition_id: Arc<str>,
    },
}

pub(crate) struct Completion {
    pub status: Cow<'static, str>,
    pub read: bool,
    pub seed: bool,
    pub latency: f64,
    pub error: String,
}

impl RequestClient {
    /// Each request loop calls this once, so connections and their queues are independent.
    pub fn new(config: &Config) -> Result<Self> {
        if config.direct {
            let channel =
                Endpoint::from_shared(config.urls[0].as_str().trim_end_matches('/').to_owned())?
                    .connect_timeout(Duration::from_secs(5))
                    .tcp_nodelay(true)
                    .connect_lazy();
            return Ok(Self::Grpc {
                client: DataPlaneClient::new(channel)
                    .max_decoding_message_size(MAX_MESSAGE_SIZE)
                    .max_encoding_message_size(MAX_MESSAGE_SIZE),
                partition_id: format!("load-table-{}", config.values["table_id"].as_str().unwrap())
                    .into(),
            });
        }
        let mut headers = reqwest::header::HeaderMap::new();
        if let Ok(token) = std::env::var("ATGS_INTERNAL_SERVICE_TOKEN") {
            let mut value = reqwest::header::HeaderValue::from_str(&format!("Bearer {token}"))
                .context("Invalid ATGS_INTERNAL_SERVICE_TOKEN")?;
            value.set_sensitive(true);
            headers.insert(reqwest::header::AUTHORIZATION, value);
        }
        Ok(Self::Http(
            reqwest::Client::builder()
                .default_headers(headers)
                .no_proxy()
                .tcp_nodelay(true)
                .pool_max_idle_per_host(config.workers)
                .timeout(Duration::from_secs(15))
                .build()?,
        ))
    }

    pub async fn backlog(&mut self) -> Result<u64> {
        match self {
            Self::Http(_) => Ok(0),
            Self::Grpc { client, .. } => Ok(tokio::time::timeout(
                Duration::from_secs(30),
                client.get_write_backlog(proto::Empty {}),
            )
            .await??
            .into_inner()
            .total),
        }
    }

    pub async fn request(
        mut self,
        target: &Target,
        payload: Bytes,
        read: bool,
        seed: bool,
    ) -> Completion {
        let started = Instant::now();
        let (status, error) = match &mut self {
            Self::Http(client) => http_request(client, target, payload, read, seed).await,
            Self::Grpc {
                client,
                partition_id,
            } => {
                let call = async {
                    if read {
                        client
                            .get_record(proto::RecordRequest {
                                table_id: target.table_id,
                                partition_key: target.partition_key,
                                secondary_key: target.secondary_key,
                                include_deleted: true,
                            })
                            .await
                            .map(|_| ())
                    } else {
                        client
                            .write_record(proto::WriteRequest {
                                table_id: target.table_id,
                                partition_key: target.partition_key,
                                secondary_key: target.secondary_key,
                                partition_id: Some(partition_id.to_string()),
                                timestamp: Some(
                                    SystemTime::now()
                                        .duration_since(UNIX_EPOCH)
                                        .unwrap()
                                        .as_micros() as i64,
                                ),
                                data_bin: payload.to_vec(),
                                ..Default::default()
                            })
                            .await
                            .map(|_| ())
                    }
                };
                let result = tokio::time::timeout(Duration::from_secs(30), call)
                    .await
                    .unwrap_or_else(|_| Err(tonic::Status::deadline_exceeded("request timed out")));
                match result {
                    Ok(()) => (Cow::Borrowed("OK"), String::new()),
                    Err(error) if read && error.code() == tonic::Code::NotFound => {
                        (Cow::Borrowed("NotFound"), String::new())
                    }
                    Err(error) => (Cow::Owned(format!("{:?}", error.code())), error.to_string()),
                }
            }
        };
        Completion {
            status,
            error,
            read,
            seed,
            latency: started.elapsed().as_secs_f64() * 1000.0,
        }
    }
}

async fn http_request(
    client: &reqwest::Client,
    target: &Target,
    payload: Bytes,
    read: bool,
    seed: bool,
) -> (Cow<'static, str>, String) {
    let result = async {
        let mut url = target.url.clone();
        if seed {
            url.query_pairs_mut()
                .clear()
                .append_pair("consistent", "true")
                .append_pair("telemetry", "false");
        }
        let request = if read {
            client.get(url)
        } else {
            client
                .put(url)
                .header("content-type", "application/json")
                .header(
                    "x-patchwork-timestamp",
                    SystemTime::now()
                        .duration_since(UNIX_EPOCH)?
                        .as_micros()
                        .to_string(),
                )
                .body(payload)
        };
        let response = request.send().await?;
        let status = response.status();
        // Drain every body so the connection can be reused, including read misses.
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
        Ok::<_, anyhow::Error>((Cow::Owned(status.as_u16().to_string()), error))
    }
    .await;
    result.unwrap_or_else(|error| (Cow::Borrowed("network"), format!("{error:#}")))
}
