//! One gRPC call from a JSON object on stdin, for the local cluster tools.
use patchwork_common::dataplane_client::Client;
use serde::Deserialize;
use serde_json::{Value, json};
use tonic::Status;

#[derive(Deserialize)]
struct Call {
    url: String,
    operation: String,
    table_id: Option<i64>,
    partition_key: Option<i64>,
    secondary_key: Option<i64>,
    lower_bound: Option<i64>,
    upper_bound: Option<i64>,
    timestamp: Option<i64>,
    #[serde(default)]
    data: Value,
    #[serde(default)]
    replicas: Vec<String>,
    #[serde(default)]
    deleted: bool,
    #[serde(default)]
    include_deleted: bool,
}

fn required(value: Option<i64>, name: &str) -> Result<i64, Status> {
    value.ok_or_else(|| Status::invalid_argument(format!("{name} is required")))
}

async fn call(request: Call) -> Result<Value, Status> {
    let client = Client::new();
    let url = &request.url;
    match request.operation.as_str() {
        "GetSize" => return client.get_size(url).await,
        "GetTelemetry" => return Ok(json!(client.get_telemetry(url).await?)),
        "GetWriteBacklog" => return client.get_write_backlog(url).await,
        _ => {}
    }
    let table = required(request.table_id, "table_id")?;
    match request.operation.as_str() {
        "GetTelemetryForTable" => {
            return Ok(json!(client.get_telemetry_for_table(url, table).await?));
        }
        "DeleteTable" => return client.delete_table(url, table).await,
        "GetRange" | "DeleteRange" | "CountRange" | "GetRangeSize" => {
            let lower = required(request.lower_bound, "lower_bound")?;
            let upper = required(request.upper_bound, "upper_bound")?;
            return match request.operation.as_str() {
                "GetRange" if request.include_deleted => Ok(json!(
                    client
                        .get_range_including_deleted(url, table, upper, lower)
                        .await?
                )),
                "GetRange" => Ok(json!(client.get_range(url, table, upper, lower).await?)),
                "DeleteRange" => client.delete_range(url, table, upper, lower).await,
                "CountRange" => client.count_range(url, table, upper, lower).await,
                _ => client.get_range_size(url, table, upper, lower).await,
            };
        }
        _ => {}
    }
    let partition = required(request.partition_key, "partition_key")?;
    match request.operation.as_str() {
        "GetPartitionKey" => {
            return Ok(json!(
                client.get_partition_key(url, table, partition).await?
            ));
        }
        "DeletePartitionKey" => return client.delete_partition_key(url, table, partition).await,
        _ => {}
    }
    let key = required(request.secondary_key, "secondary_key")?;
    let data = serde_json::to_vec(&request.data).map_err(|error| Status::invalid_argument(error.to_string()))?;
    match request.operation.as_str() {
        "GetRecord" if request.include_deleted => Ok(json!(
            client
                .get_including_deleted(url, table, partition, key)
                .await?
        )),
        "GetRecord" => Ok(json!(client.get(url, table, partition, key).await?)),
        "DeleteRecord" => {
            client
                .delete(
                    url,
                    table,
                    partition,
                    key,
                    required(request.timestamp, "timestamp")?,
                )
                .await
        }
        "WriteRecord" | "WriteWithReplicas" | "WriteWithReplicasInconsistent" => {
            let timestamp = required(request.timestamp, "timestamp")?;
            match request.operation.as_str() {
                "WriteRecord" => {
                    client
                        .write_record(
                            url,
                            table,
                            partition,
                            key,
                            &data,
                            timestamp,
                            request.deleted,
                        )
                        .await?
                }
                "WriteWithReplicas" => {
                    client
                        .write_with_replicas(
                            url,
                            table,
                            partition,
                            key,
                            &data,
                            &request.replicas,
                            timestamp,
                            request.deleted,
                        )
                        .await?
                }
                _ => {
                    client
                        .write_with_replicas_inconsistent(
                            url,
                            table,
                            partition,
                            key,
                            &data,
                            &request.replicas,
                            timestamp,
                            request.deleted,
                        )
                        .await?
                }
            }
            Ok(Value::Null)
        }
        _ => Err(Status::invalid_argument("Unknown data-plane operation")),
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let result = match serde_json::from_reader::<_, Call>(std::io::stdin().lock()) {
        Ok(request) => call(request).await,
        Err(error) => Err(Status::invalid_argument(error.to_string())),
    };
    let response = match result {
        Ok(body) => json!({"status": "OK", "body": body}),
        Err(error) => {
            json!({"status": format!("{:?}", error.code()), "body": {"error": error.message()}})
        }
    };
    println!("{response}");
}
