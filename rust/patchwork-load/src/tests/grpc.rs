use super::*;
use futures_util::StreamExt;
use patchwork_common::grpc::proto::{
    self,
    data_plane_server::{DataPlane, DataPlaneServer},
};
use std::{
    collections::HashMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
};
use tonic::{Request, Response, Status};

#[derive(Clone, Default)]
struct Stub {
    rows: Arc<Mutex<HashMap<(i64, i64, i64), proto::Record>>>,
    active: Arc<AtomicUsize>,
    maximum: Arc<AtomicUsize>,
    backlog_checks: Arc<AtomicUsize>,
    delay: Duration,
}

#[tonic::async_trait]
impl DataPlane for Stub {
    async fn write_record(
        &self,
        request: Request<proto::WriteRequest>,
    ) -> Result<Response<proto::Empty>, Status> {
        let request = request.into_inner();
        assert!(request.timestamp.unwrap() > 0);
        assert_eq!(request.partition_id.as_deref(), Some("load-table-1"));
        assert!(!request.deleted);
        assert!(request.replicas.is_empty());
        self.rows.lock().unwrap().insert(
            (
                request.table_id,
                request.partition_key,
                request.secondary_key,
            ),
            proto::Record {
                table_id: request.table_id,
                partition_key: request.partition_key,
                secondary_key: request.secondary_key,
                data_bin: request.data_bin,
                timestamp: request.timestamp.unwrap(),
                deleted: false,
                partition_id: request.partition_id,
            },
        );
        Ok(Response::new(proto::Empty {}))
    }

    async fn get_record(
        &self,
        request: Request<proto::RecordRequest>,
    ) -> Result<Response<proto::Record>, Status> {
        let request = request.into_inner();
        assert!(request.include_deleted);
        if !self.delay.is_zero() {
            let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
            self.maximum.fetch_max(active, Ordering::SeqCst);
            tokio::time::sleep(self.delay).await;
            self.active.fetch_sub(1, Ordering::SeqCst);
        }
        self.rows
            .lock()
            .unwrap()
            .get(&(
                request.table_id,
                request.partition_key,
                request.secondary_key,
            ))
            .cloned()
            .map(Response::new)
            .ok_or_else(|| Status::not_found("record not found"))
    }

    async fn get_write_backlog(
        &self,
        _: Request<proto::Empty>,
    ) -> Result<Response<proto::WriteBacklog>, Status> {
        let pending = u64::from(self.backlog_checks.fetch_add(1, Ordering::SeqCst) < 2);
        Ok(Response::new(proto::WriteBacklog {
            queued: pending,
            in_flight: 0,
            total: pending,
        }))
    }
    async fn write_with_replicas(
        &self,
        _: Request<proto::WriteRequest>,
    ) -> Result<Response<proto::Empty>, Status> {
        Err(Status::unimplemented("unused by this load test"))
    }
    async fn write_with_replicas_inconsistent(
        &self,
        _: Request<proto::WriteRequest>,
    ) -> Result<Response<proto::Empty>, Status> {
        Err(Status::unimplemented("unused by this load test"))
    }
    async fn delete_record(
        &self,
        _: Request<proto::DeleteRequest>,
    ) -> Result<Response<proto::Count>, Status> {
        Err(Status::unimplemented("unused by this load test"))
    }
    async fn get_partition_key(
        &self,
        _: Request<proto::PartitionRequest>,
    ) -> Result<Response<proto::Records>, Status> {
        Err(Status::unimplemented("unused by this load test"))
    }
    async fn delete_partition_key(
        &self,
        _: Request<proto::PartitionRequest>,
    ) -> Result<Response<proto::Count>, Status> {
        Err(Status::unimplemented("unused by this load test"))
    }
    async fn get_range(
        &self,
        _: Request<proto::RangeRequest>,
    ) -> Result<Response<proto::Records>, Status> {
        Err(Status::unimplemented("unused by this load test"))
    }
    async fn delete_range(
        &self,
        _: Request<proto::RangeRequest>,
    ) -> Result<Response<proto::Count>, Status> {
        Err(Status::unimplemented("unused by this load test"))
    }
    async fn count_range(
        &self,
        _: Request<proto::RangeRequest>,
    ) -> Result<Response<proto::Count>, Status> {
        Err(Status::unimplemented("unused by this load test"))
    }
    async fn get_range_size(
        &self,
        _: Request<proto::RangeRequest>,
    ) -> Result<Response<proto::Size>, Status> {
        Err(Status::unimplemented("unused by this load test"))
    }
    async fn delete_table(
        &self,
        _: Request<proto::TableRequest>,
    ) -> Result<Response<proto::Count>, Status> {
        Err(Status::unimplemented("unused by this load test"))
    }
    async fn get_size(&self, _: Request<proto::Empty>) -> Result<Response<proto::Size>, Status> {
        Err(Status::unimplemented("unused by this load test"))
    }
    async fn get_telemetry(
        &self,
        _: Request<proto::Empty>,
    ) -> Result<Response<proto::TelemetryEntries>, Status> {
        Err(Status::unimplemented("unused by this load test"))
    }
    async fn get_telemetry_for_table(
        &self,
        _: Request<proto::TableRequest>,
    ) -> Result<Response<proto::TelemetryEntries>, Status> {
        Err(Status::unimplemented("unused by this load test"))
    }
}

async fn serve(stub: Stub) -> (String, Arc<AtomicUsize>, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let connections = Arc::new(AtomicUsize::new(0));
    let accepted = connections.clone();
    let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener).inspect(move |_| {
        accepted.fetch_add(1, Ordering::SeqCst);
    });
    let server = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .serve_with_incoming(DataPlaneServer::new(stub), incoming)
            .await
            .unwrap();
    });
    (url, connections, server)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn grpc_loops_use_independent_connections_and_wait_for_seeding() {
    let stub = Stub::default();
    let checks = stub.backlog_checks.clone();
    let (url, connections, server) = serve(stub).await;
    let directory = tempfile::tempdir().unwrap();
    let mut config = Config::parse(
        json!({"table_id": 1, "tps": 501, "duration": 1, "workers": 7,
        "records": 8, "keys": 2, "read_percent": 100}),
        &[url],
    )
    .unwrap();
    config.direct = true;
    let report = directory
        .path()
        .join(format!("load-{}.json", config.run_id));
    let (_sender, cancel) = watch::channel(false);
    run(config, cancel, directory.path().into()).await.unwrap();
    let result: Value = serde_json::from_slice(&std::fs::read(report).unwrap()).unwrap();
    assert_eq!(result["config"]["request_loops"], 2);
    assert_eq!(result["counts"]["seeded"], 8);
    assert_eq!(result["counts"]["started"], result["counts"]["completed"]);
    assert!(result["counts"]["completed"].as_u64().unwrap() <= 501);
    assert_eq!(result["counts"]["successes"], result["counts"]["completed"]);
    assert_eq!(connections.load(Ordering::SeqCst), 3); // One seeder plus two request loops.
    assert!(checks.load(Ordering::SeqCst) >= 3);
    assert_eq!(result["counts"]["in_flight"], 0);
    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancelling_grpc_load_preserves_the_total_concurrency_limit_and_drains() {
    let stub = Stub {
        delay: Duration::from_millis(25),
        ..Default::default()
    };
    let active = stub.active.clone();
    let maximum = stub.maximum.clone();
    let (url, _, server) = serve(stub).await;
    let directory = tempfile::tempdir().unwrap();
    let mut config = Config::parse(
        json!({"table_id": 1, "tps": 10_000, "duration": 30, "workers": 5,
        "records": 8, "keys": 2, "read_percent": 100, "seed": false}),
        &[url],
    )
    .unwrap();
    config.direct = true;
    let report = directory
        .path()
        .join(format!("load-{}.json", config.run_id));
    let (sender, cancel) = watch::channel(false);
    let stop = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(150)).await;
        sender.send(true).unwrap();
    });
    run(config, cancel, directory.path().into()).await.unwrap();
    let result: Value = serde_json::from_slice(&std::fs::read(report).unwrap()).unwrap();
    assert_eq!(result["phase"], "stopped");
    assert_eq!(result["counts"]["started"], result["counts"]["completed"]);
    assert_eq!(result["counts"]["misses"], result["counts"]["completed"]);
    assert_eq!(active.load(Ordering::SeqCst), 0);
    assert!(maximum.load(Ordering::SeqCst) > 0 && maximum.load(Ordering::SeqCst) <= 5);
    stop.await.unwrap();
    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "manual before/after load-generator throughput comparison"]
async fn compare_generator_throughput() {
    let (url, _, server) = serve(Stub::default()).await;
    let before = std::env::var("PATCHWORK_LOAD_BEFORE")
        .expect("set PATCHWORK_LOAD_BEFORE to the previous executable");
    let after = std::env::var("PATCHWORK_LOAD_AFTER")
        .expect("set PATCHWORK_LOAD_AFTER to the updated executable");
    for (name, binary, threads) in [
        ("before", before.clone(), 1),
        ("after", after.clone(), 1),
        ("before", before, 2),
        ("after", after, 2),
    ] {
        let directory = tempfile::tempdir().unwrap();
        let config = directory.path().join("config.json");
        std::fs::write(
            &config,
            json!({"table_id": 1, "tps": 500_000, "duration": 3, "workers": 256,
            "records": 64, "keys": 8, "read_percent": 100})
            .to_string(),
        )
        .unwrap();
        let url = url.clone();
        let output = tokio::task::spawn_blocking(move || {
            std::process::Command::new(binary)
                .args([
                    "--data-url",
                    &url,
                    "--config",
                    config.to_str().unwrap(),
                    "--report-dir",
                    directory.path().to_str().unwrap(),
                ])
                .env("TOKIO_WORKER_THREADS", threads.to_string())
                .output()
                .unwrap()
        })
        .await
        .unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        let last = std::str::from_utf8(&output.stdout)
            .unwrap()
            .lines()
            .last()
            .unwrap();
        let report: Value = serde_json::from_str(last).unwrap();
        assert_eq!(report["counts"]["successes"], report["counts"]["completed"]);
        eprintln!(
            "{name}, {threads} threads: {} TPS, p95 {} ms",
            report["average_tps"], report["p95_ms"]
        );
    }
    server.abort();
}
