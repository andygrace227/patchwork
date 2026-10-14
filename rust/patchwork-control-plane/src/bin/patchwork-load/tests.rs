use super::*;
use axum::{
    Router,
    http::{HeaderMap, Method, StatusCode},
};

#[test]
fn validates_inputs_and_preserves_extreme_keys() {
    let urls = vec!["http://127.0.0.1:3100".into()];
    let config = Config::parse(json!({"table_id": "1", "keys": 3}), &urls).unwrap();
    assert_eq!(
        (
            config.partition_key(0),
            config.partition_key(1),
            config.partition_key(2)
        ),
        (i64::MIN, -1, i64::MAX)
    );
    let hot = Config::parse(
        json!({"table_id": 1, "distribution": "hot", "hot_key": i64::MAX.to_string()}),
        &urls,
    )
    .unwrap();
    assert_eq!(hot.partition_key(100), i64::MAX);
    for values in [
        json!({"table_id": 1, "workers": 0}),
        json!({"table_id": 1, "consistent": "false"}),
        json!({"table_id": 1, "node": "control-2"}),
    ] {
        assert!(Config::parse(values, &urls).is_err());
    }
}

async fn stub(method: Method, headers: HeaderMap) -> StatusCode {
    if method == Method::PUT {
        assert!(
            headers["x-patchwork-timestamp"]
                .to_str()
                .unwrap()
                .parse::<i64>()
                .unwrap()
                > 0
        );
        StatusCode::NO_CONTENT
    } else {
        StatusCode::NOT_FOUND
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mixed_load_seeds_counts_misses_and_saves_report() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        axum::serve(listener, Router::new().fallback(stub))
            .await
            .unwrap()
    });
    let directory = tempfile::tempdir().unwrap();
    let config = Config::parse(
        json!({"table_id": "1", "tps": 10_000, "duration": 1,
        "records": 20, "keys": 3, "workers": 128, "read_percent": 50,
        "consistent": false, "telemetry": true}),
        &[url],
    )
    .unwrap();
    let report = directory
        .path()
        .join(format!("load-{}.json", config.run_id));
    let (_sender, cancel) = watch::channel(false);
    run(config, cancel, directory.path().into()).await.unwrap();
    let result: Value = serde_json::from_slice(&std::fs::read(report).unwrap()).unwrap();
    assert_eq!(result["phase"], "finished");
    assert_eq!(result["counts"]["seeded"], 20);
    assert_eq!(
        result["counts"]
            .get("errors")
            .and_then(Value::as_u64)
            .unwrap_or(0),
        0
    );
    assert_eq!(result["counts"]["in_flight"], 0);
    assert_eq!(result["counts"]["misses"], result["counts"]["reads"]);
    assert!(result["counts"]["writes"].as_u64().unwrap() > 0);
    let completed = result["counts"]["completed"].as_u64().unwrap();
    assert!(completed > 0 && completed <= 10_001);
    eprintln!(
        "Fast HTTP stub: {} TPS; {} skipped slots",
        result["average_tps"], result["counts"]["skipped"]
    );
    server.abort();
}

#[tokio::test]
async fn cancellation_and_seed_failure_drain_in_flight_requests() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        axum::serve(
            listener,
            Router::new().fallback(|| async {
                tokio::time::sleep(Duration::from_millis(25)).await;
                StatusCode::INTERNAL_SERVER_ERROR
            }),
        )
        .await
        .unwrap()
    });
    let directory = tempfile::tempdir().unwrap();
    for seed in [false, true] {
        let config = Config::parse(
            json!({"table_id": 1, "tps": 100, "duration": 30,
            "records": 20, "keys": 3, "workers": 2, "read_percent": 50, "seed": seed}),
            &[url.clone()],
        )
        .unwrap();
        let report = directory
            .path()
            .join(format!("load-{}.json", config.run_id));
        let (sender, cancel) = watch::channel(false);
        let stop = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(100)).await;
            let _ = sender.send(true);
        });
        run(config, cancel, directory.path().into()).await.unwrap();
        let result: Value = serde_json::from_slice(&std::fs::read(report).unwrap()).unwrap();
        assert_eq!(result["counts"]["in_flight"], 0);
        assert_eq!(result["phase"], if seed { "failed" } else { "stopped" });
        if seed {
            assert!(result["counts"].get("completed").is_none());
        } else {
            assert!(result["counts"]["errors"].as_u64().unwrap() > 0);
        }
        stop.await.unwrap();
    }
    server.abort();
}
