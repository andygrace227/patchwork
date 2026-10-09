use axum::{
    Router,
    body::{Body, to_bytes},
    http::{Request, StatusCode},
};
use kameo::actor::Spawn;
use patchwork_data_plane::{api, shard_actor::ShardActor};
use serde_json::{Value, json};
use tower::ServiceExt;

async fn request(
    app: &Router,
    method: &str,
    path: &str,
    body: Option<Value>,
) -> (StatusCode, Value) {
    request_at(
        app,
        method,
        path,
        body,
        if method == "DELETE" { 3 } else { 1 },
    )
    .await
}

async fn request_at(
    app: &Router,
    method: &str,
    path: &str,
    body: Option<Value>,
    timestamp: i64,
) -> (StatusCode, Value) {
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method(method)
                .uri(path)
                .header("content-type", "application/json")
                .header("x-patchwork-timestamp", timestamp.to_string())
                .body(body.map_or_else(Body::empty, |v| Body::from(v.to_string())))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status();
    let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    (
        status,
        if bytes.is_empty() {
            Value::Null
        } else {
            serde_json::from_slice(&bytes)
                .unwrap_or_else(|_| Value::String(String::from_utf8_lossy(&bytes).into_owned()))
        },
    )
}

#[tokio::test]
async fn telemetry_tracks_local_accesses_and_clears_with_data() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("shard.sqlite").to_str().unwrap().to_owned();
    let shard = ShardActor::spawn(ShardActor::new(path.clone()).await.unwrap());
    let app = api::router(shard.clone());
    assert_eq!(
        request(&app, "GET", "/shard/telemetry", None).await,
        (StatusCode::OK, json!([]))
    );
    for (table, partition, key) in [
        (1, 10, 10),
        (1, 10, 10),
        (1, 10, 1000),
        (1, 20, 20),
        (2, 10, 30),
    ] {
        request(
            &app,
            "PUT",
            &format!("/tables/{table}/partition-keys/{partition}/records/{key}"),
            Some(json!(key)),
        )
        .await;
    }
    request(&app, "GET", "/tables/1/partition-keys/10/records/99", None).await;
    request(&app, "GET", "/tables/1/partition-keys/10", None).await;
    request(
        &app,
        "GET",
        "/tables/1/range?lower_bound=20&upper_bound=21",
        None,
    )
    .await;
    request(
        &app,
        "DELETE",
        "/tables/2/partition-keys/10/records/30",
        None,
    )
    .await;
    let stats = request(&app, "GET", "/shard/telemetry", None).await.1;
    assert_eq!(stats.as_array().unwrap().len(), 3);
    assert_eq!(stats[0]["table_id"], 1);
    assert_eq!(stats[0]["partition_key"], 10);
    assert_eq!(stats[0]["contributing_nodes"], 1);
    assert_eq!(stats[0]["average_write_position"], 340);
    assert_eq!(stats[0]["median_write_position"], 10);
    assert!(
        (stats[0]["reads_per_second"].as_f64().unwrap()
            / stats[0]["writes_per_second"].as_f64().unwrap()
            - 1.0)
            .abs()
            < 1e-9
    );
    assert!(stats[1]["reads_per_second"].as_f64().unwrap() > 0.0);
    assert_eq!(stats[2]["median_write_position"], 30);
    let (status, table_stats) = request(&app, "GET", "/tables/1/telemetry", None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(table_stats.as_object().unwrap().len(), 2);
    assert_eq!(table_stats["10"]["table_id"], 1);
    assert_eq!(table_stats["10"]["partition_key"], 10);
    assert_eq!(table_stats["10"]["contributing_nodes"], 1);
    assert_eq!(table_stats["10"]["average_write_position"], 340);
    assert_eq!(table_stats["20"]["table_id"], 1);
    assert_eq!(table_stats["20"]["partition_key"], 20);
    assert_eq!(
        request(&app, "GET", "/tables/99/telemetry", None).await,
        (StatusCode::OK, json!({}))
    );
    request(&app, "DELETE", "/tables/1/partition-keys/20", None).await;
    request(
        &app,
        "DELETE",
        "/tables/1/range?lower_bound=10&upper_bound=11",
        None,
    )
    .await;
    let stats = request(&app, "GET", "/shard/telemetry", None).await.1;
    assert_eq!(stats.as_array().unwrap().len(), 1);
    assert_eq!(stats[0]["table_id"], 2);
    assert_eq!(
        request(&app, "GET", "/tables/1/telemetry", None).await.1,
        json!({})
    );
    shard.stop_gracefully().await.unwrap();
    shard.wait_for_shutdown().await;
    drop(app);
    let reopened = ShardActor::spawn(ShardActor::new(path).await.unwrap());
    let app = api::router(reopened.clone());
    assert_eq!(
        request(&app, "GET", "/shard/telemetry", None).await.1,
        json!([])
    );
    request(&app, "GET", "/tables/2/partition-keys/10/records/99", None).await;
    let stats = request(&app, "GET", "/shard/telemetry", None).await.1;
    assert_eq!(stats[0]["average_write_position"], Value::Null);
    assert_eq!(stats[0]["median_write_position"], Value::Null);
    request(&app, "DELETE", "/tables/2", None).await;
    assert_eq!(
        request(&app, "GET", "/shard/telemetry", None).await.1,
        json!([])
    );
    reopened.stop_gracefully().await.unwrap();
    reopened.wait_for_shutdown().await;
}

#[tokio::test]
async fn operations_are_scoped_and_persist_after_reopening() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir
        .path()
        .join("nested/shard.sqlite")
        .to_str()
        .unwrap()
        .to_owned();
    let shard = ShardActor::spawn(ShardActor::new(path.clone()).await.unwrap());
    let app = api::router(shard.clone());
    for (table_id, partition_key, secondary_key) in
        [(1, 10, -1), (1, 10, 2), (1, 20, 3), (2, 10, -1)]
    {
        assert_eq!(
            request(
                &app,
                "PUT",
                &format!(
                    "/tables/{table_id}/partition-keys/{partition_key}/records/{secondary_key}"
                ),
                Some(json!({"value": "original"}))
            )
            .await
            .0,
            StatusCode::NO_CONTENT
        );
    }
    assert_eq!(
        request(&app, "GET", "/tables/1/partition-keys/10/records/-1", None)
            .await
            .1["timestamp"],
        1
    );
    assert_eq!(
        request_at(
            &app,
            "PUT",
            "/tables/1/partition-keys/10/records/-1",
            Some(json!({"value": "updated", "timestamp": 99})),
            2
        )
        .await
        .0,
        StatusCode::NO_CONTENT
    );
    let (status, record) =
        request(&app, "GET", "/tables/1/partition-keys/10/records/-1", None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(record["timestamp"], 2);
    assert_eq!(record["data"]["value"], "updated");
    assert_eq!(record["data"]["timestamp"], 99); // Payload fields do not set the record timestamp.
    assert_eq!(
        request(&app, "GET", "/tables/1/partition-keys/10", None)
            .await
            .1
            .as_array()
            .unwrap()
            .len(),
        2
    );
    let range = request(
        &app,
        "GET",
        "/tables/1/range?lower_bound=9&upper_bound=20",
        None,
    )
    .await
    .1;
    assert_eq!(range.as_array().unwrap().len(), 2);
    assert_eq!(
        request(
            &app,
            "GET",
            "/tables/1/range?lower_bound=10&upper_bound=20",
            None
        )
        .await
        .1,
        range
    );
    assert_eq!(
        request(
            &app,
            "GET",
            "/tables/1/range?lower_bound=20&upper_bound=10",
            None
        )
        .await
        .0,
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        request(&app, "PUT", "/tables/1/partition-keys/10/records/-1", None)
            .await
            .0,
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        request(
            &app,
            "DELETE",
            "/tables/1/partition-keys/10/records/-1",
            None
        )
        .await
        .1,
        json!({"deleted": 1})
    );
    assert_eq!(
        request(
            &app,
            "DELETE",
            "/tables/1/partition-keys/10/records/-1",
            None
        )
        .await
        .1,
        json!({"deleted": 0})
    );
    assert_eq!(
        request(&app, "GET", "/tables/1/partition-keys/10/records/-1", None)
            .await
            .0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        request(&app, "DELETE", "/tables/1/partition-keys/10", None)
            .await
            .1,
        json!({"deleted": 2})
    );
    assert_eq!(
        request(&app, "GET", "/tables/1/partition-keys/10", None)
            .await
            .1,
        json!([])
    );
    drop(app);
    shard.stop_gracefully().await.unwrap();
    shard.wait_for_shutdown().await;

    let shard = ShardActor::spawn(ShardActor::new(path).await.unwrap());
    let app = api::router(shard.clone());
    assert_eq!(
        request(&app, "GET", "/tables/2/partition-keys/10/records/-1", None)
            .await
            .0,
        StatusCode::OK
    );
    assert_eq!(
        request(&app, "GET", "/tables/1/partition-keys/20/records/3", None)
            .await
            .0,
        StatusCode::OK
    );
    for expected_timestamp in [2, 2] {
        assert_eq!(
            request_at(
                &app,
                "PUT",
                "/tables/1/partition-keys/20/records/3",
                Some(json!({"updated": true})),
                expected_timestamp
            )
            .await
            .0,
            StatusCode::NO_CONTENT
        );
        assert_eq!(
            request(&app, "GET", "/tables/1/partition-keys/20/records/3", None)
                .await
                .1["timestamp"],
            expected_timestamp
        );
    }
    shard.stop_gracefully().await.unwrap();
    shard.wait_for_shutdown().await;
}

#[tokio::test]
async fn timestamp_wins_regardless_of_arrival_order() {
    let dir = tempfile::tempdir().unwrap();
    let shard = ShardActor::spawn(
        ShardActor::new(
            dir.path()
                .join("timestamps.sqlite")
                .to_str()
                .unwrap()
                .into(),
        )
        .await
        .unwrap(),
    );
    let app = api::router(shard.clone());
    for (key, writes) in [
        (1, vec![(20, "new"), (10, "old"), (20, "new")]),
        (2, vec![(10, "old"), (20, "new")]),
        (3, vec![(30, "a"), (30, "z")]),
        (4, vec![(30, "z"), (30, "a")]),
    ] {
        let path = format!("/tables/1/partition-keys/1/records/{key}");
        for (timestamp, value) in writes {
            assert_eq!(
                request_at(&app, "PUT", &path, Some(json!(value)), timestamp)
                    .await
                    .0,
                StatusCode::NO_CONTENT
            );
        }
        let record = request(&app, "GET", &path, None).await.1;
        assert_eq!(
            record["data"],
            if key <= 2 { json!("new") } else { json!("z") }
        );
        assert_eq!(record["timestamp"], if key <= 2 { 20 } else { 30 });
    }
    let path = "/tables/1/partition-keys/1/records/1";
    for header in [None, Some("bad"), Some("-1")] {
        let mut req = Request::builder()
            .method("PUT")
            .uri(path)
            .header("content-type", "application/json");
        if let Some(header) = header {
            req = req.header("x-patchwork-timestamp", header);
        }
        let response = app
            .clone()
            .oneshot(req.body(Body::from("null")).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }
    assert_eq!(request(&app, "GET", path, None).await.1["timestamp"], 20);
    shard.stop_gracefully().await.unwrap();
    shard.wait_for_shutdown().await;
}

#[tokio::test]
async fn tombstones_win_ties_and_survive_reopening() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir
        .path()
        .join("tombstones.sqlite")
        .to_str()
        .unwrap()
        .to_owned();
    let shard = ShardActor::spawn(ShardActor::new(path.clone()).await.unwrap());
    let app = api::router(shard.clone());
    let record = "/tables/1/partition-keys/10/records/1";
    // Delete before the original write arrives, including an exact timestamp tie.
    assert_eq!(
        request_at(&app, "DELETE", record, None, 20).await.1,
        json!({"deleted": 1})
    );
    for timestamp in [10, 20] {
        assert_eq!(
            request_at(&app, "PUT", record, Some(json!("late")), timestamp)
                .await
                .0,
            StatusCode::NO_CONTENT
        );
        assert_eq!(
            request(&app, "GET", record, None).await.0,
            StatusCode::NOT_FOUND
        );
    }
    let stored = request(&app, "GET", &format!("{record}?include_deleted=true"), None)
        .await
        .1;
    assert_eq!(stored["deleted"], true);
    assert_eq!(stored["timestamp"], 20);
    assert_eq!(stored["data"], Value::Null);
    assert_eq!(
        request(&app, "GET", "/tables/1/partition-keys/10", None)
            .await
            .1,
        json!([])
    );
    assert_eq!(
        request(
            &app,
            "GET",
            "/tables/1/range?lower_bound=0&upper_bound=20",
            None
        )
        .await
        .1,
        json!([])
    );
    assert_eq!(
        request(
            &app,
            "GET",
            "/tables/1/range/count?lower_bound=0&upper_bound=20",
            None
        )
        .await
        .1,
        json!({"count": 0})
    );
    assert_eq!(
        request(
            &app,
            "GET",
            "/tables/1/range/size?lower_bound=0&upper_bound=20",
            None
        )
        .await
        .1,
        json!({"size_bytes": 0})
    );
    assert_eq!(
        request(
            &app,
            "GET",
            "/tables/1/range?lower_bound=0&upper_bound=20&include_deleted=true",
            None
        )
        .await
        .1,
        json!([stored])
    );
    // A newer write restores the record, but an older delete cannot remove it.
    request_at(&app, "PUT", record, Some(json!("new")), 30).await;
    assert_eq!(
        request_at(&app, "DELETE", record, None, 29).await.1,
        json!({"deleted": 0})
    );
    assert_eq!(request(&app, "GET", record, None).await.1["data"], "new");
    request_at(&app, "DELETE", record, None, 30).await;
    assert_eq!(
        request(&app, "GET", record, None).await.0,
        StatusCode::NOT_FOUND
    );
    drop(app);
    shard.stop_gracefully().await.unwrap();
    shard.wait_for_shutdown().await;
    let shard = ShardActor::spawn(ShardActor::new(path).await.unwrap());
    let app = api::router(shard.clone());
    request_at(&app, "PUT", record, Some(json!("late")), 30).await;
    assert_eq!(
        request(&app, "GET", record, None).await.0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        request(&app, "GET", &format!("{record}?include_deleted=true"), None)
            .await
            .1["timestamp"],
        30
    );
    shard.stop_gracefully().await.unwrap();
    shard.wait_for_shutdown().await;
}
