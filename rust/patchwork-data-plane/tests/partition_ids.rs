mod support;

use patchwork_common::dataplane_client::{Client, Record};
use sea_orm::{ConnectionTrait, Database};
use support::Node;

#[tokio::test]
async fn replicas_keep_partition_ids_and_relabelling_preserves_newer_data() {
    let dir = tempfile::tempdir().unwrap();
    let first_path = dir.path().join("first.sqlite");
    let first_wal = dir.path().join("first.wal");
    let first = Node::start(&first_path, &first_wal, None).await;
    let second = Node::start(
        &dir.path().join("second.sqlite"),
        &dir.path().join("second.wal"),
        None,
    )
    .await;
    let client = Client::new();
    let mut record = Record {
        table_id: 1,
        partition_id: Some("source".into()),
        partition_key: 10,
        secondary_key: 42,
        data: vec![0, 255, 128],
        timestamp: 20,
        deleted: false,
    };
    for consistent in [true, false] {
        record.secondary_key += 1;
        client
            .write_partition_record(&first.url, &record, &[second.url.clone()], consistent)
            .await
            .unwrap();
        first.flush().await;
        // Inconsistent forwarding continues after the local acknowledgement.
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                second.flush().await;
                if client
                    .get(&second.url, 1, 10, record.secondary_key)
                    .await
                    .is_ok()
                {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(1)).await;
            }
        })
        .await
        .unwrap();
        for node in [&first, &second] {
            assert_eq!(
                client
                    .get(&node.url, 1, 10, record.secondary_key)
                    .await
                    .unwrap(),
                record
            );
            assert_eq!(
                client.get_telemetry(&node.url).await.unwrap()[0]
                    .partition_id
                    .as_str(),
                "source"
            );
        }
    }

    let mut another_key = record.clone();
    another_key.partition_key = 99;
    client
        .write_partition_record(&first.url, &another_key, &[], true)
        .await
        .unwrap();
    first.flush().await;
    let telemetry = client.get_telemetry_for_table(&first.url, 1).await.unwrap();
    assert_eq!(telemetry.len(), 1);
    assert_eq!(telemetry["source"].statistics.write_count, 3);
    assert_eq!(
        telemetry["source"].statistics.average_write_position,
        Some(39)
    );
    assert_eq!(
        telemetry["source"].statistics.median_write_position,
        Some(10)
    );

    // Copying to an existing replica must relabel even when its payload is newer.
    let mut copied = record.clone();
    copied.partition_id = Some("destination".into());
    copied.timestamp = 1;
    copied.data = b"older copy".to_vec();
    client
        .write_partition_record(&first.url, &copied, &[], true)
        .await
        .unwrap();
    first.flush().await;
    record.partition_id = copied.partition_id;
    assert_eq!(
        client
            .get(&first.url, 1, 10, record.secondary_key)
            .await
            .unwrap(),
        record
    );
    // A legacy caller cannot clear a known partition label.
    client
        .upsert(&first.url, 1, 10, record.secondary_key, &record.data, 21)
        .await
        .unwrap();
    first.flush().await;
    record.timestamp = 21;
    first.stop().await;
    let restarted = Node::start(&first_path, &first_wal, None).await;
    assert_eq!(
        client
            .get(&restarted.url, 1, 10, record.secondary_key)
            .await
            .unwrap(),
        record
    );

    record.deleted = true;
    record.timestamp = 22;
    client
        .write_partition_record(&restarted.url, &record, &[], true)
        .await
        .unwrap();
    restarted.flush().await;
    let deleted = client
        .get_including_deleted(&restarted.url, 1, 10, record.secondary_key)
        .await
        .unwrap();
    assert!(deleted.deleted);
    assert!(deleted.data.is_empty());
    assert_eq!(deleted.partition_id, record.partition_id);
    second.stop().await;
    restarted.stop().await;
}

#[tokio::test]
async fn old_sqlite_files_and_wal_records_keep_unknown_partition_ids() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("legacy.sqlite");
    let db = Database::connect(format!("sqlite://{}?mode=rwc", path.display()))
        .await
        .unwrap();
    db.execute_unprepared("CREATE TABLE data (table_id INTEGER NOT NULL, partition_key INTEGER NOT NULL, secondary_key INTEGER NOT NULL, data BLOB NOT NULL, timestamp INTEGER NOT NULL, deleted BOOLEAN NOT NULL, PRIMARY KEY (table_id, partition_key, secondary_key))").await.unwrap();
    db.execute_unprepared("INSERT INTO data VALUES (1, 10, 1, X'00FF', 1, 0)")
        .await
        .unwrap();
    db.close().await.unwrap();
    let wal_path = dir.path().join("legacy.wal");
    let wal = wal_db::Wal::open_segmented(&wal_path, 16 * 1024 * 1024).unwrap();
    for (key, partition_id) in [(2, None), (3, Some("known"))] {
        let mut record = serde_json::json!({
            "table_id": 1, "partition_key": 10, "secondary_key": key,
            "data": [128, 255], "timestamp": 2, "deleted": false,
        });
        if let Some(id) = partition_id {
            record["partition_id"] = id.into();
        }
        let entry = serde_json::json!({"Write": {"mutation": {"Write": record}}});
        let _ = wal.append(&serde_json::to_vec(&entry).unwrap()).unwrap();
    }
    drop(wal);
    let node = Node::start(&path, &wal_path, None).await;
    node.flush().await;
    for (key, id) in [(1, None), (2, None), (3, Some("known"))] {
        let record = node.client.get(&node.url, 1, 10, key).await.unwrap();
        assert_eq!(record.partition_id.as_deref(), id);
    }
    assert_eq!(
        node.client.get(&node.url, 1, 10, 1).await.unwrap().data,
        vec![0, 255]
    );
    node.stop().await;
}
