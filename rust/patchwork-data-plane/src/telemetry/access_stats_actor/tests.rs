use super::*;
use kameo::actor::Spawn;

#[tokio::test]
async fn partition_ids_survive_flush_and_upgrade_legacy_history() {
    let dir = tempfile::tempdir().unwrap();
    for labelled in [false, true] {
        let path = dir.path().join(format!("{labelled}.sqlite"));
        let mut actor = AccessStatsActor::new(&path).await.unwrap();
        actor
            .track_access(1, 10, true, Some("partition-a".into()))
            .await;
        actor.flush().await.unwrap();
        // Both historical schemas had a required per-key column.
        actor
            .db
            .execute_unprepared(
                "ALTER TABLE access_statistics ADD COLUMN hash_start INTEGER NOT NULL DEFAULT 10",
            )
            .await
            .unwrap();
        actor.db.execute_unprepared("CREATE INDEX access_statistics_key_time ON access_statistics (table_id, hash_start, start_ms)").await.unwrap();
        if !labelled {
            actor
                .db
                .execute_unprepared("DROP INDEX access_statistics_partition_time")
                .await
                .unwrap();
            actor
                .db
                .execute_unprepared("ALTER TABLE access_statistics DROP COLUMN partition_id")
                .await
                .unwrap();
        }
        actor.db.close().await.unwrap();

        let mut actor = AccessStatsActor::new(&path).await.unwrap();
        let rows = Entity::find().all(&actor.db).await.unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0].partition_id.as_deref(),
            labelled.then_some("partition-a")
        );
        actor
            .track_access(1, 20, true, Some("partition-a".into()))
            .await;
        actor
            .track_access(1, 30, true, Some("partition-a".into()))
            .await;
        let telemetry = actor.get_telemetry_for_table(1).await.unwrap();
        assert_eq!(telemetry.len(), 1);
        assert_eq!(telemetry["partition-a"].statistics.write_count, 2);
        actor.flush().await.unwrap();
        let history = actor
            .get_history(1, "partition-a".into(), 0, i64::MAX)
            .await
            .unwrap();
        assert_eq!(history.len(), if labelled { 2 } else { 1 });
        assert!(history.iter().any(|row| row.write_count == 2));
    }
}

#[tokio::test]
async fn failed_batch_keeps_cache_and_rolls_back_every_chunk() {
    let dir = tempfile::tempdir().unwrap();
    let mut actor = AccessStatsActor::new(dir.path().join("stats.sqlite"))
        .await
        .unwrap();
    for key in 0..60 {
        actor
            .track_access(1, i64::MAX, true, Some(format!("{key:02}")))
            .await;
    }
    actor.db.execute_unprepared(
        "CREATE TRIGGER fail_batch BEFORE INSERT ON access_statistics WHEN NEW.partition_id = '59' BEGIN SELECT RAISE(ABORT, 'test failure'); END",
    ).await.unwrap();
    assert!(actor.flush().await.is_err());
    assert_eq!(actor.cache.stats().len(), 60);
    assert!(Entity::find().all(&actor.db).await.unwrap().is_empty());
    actor
        .db
        .execute_unprepared("DROP TRIGGER fail_batch")
        .await
        .unwrap();
    actor.flush().await.unwrap();
    assert!(actor.cache.stats().is_empty());
    assert_eq!(Entity::find().all(&actor.db).await.unwrap().len(), 60);
    // Two quick flushes of the same key append separate rows, even within one millisecond.
    for _ in 0..2 {
        actor
            .track_access(1, i64::MAX, true, Some("00".into()))
            .await;
        actor.flush().await.unwrap();
    }
    assert_eq!(
        actor
            .get_history(1, "00".into(), 0, i64::MAX)
            .await
            .unwrap()
            .len(),
        3
    );
}

#[tokio::test]
async fn idle_flush_prunes_expired_history_and_preserves_new_rows() {
    let dir = tempfile::tempdir().unwrap();
    let mut actor = AccessStatsActor::new(dir.path().join("stats.sqlite"))
        .await
        .unwrap();
    actor
        .track_access(1, i64::MAX, true, Some("partition-a".into()))
        .await;
    actor
        .track_access(1, i64::MAX, true, Some("partition-a".into()))
        .await;
    actor.flush().await.unwrap();
    let rows = actor
        .get_history(1, "partition-a".into(), 0, i64::MAX)
        .await
        .unwrap();
    assert_eq!(
        rows[0].write_position_sum,
        (i64::MAX as i128 * 2).to_string()
    );
    let mut expired = rows[0].clone().into_active_model();
    expired.id = NotSet;
    expired.start_ms = sea_orm::Set(0);
    expired.end_ms = sea_orm::Set(1);
    Entity::insert(expired).exec(&actor.db).await.unwrap();
    actor.flush().await.unwrap();
    assert_eq!(Entity::find().all(&actor.db).await.unwrap().len(), 1);
}

#[tokio::test]
async fn timer_and_shutdown_flush_to_a_database_that_survives_restart() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("stats.sqlite");
    let actor = AccessStatsActor::spawn(
        AccessStatsActor::with_config(&path, Duration::from_millis(10), RETENTION)
            .await
            .unwrap(),
    );
    actor
        .ask(TrackAccess {
            partition_id: Some("partition-a".into()),
            table_id: 1,
            partition_key: 10,
            is_write: true,
        })
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let rows = actor
                .ask(GetHistory {
                    table_id: 1,
                    partition_id: "partition-a".into(),
                    start_ms: 0,
                    end_ms: i64::MAX,
                })
                .await
                .unwrap();
            if !rows.is_empty() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    actor
        .ask(TrackAccess {
            partition_id: Some("partition-a".into()),
            table_id: 2,
            partition_key: 10,
            is_write: false,
        })
        .await
        .unwrap();
    actor.stop_gracefully().await.unwrap();
    actor.wait_for_shutdown().await;
    let reopened = AccessStatsActor::new(&path).await.unwrap();
    assert_eq!(
        reopened
            .get_history(1, "partition-a".into(), 0, i64::MAX)
            .await
            .unwrap()[0]
            .write_count,
        1
    );
    assert_eq!(
        reopened
            .get_history(2, "partition-a".into(), 0, i64::MAX)
            .await
            .unwrap()[0]
            .read_count,
        1
    );
    assert!(reopened.get_telemetry().await.unwrap().is_empty());
}
