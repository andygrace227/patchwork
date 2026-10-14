use super::*;
use kameo::actor::Spawn;

#[tokio::test]
async fn failed_batch_keeps_cache_and_rolls_back_every_chunk() {
    let dir = tempfile::tempdir().unwrap();
    let mut actor = AccessStatsActor::new(dir.path().join("stats.sqlite"))
        .await
        .unwrap();
    for key in 0..60 {
        actor.track_access(1, key, i64::MAX, true).await;
    }
    actor.db.execute_unprepared(
        "CREATE TRIGGER fail_batch BEFORE INSERT ON access_statistics WHEN NEW.hash_start = 59 BEGIN SELECT RAISE(ABORT, 'test failure'); END",
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
        actor.track_access(1, 0, i64::MAX, true).await;
        actor.flush().await.unwrap();
    }
    assert_eq!(actor.get_history(1, 0, 0, i64::MAX).await.unwrap().len(), 3);
}

#[tokio::test]
async fn idle_flush_prunes_expired_history_and_preserves_new_rows() {
    let dir = tempfile::tempdir().unwrap();
    let mut actor = AccessStatsActor::new(dir.path().join("stats.sqlite"))
        .await
        .unwrap();
    actor.track_access(1, 10, i64::MAX, true).await;
    actor.track_access(1, 10, i64::MAX, true).await;
    actor.flush().await.unwrap();
    let rows = actor.get_history(1, 10, 0, i64::MAX).await.unwrap();
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
            table_id: 1,
            partition_key: 10,
            secondary_key: 42,
            is_write: true,
        })
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let rows = actor
                .ask(GetHistory {
                    table_id: 1,
                    hash_start: 10,
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
            table_id: 2,
            partition_key: 10,
            secondary_key: 99,
            is_write: false,
        })
        .await
        .unwrap();
    actor.stop_gracefully().await.unwrap();
    actor.wait_for_shutdown().await;
    let reopened = AccessStatsActor::new(&path).await.unwrap();
    assert_eq!(
        reopened.get_history(1, 10, 0, i64::MAX).await.unwrap()[0].write_count,
        1
    );
    assert_eq!(
        reopened.get_history(2, 10, 0, i64::MAX).await.unwrap()[0].read_count,
        1
    );
    assert!(reopened.get_telemetry().await.unwrap().is_empty());
}
