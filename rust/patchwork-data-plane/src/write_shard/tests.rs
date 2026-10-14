use super::*;

async fn pragma(db: &DatabaseConnection, name: &str) -> i64 {
    db.query_one_raw(sea_orm::Statement::from_string(
        sea_orm::DbBackend::Sqlite,
        format!("PRAGMA {name}"),
    ))
    .await
    .unwrap()
    .unwrap()
    .try_get("", name)
    .unwrap()
}

#[tokio::test]
async fn auto_vacuum_handles_new_and_existing_shards() {
    let dir = tempfile::tempdir().unwrap();
    for existing in [false, true] {
        let path = dir
            .path()
            .join(format!("{existing}.sqlite"))
            .to_str()
            .unwrap()
            .to_owned();
        if existing {
            let db = Database::connect(format!("sqlite://{path}?mode=rwc"))
                .await
                .unwrap();
            db.execute_unprepared("PRAGMA auto_vacuum = NONE")
                .await
                .unwrap();
            db.execute_unprepared("CREATE TABLE legacy (id INTEGER PRIMARY KEY, value TEXT)")
                .await
                .unwrap();
            db.execute_unprepared("INSERT INTO legacy VALUES (1, 'preserved')")
                .await
                .unwrap();
            db.close().await.unwrap();
        }
        let mut shard = WriteShard::new(path.clone()).await.unwrap();
        assert_eq!(pragma(&shard.db, "auto_vacuum").await, 1);
        if existing {
            let row = shard
                .db
                .query_one_raw(sea_orm::Statement::from_string(
                    sea_orm::DbBackend::Sqlite,
                    "SELECT value FROM legacy WHERE id = 1",
                ))
                .await
                .unwrap()
                .unwrap();
            assert_eq!(row.try_get::<String>("", "value").unwrap(), "preserved");
        }
        shard
            .upsert(1, 1, 1, serde_json::json!("x".repeat(100_000)), 1)
            .await
            .unwrap();
        let before = pragma(&shard.db, "page_count").await;
        shard.delete(1, 1, 1, 2).await.unwrap();
        assert!(pragma(&shard.db, "page_count").await < before);
        assert_eq!(pragma(&shard.db, "freelist_count").await, 0);
        shard.db.close().await.unwrap();
        let reopened = WriteShard::new(path).await.unwrap();
        assert_eq!(pragma(&reopened.db, "auto_vacuum").await, 1);
        reopened.db.close().await.unwrap();
    }
}
