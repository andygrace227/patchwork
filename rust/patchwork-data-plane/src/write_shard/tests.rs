use super::*;
use patchwork_common::dataplane_client::Record;
use sea_orm::Database;
use sea_orm::QueryOrder;
use serde_json::json;

#[tokio::test]
async fn grouped_writes_match_ordered_single_writes_across_deletes_and_ties() {
    let dir = tempfile::tempdir().unwrap();
    let mut grouped = crate::utils::shard(dir.path().join("grouped.sqlite").display().to_string())
        .await
        .unwrap();
    let single = crate::utils::shard(dir.path().join("single.sqlite").display().to_string())
        .await
        .unwrap();
    let mut mutations = Vec::new();
    for i in 0..SQL_WRITE_BATCH_SIZE * 3 + 7 {
        mutations.push(Mutation::Write(Record {
            table_id: 1 + (i % 2) as i64,
            partition_id: None,
            partition_key: (i % 3) as i64,
            secondary_key: (i % 19) as i64,
            data: serde_json::to_vec(&json!({"value": i % 5})).unwrap(),
            timestamp: (i % 7) as i64,
            deleted: i % 11 == 0,
        }));
        if i == SQL_WRITE_BATCH_SIZE + 3 {
            mutations.push(Mutation::DeletePartitionKey {
                table_id: 1,
                partition_key: 0,
            });
        }
        if i == SQL_WRITE_BATCH_SIZE * 2 + 3 {
            mutations.push(Mutation::DeleteRange {
                table_id: 2,
                lower_bound: 0,
                upper_bound: 2,
            });
            mutations.push(Mutation::DeleteTable { table_id: 1 });
        }
    }
    grouped
        .apply_batch(Arc::new(mutations.clone()))
        .await
        .unwrap();
    let txn = single.db.begin().await.unwrap();
    for mutation in &mutations {
        txn.execute_raw(WriteShard::mutation_statement(mutation).unwrap())
            .await
            .unwrap();
    }
    txn.commit().await.unwrap();
    let query = || {
        data::Entity::find()
            .order_by_asc(data::Column::TableId)
            .order_by_asc(data::Column::PartitionKey)
            .order_by_asc(data::Column::SecondaryKey)
    };
    assert_eq!(
        query().all(&grouped.db).await.unwrap(),
        query().all(&single.db).await.unwrap()
    );
}

#[tokio::test]
async fn failure_in_a_later_sql_group_rolls_back_the_whole_batch() {
    let dir = tempfile::tempdir().unwrap();
    let mut shard = crate::utils::shard(dir.path().join("shard.sqlite").display().to_string())
        .await
        .unwrap();
    shard.db.execute_unprepared("CREATE TRIGGER reject_later BEFORE INSERT ON data WHEN NEW.secondary_key = 257 BEGIN SELECT RAISE(ABORT, 'reject later group'); END").await.unwrap();
    let mutations = (0..SQL_WRITE_BATCH_SIZE + 3)
        .map(|i| {
            Mutation::Write(Record {
                table_id: 1,
                partition_id: None,
                partition_key: 1,
                secondary_key: i as i64,
                data: serde_json::to_vec(&json!(i)).unwrap(),
                timestamp: 1,
                deleted: false,
            })
        })
        .collect();
    assert!(shard.apply_batch(Arc::new(mutations)).await.is_err());
    assert!(
        data::Entity::find()
            .all(&shard.db)
            .await
            .unwrap()
            .is_empty()
    );
    assert!(shard.get_telemetry().await.unwrap().is_empty());
}

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
        let mut shard = crate::utils::shard(path.clone()).await.unwrap();
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
        shard.upsert(1, 1, 1, vec![b'x'; 100_000], 1).await.unwrap();
        let before = pragma(&shard.db, "page_count").await;
        shard.delete(1, 1, 1, 2).await.unwrap();
        assert!(pragma(&shard.db, "page_count").await < before);
        assert_eq!(pragma(&shard.db, "freelist_count").await, 0);
        shard.db.close().await.unwrap();
        let reopened = crate::utils::shard(path).await.unwrap();
        assert_eq!(pragma(&reopened.db, "auto_vacuum").await, 1);
        reopened.db.close().await.unwrap();
    }
}
