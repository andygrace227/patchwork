use anyhow::{Result, ensure};
use kameo::{
    Actor,
    actor::ActorRef,
    message::Context,
    messages,
    reply::{DelegatedReply, ForwardedReply},
};
use patchwork_common::dataplane_client::Record;
use serde::Serialize;
use wal_db::{SegmentedStore, Wal};

use crate::{
    subordinate_shard_writer::{Put, SubordinateShardWriter},
    write_shard::{self, GetPath, WriteShard},
};

type StorageReply<M> = ForwardedReply<M, Result<u64>>;

// Keep the existing WAL write format; Kameo mailboxes own queued work.
#[derive(Serialize)]
enum Mutation {
    Write(Record),
    DeleteTable {
        table_id: i64,
    },
    DeletePartitionKey {
        table_id: i64,
        partition_key: i64,
    },
    DeleteRange {
        table_id: i64,
        upper_bound: i64,
        lower_bound: i64,
    },
}

#[derive(Serialize)]
enum LogEntry<'a> {
    Write {
        mutation: &'a Mutation,
        replicas: &'a [String],
    },
}

/// Appends the WAL and forwards individual requests to the storage and network actors.
pub struct WriteHandler {
    shard: ActorRef<WriteShard>,
    wal: Option<Wal<SegmentedStore>>,
    shard_writer: ActorRef<SubordinateShardWriter>,
}

impl WriteHandler {
    pub fn new(
        shard: ActorRef<WriteShard>,
        shard_writer: ActorRef<SubordinateShardWriter>,
    ) -> Self {
        Self {
            shard,
            wal: None,
            shard_writer,
        }
    }

    fn append(&self, mutation: &Mutation, replicas: &[String]) -> Result<()> {
        let _ = self
            .wal
            .as_ref()
            .expect("actor started")
            .append(&serde_json::to_vec(&LogEntry::Write {
                mutation,
                replicas,
            })?)?;
        Ok(())
    }
}

impl Actor for WriteHandler {
    type Args = Self;
    type Error = anyhow::Error;

    async fn on_start(mut state: Self, _: ActorRef<Self>) -> Result<Self> {
        let path = state.shard.ask(GetPath {}).await?;
        state.wal = Some(Wal::open_segmented(
            format!("{}.write-wal", path.display()),
            16 * 1024 * 1024,
        )?);
        Ok(state)
    }
}

#[messages]
impl WriteHandler {
    #[message(ctx)]
    pub async fn write_record(
        &mut self,
        mut record: Record,
        replicas: Vec<String>,
        consistent: bool,
        ctx: &mut Context<Self, DelegatedReply<Result<()>>>,
    ) -> DelegatedReply<Result<()>> {
        if record.timestamp < 0 {
            return ctx.reply(Err(anyhow::anyhow!("timestamp must be nonnegative")));
        }
        if record.deleted {
            record.data = serde_json::Value::Null;
        }
        if let Err(error) = self.append(&Mutation::Write(record.clone()), &replicas) {
            return ctx.reply(Err(error));
        }
        // Enqueue before releasing the handler so cleanup cannot overtake this write.
        let local = match self
            .shard
            .ask(write_shard::WriteRecord {
                table_id: record.table_id,
                partition_key: record.partition_key,
                secondary_key: record.secondary_key,
                data: record.data.clone(),
                timestamp: record.timestamp,
                deleted: record.deleted,
            })
            .enqueue()
            .await
        {
            Ok(reply) => reply,
            Err(error) => return ctx.reply(Err(anyhow::anyhow!("{error}"))),
        };
        let remote = if replicas.is_empty() {
            None
        } else if consistent {
            match self
                .shard_writer
                .ask(Put { record, replicas })
                .enqueue()
                .await
            {
                Ok(reply) => Some(reply),
                Err(error) => return ctx.reply(Err(anyhow::anyhow!("{error}"))),
            }
        } else {
            if let Err(error) = self.shard_writer.tell(Put { record, replicas }).await {
                return ctx.reply(Err(anyhow::anyhow!("{error}")));
            }
            None
        };
        ctx.spawn(async move {
            // Both actors work independently; waiting for their replies does not block admission.
            let (local, remote) = tokio::join!(local, async {
                match remote {
                    Some(reply) => reply.await.map_err(|error| anyhow::anyhow!("{error}")),
                    None => Ok(()),
                }
            });
            local.map_err(|error| anyhow::anyhow!("{error}"))?;
            remote?;
            Ok(())
        })
    }

    #[message(ctx)]
    pub async fn upsert_consistent(
        &mut self,
        mut record: Record,
        replicas: Vec<String>,
        ctx: &mut Context<Self, DelegatedReply<Result<()>>>,
    ) -> DelegatedReply<Result<()>> {
        record.deleted = false;
        self.write_record(record, replicas, true, ctx).await
    }

    #[message(ctx)]
    pub async fn upsert_inconsistent(
        &mut self,
        mut record: Record,
        replicas: Vec<String>,
        ctx: &mut Context<Self, DelegatedReply<Result<()>>>,
    ) -> DelegatedReply<Result<()>> {
        record.deleted = false;
        self.write_record(record, replicas, false, ctx).await
    }

    #[message(ctx)]
    pub async fn delete(
        &mut self,
        table_id: i64,
        partition_key: i64,
        secondary_key: i64,
        timestamp: i64,
        ctx: &mut Context<Self, StorageReply<write_shard::Delete>>,
    ) -> StorageReply<write_shard::Delete> {
        if timestamp < 0 {
            return StorageReply::from_err(anyhow::anyhow!("timestamp must be nonnegative"));
        }
        if let Err(error) = self.append(
            &Mutation::Write(Record {
                table_id,
                partition_key,
                secondary_key,
                timestamp,
                deleted: true,
                data: serde_json::Value::Null,
            }),
            &[],
        ) {
            return StorageReply::from_err(error);
        }
        ctx.forward(
            &self.shard,
            write_shard::Delete {
                table_id,
                partition_key,
                secondary_key,
                timestamp,
            },
        )
        .await
    }

    #[message(ctx)]
    pub async fn delete_partition_key(
        &mut self,
        table_id: i64,
        partition_key: i64,
        ctx: &mut Context<Self, StorageReply<write_shard::DeletePartitionKey>>,
    ) -> StorageReply<write_shard::DeletePartitionKey> {
        if let Err(error) = self.append(
            &Mutation::DeletePartitionKey {
                table_id,
                partition_key,
            },
            &[],
        ) {
            return StorageReply::from_err(error);
        }
        ctx.forward(
            &self.shard,
            write_shard::DeletePartitionKey {
                table_id,
                partition_key,
            },
        )
        .await
    }

    #[message(ctx)]
    pub async fn delete_table(
        &mut self,
        table_id: i64,
        ctx: &mut Context<Self, StorageReply<write_shard::DeleteTable>>,
    ) -> StorageReply<write_shard::DeleteTable> {
        if let Err(error) = self.append(&Mutation::DeleteTable { table_id }, &[]) {
            return StorageReply::from_err(error);
        }
        ctx.forward(&self.shard, write_shard::DeleteTable { table_id })
            .await
    }

    #[message(ctx)]
    pub async fn delete_range(
        &mut self,
        table_id: i64,
        upper_bound: i64,
        lower_bound: i64,
        ctx: &mut Context<Self, StorageReply<write_shard::DeleteRange>>,
    ) -> StorageReply<write_shard::DeleteRange> {
        if let Err(error) = (|| -> Result<()> {
            ensure!(
                lower_bound < upper_bound,
                "lower_bound must be less than upper_bound"
            );
            self.append(
                &Mutation::DeleteRange {
                    table_id,
                    upper_bound,
                    lower_bound,
                },
                &[],
            )
        })() {
            return StorageReply::from_err(error);
        }
        ctx.forward(
            &self.shard,
            write_shard::DeleteRange {
                table_id,
                upper_bound,
                lower_bound,
            },
        )
        .await
    }
}
