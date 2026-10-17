use anyhow::Result;
use kameo::{Actor, message::Context, messages, reply::DelegatedReply};
use patchwork_common::dataplane_client::{Client, Record};
use tokio::task::JoinSet;

/// Owns a reusable client and sends requests to other nodes.
#[derive(Actor, Default)]
pub struct ReplicaWriter {
    client: Client,
}

#[messages]
impl ReplicaWriter {
    #[message(ctx)]
    pub async fn put(
        &mut self,
        record: Record,
        replicas: Vec<String>,
        ctx: &mut Context<Self, DelegatedReply<Result<()>>>,
    ) -> DelegatedReply<Result<()>> {
        let client = self.client.clone();
        ctx.spawn(async move {
            let mut replicas: Vec<_> = replicas
                .into_iter()
                .map(|url| url.trim_end_matches('/').to_owned())
                .collect();
            replicas.sort_unstable();
            replicas.dedup();
            let mut requests = JoinSet::new();
            for url in replicas {
                let client = client.clone();
                let record = record.clone();
                requests.spawn(async move {
                    client
                        .write_partition_record(&url, &record, &[], true)
                        .await
                        .map_err(|error| {
                            anyhow::Error::new(error).context(format!("Replica {url}"))
                        })
                });
            }
            let mut failures = Vec::new();
            while let Some(result) = requests.join_next().await {
                match result {
                    Ok(Ok(())) => {}
                    Ok(Err(error)) => failures.push(error),
                    Err(error) => failures.push(error.into()),
                }
            }
            if failures.iter().any(|error| {
                error
                    .downcast_ref::<tonic::Status>()
                    .is_some_and(|status| status.code() == tonic::Code::ResourceExhausted)
            }) {
                return Err(tonic::Status::resource_exhausted(
                    "a replica write queue is full; retry later",
                )
                .into());
            }
            anyhow::ensure!(
                failures.is_empty(),
                "Replica writes failed: {}",
                failures
                    .iter()
                    .map(|error| format!("{error:#}"))
                    .collect::<Vec<_>>()
                    .join("; ")
            );
            Ok(())
        })
    }
}
