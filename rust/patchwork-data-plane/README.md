# Data plane

Run from the Rust workspace directory:

```sh
cp .env.example .env
cargo run -p patchwork-data-plane
```

Edit an existing .env instead of overwriting it. Startup loads .env from the working
directory or a parent; exported environment variables take precedence.

- BIND_ADDR defaults to 127.0.0.1:3000.
- SHARD_PATH is required. Relative paths resolve from the working directory.
- READ_SHARD_POOL_SIZE defaults to 4 and must be a positive integer.
- ACCESS_STATS_PATH defaults to `<SHARD_PATH>.access-stats.sqlite`, a separate database.
- ACCESS_STATS_FLUSH_SECONDS defaults to 60 and must be positive.
- ACCESS_STATS_RETENTION_SECONDS defaults to 2592000 (30 days) and must be positive.

`initialize` opens one SQLite shard through SeaORM, enables WAL, creates its data table
if missing, and keeps the single write connection in `WriteShard`. Reads use
`kameo_actors::pool::ActorPool<ReadShard>` through `Dispatch` messages. Each reader
opens one read-only connection to the same WAL database. Point reads, partition-key
reads, range scans, counts, and payload-size queries use the pool through gRPC.
Kameo selects a worker with the least queued work. Data queries use bound parameters
and SQLx's prepared statement cache on each connection; startup PRAGMA and VACUUM
commands run unprepared.
`WriteHandler` appends requests to `<SHARD_PATH>.wal` and sends pending
mutations to `WriteShard` in batches every 5 ms. One batch is in flight at a time;
new writes accumulate in a separate queue while SQLite commits. Successful batch
replies advance the committed log position. Failed batches return to the front
of the queue for retry, and cleanup removes only committed log entries.
`ReplicaWriter` sends neighbor requests independently. Consistent writes wait for
local log acceptance and all neighbor acknowledgements; inconsistent writes wait
for local log acceptance and replication queue admission. Reads use the read pool
directly and may lag acknowledged writes until SQLite applies their batch.

WAL appends do not force a disk sync. Startup replays retained log entries before
accepting requests; inconsistent neighbor requests remain best effort.

Existing tables are not migrated automatically. Ctrl-C drains requests and
`WriteHandler` before stopping its network writer, the read pool, and `WriteShard`,
then flushes access statistics before stopping their actor.

Set `READ_SHARD_POOL_SIZE=8` in `.env` or the environment to change the reader
count, then restart the data nodes. This is separate from Tokio's thread count.
An acknowledgement covers log acceptance; reads can lag until the queued batch commits.

Control-to-data and data-to-data calls use gRPC exclusively, including replication,
partition copying, cleanup, and telemetry. Each `Client` caches one HTTP/2
channel per node; clones share its cache, and separately constructed clients own
separate caches. Retain a client across requests. The data service serves only gRPC
on `BIND_ADDR`; REST routes have been removed. Node URLs still use `http://` or
`https://` because gRPC uses HTTP/2. The public control-plane API remains HTTP/JSON.
Restart all data nodes with this version before starting the updated control plane.

The service definition is `patchwork-common/proto/dataplane.proto`. Keys, timestamps,
deletion flags, and telemetry use protobuf fields; record payloads are raw bytes. The control-plane API serializes JSON before storage.
Write timestamps must be present (including zero) and are preserved
through replication and copying. Client errors are `tonic::Status`, including
`NotFound` for missing records and `InvalidArgument` for invalid requests.
Requests time out after 30 seconds, or 60 seconds for all-replica writes.
Requests and responses, including whole range replies, are limited to 64 MiB.
The build supplies `protoc` automatically; no separate installation is needed.

Writes use `WriteRecord`, `WriteWithReplicas`, or `WriteWithReplicasInconsistent`.
Every write requires a nonnegative signed 64-bit `timestamp` in Unix microseconds,
chosen by the caller. Missing or negative timestamps return `InvalidArgument`.
The server never generates one.

The highest timestamp wins for the entire JSON record. Deletion wins an equal
timestamp tie; ties between live values use the greater serialized JSON byte
sequence, so arrival order does not decide the winner. An older write is acknowledged without replacing newer
data. Reuse the original timestamp for retries and copies. Clocks are the
caller's responsibility; timestamps do not prove real-world event order.

The shared client requires a timestamp argument:

```rust,ignore
use patchwork_common::dataplane_client::Client;

let client = Client::new();
client.upsert(url, table_id, partition_key, secondary_key, &data, timestamp).await?;
client.put_with_replicas(url, table_id, partition_key, secondary_key, &data, &replicas, timestamp).await?;
client.put_with_replicas_inconsistent(url, table_id, partition_key, secondary_key, &data, &replicas, timestamp).await?;
```

`WriteWithReplicas` and `WriteWithReplicasInconsistent` accept the record and
replica URLs in `WriteRequest`. Both preserve the supplied timestamp. The first
waits for local log acceptance and all supplied neighbors; the second waits for
local log acceptance and neighbor queue admission. Replica URLs must exclude the
receiving node; the inconsistent call requires at least one. Neighbors receive
local-only `WriteRecord` RPCs, without forwarding writes again.
A failed request can leave some copies saved.

`GetRecord` returns the record including `timestamp`, or `NotFound` if absent.
Reads return the contacted node's copy; they do not reconcile multiple replicas.

`DeleteRecord` requires a timestamp and queues a tombstone (`deleted: true`,
empty data bytes), even for a key not yet present. An older or equal timestamp cannot
resurrect it. A strictly newer write can. Delete RPC counts currently return zero
because mutations are queued before SQLite applies them.

`Client::delete(..., timestamp)`, `delete_with_replicas(..., replicas, timestamp)`,
and `delete_with_replicas_inconsistent(..., replicas, timestamp)` use the same
acknowledgement rules as writes. Replicated tombstones travel through write RPCs
with `deleted: true`. The flag defaults to false for normal writes and is preserved
by the asynchronous queue.

Normal record, partition-key and range reads, counts, and payload sizes exclude
tombstones. Set `include_deleted: true` on record, partition-key, or range requests
for reconciliation. The shared client provides `get_including_deleted` and
`get_range_including_deleted`; copy workflows use the latter and preserve both
`timestamp` and `deleted` through `write_record`.

Tombstones have no automatic expiry. Bulk `DeleteTable`, `DeletePartitionKey`, and `DeleteRange`
RPCs remain physical cleanup operations, including tombstones, for retiring
local data. They are not replicated logical deletes or range tombstones; use
record deletes for logical removal. Placement and multi-replica read workflows
remain separate work.

Existing test databases must be recreated for the `timestamp` and `deleted`
columns. There is no schema migration.

`GetPartitionKey` lists one partition key. `GetRange`, `CountRange`, and
`GetRangeSize` use `[lower_bound, upper_bound)` and require the lower bound to be
less than the upper bound. `GetSize` reports the main SQLite file's `size_bytes`,
excluding WAL and SHM files. `GetWriteBacklog` reports `queued`, `in_flight`, and
`total` mutations; it does not create access samples.

`GetTelemetry` returns entries grouped by `table_id` and `partition_key`,
with `reads_per_second`, `writes_per_second`, `average_write_position`,
`median_write_position`, and `window_seconds`. Positions refer to secondary keys;
the median uses the upper middle observed key when the write count is even.
Positions are null when the window has no writes.
`contributing_nodes` is 1 for local statistics and is added during merges. Divide
summed rates by this count to get the average per reporting node. Merge each node
once per partition key; idle nodes without telemetry are not counted.
`read_count` counts sampled reads. `write_count` and `write_position_sum` carry the sampled write count and exact key
sum (a decimal string in JSON, `i128` in Rust). `AccessStatistics::merge(&other)` adds rates and combines these totals to
calculate the average. Combining two nonempty write samples clears the median,
since individual medians cannot determine the combined median. The merged
`window_seconds` is the shortest contributing window; rates remain sums of the
individual nodes' rates, measured over their own windows.
`client.get_telemetry(url)` returns these entries as `Vec<Telemetry>` from
`patchwork_common::dataplane_client`.
`GetTelemetryForTable` returns only that table's entries. `client.get_telemetry_for_table(url, table_id)` returns
`HashMap<i64, Telemetry>`; no recent accesses returns an empty map.

`AccessStatsActor` owns the host's in-memory `TelemetryCache` and its separate
SQLite database. Read workers and the write actor send it record accesses. Point
reads include misses; partition-key and range reads count each returned record.
Writes include tombstones, replicas, and acknowledged stale writes or retries.
Count, size, and telemetry requests do not count as record accesses. Physical
cleanup deletes preserve access history.

The cache retains at most 4096 accesses per `(table_id, partition_key)` between
flushes. A full buffer keeps the latest accesses and shortens only that key's
observation window. Counts and positions describe this bounded sample, rather
than every access since startup. Each timer tick appends samples and deletes rows
older than the retention period in one transaction, then clears the cache. A
failed flush leaves the cache available for retry. Unflushed samples are lost if
the process crashes.

The telemetry RPCs return current unflushed samples, so they can be empty
immediately after a flush or restart. Stored history survives restart and is
available through `AccessStatsActor`'s `GetHistory` message, filtered by table,
partition key, and an overlapping time range. Stored sums use decimal text to
preserve `i128` precision. To share statistics across multiple shards in one
process, pass the same actor to `initialize::shard_with_access_stats`.

Shards use auto_vacuum=FULL to reclaim completely free pages on commit.
Existing shards with auto-vacuum disabled are rebuilt once during startup; this
can take time and temporary disk space. Later opens skip the rebuild. In WAL
mode the main file's size may only reflect the reclamation after checkpointing.
This does not compact partially filled pages like a full VACUUM.

The local dashboard, startup checks, and smoke test use `patchwork-data`, a small
command-line wrapper around the same Rust gRPC client. It accepts a JSON object
on stdin and reports the gRPC status and decoded body on stdout:

```sh
echo '{"url":"http://127.0.0.1:3000","operation":"GetSize"}' | cargo run -p patchwork-data-plane --bin patchwork-data
```
