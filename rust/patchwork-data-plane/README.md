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

Startup opens one SQLite shard through SeaORM, enables WAL, creates its data table
if missing, and injects one Kameo actor into the HTTP handlers. Existing tables
are not migrated automatically. Ctrl-C drains HTTP requests before stopping the actor.

Writes use `PUT /tables/{table_id}/partition-keys/{partition_key}/records/{secondary_key}`.
The body is the JSON payload. Every write requires `x-patchwork-timestamp`: a
nonnegative signed 64-bit Unix timestamp in microseconds, chosen by the caller.
Missing or invalid timestamps return 400. The server never generates one.

```sh
curl -X PUT http://127.0.0.1:3000/tables/1/partition-keys/10/records/42 \
  -H 'Content-Type: application/json' \
  -H 'x-patchwork-timestamp: 1791158400000000' \
  -d '{"name":"example"}'
```

The highest timestamp wins for the entire JSON record. Deletion wins an equal
timestamp tie; ties between live values use the greater serialized JSON byte
sequence, so arrival order does not decide the winner. An older write is acknowledged without replacing newer
data. Reuse the original timestamp for retries and copies. Clocks are the
caller's responsibility; timestamps do not prove real-world event order.

The shared client requires a timestamp argument:

```rust,ignore
use patchwork_common::dataplane_client::Client;

Client::upsert(url, table_id, partition_key, secondary_key, &data, timestamp).await?;
Client::put_with_replicas(url, table_id, partition_key, secondary_key, &data, &replicas, timestamp).await?;
Client::put_with_replicas_inconsistent(url, table_id, partition_key, secondary_key, &data, &replicas, timestamp).await?;
```

The replica endpoints append `/with-replicas` or `/with-replicas-inconsistent`
to the record path and accept `{"data": ..., "replicas": ["http://..."]}`.
They use the same required timestamp header and pass it unchanged to every copy.
The first waits for local storage and all supplied replicas. The second waits
for local storage and the first supplied replica, then queues the others in
`SubordinateShardWriter`. Its queue is in memory, without retries. Replica URLs
must exclude the receiving node; the inconsistent call requires at least one.
A failed request can leave some copies saved.

GET on the record path returns the record including `timestamp` (replacing the
old per-node `version` counter), or 404 if absent. Reads currently return the
contacted node's copy; they do not reconcile multiple replicas.

Record DELETE requires the timestamp header and stores a tombstone (`deleted:
true`, `data: null`), even for a key not yet present. A later arrival with an
older or equal timestamp cannot resurrect it. A strictly newer write can.
The response `{"deleted": N}` counts stored tombstone changes, including markers
for absent keys; stale deletes and identical retries return zero.

`Client::delete(..., timestamp)`, `delete_with_replicas(..., replicas, timestamp)`,
and `delete_with_replicas_inconsistent(..., replicas, timestamp)` use the same
acknowledgment rules as writes. Replicated tombstones travel through the PUT
endpoints with `x-patchwork-deleted: true`. The flag defaults to false for normal
writes and is preserved by the asynchronous queue.

Normal GET, partition-key and range reads, counts, and payload sizes exclude
tombstones. Add `include_deleted=true` to record, partition-key, or range GETs
for reconciliation. The shared client provides `get_including_deleted` and
`get_range_including_deleted`; copy workflows use the latter and preserve both
`timestamp` and `deleted` through `write_record`.

Tombstones have no automatic expiry. Bulk table, partition-key, and range DELETE
endpoints remain physical cleanup operations, including tombstones, for retiring
local data. They are not replicated logical deletes or range tombstones; use
record deletes for logical removal. Placement and multi-replica read workflows
remain separate work.

Existing test databases must be recreated for the `timestamp` and `deleted`
columns. There is no schema migration.

GET `/tables/{table_id}/partition-keys/{partition_key}` lists one partition key.
GET `/tables/{table_id}/range?lower_bound=0&upper_bound=100` uses `[0, 100)`.
Range count and size endpoints append `/count` and `/size` before the query.
GET `/shard/size` returns `{"size_bytes": N}` for the main SQLite file,
excluding WAL and SHM files.

GET `/shard/telemetry` returns an array grouped by `table_id` and `partition_key`,
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
`Client::get_telemetry(url)` returns these entries as `Vec<Telemetry>` from
`patchwork_common::dataplane_client`.
GET `/tables/{table_id}/telemetry` returns only that table's entries as an object
keyed by partition key. `Client::get_telemetry_for_table(url, table_id)` returns
`HashMap<i64, Telemetry>`; no recent accesses returns an empty map.

Telemetry lives in the shard actor's memory and resets on restart. Each tracker
retains at most 1024 accesses from the last ten seconds. During startup or buffer
overflow, both rates use the shorter retained window reported in `window_seconds`.
Inactive trackers are discarded when telemetry is requested or during periodic
cleanup on access. Successful point reads count misses too; bulk reads count
returned records. Writes include tombstones, replicas, and acknowledged stale
writes or retries. Physical cleanup deletes clear affected trackers; count, size,
and telemetry requests do not count as record accesses.

Shards use auto_vacuum=FULL to reclaim completely free pages on commit.
Existing shards with auto-vacuum disabled are rebuilt once during startup; this
can take time and temporary disk space. Later opens skip the rebuild. In WAL
mode the main file's size may only reflect the reclamation after checkpointing.
This does not compact partially filled pages like a full VACUUM.
