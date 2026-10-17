# Local testing rig

From the repository root:

```sh
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r scripts/requirements.txt
python scripts/local_cluster.py --control-nodes 2 --data-nodes 4 --open-browser
```

Requires Python 3.9+, Cargo, and MySQL 8+ or MariaDB on `PATH` (`mysql` and
`mysqld`, plus `mariadb-install-db` when using MariaDB). The dashboard uses Flask;
Docker is not needed. The launcher builds optimized Rust binaries with `cargo build --release` once and runs each
node as its own process. Use `--no-web` for the original command-line rig without
Flask.

Every run creates a fresh database and one SQLite shard per data node in a new
`/tmp/patchwork-local-*` directory. The database is a real, isolated MySQL-compatible
server so the lease locks and transactions run normally. Metadata tables are
created from the existing SeaORM models, and all data nodes are registered before
the control nodes start. Your existing database and `.env` are not used.

Default addresses:

| Service | Address |
| --- | --- |
| Control nodes | `http://127.0.0.1:3100`, `:3101`, ... |
| Data nodes | `http://127.0.0.1:3200`, `:3201`, ... |
| Metadata database | `mysql://patchwork:patchwork@127.0.0.1:13306/patchwork` |
| Flask dashboard | `http://127.0.0.1:3300` |

Use `--control-port`, `--data-port`, and `--mysql-port` to change the ports.
Use `--web-port` to change the dashboard port and `--open-browser` to open it.
Use `--no-build` to reuse the existing release build in `rust/target/release`.
The first release build takes longer; debug binaries are never used by the rig. All servers bind to localhost.
The test database's root account has no password; the application uses the
local `patchwork` account shown above.

Ctrl-C stops the nodes and then the database. Startup failures and failed smoke
tests also stop the cluster. Logs, databases, and `cluster.json` (addresses and
process IDs) remain in the printed run directory for inspection. Remove that
directory when you're finished. Each new invocation starts fresh.

## Browser dashboard

Open `http://127.0.0.1:3300` while the rig is running. Pick a control node and send
**Create a table**, then choose **Write record · consistent** or
**Write record · inconsistent** on a control node. The created table is
selected automatically. The page shows the node's HTTP or gRPC status and response body,
and refreshes tables, partition placement, and lease locks every five seconds.

Control node writes use the existing Rust write workflows to choose replicas.
Consistent writes wait for every replica; inconsistent writes wait for two nodes
and queue the others in memory. You can also pick a data node to work directly
with its copy over gRPC. The dashboard uses the `patchwork-data` Rust helper,
built alongside the servers; no Python gRPC package is required. Data node writes and deletes use the table's actual replicas by default, including
forwarding destinations. If the selected data node is outside that set, the
request goes to a replica instead; the response shows the actual URL and replica
list. Uncheck **Write to this table’s replicas** to touch only the selected node.
Record deletion writes tombstones; table deletion goes through the control plane.

Leave the timestamp blank to generate one for each new write, or enter Unix
microseconds to try stale writes. **Repeat last request** keeps the original
body and timestamp. Reads, range counts/sizes, and telemetry are available from
the action menu. Errors from nodes appear in the response panel.

The dashboard forwards requests only to this run's nodes. Stopping the launcher
also stops Flask. The Flask debugger and auto-reloader are not enabled.

## TPS load generator

Use the dashboard's **Load generator** panel with a ready table. Set target HTTP
requests per second, duration, concurrent requests, read percentage, unique
records, payload padding, and consistency. Send traffic through one control node
or round-robin across all control nodes. Spread keys across the signed 64-bit
ring to exercise all partitions, or target one hot partition key.

Read and mixed workloads seed their records first by default; seeding is outside
the timed measurements. Each run uses its own record keys. Generated records
remain for inspection. Disable seeding to measure read misses.

The panel shows achieved and average TPS, completed requests, errors, read
misses, in-flight requests, and p95 latency from the most recent 10,000 requests.
TPS counts completed control-node HTTP requests, including misses and errors;
it does not count the extra replica writes. The target is a ceiling: concurrency,
the Python generator, or the cluster can limit achieved throughput. Missed slots
are discarded instead of causing catch-up bursts. There are no automatic retries.

**Stop load** stops scheduling requests and drains those already in flight.
Closing the browser does not stop the run. Ctrl-C in the launcher stops load
before stopping the cluster. Results are available as a JSON download and saved
as `load-<run-id>.json` in the run directory. This adds load generation only;
automatic split/merge/replication scheduling remains unfinished.

## Telemetry and operation timings

**Data-node telemetry** shows the 100 busiest partition keys per node, read/write
rates and sample counts, observation window, and average/median write positions.
It calls `GetTelemetry` over gRPC every five seconds; disable the
refresh checkbox when measuring peak throughput. These are each node’s bounded
samples, including replica traffic, so they are different from control-node TPS.

Enable **Measure fetch / put timings** in a load run or the request form to collect
control-node timings. Load seeding is excluded. Measurement is off by default.
The **Fetch / put timings** table separates consistent/inconsistent GET and PUT,
plus range fetches, for each control node. It shows average milliseconds per
measured request for table lookup, partition lookup, forwarding/replica resolution,
node URL lookup, replica HTTP, and total time. Multiple replica calls are added
within a request. **Reset timings** clears the control-node totals before a new
comparison; requests already in flight can still finish into the new totals.

Direct API requests opt in with `?telemetry=true` (or `&telemetry=true` after other
query options). Responses include a `Server-Timing` header; `GET /telemetry` on
each control node returns call counts, total milliseconds, and average per call,
and `DELETE /telemetry` resets them. Averages include failed measured requests;
HTTP 4xx/5xx responses count as errors. These timings include actor queue waits.
Replica HTTP covers the network and data-node work together, including the
replication acknowledged by a write; it does not separately measure SQLite work
or background replication queued by inconsistent writes.

Table lookups in data workflows and `GetTablePartitions` have a five-second TTL,
bounded to 1,024 entries per cache. Inconsistent point reads/writes share the
cached partition ring. Consistent point operations still use live partition
lookups. Metadata writes on the same actor invalidate its caches immediately;
other control nodes can retain old metadata for up to five seconds. Topology
changes and table deletion explicitly read fresh partition metadata.

## Smoke test

```sh
python3 scripts/local_cluster.py --control-nodes 2 --data-nodes 4 --smoke-test
```

This creates a table through one control node, finds its actual replicas in
MySQL, writes through the data plane's `WriteWithReplicas` RPC, checks each replica,
and reads the record and range through every control node. It then checks that
a newer tombstone wins a consistent read, checks that a held partition lease
blocks deletion and releases partially acquired locks, expires that lease,
and retries deletion through the last control node. It checks data, metadata,
and lock cleanup. The rig stops and returns a nonzero exit code if a check fails.

Table creation needs at least three data nodes. Smaller clusters can still be
started to exercise failure cases. Multiple control nodes share metadata and
leases; the rig doesn't add control node election or ownership behavior.

## Try it by hand

With the rig running in another terminal:

```sh
curl -sS http://127.0.0.1:3100/tables \
  -H 'Content-Type: application/json' \
  -d '{"table_name":"accounts","table_owner":1,"partition_key_name":"account","sort_key_name":"id"}'

mysql --no-defaults --protocol=TCP -h 127.0.0.1 -P 13306 \
  -u patchwork -ppatchwork patchwork \
  -e 'SELECT * FROM node; SELECT * FROM `partition`; SELECT * FROM partition_lock;'
```

The [control plane README](../rust/patchwork-control-plane/README.md) describes
table creation/deletion and record reads/writes. For direct data node requests,
see the [data plane README](../rust/patchwork-data-plane/README.md). The rig runs the
existing background scale checks, whose split/merge scheduling is still unfinished.

Control nodes now require the internal bearer token. Set
`ATGS_INTERNAL_SERVICE_TOKEN` in `rust/.env` or the launcher's environment. The
launcher passes it to the control nodes, dashboard requests, and load worker.
For remote load generation, configure the same token on the second PC. Tokens
are not included in browser metadata or downloaded load reports.
