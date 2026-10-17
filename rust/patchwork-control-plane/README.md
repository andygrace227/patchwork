# Control plane

Run from the Rust workspace directory with a prepared MySQL metadata database:

```sh
DATABASE_URL=mysql://user:password@127.0.0.1/patchwork BIND_ADDR=127.0.0.1:3001 \
  cargo run -p patchwork-control-plane
```

Every API request requires `Authorization: Bearer <token>`. Set
`ATGS_INTERNAL_SERVICE_TOKEN` in `.env` or the environment before starting the
control plane. That token identifies `AtgsInternalService` and skips the permission
service lookup. It is a trusted bootstrap credential for the internal services.

Customer and service JWTs can be verified with an RSA public key. Configure
`JWT_PUBLIC_KEY_PATH`, `JWT_ISSUER`, and `JWT_AUDIENCE` together; only RS256 is
accepted. The control plane checks the signature, expiration, optional not-before
time, issuer, audience, and a nonempty `sub`. `account_id` is optional, and
`principal_type` supports `User` (the default), `Service`, `Container`, and
`Function`. JWTs cannot claim the `AtgsInternalService` bootstrap exception.
Token issuance belongs to the future auth service; no login endpoint is added here.

The permission client is currently a stub: verified JWT callers receive `503`
until it is connected to the permission service. Missing or invalid credentials
receive `401`. The internal token works without that service or JWT configuration.
The dashboard and load generator send the internal token from `rust/.env` or the
environment; set the same token on a remote load PC.

Tables have an optional `secret_key_id`. Omit it or send `null` for unencrypted
JSON bytes, or supply a positive key ID for encryption. The control plane calls
the encryption client before writes and after reads, including range reads.
The encryption client is also a stub: tables with a key ID return `503` when
an encryption/decryption call is needed. The internal token does not override
this setting. The data plane stores raw bytes and does not handle keys.

Existing MySQL metadata databases need this migration before running the new
control plane (fresh databases made by the local rig include the column):

```sql
ALTER TABLE `table` ADD COLUMN secret_key_id BIGINT NULL;
```

The same statement is in `migrations/20261015_add_secret_key_id.sql`.

Startup loads `.env`; exported variables take precedence. `DATABASE_URL` is
required. `BIND_ADDR` defaults to `127.0.0.1:3001`; set it explicitly if your
shared `.env` contains the data plane's address. Startup expects the existing
`node`, `table`, `partition`, and `partition_lock` tables. It does not create or
migrate metadata. Register at least three distinct data nodes before creating a table.

The server starts the existing workflow executor, including its random background
scale checks. Table changes go through that executor; HTTP responses wait for
completion. Ctrl-C drains HTTP requests before stopping the executor.

The control server, data server, and load generator use Tokio's multithread runtime.
The default is one worker thread per available CPU; each executable logs its actual
worker count. To override it, export `TOKIO_WORKER_THREADS` before starting the
process (for example, `TOKIO_WORKER_THREADS=4 cargo run -p patchwork-control-plane`).
Putting it in `.env` is too late: the runtime starts before `.env` is loaded.
The load generator's `workers` setting limits in-flight requests, not OS threads.
Independent requests can use different threads. Each data node keeps one write
connection in its shard actor and uses Kameo's actor pool for read connections.

- `POST /tables` invokes `MakeTable` and returns `201` with the ready table.
  Send `{"table_name":"accounts","table_owner":1,"partition_key_name":"account","sort_key_name":"id","secret_key_id":null}`.
- `DELETE /tables/{table_id}` invokes `DeleteTable` and returns `204` after cleanup.
  An already absent table also returns `204`.
- `GET /tables/{table_id}/partition-keys/{partition_key}/records/{secondary_key}`
  invokes `GetOneConsistent` by default. Add `?consistent=false` to invoke
  `GetOneInconsistent`. Returns the full record or `404` when absent or deleted.
- `PUT` on the same record path invokes `PutOneConsistent` by default; add
  `?consistent=false` to invoke `PutOneInconsistent`. Send the record's JSON payload
  directly and supply `x-patchwork-timestamp` as nonnegative Unix microseconds.
  Returns `204`; missing, invalid, negative, or overflowing timestamps return `400`.
  Reuse the original timestamp for retries. Consistent writes wait for every replica;
  inconsistent writes wait for two nodes and queue the rest in memory without retries.
- `GET /tables/{table_id}/range?lower_bound=0&upper_bound=100` invokes `FetchRange`
  and returns an array for `[0, 100)`. Equal bounds return an empty array;
  reversed bounds return `400`. This workflow reads one replica per partition.

Consistent reads return the newest observed timestamp across replicas. Range
reads do not provide a consistent snapshot. Blank table/key names return `400`.
Duplicate queued/running table operations return `409`; an unavailable or full
executor returns `503`. Workflow failures return `500` and log details on the server.

### Optional operation timings

Add `telemetry=true` to record GET/PUT or range query options to collect timings.
Responses include `Server-Timing` for table/partition lookup, replica resolution,
replica URL lookup, replica HTTP, and total time. Measurement is off by default.
`GET /telemetry` returns cumulative counts and average milliseconds per stage call,
separated by operation and consistency; `DELETE /telemetry` resets the totals.
Replica HTTP includes acknowledged data-node/replication work and network time,
not asynchronous background replication. The local dashboard shows stage time
per measured request and the data nodes’ existing access telemetry.

Data-node calls use gRPC exclusively through `patchwork-common::dataplane_client`,
including reads, writes, partition maintenance, and telemetry. Existing node URLs
and ports still apply. Update and restart every data node before restarting the
control plane. The public control-plane API remains HTTP/JSON. Server timing for
data-node calls is reported as `replica_grpc`.
