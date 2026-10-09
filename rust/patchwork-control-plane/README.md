# Control plane

Run from the Rust workspace directory with a prepared MySQL metadata database:

```sh
DATABASE_URL=mysql://user:password@127.0.0.1/patchwork BIND_ADDR=127.0.0.1:3001 \
  cargo run -p patchwork-control-plane
```

Startup loads `.env`; exported variables take precedence. `DATABASE_URL` is
required. `BIND_ADDR` defaults to `127.0.0.1:3001`; set it explicitly if your
shared `.env` contains the data plane's address. Startup expects the existing
`node`, `table`, `partition`, and `partition_lock` tables. It does not create or
migrate metadata. Register at least three distinct data nodes before creating a table.

The server starts the existing workflow executor, including its random background
scale checks. Table changes go through that executor; HTTP responses wait for
completion. Ctrl-C drains HTTP requests before stopping the executor.

- `POST /tables` invokes `MakeTable` and returns `201` with the ready table.
  Send `{"table_name":"accounts","table_owner":1,"partition_key_name":"account","sort_key_name":"id"}`.
- `DELETE /tables/{table_id}` invokes `DeleteTable` and returns `204` after cleanup.
  An already absent table also returns `204`.
- `GET /tables/{table_id}/partition-keys/{partition_key}/records/{secondary_key}`
  invokes `GetOneConsistent` by default. Add `?consistent=false` to invoke
  `GetOneInconsistent`. Returns the full record or `404` when absent or deleted.
- `GET /tables/{table_id}/range?lower_bound=0&upper_bound=100` invokes `FetchRange`
  and returns an array for `[0, 100)`. Equal bounds return an empty array;
  reversed bounds return `400`. This workflow reads one replica per partition.

Consistent reads return the newest observed timestamp across replicas. Range
reads do not provide a consistent snapshot. Blank table/key names return `400`.
Duplicate queued/running table operations return `409`; an unavailable or full
executor returns `503`. Workflow failures return `500` and log details on the server.
