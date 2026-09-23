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

| Operation | Request |
|---|---|
| Upsert | PUT /records |
| Get record | GET /tables/{table_id}/partitions/{partition}/records/{hash} |
| Delete record | DELETE /tables/{table_id}/partitions/{partition}/records/{hash} |
| Get partition | GET /tables/{table_id}/partitions/{partition} |
| Delete partition | DELETE /tables/{table_id}/partitions/{partition} |
| Get range | GET /tables/{table_id}/range?lower_bound=0&upper_bound=100 |

Upsert accepts the three keys and JSON data (no version):

```sh
curl -i -X PUT http://127.0.0.1:3000/records \
  -H 'Content-Type: application/json' \
  -d '{"table_id":1,"partition":10,"hash":42,"data":{"name":"example"}}'
```

New records start at version 1. Upserting the same (table_id, partition, hash)
replaces data and atomically increments the stored version. Client-supplied
versions are rejected. Deleting and recreating a record starts again at 1. Hashes come from
the client. All keys and versions are signed 64-bit integers; clients must preserve
integer precision when encoding JSON.

Upsert returns 204. Missing records return 404. Collection reads return arrays.
Deletes return {"deleted": N}, including zero when nothing matches.
Ranges exclude both bounds and reject lower_bound >= upper_bound with 400.
Reads and deletes are scoped to the requested table. Database failures return 500
with details logged on the server. This API operates on its single configured shard;
routing to other shards belongs to the caller.

GET /shard/size returns {"size_bytes": N}: the main SQLite file's current length
in bytes, excluding the -wal and -shm files. Recent writes may still be in the WAL,
so this is not the total disk usage. The control-plane client exposes
Client::get_size(url).

Shards use auto_vacuum=FULL to reclaim completely free pages on commit.
Existing shards with auto-vacuum disabled are rebuilt once during startup; this
can take time and temporary disk space. Later opens skip the rebuild. In WAL
mode the main file's size may only reflect the reclamation after checkpointing.
This does not compact partially filled pages like a full VACUUM.
