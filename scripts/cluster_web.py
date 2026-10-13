"""Local Flask dashboard for the cluster owned by local_cluster.py."""

from concurrent.futures import ThreadPoolExecutor
import json
import re
import time

from flask import Flask, jsonify, request

from load_generator import LoadGenerator


RECORD_PATH = re.compile(r"/tables/(-?\d+)/partition-keys/(-?\d+)/records/(-?\d+)")


def create_app(cluster, send_request):
    app = Flask(__name__, static_folder="static")
    app.config.update(MAX_CONTENT_LENGTH=2 * 1024 * 1024, TRUSTED_HOSTS=["127.0.0.1", "localhost"])
    node_urls = {
        **{f"control-{i + 1}": url for i, url in enumerate(cluster.control_urls)},
        **{f"data-{i + 1}": url for i, url in enumerate(cluster.data_urls)},
    }
    load = LoadGenerator(send_request, {name: url for name, url in node_urls.items() if name.startswith("control-")}, cluster.directory)
    app.extensions["load_generator"] = load

    def rows(query):
        return [json.loads(line) for line in cluster.sql(query).splitlines()]

    def partitions():
        result = rows("""
            SELECT JSON_OBJECT('table_id', CAST(table_id AS CHAR),
                'hash_start', CAST(hash_start AS CHAR), 'forward_to', CAST(forward_to AS CHAR),
                'replicas', replicas) FROM `partition` ORDER BY table_id, hash_start
        """)
        for row in result:
            # MariaDB's JSON columns are text; MySQL returns a JSON array here.
            if isinstance(row["replicas"], str):
                row["replicas"] = json.loads(row["replicas"])
        return result

    def table_replicas(table_id, partition_key):
        ring = [p for p in partitions() if p["table_id"] == str(table_id)]
        if not ring:
            raise ValueError("This table has no partitions")
        partition = next((p for p in reversed(ring) if int(p["hash_start"]) <= partition_key), ring[-1])
        replicas, visited = set(), set()
        while True:
            boundary = partition["hash_start"]
            if boundary in visited:
                raise ValueError("Partition forwarding loop")
            if not partition["replicas"]:
                raise ValueError("A partition has no replicas")
            visited.add(boundary)
            replicas.update(partition["replicas"])
            if partition["forward_to"] is None:
                break
            partition = next((p for p in ring if p["hash_start"] == partition["forward_to"]), None)
            if partition is None:
                raise ValueError("Forwarded partition is missing")
        if not replicas:
            raise ValueError("This partition has no replicas")
        # Read registration rather than assuming node IDs correspond to ports.
        nodes = rows("SELECT JSON_OBJECT('node_id', node_id, 'url', url) FROM node")
        urls = {node["node_id"]: node["url"] for node in nodes}
        result = [urls[node_id] for node_id in sorted(replicas)]
        if any(url not in cluster.data_urls for url in result):
            raise ValueError("A replica points outside this local cluster")
        return result

    @app.before_request
    def local_requests_only():
        if request.method == "POST" and request.headers.get("Origin") not in (None, request.host_url.rstrip("/")):
            return jsonify(error="Use the dashboard from its localhost address"), 403

    @app.get("/")
    def index():
        return app.send_static_file("cluster.html")

    @app.get("/api/cluster")
    def metadata():
        try:
            processes = dict(cluster.processes)
            return jsonify(
                nodes=[{
                    "name": name, "url": url, "kind": name.split("-")[0],
                    "pid": processes[name].pid, "running": processes[name].poll() is None,
                } for name, url in node_urls.items()],
                tables=rows("""
                    SELECT JSON_OBJECT('table_id', CAST(table_id AS CHAR), 'table_name', table_name,
                        'is_ready', is_ready) FROM `table` ORDER BY table_id
                """),
                partitions=partitions(),
                locks=rows("""
                    SELECT JSON_OBJECT('table_id', CAST(table_id AS CHAR),
                        'hash_start', CAST(hash_start AS CHAR), 'lease_end', lease_end,
                        'lock_token', lock_token) FROM partition_lock ORDER BY table_id, hash_start
                """),
            )
        except (RuntimeError, OSError) as error:
            return jsonify(error=str(error)), 502

    @app.post("/api/request")
    def proxy():
        values = request.get_json()
        try:
            if not isinstance(values, dict):
                raise ValueError("Send a JSON request object")
            node = values["node"]
            url = node_urls[node]
            method, path = values["method"], values["path"]
            if method not in ("GET", "POST", "PUT", "DELETE") or not path.startswith(("/tables", "/shard/", "/telemetry")):
                raise ValueError("Choose a supported method and a /tables, /shard/, or /telemetry path")
            raw_body = values.get("body", "")
            body = json.loads(raw_body) if raw_body.strip() else None
            headers, replicas = {}, []
            timestamp = values.get("timestamp", "")
            record = RECORD_PATH.fullmatch(path.split("?")[0].removesuffix("/with-replicas-inconsistent").removesuffix("/with-replicas"))
            if method in ("PUT", "DELETE") and record:
                timestamp = str(timestamp or time.time_ns() // 1000)
                if not 0 <= int(timestamp) <= (1 << 63) - 1:
                    raise ValueError("Timestamp must be nonnegative Unix microseconds within a signed 64-bit integer")
                headers["x-patchwork-timestamp"] = timestamp
            if values.get("replicate"):
                if not node.startswith("data-") or method not in ("PUT", "DELETE") or not RECORD_PATH.fullmatch(path):
                    raise ValueError("Replica writes need a data node and a plain record path")
                table_id, partition_key, _ = map(int, record.groups())
                replicas = table_replicas(table_id, partition_key)
                if url not in replicas:
                    url = replicas[0]
                # Replicated deletion is the data plane's timestamped tombstone PUT.
                if method == "DELETE":
                    headers["x-patchwork-deleted"] = "true"
                    body = None
                body = {"data": body, "replicas": [replica for replica in replicas if replica != url]}
                method, path = "PUT", path + "/with-replicas"
            if values.get("telemetry") and node.startswith("control-") and method in ("GET", "PUT"):
                path += ("&" if "?" in path else "?") + "telemetry=true"
            started = time.monotonic()
            result = send_request(url + path, method, body, headers, expected=None)
            try:
                result["body"] = json.dumps(json.loads(result["body"]), indent=2, ensure_ascii=False)
            except ValueError:
                pass
            return jsonify(**result, url=url + path, method=method, timestamp=timestamp,
                           replicas=replicas, elapsed_ms=round((time.monotonic() - started) * 1000))
        except (KeyError, ValueError, TypeError, AttributeError) as error:
            return jsonify(error=str(error)), 400
        except (RuntimeError, OSError) as error:
            return jsonify(error=str(error)), 502

    def node_telemetry(item):
        name, url = item
        try:
            data = send_request(url + ("/telemetry" if name.startswith("control-") else "/shard/telemetry"))
            if name.startswith("data-"):
                # Keep signed 64-bit keys exact when the browser parses JSON.
                for row in data:
                    for key in ("table_id", "partition_key", "average_write_position", "median_write_position"):
                        if row.get(key) is not None:
                            row[key] = str(row[key])
            return {"name": name, "kind": name.split("-")[0], "telemetry": data}
        except (RuntimeError, OSError, ValueError) as error:
            return {"name": name, "kind": name.split("-")[0], "error": str(error)}

    @app.get("/api/telemetry")
    def telemetry():
        with ThreadPoolExecutor(max_workers=min(8, len(node_urls))) as pool:
            return jsonify(nodes=list(pool.map(node_telemetry, node_urls.items())))

    @app.post("/api/telemetry/reset")
    def reset_telemetry():
        errors = []
        for name, url in node_urls.items():
            if name.startswith("control-"):
                try:
                    send_request(url + "/telemetry", "DELETE", expected=204)
                except (RuntimeError, OSError) as error:
                    errors.append(f"{name}: {error}")
        return jsonify(errors=errors), 502 if errors else 200

    @app.get("/api/load")
    def load_status():
        return jsonify(load.snapshot())

    @app.post("/api/load")
    def start_load():
        values = request.get_json()
        try:
            if not isinstance(values, dict):
                raise ValueError("Send a JSON load configuration")
            table_id = int(str(values.get("table_id", "")))
            ready = cluster.sql(f"SELECT is_ready FROM `table` WHERE table_id = {table_id}")
            if ready != "1":
                raise ValueError("Select a ready table before starting load")
            load.start(values)
            return jsonify(load.snapshot()), 202
        except (ValueError, TypeError) as error:
            return jsonify(error=str(error)), 400
        except RuntimeError as error:
            return jsonify(error=str(error)), 409

    @app.post("/api/load/stop")
    def stop_load():
        load.stop()
        return jsonify(load.snapshot())

    return app
