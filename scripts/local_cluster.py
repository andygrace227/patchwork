#!/usr/bin/env python3
"""Run a fresh local Patchwork cluster. Requires Python 3, Cargo, and MySQL/MariaDB."""

import argparse
import json
import os
from pathlib import Path
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.request


ROOT = Path(__file__).resolve().parents[1]
TARGET = ROOT / "rust" / "target" / "release"
HTTP = urllib.request.build_opener(urllib.request.ProxyHandler({}))


def request(url, method="GET", body=None, headers=None, expected=200):
    headers = {"Content-Type": "application/json", **(headers or {})}
    data = None if body is None else json.dumps(body).encode()
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        response = HTTP.open(req, timeout=15)
    except urllib.error.HTTPError as error:
        response = error
    with response:
        content = response.read().decode()
        if expected is None:
            return {"status": response.code, "body": content, "server_timing": response.headers.get("Server-Timing", "")}
        if response.code != expected:
            raise RuntimeError(f"{method} {url}: expected {expected}, got {response.code}: {content}")
        return json.loads(content) if content else None


def require(name):
    path = shutil.which(name)
    if path is None:
        raise RuntimeError(f"Missing {name}; install MySQL or MariaDB and put its bin directory on PATH")
    return path


class Cluster:
    def __init__(self, args):
        self.args = args
        # A short path also keeps the Unix socket under macOS's path length limit.
        self.directory = Path(tempfile.mkdtemp(prefix="patchwork-local-", dir="/tmp"))
        self.processes = []
        self.control_urls = [f"http://127.0.0.1:{args.control_port + i}" for i in range(args.control_nodes)]
        self.data_urls = [f"http://127.0.0.1:{args.data_port + i}" for i in range(args.data_nodes)]
        self.database_url = f"mysql://patchwork:patchwork@127.0.0.1:{args.mysql_port}/patchwork"
        self.mysql = require("mysql")
        self.mysqld = require("mysqld")

    def start_process(self, name, command, env=None):
        with (self.directory / f"{name}.log").open("w") as log:
            process = subprocess.Popen(
                command, cwd=self.directory, env={**os.environ, **(env or {})},
                stdout=log, stderr=subprocess.STDOUT, start_new_session=True,
            )
        self.processes.append((name, process))
        return process

    def check_processes(self):
        for name, process in self.processes:
            if process.poll() is not None:
                raise RuntimeError(f"{name} exited ({process.returncode}); see {self.directory / (name + '.log')}")

    def wait_ready(self, check, description):
        deadline = time.monotonic() + 60
        while time.monotonic() < deadline:
            self.check_processes()
            try:
                check()
                return
            except (OSError, RuntimeError, subprocess.SubprocessError):
                time.sleep(0.2)
        raise RuntimeError(f"Timed out waiting for {description}; logs: {self.directory}")

    def sql(self, statement, database=True):
        command = [
            self.mysql, "--no-defaults", "--protocol=SOCKET",
            f"--socket={self.directory / 'mysql.sock'}", "--user=root", "--password=",
            "--batch", "--raw", "--skip-column-names",
        ]
        if database:
            command.append("--database=patchwork")
        result = subprocess.run(command, input=statement, text=True, capture_output=True, timeout=15)
        if result.returncode:
            raise RuntimeError(result.stderr.strip())
        return result.stdout.strip()

    def start(self):
        ports = [self.args.mysql_port]
        if not self.args.no_web:
            ports.append(self.args.web_port)
        ports.extend(self.args.control_port + i for i in range(self.args.control_nodes))
        ports.extend(self.args.data_port + i for i in range(self.args.data_nodes))
        if len(set(ports)) != len(ports) or any(not 1 <= port <= 65535 for port in ports):
            raise RuntimeError("Choose distinct ports between 1 and 65535")
        for port in ports:
            with socket.socket() as probe:
                probe.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                probe.bind(("127.0.0.1", port))

        print(f"Run directory: {self.directory}", flush=True)
        if not self.args.no_build:
            subprocess.run([
                "cargo", "build", "--release", "--manifest-path", str(ROOT / "rust" / "Cargo.toml"),
                "--target-dir", str(TARGET.parent), "--workspace", "--bins",
                "--example", "init_local_cluster",
            ], cwd=ROOT, check=True)

        print("Preparing an isolated database...", flush=True)
        datadir = self.directory / "mysql"
        version = subprocess.check_output([self.mysqld, "--version"], text=True)
        mariadb = "mariadb" in version.lower()
        if mariadb:
            initialize = [
                require("mariadb-install-db"), "--no-defaults", f"--datadir={datadir}",
                "--auth-root-authentication-method=normal", "--skip-test-db",
            ]
        else:
            initialize = [self.mysqld, "--no-defaults", "--initialize-insecure", f"--datadir={datadir}"]
        with (self.directory / "mysql-init.log").open("w") as log:
            subprocess.run(initialize, cwd=self.directory, stdout=log, stderr=subprocess.STDOUT, check=True)
        command = [
            self.mysqld, "--no-defaults", f"--datadir={datadir}",
            f"--socket={self.directory / 'mysql.sock'}", f"--pid-file={self.directory / 'mysql.pid'}",
            "--bind-address=127.0.0.1", f"--port={self.args.mysql_port}", "--skip-name-resolve",
        ]
        if not mariadb:
            command.append("--mysqlx=0")
        self.start_process("mysql", command)
        self.wait_ready(lambda: self.sql("SELECT 1", database=False), "MySQL")
        self.sql("""
            CREATE DATABASE patchwork;
            CREATE USER 'patchwork'@'127.0.0.1' IDENTIFIED BY 'patchwork';
            GRANT ALL ON patchwork.* TO 'patchwork'@'127.0.0.1';
        """, database=False)
        # Reuse the SeaORM models rather than maintaining a second SQL schema.
        with (self.directory / "metadata-init.log").open("w") as log:
            subprocess.run(
                [str(TARGET / "examples" / "init_local_cluster")], cwd=self.directory,
                env={**os.environ, "DATABASE_URL": self.database_url, "DATA_NODE_URLS": json.dumps(self.data_urls)},
                stdout=log, stderr=subprocess.STDOUT, check=True,
            )
        for i, url in enumerate(self.data_urls):
            self.start_process(f"data-{i + 1}", [str(TARGET / "patchwork-data-plane")], {
                "BIND_ADDR": url.removeprefix("http://"),
                "SHARD_PATH": str(self.directory / f"data-{i + 1}.sqlite"),
            })
        for url in self.data_urls:
            self.wait_ready(lambda: request(url + "/shard/size"), url)
        for i, url in enumerate(self.control_urls):
            self.start_process(f"control-{i + 1}", [str(TARGET / "patchwork-control-plane")], {
                "BIND_ADDR": url.removeprefix("http://"), "DATABASE_URL": self.database_url,
            })
        for url in self.control_urls:
            self.wait_ready(lambda: request(url + "/", expected=404), url)
        manifest = {
            "control_urls": self.control_urls, "data_urls": self.data_urls,
            "database_url": self.database_url,
            "dashboard_url": None if self.args.no_web else f"http://127.0.0.1:{self.args.web_port}",
            "processes": {name: process.pid for name, process in self.processes},
        }
        (self.directory / "cluster.json").write_text(json.dumps(manifest, indent=2) + "\n")
        print(f"Control nodes: {', '.join(self.control_urls)}\nData nodes: {', '.join(self.data_urls)}", flush=True)
        print(f"DATABASE_URL={self.database_url}\nLogs and cluster.json: {self.directory}", flush=True)

    def smoke_test(self):
        print("Testing table creation, replicas, control reads, tombstones, and cleanup...", flush=True)
        table = request(self.control_urls[0] + "/tables", "POST", {
            "table_name": "local_smoke", "table_owner": 1,
            "partition_key_name": "account", "sort_key_name": "id",
        }, expected=201)
        table_id = table["table_id"]
        if not table["is_ready"]:
            raise RuntimeError("Created table is not ready")
        replica_ids = json.loads(self.sql(
            f"SELECT replicas FROM `partition` WHERE table_id = {table_id} "
            "AND hash_start <= 10 ORDER BY hash_start DESC LIMIT 1"
        ))
        replicas = [self.data_urls[node_id - 1] for node_id in replica_ids]
        record_path = f"/tables/{table_id}/partition-keys/10/records/42"
        timestamp = time.time_ns() // 1000
        headers = {"x-patchwork-timestamp": str(timestamp)}
        payload = {"name": "local test"}
        request(replicas[0] + record_path + "/with-replicas", "PUT", {
            "data": payload, "replicas": replicas[1:],
        }, headers, expected=204)
        for url in replicas + self.control_urls:
            record = request(url + record_path)
            if record["data"] != payload or record["timestamp"] != timestamp:
                raise RuntimeError(f"Wrong record from {url}: {record}")
        for url in self.control_urls:
            records = request(url + f"/tables/{table_id}/range?lower_bound=0&upper_bound=100")
            if len(records) != 1 or records[0]["data"] != payload:
                raise RuntimeError(f"Wrong range from {url}: {records}")
        # A newer tombstone on one replica must beat older live copies.
        request(replicas[0] + record_path, "DELETE", headers={
            "x-patchwork-timestamp": str(timestamp + 1),
        })
        for url in self.control_urls:
            request(url + record_path, expected=404)

        # Block the second boundary so failed deletion must release its first lock.
        boundary = int(self.sql(
            f"SELECT hash_start FROM `partition` WHERE table_id = {table_id} "
            "ORDER BY hash_start LIMIT 1 OFFSET 1"
        ))
        self.sql(
            "INSERT INTO partition_lock (table_id, hash_start, lease_end, lock_token) "
            f"VALUES ({table_id}, {boundary}, UNIX_TIMESTAMP() + 300, 'local-smoke-held')"
        )
        request(self.control_urls[-1] + f"/tables/{table_id}", "DELETE", expected=500)
        remaining = self.sql(f"SELECT lock_token FROM partition_lock WHERE table_id = {table_id}")
        if remaining != "local-smoke-held":
            raise RuntimeError(f"Failed deletion did not preserve/release the correct locks: {remaining}")
        self.sql(
            f"UPDATE partition_lock SET lease_end = UNIX_TIMESTAMP() - 1 WHERE table_id = {table_id}"
        )
        # Retrying must replace the expired lease and finish cleanup.
        request(self.control_urls[-1] + f"/tables/{table_id}", "DELETE", expected=204)
        for url in self.data_urls:
            request(url + record_path + "?include_deleted=true", expected=404)
        counts = self.sql(
            f"SELECT (SELECT COUNT(*) FROM `table` WHERE table_id = {table_id}), "
            f"(SELECT COUNT(*) FROM `partition` WHERE table_id = {table_id}), "
            f"(SELECT COUNT(*) FROM partition_lock WHERE table_id = {table_id})"
        )
        if counts.split() != ["0", "0", "0"]:
            raise RuntimeError(f"Table metadata or locks survived cleanup: {counts}")
        print("Smoke test passed.", flush=True)

    def stop(self):
        # Stop the HTTP servers before the database they depend on.
        for name, process in reversed(self.processes):
            if process.poll() is None:
                process.send_signal(signal.SIGTERM if name == "mysql" else signal.SIGINT)
                try:
                    process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()
        print(f"Cluster stopped. Logs and databases kept in {self.directory}", flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--control-nodes", type=int, default=2)
    parser.add_argument("--data-nodes", type=int, default=3)
    parser.add_argument("--control-port", type=int, default=3100, help="first control node port")
    parser.add_argument("--data-port", type=int, default=3200, help="first data node port")
    parser.add_argument("--mysql-port", type=int, default=13306)
    parser.add_argument("--web-port", type=int, default=3300, help="local Flask dashboard port")
    parser.add_argument("--no-web", action="store_true", help="run without the Flask dashboard")
    parser.add_argument("--open-browser", action="store_true", help="open the dashboard after startup")
    parser.add_argument("--no-build", action="store_true", help="reuse the last optimized release build")
    parser.add_argument("--smoke-test", action="store_true", help="test the cluster, then stop and exit")
    args = parser.parse_args()
    if args.control_nodes < 1 or args.data_nodes < 1:
        parser.error("at least one control and one data node are required")
    if args.smoke_test and args.data_nodes < 3:
        parser.error("table creation requires at least three data nodes for the smoke test")

    def interrupted(signum, frame):
        raise KeyboardInterrupt

    signal.signal(signal.SIGTERM, interrupted)
    cluster = None
    server = None
    web_app = None
    try:
        if not args.no_web:
            try:
                from cluster_web import create_app
                from werkzeug.serving import make_server
            except ImportError as error:
                raise RuntimeError("Install the dashboard dependency: python3 -m pip install -r scripts/requirements.txt (or use --no-web)") from error
        cluster = Cluster(args)
        cluster.start()
        if not args.no_web:
            web_app = create_app(cluster, request)
            server = make_server("0.0.0.0", args.web_port, web_app, threaded=True)
            threading.Thread(target=server.serve_forever, daemon=True).start()
            web_url = f"http://127.0.0.1:{args.web_port}"
            print(f"Dashboard: {web_url}", flush=True)
            if args.open_browser:
                import webbrowser
                webbrowser.open(web_url)
        if args.smoke_test:
            cluster.smoke_test()
        else:
            print("Ready. Ctrl-C stops the cluster.", flush=True)
            while True:
                cluster.check_processes()
                time.sleep(1)
    except KeyboardInterrupt:
        return 130
    except (OSError, RuntimeError, subprocess.SubprocessError) as error:
        print(f"Error: {error}", file=sys.stderr)
        return 1
    finally:
        if web_app is not None:
            web_app.extensions["load_generator"].close()
        if server is not None:
            server.shutdown()
            server.server_close()
        if cluster is not None:
            cluster.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
