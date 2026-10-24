#!/usr/bin/env python3
"""Run one data node and a dashboard that load-tests it directly over gRPC."""

import argparse
import json
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import threading
import time

from data_client import data_request
from load_generator import LoadGenerator
from local_cluster import Cluster, ROOT, TARGET


class DataNode(Cluster):
    def __init__(self, args):
        self.args = args
        self.directory = Path(tempfile.mkdtemp(prefix="patchwork-data-", dir="/tmp"))
        self.processes = []
        self.url = f"http://127.0.0.1:{args.data_port}"

    def start(self):
        print(f"Run directory: {self.directory}", flush=True)
        if not self.args.no_build:
            subprocess.run([
                "cargo", "build", "--release", "--manifest-path", str(ROOT / "rust" / "Cargo.toml"),
                "--target-dir", str(TARGET.parent), "-p", "patchwork-data-plane", "-p", "patchwork-load", "--bins",
            ], cwd=ROOT, check=True)
        self.start_process("data-1", [str(TARGET / "patchwork-data-plane")], {
            "BIND_ADDR": f"127.0.0.1:{self.args.data_port}",
            "SHARD_PATH": str(self.directory / "data.sqlite"),
            "TOKIO_WORKER_THREADS": str(self.args.data_threads),
            "READ_DB_MAX_CONNECTIONS": str(self.args.readers),
        })
        self.wait_ready(lambda: data_request(self.url, "GetSize"), self.url)
        manifest = {
            "data_url": self.url, "data_threads": self.args.data_threads,
            "load_threads": self.args.load_threads, "readers": self.args.readers,
            "data_pid": self.processes[0][1].pid,
        }
        (self.directory / "node.json").write_text(json.dumps(manifest, indent=2) + "\n")
        print(f"Data node: {self.url} · {self.args.data_threads} Tokio threads · {self.args.readers} readers", flush=True)


def create_app(node):
    from flask import Flask, jsonify, request

    app = Flask(__name__, static_folder="static")
    app.config.update(MAX_CONTENT_LENGTH=2 * 1024 * 1024, TRUSTED_HOSTS=["127.0.0.1", "localhost"])
    load = LoadGenerator({}, node.directory, data_url=node.url, worker_threads=node.args.load_threads)
    app.extensions["load_generator"] = load

    @app.get("/")
    def index():
        return app.send_static_file("data-node.html")

    @app.get("/api/node")
    def status():
        return jsonify(url=node.url, data_threads=node.args.data_threads,
                       load_threads=node.args.load_threads, readers=node.args.readers,
                       directory=str(node.directory))

    @app.get("/api/load")
    def load_status():
        return jsonify(load.snapshot())

    @app.post("/api/load")
    def start_load():
        try:
            values = request.get_json()
            if not isinstance(values, dict):
                raise ValueError("Send a JSON load configuration")
            load.start(values)
            return jsonify(load.snapshot()), 202
        except (ValueError, RuntimeError, OSError) as error:
            return jsonify(error=str(error)), 400

    @app.post("/api/load/stop")
    def stop_load():
        load.stop()
        return jsonify(load.snapshot())

    return app


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-port", type=int, default=3210)
    parser.add_argument("--web-port", type=int, default=3310)
    parser.add_argument("--data-threads", type=int, default=2, help="Tokio worker threads for the data node (default: 2)")
    parser.add_argument("--load-threads", type=int, default=4, help="Tokio worker threads for the load generator (default: 4)")
    parser.add_argument("--readers", type=int, default=4, help="SQLite read connections (default: 4)")
    parser.add_argument("--no-build", action="store_true", help="reuse the existing release binaries")
    parser.add_argument("--no-web", action="store_true", help="start only the data node")
    parser.add_argument("--open-browser", action="store_true")
    args = parser.parse_args()
    if min(args.data_threads, args.load_threads, args.readers) < 1:
        parser.error("thread and reader counts must be positive")
    if not all(1 <= port <= 65535 for port in [args.data_port, args.web_port]):
        parser.error("ports must be between 1 and 65535")
    if not args.no_web and args.data_port == args.web_port:
        parser.error("data and dashboard ports must differ")

    def interrupted(signum, frame):
        raise KeyboardInterrupt

    signal.signal(signal.SIGTERM, interrupted)
    node = None
    app = None
    server = None
    try:
        if not args.no_web:
            from werkzeug.serving import make_server
        node = DataNode(args)
        node.start()
        if not args.no_web:
            app = create_app(node)
            server = make_server("127.0.0.1", args.web_port, app, threaded=True)
            threading.Thread(target=server.serve_forever, daemon=True).start()
            url = f"http://127.0.0.1:{args.web_port}"
            print(f"Dashboard: {url}", flush=True)
            if args.open_browser:
                import webbrowser
                webbrowser.open(url)
        print("Ready. Ctrl-C stops the data node.", flush=True)
        while True:
            node.check_processes()
            time.sleep(1)
    except KeyboardInterrupt:
        return 130
    except (ImportError, OSError, RuntimeError, subprocess.SubprocessError) as error:
        print(f"Error: {error}", file=sys.stderr)
        return 1
    finally:
        signal.signal(signal.SIGINT, signal.SIG_IGN)
        signal.signal(signal.SIGTERM, signal.SIG_IGN)
        try:
            if app is not None:
                app.extensions["load_generator"].close()
        finally:
            try:
                if server is not None:
                    server.shutdown()
                    server.server_close()
            finally:
                if node is not None:
                    node.stop()


if __name__ == "__main__":
    sys.exit(main())
