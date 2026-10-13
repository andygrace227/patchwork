"""A rate-limited HTTP load generator for the local control nodes."""

from collections import Counter, deque
from concurrent.futures import ThreadPoolExecutor
import json
import random
import threading
import time
import uuid


class LoadGenerator:
    def __init__(self, send_request, control_urls, directory):
        self.send_request = send_request
        self.control_urls = control_urls
        self.directory = directory
        self.lock = threading.Lock()
        self.cancel = threading.Event()
        self.thread = None
        self.closed = False
        self.phase = "idle"
        self.config = {}
        self.started = self.finished = None
        self.counts = Counter()
        self.statuses = Counter()
        self.latencies = deque(maxlen=10000)
        self.recent = deque()
        self.last_error = ""
        self.report = None

    def start(self, values):
        def number(name, default, lower, upper):
            value = int(str(values.get(name, default)))
            if not lower <= value <= upper:
                raise ValueError(f"{name} must be between {lower} and {upper}")
            return value

        config = {
            "table_id": str(number("table_id", 0, 1, (1 << 63) - 1)),
            "tps": number("tps", 100, 1, 100000),
            "duration": number("duration", 30, 1, 3600),
            "workers": number("workers", 8, 1, 128),
            "read_percent": number("read_percent", 0, 0, 100),
            "records": number("records", 1000, 1, 100000),
            "keys": number("keys", 100, 1, 10000),
            "payload_bytes": number("payload_bytes", 256, 0, 1048576),
            "hot_key": str(number("hot_key", 10, -(1 << 63), (1 << 63) - 1)),
            "distribution": values.get("distribution", "spread"),
            "node": values.get("node", "all"),
            "consistent": values.get("consistent", True),
            "seed": values.get("seed", True),
            "telemetry": values.get("telemetry", False),
            "record_base": str(random.getrandbits(61) + (1 << 61)),
            "run_id": uuid.uuid4().hex[:12],
        }
        if config["distribution"] not in ("spread", "hot"):
            raise ValueError("Choose spread or hot key distribution")
        if config["distribution"] == "hot":
            config["keys"] = 1
        if config["keys"] > config["records"]:
            raise ValueError("Partition keys cannot exceed the number of records")
        if config["node"] != "all" and config["node"] not in self.control_urls:
            raise ValueError("Choose a control node from this cluster")
        if any(not isinstance(config[key], bool) for key in ("consistent", "seed", "telemetry")):
            raise ValueError("consistent, seed, and telemetry must be true or false")
        with self.lock:
            if self.closed:
                raise RuntimeError("The load generator is shutting down")
            if self.thread is not None and self.thread.is_alive():
                raise RuntimeError("A load test is already running")
            self.config = config
            self.phase = "seeding" if config["read_percent"] and config["seed"] else "running"
            self.started = self.finished = None
            self.counts.clear()
            self.statuses.clear()
            self.latencies.clear()
            self.recent.clear()
            self.last_error = ""
            self.report = None
            self.cancel.clear()
            self.thread = threading.Thread(target=self.run, daemon=True)
            self.thread.start()

    def call(self, index, method, sequence, seed=False):
        config = self.config
        if config["distribution"] == "hot":
            partition_key = int(config["hot_key"])
        else:
            slot = index % config["keys"]
            partition_key = -(1 << 63) + ((1 << 64) - 1) * slot // max(1, config["keys"] - 1)
        key = int(config["record_base"]) + index
        path = f"/tables/{config['table_id']}/partition-keys/{partition_key}/records/{key}"
        path += f"?consistent={str(config['consistent']).lower()}&telemetry={str(config['telemetry'] and not seed).lower()}"
        urls = list(self.control_urls.values()) if config["node"] == "all" else [self.control_urls[config["node"]]]
        body, headers = None, None
        if method == "PUT":
            body = {"load_run": config["run_id"], "record": index, "padding": "x" * config["payload_bytes"]}
            headers = {"x-patchwork-timestamp": str(time.time_ns() // 1000)}
        result = self.send_request(urls[sequence % len(urls)] + path, method, body, headers, expected=None)
        return result

    def worker(self, worker_id, seed):
        randomizer = random.Random(worker_id)
        while not self.cancel.is_set():
            with self.lock:
                sequence = self.sequence
                self.sequence += 1
                if seed:
                    if sequence >= self.config["records"]:
                        return
                    due = time.monotonic()
                else:
                    # Discard missed slots when overloaded instead of sending a catch-up burst.
                    due = max(self.next_due, time.monotonic())
                    if due >= self.deadline:
                        return
                    self.next_due = due + 1 / self.config["tps"]
            if self.cancel.wait(max(0, due - time.monotonic())):
                return
            if not seed and time.monotonic() >= self.deadline:
                return
            index = sequence if seed else randomizer.randrange(self.config["records"])
            method = "PUT" if seed or randomizer.randrange(100) >= self.config["read_percent"] else "GET"
            started = time.monotonic()
            status, error = "network", ""
            with self.lock:
                self.counts["in_flight"] += 1
            try:
                result = self.call(index, method, sequence, seed)
                status = str(result["status"])
                if not 200 <= result["status"] < 300 and not (method == "GET" and result["status"] == 404):
                    error = f"HTTP {status}: {result['body'][:400]}"
            except Exception as failure:
                error = str(failure)[:500]
            ended = time.monotonic()
            with self.lock:
                self.counts["in_flight"] -= 1
                if seed:
                    self.counts["seeded"] += 1
                    if status != "204":
                        self.counts["seed_errors"] += 1
                        self.last_error = error or f"Seed write returned HTTP {status}"
                        self.cancel.set()
                else:
                    self.counts["completed"] += 1
                    self.counts["reads" if method == "GET" else "writes"] += 1
                    category = "errors" if error else "misses" if status == "404" else "successes"
                    self.counts[category] += 1
                    self.statuses[status] += 1
                    self.latencies.append((ended - started) * 1000)
                    self.recent.append(ended)
                    while self.recent and self.recent[0] < ended - 1:
                        self.recent.popleft()
                    if error:
                        self.last_error = error

    def run(self):
        try:
            with ThreadPoolExecutor(max_workers=self.config["workers"]) as pool:
                if self.phase == "seeding":
                    self.sequence = 0
                    futures = [pool.submit(self.worker, i, True) for i in range(self.config["workers"])]
                    for future in futures:
                        future.result()
                if not self.cancel.is_set():
                    with self.lock:
                        self.phase = "running"
                        self.started = self.next_due = time.monotonic()
                        self.deadline = self.started + self.config["duration"]
                        self.sequence = 0
                    futures = [pool.submit(self.worker, i, False) for i in range(self.config["workers"])]
                    for future in futures:
                        future.result()
            with self.lock:
                self.finished = time.monotonic()
                self.phase = "failed" if self.counts["seed_errors"] else "stopped" if self.cancel.is_set() else "finished"
        except Exception as error:
            with self.lock:
                self.phase, self.last_error, self.finished = "failed", str(error), time.monotonic()
        report = self.directory / f"load-{self.config['run_id']}.json"
        with self.lock:
            self.report = str(report)
        try:
            report.write_text(json.dumps(self.snapshot(), indent=2) + "\n")
        except OSError as error:
            with self.lock:
                self.last_error = f"Could not save report: {error}"

    def snapshot(self):
        with self.lock:
            now = time.monotonic()
            while self.recent and self.recent[0] < now - 1:
                self.recent.popleft()
            elapsed = max(0, (self.finished or now) - self.started) if self.started else 0
            samples = sorted(self.latencies)
            phase = self.phase
            if phase == "running" and self.started and now >= self.deadline:
                phase = "draining"
            return {
                "phase": phase, "running": self.phase in ("seeding", "running", "stopping"),
                "config": dict(self.config), "elapsed": round(elapsed, 2),
                "counts": dict(self.counts), "statuses": dict(self.statuses),
                "current_tps": round(len(self.recent) / min(1, elapsed), 1) if elapsed else 0,
                "average_tps": round(self.counts["completed"] / elapsed, 1) if elapsed else 0,
                "p95_ms": round(samples[int((len(samples) - 1) * .95)], 2) if samples else 0,
                "last_error": self.last_error, "report": self.report,
            }

    def stop(self):
        with self.lock:
            if self.phase in ("seeding", "running"):
                self.phase = "stopping"
                self.cancel.set()

    def close(self):
        with self.lock:
            self.closed = True
        self.stop()
        if self.thread is not None:
            self.thread.join()
