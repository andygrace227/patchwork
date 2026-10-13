"""Run the Rust load worker; Flask only handles configuration and progress."""

from copy import deepcopy
import json
from pathlib import Path
import subprocess
import sys
import threading


BINARY = Path(__file__).resolve().parents[1] / "rust" / "target" / "release" / ("patchwork-load.exe" if sys.platform == "win32" else "patchwork-load")


class LoadGenerator:
    def __init__(self, control_urls, directory):
        self.control_urls = control_urls
        self.directory = directory
        self.lock = threading.Lock()
        self.process = self.thread = None
        self.closed = False
        self.state = {
            "engine": "rust", "phase": "idle", "running": False, "config": {},
            "elapsed": 0, "counts": {}, "statuses": {}, "current_tps": 0,
            "average_tps": 0, "p95_ms": 0, "last_error": "", "report": None,
        }

    def start(self, values):
        with self.lock:
            if self.closed:
                raise RuntimeError("The load generator is shutting down")
            if self.thread is not None and self.thread.is_alive():
                raise RuntimeError("A load test is already running")
            if not BINARY.is_file():
                raise RuntimeError("Rust load worker is missing. Restart without --no-build to build it.")
            command = [str(BINARY), "--report-dir", str(self.directory)]
            for url in self.control_urls.values():
                command.extend(["--control-url", url])
            with (self.directory / "load-worker.log").open("w") as log:
                process = subprocess.Popen(command, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                           stderr=log, text=True, bufsize=1)
            try:
                process.stdin.write(json.dumps(values) + "\n")
                process.stdin.flush()
                initial = json.loads(process.stdout.readline())
                if "error" in initial:
                    raise ValueError(initial["error"])
            except Exception:
                process.stdin.close()
                process.wait(timeout=20)
                process.stdout.close()
                raise
            self.process, self.state = process, initial
            self.thread = threading.Thread(target=self.read_progress, args=(process,), daemon=True)
            self.thread.start()

    def read_progress(self, process):
        try:
            for line in process.stdout:
                state = json.loads(line)
                with self.lock:
                    if "error" in state:
                        self.state.update(phase="failed", running=False, last_error=state["error"])
                    else:
                        self.state = state
            code = process.wait()
            with self.lock:
                if code or self.state["running"]:
                    self.state.update(phase="failed", running=False)
                    self.state["last_error"] = self.state["last_error"] or f"Rust load worker exited ({code}); see load-worker.log"
        except (OSError, ValueError) as error:
            with self.lock:
                self.state.update(phase="failed", running=False, last_error=str(error))
        finally:
            process.stdout.close()
            process.stdin.close()

    def snapshot(self):
        with self.lock:
            return deepcopy(self.state)

    def stop(self):
        with self.lock:
            if self.process is not None and self.process.poll() is None and self.state["running"]:
                try:
                    self.process.stdin.write("stop\n")
                    self.process.stdin.flush()
                    self.state["phase"] = "stopping"
                except (BrokenPipeError, ValueError):
                    pass

    def close(self):
        with self.lock:
            self.closed = True
        self.stop()
        if self.thread is not None:
            self.thread.join(timeout=20)
            if self.thread.is_alive():
                self.process.kill()
                self.thread.join()
