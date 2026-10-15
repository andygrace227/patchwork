"""Use the existing Rust gRPC client from the local Python tools."""

import json
from pathlib import Path
import subprocess
import sys


BINARY = Path(__file__).resolve().parents[1] / "rust" / "target" / "release" / ("patchwork-data.exe" if sys.platform == "win32" else "patchwork-data")


def data_request(url, operation, fields=None, expected="OK"):
    if not BINARY.is_file():
        raise RuntimeError("Rust data client is missing. Restart without --no-build to build it.")
    try:
        process = subprocess.run(
            [str(BINARY)], input=json.dumps({**(fields or {}), "url": url, "operation": operation}),
            text=True, capture_output=True, timeout=65,
        )
    except subprocess.SubprocessError as error:
        raise RuntimeError(f"{operation} {url}: {error}") from error
    if process.returncode:
        raise RuntimeError(process.stderr.strip() or "Rust data client exited without a response")
    result = json.loads(process.stdout)
    if expected is None:
        return {"status": result["status"], "body": json.dumps(result["body"]), "protocol": "gRPC"}
    if result["status"] != expected:
        raise RuntimeError(f"{operation} {url}: expected {expected}, got {result['status']}: {result['body']}")
    return result["body"]
