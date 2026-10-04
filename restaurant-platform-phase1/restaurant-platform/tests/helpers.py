"""
Small helpers shared by the test layers: repo paths, and a thin wrapper for
driving the Docker Compose test stack from Python.
"""
import json
import os
import shutil
import subprocess
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent  # .../restaurant-platform
COMPOSE_FILE = ROOT / "docker-compose.yml"
COMPOSE_TEST_OVERRIDE = ROOT / "tests" / "e2e" / "docker-compose.test.yml"
SCHEMAS = ROOT / "schemas"

# A separate project name keeps the test stack's containers, network and
# volumes completely apart from a developer's own `docker compose up`.
PROJECT = os.environ.get("TEST_COMPOSE_PROJECT", "rp-test")
DASHBOARD_PORT = os.environ.get("DASHBOARD_PORT", "18080")


def have(command: str) -> bool:
    return shutil.which(command) is not None


def run(cmd, check=True, timeout=300, input_text=None):
    result = subprocess.run(
        cmd, capture_output=True, text=True, timeout=timeout, input=input_text,
        env={**os.environ, "DASHBOARD_PORT": DASHBOARD_PORT},
    )
    if check and result.returncode != 0:
        raise RuntimeError(f"{' '.join(map(str, cmd))} failed ({result.returncode}):\n{result.stdout}\n{result.stderr}")
    return result


def compose(*args, check=True, timeout=300, input_text=None):
    cmd = ["docker", "compose", "-p", PROJECT, "-f", str(COMPOSE_FILE), "-f", str(COMPOSE_TEST_OVERRIDE), *args]
    return run(cmd, check=check, timeout=timeout, input_text=input_text)


def sql(query: str, timeout=60) -> str:
    """Run a query inside the stack's own TimescaleDB container; returns trimmed stdout."""
    result = compose(
        "exec", "-T", "timescaledb", "psql", "-U", "restaurant_app", "-d", "restaurant_platform",
        "-tA", "-c", query, timeout=timeout,
    )
    return result.stdout.strip()


def sql_int(query: str) -> int:
    return int(sql(query) or 0)


def wait_for(predicate, timeout, interval=3.0, description="condition"):
    """Poll `predicate` until it returns truthy; raise with `description` on timeout."""
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        try:
            last = predicate()
            if last:
                return last
        except Exception as exc:  # the stack may still be starting
            last = exc
        time.sleep(interval)
    raise AssertionError(f"timed out after {timeout}s waiting for {description} (last value: {last!r})")


def container_id(service: str) -> str:
    # -a: Compose v2's `ps` lists only running containers, so a one-shot service that has
    # already exited (kafka-connect-init) was not found on GitHub's runners (Docker Engine, v2).
    # Compose v1 accepts the flag and lists the same containers.
    ids = compose("ps", "-a", "-q", service).stdout.strip().splitlines()
    assert ids, f"no container found for service {service}"
    return ids[0]


def inspect(service: str) -> dict:
    return json.loads(run(["docker", "inspect", container_id(service)]).stdout)[0]


def restart_count(service: str) -> int:
    return int(inspect(service)["RestartCount"])


def logs(service: str, tail=200) -> str:
    result = compose("logs", "--no-color", "--tail", str(tail), service, check=False)
    return result.stdout + result.stderr
