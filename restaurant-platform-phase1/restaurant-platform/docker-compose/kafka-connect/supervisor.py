"""
Connector supervisor for the Compose deployment.

Kafka Connect does not restart a failed task, and keeps reporting a connector
RUNNING while its task is FAILED. On the Kubernetes path Strimzi can be asked to
restart them; the portable path had nothing, so two real failures were left for
a human to find:

  * DEF-137: a transient failure to resolve the `mosquitto` host as a task
    started (`UnknownHostException`) left every connector's task FAILED for good,
    and nothing was ingested.
  * DEF-142: after a Kafka restart under Podman (the broker comes back on a new
    address) the worker's connectors sat UNASSIGNED with stale tasks until the
    next rebalance, about six minutes later.

Every INTERVAL seconds this reads each connector's status and restarts what is
not healthy:

  * any task FAILED, or the connector itself FAILED: restart the failed parts
    at once (`onlyFailed=true`);
  * the connector UNASSIGNED, or RUNNING with no task at all, for at least GRACE
    seconds in a row: restart the connector and all its tasks (a worker that has
    just rebalanced legitimately shows these briefly);
  * PAUSED or STOPPED: left alone, because somebody chose that.

A connector is not restarted again within BACKOFF seconds of the last restart,
so a connector that cannot start does not get hammered. Everything is logged.
No third-party packages: urllib only.
"""
import json
import logging
import os
import time
import urllib.error
import urllib.request

log = logging.getLogger("connector-supervisor")

CONNECT_URL = os.environ.get("CONNECT_URL", "http://kafka-connect:8083").rstrip("/")
INTERVAL = float(os.environ.get("SUPERVISE_INTERVAL_SECONDS", "10"))
GRACE = float(os.environ.get("SUPERVISE_GRACE_SECONDS", "30"))
BACKOFF = float(os.environ.get("SUPERVISE_BACKOFF_SECONDS", "30"))

HEALTHY, RESTART_FAILED, RESTART_ALL, LEAVE = "healthy", "restart_failed", "restart_all", "leave"


def classify(status: dict) -> tuple[str, str]:
    """(verdict, why) for one connector's status document. Pure."""
    connector = status["connector"]["state"]
    tasks = [t["state"] for t in status.get("tasks", [])]
    if connector in ("PAUSED", "STOPPED"):
        return LEAVE, f"connector is {connector}, which someone chose"
    if connector == "FAILED" or "FAILED" in tasks:
        return RESTART_FAILED, f"connector {connector}, tasks {tasks}"
    if connector == "UNASSIGNED":
        return RESTART_ALL, f"connector UNASSIGNED, tasks {tasks}"
    if not tasks:
        return RESTART_ALL, "connector RUNNING with no task"
    if any(t == "UNASSIGNED" for t in tasks):
        return RESTART_ALL, f"tasks {tasks}"
    return HEALTHY, "ok"


class Supervisor:
    def __init__(self, request, clock=time.monotonic):
        self._request = request  # request(method, path) -> parsed JSON or None
        self._clock = clock
        self._unhealthy_since: dict[str, float] = {}
        self._last_restart: dict[str, float] = {}

    def check_once(self) -> list[tuple[str, str]]:
        """One pass over every connector. Returns the restarts it made as (name, kind)."""
        acted = []
        names = self._request("GET", "/connectors") or []
        for name in names:
            status = self._request("GET", f"/connectors/{name}/status")
            if not status:
                continue
            verdict, why = classify(status)
            now = self._clock()
            if verdict in (HEALTHY, LEAVE):
                self._unhealthy_since.pop(name, None)
                continue
            if verdict == RESTART_ALL:
                since = self._unhealthy_since.setdefault(name, now)
                if now - since < GRACE:
                    continue
            if now - self._last_restart.get(name, -BACKOFF) < BACKOFF:
                continue
            only_failed = "true" if verdict == RESTART_FAILED else "false"
            log.warning("restarting %s (%s): %s", name, verdict, why)
            self._request("POST", f"/connectors/{name}/restart?includeTasks=true&onlyFailed={only_failed}")
            self._last_restart[name] = now
            self._unhealthy_since.pop(name, None)
            acted.append((name, verdict))
        return acted


def http_request(method: str, path: str):
    request = urllib.request.Request(CONNECT_URL + path, method=method)
    try:
        with urllib.request.urlopen(request, timeout=10) as response:
            body = response.read()
    except (urllib.error.URLError, OSError) as exc:
        log.warning("%s %s failed: %s", method, path, exc)
        return None
    return json.loads(body) if body else None


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    log.info("supervising %s every %.0fs (grace %.0fs, backoff %.0fs)", CONNECT_URL, INTERVAL, GRACE, BACKOFF)
    supervisor = Supervisor(http_request)
    while True:
        try:
            supervisor.check_once()
        except Exception:
            log.exception("supervision pass failed; continuing")
        time.sleep(INTERVAL)


if __name__ == "__main__":
    main()
