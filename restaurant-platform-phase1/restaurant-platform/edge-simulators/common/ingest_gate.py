"""
Store-and-forward gate: is the bridge that carries this sensor's events into Kafka running?

The MQTT broker keeps no message for a subscriber that is not connected, and the
Kafka Connect MQTT source connectors subscribe with a clean session. So whatever a
sensor published while its connector was down, restarting or not yet started was
gone (measured on 2026-10-05: 470 of 1,266 events across one 45 s Connect outage
plus its recovery; and about 30 s of events on every cold start, which is the
likely cause of DEF-147).

A persistent MQTT session is the textbook answer and does not work with this
connector: Mosquitto hands the whole backlog over the moment the connector
connects, the connector's callback thread blocks in the first message waiting
for the sink route, and the sink route cannot start until the MQTT consumer has
finished starting. Every queued message then fails after 30 s and is lost, and
the task never finishes starting. (Reproduced; see DEF-148.)

So the sensor does what a real edge device does: it holds its events while the
bridge is not running and sends them, in order, when it is. This module only
answers "is it running?" by asking the Kafka Connect REST API about this
sensor's own connector. A gate that cannot be reached counts as closed: that is
exactly what a Connect outage looks like from here.

RUNNING is not "subscribed". Kafka Connect reports a task RUNNING before the
task's start() has finished, and for these connectors start() is what starts the
Camel route and subscribes to MQTT. Measured on 2026-10-05: task started 02:08:27.4,
REST said RUNNING by 02:08:28.5, the route actually started at 02:08:30.3; the 312
events flushed in between were all lost. The REST API has nothing that says "the
subscription exists", so the gate opens only after the connector has been seen
RUNNING continuously for a settle period (default 20 s; the task takes about 3 s to
finish starting under Compose and about 7 s on the k3s cluster), and closes at the first look that says otherwise.

What this cannot fully cover: when Connect shuts down its REST API stops answering
about 0.7 s before its MQTT consumers stop, and an event published after that and
before the gate's next poll notices is still lost. Polled once a second, that cost
5 events in six outages at the test rate (2,632 published; every one was published
0.2 to 0.8 s after "Stopping REST server"), against 37% before the gate. The poll
interval is the lever, so it is short (0.25 s); Connect's per-request INFO log is
turned down to WARN in both deployments so that frequency does not flood its log.

Opt-in (INGEST_GATE_URL unset means always open) so a simulator run on its own,
without Kafka Connect, behaves as it always did.
"""
import json
import logging
import threading
import time
import urllib.error
import urllib.request

log = logging.getLogger("ingest-gate")

POLL_TIMEOUT_SECONDS = 3


def connector_is_running(status: dict | None) -> bool:
    """True only for a connector, and every one of its tasks, in state RUNNING (and at least one task)."""
    if not status:
        return False
    if (status.get("connector") or {}).get("state") != "RUNNING":
        return False
    tasks = status.get("tasks") or []
    return bool(tasks) and all(task.get("state") == "RUNNING" for task in tasks)


def fetch_connector_status(base_url: str, connector: str) -> dict | None:
    """The connector's status from the Connect REST API, or None if it cannot be had (down, unknown, unreachable)."""
    try:
        with urllib.request.urlopen(f"{base_url.rstrip('/')}/connectors/{connector}/status", timeout=POLL_TIMEOUT_SECONDS) as response:
            return json.loads(response.read().decode("utf-8"))
    except (urllib.error.URLError, OSError, ValueError):
        return None


class IngestGate:
    """
    Open once the connector has been RUNNING continuously for `settle_seconds`. Closed until then,
    and again at the first look that says it is not running or cannot be had. `fetch` and `clock` are
    injectable so the logic is testable without a network or real waiting.
    """

    def __init__(self, base_url: str | None, connector: str, poll_seconds: float = 0.25, settle_seconds: float = 20.0,
                 fetch=fetch_connector_status, clock=time.monotonic):
        self.base_url = base_url
        self.connector = connector
        self.poll_seconds = poll_seconds
        self.settle_seconds = settle_seconds
        self._fetch = fetch
        self._clock = clock
        self._running_since = None
        self._open = not base_url  # no URL configured: there is nothing to wait for
        self._stop = threading.Event()
        self._thread = None

    @property
    def enabled(self) -> bool:
        return bool(self.base_url)

    def is_open(self) -> bool:
        return self._open

    def poll_once(self) -> bool:
        if connector_is_running(self._fetch(self.base_url, self.connector)):
            if self._running_since is None:
                self._running_since = self._clock()
                if not self._open:
                    log.info("connector %s is running; waiting %.0fs for it to finish subscribing before sending",
                             self.connector, self.settle_seconds)
            now_open = self._clock() - self._running_since >= self.settle_seconds
        else:
            self._running_since = None
            now_open = False
        if now_open != self._open:
            log.info("ingest gate %s: connector %s %s", "open" if now_open else "closed", self.connector,
                     "has been running long enough" if now_open else "is not running; holding events")
        self._open = now_open
        return now_open

    def start(self) -> None:
        if not self.enabled or self._thread is not None:
            return
        self._thread = threading.Thread(target=self._run, name="ingest-gate", daemon=True)
        self._thread.start()

    def _run(self) -> None:
        while not self._stop.is_set():
            try:
                self.poll_once()
            except Exception:  # the poller must outlive anything unexpected: a dead poller is a stuck gate
                log.exception("ingest gate poll failed; treating the connector as not running")
                self._open = False
                self._running_since = None
            self._stop.wait(self.poll_seconds)

    def stop(self) -> None:
        self._stop.set()
