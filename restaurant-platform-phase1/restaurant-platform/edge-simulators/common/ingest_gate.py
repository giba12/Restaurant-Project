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

Two more signals, both added after the nightly workflow's resilience layer lost events the
first version could not see (DEF-151), each a direct fact the simulator can check itself:

  * Kafka. With Kafka down Connect's REST API keeps saying RUNNING (its status is written to a
    Kafka topic it cannot reach), yet three seconds after Connect loses the group coordinator
    (`heartbeat.interval.ms`, by design: "to avoid running tasks while not being a member of the
    group") it revokes every task and discards what they had buffered: 109 to 166 events across one
    restart. The gate closes when Kafka's bootstrap address stops accepting connections and stays
    shut for 45 s after it returns, because the tasks came back 19 to 24 s later and REST does not
    say so until then. (Raising `heartbeat.interval.ms` to 60 s made Connect ride out a restart
    with no loss, but a Connect crash then took 63 s to rejoin the group and lost 318 of 644.)
  * The simulator's own MQTT connection. When Mosquitto restarts every client is dropped; the
    simulator is back within a second but each connector reconnects on its own Paho backoff
    (measured 2, 4, 8 and 17 s), and what is sent before it has resubscribed is lost (11, 2 and 2
    events in three restarts). The gate closes when the connection drops and stays shut for 30 s
    after a *re*connection.

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
import socket
import ssl
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


def kafka_is_reachable(address: tuple[str, int], ssl_context: ssl.SSLContext | None = None) -> bool:
    """
    True if the Kafka bootstrap address answers. A plain connection for a plaintext listener; for a TLS
    listener a full handshake, because a connection dropped before the handshake makes Kafka log a failed
    authentication on every check. A TLS *error* still means something answered (the broker is up and
    it is this side's trust that is wrong), so it counts as reachable: a misconfigured certificate must
    not leave the gate shut for ever.
    """
    try:
        with socket.create_connection(address, timeout=1.0) as sock:
            if ssl_context is not None:
                with ssl_context.wrap_socket(sock, server_hostname=address[0]):
                    pass
            return True
    except ssl.SSLError:
        return True
    except OSError:
        return False


def first_bootstrap_address(bootstrap_servers: str | None) -> tuple[str, int] | None:
    """('host', port) of the first entry of a Kafka bootstrap list, or None if there is none to read."""
    first = (bootstrap_servers or "").split(",")[0].strip()
    host, _, port = first.rpartition(":")
    return (host, int(port)) if host and port.isdigit() else None


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
                 reconnect_hold_seconds: float = 30.0, kafka_address: tuple[str, int] | None = None,
                 kafka_poll_seconds: float = 0.25, kafka_hold_seconds: float = 45.0,
                 fetch=fetch_connector_status, kafka_check=kafka_is_reachable, clock=time.monotonic):
        self.base_url = base_url
        self.connector = connector
        self.poll_seconds = poll_seconds
        self.settle_seconds = settle_seconds
        self.reconnect_hold_seconds = reconnect_hold_seconds
        self._fetch = fetch
        self._clock = clock
        # Kafka itself. While the broker is down Connect's REST API keeps saying RUNNING (its status is
        # written to a Kafka topic it cannot reach), yet Connect revokes every task three seconds after
        # it loses the group coordinator and throws away what they had buffered; measured: 109 to 166
        # events lost across one restart. The tasks come back 19 to 24 s after Kafka is up again, and
        # REST does not say so until then, so the gate stays shut for `kafka_hold_seconds` after Kafka
        # returns (about twice the longest recovery seen).
        self.kafka_address = kafka_address
        self.kafka_poll_seconds = kafka_poll_seconds
        self.kafka_hold_seconds = kafka_hold_seconds
        self._kafka_check = kafka_check
        self._kafka_up = True
        self._kafka_hold_until = 0.0
        self._kafka_checked_at = None
        self._running_since = None
        self._open = not base_url  # no URL configured: there is nothing to wait for
        # This simulator's own connection to the MQTT broker. If the broker restarts, every client,
        # the connectors included, is dropped; the simulator is back within a second but each
        # connector reconnects on its own exponential backoff (measured: 2, 4, 8 and 17 s), and an
        # event published before its connector has resubscribed is lost. REST says nothing about it.
        self._broker_up = True
        self._hold_until = 0.0
        self._stop = threading.Event()
        self._thread = None

    @property
    def enabled(self) -> bool:
        return bool(self.base_url)

    def is_open(self) -> bool:
        if not self.enabled:
            return True
        now = self._clock()
        return self._open and self._broker_up and now >= self._hold_until and self._kafka_up and now >= self._kafka_hold_until

    def broker_lost(self) -> None:
        """This simulator lost its MQTT connection: hold events until it is back and the connectors have had time to follow."""
        if self.enabled and self._broker_up:
            log.info("ingest gate closed: the MQTT connection was lost; holding events")
        self._broker_up = False

    def broker_back(self, reconnect: bool) -> None:
        """The MQTT connection is up. After a *re*connection the gate stays shut for `reconnect_hold_seconds` more."""
        self._broker_up = True
        if reconnect and self.enabled:
            self._hold_until = self._clock() + self.reconnect_hold_seconds
            log.info("MQTT connection restored; holding events %.0fs while the connectors reconnect", self.reconnect_hold_seconds)

    def _check_kafka(self) -> None:
        if not self.kafka_address:
            return
        now = self._clock()
        if self._kafka_checked_at is not None and now - self._kafka_checked_at < self.kafka_poll_seconds:
            return
        self._kafka_checked_at = now
        up = self._kafka_check(self.kafka_address)
        if up and not self._kafka_up:
            self._kafka_hold_until = now + self.kafka_hold_seconds
            log.info("Kafka is reachable again; holding events %.0fs while Connect restarts its tasks", self.kafka_hold_seconds)
        elif not up and self._kafka_up:
            log.info("ingest gate closed: Kafka at %s:%s is not reachable; holding events", *self.kafka_address)
        self._kafka_up = up

    def poll_once(self) -> bool:
        self._check_kafka()
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
        return self.is_open()

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
