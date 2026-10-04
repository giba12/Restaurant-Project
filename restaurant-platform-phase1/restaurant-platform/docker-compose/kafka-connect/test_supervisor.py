"""
Tests for the Compose connector supervisor (supervisor.py).

The supervisor exists because Kafka Connect never restarts a failed task and
reports a connector RUNNING while its task is FAILED (DEF-137: a transient
start-up DNS failure left every task failed for good; DEF-142: connectors sat
UNASSIGNED for six minutes after a broker restart). These tests drive it with a
fake clock, then against a real HTTP server that behaves like Connect's REST API.

    cd docker-compose/kafka-connect && python -m pytest test_supervisor.py -v
"""
import json
import os
import sys
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

import pytest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import supervisor  # noqa: E402


def status(connector="RUNNING", *tasks):
    return {"connector": {"state": connector}, "tasks": [{"id": i, "state": s} for i, s in enumerate(tasks)]}


class FakeConnect:
    """A scripted Connect: connector name -> status, and a record of the restarts requested."""

    def __init__(self, **statuses):
        self.statuses = dict(statuses)
        self.restarts = []

    def __call__(self, method, path):
        if method == "GET" and path == "/connectors":
            return list(self.statuses) if self.statuses is not None else None
        if method == "GET" and path.startswith("/connectors/") and path.endswith("/status"):
            return self.statuses.get(path.split("/")[2])
        if method == "POST":
            self.restarts.append(path)
            return None
        raise AssertionError((method, path))


class Clock:
    def __init__(self):
        self.now = 1000.0

    def __call__(self):
        return self.now


@pytest.fixture(autouse=True)
def short_timers(monkeypatch):
    monkeypatch.setattr(supervisor, "GRACE", 30.0)
    monkeypatch.setattr(supervisor, "BACKOFF", 60.0)


@pytest.mark.parametrize("state, expected", [
    (status("RUNNING", "RUNNING"), supervisor.HEALTHY),
    (status("RUNNING", "RUNNING", "RUNNING"), supervisor.HEALTHY),
    (status("RUNNING", "FAILED"), supervisor.RESTART_FAILED),
    (status("RUNNING", "RUNNING", "FAILED"), supervisor.RESTART_FAILED),
    (status("FAILED"), supervisor.RESTART_FAILED),
    (status("UNASSIGNED", "RUNNING"), supervisor.RESTART_ALL),
    (status("RUNNING"), supervisor.RESTART_ALL),
    (status("RUNNING", "UNASSIGNED"), supervisor.RESTART_ALL),
    (status("PAUSED", "RUNNING"), supervisor.LEAVE),
    (status("STOPPED"), supervisor.LEAVE),
], ids=["healthy", "healthy-3-tasks", "failed-task", "one-of-three-failed", "failed-connector",
        "unassigned", "no-task", "unassigned-task", "paused", "stopped"])
def test_classification_of_a_connectors_status(state, expected):
    assert supervisor.classify(state)[0] == expected


def test_a_failed_task_is_restarted_at_once_and_only_the_failed_part():
    connect, clock = FakeConnect(a=status("RUNNING", "FAILED")), Clock()
    acted = supervisor.Supervisor(connect, clock).check_once()
    assert acted == [("a", supervisor.RESTART_FAILED)]
    assert connect.restarts == ["/connectors/a/restart?includeTasks=true&onlyFailed=true"]


def test_healthy_connectors_are_never_touched():
    connect = FakeConnect(a=status("RUNNING", "RUNNING"), b=status("RUNNING", "RUNNING"))
    sup = supervisor.Supervisor(connect, Clock())
    for _ in range(20):
        assert sup.check_once() == []
    assert connect.restarts == []


def test_only_the_unhealthy_connector_is_restarted():
    connect = FakeConnect(good=status("RUNNING", "RUNNING"), bad=status("RUNNING", "FAILED"))
    supervisor.Supervisor(connect, Clock()).check_once()
    assert connect.restarts == ["/connectors/bad/restart?includeTasks=true&onlyFailed=true"]


def test_an_unassigned_connector_is_given_a_grace_period_then_fully_restarted():
    connect, clock = FakeConnect(a=status("UNASSIGNED", "RUNNING")), Clock()
    sup = supervisor.Supervisor(connect, clock)
    assert sup.check_once() == []          # first sighting: a worker mid-rebalance looks like this
    clock.now += 29
    assert sup.check_once() == []          # still inside the grace period
    clock.now += 2
    assert sup.check_once() == [("a", supervisor.RESTART_ALL)]
    assert connect.restarts == ["/connectors/a/restart?includeTasks=true&onlyFailed=false"]


def test_a_connector_that_recovers_by_itself_resets_its_grace_clock():
    connect, clock = FakeConnect(a=status("UNASSIGNED", "RUNNING")), Clock()
    sup = supervisor.Supervisor(connect, clock)
    sup.check_once()
    clock.now += 25
    connect.statuses["a"] = status("RUNNING", "RUNNING")   # the rebalance finished
    sup.check_once()
    clock.now += 25
    connect.statuses["a"] = status("UNASSIGNED", "RUNNING")  # a new, unrelated blip
    assert sup.check_once() == []
    clock.now += 25
    assert sup.check_once() == [], "the earlier blip must not count towards this one"
    assert connect.restarts == []


def test_a_connector_that_cannot_start_is_not_hammered():
    connect, clock = FakeConnect(a=status("RUNNING", "FAILED")), Clock()
    sup = supervisor.Supervisor(connect, clock)
    restarts = 0
    for _ in range(40):                    # ten minutes of passes, fifteen seconds apart
        restarts += len(sup.check_once())
        clock.now += 15
    # BACKOFF is 60 s, so at most one restart a minute: about ten in ten minutes, never forty.
    assert 8 <= restarts <= 11


def test_a_paused_connector_is_left_alone_however_long_it_stays_paused():
    connect, clock = FakeConnect(a=status("PAUSED", "RUNNING")), Clock()
    sup = supervisor.Supervisor(connect, clock)
    for _ in range(50):
        sup.check_once()
        clock.now += 60
    assert connect.restarts == []


def test_connect_being_down_or_a_connector_vanishing_does_not_crash_a_pass():
    sup = supervisor.Supervisor(lambda method, path: None, Clock())
    assert sup.check_once() == []

    def vanishing(method, path):
        return ["gone"] if path == "/connectors" else None  # listed, but its status cannot be read
    assert supervisor.Supervisor(vanishing, Clock()).check_once() == []


# ------------------------------------------------------------------ against a real HTTP server


class ConnectHandler(BaseHTTPRequestHandler):
    state = {}
    posts = []

    def log_message(self, *args):
        pass

    def _send(self, payload, code=200):
        body = json.dumps(payload).encode() if payload is not None else b""
        self.send_response(code)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        if self.path == "/connectors":
            return self._send(list(self.state))
        name = self.path.split("/")[2]
        self._send({"name": name, **self.state[name]})

    def do_POST(self):
        self.posts.append(self.path)
        self._send(None, 202)


@pytest.fixture()
def connect_server(monkeypatch):
    ConnectHandler.state = {"plate": status("RUNNING", "FAILED"), "pos": status("RUNNING", "RUNNING")}
    ConnectHandler.posts = []
    server = HTTPServer(("127.0.0.1", 0), ConnectHandler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    monkeypatch.setattr(supervisor, "CONNECT_URL", f"http://127.0.0.1:{server.server_port}")
    yield ConnectHandler
    server.shutdown()


def test_against_a_real_http_server_a_failed_task_is_restarted_through_the_rest_api(connect_server):
    acted = supervisor.Supervisor(supervisor.http_request).check_once()
    assert acted == [("plate", supervisor.RESTART_FAILED)]
    assert connect_server.posts == ["/connectors/plate/restart?includeTasks=true&onlyFailed=true"]


def test_when_connect_is_unreachable_the_request_helper_returns_none_instead_of_raising(monkeypatch):
    monkeypatch.setattr(supervisor, "CONNECT_URL", "http://127.0.0.1:1")  # nothing listens here
    assert supervisor.http_request("GET", "/connectors") is None
