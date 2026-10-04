"""
Integration tests: the dashboard API serving real rows from a real database.

The API is the only thing a viewer of this project ever touches, so it has to
(a) refuse unauthenticated callers, (b) report what is actually in the
database, and (c) not leak resources under sustained use.
"""
import os

import pytest
from fastapi.testclient import TestClient

import causal_engine
import detector
import factory
import main as dashboard
import twin
from conftest import scalar

KEY = {"X-API-Key": os.environ["API_KEY"]}


@pytest.fixture()
def client(conn):
    return TestClient(dashboard.app)


def seed_a_little_of_everything(conn):
    twin.handle_service_timing(conn, factory.service_timing_event("order_fired", ticket_id="t1", table_id="table-07", station_id="station-grill"))
    twin.handle_staff_shift(conn, factory.staff_shift_event("clock_in", staff_id="s1", role="line_cook", station_id="station-grill"))
    detector.insert_anomaly(conn, detector.build_control_limit_event(
        "pickup_delay_ms",
        {"restaurant_id": "rest-001", "station_id": "station-grill", "table_id": "table-07", "ticket_id": "t1",
         "order_time": "2026-10-01T12:00:00Z", "computed_at": "2026-10-01T12:05:00Z"}, 9e6, (1.0, 2.0)))
    spec = causal_engine.TREATMENT_MAP["pickup_delay_ms"]
    finding = causal_engine._build_finding(spec, {"effect_estimate": 74000.0, "confidence_interval": None, "method": "m", "refutation_passed": True}, "rest-001", None, None)
    causal_engine.insert_finding(conn, finding)
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO narrated_findings (finding_id, restaurant_id, narrative_text, model_used) VALUES (%s, 'rest-001', 'Staffing raised pickup delay.', 'template-fallback')",
            (finding["finding_id"],),
        )
    conn.commit()
    return finding


# ------------------------------------------------------------------ authentication

def test_health_is_open_so_a_plain_uptime_check_works(client):
    assert client.get("/api/health").json() == {"status": "ok"}


@pytest.mark.parametrize("path", ["/api/twin/tables", "/api/twin/staff", "/api/twin/stations", "/api/findings/narrated", "/api/anomalies/summary", "/api/comparison", "/api/edge/plate-waste"])
def test_every_data_route_rejects_a_missing_or_wrong_key(client, path):
    assert client.get(path).status_code == 401
    assert client.get(path, headers={"X-API-Key": "wrong"}).status_code == 401
    assert client.get(path, headers=KEY).status_code == 200


# ------------------------------------------------------------------ the API reports the database

def test_the_twin_routes_report_what_the_twin_wrote(conn, client):
    seed_a_little_of_everything(conn)
    tables = client.get("/api/twin/tables", headers=KEY).json()
    assert [(t["table_id"], t["status"]) for t in tables] == [("table-07", "occupied")]
    staff = client.get("/api/twin/staff", headers=KEY).json()
    assert [(s["staff_id"], s["status"], s["station_id"]) for s in staff] == [("s1", "on_shift", "station-grill")]
    stations = client.get("/api/twin/stations", headers=KEY).json()
    assert [(s["station_id"], s["open_ticket_count"]) for s in stations] == [("station-grill", 1)]


def test_narrated_findings_are_joined_to_the_evidence_behind_them(conn, client):
    finding = seed_a_little_of_everything(conn)
    body = client.get("/api/findings/narrated", headers=KEY).json()
    assert len(body) == 1
    row = body[0]
    assert row["finding_id"] == finding["finding_id"]
    assert row["narrative_text"] == "Staffing raised pickup delay."
    assert row["model_used"] == "template-fallback"
    assert row["effect_estimate"] == pytest.approx(74000.0)
    assert row["refutation_passed"] is True


def test_the_anomaly_summary_groups_by_method_and_severity(conn, client):
    seed_a_little_of_everything(conn)
    body = client.get("/api/anomalies/summary", headers=KEY).json()
    assert [(r["detection_method"], r["count"]) for r in body] == [("control_limit", 1)]


def test_an_empty_database_returns_empty_lists_not_errors(conn, client):
    for path in ("/api/twin/tables", "/api/twin/staff", "/api/twin/stations", "/api/findings/narrated", "/api/anomalies/summary"):
        assert client.get(path, headers=KEY).json() == []


@pytest.mark.parametrize("limit,status", [(1, 200), (200, 200), (0, 400), (201, 400), (-5, 400)])
def test_the_narrated_findings_limit_is_bounded(conn, client, limit, status):
    assert client.get(f"/api/findings/narrated?limit={limit}", headers=KEY).status_code == status


def test_the_comparison_endpoint_validates_its_inputs(conn, client):
    assert client.get("/api/comparison?source_id=ok-id-1", headers=KEY).status_code == 200
    assert client.get("/api/comparison?source_id=Bad;DROP TABLE x", headers=KEY).status_code == 422
    assert client.get("/api/comparison?hours=0", headers=KEY).status_code == 422
    assert client.get("/api/comparison?hours=100000", headers=KEY).status_code == 422


def test_sql_injection_through_a_query_parameter_does_nothing(conn, client):
    client.get("/api/findings/narrated?limit=1;DROP TABLE causal_findings", headers=KEY)
    assert scalar(conn, "SELECT to_regclass('public.causal_findings') IS NOT NULL") is True


# ------------------------------------------------------------------ resource hygiene

def test_sustained_use_does_not_leak_database_connections(conn, client):
    # Every request opens a connection. If they were never closed, a dashboard
    # left open for a day would exhaust the database's connection limit.
    def open_connections():
        return scalar(conn, "SELECT count(*) FROM pg_stat_activity WHERE datname = current_database() AND pid <> pg_backend_pid()")

    baseline = open_connections()
    for _ in range(150):
        assert client.get("/api/twin/tables", headers=KEY).status_code == 200
    assert open_connections() <= baseline + 5, "connections are accumulating across requests"


# ------------------------------------------------------------------ the edge fleet view

def _edge_event(conn, minutes_ago=1, **edge):
    import datetime
    stamp = (datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(minutes=minutes_ago)).strftime("%Y-%m-%dT%H:%M:%S.000Z")
    event = factory.plate_waste_event(timestamp=stamp)
    event["edge_inference"].update(edge)
    import consumer
    consumer.insert_plate_waste(conn.cursor(), event)
    conn.commit()
    return event


def test_the_edge_route_reports_each_nodes_health_from_its_own_flags(conn, client):
    for latency in (0.1, 0.1, 0.1, 0.1):
        _edge_event(conn, minutes_ago=30, out_of_distribution=False, drift_suspected=False, inference_latency_ms=latency)
    _edge_event(conn, minutes_ago=20, out_of_distribution=True, drift_suspected=False, inference_latency_ms=0.1)
    _edge_event(conn, minutes_ago=2, out_of_distribution=False, drift_suspected=True, inference_latency_ms=0.9)
    body = client.get("/api/edge/plate-waste?minutes=60", headers=KEY).json()
    assert body["window_minutes"] == 60
    (node,) = body["nodes"]
    assert node["readings"] == 6
    assert node["out_of_distribution"] == 1 and node["out_of_distribution_rate"] == pytest.approx(1 / 6, abs=1e-4)
    assert node["drift_suspected"] == 1 and node["drift_rate"] == pytest.approx(1 / 6, abs=1e-4)
    assert node["drifting_now"] is True, "the most recent reading was the drifting one"
    assert node["latency_ms_p50"] == pytest.approx(0.1) and node["latency_ms_p95"] > 0.1
    assert len(node["model_sha256"]) == 64 and node["model_id"] and node["model_version"]


def test_the_edge_route_ignores_events_with_no_on_node_inference_and_respects_the_window(conn, client):
    import consumer
    vendor = factory.plate_waste_event()
    del vendor["edge_inference"]
    vendor["timestamp"] = _edge_event(conn)["timestamp"]
    consumer.insert_plate_waste(conn.cursor(), vendor)
    _edge_event(conn, minutes_ago=600)  # ten hours old: outside a 60-minute window
    conn.commit()
    body = client.get("/api/edge/plate-waste?minutes=60", headers=KEY).json()
    assert [node["readings"] for node in body["nodes"]] == [1]


def test_the_edge_route_with_no_edge_data_is_an_empty_fleet_not_an_error(client):
    assert client.get("/api/edge/plate-waste", headers=KEY).json() == {"window_minutes": 60, "nodes": []}


@pytest.mark.parametrize("minutes", ["0", "-5", "10081", "abc"])
def test_the_edge_route_rejects_a_nonsensical_window(client, minutes):
    assert client.get(f"/api/edge/plate-waste?minutes={minutes}", headers=KEY).status_code == 422
