"""
Tests for the X-API-Key check every route except /api/health goes through.
No database: reaching a route's body at all (rather than 401ing first) is
proven by monkeypatching common.pg_connect to raise a distinctive error,
not by a real connection.

    pip install fastapi httpx pytest
    cd services/dashboard-api && python -m pytest test_auth.py
"""
import os
import sys
import types

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(HERE))
sys.path.insert(0, HERE)
sys.modules.setdefault("psycopg2", types.SimpleNamespace())

import pytest
from fastapi.testclient import TestClient

import main


class _NoDatabase(Exception):
    pass


@pytest.fixture()
def client(monkeypatch):
    monkeypatch.setattr(main, "API_KEY", "the-real-key")
    monkeypatch.setattr(main.common, "pg_connect", lambda: (_ for _ in ()).throw(_NoDatabase()))
    return TestClient(main.app)


def test_a_request_with_no_key_is_401_before_reaching_the_database(client):
    r = client.get("/api/twin/stations")
    assert r.status_code == 401 and "X-API-Key" in r.text


def test_a_request_with_the_wrong_key_is_401(client):
    assert client.get("/api/twin/stations", headers={"X-API-Key": "wrong"}).status_code == 401


def test_the_right_key_reaches_the_route(client):
    # 500 (from _NoDatabase, not caught here) proves auth passed and the
    # handler actually ran, without needing a real database.
    with pytest.raises(_NoDatabase):
        client.get("/api/twin/stations", headers={"X-API-Key": "the-real-key"})


def test_health_needs_no_key_at_all(client):
    assert client.get("/api/health").status_code == 200


def test_an_unset_api_key_fails_closed_not_open(client, monkeypatch):
    monkeypatch.setattr(main, "API_KEY", "")
    r = client.get("/api/twin/stations", headers={"X-API-Key": "anything"})
    assert r.status_code == 500 and "API_KEY" in r.text
    assert client.get("/api/health").status_code == 200
