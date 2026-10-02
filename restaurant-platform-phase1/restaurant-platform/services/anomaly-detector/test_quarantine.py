"""
Tests for the anomaly detector's quarantine of interactive tickets. No Kafka:
the kafka import is stubbed. The point being tested is the one that matters:
quarantined tickets must not change any window the baseline is built from.

    pip install numpy pytest jsonschema prometheus-client==0.26.0
    cd services/anomaly-detector && python -m pytest test_quarantine.py
"""
import os
import sys
import types

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(HERE))
sys.path.insert(0, HERE)
os.environ.setdefault("SCHEMA_DIR", os.path.join(os.path.dirname(os.path.dirname(HERE)), "schemas"))

sys.modules.setdefault("kafka", types.SimpleNamespace(KafkaConsumer=None, KafkaProducer=None))
sys.modules.setdefault("psycopg2", types.SimpleNamespace())

import detector


def summary(origin=None, **over):
    s = {"ticket_id": "t", "station_id": "station-grill", "is_complete": True}
    if origin is not None:
        s["origin"] = origin
    s.update(over)
    return s


def test_interactive_is_quarantined_by_default():
    assert detector.is_quarantined(summary("interactive"))


def test_simulated_vendor_and_unlabelled_summaries_are_not():
    assert not detector.is_quarantined(summary("simulated"))
    assert not detector.is_quarantined(summary("vendor_integration"))
    assert not detector.is_quarantined(summary())  # older producers send no origin


def test_the_quarantined_set_is_configurable(monkeypatch):
    monkeypatch.setattr(detector, "QUARANTINE_ORIGINS", {"vendor_integration"})
    assert detector.is_quarantined(summary("vendor_integration"))
    assert not detector.is_quarantined(summary("interactive"))
    monkeypatch.setattr(detector, "QUARANTINE_ORIGINS", set())
    assert not detector.is_quarantined(summary("interactive"))


def test_the_main_loop_never_adds_a_quarantined_ticket_to_a_window(monkeypatch):
    """Drive main() with a fake consumer: a station's window grows only for simulated tickets."""
    tickets = ([summary("simulated", ticket_id=f"s{i}", time_to_cook_start_ms=1000 + i) for i in range(3)]
               + [summary("interactive", ticket_id=f"g{i}", time_to_cook_start_ms=99999) for i in range(5)])
    committed, seen_windows = [], {}

    class Msg:
        def __init__(self, value):
            self.value = value

    class FakeConsumer:
        def __init__(self, *a, **k):
            pass

        def __iter__(self):
            return iter(Msg(t) for t in tickets)

        def commit(self):
            committed.append(1)

    class FakeProducer:
        def __init__(self, *a, **k):
            pass

        def send(self, *a, **k):
            pass

        def flush(self):
            pass

    original_add = detector.StationWindow.add

    def spying_add(self, s):
        seen_windows.setdefault(s["station_id"], []).append(s["ticket_id"])
        original_add(self, s)

    monkeypatch.setattr(detector, "KafkaConsumer", FakeConsumer)
    monkeypatch.setattr(detector, "KafkaProducer", FakeProducer)
    monkeypatch.setattr(detector.common, "pg_connect", lambda: None)
    monkeypatch.setattr(detector.common, "load_schema", lambda name: {})
    monkeypatch.setattr(detector.StationWindow, "add", spying_add)
    detector.main()

    assert seen_windows == {"station-grill": ["s0", "s1", "s2"]}  # no g* ticket ever entered a window
    assert len(committed) == 8                                    # but every ticket's offset was committed
