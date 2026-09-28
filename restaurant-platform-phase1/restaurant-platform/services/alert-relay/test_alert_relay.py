"""
Tests for the Alertmanager-to-ntfy payload reshaping in main.py. No network:
send_to_ntfy is monkeypatched, this only checks build_ntfy_payload's output
and that the HTTP handler calls it.

    pip install pytest
    cd services/alert-relay && NTFY_TOPIC=test python -m pytest test_alert_relay.py
"""
import os

os.environ.setdefault("NTFY_TOPIC", "test-topic")

import main


def test_firing_alert_has_summary_and_priority():
    payload = main.build_ntfy_payload({
        "status": "firing",
        "commonLabels": {"alertname": "KafkaConsumerLagHigh"},
        "alerts": [
            {
                "status": "firing",
                "labels": {"alertname": "KafkaConsumerLagHigh"},
                "annotations": {"summary": "lag is 5000 on group storage-consumer"},
            }
        ],
    })
    assert payload["topic"] == "test-topic"
    assert payload["title"] == "KafkaConsumerLagHigh (firing)"
    assert "lag is 5000 on group storage-consumer" in payload["message"]
    assert payload["priority"] == 4
    assert payload["tags"] == ["rotating_light"]


def test_resolved_alert_gets_lower_priority():
    payload = main.build_ntfy_payload({
        "status": "resolved",
        "commonLabels": {"alertname": "TargetDown"},
        "alerts": [{"status": "resolved", "labels": {"alertname": "TargetDown"}, "annotations": {}}],
    })
    assert payload["priority"] == 3
    assert payload["tags"] == ["white_check_mark"]
    assert "TargetDown" in payload["message"]


def test_multiple_alerts_in_one_group_each_get_a_line():
    payload = main.build_ntfy_payload({
        "status": "firing",
        "commonLabels": {"alertname": "PodCrashLooping"},
        "alerts": [
            {"status": "firing", "labels": {"alertname": "PodCrashLooping"}, "annotations": {"summary": "pod A"}},
            {"status": "firing", "labels": {"alertname": "PodCrashLooping"}, "annotations": {"summary": "pod B"}},
        ],
    })
    assert payload["message"].count("\n") == 1
    assert "pod A" in payload["message"] and "pod B" in payload["message"]


def test_no_alerts_falls_back_to_status_and_alertname():
    payload = main.build_ntfy_payload({"status": "firing", "commonLabels": {"alertname": "X"}, "alerts": []})
    assert payload["message"] == "firing: X"


def test_handler_forwards_body_to_send_to_ntfy(monkeypatch):
    calls = []
    monkeypatch.setattr(main, "send_to_ntfy", lambda payload: calls.append(payload))

    class FakeRequest:
        def makefile(self, *a, **k):
            import io
            return io.BytesIO(b"")

    handler = main.AlertHandler.__new__(main.AlertHandler)
    handler.headers = {"Content-Length": "2"}
    import io
    handler.rfile = io.BytesIO(b"{}")
    handler.wfile = io.BytesIO()
    handler.send_response = lambda code: setattr(handler, "_status", code)
    handler.end_headers = lambda: None

    handler.do_POST()

    assert len(calls) == 1
    assert handler._status == 200
