"""
Validates that every producer's real event-construction code actually
conforms to its own committed schema -- catches producer/schema drift
automatically, the class of bug this project has otherwise only caught by
hand (see the implementation-status doc's problem log). Each check calls
the real function a producer uses to build its event, not a hand-written
example dict that could itself drift out of sync with the real code.

TicketTimingSummary is deliberately not covered here --
services/ticket-timing-aggregator/test_origin.py already validates it
against the real schema, and duplicating that would just be two places to
keep in sync instead of one.

No Kafka, MQTT, or database: kafka/psycopg2 are stubbed the same way every
other test file in this project stubs them, since every event-construction
function tested here is a pure function of its inputs.

    pip install jsonschema pandas pytest
    cd schemas && python -m pytest test_producer_schema_compatibility.py -v
"""
import json
import os
import sys
import types

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
os.environ.setdefault("SCHEMA_DIR", HERE)

sys.modules.setdefault("kafka", types.SimpleNamespace(KafkaConsumer=None, KafkaProducer=None))
sys.modules.setdefault("psycopg2", types.SimpleNamespace())
# edge-simulators/common/runtime.py imports paho.mqtt.client at module level
# for its Simulator class (never constructed here -- generate_event() is a
# pure function none of these tests need MQTT for), so all three levels
# need stubbing for the import itself to succeed.
_paho = types.ModuleType("paho")
_paho_mqtt = types.ModuleType("paho.mqtt")
_paho_mqtt_client = types.ModuleType("paho.mqtt.client")
_paho_mqtt_client.Client = None
_paho_mqtt.client = _paho_mqtt_client
_paho.mqtt = _paho_mqtt
sys.modules.setdefault("paho", _paho)
sys.modules.setdefault("paho.mqtt", _paho_mqtt)
sys.modules.setdefault("paho.mqtt.client", _paho_mqtt_client)

import jsonschema


def _validate(event: dict, schema_filename: str) -> None:
    with open(os.path.join(HERE, schema_filename)) as f:
        schema = json.load(f)
    jsonschema.validate(instance=event, schema=schema)


# ---------------------------------------------------------------------
# edge-simulators: all four import `common` as a top-level package, so
# edge-simulators/ itself (not simulators/) goes on sys.path; simulators/
# has its own __init__.py, so `from simulators import ...` works from there.
# ---------------------------------------------------------------------
sys.path.insert(0, os.path.join(ROOT, "edge-simulators"))
from simulators import plate_waste, pos_transaction, service_timing, staff_shift  # noqa: E402


def test_plate_waste_event_matches_schema():
    _validate(plate_waste.generate_event(), "PlateWasteEvent.schema.json")


def test_pos_transaction_event_matches_schema():
    _validate(pos_transaction.generate_event(), "POSTransactionEvent.schema.json")


def test_service_timing_event_matches_schema():
    # A fresh ticket (stage_index 0, "order_fired") -- next_event() on a new
    # TicketLifecycle always starts one, since there is nothing open yet.
    lifecycle = service_timing.TicketLifecycle()
    _validate(lifecycle.next_event(), "ServiceTimingEvent.schema.json")


def test_staff_shift_event_matches_schema():
    # A fresh ShiftState has nobody clocked in, so next_event() always
    # produces a clock_in -- still a real, schema-shaped event.
    state = staff_shift.ShiftState()
    _validate(state.next_event(), "StaffShiftEvent.schema.json")


# ---------------------------------------------------------------------
# anomaly-detector
# ---------------------------------------------------------------------
sys.path.insert(0, os.path.join(ROOT, "services", "anomaly-detector"))
import detector  # noqa: E402

_ANOMALY_SUMMARY = {
    "restaurant_id": "rest-001",
    "station_id": "station-grill",
    "table_id": "table-01",
    "ticket_id": "t-1",
    "order_time": "2026-09-30T12:00:00Z",
    "computed_at": "2026-09-30T12:05:00Z",
    "total_ticket_duration_ms": 900000,
}


def test_control_limit_anomaly_event_matches_schema():
    event = detector.build_control_limit_event(
        "pickup_delay_ms", _ANOMALY_SUMMARY, 500.0, (100.0, 400.0)
    )
    _validate(event, "AnomalyEvent.schema.json")


def test_isolation_forest_anomaly_event_matches_schema():
    event = detector.build_isolation_forest_event(_ANOMALY_SUMMARY, -0.3)
    _validate(event, "AnomalyEvent.schema.json")


# ---------------------------------------------------------------------
# causal-engine -- _build_finding is a pure function of already-computed
# spec/result dicts (the actual DoWhy call is a lazy import inside a
# different function), so this needs pandas/jsonschema only, not the full
# dowhy/statsmodels/scipy/networkx stack _run_dowhy would need.
# ---------------------------------------------------------------------
sys.path.insert(0, os.path.join(ROOT, "services", "causal-engine"))
import causal_engine  # noqa: E402

_FINDING_SPEC = {
    "treatment": "to_go_container_used",
    "outcome": "estimated_waste_grams",
    "confounders": ["portion_size_variant", "declared_dietary_restriction"],
    "effect_unit": "grams",
}
_FINDING_RESULT = {
    "effect_estimate": -93.26,
    "confidence_interval": None,
    "method": "backdoor.linear_regression",
    "refutation_passed": True,
}


def test_causal_finding_matches_schema():
    finding = causal_engine._build_finding(
        _FINDING_SPEC, _FINDING_RESULT, "rest-001", triggering_anomaly_id=None, scenario_injection_id=None
    )
    _validate(finding, "CausalFinding.schema.json")
