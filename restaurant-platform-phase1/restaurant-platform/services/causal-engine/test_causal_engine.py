"""
Tests for the causal engine's control flow and contracts, without DoWhy,
Kafka, or a database (all faked or stubbed). The statistical correctness of
the estimates themselves is tested separately in
tests/statistical/test_causal_ground_truth.py, which does need DoWhy.

The most important thing tested here is the narrative gate: a finding may only
reach the LLM narrator if its refutation test passed. That rule is the
platform's main defence against narrating a spurious correlation.

    pip install pandas jsonschema pytest prometheus-client==0.26.0
    cd services/causal-engine && python -m pytest test_causal_engine.py -v
"""
import json
import os
import sys
import types

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(HERE))
sys.path.insert(0, HERE)
SCHEMAS = os.path.join(os.path.dirname(os.path.dirname(HERE)), "schemas")
os.environ.setdefault("SCHEMA_DIR", SCHEMAS)

sys.modules.setdefault("kafka", types.SimpleNamespace(KafkaConsumer=None, KafkaProducer=None))
sys.modules.setdefault("psycopg2", types.SimpleNamespace())

import jsonschema
import pytest
from prometheus_client import REGISTRY

import causal_engine
import phase5_common as common

FINDING_SCHEMA = json.load(open(os.path.join(SCHEMAS, "CausalFinding.schema.json")))


# ------------------------------------------------------------ the treatment map

@pytest.mark.parametrize("metric", sorted(causal_engine.TREATMENT_MAP))
def test_every_treatment_spec_is_complete_and_its_query_selects_what_it_names(metric):
    spec = causal_engine.TREATMENT_MAP[metric]
    for key in ("treatment", "outcome", "confounders", "query", "effect_unit"):
        assert key in spec, f"{metric} spec is missing '{key}'"
    assert spec["outcome"] == metric, "the outcome variable must be the metric that raised the anomaly"
    for column in [spec["treatment"], spec["outcome"], *spec["confounders"]]:
        # A renamed database column once silently produced all-NULL confounders
        # (problem log item 46). Every named variable must appear in the SQL.
        assert column in spec["query"], f"{metric}: '{column}' is not selected by the query"


def test_human_driven_sessions_are_excluded_from_every_analysis_query():
    # Interactive (player/crew) data runs at a different pace than the
    # simulators; mixing it in would confound every estimate.
    assert "source_kind <> 'player'" in causal_engine.TREATMENT_MAP["estimated_waste_grams"]["query"]
    assert "origin <> 'interactive'" in causal_engine.TREATMENT_MAP["pickup_delay_ms"]["query"]
    assert "source_kind <> 'player'" in causal_engine.TREATMENT_MAP["pickup_delay_ms"]["query"]


# ------------------------------------------------------------ building findings

@pytest.mark.parametrize("refutation", [True, False, None])
@pytest.mark.parametrize("metric", sorted(causal_engine.TREATMENT_MAP))
def test_built_findings_satisfy_the_published_contract(metric, refutation):
    spec = causal_engine.TREATMENT_MAP[metric]
    result = {"effect_estimate": -93.2, "confidence_interval": None, "method": "backdoor.linear_regression", "refutation_passed": refutation}
    finding = causal_engine._build_finding(spec, result, "rest-001", "anomaly-1", None)
    jsonschema.validate(finding, FINDING_SCHEMA)


def test_a_new_finding_is_never_narrative_ready():
    # The gate: readiness is decided later, by the reviewer, never here.
    spec = causal_engine.TREATMENT_MAP["pickup_delay_ms"]
    result = {"effect_estimate": 74000.0, "confidence_interval": None, "method": "m", "refutation_passed": True}
    assert causal_engine._build_finding(spec, result, "rest-001", None, None)["narrative_ready"] is False


def test_the_scenario_injection_id_is_carried_so_a_finding_can_be_checked_against_ground_truth():
    spec = causal_engine.TREATMENT_MAP["pickup_delay_ms"]
    result = {"effect_estimate": 1.0, "confidence_interval": None, "method": "m", "refutation_passed": True}
    finding = causal_engine._build_finding(spec, result, "rest-001", None, "scenario-123")
    assert finding["scenario_injection_id"] == "scenario-123"


def test_finding_ids_are_unique():
    spec = causal_engine.TREATMENT_MAP["pickup_delay_ms"]
    result = {"effect_estimate": 1.0, "confidence_interval": None, "method": "m", "refutation_passed": True}
    ids = {causal_engine._build_finding(spec, result, "rest-001", None, None)["finding_id"] for _ in range(50)}
    assert len(ids) == 50


# ------------------------------------------------------------ skipping unknown anomalies

def _counter(name, **labels):
    return REGISTRY.get_sample_value(name, labels) or 0.0


def test_an_anomaly_on_an_unmapped_metric_is_skipped_and_counted_not_crashed_on():
    before = _counter("causal_engine_anomalies_skipped_total", reason="no_treatment_map_entry")
    result = causal_engine.process_anomaly(None, None, FINDING_SCHEMA, {"metric_name": "joint:a+b+c+d"})
    assert result is None
    assert _counter("causal_engine_anomalies_skipped_total", reason="no_treatment_map_entry") == before + 1


# ------------------------------------------------------------ the reviewer gate

class FakeMessage:
    def __init__(self, value):
        self.value = value


class FakeConsumer:
    def __init__(self, messages):
        self.messages, self.commits = messages, 0

    def __iter__(self):
        return iter(self.messages)

    def commit(self):
        self.commits += 1


class FakeProducer:
    def __init__(self):
        self.sent = []

    def send(self, topic, value):
        self.sent.append((topic, value))

    def flush(self):
        pass


class FakeCursor:
    def __init__(self, log):
        self.log = log

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, sql, params=None):
        self.log.append((sql, params))


class FakeConn:
    def __init__(self):
        self.statements, self.commits, self.rollbacks = [], 0, 0

    def cursor(self):
        return FakeCursor(self.statements)

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1


def run_reviewer_over(monkeypatch, findings):
    consumer = FakeConsumer([FakeMessage(f) for f in findings])
    producer, conn = FakeProducer(), FakeConn()
    monkeypatch.setattr(causal_engine, "KafkaConsumer", lambda *a, **k: consumer)
    monkeypatch.setattr(causal_engine, "KafkaProducer", lambda *a, **k: producer)
    monkeypatch.setattr(common, "pg_connect", lambda: conn)
    monkeypatch.setattr(common, "start_metrics_server", lambda port: None)
    causal_engine.run_reviewer()
    return consumer, producer, conn


def test_reviewer_passes_only_findings_that_survived_refutation(monkeypatch):
    findings = [
        {"finding_id": "pass", "restaurant_id": "rest-001", "refutation_passed": True},
        {"finding_id": "fail", "restaurant_id": "rest-001", "refutation_passed": False},
        {"finding_id": "untested", "restaurant_id": "rest-001", "refutation_passed": None},
        {"finding_id": "missing", "restaurant_id": "rest-001"},
    ]
    consumer, producer, conn = run_reviewer_over(monkeypatch, findings)

    updated = [params["finding_id"] for _, params in conn.statements]
    assert updated == ["pass"], "only the refutation-passed finding may be flipped to narrative_ready"
    assert [(t, v["finding_id"]) for t, v in producer.sent] == [(causal_engine.NARRATION_TOPIC, "pass")]
    assert consumer.commits == len(findings), "every message is committed, including the ones left un-narrated"


def test_reviewer_does_not_commit_an_offset_when_the_database_write_fails(monkeypatch):
    # If the UPDATE fails the message must be redelivered, not silently lost.
    class ExplodingCursor(FakeCursor):
        def execute(self, sql, params=None):
            raise RuntimeError("database unavailable")

    class ExplodingConn(FakeConn):
        def cursor(self):
            return ExplodingCursor(self.statements)

    consumer = FakeConsumer([FakeMessage({"finding_id": "x", "restaurant_id": "r", "refutation_passed": True})])
    conn = ExplodingConn()
    monkeypatch.setattr(causal_engine, "KafkaConsumer", lambda *a, **k: consumer)
    monkeypatch.setattr(causal_engine, "KafkaProducer", lambda *a, **k: FakeProducer())
    monkeypatch.setattr(common, "pg_connect", lambda: conn)
    monkeypatch.setattr(common, "start_metrics_server", lambda port: None)
    with pytest.raises(RuntimeError):
        causal_engine.run_reviewer()
    assert consumer.commits == 0
    assert conn.rollbacks == 1
