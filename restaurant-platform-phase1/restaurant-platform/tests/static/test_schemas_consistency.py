"""
Static checks on the JSON Schemas in schemas/ -- the contract every producer
and consumer in the platform agrees on.

    pip install jsonschema pytest
    python -m pytest tests/static/test_schemas_consistency.py -v
"""
import json
import os
import sys

import jsonschema
import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from helpers import ROOT, SCHEMAS  # noqa: E402

SCHEMA_FILES = sorted(SCHEMAS.glob("*.schema.json"))
RAW_EVENT_SCHEMAS = ["PlateWasteEvent", "POSTransactionEvent", "ServiceTimingEvent", "StaffShiftEvent"]
COMMON_REQUIRED = {"event_id", "event_type", "schema_version", "source_id", "timestamp", "restaurant_id"}


def _load(name):
    return json.loads((SCHEMAS / f"{name}.schema.json").read_text())


@pytest.mark.parametrize("path", SCHEMA_FILES, ids=lambda p: p.name)
def test_every_schema_is_itself_a_valid_json_schema(path):
    jsonschema.Draft202012Validator.check_schema(json.loads(path.read_text()))


def test_schema_ids_are_unique_and_match_file_names():
    seen = {}
    for path in SCHEMA_FILES:
        schema = json.loads(path.read_text())
        schema_id = schema["$id"]
        assert schema_id.endswith("/" + path.name), f"{path.name}: $id '{schema_id}' does not end in the file name"
        assert schema_id not in seen, f"{path.name} and {seen[schema_id]} share $id {schema_id}"
        seen[schema_id] = path.name


@pytest.mark.parametrize("name", RAW_EVENT_SCHEMAS)
def test_raw_event_schemas_share_the_common_envelope(name):
    schema = _load(name)
    assert COMMON_REQUIRED <= set(schema["required"]), f"{name} is missing common envelope fields"
    assert schema["properties"]["event_type"].get("const") == name, f"{name}: event_type const must be '{name}'"


@pytest.mark.parametrize("path", SCHEMA_FILES, ids=lambda p: p.name)
def test_schemas_reject_unknown_fields(path):
    # additionalProperties:false is what makes a typo'd field name fail loudly
    # at the producer instead of silently becoming a NULL column downstream.
    assert json.loads(path.read_text()).get("additionalProperties") is False


@pytest.mark.parametrize("name", RAW_EVENT_SCHEMAS)
def test_every_raw_schema_declares_source_kind_with_the_base_values(name):
    kinds = _load(name)["properties"]["source_kind"]["enum"]
    assert {"simulated", "vendor_integration"} <= set(kinds)
    assert _load(name)["properties"]["source_kind"]["default"] == "simulated"


def test_the_chart_copies_of_schemas_are_identical_to_the_source_of_truth():
    # k8s/phase5-schemas packages copies of three schemas into a ConfigMap. A
    # stale copy there was a real bug once (problem log item 40: the copy
    # missing a schema the service needed on its first call).
    copies = sorted((ROOT / "k8s" / "phase5-schemas" / "files").glob("*.schema.json"))
    assert copies, "no schema copies found in k8s/phase5-schemas/files"
    for copy in copies:
        assert copy.read_bytes() == (SCHEMAS / copy.name).read_bytes(), f"{copy.name} has drifted from schemas/{copy.name}"


def test_the_consumer_covers_exactly_the_raw_event_schemas():
    # storage-consumer's topic table must map to a schema file for each raw
    # event type, and to no schema that does not exist.
    import re

    source = (ROOT / "storage" / "consumer" / "consumer.py").read_text()
    mapped = set(re.findall(r'\("(\w+)", "\1\.schema\.json"\)', source))
    assert mapped == set(RAW_EVENT_SCHEMAS), f"consumer maps {mapped}, schemas define {set(RAW_EVENT_SCHEMAS)}"


def test_every_migration_has_an_identical_chart_copy_and_is_registered_everywhere():
    # The SQL migrations exist twice (storage/schema and the Helm chart's files/) and are
    # listed in four more places. A stale copy once caused weeks of confusing column errors
    # (DEF-053), and a migration missing from any one list fails only on the path that list
    # serves (RSK-018). Each migration must be byte-identical in the chart and named in all
    # of them: Compose, the chart's ConfigMap and init Job, the integration harness and
    # the migration test.
    migrations = sorted((ROOT / "storage" / "schema").glob("[0-9][0-9][0-9]_*.sql"))
    assert len(migrations) >= 6, "migrations not found"
    registers = {
        "docker-compose.yml": (ROOT / "docker-compose.yml").read_text(),
        "the chart's schema ConfigMap": (ROOT / "k8s" / "timescaledb" / "templates" / "schema-configmap.yaml").read_text(),
        "the chart's schema-init Job": (ROOT / "k8s" / "timescaledb" / "templates" / "schema-init-job.yaml").read_text(),
        "tests/integration/run_db_tests.sh": (ROOT / "tests" / "integration" / "run_db_tests.sh").read_text(),
        "tests/integration/test_migrations_db.py": (ROOT / "tests" / "integration" / "test_migrations_db.py").read_text(),
    }
    for migration in migrations:
        copy = ROOT / "k8s" / "timescaledb" / "files" / migration.name
        assert copy.exists(), f"{migration.name} has no copy in k8s/timescaledb/files"
        assert copy.read_bytes() == migration.read_bytes(), f"{migration.name} has drifted from its chart copy"
        for where, text in registers.items():
            assert migration.name in text, f"{migration.name} is not registered in {where}"
