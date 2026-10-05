"""
Runs docker-compose/kafka-connect/register-connectors.sh for real against a fake `curl`.

The script is the Compose path's replacement for the KafkaConnector resources the
operator manages on Kubernetes. It used to POST each connector, which fails for one
that already exists, and then exit 0 whether that was why or the registration had
genuinely failed. So a changed config never reached an existing stack, and a failed
registration looked like success. These tests execute the script with a stand-in
`curl` that records every call and can be told to fail.

    pip install pytest
    python -m pytest tests/static/test_register_connectors.py -v
"""
import json
import os
import stat
import subprocess
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from helpers import ROOT, have  # noqa: E402

pytestmark = pytest.mark.skipif(not have("sh"), reason="no POSIX shell")

SCRIPT = ROOT / "docker-compose" / "kafka-connect" / "register-connectors.sh"
CONNECTORS = ROOT / "docker-compose" / "kafka-connect" / "connectors"

FAKE_CURL = """#!/bin/sh
method=GET; data=""; url=""
while [ $# -gt 0 ]; do
  case "$1" in
    -X) method="$2"; shift 2;;
    --data) data="$2"; shift 2;;
    -H) shift 2;;
    -*) shift;;
    *) url="$1"; shift;;
  esac
done
echo "$method $url" >> "$CALLS"
if [ -n "$data" ]; then printf '%s' "$data" > "$BODIES/$(basename "$(dirname "$url")")"; fi
count_file="$WORK/count.$method"
n=$(cat "$count_file" 2>/dev/null || echo 0); n=$((n + 1)); echo "$n" > "$count_file"
if [ "$method" = PUT ] && [ "$n" -le "${FAIL_PUTS:-0}" ]; then exit 22; fi
if [ "$method" = GET ] && [ "$n" -le "${FAIL_GETS:-0}" ]; then exit 7; fi
exit 0
"""


def run_script(tmp_path, **env):
    shim = tmp_path / "bin"
    shim.mkdir()
    curl = shim / "curl"
    curl.write_text(FAKE_CURL)
    curl.chmod(curl.stat().st_mode | stat.S_IEXEC)
    (tmp_path / "bodies").mkdir()
    calls = tmp_path / "calls"
    calls.touch()
    result = subprocess.run(
        ["sh", str(SCRIPT)], capture_output=True, text=True, timeout=60,
        env={**os.environ, "PATH": f"{shim}:{os.environ['PATH']}", "WORK": str(tmp_path), "CALLS": str(calls),
             "BODIES": str(tmp_path / "bodies"), "CONNECTORS_DIR": str(CONNECTORS), "RETRY_DELAY": "0", **env},
    )
    return result, calls.read_text().splitlines()


def expected_connectors():
    return {p.stem: json.loads(p.read_text())["config"] for p in CONNECTORS.glob("*.json")}


def test_the_four_connector_files_were_found():
    assert len(expected_connectors()) == 4


def test_each_connector_is_put_to_its_config_endpoint_with_exactly_its_config(tmp_path):
    result, calls = run_script(tmp_path)
    assert result.returncode == 0, result.stderr
    puts = [c for c in calls if c.startswith("PUT ")]
    assert sorted(puts) == sorted(f"PUT http://kafka-connect:8083/connectors/{name}/config" for name in expected_connectors())
    for name, config in expected_connectors().items():
        assert json.loads((tmp_path / "bodies" / name).read_text()) == config, f"{name}: the body was not the file's config object"


def test_it_never_uses_post_which_cannot_update_a_connector_that_already_exists(tmp_path):
    _, calls = run_script(tmp_path)
    assert not [c for c in calls if c.startswith("POST ")]


def test_a_transient_failure_is_retried_until_it_works(tmp_path):
    # Connect answers 409 or 500 to a write while it is rebalancing.
    result, calls = run_script(tmp_path, FAIL_PUTS="3")
    assert result.returncode == 0, result.stderr
    assert len([c for c in calls if c.startswith("PUT ")]) == 4 + 3


def test_a_registration_that_never_succeeds_fails_the_script_instead_of_looking_like_success(tmp_path):
    result, _ = run_script(tmp_path, FAIL_PUTS="1000", ATTEMPTS="3")
    assert result.returncode != 0
    assert "FAILED to register" in result.stderr


def test_it_waits_for_the_rest_api_before_registering_anything(tmp_path):
    result, calls = run_script(tmp_path, FAIL_GETS="3")
    assert result.returncode == 0, result.stderr
    first_put = next(i for i, c in enumerate(calls) if c.startswith("PUT "))
    assert len([c for c in calls[:first_put] if c.startswith("GET ")]) == 4
