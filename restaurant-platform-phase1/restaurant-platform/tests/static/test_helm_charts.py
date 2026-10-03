"""
Static checks on every Helm chart under k8s/: lint, render, and a handful of
production-hardening rules applied to the *rendered* manifests.

Rendering (rather than reading values.yaml) matters: the hardening rules have
to hold for what Kubernetes would actually receive, after templating.

Needs the `helm` binary (preinstalled on GitHub's ubuntu runners). No cluster.

    pip install pyyaml pytest
    python -m pytest tests/static/test_helm_charts.py -v
"""
import functools
import os
import subprocess
import sys

import pytest
import yaml

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from helpers import ROOT, have  # noqa: E402

pytestmark = pytest.mark.skipif(not have("helm"), reason="helm not installed")

CHARTS = sorted(p.parent for p in (ROOT / "k8s").glob("*/Chart.yaml"))
WORKLOADS = {"Deployment", "StatefulSet", "DaemonSet", "Job", "CronJob"}

# Charts that wrap an operator-managed custom resource rather than a plain
# workload: their compute limits live inside the CR (Strimzi's `Kafka`,
# `KafkaConnect`), which the generic container scan below cannot see.
OPERATOR_MANAGED = {"kafka-strimzi", "kafka-connect-mqtt"}


def _name(path):
    return path.name


@functools.lru_cache(maxsize=None)
def _render(chart):
    result = subprocess.run(
        ["helm", "template", "test-release", str(chart), "-n", "kafka"], capture_output=True, text=True
    )
    assert result.returncode == 0, f"helm template failed for {chart.name}:\n{result.stderr}"
    return [doc for doc in yaml.safe_load_all(result.stdout) if doc]


def _containers(doc):
    spec = doc.get("spec", {})
    if doc["kind"] == "CronJob":
        spec = spec.get("jobTemplate", {}).get("spec", {})
    pod = spec.get("template", {}).get("spec", {})
    return pod.get("containers", []) + pod.get("initContainers", [])


def test_charts_were_found():
    assert len(CHARTS) >= 15, f"expected the project's ~19 charts, found {len(CHARTS)}"


@pytest.mark.parametrize("chart", CHARTS, ids=_name)
def test_helm_lint_passes(chart):
    result = subprocess.run(["helm", "lint", str(chart)], capture_output=True, text=True)
    assert result.returncode == 0, result.stdout + result.stderr


@pytest.mark.parametrize("chart", CHARTS, ids=_name)
def test_chart_renders_well_formed_kubernetes_objects(chart):
    docs = _render(chart)
    assert docs, f"{chart.name} rendered nothing"
    for doc in docs:
        assert doc.get("apiVersion") and doc.get("kind"), f"{chart.name}: object without apiVersion/kind"
        assert doc.get("metadata", {}).get("name"), f"{chart.name}: {doc.get('kind')} without metadata.name"


@pytest.mark.parametrize("chart", CHARTS, ids=_name)
def test_every_container_has_cpu_and_memory_limits(chart):
    # One unbounded pod can starve everything else on a shared node. The live
    # audit (k8s/audit/audit-live-cluster.sh) checks the deployed cluster; this
    # catches it before it is ever deployed.
    if chart.name in OPERATOR_MANAGED:
        pytest.skip("limits live inside an operator CR; covered by the live audit")
    for doc in _render(chart):
        if doc["kind"] not in WORKLOADS:
            continue
        for container in _containers(doc):
            limits = (container.get("resources") or {}).get("limits") or {}
            assert {"cpu", "memory"} <= set(limits), (
                f"{chart.name}: {doc['kind']}/{doc['metadata']['name']} container '{container['name']}' "
                f"has no cpu+memory limits"
            )


@pytest.mark.parametrize("chart", CHARTS, ids=_name)
def test_no_container_image_is_unpinned(chart):
    for doc in _render(chart):
        if doc["kind"] not in WORKLOADS:
            continue
        for container in _containers(doc):
            image = container["image"]
            tag = image.rsplit("/", 1)[-1]
            assert ":" in tag and not tag.endswith(":latest"), (
                f"{chart.name}: container '{container['name']}' uses unpinned image '{image}'"
            )


@pytest.mark.parametrize("chart", CHARTS, ids=_name)
def test_no_plaintext_secret_with_a_real_looking_value(chart):
    # Secrets in charts may only hold the documented placeholders that
    # k8s/harden/harden-live-cluster.sh rotates at deploy time.
    ok = ("REPLACE-AT-DEPLOY-TIME", "changeme-local-dev-only")
    secret_like = ("password", "passwd", "secret", "token", "api-key", "apikey")
    for doc in _render(chart):
        if doc["kind"] != "Secret":
            continue
        for key, value in (doc.get("stringData") or {}).items():
            if not any(word in key.lower() for word in secret_like):
                continue  # a username is an identifier, not a credential
            assert any(marker in str(value) for marker in ok) or not value, (
                f"{chart.name}: Secret/{doc['metadata']['name']} key '{key}' holds a non-placeholder value"
            )
