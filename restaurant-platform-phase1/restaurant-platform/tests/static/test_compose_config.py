"""
Static checks on docker-compose.yml: no containers, no network.

Every rule here exists because the portable path broke in that exact way once
(see the portability row of restaurant-platform-implementation-status.md):
unqualified image names failing under Podman, no restart policy leaving a
crashed service down forever, a healthcheck too tight to be useful. Checking
them mechanically means they cannot silently come back.

    pip install pyyaml pytest
    python -m pytest tests/static/test_compose_config.py -v
"""
import re
import shutil
import subprocess
import sys
import os
from urllib.parse import urlparse

import pytest
import yaml

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from helpers import COMPOSE_FILE, ROOT  # noqa: E402

COMPOSE = yaml.safe_load(COMPOSE_FILE.read_text())
SERVICES = COMPOSE["services"]

# Run-once helpers: they pull a model, then exit.
ONE_SHOT = {"ollama-init"}
# Images deliberately not pinned to an exact tag. ollama publishes no stable
# tag series for the CPU image this project uses; the model itself is pinned
# separately (OLLAMA_MODEL).
UNPINNED_ALLOWED = {"ollama"}


def test_every_service_has_exactly_one_of_image_or_build():
    for name, svc in SERVICES.items():
        assert ("image" in svc) != ("build" in svc), f"{name} must have exactly one of image/build"


def test_every_image_is_fully_qualified():
    # Podman has no unqualified-search registries configured by default, so
    # `image: nginx` fails to resolve there. Every reference must name its registry.
    for name, svc in SERVICES.items():
        image = svc.get("image")
        if image is None:
            continue
        first = image.split("/")[0]
        assert "." in first or ":" in first or first == "localhost", (
            f"{name}: image '{image}' has no registry prefix (e.g. docker.io/library/...)"
        )


def test_dockerfile_base_images_are_fully_qualified():
    for dockerfile in ROOT.rglob("Dockerfile"):
        if "node_modules" in dockerfile.parts:
            continue
        stages = set()
        for line in dockerfile.read_text().splitlines():
            match = re.match(r"^FROM\s+(\S+)(?:\s+AS\s+(\S+))?", line, re.IGNORECASE)
            if not match:
                continue
            base, stage = match.group(1), match.group(2)
            if stage:
                stages.add(stage.lower())
            if base.lower() == "scratch" or base.lower() in stages:
                continue
            first = base.split("/")[0]
            assert "." in first or ":" in first, f"{dockerfile.relative_to(ROOT)}: '{base}' is unqualified"


def test_external_images_are_pinned_to_a_tag():
    for name, svc in SERVICES.items():
        image = svc.get("image")
        if image is None or name in UNPINNED_ALLOWED:
            continue
        tag = image.rsplit("/", 1)[-1]
        assert ":" in tag and not tag.endswith(":latest"), f"{name}: '{image}' is not pinned to a version"


def test_every_build_context_and_dockerfile_exists():
    for name, svc in SERVICES.items():
        build = svc.get("build")
        if build is None:
            continue
        context = ROOT / build["context"]
        assert context.is_dir(), f"{name}: build context {context} does not exist"
        dockerfile = context / build.get("dockerfile", "Dockerfile")
        assert dockerfile.is_file(), f"{name}: {dockerfile} does not exist"


def test_every_dockerfile_copy_source_exists():
    # The class of bug where a Dockerfile COPYs a file nobody committed (the
    # missing TicketTimingSummary schema, the missing phase5-schemas chart).
    for name, svc in SERVICES.items():
        build = svc.get("build")
        if build is None:
            continue
        context = ROOT / build["context"]
        dockerfile = context / build.get("dockerfile", "Dockerfile")
        for line in dockerfile.read_text().splitlines():
            match = re.match(r"^COPY\s+(?!--from)(.+)$", line.strip(), re.IGNORECASE)
            if not match:
                continue
            parts = [p for p in match.group(1).split() if not p.startswith("--")]
            for source in parts[:-1]:
                if any(ch in source for ch in "*?["):
                    continue  # glob (package-lock.json* etc.) may legitimately match nothing
                assert (context / source).exists(), f"{name}: COPY source '{source}' not found under {context}"


def test_every_long_running_service_restarts_on_failure():
    # With no policy a transient crash (a Kafka rebalance timeout under load)
    # leaves the service down forever, unlike a k8s Deployment's self-healing.
    for name, svc in SERVICES.items():
        if name in ONE_SHOT:
            assert str(svc.get("restart")) == "no", f"{name} is run-once and must be restart: 'no'"
        else:
            assert svc.get("restart") == "unless-stopped", f"{name} needs restart: unless-stopped"


def test_depends_on_targets_exist_and_have_no_cycles():
    graph = {}
    for name, svc in SERVICES.items():
        deps = svc.get("depends_on", [])
        graph[name] = list(deps) if isinstance(deps, (list, dict)) else []
        for dep in graph[name]:
            assert dep in SERVICES, f"{name} depends on unknown service '{dep}'"
    visiting, done = set(), set()

    def visit(node):
        assert node not in visiting, f"dependency cycle through {node}"
        if node in done:
            return
        visiting.add(node)
        for dep in graph[node]:
            visit(dep)
        visiting.discard(node)
        done.add(node)

    for node in graph:
        visit(node)


def test_services_waiting_on_health_depend_on_services_that_define_one():
    for name, svc in SERVICES.items():
        deps = svc.get("depends_on", {})
        if not isinstance(deps, dict):
            continue
        for dep, cfg in deps.items():
            if isinstance(cfg, dict) and cfg.get("condition") == "service_healthy":
                assert "healthcheck" in SERVICES[dep], f"{name} waits for {dep} to be healthy but {dep} has no healthcheck"


def test_every_bind_mount_source_exists():
    for name, svc in SERVICES.items():
        for volume in svc.get("volumes", []):
            source = str(volume).split(":")[0]
            if source.startswith("./"):
                assert (ROOT / source).exists(), f"{name}: bind mount source {source} does not exist"


def test_named_volumes_are_declared_and_used():
    declared = set(COMPOSE.get("volumes", {}))
    used = {
        str(v).split(":")[0]
        for svc in SERVICES.values()
        for v in svc.get("volumes", [])
        if not str(v).startswith((".", "/"))
    }
    assert used <= declared, f"volumes used but not declared: {used - declared}"
    assert declared <= used, f"volumes declared but never mounted: {declared - used}"


def _env(svc) -> dict:
    env = svc.get("environment", {})
    if isinstance(env, list):
        env = dict(item.split("=", 1) for item in env)
    return {k: str(v) for k, v in env.items()}


def test_connection_targets_resolve_to_compose_services():
    # A typo'd hostname only shows up as a retry loop at runtime. Resolve every
    # host a service is configured to dial against the service list instead.
    for name, svc in SERVICES.items():
        for key, value in _env(svc).items():
            hosts = []
            if key in ("KAFKA_BOOTSTRAP_SERVERS",):
                hosts = [value.split(":")[0]]
            elif key in ("PGHOST", "MQTT_HOST"):
                hosts = [value]
            elif key in ("OLLAMA_HOST",):
                hosts = [urlparse(value).hostname]
            elif key == "TIMESCALE_DSN":
                hosts = [re.search(r"@([^:/]+)", value).group(1)]
            for host in hosts:
                assert host in SERVICES, f"{name}: {key} points at '{host}', which is not a compose service"


def test_no_credential_default_other_than_the_documented_placeholder():
    # Compose passwords may only default to the one documented, obviously
    # fake value. Anything else is a real secret about to be committed.
    pattern = re.compile(r"\$\{(\w*(?:PASSWORD|API_KEY|SECRET|TOKEN)\w*):-([^}]*)\}")
    for match in pattern.finditer(COMPOSE_FILE.read_text()):
        assert match.group(2) == "changeme-local-dev-only", (
            f"{match.group(1)} defaults to '{match.group(2)}', not the documented placeholder"
        )


def test_only_the_dashboard_is_published_to_the_host():
    # Everything else is reachable only inside the Compose network. Publishing
    # Kafka/Postgres/Ollama to the host would widen the attack surface for no reason.
    published = {name for name, svc in SERVICES.items() if svc.get("ports")}
    assert published == {"dashboard-web"}, f"unexpected published ports on: {published - {'dashboard-web'}}"


@pytest.mark.skipif(shutil.which("docker") is None, reason="docker not installed")
def test_docker_compose_itself_accepts_the_file():
    result = subprocess.run(
        ["docker", "compose", "-f", str(COMPOSE_FILE), "config", "-q"], capture_output=True, text=True, cwd=ROOT
    )
    assert result.returncode == 0, result.stderr


def test_mosquitto_persists_the_bridges_session_and_queue_across_its_own_restart():
    # The bridge acknowledges a message only after Kafka has it, so Mosquitto holds everything not yet
    # safely in Kafka. If Mosquitto's own restart discarded that, the guarantee would stop at the first
    # restart (DEF-152). It needs the setting, a place to write, and a queue big enough for an outage.
    import re

    conf = (ROOT / "docker-compose" / "mosquitto" / "mosquitto.conf").read_text()
    assert re.search(r"^persistence true\s*$", conf, flags=re.M)
    assert re.search(r"^persistence_location /mosquitto/data/\s*$", conf, flags=re.M)
    assert int(re.search(r"^max_queued_messages (\d+)\s*$", conf, flags=re.M).group(1)) >= 100_000
    assert "mosquitto-data:/mosquitto/data" in SERVICES["mosquitto"]["volumes"]
    assert "mosquitto-data" in COMPOSE["volumes"]


def test_the_bridge_waits_for_kafka_and_the_simulators_wait_for_the_bridge():
    # The bridge's persistent session must exist before the first event is published, or that event has
    # no subscriber; and the bridge must not start before Kafka can take what it forwards.
    bridge = SERVICES["mqtt-kafka-bridge"]
    assert bridge["depends_on"]["kafka"]["condition"] == "service_healthy"
    assert "healthcheck" in bridge and bridge["restart"] == "unless-stopped"
    for name, svc in SERVICES.items():
        if name.startswith("edge-sim-"):
            assert svc["depends_on"]["mqtt-kafka-bridge"]["condition"] == "service_healthy", name


def test_the_bridge_carries_exactly_the_topics_the_simulators_publish():
    import re

    source = (ROOT / "services" / "mqtt-kafka-bridge" / "bridge.py").read_text()
    default = re.search(r'DEFAULT_ROUTES = \((.*?)\n\)', source, flags=re.S).group(1)
    routes = dict(re.findall(r'(sensors/[\w-]+)=([\w-]+)', default))
    published = {svc["environment"]["MQTT_TOPIC"] for name, svc in SERVICES.items() if name.startswith("edge-sim-")}
    assert set(routes) == published and len(published) == 4
    assert set(routes.values()) == {"plate-waste-events", "pos-transaction-events", "service-timing-events", "staff-shift-events"}
