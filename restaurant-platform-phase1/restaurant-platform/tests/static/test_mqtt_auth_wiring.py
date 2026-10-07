"""
Static checks that the MQTT broker's login and access rules are wired the way their safety needs (RSK-036). Nothing runs.

The broker's rules live in one file (docker-compose/mosquitto/acl, copied into the Helm chart) and are enforced by Mosquitto, which
enforces them silently: a client whose user, topic or password does not match what the broker expects does not get an error, its
events just vanish. So the facts that must agree across files are checked here, and the broker itself is tested for real in
tests/integration/test_mosquitto_auth.py.

    pip install pyyaml pytest
    python -m pytest tests/static/test_mqtt_auth_wiring.py -v
"""
import hashlib
import hmac
import os
import re
import subprocess
import sys

import pytest
import yaml

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from helpers import ROOT, have  # noqa: E402

needs_helm = pytest.mark.skipif(not have("helm"), reason="helm not installed")
MOSQUITTO = ROOT / "docker-compose" / "mosquitto"
SIMS = ["edge-sim-plate-waste", "edge-sim-pos-transaction", "edge-sim-service-timing", "edge-sim-staff-shift"]
DEV_PASSWORD = "changeme-local-dev-only"


def compose():
    return yaml.safe_load((ROOT / "docker-compose.yml").read_text())["services"]


def parse_acl(text):
    """{user: [(permission, topic), ...]} from a Mosquitto ACL file."""
    rules, user = {}, None
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("user "):
            user = line.split(None, 1)[1]
            rules[user] = []
        elif line.startswith("topic "):
            _, permission, topic = line.split(None, 2)
            assert user is not None, f"a topic rule before any user: {line}"
            rules[user].append((permission, topic))
        else:
            raise AssertionError(f"an ACL line this test does not understand (and Mosquitto may read differently): {line}")
    return rules


ACL = parse_acl((MOSQUITTO / "acl").read_text())


def render(chart, *extra):
    result = subprocess.run(["helm", "template", "t", str(ROOT / "k8s" / chart), "-n", "kafka", *extra], capture_output=True, text=True)
    assert result.returncode == 0, result.stderr
    return [d for d in yaml.safe_load_all(result.stdout) if d]


def settings(conf):
    return {m.group(1): m.group(2).strip() for m in re.finditer(r"^\s*(allow_anonymous|password_file|acl_file|use_username_as_clientid)\s+(\S+)\s*$", conf, flags=re.M)}


# ------------------------------------------------------------------ the broker's configuration

def test_the_compose_broker_refuses_anonymous_clients_and_uses_a_password_file_an_acl_and_the_username_as_client_id():
    found = settings((MOSQUITTO / "mosquitto.conf").read_text())
    assert found == {"allow_anonymous": "false", "password_file": "/mosquitto/auth/passwd", "acl_file": "/mosquitto/auth/acl",
                     "use_username_as_clientid": "true"}


@needs_helm
def test_the_chart_broker_has_the_same_four_settings_by_default():
    conf = next(d for d in render("mosquitto") if d["kind"] == "ConfigMap")["data"]["mosquitto.conf"]
    assert settings(conf) == settings((MOSQUITTO / "mosquitto.conf").read_text())


@needs_helm
def test_anonymous_access_exists_only_when_auth_is_switched_off_for_a_cutover_and_then_nothing_else_is_mounted():
    docs = render("mosquitto", "--set", "auth.enabled=false")
    conf = next(d for d in docs if d["kind"] == "ConfigMap")["data"]
    assert settings(conf["mosquitto.conf"]) == {"allow_anonymous": "true"} and "acl" not in conf
    pod = next(d for d in docs if d["kind"] == "Deployment")["spec"]["template"]["spec"]
    assert "initContainers" not in pod and not [v for v in pod["volumes"] if v["name"] in ("auth", "acl-source", "passwd-source")]


@needs_helm
def test_the_chart_acl_is_the_compose_acl():
    chart = next(d for d in render("mosquitto") if d["kind"] == "ConfigMap")["data"]["acl"]
    assert parse_acl(chart) == ACL, "the Helm chart's ACL and docker-compose/mosquitto/acl have drifted apart"


@needs_helm
def test_the_chart_gives_the_broker_a_private_copy_of_its_password_file_and_acl_owned_by_its_own_user():
    # Mosquitto warns that a future version will refuse a password or ACL file it does not own; a Secret or ConfigMap mount is
    # always owned by root, so an init container copies them into a directory the broker's user owns.
    pod = next(d for d in render("mosquitto") if d["kind"] == "Deployment")["spec"]["template"]["spec"]
    command = " ".join(pod["initContainers"][0]["command"])
    assert "chown mosquitto:mosquitto" in command and "chmod 0600" in command
    assert {"name": "auth", "emptyDir": {"medium": "Memory"}} in pod["volumes"], "the copies must live in memory, not on disk"
    main_mounts = {m["name"]: m for m in pod["containers"][0]["volumeMounts"]}
    assert main_mounts["auth"]["mountPath"] == "/mosquitto/auth" and main_mounts["auth"].get("readOnly")
    assert "passwd-source" not in main_mounts and "acl-source" not in main_mounts, "the broker must read only the copies"


# ------------------------------------------------------------------ what the ACL grants

def test_no_rule_grants_everything_and_only_the_operator_may_publish_control_topics():
    for user, rules in ACL.items():
        for permission, topic in rules:
            assert permission in ("read", "write"), f"{user}: 'readwrite' hides what is granted ({topic})"
            assert topic != "#" and not topic.startswith("$"), f"{user} is granted {topic}"
            if permission == "write" and topic.startswith("control/"):
                assert user == "edge-operator", f"{user} may publish control topics"


def test_each_simulator_may_publish_its_own_sensor_topic_and_no_other_sensor_topic():
    for name in SIMS:
        env = compose()[name]["environment"]
        user, topic = env["MQTT_USERNAME"], env["MQTT_TOPIC"]
        assert user == env["SOURCE_ID"], f"{name}: the broker makes the client id the user name, and the client id is SOURCE_ID"
        writes = {t for p, t in ACL[user] if p == "write" and t.startswith("sensors/")}
        assert writes == {topic}, f"{name} ({user}) may publish {writes}, but publishes {topic}"


def test_only_the_bridge_may_read_sensor_topics_and_it_may_read_all_four_and_publish_nothing():
    readers = {user for user, rules in ACL.items() if any(p == "read" and t.startswith("sensors/") for p, t in rules)}
    assert readers == {"rp-mqtt-kafka-bridge"}
    topics = {t for p, t in ACL["rp-mqtt-kafka-bridge"] if p == "read"}
    assert topics == {compose()[n]["environment"]["MQTT_TOPIC"] for n in SIMS}
    assert not [r for r in ACL["rp-mqtt-kafka-bridge"] if r[0] == "write"]


def test_a_node_may_read_only_its_own_control_topic_and_write_only_its_own_status_topic():
    rules = ACL["sim-plate-cam-01"]
    assert ("read", "control/edge/plate-waste/sim-plate-cam-01") in rules and ("write", "edge/status/plate-waste/sim-plate-cam-01") in rules
    assert not [t for p, t in rules if "+" in t or "#" in t], "a wildcard would reach other nodes"
    plate_source = (ROOT / "edge-simulators" / "simulators" / "plate_waste.py").read_text()
    assert 'CONTROL_TOPIC = "control/edge/plate-waste/{}"' in plate_source and 'STATUS_TOPIC = "edge/status/plate-waste/{}"' in plate_source


def test_the_staffing_level_is_written_by_the_staffing_sensor_alone_and_read_by_the_ticket_timer_alone():
    writers = {u for u, r in ACL.items() if ("write", "sim/world/staffing") in r}
    readers = {u for u, r in ACL.items() if ("read", "sim/world/staffing") in r}
    assert writers == {"sim-staffing-sensor-01"} and readers == {"sim-ticket-timer-01"}


def test_the_acl_names_a_user_for_every_client_and_the_bridges_user_is_its_client_id():
    assert set(ACL) == {"sim-plate-cam-01", "sim-pos-01", "sim-ticket-timer-01", "sim-staffing-sensor-01", "rp-mqtt-kafka-bridge", "edge-operator"}
    assert 'os.environ.get("BRIDGE_CLIENT_ID", "rp-mqtt-kafka-bridge")' in (ROOT / "services" / "mqtt-kafka-bridge" / "bridge.py").read_text()


# ------------------------------------------------------------------ Compose: every client's login matches the broker's

def test_every_compose_client_logs_in_with_the_password_the_broker_is_given_for_that_user():
    services = compose()
    broker_env = services["mosquitto"]["environment"]
    clients = {name: services[name]["environment"] for name in SIMS + ["mqtt-kafka-bridge", "edge-operator"]}
    for name, env in clients.items():
        user = env["MQTT_USERNAME"]
        variable = "MQTT_PASSWORD_" + user.upper().replace("-", "_")
        assert env["MQTT_PASSWORD"] == f"${{{variable}:-{DEV_PASSWORD}}}", f"{name}: its password is not the broker's {variable}"
        assert broker_env[variable] == f"${{{variable}:-{DEV_PASSWORD}}}", f"the broker is not given {variable}"
    assert {env["MQTT_USERNAME"] for env in clients.values()} == set(ACL)


def test_the_broker_entrypoint_builds_the_password_file_for_exactly_the_users_in_the_acl():
    entrypoint = (MOSQUITTO / "mosquitto-auth-entrypoint.sh").read_text()
    users = re.search(r"for user in ([^;]+); do", entrypoint).group(1).split()
    assert sorted(users) == sorted(ACL)


def test_only_the_operator_tool_holds_the_master_secret_and_the_operator_login():
    for name, svc in compose().items():
        env = svc.get("environment", {})
        assert ("EDGE_CONTROL_MASTER_KEY" in env) == (name == "edge-operator"), f"{name} and the master secret"
        if name != "mosquitto":
            assert ("MQTT_USERNAME" in env and env["MQTT_USERNAME"] == "edge-operator") == (name == "edge-operator"), f"{name} and the operator login"


def test_the_default_node_key_is_the_one_derived_for_that_node_from_the_default_master_and_nothing_else_does_the_deriving():
    services = compose()
    master = re.search(r":-([^}]+)\}", services["edge-operator"]["environment"]["EDGE_CONTROL_MASTER_KEY"]).group(1)
    node_key = re.search(r":-([^}]+)\}", services["edge-sim-plate-waste"]["environment"]["EDGE_CONTROL_KEY"]).group(1)
    assert node_key == hmac.new(master.encode(), b"edge-control/v1/sim-plate-cam-01/1", hashlib.sha256).hexdigest()
    for name in SIMS[1:]:
        assert "EDGE_CONTROL_KEY" not in services[name]["environment"]


def test_the_operator_service_is_a_tool_run_on_demand_not_part_of_the_stack():
    operator = compose()["edge-operator"]
    assert operator["profiles"] == ["tools"] and operator["entrypoint"] == ["python", "-m", "control.edge_control"]


# ------------------------------------------------------------------ Kubernetes: the clients carry credentials from Secrets

@needs_helm
def test_each_chart_simulator_logs_in_as_its_source_id_with_a_password_from_its_own_secret_never_a_literal():
    deployments = [d for d in render("edge-simulators") if d["kind"] == "Deployment"]
    assert len(deployments) == 4
    for deployment in deployments:
        env = {e["name"]: e for e in deployment["spec"]["template"]["spec"]["containers"][0]["env"]}
        user = env["SOURCE_ID"]["value"]
        assert env["MQTT_USERNAME"]["value"] == user and user in ACL
        assert env["MQTT_PASSWORD"]["valueFrom"]["secretKeyRef"] == {"name": f"mqtt-{user}", "key": "password"}
        assert "value" not in env["MQTT_PASSWORD"]


@needs_helm
def test_each_chart_simulator_publishes_the_topic_its_user_may_publish():
    for deployment in (d for d in render("edge-simulators") if d["kind"] == "Deployment"):
        env = {e["name"]: e.get("value") for e in deployment["spec"]["template"]["spec"]["containers"][0]["env"]}
        assert ("write", env["MQTT_TOPIC"]) in ACL[env["SOURCE_ID"]]


@needs_helm
def test_with_auth_off_the_clients_carry_no_credentials():
    # Only for the cutover step in which clients are given credentials before the broker requires them.
    for chart, extra in (("edge-simulators", ["--set", "mqtt.authEnabled=false"]), ("mqtt-kafka-bridge", ["--set", "mqtt.authEnabled=false"])):
        for deployment in (d for d in render(chart, *extra) if d["kind"] == "Deployment"):
            names = {e["name"] for e in deployment["spec"]["template"]["spec"]["containers"][0]["env"]}
            assert not names & {"MQTT_USERNAME", "MQTT_PASSWORD"}


@needs_helm
def test_a_node_in_a_key_rotation_holds_both_keys_each_from_its_own_secret():
    values = yaml.safe_load((ROOT / "k8s" / "edge-simulators" / "values.yaml").read_text())
    sims = values["simulators"]
    sims[0]["controlKeySecret"], sims[0]["controlKeyPreviousSecret"] = "edge-control-sim-plate-cam-01", "edge-control-sim-plate-cam-01-previous"
    import tempfile

    with tempfile.NamedTemporaryFile("w", suffix=".yaml", delete=False) as f:
        yaml.safe_dump({"simulators": sims}, f)
    try:
        found = {}
        for deployment in (d for d in render("edge-simulators", "-f", f.name) if d["kind"] == "Deployment"):
            for e in deployment["spec"]["template"]["spec"]["containers"][0]["env"]:
                if e["name"] in ("EDGE_CONTROL_KEY", "EDGE_CONTROL_KEY_PREVIOUS"):
                    found[e["name"]] = e["valueFrom"]["secretKeyRef"]["name"]
    finally:
        os.unlink(f.name)
    assert found == {"EDGE_CONTROL_KEY": "edge-control-sim-plate-cam-01", "EDGE_CONTROL_KEY_PREVIOUS": "edge-control-sim-plate-cam-01-previous"}


# ------------------------------------------------------------------ the cutover order

def test_the_realign_script_cuts_the_broker_over_in_the_order_that_never_locks_a_client_out():
    script = (ROOT / "k8s" / "realign" / "realign-live-cluster.sh").read_text()
    broker_off = script.index('--set auth.enabled="$BROKER_AUTH"')
    provision = script.index("run bash k8s/mosquitto/provision-mqtt-auth.sh")  # the command, not the header comment
    simulators = script.index("helm upgrade --install edge-simulators")
    bridge = script.index("helm upgrade --install mqtt-kafka-bridge")
    broker_on = script.index("--set auth.enabled=true")
    # Credentials exist before any client needs them, every client carries them before the broker requires them.
    assert broker_off < provision < simulators < bridge < broker_on
    assert "BROKER_AUTH=false" in script, "the default for a cluster whose broker is not yet protected is the staged path"


def test_a_rerun_of_the_realign_script_does_not_reopen_a_broker_that_already_requires_a_login():
    script = (ROOT / "k8s" / "realign" / "realign-live-cluster.sh").read_text()
    assert "helm get values mosquitto" in script and 'BROKER_AUTH=true' in script
    assert script.index("helm get values mosquitto") < script.index('--set auth.enabled="$BROKER_AUTH"')
