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


# ------------------------------------------------------------------ TLS on the Compose broker

def _compose():
    return yaml.safe_load((ROOT / "docker-compose.yml").read_text())


BROKER_CLIENTS = ["edge-sim-plate-waste", "edge-sim-pos-transaction", "edge-sim-service-timing", "edge-sim-staff-shift", "mqtt-kafka-bridge", "edge-operator"]


def test_the_compose_broker_listens_for_tls_only():
    conf = (MOSQUITTO / "mosquitto.conf").read_text()
    listeners = re.findall(r"^listener (\d+)\s*$", conf, flags=re.M)
    assert listeners == ["8883"], f"listeners: {listeners}"
    for setting in ("certfile /mosquitto/auth/tls.crt", "keyfile /mosquitto/auth/tls.key"):
        assert re.search(rf"^{re.escape(setting)}\s*$", conf, flags=re.M), setting
    assert not re.search(r"^tls_version", conf, flags=re.M), "tls_version pins one protocol version, and would exclude TLS 1.3"


def test_every_compose_client_of_the_broker_uses_tls_and_trusts_the_certificate_volume():
    services = _compose()["services"]
    for name in BROKER_CLIENTS:
        env = services[name]["environment"]
        assert env["MQTT_PORT"] == "8883" and env["MQTT_TLS_ENABLED"] == "true", name
        assert "mosquitto-tls:/etc/mosquitto-tls:ro" in services[name]["volumes"], f"{name} does not mount the certificate read-only"


def test_only_the_broker_and_the_certificate_generator_can_see_the_private_key():
    services = _compose()["services"]
    holders = sorted(n for n, svc in services.items() if any(str(v).startswith("mosquitto-tls-private:") for v in svc.get("volumes", [])))
    assert holders == ["mosquitto", "mqtt-tls-init"], holders
    broker_mount = next(v for v in services["mosquitto"]["volumes"] if str(v).startswith("mosquitto-tls-private:"))
    assert broker_mount.endswith(":ro")


def test_the_broker_waits_for_its_certificate_to_exist():
    broker = _compose()["services"]["mosquitto"]
    assert broker["depends_on"]["mqtt-tls-init"]["condition"] == "service_completed_successfully"
    init = _compose()["services"]["mqtt-tls-init"]
    assert init["restart"] == "no" and ":" in init["image"].rsplit("/", 1)[-1] and not init["image"].endswith(":latest")


def test_the_entrypoint_copies_the_key_to_a_file_only_the_broker_can_read():
    text = (MOSQUITTO / "mosquitto-auth-entrypoint.sh").read_text()
    assert "cp /tls-private/tls.key /mosquitto/auth/tls.key" in text
    assert re.search(r"chmod 0600 [^\n]*/mosquitto/auth/tls\.key", text)
    assert re.search(r"chown mosquitto:mosquitto [^\n]*/mosquitto/auth/tls\.key", text)


def test_the_certificate_script_is_executable_parses_and_makes_the_names_the_clients_use():
    script = MOSQUITTO / "generate-tls.sh"
    assert os.access(script, os.X_OK)
    assert subprocess.run(["sh", "-n", str(script)], capture_output=True).returncode == 0
    text = script.read_text()
    assert "DNS:mosquitto" in text and "DNS:localhost" in text
    assert re.search(r'chmod 0600 "\$private"', text), "the private key must be private"


@needs_helm
def test_the_chart_simulators_use_tls_by_default_as_the_live_release_does_so_a_plain_upgrade_cannot_send_logins_in_the_clear():
    deployments = [d for d in render("edge-simulators") if d["kind"] == "Deployment"]
    assert len(deployments) == 4
    for deployment in deployments:
        env = {e["name"]: e.get("value") for e in deployment["spec"]["template"]["spec"]["containers"][0]["env"]}
        assert env["MQTT_PORT"] == "8883" and env["MQTT_TLS_ENABLED"] == "true", deployment["metadata"]["name"]


@needs_helm
def test_the_chart_broker_listens_for_tls_only_and_exposes_only_that_port():
    docs = render("mosquitto")
    conf = next(d for d in docs if d["kind"] == "ConfigMap")["data"]["mosquitto.conf"]
    assert re.findall(r"^\s*listener (\d+)\s*$", conf, flags=re.M) == ["8883"]
    service = next(d for d in docs if d["kind"] == "Service")
    assert [p["port"] for p in service["spec"]["ports"]] == [8883] and [p["targetPort"] for p in service["spec"]["ports"]] == [8883]
    deployment = next(d for d in docs if d["kind"] == "Deployment")
    ports = [p["containerPort"] for c in deployment["spec"]["template"]["spec"]["containers"] for p in c.get("ports", [])]
    assert ports == [8883]


@needs_helm
def test_every_live_client_chart_of_the_broker_defaults_to_the_tls_port():
    # With no plaintext listener a client left on 1883 gets nothing, so every chart that talks to the broker must default to 8883.
    for chart in ("edge-simulators", "mqtt-kafka-bridge"):
        for deployment in (d for d in render(chart) if d["kind"] == "Deployment"):
            env = {e["name"]: e.get("value") for e in deployment["spec"]["template"]["spec"]["containers"][0]["env"]}
            assert env.get("MQTT_PORT") == "8883" and env.get("MQTT_TLS_ENABLED") == "true", (chart, deployment["metadata"]["name"])


# ------------------------------------------------------------------ key rotation and rollouts of the simulators (DEF-174)

def _simulator_envs(*sets):
    docs = render("edge-simulators", *sets)
    return {d["metadata"]["name"]: {e["name"]: e for e in d["spec"]["template"]["spec"]["containers"][0]["env"]}
            for d in docs if d["kind"] == "Deployment"}


@needs_helm
def test_the_previous_key_map_gives_that_node_and_only_that_node_its_old_key_from_the_secret_named():
    envs = _simulator_envs("--set", "controlKeyPreviousByNode.sim-plate-cam-01=edge-control-sim-plate-cam-01-previous")
    holders = {name: e["EDGE_CONTROL_KEY_PREVIOUS"]["valueFrom"]["secretKeyRef"] for name, e in envs.items() if "EDGE_CONTROL_KEY_PREVIOUS" in e}
    assert holders == {"edge-sim-plate-waste": {"name": "edge-control-sim-plate-cam-01-previous", "key": "key"}}
    assert envs["edge-sim-plate-waste"]["EDGE_CONTROL_KEY"]["valueFrom"]["secretKeyRef"]["name"] == "edge-control-sim-plate-cam-01", "the new key went"


@needs_helm
def test_an_empty_previous_key_map_entry_closes_the_window_and_by_default_no_node_has_an_old_key():
    for sets in ((), ("--set", "controlKeyPreviousByNode.sim-plate-cam-01=")):
        assert not [n for n, e in _simulator_envs(*sets).items() if "EDGE_CONTROL_KEY_PREVIOUS" in e], f"an old key with {sets}"


@needs_helm
def test_the_rotation_script_closes_the_window_with_the_empty_value_the_chart_treats_as_none():
    text = (ROOT / "k8s" / "audit" / "rotate-master-key.sh").read_text()
    assert '--set "controlKeyPreviousByNode.$NODE=$1"' in text and 'helm_node_previous ""' in text


@needs_helm
def test_a_simulator_is_never_run_twice_at_once_during_a_rollout_because_two_would_take_each_others_broker_session_over():
    docs = [d for d in render("edge-simulators") if d["kind"] == "Deployment"]
    assert len(docs) == 4
    for deployment in docs:
        strategy = deployment["spec"]["strategy"]
        assert strategy["type"] == "RollingUpdate" and strategy["rollingUpdate"] == {"maxSurge": 0, "maxUnavailable": 1}, deployment["metadata"]["name"]
        assert deployment["spec"]["template"]["spec"]["terminationGracePeriodSeconds"] <= 10, "a pod that does not stop would overlap its replacement for too long"
