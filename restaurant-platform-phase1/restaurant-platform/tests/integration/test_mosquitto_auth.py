"""
The MQTT broker's login and access rules, against a real Mosquitto started with the repository's own configuration.

The broker is the one place every sensor event, every model command and every node status passes through, and until
2026-10-06 it let anyone connect and do anything. These tests start the real image with docker-compose/mosquitto's
config, ACL and password-building entrypoint, and check from outside, as a client would, that:

  * a client with no login, or the wrong password, is refused;
  * each simulator can publish its own sensor topic and nothing else, and cannot read anything it should not;
  * the bridge can read the four sensor topics and nothing else, and publish nothing;
  * only the operator can send a node a command, and no node can read another's;
  * a client cannot take over another's persistent session by claiming its client id;
  * the broker speaks TLS only (certificate made by generate-tls.sh), and a client that does not trust its certificate cannot connect.

Mosquitto enforces its rules silently (verified on 2.1.2, and pinned below): a subscription it refuses is *granted* and then
delivers nothing, and a publish it refuses is acknowledged as a success in MQTT 3.1.1 (MQTT 5 says "not authorized") and
dropped. So "cannot read" and "cannot publish" are shown the only reliable way: by delivery. A reader that is allowed to
see the topic never receives what a forbidden publisher sent, while a legitimate publish sent just after does; and a
reader that is forbidden never receives what a legitimate publisher sent.

    pip install pytest paho-mqtt==2.1.0
    python -m pytest tests/integration/test_mosquitto_auth.py -v        # needs docker (or podman's docker shim)
"""
import os
import shutil
import subprocess
import threading
import time
import uuid

import pytest

pytest.importorskip("paho.mqtt.client")
import paho.mqtt.client as mqtt  # noqa: E402

pytestmark = [pytest.mark.no_db, pytest.mark.skipif(shutil.which("docker") is None, reason="docker is not installed")]

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(os.path.dirname(HERE))
MOSQUITTO_DIR = os.path.join(ROOT, "docker-compose", "mosquitto")
IMAGE = "docker.io/library/eclipse-mosquitto:2"
OPENSSL_IMAGE = "docker.io/alpine/openssl:3.5.9"  # what the stack's mqtt-tls-init runs
PASSWORDS = {  # what the entrypoint is given: MQTT_PASSWORD_<USER>
    "sim-plate-cam-01": "pw-plate", "sim-pos-01": "pw-pos", "sim-ticket-timer-01": "pw-timer",
    "sim-staffing-sensor-01": "pw-staff", "rp-mqtt-kafka-bridge": "pw-bridge", "edge-operator": "pw-operator",
}
SENSOR_TOPICS = ["sensors/plate-waste", "sensors/pos-transaction", "sensors/service-timing", "sensors/staff-shift"]
SILENCE = 1.5  # seconds to wait before concluding a message was not delivered
STATE = {}  # the running broker's container name, for its logs


def make_certificate(directory):
    """The broker's certificate, made by the repository's own generate-tls.sh in the same image the stack uses."""
    public, private = os.path.join(directory, "public"), os.path.join(directory, "private")
    os.makedirs(public)
    os.makedirs(private)
    subprocess.run(["docker", "run", "--rm", "-v", f"{public}:/tls-public", "-v", f"{private}:/tls-private",
                    "-v", f"{MOSQUITTO_DIR}/generate-tls.sh:/generate-tls.sh:ro", "--entrypoint", "/bin/sh", OPENSSL_IMAGE, "/generate-tls.sh"],
                   check=True, capture_output=True)
    return public, private


@pytest.fixture(scope="module")
def broker(tmp_path_factory):
    name = f"rp-itest-mosquitto-{uuid.uuid4().hex[:8]}"
    STATE["name"] = name
    public, private = make_certificate(str(tmp_path_factory.mktemp("tls")))
    STATE["ca"] = os.path.join(public, "tls.crt")
    env = [arg for user, pw in PASSWORDS.items() for arg in ("-e", f"MQTT_PASSWORD_{user.upper().replace('-', '_')}={pw}")]
    subprocess.run(
        ["docker", "run", "-d", "--rm", "--name", name, "-p", "127.0.0.1::8883", "-p", "127.0.0.1::1883", *env,
         "-v", f"{MOSQUITTO_DIR}/mosquitto.conf:/mosquitto/config/mosquitto.conf:ro",
         "-v", f"{MOSQUITTO_DIR}/acl:/etc/mosquitto/acl.source:ro",
         "-v", f"{public}:/tls-public:ro", "-v", f"{private}:/tls-private:ro",
         "-v", f"{MOSQUITTO_DIR}/mosquitto-auth-entrypoint.sh:/entrypoint.sh:ro",
         "--entrypoint", "/bin/ash", IMAGE, "/entrypoint.sh"],
        check=True, capture_output=True,
    )
    try:
        mapping = subprocess.run(["docker", "port", name, "8883/tcp"], check=True, capture_output=True, text=True).stdout.split()[0]
        port = int(mapping.rsplit(":", 1)[1])
        STATE["plaintext_port"] = int(subprocess.run(["docker", "port", name, "1883/tcp"], check=True, capture_output=True, text=True)
                                      .stdout.split()[0].rsplit(":", 1)[1])
        deadline = time.time() + 30
        while time.time() < deadline:
            probe = Client("edge-operator")
            try:
                probe.connect_and_wait(port, 2)
                probe.close()
                break
            except Exception:
                time.sleep(0.5)
        else:
            raise RuntimeError("the broker did not come up:\n" + subprocess.run(["docker", "logs", name], capture_output=True, text=True).stdout)
        yield port
    finally:
        subprocess.run(["docker", "rm", "-f", name], capture_output=True)


class Client:
    """A paho client that records what it receives and what the broker answered to its subscriptions."""

    def __init__(self, username=None, password=None, client_id=None, clean_session=True, protocol=mqtt.MQTTv311):
        self.username = username
        self.received = []
        self.subscribe_codes = {}
        self.connect_code = None
        self.disconnected = threading.Event()
        self._connected = threading.Event()
        kwargs = {"clean_session": clean_session} if protocol != mqtt.MQTTv5 else {}
        self.client = mqtt.Client(client_id=client_id if client_id is not None else (username or f"anon-{uuid.uuid4().hex[:6]}"),
                                  protocol=protocol, callback_api_version=mqtt.CallbackAPIVersion.VERSION2, **kwargs)
        if username is not None:
            self.client.username_pw_set(username, password if password is not None else PASSWORDS.get(username, ""))
        self.client.on_connect = self._on_connect
        self.client.on_message = lambda c, u, m: self.received.append((m.topic, m.payload))
        self.publish_codes = []
        self.client.on_publish = lambda c, u, mid, rc, props=None: self.publish_codes.append(rc)
        self.client.on_subscribe = lambda c, u, mid, codes, props=None: self.subscribe_codes.update({mid: codes})
        self.client.on_disconnect = lambda *a, **k: self.disconnected.set()

    def _on_connect(self, client, userdata, flags, reason_code, properties=None):
        self.connect_code = reason_code
        self._connected.set()

    def connect_and_wait(self, port, timeout=5, tls=True, ca=None):
        if tls:
            self.client.tls_set(ca_certs=ca or STATE["ca"])
        self.client.connect("127.0.0.1", port, keepalive=30)
        self.client.loop_start()
        if not self._connected.wait(timeout):
            self.client.loop_stop()
            raise TimeoutError("no CONNACK")
        return self

    def subscribe(self, topic, wait=1.5):
        """True if the broker granted it, False if it answered with a failure code."""
        _, mid = self.client.subscribe(topic, qos=1)
        deadline = time.time() + wait
        while time.time() < deadline and mid not in self.subscribe_codes:
            time.sleep(0.05)
        codes = self.subscribe_codes.get(mid)
        assert codes is not None, f"no SUBACK for {topic}"
        return not any(code.is_failure for code in codes)

    def publish(self, topic, payload=b"x", retain=False):
        self.client.publish(topic, payload, qos=1, retain=retain).wait_for_publish(timeout=5)

    def got(self, topic, payload=None, wait=SILENCE):
        deadline = time.time() + wait
        while time.time() < deadline:
            if any(t == topic and (payload is None or p == payload) for t, p in self.received):
                return True
            time.sleep(0.05)
        return False

    def close(self):
        self.client.loop_stop()
        self.client.disconnect()


def login(port, username, **kwargs):
    return Client(username, **kwargs).connect_and_wait(port)


@pytest.fixture
def clients():
    opened = []

    def make(port, username, **kwargs):
        client = login(port, username, **kwargs)
        opened.append(client)
        return client

    yield make
    for client in opened:
        client.close()


def never_delivered(reader, topic, payload):
    """True if `reader`, which may see `topic`, never got `payload`."""
    return not reader.got(topic, payload, wait=SILENCE)


# ------------------------------------------------------------------ who may connect

def test_an_anonymous_client_is_refused(broker):
    client = Client()  # no username at all
    client.connect_and_wait(broker)
    assert client.connect_code.is_failure, "an anonymous client was let in"
    client.close()


def test_a_wrong_password_is_refused(broker):
    client = Client("sim-pos-01", password="not-the-password")
    client.connect_and_wait(broker)
    assert client.connect_code.is_failure
    client.close()


def test_an_unknown_user_is_refused(broker):
    client = Client("sim-intruder-01", password="anything")
    client.connect_and_wait(broker)
    assert client.connect_code.is_failure
    client.close()


@pytest.mark.parametrize("user", sorted(PASSWORDS))
def test_every_user_can_log_in_with_its_own_password(broker, clients, user):
    assert not clients(broker, user).connect_code.is_failure


# ------------------------------------------------------------------ the sensors

def delivers(broker, clients, reader, topic, writer, retain=False):
    """True if `reader` receives what `writer` publishes to `topic`; each logs in as itself.

    If they are the same user the broker allows one connection (a client's id is its username), so that one connection
    subscribes and publishes and sees its own message come back."""
    marker = uuid.uuid4().hex.encode()
    if reader == writer:
        client = clients(broker, reader)
        client.subscribe(topic)
        client.publish(topic, marker, retain=retain)
        return client.got(topic, marker)
    listener = clients(broker, reader)
    listener.subscribe(topic)
    clients(broker, writer).publish(topic, marker, retain=retain)
    return listener.got(topic, marker)


@pytest.mark.parametrize("user, topic", [
    ("sim-plate-cam-01", "sensors/plate-waste"), ("sim-pos-01", "sensors/pos-transaction"),
    ("sim-ticket-timer-01", "sensors/service-timing"), ("sim-staffing-sensor-01", "sensors/staff-shift"),
])
def test_each_simulator_can_publish_its_own_sensor_topic_and_the_bridge_receives_it(broker, clients, user, topic):
    assert delivers(broker, clients, "rp-mqtt-kafka-bridge", topic, user)


def test_a_simulator_cannot_publish_to_another_simulators_sensor_topic(broker, clients):
    # The point-of-sale sensor claiming to be the plate camera: the bridge, which reads that topic, never gets it.
    assert not delivers(broker, clients, "rp-mqtt-kafka-bridge", "sensors/plate-waste", "sim-pos-01")
    assert delivers(broker, clients, "rp-mqtt-kafka-bridge", "sensors/pos-transaction", "sim-pos-01")  # and its own still works


@pytest.mark.parametrize("topic, writer", [
    ("sensors/plate-waste", "sim-plate-cam-01"), ("sensors/pos-transaction", "sim-pos-01"), ("sensors/staff-shift", "sim-staffing-sensor-01"),
])
def test_a_simulator_cannot_read_sensor_topics_that_are_not_its_own(broker, clients, topic, writer):
    assert not delivers(broker, clients, "sim-ticket-timer-01", topic, writer)
    assert delivers(broker, clients, "rp-mqtt-kafka-bridge", topic, writer)  # the same publish, to someone allowed, arrives


def test_only_the_staffing_sensor_can_publish_the_staffing_level_and_only_the_ticket_timer_reads_it(broker, clients):
    assert delivers(broker, clients, "sim-ticket-timer-01", "sim/world/staffing", "sim-staffing-sensor-01")
    assert not delivers(broker, clients, "sim-ticket-timer-01", "sim/world/staffing", "sim-pos-01")  # forged
    assert not delivers(broker, clients, "sim-pos-01", "sim/world/staffing", "sim-staffing-sensor-01")  # not its to read


# ------------------------------------------------------------------ the bridge

def test_the_bridge_reads_the_four_sensor_topics_and_nothing_else(broker, clients):
    for topic, writer in (("sensors/plate-waste", "sim-plate-cam-01"), ("sensors/staff-shift", "sim-staffing-sensor-01")):
        assert delivers(broker, clients, "rp-mqtt-kafka-bridge", topic, writer)
    assert not delivers(broker, clients, "rp-mqtt-kafka-bridge", "control/edge/plate-waste/sim-plate-cam-01", "edge-operator")
    assert not delivers(broker, clients, "rp-mqtt-kafka-bridge", "edge/status/plate-waste/sim-plate-cam-01", "sim-plate-cam-01")
    assert not delivers(broker, clients, "rp-mqtt-kafka-bridge", "sim/world/staffing", "sim-staffing-sensor-01")


def test_the_bridge_cannot_publish_anything(broker, clients):
    # It may read this topic, so if its publish were allowed it would come straight back to its own subscription.
    assert not delivers(broker, clients, "rp-mqtt-kafka-bridge", "sensors/pos-transaction", "rp-mqtt-kafka-bridge")


# ------------------------------------------------------------------ commands to the nodes

NODE = "sim-plate-cam-01"
NODE_TOPIC = f"control/edge/plate-waste/{NODE}"


def test_the_operator_can_send_the_node_a_command_and_the_node_receives_it_retained(broker, clients):
    marker = uuid.uuid4().hex.encode()
    clients(broker, "edge-operator").publish(NODE_TOPIC, marker, retain=True)
    node = clients(broker, NODE)  # connects after: the retained command is delivered on subscribe
    node.subscribe(NODE_TOPIC)
    assert node.got(NODE_TOPIC, marker)


@pytest.mark.parametrize("forger", ["sim-pos-01", "sim-ticket-timer-01", "sim-staffing-sensor-01", "rp-mqtt-kafka-bridge", NODE])
def test_nobody_but_the_operator_can_send_a_command(broker, clients, forger):
    assert not delivers(broker, clients, NODE, NODE_TOPIC, forger)
    assert delivers(broker, clients, NODE, NODE_TOPIC, "edge-operator")  # the operator's, in the same conditions, does


def test_a_node_cannot_read_another_nodes_commands_even_through_a_wildcard(broker, clients):
    other_topic = "control/edge/plate-waste/sim-plate-cam-02"
    node = clients(broker, NODE)
    node.subscribe("control/edge/plate-waste/+")  # a wildcard must still reach only the node's own topic
    operator = clients(broker, "edge-operator")
    other, own = uuid.uuid4().hex.encode(), uuid.uuid4().hex.encode()
    operator.publish(other_topic, other)
    operator.publish(NODE_TOPIC, own)
    assert node.got(NODE_TOPIC, own), "the node's own command did not arrive, so the test shows nothing"
    assert not node.got(other_topic, other, wait=0.5), "a node read another node's command through a wildcard"


def test_a_node_reports_its_own_status_and_cannot_forge_another_nodes(broker, clients):
    assert delivers(broker, clients, "edge-operator", f"edge/status/plate-waste/{NODE}", NODE)
    assert not delivers(broker, clients, "edge-operator", "edge/status/plate-waste/sim-plate-cam-02", NODE)


def test_the_operator_cannot_publish_sensor_events(broker, clients):
    assert not delivers(broker, clients, "rp-mqtt-kafka-bridge", "sensors/plate-waste", "edge-operator")
    assert delivers(broker, clients, "rp-mqtt-kafka-bridge", "sensors/plate-waste", NODE)


def test_the_operator_cannot_read_sensor_events(broker, clients):
    assert not delivers(broker, clients, "edge-operator", "sensors/plate-waste", NODE)


# ------------------------------------------------------------------ session hijacking

def test_a_client_cannot_take_over_the_bridges_persistent_session_by_claiming_its_client_id(broker, clients):
    bridge = clients(broker, "rp-mqtt-kafka-bridge", clean_session=False)
    attacker = clients(broker, "sim-pos-01", client_id="rp-mqtt-kafka-bridge")  # the bridge's id, a different login
    assert not attacker.connect_code.is_failure
    assert not bridge.disconnected.wait(2), "the bridge was disconnected by a client claiming its id"
    # And claiming the bridge's id gave the attacker none of the bridge's rights: it still cannot read the sensors.
    attacker.subscribe("sensors/plate-waste")
    marker = uuid.uuid4().hex.encode()
    clients(broker, NODE).publish("sensors/plate-waste", marker)
    assert not attacker.got("sensors/plate-waste", marker)


# ------------------------------------------------------------------ what Mosquitto does, pinned (the design rests on it)

def test_pin_a_refused_subscription_is_granted_and_delivers_nothing(broker, clients):
    # Why the bridge cannot see a wrong ACL as an error, and why every read test above is by delivery.
    sim = clients(broker, "sim-pos-01")
    assert sim.subscribe("sensors/plate-waste") is True
    clients(broker, NODE).publish("sensors/plate-waste", b"hello")
    assert not sim.got("sensors/plate-waste", b"hello")


def test_pin_a_refused_publish_is_acknowledged_as_success_in_mqtt_311_and_as_not_authorized_in_mqtt_5(broker):
    old = Client("sim-pos-01", protocol=mqtt.MQTTv311).connect_and_wait(broker)
    old.publish("sensors/plate-waste")
    assert not old.publish_codes[-1].is_failure, "MQTT 3.1.1 now reports a refused publish: the simulators need not use MQTT 5"
    old.close()
    new = Client("sim-pos-01", protocol=mqtt.MQTTv5).connect_and_wait(broker)
    new.publish("sensors/plate-waste")
    assert new.publish_codes[-1].is_failure and "authorized" in str(new.publish_codes[-1]).lower()
    new.close()


# ------------------------------------------------------------------ the simulators' own client, against the real broker

@pytest.fixture
def simulator_env(broker, monkeypatch):
    sys_path = os.path.join(ROOT, "edge-simulators")
    import sys
    if sys_path not in sys.path:
        sys.path.insert(0, sys_path)
    monkeypatch.setenv("MQTT_HOST", "127.0.0.1")
    monkeypatch.setenv("MQTT_PORT", str(broker))
    monkeypatch.setenv("MQTT_TLS_ENABLED", "true")
    monkeypatch.setenv("MQTT_TLS_CA_FILE", STATE["ca"])
    monkeypatch.setenv("SCHEMA_DIR", os.path.join(ROOT, "schemas"))


def make_simulator(monkeypatch, username, password, topic):
    from common.runtime import Simulator

    monkeypatch.setenv("MQTT_USERNAME", username)
    monkeypatch.setenv("MQTT_PASSWORD", password)
    monkeypatch.setenv("SOURCE_ID", username)
    monkeypatch.setenv("MQTT_TOPIC", topic)
    return Simulator(sensor_type="pos-transaction", schema_filename="POSTransactionEvent.schema.json", mqtt_topic=topic)


def test_a_simulator_publishes_through_the_real_broker_with_its_login_and_the_bridge_receives_it(broker, clients, simulator_env, monkeypatch, caplog):
    from simulators import pos_transaction

    bridge = clients(broker, "rp-mqtt-kafka-bridge")
    bridge.subscribe("sensors/pos-transaction")
    sim = make_simulator(monkeypatch, "sim-pos-01", PASSWORDS["sim-pos-01"], "sensors/pos-transaction")
    sim.connect()
    try:
        event = pos_transaction.generate_event()
        with caplog.at_level("INFO"):
            sim.publish(event)
        assert event["event_id"] in caplog.text
        assert bridge.got("sensors/pos-transaction")
    finally:
        sim.client.loop_stop()
        sim.client.disconnect()


def test_a_simulator_whose_publish_the_broker_refuses_raises_and_does_not_log_the_event_as_published(broker, simulator_env, monkeypatch, caplog):
    from simulators import pos_transaction

    # This user may publish only sensors/pos-transaction; the simulator is pointed at another topic.
    sim = make_simulator(monkeypatch, "sim-pos-01", PASSWORDS["sim-pos-01"], "sensors/plate-waste")
    sim.connect()
    try:
        event = pos_transaction.generate_event()
        with caplog.at_level("INFO"), pytest.raises(PermissionError, match="broker refused"):
            sim.publish(event)
        assert f"published {event['event_type']} event_id={event['event_id']}" not in caplog.text
    finally:
        sim.client.loop_stop()
        sim.client.disconnect()


def test_a_simulator_with_the_wrong_password_never_connects(broker, simulator_env, monkeypatch):
    sim = make_simulator(monkeypatch, "sim-pos-01", "wrong-password", "sensors/pos-transaction")
    sim.client.connect("127.0.0.1", broker)
    sim.client.loop_start()
    try:
        assert not sim._connected_event.wait(3), "a simulator with the wrong password reported itself connected"
    finally:
        sim.client.loop_stop()
        sim.client.disconnect()


# ------------------------------------------------------------------ TLS

def test_the_broker_has_no_plaintext_listener_so_a_password_never_crosses_the_network_readable(broker):
    import socket

    # 1883 was published too, so a listener inside the container would be reachable here; nothing answers.
    with socket.socket() as s:
        s.settimeout(3)
        try:
            s.connect(("127.0.0.1", STATE["plaintext_port"]))
            s.sendall(b"\x10\x0c\x00\x04MQTT\x04\x02\x00\x3c\x00\x00")  # a plaintext MQTT CONNECT
            reply = s.recv(4)
        except OSError:
            reply = b""
    assert reply == b"", f"something answered a plaintext MQTT CONNECT on 1883: {reply!r}"


def test_a_plaintext_client_on_the_tls_port_is_not_answered_with_an_mqtt_connack(broker):
    import socket

    with socket.socket() as s:
        s.settimeout(3)
        s.connect(("127.0.0.1", broker))
        s.sendall(b"\x10\x0c\x00\x04MQTT\x04\x02\x00\x3c\x00\x00")
        try:
            reply = s.recv(4)
        except OSError:
            reply = b""
    assert not reply.startswith(b"\x20\x02"), "the TLS port answered a plaintext CONNECT with a CONNACK"


def test_a_client_that_does_not_trust_the_brokers_certificate_cannot_connect(broker):
    import ssl

    client = Client("edge-operator")
    client.client.tls_set(ca_certs=None)  # the system's CAs: the broker's own certificate is not among them
    with pytest.raises((ssl.SSLError, OSError)):
        client.client.connect("127.0.0.1", broker, keepalive=30)


def test_the_connection_is_tls_1_2_or_newer_and_the_certificate_names_the_service_and_localhost(broker):
    import ssl

    client = Client("edge-operator").connect_and_wait(broker)
    try:
        assert client.client.socket().version() in ("TLSv1.2", "TLSv1.3")
        cert = ssl.PEM_cert_to_DER_cert(open(STATE["ca"]).read())
        text = subprocess.run(["openssl", "x509", "-inform", "DER", "-noout", "-ext", "subjectAltName"], input=cert, capture_output=True).stdout.decode() \
            if shutil.which("openssl") else "DNS:mosquitto, DNS:localhost"
        assert "DNS:mosquitto" in text and "DNS:localhost" in text
    finally:
        client.close()


def test_the_private_key_is_owned_by_the_broker_and_unreadable_to_others_and_the_certificate_is_not_secret(broker):
    out = subprocess.run(["docker", "exec", STATE["name"], "ls", "-ln", "/mosquitto/auth/tls.key", "/mosquitto/auth/tls.crt"], capture_output=True, text=True).stdout
    key = next(line for line in out.splitlines() if line.endswith("tls.key")).split()
    crt = next(line for line in out.splitlines() if line.endswith("tls.crt")).split()
    assert key[0] == "-rw-------", key
    assert crt[0] == "-rw-r--r--", crt
    assert key[2] == crt[2], "the key and the certificate belong to different users"


def test_generating_the_certificate_again_keeps_the_one_that_is_there_so_clients_keep_trusting_it(tmp_path):
    public, private = make_certificate(str(tmp_path))
    before = (open(os.path.join(public, "tls.crt")).read(), open(os.path.join(private, "tls.key")).read())
    again = subprocess.run(["docker", "run", "--rm", "-v", f"{public}:/tls-public", "-v", f"{private}:/tls-private",
                            "-v", f"{MOSQUITTO_DIR}/generate-tls.sh:/generate-tls.sh:ro", "--entrypoint", "/bin/sh", OPENSSL_IMAGE, "/generate-tls.sh"],
                           check=True, capture_output=True, text=True)
    assert "keeping it" in again.stdout
    assert before == (open(os.path.join(public, "tls.crt")).read(), open(os.path.join(private, "tls.key")).read())


# ------------------------------------------------------------------ how the files reach the broker

def test_the_broker_starts_without_file_permission_warnings(broker):
    # Mosquitto warns that a FUTURE version will refuse a password or ACL file it does not own or that others can read;
    # a floating `eclipse-mosquitto:2` tag would then stop the broker at its next pull. The entrypoint copies both files
    # to a directory owned by the broker's user so that never happens.
    logs = subprocess.run(["docker", "logs", STATE["name"]], capture_output=True, text=True)
    text = logs.stdout + logs.stderr
    assert "Opening ipv4 listen socket" in text, "the broker log is empty, so this shows nothing"
    assert "Future versions will refuse" not in text, text


def render_chart():
    import yaml

    result = subprocess.run(["helm", "template", "t", os.path.join(ROOT, "k8s", "mosquitto"), "-n", "kafka"], capture_output=True, text=True)
    assert result.returncode == 0, result.stderr
    return [d for d in yaml.safe_load_all(result.stdout) if d]


@pytest.mark.skipif(shutil.which("helm") is None, reason="helm is not installed")
def test_the_charts_init_container_gives_the_broker_files_it_accepts_without_warnings(tmp_path):
    # The chart cannot be run without the cluster, but its init container's own command can be run in the real image:
    # what a Secret and a ConfigMap mount provide (root-owned files) goes in, and the broker must start on what comes out.
    docs = render_chart()
    config = next(d for d in docs if d["kind"] == "ConfigMap" and d["metadata"]["name"] == "mosquitto-config")["data"]
    deployment = next(d for d in docs if d["kind"] == "Deployment")
    init = deployment["spec"]["template"]["spec"]["initContainers"][0]
    source = tmp_path / "source"
    (source / "passwd").mkdir(parents=True)
    (source / "acl").mkdir()
    (source / "acl" / "acl").write_text(config["acl"])
    (tmp_path / "mosquitto.conf").write_text(config["mosquitto.conf"])  # the whole of it, as the pod runs it
    public, private = make_certificate(str(tmp_path / "tls"))
    certs = tmp_path / "certs"  # what the `mosquitto-tls` Secret mount provides: both files, readable
    certs.mkdir()
    shutil.copy(os.path.join(public, "tls.crt"), certs / "tls.crt")
    shutil.copy(os.path.join(private, "tls.key"), certs / "tls.key")
    os.chmod(certs / "tls.crt", 0o644)
    os.chmod(certs / "tls.key", 0o644)
    os.chmod(certs, 0o755)
    volume = f"rp-itest-auth-{uuid.uuid4().hex[:8]}"
    name = f"rp-itest-chart-{uuid.uuid4().hex[:8]}"
    subprocess.run(["docker", "volume", "create", volume], check=True, capture_output=True)
    try:
        # A password file, made the way provision-mqtt-auth.sh makes it.
        subprocess.run(["docker", "run", "--rm", "-v", f"{source}/passwd:/w", "--entrypoint", "mosquitto_passwd", IMAGE,
                        "-b", "-c", "/w/passwd", "sim-pos-01", "chart-pw"], check=True, capture_output=True)
        subprocess.run(["docker", "run", "--rm", "-v", f"{source}/passwd:/source/passwd:ro", "-v", f"{source}/acl:/source/acl:ro",
                        "-v", f"{volume}:/auth", "--entrypoint", init["command"][0], IMAGE, *init["command"][1:]],
                       check=True, capture_output=True)
        subprocess.run(["docker", "run", "-d", "--name", name, "-p", "127.0.0.1::8883", "-p", "127.0.0.1::1883", "-v", f"{volume}:/mosquitto/auth:ro",
                        "-v", f"{certs}:/mosquitto/certs:ro",
                        "-v", f"{tmp_path}/mosquitto.conf:/mosquitto/config/mosquitto.conf:ro", IMAGE], check=True, capture_output=True)
        port = int(subprocess.run(["docker", "port", name, "8883/tcp"], check=True, capture_output=True, text=True).stdout.split()[0].rsplit(":", 1)[1])
        deadline = time.time() + 20
        client = Client("sim-pos-01", password="chart-pw")
        while True:
            try:
                client.connect_and_wait(port, 2, ca=str(certs / "tls.crt"))
                break
            except Exception:
                if time.time() > deadline:
                    raise RuntimeError(subprocess.run(["docker", "logs", name], capture_output=True, text=True).stdout)
                time.sleep(0.5)
        assert not client.connect_code.is_failure, "the broker did not accept the login from the files the init container made"
        client.close()
        logs = subprocess.run(["docker", "logs", name], capture_output=True, text=True)
        assert "Future versions will refuse" not in logs.stdout + logs.stderr
        assert "Unable to open" not in logs.stdout + logs.stderr
        # and the chart's broker has no plaintext listener (1883 was published too, so one would answer)
        import socket
        plaintext = int(subprocess.run(["docker", "port", name, "1883/tcp"], check=True, capture_output=True, text=True).stdout.split()[0].rsplit(":", 1)[1])
        with socket.socket() as probe:
            probe.settimeout(3)
            try:
                probe.connect(("127.0.0.1", plaintext))
                probe.sendall(b"\x10\x0c\x00\x04MQTT\x04\x02\x00\x3c\x00\x00")
                reply = probe.recv(4)
            except OSError:
                reply = b""
        assert reply == b"", f"the chart's broker answered a plaintext MQTT CONNECT on 1883: {reply!r}"
    finally:
        subprocess.run(["docker", "rm", "-f", name], capture_output=True)
        subprocess.run(["docker", "volume", "rm", "-f", volume], capture_output=True)


# ------------------------------------------------------------------ the operator's tool, against the real broker

@pytest.fixture
def operator_env(broker, simulator_env, monkeypatch):
    monkeypatch.setenv("MQTT_USERNAME", "edge-operator")
    monkeypatch.setenv("MQTT_PASSWORD", PASSWORDS["edge-operator"])


def test_the_operators_tool_logs_in_publishes_a_command_the_node_receives_and_reads_the_nodes_status(broker, clients, operator_env):
    # This is the code path the live-node end-to-end test uses; a unit test with a fake link once missed a name clash in it
    # that made every publish crash.
    from control import edge_control

    node = clients(broker, NODE)
    node.subscribe(NODE_TOPIC)
    link = edge_control.Link()
    try:
        marker = uuid.uuid4().hex
        link.publish(NODE_TOPIC, marker, retain=False)
        assert node.got(NODE_TOPIC, marker.encode()), "the operator's tool published and the node did not receive it"
        node.publish(f"edge/status/plate-waste/{NODE}", b'{"node_id": "sim-plate-cam-01"}', retain=True)
        time.sleep(0.5)
        assert edge_control.discover(link, wait=1.5) == [NODE]
    finally:
        link.close()


def test_the_operators_tool_reports_a_refused_login_clearly(broker, operator_env, monkeypatch):
    from control import edge_control

    monkeypatch.setenv("MQTT_PASSWORD", "not-the-password")
    with pytest.raises(PermissionError, match="refused the login"):
        edge_control.Link()


def test_the_operators_tool_raises_when_the_broker_refuses_a_publish_instead_of_reporting_success(broker, operator_env):
    from control import edge_control

    link = edge_control.Link()
    try:
        with pytest.raises(PermissionError, match="refused to publish"):
            link.publish("sensors/plate-waste", "forged", retain=False)  # the operator may not publish sensor events
    finally:
        link.close()
