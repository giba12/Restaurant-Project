"""
The cloud side of the edge control path: a versioned model store, the signed commands that roll a model out to
nodes (or back), and what to read from the nodes' replies. The node side is edge_ai/updater.py.

    python -m control.edge_control list
    python -m control.edge_control rollout  --version 1.2.0 --nodes sim-plate-cam-01        # a canary
    python -m control.edge_control rollout  --version 1.2.0 --nodes all                     # then every live node
    python -m control.edge_control rollback --to 1.1.0 --nodes all                          # back, no shadow
    python -m control.edge_control status
    python -m control.edge_control clear --nodes all                                        # drop the desired state
    python -m control.edge_control derive-key --node sim-plate-cam-01 [--generation 2]      # a node's own key
    curl -s -H "X-API-Key: ..." http://dashboard-api:8000/api/edge/plate-waste | python -m control.edge_control advise

KEYS. Every node has its own key, derived from a master secret the operator keeps:
HMAC-SHA256(master, "edge-control/v1/<node id>/<generation>"). A node holds only its own key, so a key taken from one
node cannot command another, and each command also names the node it is for. Given EDGE_CONTROL_MASTER_KEY this tool
derives the key of any node it addresses (`--nodes all` means every node that has reported a status). Run inside a
node's own container (`docker compose exec edge-sim-plate-waste ...`, `kubectl exec`) with only that node's
EDGE_CONTROL_KEY it can command that node alone. To ROTATE a node's key: derive the next generation
(`derive-key --node N --generation G+1`), give the node both keys (EDGE_CONTROL_KEY the new one, EDGE_CONTROL_KEY_PREVIOUS
the old), command it with `--generation G+1`, then take the previous key away.

BROKER LOGIN. The broker refuses anonymous clients and limits each user to its own topics (docker-compose/mosquitto/acl,
k8s/mosquitto). This tool logs in as MQTT_USERNAME / MQTT_PASSWORD, the `edge-operator` user, the only one that may
publish control topics.

A rollout is a DESIRED STATE: a retained message, so a node that restarts is told again. Replacing it with a
rollback replaces what a restarting node will be told; `clear` removes it, and a node that restarts after that
runs the model baked into its image.

The model store is `model_store/<model_id>/<version>.json`: immutable artifacts, each checked against its own
hash and its own version before it is sent. Nothing here decides to roll anything out. A drift alarm cannot tell
a dirty lens from a changed population, and a model retrained on a fouled lens's data would learn the fault, so
`advise` says what to look at and a person decides.
"""
import argparse
import hashlib
import hmac
import json
import os
import random
import sys
import threading
import time
import uuid
from pathlib import Path

from edge_ai import model as edge_model
from edge_ai import sensor
from edge_ai import updater

DEFAULT_STORE = Path(__file__).resolve().parent.parent / "model_store"
MODEL_ID = "plate-waste-edge-regressor"
CONTROL_TOPIC = "control/edge/plate-waste/{}"  # one per node: there is no fleet-wide topic, a command is signed for one node
STATUS_TOPIC = "edge/status/plate-waste/{}"
PROBE_READINGS = 16
PROBE_SEED = 20261006


class ModelStore:
    def __init__(self, root=DEFAULT_STORE):
        self.root = Path(root)

    def versions(self, model_id: str = MODEL_ID) -> list:
        folder = self.root / model_id
        found = [p.stem for p in folder.glob("*.json")] if folder.is_dir() else []
        return sorted(found, key=lambda v: tuple(int(part) for part in v.split(".")))

    def load(self, version: str, model_id: str = MODEL_ID):
        """(artifact, EdgeModel) for one stored version, refusing one that does not match its own hash or name."""
        path = self.root / model_id / f"{version}.json"
        if not path.is_file():
            raise FileNotFoundError(f"{model_id} {version} is not in the model store (have: {', '.join(self.versions(model_id)) or 'nothing'})")
        artifact = json.loads(path.read_text())
        model = edge_model.EdgeModel(artifact)  # refuses a hash mismatch
        if artifact["model_version"] != version or artifact["model_id"] != model_id:
            raise ValueError(f"{path} claims to be {artifact['model_id']} {artifact['model_version']}")
        return artifact, model


def probe(model, n: int = PROBE_READINGS, seed: int = PROBE_SEED) -> list:
    """Known-answer readings: inputs from the sensor simulation, and the grams this model computes for them."""
    rng = random.Random(seed)
    out = []
    for _ in range(n):
        grams = sensor.true_waste_grams(rng, to_go=rng.random() < 0.15)
        features = sensor.feature_vector(sensor.read_sensors(grams, rng))
        out.append({"features": features, "grams": model.infer(features).grams})
    return out


def derive_node_key(master: str, node_id: str, generation: int = 1) -> str:
    """The control key of one node: a keyed hash of its identity under the operator's master secret."""
    message = f"edge-control/v1/{node_id}/{generation}".encode("utf-8")
    return hmac.new(master.encode("utf-8"), message, hashlib.sha256).hexdigest()


def command_from_artifact(artifact: dict, key: str, node_id: str, shadow_readings=updater.DEFAULT_SHADOW_READINGS,
                          max_mean_abs_diff_g=updater.DEFAULT_MAX_MEAN_ABS_DIFF_G, force=False, now=None) -> dict:
    """A signed set_model command for one node and any artifact. The store path below is the normal one; this is for tests."""
    model = edge_model.EdgeModel(artifact)
    command = {
        "version": updater.CONTROL_VERSION,
        "node_id": node_id,
        "request_id": str(uuid.uuid4()),
        "command": "set_model",
        "issued_at": time.time() if now is None else now,
        "model": {"model_id": model.model_id, "model_version": model.model_version, "sha256": model.sha256, "artifact": artifact},
        "probe": probe(model),
        "shadow_readings": shadow_readings,
        "max_mean_abs_diff_g": max_mean_abs_diff_g,
        "force": bool(force),
    }
    return updater.sign(command, key)


def build_set_model(store: ModelStore, version: str, key: str, node_id: str, **options) -> dict:
    artifact, _ = store.load(version)
    return command_from_artifact(artifact, key, node_id, **options)


def signing_key(node_id: str, generation: int, environ=None) -> str:
    """The key to sign a command for `node_id`: derived from the master secret if this tool has it, otherwise the
    node's own key, which only works from inside that node's container."""
    env = os.environ if environ is None else environ
    master = env.get("EDGE_CONTROL_MASTER_KEY")
    if master:
        return derive_node_key(master, node_id, generation)
    own = env.get("EDGE_CONTROL_KEY")
    if own and node_id == env.get("SOURCE_ID"):
        return own
    raise KeyError(f"no key for node {node_id!r}: set EDGE_CONTROL_MASTER_KEY to command any node, or run inside that node's "
                   "container (where EDGE_CONTROL_KEY is its own)")


def targets(nodes: str) -> list:
    names = [n.strip() for n in nodes.split(",") if n.strip()]
    if not names:
        raise ValueError("name at least one node, or 'all'")
    return names


# ---- reading the nodes' replies

def advise(nodes: list) -> list:
    """What a person should look at, from the rows of the dashboard API's edge route. Never an action."""
    lines = []
    by_source = {}
    for n in nodes:
        by_source.setdefault(n["source_id"], []).append(n)
        if n.get("drifting_now"):
            lines.append(
                f"{n['source_id']} ({n['model_version']}): DRIFT. Inspect and clean the sensor first. No model change is recommended "
                "or made automatically: a drift alarm cannot tell a dirty lens from a changed population, and a model retrained "
                "on a fouled lens's data would learn the fault.")
        if n.get("out_of_distribution_rate", 0) > 0.05:
            lines.append(f"{n['source_id']} ({n['model_version']}): {n['out_of_distribution_rate']:.1%} of readings are outside the "
                         "training data (calibrated to about 0.1%). Check the sensor and its lighting.")
    for source, rows in by_source.items():
        if len({r["model_sha256"] for r in rows}) > 1:
            versions = ", ".join(f"{r['model_version']} ({r['readings']} readings)" for r in rows)
            lines.append(f"{source}: estimates came from more than one model in the window: {versions}. A rollout or a rollback is in "
                         "progress, or the node restarted onto the model baked into its image.")
    if len(by_source) > 1 and len({rows[0]["model_sha256"] for rows in by_source.values()}) > 1:
        lines.append("The fleet is running different models on different nodes: " + ", ".join(
            f"{source} {rows[0]['model_version']}" for source, rows in sorted(by_source.items())) + ".")
    return lines or ["Nothing to look at: no node is drifting, none distrusts its readings, and every node runs one model."]


# ---- the MQTT link (same environment variables as the simulators)

class Link:
    def __init__(self):
        import paho.mqtt.client as mqtt
        username = os.environ.get("MQTT_USERNAME")
        # The broker makes a client's id its username, so a logged-in tool uses the username as its id.
        self.client = mqtt.Client(client_id=username or f"edge-control-{uuid.uuid4().hex[:8]}", protocol=mqtt.MQTTv5,
                                  callback_api_version=mqtt.CallbackAPIVersion.VERSION2)
        self._refused = {}
        self.client.on_publish = lambda c, u, mid, rc, props=None: self._refused.__setitem__(mid, rc) if rc.is_failure else None
        if os.environ.get("MQTT_TLS_ENABLED", "false").lower() == "true":
            self.client.tls_set(ca_certs=os.environ.get("MQTT_TLS_CA_FILE", "/etc/mosquitto-tls/tls.crt"))
        if username:
            self.client.username_pw_set(username, os.environ.get("MQTT_PASSWORD", ""))
        self._connected = threading.Event()
        self._connect_refused = None  # why the login was refused (not to be confused with _refused: publishes the broker refused)

        def on_connect(client, userdata, flags, reason_code, properties=None):
            if reason_code.is_failure:
                self._connect_refused = str(reason_code)
            self._connected.set()

        self.client.on_connect = on_connect
        self.client.connect(os.environ.get("MQTT_HOST", "mosquitto"), int(os.environ.get("MQTT_PORT", "1883")), keepalive=30)
        self.client.loop_start()
        if not self._connected.wait(timeout=15):
            raise TimeoutError("could not connect to the MQTT broker")
        if self._connect_refused:
            self.client.loop_stop()
            raise PermissionError(f"the broker refused the login ({self._connect_refused}): set MQTT_USERNAME and MQTT_PASSWORD to the edge-operator user")

    def publish(self, topic: str, payload: str, retain: bool) -> None:
        info = self.client.publish(topic, payload, qos=1, retain=retain)
        info.wait_for_publish(timeout=10)
        refused = self._refused.pop(info.mid, None)
        if refused is not None:
            raise PermissionError(f"the broker refused to publish to {topic} ({refused}): this login is not allowed to")

    def collect(self, topic_filter: str, seconds: float) -> dict:
        found = {}
        self.client.on_message = lambda c, u, m: found.__setitem__(m.topic, m.payload)
        self.client.subscribe(topic_filter, qos=1)
        time.sleep(seconds)
        return found

    def close(self) -> None:
        self.client.loop_stop()
        self.client.disconnect()


def discover(link, wait: float = 3.0) -> list:
    """The nodes that have reported a status (a node announces itself, retained, whenever it connects)."""
    nodes = set()
    for payload in link.collect(STATUS_TOPIC.format("+"), wait).values():
        try:
            nodes.add(json.loads(payload)["node_id"])
        except (ValueError, KeyError, TypeError):
            continue
    return sorted(nodes)


def resolve_targets(link, nodes: str) -> list:
    names = targets(nodes)
    if names == ["all"]:
        found = discover(link)
        if not found:
            raise LookupError("no node has reported a status, so 'all' is empty; name the nodes instead")
        return found
    if "all" in names:
        raise ValueError("'all' stands for every live node and cannot be mixed with names")
    return names


def main(argv=None, link_factory=Link) -> int:
    parser = argparse.ArgumentParser(prog="edge_control")
    parser.add_argument("--store", default=str(DEFAULT_STORE))
    sub = parser.add_subparsers(dest="action", required=True)
    sub.add_parser("list")
    for name in ("rollout", "rollback"):
        p = sub.add_parser(name)
        p.add_argument("--version" if name == "rollout" else "--to", dest="version", required=True)
        p.add_argument("--nodes", required=True, help="node ids, comma separated, or 'all' (every node that has reported a status)")
        p.add_argument("--generation", type=int, default=int(os.environ.get("EDGE_CONTROL_GENERATION", "1")),
                       help="the key generation to sign with (bump it to rotate a node's key)")
        if name == "rollout":
            p.add_argument("--shadow-readings", type=int, default=updater.DEFAULT_SHADOW_READINGS)
            p.add_argument("--max-diff-g", type=float, default=updater.DEFAULT_MAX_MEAN_ABS_DIFF_G)
    p = sub.add_parser("clear")
    p.add_argument("--nodes", required=True)
    p = sub.add_parser("status")
    p.add_argument("--wait", type=float, default=3.0)
    p = sub.add_parser("derive-key", help="print a node's own control key (needs EDGE_CONTROL_MASTER_KEY)")
    p.add_argument("--node", required=True)
    p.add_argument("--generation", type=int, default=int(os.environ.get("EDGE_CONTROL_GENERATION", "1")))
    sub.add_parser("advise")
    args = parser.parse_args(argv)
    store = ModelStore(args.store)

    if args.action == "list":
        for version in store.versions():
            _, model = store.load(version)
            print(f"{MODEL_ID} {version}  sha256 {model.sha256[:12]}  shift monitor: {'yes' if model.shift_window else 'no'}, flatline monitor: {'yes' if model.flatline_window else 'no'}")
        return 0

    if args.action == "advise":
        body = json.load(sys.stdin)
        for line in advise(body["nodes"] if isinstance(body, dict) else body):
            print(line)
        return 0

    if args.action == "derive-key":
        master = os.environ.get("EDGE_CONTROL_MASTER_KEY")
        if not master:
            print("EDGE_CONTROL_MASTER_KEY is not set: only the operator who holds the master secret can derive a node's key", file=sys.stderr)
            return 2
        key = derive_node_key(master, args.node, args.generation)
        print(key)
        print(f"# node {args.node}, generation {args.generation}, key id {updater.key_id(key)}", file=sys.stderr)
        return 0

    link = link_factory()
    try:
        if args.action in ("rollout", "rollback"):
            rollback = args.action == "rollback"
            names = resolve_targets(link, args.nodes)
            try:
                keys = {name: signing_key(name, args.generation) for name in names}  # every key first: publish nothing if one is missing
            except KeyError as exc:
                print(exc.args[0], file=sys.stderr)
                return 2
            for name in names:
                command = build_set_model(
                    store, args.version, keys[name], name,
                    shadow_readings=0 if rollback else args.shadow_readings,
                    max_mean_abs_diff_g=updater.DEFAULT_MAX_MEAN_ABS_DIFF_G if rollback else args.max_diff_g, force=rollback)
                link.publish(CONTROL_TOPIC.format(name), json.dumps(command), retain=True)
                print(f"{'rolled back to' if rollback else 'rolled out'} {args.version} to {name} (request {command['request_id']}, key id {command['key_id']})")
        elif args.action == "clear":
            for name in resolve_targets(link, args.nodes):
                link.publish(CONTROL_TOPIC.format(name), "", retain=True)
                print(f"cleared the desired state for {name}")
        elif args.action == "status":
            replies = link.collect(STATUS_TOPIC.format("+"), args.wait)
            if not replies:
                print("no node has reported a status")
            for topic, payload in sorted(replies.items()):
                s = json.loads(payload)
                print(f"{s['node_id']}: {s['state']} - {s['reason']}; running {s['active']['model_version']} ({s['active']['sha256'][:12]})"
                      + (f"; candidate {s['candidate']['model_version']}" if s.get("candidate") else ""))
        return 0
    finally:
        link.close()


if __name__ == "__main__":
    sys.exit(main())
