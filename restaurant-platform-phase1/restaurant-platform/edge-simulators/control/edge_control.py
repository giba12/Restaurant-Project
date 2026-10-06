"""
The cloud side of the edge control path: a versioned model store, the signed commands that roll a model out to
nodes (or back), and what to read from the nodes' replies. The node side is edge_ai/updater.py.

    python -m control.edge_control list
    python -m control.edge_control rollout  --version 1.1.0 --nodes sim-plate-cam-01        # a canary
    python -m control.edge_control rollout  --version 1.1.0 --nodes all                     # then everyone
    python -m control.edge_control rollback --to 1.0.0 --nodes all                          # back, no shadow
    python -m control.edge_control status
    python -m control.edge_control clear --nodes all                                        # drop the desired state
    curl -s -H "X-API-Key: ..." http://dashboard-api:8000/api/edge/plate-waste | python -m control.edge_control advise

It runs wherever it can reach the MQTT broker with the node's key: inside a node's own container
(`docker compose exec edge-sim-plate-waste python -m control.edge_control ...`, `kubectl exec`) it picks up the
broker address and EDGE_CONTROL_KEY from the container's environment.

A rollout is a DESIRED STATE: a retained message, so a node that restarts is told again. Replacing it with a
rollback replaces what a restarting node will be told; `clear` removes it, and a node that restarts after that
runs the model baked into its image.

The model store is `model_store/<model_id>/<version>.json`: immutable artifacts, each checked against its own
hash and its own version before it is sent. Nothing here decides to roll anything out. A drift alarm cannot tell
a dirty lens from a changed population, and a model retrained on a fouled lens's data would learn the fault, so
`advise` says what to look at and a person decides.
"""
import argparse
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
CONTROL_TOPIC = "control/edge/plate-waste/{}"
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


def command_from_artifact(artifact: dict, key: str, shadow_readings=updater.DEFAULT_SHADOW_READINGS,
                          max_mean_abs_diff_g=updater.DEFAULT_MAX_MEAN_ABS_DIFF_G, force=False, now=None) -> dict:
    """A signed set_model command for any artifact. The store path below is the normal one; this is for tests."""
    model = edge_model.EdgeModel(artifact)
    command = {
        "version": updater.CONTROL_VERSION,
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


def build_set_model(store: ModelStore, version: str, key: str, **options) -> dict:
    artifact, _ = store.load(version)
    return command_from_artifact(artifact, key, **options)


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
        self.client = mqtt.Client(client_id=f"edge-control-{uuid.uuid4().hex[:8]}", callback_api_version=mqtt.CallbackAPIVersion.VERSION2)
        if os.environ.get("MQTT_TLS_ENABLED", "false").lower() == "true":
            self.client.tls_set(ca_certs=os.environ.get("MQTT_TLS_CA_FILE", "/etc/mosquitto-tls/tls.crt"))
        self._connected = threading.Event()
        self.client.on_connect = lambda c, u, f, rc, p=None: self._connected.set() if not rc.is_failure else None
        self.client.connect(os.environ.get("MQTT_HOST", "mosquitto"), int(os.environ.get("MQTT_PORT", "1883")), keepalive=30)
        self.client.loop_start()
        if not self._connected.wait(timeout=15):
            raise TimeoutError("could not connect to the MQTT broker")

    def publish(self, topic: str, payload: str, retain: bool) -> None:
        self.client.publish(topic, payload, qos=1, retain=retain).wait_for_publish(timeout=10)

    def collect(self, topic_filter: str, seconds: float) -> dict:
        found = {}
        self.client.on_message = lambda c, u, m: found.__setitem__(m.topic, m.payload)
        self.client.subscribe(topic_filter, qos=1)
        time.sleep(seconds)
        return found

    def close(self) -> None:
        self.client.loop_stop()
        self.client.disconnect()


def main(argv=None, link_factory=Link) -> int:
    parser = argparse.ArgumentParser(prog="edge_control")
    parser.add_argument("--store", default=str(DEFAULT_STORE))
    sub = parser.add_subparsers(dest="action", required=True)
    sub.add_parser("list")
    for name in ("rollout", "rollback"):
        p = sub.add_parser(name)
        p.add_argument("--version" if name == "rollout" else "--to", dest="version", required=True)
        p.add_argument("--nodes", required=True, help="node ids, comma separated, or 'all'")
        if name == "rollout":
            p.add_argument("--shadow-readings", type=int, default=updater.DEFAULT_SHADOW_READINGS)
            p.add_argument("--max-diff-g", type=float, default=updater.DEFAULT_MAX_MEAN_ABS_DIFF_G)
    p = sub.add_parser("clear")
    p.add_argument("--nodes", required=True)
    p = sub.add_parser("status")
    p.add_argument("--wait", type=float, default=3.0)
    sub.add_parser("advise")
    args = parser.parse_args(argv)
    store = ModelStore(args.store)

    if args.action == "list":
        for version in store.versions():
            _, model = store.load(version)
            print(f"{MODEL_ID} {version}  sha256 {model.sha256[:12]}  shift monitor: {'yes' if model.shift_window else 'no'}")
        return 0

    if args.action == "advise":
        body = json.load(sys.stdin)
        for line in advise(body["nodes"] if isinstance(body, dict) else body):
            print(line)
        return 0

    link = link_factory()
    try:
        if args.action in ("rollout", "rollback"):
            key = os.environ.get("EDGE_CONTROL_KEY")
            if not key:
                print("EDGE_CONTROL_KEY is not set: nodes ignore commands that are not signed with it", file=sys.stderr)
                return 2
            rollback = args.action == "rollback"
            command = build_set_model(
                store, args.version, key,
                shadow_readings=0 if rollback else args.shadow_readings,
                max_mean_abs_diff_g=updater.DEFAULT_MAX_MEAN_ABS_DIFF_G if rollback else args.max_diff_g, force=rollback)
            for target in targets(args.nodes):
                link.publish(CONTROL_TOPIC.format(target), json.dumps(command), retain=True)
                print(f"{'rolled back to' if rollback else 'rolled out'} {args.version} to {target} (request {command['request_id']})")
        elif args.action == "clear":
            for target in targets(args.nodes):
                link.publish(CONTROL_TOPIC.format(target), "", retain=True)
                print(f"cleared the desired state for {target}")
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
