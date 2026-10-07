"""
The plate-waste node.

This node does not report a weight it was handed. It carries (simulated)
sensing hardware, reads four raw channels from it per plate, and runs a small
model on the node itself to estimate the waste in grams; only that estimate,
and the node's own assessment of how far to trust it, leave the node. See
edge_ai/ for the sensors, the model and the guards.

The simulator draws the true waste (so the confounder structure the causal
engine is meant to find is unchanged: a to-go box cuts what is left to a
quarter), but never gives it to the node. `last_true_grams` keeps it on the
simulator side so tests can score the node's estimates; it is never published.
"""
import json
import os
import random

from common import world
from common.ids import new_event_id, now_iso
from common.runtime import Simulator
from edge_ai import sensor
from edge_ai.model import DEFAULT_MODEL_PATH, EdgeModel
from edge_ai.updater import ModelUpdater

PORTION_VARIANTS = ["standard", "standard", "standard", "half", "large", "unknown"]

# 1.1.0: events carry edge_inference. 1.2.0: edge_inference also carries shift_score. 1.3.0: and flatline_score.
# See schemas/PlateWasteEvent.schema.json.
SCHEMA_VERSION = "1.3.0"

# The one fault knob: 0 is a clean lens, 1 a badly fouled one. Set it to see
# the node's drift monitor react; leave it at 0 in normal operation.
LENS_FOULING = float(os.environ.get("EDGE_LENS_FOULING", "0"))

# A named sensor fault (edge_ai/sensor.py FAULTS: a sensor stuck at a normal value, or a gain loss); empty is none.
SENSOR_FAULT = os.environ.get("EDGE_SENSOR_FAULT", "")


DEFAULT_NODE_ID = "sim-plate-cam-01"
CONTROL_TOPIC = "control/edge/plate-waste/{}"
STATUS_TOPIC = "edge/status/plate-waste/{}"


class PlateWasteNode:
    def __init__(self, model: EdgeModel, lens_fouling: float = 0.0, rng=random,
                 node_id: str = DEFAULT_NODE_ID, control_key: str | None = None, sensor_fault: str = "",
                 previous_control_key: str | None = None):
        self.model = model
        self.node_id = node_id
        self.lens_fouling = lens_fouling
        self.sensor_fault = sensor_fault
        self.rng = rng
        # Owns which model is in service: a command from the cloud can change it between two readings
        # (edge_ai/updater.py). The monitors are tied to a model's own calibration, so a swap restarts them.
        self.updater = ModelUpdater(model, node_id=node_id, key=control_key, previous_key=previous_control_key)
        self._adopt(model)
        self.last_true_grams = None  # simulator-side ground truth; never published

    def _adopt(self, model: EdgeModel) -> None:
        self.model = model
        self.drift_monitor = model.new_drift_monitor()
        self.shift_monitor = model.new_shift_monitor()
        self.flatline_monitor = model.new_flatline_monitor()

    def next_event(self) -> dict:
        rng = self.rng
        in_service = self.updater.active
        if in_service is not self.model:
            self._adopt(in_service)
        n_items = rng.randint(1, 3)
        plate_item_ids = rng.sample(world.MENU_ITEM_IDS, k=n_items)

        to_go = rng.random() < 0.15
        dietary = rng.random() < 0.10

        true_grams = sensor.true_waste_grams(rng, to_go)
        self.last_true_grams = true_grams
        reading = sensor.read_sensors(true_grams, rng, self.lens_fouling)

        # From here on the node uses only its sensor channels.
        features = sensor.feature_vector(reading)
        if self.sensor_fault:
            features = [float(v) for v in sensor.inject_fault(features, self.sensor_fault)]
        inference = self.model.infer(features)
        self.updater.observe(features, inference.grams)
        drift_score, spread_alarm = self.drift_monitor.update(inference.ood_score)
        shift_score, shift_alarm = self.shift_monitor.update(inference.deviation)
        flatline_score, flatline_alarm = self.flatline_monitor.update(inference.deviation)

        return {
            "event_id": new_event_id(),
            "event_type": "PlateWasteEvent",
            "schema_version": SCHEMA_VERSION,
            "source_id": self.node_id,
            "source_kind": "simulated",
            "timestamp": now_iso(),
            "restaurant_id": world.RESTAURANT_ID,
            "station_id": "station-bussing-01",
            "table_id": rng.choice(world.TABLES) if rng.random() < 0.7 else None,
            "estimated_waste_grams": inference.grams,
            "plate_item_ids": plate_item_ids,
            "confounder_flags": {
                "to_go_container_used": to_go,
                "declared_dietary_restriction": dietary,
                "portion_size_variant": rng.choice(PORTION_VARIANTS),
            },
            "image_ref": None,
            "edge_inference": {
                "model_id": self.model.model_id,
                "model_version": self.model.model_version,
                "model_sha256": self.model.sha256,
                "inference_latency_ms": inference.latency_ms,
                "ood_score": inference.ood_score,
                "out_of_distribution": inference.out_of_distribution,
                "drift_score": drift_score,
                "shift_score": shift_score,
                "flatline_score": flatline_score,
                "drift_suspected": spread_alarm or shift_alarm or flatline_alarm,
            },
        }


def attach_control(sim, node: PlateWasteNode) -> None:
    """Wire the node to its own control topic (commands in) and its status topic (retained, out).

    There is no fleet-wide topic: every command is signed for one node with that node's key, so it goes to that node's
    topic. The node announces itself with a retained status whenever it connects, which is how the operator finds the
    live nodes."""
    sim.subscribe(CONTROL_TOPIC.format(node.node_id), node.updater.handle)
    node.updater.on_status = lambda status: sim.publish_state(STATUS_TOPIC.format(node.node_id), json.dumps(status))
    if hasattr(sim, "on_connected"):
        sim.on_connected(node.updater.announce)


_default_node = None


def generate_event() -> dict:
    """One event from a process-wide node (what the producer/schema compatibility test calls)."""
    global _default_node
    if _default_node is None:
        _default_node = PlateWasteNode(EdgeModel.from_file(DEFAULT_MODEL_PATH), LENS_FOULING, sensor_fault=SENSOR_FAULT)
    return _default_node.next_event()


def main():
    # Loaded here, before the event loop, on purpose: Simulator.run_forever logs
    # and continues past errors raised while generating an event, so a model
    # that fails its integrity check inside generate_event would be retried
    # forever instead of stopping the pod where someone can see it.
    node = PlateWasteNode(
        EdgeModel.from_file(os.environ.get("EDGE_MODEL_PATH", DEFAULT_MODEL_PATH)), LENS_FOULING,
        node_id=os.environ.get("SOURCE_ID", DEFAULT_NODE_ID), control_key=os.environ.get("EDGE_CONTROL_KEY"),
        sensor_fault=SENSOR_FAULT, previous_control_key=os.environ.get("EDGE_CONTROL_KEY_PREVIOUS"),
    )
    sim = Simulator(
        sensor_type="plate-waste",
        schema_filename="PlateWasteEvent.schema.json",
        mqtt_topic="sensors/plate-waste",
    )
    attach_control(sim, node)
    sim.log.info(
        "plate-waste node: model %s v%s sha256=%s, drift window %d, shift window %s, flatline window %s, lens_fouling=%.2f",
        node.model.model_id, node.model.model_version, node.model.sha256[:12], node.model.drift_window,
        node.model.shift_window, node.model.flatline_window, LENS_FOULING,
    )
    sim.run_forever(node.next_event)


if __name__ == "__main__":
    main()
