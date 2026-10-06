"""
Tests for the model update path: the cloud's model store and signed commands (control/edge_control.py) and the
node's checks before it swaps a model (edge_ai/updater.py, simulators/plate_waste.py).

What they check is that a good model reaches a node and a bad one does not: every way a command or a model can
be wrong (unsigned, tampered, the wrong model, too big, unreadable here, too slow, quietly different) is turned
away with a reason, that the node keeps publishing valid events from the model in service through all of it,
and that a rollback works, including to a version older than the shift monitor.

No broker, Kafka or database: paho is stubbed, and fleets run against a small in-memory broker with retained
messages.

    pip install numpy jsonschema pytest
    cd edge-simulators && python -m pytest test_edge_update.py -v
"""
import copy
import json
import os
import random
import sys
import types

import jsonschema
import pytest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
os.environ.setdefault("SCHEMA_DIR", os.path.join(os.path.dirname(HERE), "schemas"))

_paho = types.ModuleType("paho")
_paho_mqtt = types.ModuleType("paho.mqtt")
_paho_mqtt_client = types.ModuleType("paho.mqtt.client")
_paho_mqtt_client.Client = None
_paho_mqtt.client = _paho_mqtt_client
_paho.mqtt = _paho_mqtt
sys.modules.setdefault("paho", _paho)
sys.modules.setdefault("paho.mqtt", _paho_mqtt)
sys.modules.setdefault("paho.mqtt.client", _paho_mqtt_client)

from control import edge_control as control  # noqa: E402
from edge_ai import model as edge_model  # noqa: E402
from edge_ai import updater  # noqa: E402
from simulators import plate_waste  # noqa: E402

SCHEMA = json.load(open(os.path.join(os.environ["SCHEMA_DIR"], "PlateWasteEvent.schema.json")))
KEY = "test-control-key"
STORE = control.ModelStore()
OLD, NEW = STORE.load("1.0.0")[0], STORE.load("1.1.0")[0]


def node(artifact=NEW, key=KEY, node_id="sim-plate-cam-01", seed=1):
    n = plate_waste.PlateWasteNode(edge_model.EdgeModel(copy.deepcopy(artifact)), rng=random.Random(seed), node_id=node_id, control_key=key)
    statuses = []
    n.updater.on_status = statuses.append
    return n, statuses


def send(n, command):
    n.updater.handle(json.dumps(command).encode())


def command(artifact, **options):
    return control.command_from_artifact(copy.deepcopy(artifact), options.pop("key", KEY), **options)


def readings(n, count):
    events = [n.next_event() for _ in range(count)]
    for event in events:
        jsonschema.validate(event, SCHEMA)  # the contract holds through every swap
    return events


def hashes(events):
    return {e["edge_inference"]["model_sha256"] for e in events}


def biased(artifact, grams, version="9.9.9"):
    """A model that is internally consistent (a valid hash) but 'grams' heavier on every estimate."""
    bad = copy.deepcopy(artifact)
    bad["layers"][-1]["bias"][0] += grams / bad["output_scale_g"]
    bad["model_version"] = version
    bad["weights_sha256"] = edge_model.behaviour_hash(bad)
    return bad


def last(statuses):
    return statuses[-1]


# ------------------------------------------------------------------ a good model gets through

def test_a_valid_model_is_shadowed_then_promoted_and_events_change_models_only_at_promotion():
    n, statuses = node(OLD)
    send(n, command(NEW, shadow_readings=20))
    assert [s["state"] for s in statuses] == ["shadowing"]
    during = readings(n, 19)
    assert hashes(during) == {edge_model.EdgeModel(OLD).sha256}  # still the model in service
    after = readings(n, 5)
    assert [s["state"] for s in statuses] == ["shadowing", "applied"]
    assert hashes(after[:1]) == {edge_model.EdgeModel(OLD).sha256}  # promotion happens inside the 20th reading, after its own event
    assert hashes(after[1:]) == {edge_model.EdgeModel(NEW).sha256}  # from the 21st reading on, events name the new model
    applied = last(statuses)
    assert applied["active"]["model_version"] == "1.1.0" and applied["shadow"]["readings"] == 20
    assert applied["shadow"]["mean_abs_diff_g"] == 0.0  # the weights are the same, so the estimates agree exactly


def test_a_swap_restarts_the_monitors_because_they_belong_to_the_model_calibration():
    n, _ = node(OLD)
    readings(n, 60)  # the old model's spread monitor has filled its window
    send(n, command(NEW, shadow_readings=0))
    events = readings(n, 60)
    assert events[0]["edge_inference"]["drift_score"] is None  # a fresh window after the swap
    assert events[0]["edge_inference"]["shift_score"] is None
    assert events[29]["edge_inference"]["shift_score"] is not None  # the new model's 30-reading shift window
    assert events[48]["edge_inference"]["drift_score"] is None and events[49]["edge_inference"]["drift_score"] is not None


def test_a_model_from_before_the_shift_monitor_loads_and_runs_with_no_shift_score():
    # A rollback to 1.0.0 has to be possible: the 1.1.0 code must still run the artifact it replaced.
    n, _ = node(OLD)
    events = readings(n, 80)
    assert all(e["edge_inference"]["shift_score"] is None for e in events)
    assert events[60]["edge_inference"]["drift_score"] is not None


def test_the_same_model_again_is_reported_as_unchanged_and_not_shadowed():
    n, statuses = node(NEW)
    send(n, command(NEW))
    assert last(statuses)["state"] == "unchanged" and n.updater.candidate is None


def test_a_rollback_to_an_older_version_is_applied_at_once_and_events_follow():
    n, statuses = node(NEW)
    readings(n, 5)
    send(n, command(OLD, shadow_readings=0, force=True))
    assert last(statuses)["state"] == "applied" and "forced" in last(statuses)["reason"]
    events = readings(n, 5)
    assert hashes(events) == {edge_model.EdgeModel(OLD).sha256}


# ------------------------------------------------------------------ every way to be wrong is turned away

def rejected(n, statuses, fragment):
    assert last(statuses)["state"] == "rejected", last(statuses)
    assert fragment in last(statuses)["reason"], last(statuses)["reason"]
    assert n.updater.active.sha256 == edge_model.EdgeModel(OLD).sha256, "the model in service changed"
    assert n.updater.candidate is None


def test_a_node_with_no_control_key_takes_no_commands():
    n, statuses = node(OLD, key=None)
    send(n, command(NEW, shadow_readings=0))
    rejected(n, statuses, "control is disabled")


def test_a_command_signed_with_another_key_is_refused():
    n, statuses = node(OLD)
    send(n, command(NEW, key="somebody-elses-key", shadow_readings=0))
    rejected(n, statuses, "bad signature")


def test_a_command_changed_after_it_was_signed_is_refused():
    n, statuses = node(OLD)
    tampered = command(NEW, shadow_readings=50)
    tampered["shadow_readings"] = 0  # try to skip the shadow comparison
    tampered["force"] = True
    send(n, tampered)
    rejected(n, statuses, "bad signature")


def test_an_artifact_that_no_longer_matches_its_own_hash_is_refused():
    corrupted = copy.deepcopy(NEW)
    corrupted["layers"][0]["weights_int8"][0][0] += 1  # one weight changed, the declared hash left alone
    n, statuses = node(OLD)
    body = command(NEW, shadow_readings=0)
    body["model"]["artifact"] = corrupted
    send(n, updater.sign(body, KEY))
    rejected(n, statuses, "hash mismatch")


def test_a_command_whose_declared_hash_is_not_the_artifacts_is_refused():
    n, statuses = node(OLD)
    body = command(NEW, shadow_readings=0)
    body["model"]["sha256"] = "0" * 64
    send(n, updater.sign(body, KEY))
    rejected(n, statuses, "declares hash")


def test_a_model_for_a_different_job_is_refused():
    other = copy.deepcopy(NEW)
    other["model_id"] = "something-else"
    other["weights_sha256"] = edge_model.behaviour_hash(other)
    n, statuses = node(OLD)
    send(n, command(other, shadow_readings=0))
    rejected(n, statuses, "this node runs plate-waste-edge-regressor")


def test_a_model_that_reads_different_sensor_channels_is_refused():
    other = copy.deepcopy(NEW)
    other["feature_names"] = ["scale_g", "area_frac", "height_mm", "temperature_c"]
    other["weights_sha256"] = edge_model.behaviour_hash(other)
    n, statuses = node(OLD)
    send(n, command(other, shadow_readings=0))
    rejected(n, statuses, "this node's sensors give")


def test_a_node_that_computes_different_answers_from_the_clouds_refuses_the_model():
    n, statuses = node(OLD)
    body = command(NEW, shadow_readings=0)
    body["probe"][3]["grams"] += 5.0  # what the cloud claims this model gives for a probe reading
    send(n, updater.sign(body, KEY))
    rejected(n, statuses, "known-answer test failed")


def test_a_command_with_no_probe_is_refused_not_trusted():
    n, statuses = node(OLD)
    body = command(NEW, shadow_readings=0)
    del body["probe"]
    send(n, updater.sign(body, KEY))
    rejected(n, statuses, "no known-answer probe")


def test_an_artifact_over_the_size_budget_is_refused_even_though_its_hash_is_valid():
    fat = copy.deepcopy(NEW)
    fat["card"]["padding"] = "x" * (edge_model.MAX_ARTIFACT_BYTES + 100)  # the card is documentation, so not hashed
    assert edge_model.EdgeModel(fat).sha256 == edge_model.EdgeModel(NEW).sha256
    n, statuses = node(OLD)
    send(n, command(fat, shadow_readings=0))
    rejected(n, statuses, "size budget")


def test_a_model_too_slow_for_the_latency_budget_is_refused(monkeypatch):
    n, statuses = node(OLD)
    monkeypatch.setattr(edge_model, "MAX_INFERENCE_P99_MS", 1e-6)  # a budget no model can meet stands for a slow one
    send(n, command(NEW, shadow_readings=0))
    rejected(n, statuses, "inference is too slow")


@pytest.mark.parametrize("payload", [b"not json", b"[1, 2]", b"{}", b'{"command": "set_model"}', b'"text"'], ids=["text", "list", "empty-object", "unsigned", "string"])
def test_garbage_is_reported_and_never_raises(payload):
    n, statuses = node(OLD)
    n.updater.handle(payload)
    assert last(statuses)["state"] == "rejected"
    assert n.updater.active.sha256 == edge_model.EdgeModel(OLD).sha256
    readings(n, 3)  # the node carries on


def test_an_empty_message_is_how_a_desired_state_is_cleared_and_is_not_an_error():
    n, statuses = node(OLD)
    n.updater.handle(b"")
    assert statuses == []


# ------------------------------------------------------------------ the shadow comparison is the quality gate

def test_a_model_that_disagrees_with_the_one_in_service_is_rejected_after_the_shadow_and_never_published():
    heavy = biased(NEW, grams=30.0)
    n, statuses = node(OLD)
    send(n, command(heavy, shadow_readings=25))
    assert last(statuses)["state"] == "shadowing"  # everything else about it is valid
    events = readings(n, 30)
    assert last(statuses)["state"] == "rejected" and "shadow disagreement" in last(statuses)["reason"]
    assert last(statuses)["shadow"]["mean_abs_diff_g"] == pytest.approx(30.0, abs=0.5)
    assert hashes(events) == {edge_model.EdgeModel(OLD).sha256}
    assert n.updater.active.sha256 == edge_model.EdgeModel(OLD).sha256


def test_a_model_that_differs_within_the_tolerance_is_promoted():
    slight = biased(NEW, grams=3.0, version="1.1.1")
    n, statuses = node(OLD)
    send(n, command(slight, shadow_readings=20, max_mean_abs_diff_g=10.0))
    readings(n, 21)
    assert last(statuses)["state"] == "applied" and n.updater.active.model_version == "1.1.1"


def test_force_skips_the_shadow_comparison_so_a_rollback_is_not_judged_against_the_model_it_replaces():
    heavy = biased(NEW, grams=30.0)
    n, statuses = node(OLD)
    send(n, command(heavy, force=True))  # force is for known-good versions; it does not weaken the other checks
    assert last(statuses)["state"] == "applied"


def test_force_does_not_skip_the_hash_check():
    corrupted = copy.deepcopy(NEW)
    corrupted["layers"][0]["bias"][0] += 0.5
    n, statuses = node(OLD)
    body = command(NEW, force=True)
    body["model"]["artifact"] = corrupted
    send(n, updater.sign(body, KEY))
    rejected(n, statuses, "hash mismatch")


def test_a_candidate_that_fails_on_a_live_reading_is_dropped_and_the_node_is_unaffected(monkeypatch):
    n, statuses = node(OLD)
    send(n, command(NEW, shadow_readings=30))
    candidate = n.updater.candidate

    def explode(features):
        raise RuntimeError("numeric trouble")

    monkeypatch.setattr(candidate, "infer", explode)
    events = readings(n, 5)
    assert last(statuses)["state"] == "rejected" and "failed on a live reading" in last(statuses)["reason"]
    assert hashes(events) == {edge_model.EdgeModel(OLD).sha256} and n.updater.candidate is None


def test_a_newer_command_replaces_a_candidate_still_being_shadowed():
    n, statuses = node(OLD)
    send(n, command(biased(NEW, grams=3.0, version="1.1.1"), shadow_readings=50, now=1000.0))
    send(n, command(NEW, shadow_readings=50, now=2000.0))
    assert [s["state"] for s in statuses] == ["shadowing", "superseded", "shadowing"]
    assert n.updater.candidate.model_version == "1.1.0"


def test_an_older_command_cannot_undo_a_newer_one():
    n, statuses = node(NEW)
    send(n, command(OLD, shadow_readings=0, force=True, now=2000.0))
    send(n, command(NEW, shadow_readings=0, force=True, now=1000.0))  # a stale retained message arriving late
    assert last(statuses)["state"] == "ignored" and n.updater.active.model_version == "1.0.0"


# ------------------------------------------------------------------ a fleet, over a broker that retains

class Broker:
    def __init__(self):
        self.retained = {}
        self.subscribers = {}

    def publish(self, topic, payload, retain=False):
        if retain:
            self.retained[topic] = payload
        for handler in self.subscribers.get(topic, []):
            handler(payload if isinstance(payload, bytes) else payload.encode())

    def subscribe(self, topic, handler):
        self.subscribers.setdefault(topic, []).append(handler)
        if topic in self.retained:
            handler(self.retained[topic] if isinstance(self.retained[topic], bytes) else self.retained[topic].encode())


class Sim:
    def __init__(self, broker):
        self.broker = broker

    def subscribe(self, topic, handler):
        self.broker.subscribe(topic, handler)

    def publish_state(self, topic, payload):
        self.broker.publish(topic, payload, retain=True)


def fleet(broker, ids, artifact=OLD):
    nodes = {}
    for i, node_id in enumerate(ids):
        n, _ = node(artifact, node_id=node_id, seed=10 + i)
        plate_waste.attach_control(Sim(broker), n)
        nodes[node_id] = n
    return nodes


def running(nodes):
    return {i: n.updater.active.model_version for i, n in nodes.items()}


def test_a_canary_reaches_one_node_first_and_the_rest_follow_when_the_whole_fleet_is_told():
    broker = Broker()
    nodes = fleet(broker, ["cam-a", "cam-b", "cam-c"])
    broker.publish(control.CONTROL_TOPIC.format("cam-a"), json.dumps(command(NEW, shadow_readings=10, now=1000.0)), retain=True)
    for n in nodes.values():
        readings(n, 12)
    assert running(nodes) == {"cam-a": "1.1.0", "cam-b": "1.0.0", "cam-c": "1.0.0"}
    broker.publish(control.CONTROL_TOPIC.format("all"), json.dumps(command(NEW, shadow_readings=10, now=2000.0)), retain=True)
    for n in nodes.values():
        readings(n, 12)
    assert running(nodes) == {"cam-a": "1.1.0", "cam-b": "1.1.0", "cam-c": "1.1.0"}
    assert json.loads(broker.retained[control.STATUS_TOPIC.format("cam-a")])["state"] == "unchanged"
    assert json.loads(broker.retained[control.STATUS_TOPIC.format("cam-b")])["state"] == "applied"


def test_a_canary_that_is_rejected_leaves_the_rest_of_the_fleet_untouched():
    broker = Broker()
    nodes = fleet(broker, ["cam-a", "cam-b"])
    broker.publish(control.CONTROL_TOPIC.format("cam-a"), json.dumps(command(biased(NEW, 40.0), shadow_readings=10)), retain=True)
    for n in nodes.values():
        readings(n, 12)
    assert running(nodes) == {"cam-a": "1.0.0", "cam-b": "1.0.0"}
    assert json.loads(broker.retained[control.STATUS_TOPIC.format("cam-a")])["state"] == "rejected"
    assert control.STATUS_TOPIC.format("cam-b") not in broker.retained


def test_a_node_that_restarts_is_told_again_because_the_desired_state_is_retained():
    broker = Broker()
    first = fleet(broker, ["cam-a"])["cam-a"]
    broker.publish(control.CONTROL_TOPIC.format("all"), json.dumps(command(NEW, shadow_readings=5)), retain=True)
    readings(first, 6)
    assert first.updater.active.model_version == "1.1.0"
    reborn = fleet(broker, ["cam-a"])["cam-a"]  # the pod restarted onto the model baked into its image
    assert reborn.updater.candidate is not None  # told again at once
    readings(reborn, 6)
    assert reborn.updater.active.model_version == "1.1.0"


def test_the_newest_command_wins_when_a_node_is_told_by_both_its_own_topic_and_the_fleets():
    broker = Broker()
    broker.publish(control.CONTROL_TOPIC.format("all"), json.dumps(command(NEW, shadow_readings=0, force=True, now=1000.0)), retain=True)
    broker.publish(control.CONTROL_TOPIC.format("cam-a"), json.dumps(command(OLD, shadow_readings=0, force=True, now=2000.0)), retain=True)
    n = fleet(broker, ["cam-a"], artifact=NEW)["cam-a"]
    assert n.updater.active.model_version == "1.0.0"


def test_attaching_control_subscribes_to_the_nodes_own_topic_and_the_fleets_and_reports_on_a_retained_status():
    broker = Broker()
    n = fleet(broker, ["cam-a"])["cam-a"]
    assert set(broker.subscribers) == {"control/edge/plate-waste/cam-a", "control/edge/plate-waste/all"}
    n.updater.handle(b"garbage")
    assert control.STATUS_TOPIC.format("cam-a") in broker.retained


# ------------------------------------------------------------------ the model store and the cloud's commands

def test_versions_sort_as_numbers_not_as_text(tmp_path):
    folder = tmp_path / control.MODEL_ID
    folder.mkdir()
    for v in ("1.9.0", "1.10.0", "1.2.0"):
        (folder / f"{v}.json").write_text("{}")
    assert control.ModelStore(tmp_path).versions() == ["1.2.0", "1.9.0", "1.10.0"]


def test_the_store_refuses_a_stored_model_that_no_longer_matches_its_hash(tmp_path):
    folder = tmp_path / control.MODEL_ID
    folder.mkdir()
    tampered = copy.deepcopy(NEW)
    tampered["layers"][0]["bias"][0] += 0.1
    (folder / "1.1.0.json").write_text(json.dumps(tampered))
    with pytest.raises(edge_model.ModelIntegrityError):
        control.ModelStore(tmp_path).load("1.1.0")


def test_the_store_refuses_a_file_whose_name_is_not_the_version_inside_it(tmp_path):
    folder = tmp_path / control.MODEL_ID
    folder.mkdir()
    (folder / "2.0.0.json").write_text(json.dumps(NEW))  # version 1.1.0 filed as 2.0.0
    with pytest.raises(ValueError, match="claims to be"):
        control.ModelStore(tmp_path).load("2.0.0")


def test_asking_for_a_version_the_store_does_not_have_says_what_it_does_have():
    with pytest.raises(FileNotFoundError, match="have: 1.0.0, 1.1.0"):
        STORE.load("7.7.7")


def test_every_model_in_the_committed_store_verifies_and_the_newest_is_the_one_baked_into_the_image():
    versions = STORE.versions()
    assert versions[:2] == ["1.0.0", "1.1.0"]
    for version in versions:
        STORE.load(version)  # verifies hash, id and version against the file name
    newest = versions[-1]
    baked = open(edge_model.DEFAULT_MODEL_PATH, "rb").read()
    stored = open(os.path.join(control.DEFAULT_STORE, control.MODEL_ID, f"{newest}.json"), "rb").read()
    assert baked == stored, "the model baked into the image is not the newest in the store"


def test_a_stored_model_is_immutable_the_first_version_is_byte_for_byte_what_was_first_shipped():
    # Hash of the model that ran on the live cluster before the shift monitor existed (model card, version 1.0.0).
    assert STORE.load("1.0.0")[1].sha256 == "155f41cd2a84a57d486e1af427ffc042536ac6a154e493a02243ab4d8ff47be3"


def test_a_built_command_is_signed_and_carries_sixteen_known_answers_the_node_can_check():
    built = control.build_set_model(STORE, "1.1.0", KEY)
    assert updater.verified(built, KEY) and not updater.verified(built, "another-key")
    assert len(built["probe"]) == 16 and built["force"] is False
    model = edge_model.EdgeModel(STORE.load("1.1.0")[0])
    assert all(abs(model.infer(p["features"]).grams - p["grams"]) < 1e-9 for p in built["probe"])


# ------------------------------------------------------------------ the command line

class FakeLink:
    def __init__(self):
        self.published = []
        self.closed = False
        self.replies = {}

    def publish(self, topic, payload, retain):
        self.published.append((topic, payload, retain))

    def collect(self, topic_filter, seconds):
        return self.replies

    def close(self):
        self.closed = True


def cli(argv, monkeypatch, replies=None):
    link = FakeLink()
    link.replies = replies or {}
    code = control.main(argv, link_factory=lambda: link)
    return code, link


def test_rollout_publishes_a_retained_signed_shadowed_command_to_each_named_node(monkeypatch):
    monkeypatch.setenv("EDGE_CONTROL_KEY", KEY)
    code, link = cli(["rollout", "--version", "1.1.0", "--nodes", "cam-a, cam-b", "--shadow-readings", "12"], monkeypatch)
    assert code == 0 and link.closed
    assert [t for t, _, _ in link.published] == ["control/edge/plate-waste/cam-a", "control/edge/plate-waste/cam-b"]
    assert all(retain for _, _, retain in link.published)
    sent = json.loads(link.published[0][1])
    assert updater.verified(sent, KEY) and sent["shadow_readings"] == 12 and sent["force"] is False


def test_rollback_is_a_forced_rollout_of_an_older_version_with_no_shadow(monkeypatch):
    monkeypatch.setenv("EDGE_CONTROL_KEY", KEY)
    code, link = cli(["rollback", "--to", "1.0.0", "--nodes", "all"], monkeypatch)
    sent = json.loads(link.published[0][1])
    assert code == 0 and link.published[0][0] == "control/edge/plate-waste/all"
    assert sent["force"] is True and sent["shadow_readings"] == 0 and sent["model"]["model_version"] == "1.0.0"


def test_a_rollout_without_a_key_publishes_nothing(monkeypatch, capsys):
    monkeypatch.delenv("EDGE_CONTROL_KEY", raising=False)
    code, link = cli(["rollout", "--version", "1.1.0", "--nodes", "all"], monkeypatch)
    assert code == 2 and link.published == []
    assert "EDGE_CONTROL_KEY is not set" in capsys.readouterr().err


def test_a_rollout_of_a_version_that_is_not_in_the_store_publishes_nothing(monkeypatch):
    monkeypatch.setenv("EDGE_CONTROL_KEY", KEY)
    link = FakeLink()
    with pytest.raises(FileNotFoundError):
        control.main(["rollout", "--version", "7.7.7", "--nodes", "all"], link_factory=lambda: link)
    assert link.published == [] and link.closed


def test_clear_publishes_an_empty_retained_message_which_removes_the_desired_state(monkeypatch):
    code, link = cli(["clear", "--nodes", "cam-a"], monkeypatch)
    assert code == 0 and link.published == [("control/edge/plate-waste/cam-a", "", True)]


def test_status_prints_each_nodes_state_and_model(monkeypatch, capsys):
    status = {"node_id": "cam-a", "state": "applied", "reason": "promoted", "active": {"model_version": "1.1.0", "sha256": "a" * 64}, "candidate": None}
    code, _ = cli(["status", "--wait", "0"], monkeypatch, replies={"edge/status/plate-waste/cam-a": json.dumps(status).encode()})
    assert code == 0 and "cam-a: applied - promoted; running 1.1.0 (aaaaaaaaaaaa)" in capsys.readouterr().out


# ------------------------------------------------------------------ what the cloud advises (never acts on)

def row(**overrides):
    return {"source_id": "cam-a", "model_id": "m", "model_version": "1.1.0", "model_sha256": "a" * 64, "readings": 100,
            "out_of_distribution_rate": 0.002, "drift_rate": 0.0, "drifting_now": False, **overrides}


def test_a_healthy_single_model_fleet_has_nothing_to_look_at():
    assert "Nothing to look at" in control.advise([row()])[0]


def test_a_drifting_node_is_told_to_have_its_sensor_inspected_and_no_model_change_is_recommended():
    (line,) = control.advise([row(drifting_now=True)])
    assert "DRIFT" in line and "Inspect and clean the sensor first" in line and "No model change is recommended" in line
    assert "rollout" not in line.lower()


def test_a_node_that_distrusts_many_readings_is_pointed_at_its_sensor():
    assert "outside the training data" in control.advise([row(out_of_distribution_rate=0.2)])[0]


def test_one_node_estimating_with_two_models_is_reported_as_a_rollout_or_a_restart():
    rows = [row(), row(model_version="1.0.0", model_sha256="b" * 64, readings=12)]
    (line,) = control.advise(rows)
    assert "more than one model" in line and "1.1.0 (100 readings), 1.0.0 (12 readings)" in line


def test_a_fleet_running_different_models_on_different_nodes_is_reported():
    lines = control.advise([row(), row(source_id="cam-b", model_version="1.0.0", model_sha256="b" * 64)])
    assert any("different models on different nodes" in line for line in lines)


def test_the_advise_command_reads_the_api_answer_and_never_opens_a_connection_to_the_broker(monkeypatch, capsys):
    import io

    def no_broker():
        raise AssertionError("advise must not connect to the broker")

    monkeypatch.setattr(sys, "stdin", io.StringIO(json.dumps({"window_minutes": 60, "nodes": [row(drifting_now=True)]})))
    assert control.main(["advise"], link_factory=no_broker) == 0
    assert "DRIFT" in capsys.readouterr().out
