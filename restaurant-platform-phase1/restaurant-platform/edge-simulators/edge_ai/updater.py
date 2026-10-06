"""
How the plate-waste node takes a new model from the cloud, and refuses a bad one.

The cloud sends a signed `set_model` command (see control/edge_control.py) carrying the whole model artifact
(about 5 KB). The node never swaps a model it has not checked, in this order:

  1. The command is signed with the node's shared control key (HMAC-SHA256). With no key configured the node
     takes no commands at all: the MQTT broker is open, so an unsigned command is just anyone's.
  2. It is newer than the last command acted on (`issued_at`), so a retained old command cannot undo a newer one.
  3. The artifact is for this model (same id, same input channels), within the size budget, and passes the
     loader's own hash check; the hash the command declares is the hash the artifact computes.
  4. A known-answer test: the cloud sends probe readings with the grams it computed for them, and the node
     must compute the same. This catches an artifact the node reads differently from the cloud (a numeric or
     format difference), not a bad model: the cloud computed the answers from the very model it is sending.
  5. A latency check against the same budget the repository's tests enforce.
  6. SHADOW: for `shadow_readings` live readings the candidate runs beside the model in service, on the same
     inputs, while the estimates the node publishes still come from the model in service. The candidate is
     promoted only if its estimates agree with the model in service (mean absolute difference under
     `max_mean_abs_diff_g`) and it raised no errors. This is the quality gate. There is no ground truth in the
     field, and the out-of-distribution rate cannot serve: it depends on the inputs, not the weights, so a
     fouled lens would look like a bad model and a bad model fed normal inputs would look fine.

`force` skips step 6 only (a rollback to a known-good older model must not be judged against the faulty model
it replaces). Everything else still applies. A rollback is just a `set_model` for an older version.

What this does not do: it cannot tell a model that is wrong in a way the old one is also wrong, or one that
differs from the old one by less than the tolerance, from a good one; and the model in service after a restart
is the one baked into the image until the retained command arrives again (a few seconds after connecting).
"""
import hashlib
import hmac
import json
import threading
import time

import numpy as np

from edge_ai import model as edge_model

CONTROL_VERSION = 1
DEFAULT_SHADOW_READINGS = 50
DEFAULT_MAX_MEAN_ABS_DIFF_G = 10.0
KNOWN_ANSWER_TOLERANCE_G = 0.05  # the same code on another CPU can differ in the last float32 bits
LATENCY_PROBES = 80


def canonical(body: dict) -> str:
    return json.dumps(body, sort_keys=True, separators=(",", ":"))


def signature(command: dict, key: str) -> str:
    body = {k: v for k, v in command.items() if k != "hmac"}
    return hmac.new(key.encode("utf-8"), canonical(body).encode("utf-8"), hashlib.sha256).hexdigest()


def sign(command: dict, key: str) -> dict:
    return {**{k: v for k, v in command.items() if k != "hmac"}, "hmac": signature(command, key)}


def verified(command: dict, key: str) -> bool:
    given = command.get("hmac")
    return isinstance(given, str) and hmac.compare_digest(given, signature(command, key))


class Rejected(Exception):
    """The command or the candidate model failed a check; the message is the reason reported to the cloud."""


def describe(model) -> dict:
    return {"model_id": model.model_id, "model_version": model.model_version, "sha256": model.sha256}


class ModelUpdater:
    def __init__(self, model, node_id: str, key: str | None = None, on_status=None):
        self._active = model
        self.node_id = node_id
        self._key = key or None
        self.on_status = on_status or (lambda status: None)
        self._lock = threading.Lock()
        self._last_issued = 0.0
        self._candidate = None
        self._shadow = None

    @property
    def active(self):
        with self._lock:
            return self._active

    @property
    def candidate(self):
        with self._lock:
            return self._candidate

    # ---- called from the MQTT thread

    def handle(self, raw: bytes) -> None:
        """One control message. Never raises: whatever is wrong is reported as a status, and the node carries on."""
        if not raw:
            return  # an empty retained message is how the cloud clears a desired state
        request_id = None
        try:
            command = json.loads(raw)
            if not isinstance(command, dict):
                raise Rejected("the command is not a JSON object")
            request_id = command.get("request_id")
            self._process(command)
        except Rejected as exc:
            self._report("rejected", request_id, reason=str(exc))
        except edge_model.ModelIntegrityError as exc:
            self._report("rejected", request_id, reason=f"hash mismatch: {exc}")
        except Exception as exc:  # a malformed command must not take the node down
            self._report("rejected", request_id, reason=f"could not read the command: {type(exc).__name__}: {exc}")

    def _process(self, command: dict) -> None:
        if self._key is None:
            raise Rejected("control is disabled on this node: no EDGE_CONTROL_KEY is set")
        if not verified(command, self._key):
            raise Rejected("bad signature")
        if command.get("version") != CONTROL_VERSION:
            raise Rejected(f"unsupported control version {command.get('version')!r}")
        if command.get("command") != "set_model":
            raise Rejected(f"unknown command {command.get('command')!r}")
        issued = command.get("issued_at")
        if not isinstance(issued, (int, float)) or isinstance(issued, bool):
            raise Rejected("issued_at is missing or not a number")

        with self._lock:
            if issued <= self._last_issued:
                self._report_locked("ignored", command.get("request_id"), reason="older than the last command acted on")
                return
            self._last_issued = issued

        candidate = self._check_artifact(command)
        self._check_known_answers(candidate, command.get("probe"))
        self._check_latency(candidate, command.get("probe"))

        with self._lock:
            if candidate.sha256 == self._active.sha256:
                self._candidate = None
                self._shadow = None
                self._report_locked("unchanged", command.get("request_id"), reason="already running this model")
                return
            shadow_readings = int(command.get("shadow_readings", DEFAULT_SHADOW_READINGS))
            if self._shadow is not None:
                self._report_locked("superseded", self._shadow["request_id"], reason="a newer command replaced this candidate")
                self._candidate = None
                self._shadow = None
            if command.get("force") or shadow_readings <= 0:
                self._promote_locked(candidate, command.get("request_id"), forced=True)
                return
            self._candidate = candidate
            self._shadow = {
                "request_id": command.get("request_id"), "needed": shadow_readings,
                "limit": float(command.get("max_mean_abs_diff_g", DEFAULT_MAX_MEAN_ABS_DIFF_G)),
                "seen": 0, "diff_total": 0.0, "diff_max": 0.0,
            }
            self._report_locked("shadowing", command.get("request_id"), reason=f"comparing on {shadow_readings} live readings")

    def _check_artifact(self, command: dict):
        spec = command.get("model")
        if not isinstance(spec, dict) or not isinstance(spec.get("artifact"), dict):
            raise Rejected("the command carries no model artifact")
        artifact = spec["artifact"]
        if len(canonical(artifact).encode("utf-8")) > edge_model.MAX_ARTIFACT_BYTES:
            raise Rejected(f"the artifact is over the {edge_model.MAX_ARTIFACT_BYTES}-byte size budget")
        candidate = edge_model.EdgeModel(artifact)  # raises ModelIntegrityError on a hash mismatch
        if spec.get("sha256") != candidate.sha256:
            raise Rejected(f"the command declares hash {spec.get('sha256')} but the artifact computes {candidate.sha256}")
        with self._lock:
            active = self._active
        if candidate.model_id != active.model_id:
            raise Rejected(f"this node runs {active.model_id}, not {candidate.model_id}")
        if candidate.feature_names != active.feature_names:
            raise Rejected(f"the model reads {list(candidate.feature_names)}, this node's sensors give {list(active.feature_names)}")
        return candidate

    @staticmethod
    def _probe(probe):
        if not isinstance(probe, list) or not probe:
            raise Rejected("the command carries no known-answer probe")
        return probe

    def _check_known_answers(self, candidate, probe) -> None:
        worst = 0.0
        for item in self._probe(probe):
            worst = max(worst, abs(candidate.infer(item["features"]).grams - float(item["grams"])))
        if worst > KNOWN_ANSWER_TOLERANCE_G:
            raise Rejected(f"known-answer test failed: this node computes up to {worst:.3f} g away from the cloud's answers")

    def _check_latency(self, candidate, probe) -> None:
        vectors = [item["features"] for item in self._probe(probe)]
        timings = []
        for i in range(LATENCY_PROBES):
            start = time.perf_counter()
            candidate.infer(vectors[i % len(vectors)])
            timings.append((time.perf_counter() - start) * 1000.0)
        p95 = float(np.percentile(timings, 95))
        if p95 > edge_model.MAX_INFERENCE_P99_MS:
            raise Rejected(f"inference is too slow: p95 {p95:.2f} ms against a {edge_model.MAX_INFERENCE_P99_MS} ms budget")

    # ---- called from the node's own loop, once per reading, after it has made its own estimate

    def observe(self, features, in_service_grams: float) -> None:
        with self._lock:
            if self._candidate is None:
                return
            shadow = self._shadow
            try:
                candidate_grams = self._candidate.infer(features).grams
                if not np.isfinite(candidate_grams):
                    raise ValueError("a non-finite estimate")
            except Exception as exc:
                self._reject_candidate_locked(f"the candidate failed on a live reading: {type(exc).__name__}: {exc}")
                return
            difference = abs(candidate_grams - in_service_grams)
            shadow["seen"] += 1
            shadow["diff_total"] += difference
            shadow["diff_max"] = max(shadow["diff_max"], difference)
            if shadow["seen"] < shadow["needed"]:
                return
            mean = shadow["diff_total"] / shadow["seen"]
            if mean > shadow["limit"]:
                self._reject_candidate_locked(
                    f"shadow disagreement: mean absolute difference {mean:.2f} g over {shadow['seen']} readings, limit {shadow['limit']:.2f} g")
                return
            self._promote_locked(self._candidate, shadow["request_id"], forced=False)

    # ---- state changes and reports (callers hold the lock)

    def _promote_locked(self, candidate, request_id, forced: bool) -> None:
        shadow = self._shadow
        self._active = candidate
        self._candidate = None
        self._shadow = None
        detail = "promoted without a shadow comparison (forced)" if forced else \
            f"promoted after {shadow['seen']} readings, mean absolute difference {shadow['diff_total'] / shadow['seen']:.2f} g"
        self._report_locked("applied", request_id, reason=detail, shadow=shadow)

    def _reject_candidate_locked(self, reason: str) -> None:
        request_id = self._shadow["request_id"]
        shadow = self._shadow
        self._candidate = None
        self._shadow = None
        self._report_locked("rejected", request_id, reason=reason, shadow=shadow)

    def _report(self, state: str, request_id, reason: str) -> None:
        with self._lock:
            self._report_locked(state, request_id, reason=reason)

    def _report_locked(self, state: str, request_id, reason: str, shadow=None) -> None:
        status = {
            "node_id": self.node_id, "request_id": request_id, "state": state, "reason": reason,
            "active": describe(self._active),
            "candidate": describe(self._candidate) if self._candidate is not None else None,
            "at": time.time(),
        }
        if shadow:
            status["shadow"] = {
                "readings": shadow["seen"],
                "mean_abs_diff_g": round(shadow["diff_total"] / shadow["seen"], 3) if shadow["seen"] else None,
                "max_abs_diff_g": round(shadow["diff_max"], 3),
            }
        try:
            self.on_status(status)
        except Exception:
            pass  # a failed status publish must never change what the node does
