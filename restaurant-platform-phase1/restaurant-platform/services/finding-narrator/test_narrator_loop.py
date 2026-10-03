"""
Tests for the narrator's whole generate-verify-retry-fallback loop, using a
fake Ollama: a tiny local HTTP server that returns scripted "model output".

The guard's own tests (test_narration_guard.py) prove it can tell a faithful
sentence from a fabricated one. These prove the narrator actually *uses* that
verdict: that a fabricated sentence is retried, that three failures end in the
plain template, that an unreachable model crashes loudly instead of storing
garbage. Because the model is faked, the run is instant, free and
deterministic -- no model is downloaded and no GPU is involved.

    pip install pytest requests
    cd services/finding-narrator && python -m pytest test_narrator_loop.py -v
"""
import http.server
import json
import os
import sys
import threading
import types

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(HERE))
sys.path.insert(0, HERE)
sys.modules.setdefault("kafka", types.SimpleNamespace(KafkaConsumer=None, KafkaProducer=None))
sys.modules.setdefault("psycopg2", types.SimpleNamespace())

import pytest
import requests

import narration_guard as guard
import narrator

FAITHFUL = "Higher staffing raised pickup delay by 7627.92 milliseconds, controlling for station."
FABRICATED = (
    "The treatment variable of staff level has an effect of 7627.92 milliseconds on pickup delay, "
    "which is 1.81% greater than the baseline level of staff."
)
REVERSED = "Higher staffing reduced pickup delay by 7627.92 milliseconds, controlling for station."

FINDING = {
    "finding_id": "f-1", "restaurant_id": "rest-001", "treatment_variable": "staffing_level",
    "outcome_variable": "pickup_delay_ms", "confounders_controlled": ["station_id"],
    "effect_estimate": 7627.916216216181, "effect_estimate_unit": "milliseconds",
    "method": "backdoor.linear_regression", "refutation_passed": True,
}


class FakeOllama:
    """Serves scripted replies in order (repeating the last one) and records every prompt it was sent."""

    def __init__(self, replies, status=200):
        self.replies, self.status, self.prompts = list(replies), status, []
        outer = self

        class Handler(http.server.BaseHTTPRequestHandler):
            def do_POST(self):
                body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
                outer.prompts.append(body["prompt"])
                reply = outer.replies[min(len(outer.prompts) - 1, len(outer.replies) - 1)]
                payload = json.dumps({"response": f"  {reply}\n"}).encode()
                self.send_response(outer.status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)

            def log_message(self, *args):
                pass

        self.server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.url = f"http://127.0.0.1:{self.server.server_port}"
        threading.Thread(target=self.server.serve_forever, daemon=True).start()

    def close(self):
        self.server.shutdown()


@pytest.fixture()
def ollama(monkeypatch):
    created = []

    def make(replies, status=200):
        fake = FakeOllama(replies, status)
        created.append(fake)
        monkeypatch.setattr(narrator, "OLLAMA_HOST", fake.url)
        return fake

    yield make
    for fake in created:
        fake.close()


def test_a_faithful_sentence_is_accepted_first_time_and_labelled_with_the_model(ollama):
    fake = ollama([FAITHFUL])
    text, label = narrator.narrate(FINDING)
    assert (text, label) == (FAITHFUL, narrator.OLLAMA_MODEL)
    assert len(fake.prompts) == 1


def test_a_fabricated_sentence_is_rejected_and_the_retry_is_used(ollama):
    fake = ollama([FABRICATED, FAITHFUL])
    text, label = narrator.narrate(FINDING)
    assert text == FAITHFUL and label == narrator.OLLAMA_MODEL
    assert len(fake.prompts) == 2


def test_a_reversed_direction_is_rejected_and_retried(ollama):
    fake = ollama([REVERSED, FAITHFUL])
    text, _ = narrator.narrate(FINDING)
    assert text == FAITHFUL
    assert len(fake.prompts) == 2


def test_after_the_last_attempt_the_template_is_stored_and_labelled_as_such(ollama):
    fake = ollama([FABRICATED])  # the model never improves
    text, label = narrator.narrate(FINDING)
    assert label == narrator.FALLBACK_MODEL_LABEL == "template-fallback"
    assert len(fake.prompts) == narrator.MAX_ATTEMPTS
    assert guard.check_narration(text, FINDING) == [], "the fallback must itself be verifiably faithful"


def test_nothing_unverified_is_ever_returned(ollama):
    # Whatever the model says, what comes back must pass the guard.
    for replies in ([FABRICATED], [REVERSED], ["x"], [""], [FAITHFUL]):
        ollama(replies)
        text, _ = narrator.narrate(FINDING)
        assert guard.check_narration(text, FINDING) == [], replies


def test_an_unreachable_or_failing_model_raises_rather_than_storing_anything(ollama):
    # The consumer relies on the exception to leave the Kafka offset
    # uncommitted so the message is retried when the model is back.
    ollama([FAITHFUL], status=500)
    with pytest.raises(requests.HTTPError):
        narrator.narrate(FINDING)


def test_the_prompt_contains_the_facts_and_withholds_the_method(ollama):
    fake = ollama([FAITHFUL])
    narrator.narrate(FINDING)
    prompt = fake.prompts[0]
    assert "7627.92" in prompt, "the pre-rounded effect size must be in the prompt"
    assert "station_id" in prompt and "staffing_level" in prompt
    # The finding's method is deliberately withheld from the model: shown it,
    # small models parrot jargon. (The instructions themselves *name* a few
    # forbidden words, so check for the finding's actual value, not the word.)
    assert FINDING["method"] not in prompt
