"""
finding-narrator (Phase 6)

Consumes 'narration-ready-events' (published by causal-engine's
run_reviewer() the moment a finding's narrative_ready flips to true --
NOT causal-findings-events, which is published before the reviewer ever
runs and so always carries narrative_ready=false). For each message,
reads exactly one row from causal_findings by finding_id, phrases it via
a local Ollama model, and writes the result to narrated_findings.

Hard boundary, enforced at the database layer, not just here in
application code: this service connects as the narrator_app role (see
storage/schema/003_phase6.sql), which has SELECT on causal_findings and
nothing else -- no anomaly_events, no raw event tables. The narrator
cannot originate a claim because it is structurally incapable of seeing
anything to originate one from.

That boundary stops it seeing other data, but not a weak model inventing detail
from within the finding it was given, so every generated text is verified by
narration_guard before it is stored (numbers must trace to the finding, direction
must match the sign). A text that fails is regenerated up to NARRATOR_MAX_ATTEMPTS
times, then replaced by a deterministic template; model_used records which one
was stored ("template-fallback" for the template).
"""
import json
import logging
import os
import sys

import requests
from kafka import KafkaConsumer

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import narration_guard as guard
import phase5_common as common

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("finding-narrator")

NARRATION_TOPIC = "narration-ready-events"

OLLAMA_HOST = os.environ.get("OLLAMA_HOST", "http://localhost:11434")
OLLAMA_MODEL = os.environ.get("OLLAMA_MODEL", "qwen2.5:3b-instruct")
MAX_ATTEMPTS = int(os.environ.get("NARRATOR_MAX_ATTEMPTS", "3"))
FALLBACK_MODEL_LABEL = "template-fallback"

PROMPT_TEMPLATE = """You are narrating a single statistical finding for a restaurant operations dashboard. You must describe ONLY the facts given below. Do not invent, assume, or add any cause, number, or detail that is not explicitly present in this data. If a value is null or not given, do not mention it.

Finding:
- Treatment variable: {treatment_variable}
- Outcome variable: {outcome_variable}
- Effect estimate: {effect_estimate} {effect_estimate_unit}
- Confounders controlled for: {confounders_controlled}
- Refutation test passed: {refutation_passed}

Write exactly 1-2 plain-language sentences describing this finding for a restaurant manager. State the direction and magnitude of the effect, and mention that the listed confounders were controlled for. Do not recommend any action. Do not name or describe the statistical method, model, or software used -- it is deliberately not given to you above. Do not use any technical/statistical jargon (e.g. "regression", "backdoor", "estimator", "DoWhy"). Use only the numbers given above, exactly as given. Do not add percentages, ratios, comparisons to a baseline, counts, or any other figure that is not listed above."""


def fetch_finding(conn, finding_id: str) -> dict | None:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT finding_id, restaurant_id, treatment_variable, outcome_variable,
                   confounders_controlled, effect_estimate, effect_estimate_unit,
                   method, refutation_passed
            FROM causal_findings
            WHERE finding_id = %(finding_id)s
            """,
            {"finding_id": finding_id},
        )
        row = cur.fetchone()
        if row is None:
            return None
        columns = [desc[0] for desc in cur.description]
        return dict(zip(columns, row))


def generate(finding: dict) -> str:
    prompt = PROMPT_TEMPLATE.format(
        treatment_variable=finding["treatment_variable"],
        outcome_variable=finding["outcome_variable"],
        # Pre-rounded so the model copies a short number instead of a 15-digit float.
        effect_estimate=("-" if finding["effect_estimate"] < 0 else "") + guard.format_number(finding["effect_estimate"]),
        effect_estimate_unit=finding["effect_estimate_unit"] or "",
        confounders_controlled=", ".join(finding["confounders_controlled"] or []),
        method=finding["method"],
        refutation_passed=finding["refutation_passed"],
    )
    response = requests.post(
        f"{OLLAMA_HOST}/api/generate",
        json={"model": OLLAMA_MODEL, "prompt": prompt, "stream": False},
        # Generous on purpose -- confirmed via real testing that ordinary
        # inference latency for this size of model is a few seconds, but
        # under heavy host CPU/GPU contention (e.g. this project's k3s and
        # Docker Compose paths both running at once) a 60s timeout has
        # actually been hit and crashed this pod. Same reasoning as the
        # Kafka healthcheck timeout in docker-compose.yml.
        timeout=180,
    )
    response.raise_for_status()
    return response.json()["response"].strip()


def narrate(finding: dict) -> tuple[str, str]:
    """Returns (text, model_label). Never returns unverified model output."""
    for attempt in range(1, MAX_ATTEMPTS + 1):
        text = generate(finding)
        problems = guard.check_narration(text, finding)
        if not problems:
            return text, OLLAMA_MODEL
        log.warning(
            "finding_id=%s attempt %d/%d rejected (%s): %r",
            finding["finding_id"], attempt, MAX_ATTEMPTS, "; ".join(problems), text,
        )
    log.warning("finding_id=%s: no attempt passed verification, using the template", finding["finding_id"])
    return guard.render_fallback(finding), FALLBACK_MODEL_LABEL


def insert_narration(conn, finding_id: str, restaurant_id: str, narrative_text: str, model_used: str):
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO narrated_findings (finding_id, restaurant_id, narrative_text, model_used)
            VALUES (%(finding_id)s, %(restaurant_id)s, %(narrative_text)s, %(model_used)s)
            ON CONFLICT (finding_id) DO NOTHING
            """,
            {
                "finding_id": finding_id,
                "restaurant_id": restaurant_id,
                "narrative_text": narrative_text,
                "model_used": model_used,
            },
        )
    conn.commit()


def main():
    consumer = KafkaConsumer(
        NARRATION_TOPIC,
        bootstrap_servers=common.KAFKA_BOOTSTRAP_SERVERS,
        api_version=common.KAFKA_API_VERSION,
        **common.KAFKA_TLS_KWARGS,
        group_id="finding-narrator",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        enable_auto_commit=False,
    )
    conn = common.pg_connect()

    log.info("finding-narrator started, consuming %s (ollama=%s, model=%s)", NARRATION_TOPIC, OLLAMA_HOST, OLLAMA_MODEL)
    for msg in consumer:
        signal = msg.value
        finding_id = signal.get("finding_id")
        try:
            finding = fetch_finding(conn, finding_id)
            if finding is None:
                log.warning("finding_id=%s not found in causal_findings; skipping", finding_id)
                consumer.commit()
                continue
            narrative_text, model_used = narrate(finding)
            insert_narration(conn, finding_id, signal.get("restaurant_id"), narrative_text, model_used)
            log.info("narrated finding_id=%s (%s): %s", finding_id, model_used, narrative_text)
            consumer.commit()
        except Exception:
            conn.rollback()
            log.exception("Failed narrating finding_id=%s; offset not committed", finding_id)
            raise


if __name__ == "__main__":
    main()
