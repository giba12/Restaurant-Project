"""
The scenario-control consumer shared by the simulators.

A background thread reads 'scenario-control-events' (published by the
scenario-injection controller) and exposes the single most recent
still-active scenario through a plain function, so a simulator's own logic
needs no Kafka knowledge. The import of kafka-python is deferred into this
function so it is not a hard dependency when scenario control is disabled.

Each simulator passes the targets it answers to; a message whose `target` is
not one of them is ignored. A message with target "all" reaches every
simulator that has this hook, which is how one scenario can act on the kitchen
and on the staff together.
"""
import json
import os
import ssl
import threading


def accepts(control: dict, targets) -> bool:
    return control.get("target") in targets


def apply(state: dict, control: dict, targets) -> None:
    """Fold one control message into `state` (the pure core of the consumer thread)."""
    if not accepts(control, targets):
        return
    if control.get("action") == "start":
        state["active"] = control
    elif control.get("action") == "end":
        if state["active"] and state["active"].get("scenario_injection_id") == control.get("scenario_injection_id"):
            state["active"] = None


def make_scenario_getter(group_id: str, targets):
    from kafka import KafkaConsumer

    state = {"active": None}

    # Same opt-in TLS pattern as services/phase5_common.py's KAFKA_TLS_KWARGS
    # (kept local rather than imported -- this package is deliberately
    # separate from services/): defaults to plaintext; a k8s chart switches it
    # over with KAFKA_SECURITY_PROTOCOL=SSL and a mounted CA.
    #
    # ssl_context, not ssl_cafile: kafka-python 2.0.2's own SSLContext
    # construction failed the handshake against this broker for reasons that
    # did not trace to the cert, hostname or network path (see
    # services/phase5_common.py's longer note); ssl_context sidesteps it.
    security_protocol = os.environ.get("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT")
    tls_kwargs = (
        {"security_protocol": security_protocol,
         "ssl_context": ssl.create_default_context(cafile=os.environ.get("KAFKA_SSL_CAFILE", "/etc/kafka-tls/ca.crt"))}
        if security_protocol != "PLAINTEXT"
        else {}
    )

    def run():
        consumer = KafkaConsumer(
            "scenario-control-events",
            bootstrap_servers=os.environ.get(
                "KAFKA_BOOTSTRAP_SERVERS",
                "restaurant-platform-kafka-kafka-bootstrap.kafka.svc.cluster.local:9093",
            ),
            api_version=(2, 8, 0),  # required -- automatic negotiation fails against Kafka 4.3.1
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            group_id=group_id,
            **tls_kwargs,
        )
        for message in consumer:
            apply(state, message.value, targets)

    threading.Thread(target=run, daemon=True).start()
    return lambda: state["active"]
