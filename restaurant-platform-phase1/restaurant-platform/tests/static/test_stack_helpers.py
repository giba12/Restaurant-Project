"""
Tests of the stack-test helpers that need no stack.

The harness's own checks have been wrong before (a connector-health test that read only the
connector's state, and a container lookup that missed exited containers on Compose v2), so the
parts that can be tested without containers are tested here.

    python -m pytest tests/static/test_stack_helpers.py -v
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import stack_fixture  # noqa: E402

SAMPLE = """# HELP bridge_mqtt_connected 1 while connected to the MQTT broker
# TYPE bridge_mqtt_connected gauge
bridge_mqtt_connected 1.0
# HELP bridge_messages_forwarded_total Messages confirmed by Kafka and acknowledged to MQTT
# TYPE bridge_messages_forwarded_total counter
bridge_messages_forwarded_total{kafka_topic="plate-waste-events"} 12.0
bridge_messages_forwarded_total{kafka_topic="pos-transaction-events"} 40.0
bridge_oldest_unconfirmed_seconds 0.25
"""


def test_metrics_text_is_parsed_into_series_with_their_labels_kept():
    metrics = stack_fixture.parse_metrics(SAMPLE)
    assert metrics["bridge_mqtt_connected"] == 1.0
    assert metrics['bridge_messages_forwarded_total{kafka_topic="plate-waste-events"}'] == 12.0
    assert metrics["bridge_oldest_unconfirmed_seconds"] == 0.25


def test_comments_blank_lines_and_unparseable_lines_are_ignored():
    assert stack_fixture.parse_metrics("# HELP x y\n\nnot a metric line\nx 3\n") == {"x": 3.0}


def test_an_empty_or_missing_metrics_body_gives_no_series_rather_than_an_error():
    assert stack_fixture.parse_metrics("") == {}
