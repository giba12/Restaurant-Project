#!/usr/bin/env bash
# Proves, on the live cluster, that the arrival alarm works end to end: a sensor that stops sending is reported by Prometheus,
# reaches Alertmanager, is delivered by the alert relay (a push notification arrives on the phone subscribed to the ntfy topic),
# and clears when the sensor starts again. Nothing else shows it: the rule's own tests (tests/static/test_sensor_alert_rule.py)
# run it on made-up time series, and until this has been run the alarm has never been seen to fire on the cluster.
#
#   bash k8s/audit/test-sensor-alert.sh                       # pauses edge-sim-pos-transaction for about 13 minutes
#   SIM=edge-sim-staff-shift TOPIC=staff-shift-events bash k8s/audit/test-sensor-alert.sh
#
# What it does (namespace $NS):
#   1. refuses to start unless the rule is loaded, the chosen topic is arriving, and nothing is already firing
#   2. scales the chosen simulator to 0 (a stopped simulator generates nothing, so no event is lost; the simulated data has a gap)
#   3. waits for SensorTopicSilent to fire for that topic and for no other (the rule needs ten silent minutes plus two: about 13)
#   4. checks Alertmanager holds the alert and the relay logged a delivery, and no forwarding error
#   5. scales the simulator back to what it was, ALWAYS, even if a check fails or this is interrupted
#   6. waits for the alert to clear
# Exit 0 only if every check passed. Run it when a phone notification is welcome: that is what it proves.
set -Eeuo pipefail

NS="${NS:-kafka}"
SIM="${SIM:-edge-sim-pos-transaction}"
TOPIC="${TOPIC:-pos-transaction-events}"
FIRE_TIMEOUT_MIN="${FIRE_TIMEOUT_MIN:-20}"
CLEAR_TIMEOUT_MIN="${CLEAR_TIMEOUT_MIN:-15}"
POLL_SECONDS="${POLL_SECONDS:-30}"
RETRY_SLEEP="${RETRY_SLEEP:-10}"

failures=0
pass() { echo "PASS  $*"; }
fail() { echo "FAIL  $*"; failures=$((failures + 1)); }

prom() { kubectl exec -n "$NS" deploy/prometheus -- wget -qO- "http://localhost:9090/api/v1/$1"; }
# The names of the topics for which SensorTopicSilent is firing, one per line.
firing_topics() {
  prom 'query?query=ALERTS%7Balertname%3D%22SensorTopicSilent%22%2Calertstate%3D%22firing%22%7D' |
    python3 -c 'import json, sys; [print(r["metric"].get("kafka_topic", "")) for r in json.load(sys.stdin)["data"]["result"]]'
}
rule_loaded() {
  prom rules | python3 -c 'import json, sys; sys.exit(0 if any(r["name"] == "SensorTopicSilent" and r["health"] == "ok" for g in json.load(sys.stdin)["data"]["groups"] for r in g["rules"]) else 1)'
}
topic_arriving() {
  prom 'query?query=increase(bridge_messages_forwarded_total%5B10m%5D)' |
    TOPIC="$TOPIC" python3 -c 'import json, os, sys; sys.exit(0 if any(r["metric"].get("kafka_topic") == os.environ["TOPIC"] and float(r["value"][1]) > 0 for r in json.load(sys.stdin)["data"]["result"]) else 1)'
}
alertmanager_has_it() {
  kubectl exec -n "$NS" deploy/alertmanager -- wget -qO- "http://localhost:9093/api/v2/alerts" |
    TOPIC="$TOPIC" python3 -c 'import json, os, sys; sys.exit(0 if any(a["labels"].get("alertname") == "SensorTopicSilent" and a["labels"].get("kafka_topic") == os.environ["TOPIC"] for a in json.load(sys.stdin)) else 1)'
}
relay_posts() { kubectl logs -n "$NS" deploy/alert-relay 2>/dev/null | grep -c 'POST /alert' || true; }
relay_errors() { kubectl logs -n "$NS" deploy/alert-relay 2>/dev/null | grep -c 'error forwarding alert' || true; }

original=""
scaled_down=0
restore() {
  if [ "$scaled_down" = 1 ]; then
    echo "-- restoring $SIM to $original replica(s)"
    kubectl scale "deploy/$SIM" -n "$NS" --replicas="$original" >/dev/null
    scaled_down=0
  fi
}
trap restore EXIT
trap 'echo "interrupted"; exit 130' INT TERM

echo "== 1. is it safe to start?"
rule_loaded || { fail "the SensorTopicSilent rule is not loaded in Prometheus (helm upgrade observability, restart Prometheus)"; exit 1; }
pass "the rule is loaded and healthy"
topic_arriving || { fail "$TOPIC has not arrived in the last 10 minutes, so a test would prove nothing"; exit 1; }
pass "$TOPIC is arriving"
already="$(firing_topics)"
[ -z "$already" ] || { fail "SensorTopicSilent is already firing for: $already"; exit 1; }
pass "nothing is firing"
original="$(kubectl get "deploy/$SIM" -n "$NS" -o jsonpath='{.spec.replicas}')"
[ "${original:-0}" -ge 1 ] || { fail "$SIM is not running (replicas: ${original:-?})"; exit 1; }
posts_before="$(relay_posts)"
errors_before="$(relay_errors)"

echo "== 2. stop the sensor ($SIM -> 0 replicas)"
kubectl scale "deploy/$SIM" -n "$NS" --replicas=0 >/dev/null
scaled_down=1
started=$SECONDS

echo "== 3. wait for the alert (about 13 minutes; up to $FIRE_TIMEOUT_MIN)"
fired=0
while [ $((SECONDS - started)) -lt $((FIRE_TIMEOUT_MIN * 60)) ]; do
  now="$(firing_topics)"
  if echo "$now" | grep -qx "$TOPIC"; then fired=1; break; fi
  sleep "$POLL_SECONDS"
done
if [ "$fired" = 1 ]; then
  pass "SensorTopicSilent fired for $TOPIC after $(( (SECONDS - started) / 60 )) min $(( (SECONDS - started) % 60 )) s"
  others="$(echo "$now" | grep -vx "$TOPIC" | grep -v '^$' || true)"
  if [ -z "$others" ]; then pass "it fired for no other topic"; else fail "it also fired for: $others"; fi
else
  fail "it did not fire within $FIRE_TIMEOUT_MIN minutes"
fi

echo "== 4. did it get out? (Alertmanager, then the relay; the phone is the last hop and only you can see it)"
if [ "$fired" = 1 ]; then
  for _ in $(seq 1 12); do alertmanager_has_it && break; sleep "$RETRY_SLEEP"; done
  if alertmanager_has_it; then pass "Alertmanager holds the alert"; else fail "Alertmanager does not hold the alert"; fi
  for _ in $(seq 1 30); do [ "$(relay_posts)" -gt "$posts_before" ] && break; sleep "$RETRY_SLEEP"; done
  if [ "$(relay_posts)" -gt "$posts_before" ]; then pass "the relay received it ($(( $(relay_posts) - posts_before )) POST(s))"; else fail "the relay received nothing (Alertmanager waits 30 s to group, then sends)"; fi
  if [ "$(relay_errors)" -le "$errors_before" ]; then pass "the relay logged no forwarding error"; else fail "the relay logged a forwarding error"; fi
fi

echo "== 5. start the sensor again"
restore

echo "== 6. wait for the alert to clear (up to $CLEAR_TIMEOUT_MIN minutes)"
cleared=0
restarted=$SECONDS
while [ $((SECONDS - restarted)) -lt $((CLEAR_TIMEOUT_MIN * 60)) ]; do
  if ! firing_topics | grep -qx "$TOPIC"; then cleared=1; break; fi
  sleep "$POLL_SECONDS"
done
if [ "$cleared" = 1 ]; then pass "the alert cleared $(( (SECONDS - restarted) / 60 )) min $(( (SECONDS - restarted) % 60 )) s after the sensor returned"; else fail "the alert is still firing $CLEAR_TIMEOUT_MIN minutes after the sensor returned"; fi

echo
if [ "$failures" = 0 ]; then
  echo "the arrival alarm fired for the silent sensor, reached the relay, and cleared when the sensor returned. Did a notification arrive on the phone?"
else
  echo "$failures check(s) failed."
  exit 1
fi
