"""
Reshapes Prometheus Alertmanager's webhook payload into ntfy.sh's JSON
publish format and forwards it.

Why this exists: Alertmanager's `webhook_configs` receiver always POSTs its
own fixed JSON schema (status/groupLabels/commonLabels/alerts[]) -- there is
no template option to make it send a different body shape. ntfy has no
native Alertmanager support (confirmed against docs.ntfy.sh/integrations,
2026-09-28): posting Alertmanager's JSON straight to a topic URL would just
show the raw JSON blob as the notification text. The only documented fix is
a small reshaping relay in between; ntfy's own docs list several
community-built ones (alertmanager-ntfy-relay, ntfy-alertmanager, etc.), but
pulling in someone else's binary is exactly the kind of dependency this
project avoids elsewhere (see k8s/observability/Chart.yaml and
k8s/timescaledb/README -- hand-roll from official images/stdlib rather than
depend on a third party that can go stale), so this is a ~60-line stdlib
shim instead.

Deliberately stdlib-only (http.server + urllib.request, no requirements.txt,
no web framework) -- this reshapes one JSON body into another and has no
other job.

Run:   NTFY_TOPIC=my-topic python3 main.py
Test:  python3 -m pytest test_alert_relay.py -v
"""
import json
import os
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

NTFY_SERVER = os.environ.get("NTFY_SERVER", "https://ntfy.sh")
NTFY_TOPIC = os.environ["NTFY_TOPIC"]
PORT = int(os.environ.get("PORT", "8090"))


def build_ntfy_payload(alertmanager_payload: dict) -> dict:
    """Alertmanager webhook schema -> ntfy's base-URL JSON publish schema.

    ntfy's JSON publish form (POST to the bare server URL with a "topic"
    field in the body, rather than POSTing to <server>/<topic> directly) is
    the one documented way to also set title/priority/tags without custom
    headers -- see docs.ntfy.sh/publish/#publish-as-json.
    """
    status = alertmanager_payload.get("status", "unknown")
    alerts = alertmanager_payload.get("alerts", [])
    alertname = alertmanager_payload.get("commonLabels", {}).get("alertname", "alert")

    lines = []
    for alert in alerts:
        name = alert.get("labels", {}).get("alertname", alertname)
        summary = alert.get("annotations", {}).get("summary", "")
        alert_status = alert.get("status", status)
        lines.append(f"[{alert_status}] {name}: {summary}".strip())
    message = "\n".join(lines) if lines else f"{status}: {alertname}"

    return {
        "topic": NTFY_TOPIC,
        "title": f"{alertname} ({status})",
        "message": message,
        "priority": 4 if status == "firing" else 3,
        "tags": ["rotating_light"] if status == "firing" else ["white_check_mark"],
    }


def send_to_ntfy(payload: dict) -> None:
    body = json.dumps(payload).encode()
    req = urllib.request.Request(
        NTFY_SERVER, data=body, headers={"Content-Type": "application/json"}
    )
    with urllib.request.urlopen(req, timeout=10) as resp:
        resp.read()


class AlertHandler(BaseHTTPRequestHandler):
    def do_POST(self):
        length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(length)
        try:
            alertmanager_payload = json.loads(body)
            send_to_ntfy(build_ntfy_payload(alertmanager_payload))
            self.send_response(200)
        except Exception as exc:
            print(f"alert-relay: error forwarding alert: {exc}")
            self.send_response(502)
        self.end_headers()

    def log_message(self, fmt, *args):
        print(f"alert-relay: {fmt % args}")


if __name__ == "__main__":
    server = ThreadingHTTPServer(("0.0.0.0", PORT), AlertHandler)
    print(f"alert-relay listening on :{PORT}, forwarding to {NTFY_SERVER} topic={NTFY_TOPIC}")
    server.serve_forever()
