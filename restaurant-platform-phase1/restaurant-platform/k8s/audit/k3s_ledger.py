"""Cluster ledger: what the simulators logged as published vs what is in the database."""
import re, subprocess, sys, datetime as dt
NS = "kafka"
SIMS = ["edge-sim-plate-waste", "edge-sim-pos-transaction", "edge-sim-service-timing", "edge-sim-staff-shift"]
TABLES = ["plate_waste_events", "pos_transaction_events", "service_timing_events", "staff_shift_events"]
LINE = re.compile(r"(\d{4}-\d\d-\d\d \d\d:\d\d:\d\d),\d+ INFO \[[\w-]+\] published \w+ event_id=([0-9a-f-]{36})")

def sh(cmd, stdin=None, check=True):
    r = subprocess.run(cmd, capture_output=True, text=True, input=stdin)
    if check and r.returncode != 0:
        raise RuntimeError(f"{cmd}: {r.stderr[:300]}")
    return r

def published(since, until):
    out = {}
    for sim in SIMS:
        for previous in (False, True):
            cmd = ["kubectl", "-n", NS, "logs", f"deploy/{sim}", "--since-time", since.strftime("%Y-%m-%dT%H:%M:%SZ")]
            if previous: cmd.append("--previous")
            r = sh(cmd, check=False)
            for line in r.stdout.splitlines():
                m = LINE.search(line)
                if m:
                    at = dt.datetime.strptime(m.group(1), "%Y-%m-%d %H:%M:%S").replace(tzinfo=dt.timezone.utc)
                    if since <= at <= until: out[m.group(2)] = (at, sim.replace("edge-sim-", ""))
    return out

def stored(since):
    ids = set()
    sql = "\n".join(f"SELECT event_id FROM {t} WHERE ingested_at >= '{since:%Y-%m-%d %H:%M:%S}+00';" for t in TABLES)
    r = sh(["kubectl", "exec", "-i", "-n", NS, "timescaledb-0", "--", "sh", "-c", 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -At'], stdin=sql)
    ids.update(l.strip() for l in r.stdout.splitlines() if re.fullmatch(r"[0-9a-f-]{36}", l.strip()))
    return ids

def ledger(since, settle_seconds=60, label=""):
    until = dt.datetime.now(dt.timezone.utc) - dt.timedelta(seconds=settle_seconds)
    pub = published(since, until)
    db = stored(since)
    missing = sorted(((at, sim, i) for i, (at, sim) in pub.items() if i not in db))
    print(f"[{label}] since {since:%H:%M:%S}Z until {until:%H:%M:%S}Z: published {len(pub)}, missing {len(missing)}", flush=True)
    return pub, missing

if __name__ == "__main__":
    since = dt.datetime.strptime(sys.argv[1], "%Y-%m-%dT%H:%M:%S").replace(tzinfo=dt.timezone.utc)
    pub, missing = ledger(since, label="natural restart")
    for at, sim, i in missing[:40]: print("  ", at.strftime("%H:%M:%S"), sim, i[:8])
    if pub:
        times = sorted(at for at, _ in pub.values())
        gaps = [(times[k], times[k+1], (times[k+1]-times[k]).total_seconds()) for k in range(len(times)-1) if (times[k+1]-times[k]).total_seconds() > 120]
        print("publish gaps > 2 min (the outage):", [(a.strftime('%H:%M:%S'), b.strftime('%H:%M:%S'), int(g)) for a,b,g in gaps])
