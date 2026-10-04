import { useEffect, useState } from "react";
import "./App.css";

// In production (served by nginx alongside dashboard-api) this is
// same-origin, proxied by nginx to the API container -- see
// docker-compose.yml / k8s/dashboard's nginx config. In local dev
// (`npm run dev`), set VITE_API_BASE to point at a running dashboard-api.
const API_BASE = import.meta.env.VITE_API_BASE || "/api";

function useApi(path, intervalMs = 4000) {
  const [data, setData] = useState(null);
  const [error, setError] = useState(null);

  useEffect(() => {
    let cancelled = false;
    async function fetchOnce() {
      try {
        const res = await fetch(`${API_BASE}${path}`);
        if (!res.ok) throw new Error(`${res.status} ${res.statusText}`);
        const json = await res.json();
        if (!cancelled) {
          setData(json);
          setError(null);
        }
      } catch (e) {
        if (!cancelled) setError(e.message);
      }
    }
    fetchOnce();
    const id = setInterval(fetchOnce, intervalMs);
    return () => {
      cancelled = true;
      clearInterval(id);
    };
  }, [path, intervalMs]);

  return { data, error };
}

function StationsPanel() {
  const { data, error } = useApi("/twin/stations");
  return (
    <section className="panel">
      <h2>Station load</h2>
      {error && <p className="error">{error}</p>}
      <table>
        <thead>
          <tr><th>Station</th><th>Open tickets</th></tr>
        </thead>
        <tbody>
          {(data || []).map((s) => (
            <tr key={s.station_id}>
              <td>{s.station_id}</td>
              <td>{s.open_ticket_count}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </section>
  );
}

function StaffPanel() {
  const { data, error } = useApi("/twin/staff");
  return (
    <section className="panel">
      <h2>Staff on shift</h2>
      {error && <p className="error">{error}</p>}
      <table>
        <thead>
          <tr><th>Staff</th><th>Role</th><th>Status</th><th>Station</th></tr>
        </thead>
        <tbody>
          {(data || []).map((s) => (
            <tr key={s.staff_id}>
              <td>{s.staff_id}</td>
              <td>{s.role}</td>
              <td>{s.status}</td>
              <td>{s.station_id || "—"}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </section>
  );
}

function AnomaliesPanel() {
  const { data, error } = useApi("/anomalies/summary");
  return (
    <section className="panel">
      <h2>Anomalies detected</h2>
      {error && <p className="error">{error}</p>}
      <table>
        <thead>
          <tr><th>Method</th><th>Severity</th><th>Count</th></tr>
        </thead>
        <tbody>
          {(data || []).map((a, i) => (
            <tr key={i}>
              <td>{a.detection_method}</td>
              <td>{a.severity}</td>
              <td>{a.count}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </section>
  );
}

// The plate-waste node runs a small model on the node itself and reports how far
// to trust each estimate. There is no ground truth in the field, so this panel
// shows the node's own self-assessment, not accuracy: how often it distrusted
// its inputs, and whether its drift monitor is alarming right now.
function EdgePanel() {
  const { data, error } = useApi("/edge/plate-waste?minutes=60", 8000);
  const nodes = data?.nodes || [];
  const pct = (x) => `${(x * 100).toFixed(1)}%`;
  return (
    <section className="panel wide">
      <h2>Edge nodes (plate waste)</h2>
      {error && <p className="error">{error}</p>}
      {data && nodes.length === 0 && (
        <p className="muted">No edge estimates in the last {data.window_minutes} minutes.</p>
      )}
      {nodes.length > 0 && (
        <>
          <p className="muted">
            Each node estimates waste on the node itself and flags readings it does not trust.
            Estimates flagged out-of-distribution, or made while a node is drifting, are left out of causal analysis.
          </p>
          <table>
            <thead>
              <tr><th>Node</th><th>Model</th><th>Readings</th><th>Distrusted</th><th>Drift alarm</th><th>Inference p95</th><th>Now</th></tr>
            </thead>
            <tbody>
              {nodes.map((n) => (
                <tr key={`${n.source_id}-${n.model_sha256}`}>
                  <td>{n.source_id}</td>
                  <td title={n.model_sha256}>{n.model_id} v{n.model_version} · {n.model_sha256.slice(0, 8)}</td>
                  <td>{n.readings}</td>
                  <td>{pct(n.out_of_distribution_rate)}</td>
                  <td>{pct(n.drift_rate)} of readings</td>
                  <td>{n.latency_ms_p95 == null ? "–" : `${n.latency_ms_p95.toFixed(2)} ms`}</td>
                  <td>{n.drifting_now ? "drifting" : "ok"}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </>
      )}
    </section>
  );
}

const METRIC_LABELS = {
  time_to_cook_start_ms: "Time to cook start",
  cook_duration_ms: "Cook time",
  pickup_delay_ms: "Pickup delay",
  service_delay_ms: "Service delay",
  total_ticket_duration_ms: "Whole ticket",
};

function secs(ms) {
  return ms == null ? "–" : `${(ms / 1000).toFixed(1)} s`;
}

// Interactive sessions against the simulated restaurant, and a player
// against the crew. Interactive tickets are kept out of the anomaly baseline,
// so this comparison is where they show up. Empty until someone plays.
function ComparisonPanel() {
  const { data, error } = useApi("/comparison", 10000);
  const vs = data?.vs_simulated;
  const clock = data?.same_clock;
  const hasGame = vs && vs.interactive_tickets > 0;
  return (
    <section className="panel wide">
      <h2>Interactive sessions vs the simulated restaurant</h2>
      {error && <p className="error">{error}</p>}
      {data && !hasGame && (
        <p className="muted">No completed interactive tickets in the last {data.scope.reference_hours} hours. Play a shift, then this compares it with the simulated restaurant.</p>
      )}
      {hasGame && (
        <>
          <p className="muted">
            {vs.interactive_tickets} interactive tickets vs {vs.simulated_tickets} simulated (last {data.scope.reference_hours} h).
            Interactive tickets are quarantined from the anomaly baseline, so they cannot make simulated tickets look abnormal.
          </p>
          <table>
            <thead>
              <tr><th>Stage</th><th>Interactive median</th><th>Simulated median</th><th>Simulated p90</th><th>Interactive median beats</th></tr>
            </thead>
            <tbody>
              {vs.metrics.map((m) => (
                <tr key={m.metric}>
                  <td>{METRIC_LABELS[m.metric] || m.metric}</td>
                  <td>{secs(m.interactive.median_ms)}</td>
                  <td>{secs(m.simulated.median_ms)}</td>
                  <td>{secs(m.simulated.p90_ms)}</td>
                  <td>{m.interactive_median_faster_than_pct_of_simulated == null ? "–" : `${Math.round(m.interactive_median_faster_than_pct_of_simulated)}% of simulated`}</td>
                </tr>
              ))}
            </tbody>
          </table>
          <p className="muted">
            Same clock, players vs crew: players {secs(clock.player.median_ms)} median over {clock.player.n} actions; crew {secs(clock.crew.median_ms)} over {clock.crew.n}.
            Interactive sessions run on a shorter clock than the simulators, so read this as a comparison of pace, not a score.
          </p>
        </>
      )}
    </section>
  );
}

function NarratedFindingsFeed() {
  const { data, error } = useApi("/findings/narrated", 5000);
  return (
    <section className="panel wide">
      <h2>Narrated findings</h2>
      {error && <p className="error">{error}</p>}
      {(data || []).length === 0 && !error && (
        <p className="muted">No findings narrated yet — the pipeline needs a little time to build up a baseline, or trigger one manually (see QUICKSTART.md).</p>
      )}
      <ul className="findings">
        {(data || []).map((f) => (
          <li key={f.finding_id}>
            <p className="narrative">{f.narrative_text}</p>
            <p className="meta">
              <span
                className="badge"
                title={
                  f.model_used === "template-fallback"
                    ? "The language model's draft failed the number/claim checks, so a fixed template built from the finding's own fields was used instead."
                    : "Written by the language model and passed the number/claim checks."
                }
              >
                {f.model_used === "template-fallback" ? "template" : "model · verified"}
              </span>
              {" "}
              {f.treatment_variable} → {f.outcome_variable} · effect {f.effect_estimate?.toFixed(2)} {f.effect_estimate_unit}
              {" · "}refutation {f.refutation_passed ? "passed" : "not passed"}
              {" · "}{new Date(f.narrated_at).toLocaleString()}
            </p>
          </li>
        ))}
      </ul>
    </section>
  );
}

export default function App() {
  return (
    <div className="app">
      <header>
        <h1>Restaurant Platform — Live Dashboard</h1>
        <p className="muted">Digital twin state and causally-attributed findings, updated continuously from the live simulated pipeline.</p>
      </header>
      <main className="grid">
        <StationsPanel />
        <StaffPanel />
        <AnomaliesPanel />
        <EdgePanel />
        <ComparisonPanel />
        <NarratedFindingsFeed />
      </main>
    </div>
  );
}
