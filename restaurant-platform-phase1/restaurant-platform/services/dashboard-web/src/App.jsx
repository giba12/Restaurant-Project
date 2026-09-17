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
        <NarratedFindingsFeed />
      </main>
    </div>
  );
}
