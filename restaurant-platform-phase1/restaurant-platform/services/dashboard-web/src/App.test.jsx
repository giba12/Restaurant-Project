// Component tests for the dashboard. They render the real <App/> with `fetch` replaced by a table of canned
// dashboard-api responses, so what is checked is what a person would see for a given API answer, and which
// routes the page asks for. No browser, no network, no API: run with `npm test`.
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { cleanup, render, screen, within } from "@testing-library/react";
import App from "./App.jsx";

const SHA_A = "a".repeat(64);
const SHA_B = "b".repeat(64);

function node(overrides = {}) {
  return {
    source_id: "sim-plate-cam-01",
    model_id: "plate-waste-edge-regressor",
    model_version: "1.1.0",
    model_sha256: SHA_A,
    readings: 480,
    out_of_distribution_rate: 0.0125,
    drift_rate: 0.0,
    latency_ms_p95: 0.1834,
    drifting_now: false,
    ...overrides,
  };
}

const EMPTY = {
  "/twin/stations": [],
  "/twin/staff": [],
  "/anomalies/summary": [],
  "/findings/narrated": [],
  "/edge/plate-waste?minutes=60": { window_minutes: 60, nodes: [] },
  "/comparison": { scope: { reference_hours: 6 }, vs_simulated: { interactive_tickets: 0 } },
};

// `routes` maps an API path (with its query string) to a body, or to {status} for a failure.
// Every request made is recorded in `calls`; an unknown route fails the test loudly.
function mockApi(routes = {}) {
  const table = { ...EMPTY, ...routes };
  const calls = [];
  vi.stubGlobal("fetch", vi.fn(async (url) => {
    const path = String(url).replace(/^\/api/, "");
    calls.push(path);
    if (!(path in table)) throw new Error(`the page asked for a route the test does not know: ${path}`);
    const entry = table[path];
    if (entry && entry.__status) return { ok: false, status: entry.__status, statusText: entry.statusText || "error", json: async () => ({}) };
    return { ok: true, status: 200, json: async () => entry };
  }));
  return calls;
}
const failure = (status, statusText) => ({ __status: status, statusText });

const edgePanel = () => screen.getByRole("heading", { name: "Edge nodes (plate waste)" }).closest("section");

beforeEach(() => {
  vi.useFakeTimers({ shouldAdvanceTime: true });
});
afterEach(() => {
  cleanup();
  vi.useRealTimers();
  vi.unstubAllGlobals();
});

describe("the edge panel", () => {
  it("asks the API for the last 60 minutes of plate-waste edge estimates", async () => {
    const calls = mockApi();
    render(<App />);
    await screen.findByText(/No edge estimates in the last 60 minutes/);
    expect(calls).toContain("/edge/plate-waste?minutes=60");
  });

  it("says so when no node has reported, rather than showing an empty table", async () => {
    mockApi({ "/edge/plate-waste?minutes=60": { window_minutes: 90, nodes: [] } });
    render(<App />);
    expect(await screen.findByText("No edge estimates in the last 90 minutes.")).toBeTruthy();
    expect(within(edgePanel()).queryByRole("table")).toBeNull();
  });

  it("shows a node's model, volume, distrust rate, drift rate, latency and state", async () => {
    mockApi({ "/edge/plate-waste?minutes=60": { window_minutes: 60, nodes: [node()] } });
    render(<App />);
    const row = (await screen.findByText("sim-plate-cam-01")).closest("tr");
    const cells = within(row).getAllByRole("cell").map((c) => c.textContent);
    expect(cells).toEqual([
      "sim-plate-cam-01",
      "plate-waste-edge-regressor v1.1.0 · aaaaaaaa",
      "480",
      "1.3%",
      "0.0% of readings",
      "0.18 ms",
      "ok",
    ]);
  });

  it("names the whole model hash in a tooltip and only its first eight characters on screen", async () => {
    mockApi({ "/edge/plate-waste?minutes=60": { window_minutes: 60, nodes: [node()] } });
    render(<App />);
    const model = await screen.findByText(/plate-waste-edge-regressor v1\.1\.0/);
    expect(model.getAttribute("title")).toBe(SHA_A);
    expect(model.textContent).not.toContain("a".repeat(9));
  });

  it("marks a node whose drift monitor is alarming right now", async () => {
    mockApi({ "/edge/plate-waste?minutes=60": { window_minutes: 60, nodes: [node({ drifting_now: true, drift_rate: 0.4271 })] } });
    render(<App />);
    const row = (await screen.findByText("sim-plate-cam-01")).closest("tr");
    expect(within(row).getByText("drifting")).toBeTruthy();
    expect(within(row).getByText("42.7% of readings")).toBeTruthy();
    expect(within(row).queryByText("ok")).toBeNull();
  });

  it("shows a dash, not 'NaN ms' or a crash, when a node has no latency figure", async () => {
    mockApi({ "/edge/plate-waste?minutes=60": { window_minutes: 60, nodes: [node({ latency_ms_p95: null })] } });
    render(<App />);
    const row = (await screen.findByText("sim-plate-cam-01")).closest("tr");
    const cells = within(row).getAllByRole("cell").map((c) => c.textContent);
    expect(cells[5]).toBe("–");
  });

  it("shows one row per model, so a fleet running two model versions is visibly mixed", async () => {
    // Two models from one node share a source_id, so the row key must include the model hash: with a clashing key
    // React still draws both rows but may merge or drop them when the data changes, and says so on the console.
    const consoleErrors = vi.spyOn(console, "error").mockImplementation(() => {});
    mockApi({
      "/edge/plate-waste?minutes=60": {
        window_minutes: 60,
        nodes: [
          node({ model_version: "1.1.0", model_sha256: SHA_A }),
          node({ model_version: "1.0.0", model_sha256: SHA_B, readings: 12 }),
        ],
      },
    });
    render(<App />);
    await screen.findAllByText("sim-plate-cam-01");
    const body = within(edgePanel()).getAllByRole("row").slice(1);
    expect(body).toHaveLength(2);
    expect(body[0].textContent).toContain("v1.1.0 · aaaaaaaa");
    expect(body[1].textContent).toContain("v1.0.0 · bbbbbbbb");
    const keyWarnings = consoleErrors.mock.calls.filter((args) => /same key/.test(String(args[0])));
    expect(keyWarnings).toEqual([]);
    consoleErrors.mockRestore();
  });

  it("shows the API's error in its own panel and leaves the other panels working", async () => {
    mockApi({
      "/edge/plate-waste?minutes=60": failure(503, "Service Unavailable"),
      "/twin/stations": [{ station_id: "station-grill-01", open_ticket_count: 3 }],
    });
    render(<App />);
    expect(await within(edgePanel()).findByText("503 Service Unavailable")).toBeTruthy();
    expect(await screen.findByText("station-grill-01")).toBeTruthy();
  });

  it("polls again every 8 seconds, shows the new answer, and stops polling when it goes away", async () => {
    const calls = mockApi({ "/edge/plate-waste?minutes=60": { window_minutes: 60, nodes: [node({ readings: 100 })] } });
    const { unmount } = render(<App />);
    await screen.findByText("100");
    const edgeCalls = () => calls.filter((c) => c.startsWith("/edge/")).length;
    expect(edgeCalls()).toBe(1);

    fetch.mockImplementation(async (url) => {
      calls.push(String(url).replace(/^\/api/, ""));
      const body = String(url).includes("/edge/") ? { window_minutes: 60, nodes: [node({ readings: 250 })] } : [];
      return { ok: true, status: 200, json: async () => (String(url).includes("/comparison") ? EMPTY["/comparison"] : body) };
    });
    await vi.advanceTimersByTimeAsync(8000);
    expect(await screen.findByText("250")).toBeTruthy();
    expect(edgeCalls()).toBe(2);

    unmount();
    await vi.advanceTimersByTimeAsync(30000);
    expect(edgeCalls()).toBe(2);
  });
});

describe("the other panels", () => {
  it("lists station load and staff, with a dash for staff not at a station", async () => {
    mockApi({
      "/twin/stations": [{ station_id: "station-grill-01", open_ticket_count: 4 }, { station_id: "station-fry-01", open_ticket_count: 0 }],
      "/twin/staff": [
        { staff_id: "staff-001", role: "cook", status: "active", station_id: "station-grill-01" },
        { staff_id: "staff-002", role: "server", status: "on_break", station_id: null },
      ],
    });
    render(<App />);
    const grill = (await screen.findAllByText("station-grill-01"))[0].closest("tr");
    expect(within(grill).getAllByRole("cell").map((c) => c.textContent)).toEqual(["station-grill-01", "4"]);
    const server = (await screen.findByText("staff-002")).closest("tr");
    expect(within(server).getAllByRole("cell").map((c) => c.textContent)).toEqual(["staff-002", "server", "on_break", "—"]);
  });

  it("lists anomaly counts by method and severity", async () => {
    mockApi({ "/anomalies/summary": [{ detection_method: "robust_zscore", severity: "high", count: 7 }] });
    render(<App />);
    const row = (await screen.findByText("robust_zscore")).closest("tr");
    expect(within(row).getAllByRole("cell").map((c) => c.textContent)).toEqual(["robust_zscore", "high", "7"]);
  });

  const finding = (overrides) => ({
    finding_id: "f-1",
    narrative_text: "Staffing at the grill fell and delays rose.",
    model_used: "qwen2.5:3b-instruct",
    treatment_variable: "staff_count",
    outcome_variable: "pickup_delay_ms",
    effect_estimate: -2414.2,
    effect_estimate_unit: "ms per extra staff",
    refutation_passed: true,
    narrated_at: "2026-10-06T12:00:00Z",
    ...overrides,
  });

  it("labels a model-written narration 'model · verified' and a fallback one 'template'", async () => {
    mockApi({
      "/findings/narrated": [
        finding({ finding_id: "f-1", narrative_text: "Written by the model." }),
        finding({ finding_id: "f-2", narrative_text: "Written from a template.", model_used: "template-fallback" }),
      ],
    });
    render(<App />);
    const model = (await screen.findByText("Written by the model.")).closest("li");
    const template = (await screen.findByText("Written from a template.")).closest("li");
    expect(within(model).getByText("model · verified")).toBeTruthy();
    expect(within(template).getByText("template")).toBeTruthy();
    expect(within(template).getByText("template").getAttribute("title")).toMatch(/failed the number\/claim checks/);
  });

  it("states the finding's effect and whether it survived refutation", async () => {
    mockApi({ "/findings/narrated": [finding({ refutation_passed: false })] });
    render(<App />);
    const meta = (await screen.findByText(/staff_count → pickup_delay_ms/)).closest("p");
    expect(meta.textContent).toContain("effect -2414.20 ms per extra staff");
    expect(meta.textContent).toContain("refutation not passed");
  });

  it("explains an empty findings feed instead of leaving a blank panel", async () => {
    mockApi();
    render(<App />);
    expect(await screen.findByText(/No findings narrated yet/)).toBeTruthy();
  });

  it("tells you to play a shift when there are no interactive tickets, and compares them when there are", async () => {
    mockApi();
    const empty = render(<App />);
    expect(await screen.findByText(/No completed interactive tickets in the last 6 hours/)).toBeTruthy();
    empty.unmount();

    mockApi({
      "/comparison": {
        scope: { reference_hours: 6 },
        vs_simulated: {
          interactive_tickets: 12,
          simulated_tickets: 340,
          metrics: [{
            metric: "pickup_delay_ms",
            interactive: { median_ms: 21000 },
            simulated: { median_ms: 18500, p90_ms: 40000 },
            interactive_median_faster_than_pct_of_simulated: 37.4,
          }],
        },
        same_clock: { player: { median_ms: 5200, n: 30 }, crew: { median_ms: 4100, n: 210 } },
      },
    });
    render(<App />);
    const row = (await screen.findByText("Pickup delay")).closest("tr");
    expect(within(row).getAllByRole("cell").map((c) => c.textContent)).toEqual(["Pickup delay", "21.0 s", "18.5 s", "40.0 s", "37% of simulated"]);
    expect(screen.getByText(/12 interactive tickets vs 340 simulated/)).toBeTruthy();
  });
});
