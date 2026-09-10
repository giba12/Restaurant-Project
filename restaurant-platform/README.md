# Restaurant Operations / CX Analytics Platform

Personal portfolio project. Simulated IoT-to-analytics pipeline for restaurant
operations, built contract-first on a local Kubernetes cluster.

## Status

Phase 1 (scaffold cluster and repo) — in progress.

- [x] Kubernetes distribution selected: k3s
- [x] Repo structure created (`schemas/`, `edge-simulators/`, `platform-services/`, `k8s/`)
- [x] Placeholder Helm chart skeletons created for all nine planned services
- [x] Four core event schemas drafted and committed, plus one derived schema (`TicketTimingSummary`)
- [x] `ServiceTimingEvent` stage enum revised to separate cook time from server-pickup delay (`plated` vs `picked_up_by_server` vs `delivered`)
- [ ] k3s installed and cluster running locally
- [ ] Phase 1 done-condition confirmed: cluster live, schema files committed, no application code written against them yet

## Repo layout

```
schemas/             JSON Schema definitions for all event contracts (source of truth)
edge-simulators/      Simulated sensor producer containers (Phase 3)
platform-services/    Causal engine, digital twin, LLM narrator (Phases 5-6)
k8s/charts/           Helm chart per service, one per pipeline component
```

## Design constraints

- All components free/open-source or hand-built; no paid APIs or licensed vendor tools.
- Every simulated component ("fairy dust" stand-in for a real sensor/vendor
  integration) is built against a schema in `schemas/` before any producer or
  consumer code, so it can later be swapped for a real integration (e.g. a
  Winnow webhook, a Toast POS API) without touching downstream code.

## Production considerations (not addressed by this simulation)

- Any real deployment involving employee or customer-facing cameras (e.g. a
  plate-waste vision system) would need to account for state biometric
  privacy statutes and labor-law employee-notice requirements. This
  simulation does not implement or resolve that exposure; it is noted here
  for realism only.
- Plate-waste weight/volume is not evidence of customer dissatisfaction on
  its own — portion size, doggy-bag intent, and dietary restriction are
  unaddressed confounders in any naive correlation. The causal-inference
  layer (Phase 5) is the mechanism intended to address this; raw waste
  metrics should not be surfaced as a dissatisfaction signal without it.
