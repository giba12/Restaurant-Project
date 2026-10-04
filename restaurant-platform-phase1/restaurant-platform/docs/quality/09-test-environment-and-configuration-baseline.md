# Test Environment and Configuration Baseline

| | |
|---|---|
| Document | Test environment and configuration baseline |
| Project | Restaurant Operations Digital Twin Platform |
| Version | 1.0 |
| Date | 2026-10-03 |
| Baseline | Repository commit `898c7aa` (plus the documents in `docs/quality/`) |
| Status | Records where every result in this document set was obtained |

## 1. Why this exists

A test result is only meaningful with the environment that produced it. Every "verified" in the traceability matrix means *verified here, on this machine, with these tools*. The single most important fact in this document is how narrow that environment is: **one laptop, one container engine, one operating system, no CI run of the new workflows.**

## 2. The test machine

| Item | Value |
|---|---|
| Host | MSI laptop, Windows with WSL2 |
| Linux | Ubuntu 24.04.3 LTS, kernel 6.18.33.2-microsoft-standard-WSL2 |
| CPU | 12 logical processors |
| Memory | 15 GB (7.2 GB in use at one sampling during the test runs, including everything else on the machine) |
| Disk | Local; the project's images total about 14.5 GB |
| Container engine | **Podman 4.9.3, rootless**, with the `podman-docker` shim presenting a `docker` command |
| Compose provider used by the test runs | `docker-compose` 1.29.2 (Compose v1) via the Podman shim |
| Other Compose provider on the machine | The owner's own plugin, `docker-compose` v5.5.1 (`~/.docker/cli-plugins/`); **not used by the stack-layer runs** (DEF-123) |
| Kubernetes | k3s (the owner's development cluster; reached only by manual runs and the audit script, never by automated tests) |
| Helm | v4.2.4 |
| Godot | 4.5 (headless, for the game client's smoke test) |

### Known behaviours of this environment (each cost time; see the defect log)

- Rootless Podman's API socket is **down after a WSL restart**; start it with `systemctl --user start podman.socket` (DEF-121).
- Podman does **not** apply a restart policy after `docker kill` (it counts as a deliberate stop) but does after a real crash (DEF-118).
- A heavily loaded machine can drop WSL's connection to the Podman backend mid-build (DEF-109).
- Rootless containers do not survive a WSL restart; named volumes do.
- Local Python is 3.12; DoWhy 0.11.1 needs 3.11 (DEF-111).

## 3. Test tooling

Installed in a virtual environment at `~/.cache/rp-test-venv` (the exact install line is in the project memory and `TESTING.md`).

| Tool | Version |
|---|---|
| Python (host, layers 1-3, 5-8) | 3.12.3 |
| Python (causal-engine image, layer 4) | 3.11 (`python:3.11-slim`) |
| pytest | 9.1.1 |
| ruff | 0.16.10 |
| pip-audit | latest at run time |
| jsonschema | 4.23.0 |
| PyYAML | 6.0.3 |
| psycopg2-binary | 2.9.9 |
| kafka-python | 3.0.11 |
| pandas / numpy / scikit-learn | 2.2.2 / 1.26.4 / 1.5.1 |
| prometheus-client | 0.26.0 |
| requests | 2.33.0 |
| fastapi / httpx | 0.115.6 / 0.28.1 |
| DoWhy / statsmodels / scipy / networkx (inside the image) | 0.11.1 / 0.14.2 / 1.13.1 / 3.2.1 |

## 4. System under test: configuration baseline

| Component | Version or image |
|---|---|
| Kafka | `docker.io/apache/kafka:4.3.1` (Compose); Kafka 4.3.1 under Strimzi 1.2.0 (k3s) |
| TimescaleDB | `docker.io/timescale/timescaledb-ha:pg16-ts2.16-all` (about 5.7 GB) |
| Mosquitto | `docker.io/library/eclipse-mosquitto:2` |
| Kafka Connect | custom image on `apache/kafka:4.3.1` with the Camel MQTT source connector 4.18.0 |
| Ollama | `docker.io/ollama/ollama:latest` (about 5.2 GB; **unpinned by design**), model `qwen2.5:0.5b-instruct` on Compose, `qwen2.5:3b-instruct` on k3s |
| Application images | `python:3.11-slim`, non-root user 1001; `node:22-slim` and `nginx:1.27-alpine` for the dashboard |
| MinIO | `cgr.dev/chainguard/minio`, pinned by digest (k3s only) |
| Python dependencies | Exact pins in each service's `requirements.txt` (one range, `numpy>=1.26` in the simulators, which the edge node's model now also relies on); the offline trainer's own pins (`scikit-learn==1.5.1`, `numpy==1.26.4`) are in `edge-simulators/training/requirements.txt` and, since 2026-10-04, are audited like every other requirements file |
| Schemas | `schemas/*.schema.json`, JSON Schema 2020-12 |
| Migrations | `storage/schema/001`-`005`, idempotent |

## 5. Test environments, by layer

| Layer | Environment | Isolation |
|---|---|---|
| 1 Static | Host Python plus `helm` | None needed; reads files |
| 2 Unit | Host Python | Kafka, Postgres, MQTT stubbed or faked |
| 3 Integration | Host Python plus a throwaway TimescaleDB container on port 15432 | Own container, removed afterwards |
| 4 Statistical | The causal-engine image, built from `services/causal-engine/Dockerfile` | `docker run --rm`, source mounted read-only |
| 5-7 Stack | Compose project `rp-test`, dashboard on port 18080, 18 services (the three LLM services are omitted); `tests/e2e/docker-compose.test.yml` changes only pacing | Own project name, network, volumes and port; torn down (volumes included) afterwards |
| 8 Security | Host Python plus the internet | None |

### Pacing overlay for the stack layers

| Setting | Real default | Test overlay |
|---|---|---|
| Service-timing events per minute | 20 | 120 |
| Plate-waste events per minute | 8 | 60 |
| POS events per minute | 12 | 60 |
| Staff-shift events per minute | 3 | 30 |
| Anomaly detector minimum window | 30 | 10 |

Nothing else differs from the real `docker-compose.yml`; the services, images and wiring are the shipped ones.

## 6. What the environment cannot tell us

| Not exercised | Consequence |
|---|---|
| A clean machine or a cold image cache | First-run time is unmeasured (the cached runs are fast) |
| Docker Engine, or the owner's Compose plugin | Restart-policy and Compose semantics on Docker are assumed |
| GitHub Actions runners | The `static`, `integration`, nightly and image-publish workflows have never run there |
| GitHub Codespaces | The devcontainer has never been opened |
| A multi-node Kubernetes cluster | Not a goal |
| Real restaurant data | Does not exist |
| A second run of the full stack pass | Only one fully clean pass exists, so flakiness is unmeasured |
| Another operating system | Linux only |

## 7. Reproducing a result

```bash
# one-time: the test environment
python3 -m venv ~/.cache/rp-test-venv
~/.cache/rp-test-venv/bin/pip install pytest pyyaml jsonschema==4.23.0 pandas==2.2.2 numpy==1.26.4 \
  scikit-learn==1.5.1 prometheus-client==0.26.0 psycopg2-binary==2.9.9 kafka-python==3.0.11 ruff \
  pip-audit requests==2.33.0 fastapi==0.115.6 httpx pytest-cov   # pytest-cov only to measure coverage (DEF-128)
export PATH=~/.cache/rp-test-venv/bin:$PATH
systemctl --user start podman.socket          # rootless Podman only, after a WSL restart

cd restaurant-platform-phase1/restaurant-platform
python -m pytest tests/static                       # layer 1
bash tests/integration/run_db_tests.sh               # layer 3
bash tests/statistical/run_statistical_tests.sh --run-slow   # layer 4
bash tests/run_stack_tests.sh full                   # layers 5-7, about an hour
python -m pytest tests/security                      # layer 8 (internet)
```

## 8. Change control for this baseline

Update this document when the host, container engine, Compose provider, Python version, a pinned dependency or an image tag changes, and re-run the layers it affects. The baseline commit is the repository's `HEAD` at the time of the run recorded in `07-test-summary-report.md`.
