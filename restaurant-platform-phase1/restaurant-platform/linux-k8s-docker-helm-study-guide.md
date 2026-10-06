# Study Guide: Linux, Docker/Podman, Kubernetes, and Helm

**Purpose:** general-context reference covering the tools and concepts used while building the Kafka/Kubernetes portion of a separate project. Organized by tool, not by project timeline. Each section pairs a concept with the actual command(s) used as a working example, followed by why it works the way it does.

**Revision note:** Sections 1–5 (through the original Quick-Reference table) and Section 6–7 (Python packaging, `ENTRYPOINT` semantics) are unchanged from the prior revision. Section 2.7 (Podman's implicit `localhost/` image-name prefix), Section 3.8 (Helm hook timeouts vs. real resource failures), Section 4.7 (external Helm chart dependencies going stale), Section 6.3 (Python base-image version vs. library compatibility), and Section 8 (a new top-level section on container image/chart supply-chain volatility) were added mid-Phase-4. This revision adds Section 3.9 (Kubernetes `$(VAR)` env-value substitution ordering) and Section 6.4 (bypassing a broken client-library auto-negotiation via an explicit version pin), added at the close of the Phase 4 storage-layer build, once the storage-consumer was confirmed fully working end-to-end. The Quick-Reference table is extended accordingly at the end.

---

## 1. Linux Fundamentals

### 1.1. Working directory discipline

Every shell command that takes a relative path resolves it against the **current working directory**, not against any notion of a "project root." This is a frequent source of "file not found" errors that look like tooling bugs but are not.

```
cd ~/'res proj'/restaurant-platform-phase1/restaurant-platform
```

Single quotes around a path segment (`'res proj'`) prevent the shell from splitting it into two separate arguments at the space. Always confirm your actual location with `pwd` before running a relative-path command if there's any doubt.

**General rule:** if a command fails with "No such file or directory" and the file *should* exist, check `pwd` before questioning the command itself. This same principle applies beyond plain shell commands — see Section 4.4 (a `helm template` path-resolution failure) and, as of Phase 4, extends further still to script-relative failures: running `bash storage/consumer/build.sh` failed with "No such file or directory" simply because the extracted delivery had landed one directory level deeper than expected (`phase4/storage/consumer/build.sh`) — same root cause, a relative path not resolving where assumed, surfaced yet again through a different-looking error.

### 1.2. Locating files/directories you know exist somewhere

```
find ~ -maxdepth 4 -type d -name 'kafka-strimzi' 2>/dev/null
```

`-maxdepth` bounds the search, `-type d` restricts to directories, `2>/dev/null` discards permission-denied noise. Generalizes to locating a Helm chart:

```
find k8s -name "Chart.yaml"
```

**Phase 4 addition:** this same pattern is the correct way to *verify a documented convention against the live repo* rather than trust a written description of it. `find . -maxdepth 4 -name 'Chart.yaml'` was what settled a real discrepancy between this project's own implementation-status document (which claimed `k8s/charts/<service>/`) and the actual repo layout (`k8s/<service>/`, no `charts/` segment) — a one-line `find` resolved in seconds what could otherwise have caused a repeat of the exact path-mismatch class of error documented in Section 4.4.

### 1.3. Heredocs for writing multi-line files

```
cat > filename.yaml << 'EOF'
line 1
line 2
EOF
```

`<< 'EOF'` (quoted delimiter) treats everything between the markers as literal text, no variable expansion. **Known failure mode:** pasting a large heredoc block quickly into some terminal emulators can cause the terminal's *display* to lag and show interleaved/duplicated lines — a rendering artifact, not necessarily data corruption, but not assumable as harmless either. Always verify with `cat filename.yaml` after a suspicious-looking paste. This also recurred with inline `python3 -c "..."` commands (see Section 6); the mitigation adopted is to always write multi-line content to a real file first, inspect it, then execute/apply it.

### 1.4. File permissions

```
find . -type f -exec chmod 644 {} \;
find . -type d -exec chmod 755 {} \;
```

`644` for regular files, `755` for directories/executables. `777`/`666`-style permissions are almost never correct outside genuinely shared, low-trust scratch space.

### 1.5. Backgrounding and job control

```
some_interactive_command &
```

Backgrounds a process, returning control immediately. **Caveat:** a backgrounded process holding a TTY (`-it`) receives `SIGTTIN`/`SIGTTOU` and shows `Stopped` — a structural shell/TTY interaction, not a timing bug. Fix: two separate terminal sessions, or drop TTY allocation and redirect to a file.

### 1.6. systemd services

```
sudo systemctl status <service>
sudo systemctl start <service>
sudo journalctl -u <service> -n 100 --no-pager
```

`journalctl -u` pulls a specific service's log stream, usually where the actual error detail lives when `systemctl status` alone is inconclusive.

### 1.7. `sudo` access vs. a locked package manager lock file — these are not the same failure

`sudo -l` lists exactly what a user is permitted to run as root — the authoritative way to check permission level. A `dpkg`/`apt` lock-file error can *look* like a permissions problem while actually being a transient conflict with another process holding the same lock. **General principle:** don't assume the literal wording of a permissions-flavored error is diagnostically accurate — confirm actual permission level independently before treating it as a permissions issue to fix.

---

## 2. Docker / Podman (Container Images)

### 2.1. Docker vs. Podman — know which one you're actually running

```
docker build ...
```

`docker` may be aliased/shimmed to **Podman** via `podman-docker`. Podman's rootless mode runs builds in their own network namespace, separate from the host's. Confirm with `which docker` and `docker version` rather than assuming.

### 2.2. Anatomy of a Dockerfile used in this session

`FROM` sets the base image. `USER root:root` temporarily elevates for build steps needing elevated write access, then a numeric `USER` (or a created non-root user) drops privileges before the image is finalized. `ENV` sets a build-time variable. `--strip-components=1` on `tar` discards an archive's top-level wrapper directory. Each `RUN` is a distinct layer; chaining with `&&` keeps related steps atomic.

### 2.3. Building with host networking (fixing a namespace-isolated DNS failure)

```
docker build --network=host -t local/kafka-connect-mqtt:1.0 -f Dockerfile ./context
```

`--network=host` makes build-time `RUN` steps use the host's own network stack/DNS resolver, instead of the engine's isolated build namespace. Diagnostic principle: if a plain host-shell `curl` against the same target succeeds while the same request fails *inside* a build, the fault is specific to the build's network path. Applied proactively (not rediscovered) for every subsequent image build in this project needing to reach a package registry (PyPI, Maven, quay.io) — a general pattern by Phase 4, not a one-off fix.

### 2.4. Saving an image and importing it elsewhere without a registry

```
docker save <image>:<tag> | sudo k3s ctr images import -
```

Serializes an image and loads it directly into k3s's containerd image store, bypassing any registry. Verify with `sudo k3s ctr images ls | grep <image-name>` and compare the digest against what `docker build`/`docker save` reported.

### 2.5. Inspecting image contents without deploying it anywhere

```
docker run --rm <image>:<tag> ls -la /some/path
```

`--rm` deletes the container immediately after exit — disposable, side-effect-free inspection.

### 2.6. `ENTRYPOINT` (exec form) appends arguments — it does not accept a replacement command

Exec-form `ENTRYPOINT ["python", "entrypoint.py"]` means anything passed after the image name on `docker run` is **appended** as arguments to the entrypoint, not substituted for it — `docker run --rm image python -c "..."` actually runs `python entrypoint.py python -c "..."`. To genuinely override, use `--entrypoint` explicitly: `docker run --rm --entrypoint python image -c "..."`.

**Phase 4 extension — this same class of issue recurs with minimal/distroless images, but manifests differently:** a distroless-style image (see Section 8.2) may have **no shell at all**, meaning a `command: [sh, -c, "..."]` Kubernetes container spec fails to start entirely — not "runs the wrong thing" the way an exec-form `ENTRYPOINT` mismatch does, but "fails to run anything, with empty logs, because the shell itself doesn't exist to interpret the `-c` string." The diagnostic signal is different too: an `ENTRYPOINT`-argument mismatch produces *wrong but present* output/behavior (e.g. the unexpected DNS-resolution attempt in the original edge-simulator incident); a missing-shell failure produces `kubectl describe pod` showing `exec: "sh": executable file not found in $PATH` under `Events`, with `kubectl logs` returning nothing at all, since the container process never started. **General rule, extended:** before writing any `sh -c`/shell-wrapped command against a container image, confirm the image actually ships a shell — don't assume every image has one just because most Debian/Ubuntu-based images do. Minimal/distroless image families (Chainguard, `gcr.io/distroless/*`, and similar) frequently reserve the shell/package-manager tooling for a separate `-dev`/`-debug` tag, with the default production tag intentionally shell-less.

### 2.7. Podman's implicit `localhost/` prefix on locally-built image names (Phase 4)

```
docker build -t local/storage-consumer:1.0 .
# ...
# Successfully tagged localhost/local/storage-consumer:1.0
```

Podman (via the `docker` compatibility shim) silently prefixes any locally-built, unqualified image name with `localhost/` at tag time — visible directly in the build's own final output line, and confirmable afterward via `sudo k3s ctr images ls`, which will show the same `localhost/`-prefixed name. This is not cosmetic: it is the image's actual, fully-qualified name as far as containerd (and therefore Kubernetes) is concerned.

**Practical consequence:** a Kubernetes manifest or Helm chart `values.yaml` referencing the image as `local/<name>:<tag>` (omitting the `localhost/` prefix) will not match the locally-cached image at all. `imagePullPolicy: IfNotPresent`'s local-cache lookup checks the exact fully-qualified name; a mismatch causes it to silently fall through to attempting a real registry pull — against `docker.io/local/<name>:<tag>` by default, a repository that (almost certainly) doesn't exist, producing a confusing `pull access denied, repository does not exist or may require authorization` error that has nothing to do with actual registry credentials or access rights, despite how it reads.

**General rule:** any Helm chart or raw manifest referencing a locally-built (Podman-built) image must include the `localhost/` prefix explicitly in the image reference — check the literal name reported at the end of the `docker build`/`docker save` output (or `sudo k3s ctr images ls`) rather than assuming the tag you passed to `-t` is the image's full name as stored.

---

## 3. Kubernetes

### 3.1. Core objects encountered

| Object | What it is |
|---|---|
| **Pod** | The smallest deployable unit. |
| **Deployment** | Manages identical Pod replicas. |
| **StatefulSet** | Like a Deployment, but for workloads needing stable network identity and per-replica persistent storage — used directly (hand-rolled, not via a wrapped chart) for both TimescaleDB and MinIO in Phase 4. Requires a paired headless `Service` (`clusterIP: None`) for pod DNS identity; `volumeClaimTemplates` gives each replica its own `PersistentVolumeClaim` rather than sharing one. |
| **Service** | A stable network identity/DNS name routing to a dynamic set of Pods. |
| **Namespace** | A logical partition; names only need to be unique within one. |
| **ConfigMap** | Non-secret configuration data. Phase 4 use: holding the TimescaleDB DDL file, injected into a schema-init Job via a mounted volume rather than embedding SQL inline in the Job spec. |
| **Secret** | Same mechanism as ConfigMap, base64-encoded, intended for credential-shaped data. Phase 4 pattern: both hand-rolled StatefulSet charts generate their own dev-only Secret from `values.yaml` by default (`credentials.create: true`), so the chart is self-contained rather than depending on an out-of-band `kubectl create secret` living only in shell history. |
| **PersistentVolumeClaim (PVC)** | A request for durable storage outliving any individual Pod. |
| **Job** | Runs a pod to completion once (or a bounded number of retries via `backoffLimit`), rather than keeping it running indefinitely. Phase 4 use: schema-init and bucket-init, both as Helm post-install/post-upgrade hooks — see 3.8. |
| **Custom Resource Definition (CRD)** | Extends the Kubernetes API with new object types (e.g. Strimzi's `Kafka`, `KafkaConnect`, `KafkaConnector`). |
| **StrimziPodSet** | A Strimzi-specific controller type some operator versions use instead of a standard `Deployment`/`StatefulSet`. |

### 3.2. Reading cluster state

```
kubectl get pods -n <namespace>
kubectl get pods -n <namespace> -w
kubectl describe pod -n <namespace> <pod-name>
kubectl logs -n <namespace> <pod-name> --tail=100
kubectl logs -n <namespace> deploy/<deployment-name> --tail=50
```

`get` is a fast summary, often not enough to diagnose. `-w` streams live updates (Ctrl+C to exit, normal behavior, not a hang). `describe`'s `Events` section is usually the fastest path to *why* — image pull failures, scheduling failures, probe failures, and (Phase 4 addition) `FailedMount` volume errors all show up here. **General diagnostic order, reconfirmed repeatedly through Phase 4:** `get` → `describe` → `logs`. Phase 4 added one more layer to this chain specifically for Jobs: `kubectl get jobs` (is it `Complete`/`Failed`/still `Running`?) sits between `get pods` and `describe pod` in practice, since a Job's own status line often narrows the search before you even need `describe`.

**Working with a specific Job's pod, which changes name across retries:** unlike a Deployment's pods (predictable name pattern, persist across `kubectl get` calls for a while), a Job's individual attempt-pods are named uniquely per retry and are garbage-collected fairly quickly once superseded. Referencing an old pod name from a previous `kubectl get` output (rather than re-querying) reliably produces `Error from server (NotFound)`. Get the *current* attempt fresh, every time:

```
kubectl get pods -n <namespace> -l job-name=<job-name>
```

### 3.3. Running commands inside a running container

```
kubectl exec -n <namespace> -it <pod-name> -- <command>
```

`-it` allocates a pseudo-TTY, harmless but not strictly required for one-shot commands. `--` separates `kubectl`'s own flags from the in-container command.

### 3.4. Distinguishing pod-level health from application-level health

A pod showing `1/1 Running` only confirms Kubernetes' own probes are passing — not that the application inside is functioning for its actual purpose. Recurred repeatedly: a Kafka connector's top-level `RUNNING` state not implying its task was actually running (Section 3.7 below); all four Phase 3 `edge-sim-*` deployments needing an explicit `kubectl logs` check despite `1/1 Running`; and in Phase 4, most sharply, the storage-consumer pod cycling between `ImagePullBackOff` and a genuine post-start crash loop — two entirely different failure classes that both read as "not working" at the `get pods` summary level, distinguishable only by checking `describe` (pull-level) vs. `logs` (application-level) directly rather than assuming which one applies.

### 3.5. A subtle default worth knowing: `imagePullPolicy`

Kubernetes defaults `imagePullPolicy` to `IfNotPresent` for any image tag other than `:latest`. This is what makes the no-registry local-import workflow (Section 2.4) possible — but it depends entirely on the image *name* matching exactly what's cached (see Section 2.7's `localhost/` prefix issue for a case where this silently fails to match despite the image genuinely being present locally). The practical consequence when deliberately rebuilding an unchanged tag: Kubernetes may keep running the stale cached version unless the pod is explicitly deleted to force a re-check, or the tag is bumped.

### 3.6. Not every workload is a `Deployment` — `StrimziPodSet`

A workload's controller type should be verified (`kubectl get deploy`, then `statefulset`, then any operator-specific CRD), not assumed from prior experience with a different cluster/operator version. Locate a pod by stable **labels** instead when the controller type is uncertain: `kubectl get pods -n kafka -l strimzi.io/kind=KafkaConnect`.

### 3.7. Diagnosing a `KafkaConnector` task failure with `describe`

*(Kafka Connect was retired on 2026-10-05 and replaced by the MQTT-Kafka bridge, so this is kept as the record of how its failures were diagnosed; the technique applies to any Strimzi connector.)*

`kubectl get kafkaconnector` shows only a `READY` column. `kubectl describe kafkaconnector` exposes a `Status.Connector Status` block distinguishing the connector-level object (`Connector: RUNNING`) from its actual data-moving `Task`(s) (`State: FAILED` possible independently) — two different sub-objects with independent state. Stack traces are read innermost `Caused by:` first. A `FAILED` task does not auto-retry; restart via the Connect REST API (`POST .../tasks/0/restart`).

### 3.8. Helm hook timeouts are a distinct failure class from the hooked resource actually failing (Phase 4)

```
helm upgrade --install timescaledb k8s/timescaledb -n kafka
Error: failed post-install: resource Job/kafka/timescaledb-schema-init not ready. status: InProgress, message: Job in progress
context deadline exceeded
```

`context deadline exceeded` means Helm's own wait-for-hook-completion window (default 5 minutes) elapsed before the hooked Job reported `Complete` — it is **not**, by itself, evidence that the Job failed or ever will fail. This recurred twice in Phase 4 (TimescaleDB's schema-init Job, indirectly implicated in MinIO's bucket-init Job investigation) and in both cases the underlying cause was a slow first-time image pull for a large image that had never been cached on the node before (`timescale/timescaledb-ha`), not a defect in the Job or chart.

**Diagnostic approach, distinct from a normal pod failure:** check both layers independently before concluding anything failed:

```
kubectl get pods -n <namespace> -l <workload-label>   # is the underlying Pod still just pulling/starting?
kubectl get jobs -n <namespace>                        # is the Job's own status genuinely Running, or did it actually fail?
```

If the underlying Pod is still `ContainerCreating` and the Job is still `Running` (not `Failed`), the correct action is to **wait**, not to re-run `helm upgrade --install` again — a second invocation while the first is still legitimately in progress just restarts the same timeout race. Once the Pod reaches `Running` on its own, a Job with a readiness-waiting `initContainer` (see the pattern in 3.1's Secret/ConfigMap note) typically unblocks and completes without any further Helm command being needed — the StatefulSet/Secret/ConfigMap/Service from the same release are generally already live regardless of whether the hook itself timed out, since those aren't gated by the hook's success.

**Only escalate to treating it as a real failure** if `kubectl get jobs` shows an actual `Failed` status (not `Running`) after `backoffLimit` retries are exhausted — that's a genuinely different situation requiring `kubectl logs job/<name>` investigation, as in Section 3.2's Job-pod-naming note.

### 3.9. `$(VAR)` substitution in container env values is order-sensitive within the `env:` list (Phase 4)

```yaml
env:
  - name: TIMESCALE_DSN
    value: "postgresql://$(DB_USER):$(DB_PASSWORD)@host:5432/db"
  - name: DB_USER
    valueFrom: { secretKeyRef: { name: creds, key: username } }
  - name: DB_PASSWORD
    valueFrom: { secretKeyRef: { name: creds, key: password } }
```

This looks reasonable — a normal shell environment doesn't care about declaration order, every variable in scope is simultaneously available regardless of where it was set. Kubernetes' env-value `$(VAR_NAME)` substitution does **not** work this way: it only resolves a reference if `VAR_NAME` was defined **earlier in the same container's `env:` list**, evaluated top-to-bottom as the list is processed. In the example above, `TIMESCALE_DSN` is defined before `DB_USER`/`DB_PASSWORD`, so at the point Kubernetes evaluates it, those two variables don't exist yet for substitution purposes — the literal text `$(DB_USER)` and `$(DB_PASSWORD)` is left in the value unresolved and passed to the container as-is.

**Diagnostic signature:** an application-level authentication or connection failure where the *username itself*, in the error message, is the literal unsubstituted placeholder string (e.g. a database error like `password authentication failed for user "$(DB_USER)"`) rather than any real or even plausible-looking username. This is a strong, specific tell — worth recognizing immediately rather than suspecting the credentials themselves are wrong.

**Fix:** reorder the `env:` list so any variable referenced via `$(...)` is declared strictly before the value that references it. No other change needed — this is a pure ordering fix, not a values/Secret-content problem.

---

## 4. Helm

### 4.1. What Helm actually is

A **chart** is a directory (`Chart.yaml` + `values.yaml` + `templates/`) bundling Kubernetes object definitions, optionally templated. A **release** is one deployed instance of a chart, tracked by Helm for later upgrade/rollback.

### 4.2. The core command

```
helm upgrade --install <release-name> ./path/to/chart -n <namespace>
```

`--install` makes this idempotent regardless of whether this is a first deploy.

### 4.3. A critical gotcha: Helm ownership tracking

Helm tracks ownership via labels/annotations applied at creation time. An object with the same name that already exists but lacks those labels (e.g. created via direct `kubectl apply`, or — Phase 4 addition — `kubectl create secret`) causes `helm upgrade`/`--install` to refuse to touch it, correctly, to avoid silently overwriting something it doesn't recognize as its own. **General principle, reconfirmed in Phase 4 for a Secret specifically (not just a KafkaConnector, the original Phase 3 case):** once a chart's own `templates/` is meant to own an object, any earlier manually-created object of the same name must be deleted first (`kubectl delete secret <name>`) before the chart-managed version can be installed — the manual object and the chart's own template will otherwise permanently conflict on every future `helm upgrade`.

### 4.4. Path resolution failures can surface as a misleading "repo not found" error

```
helm template edge-simulators k8s/edge-simulators
Error: repo k8s not found
```

Looks like a missing **chart repository** error, but is usually a plain local-path resolution failure — Helm's fallback interpretation of an unresolvable relative path is `<repo-name>/<chart-name>` shorthand, producing this more confusing message. **General principle:** check the literal local path first (`ls`, `find . -name Chart.yaml`) before suspecting anything about remote repositories.

### 4.5. `helm template`/`helm upgrade` render *every* file in `templates/` unconditionally and independently

No cross-file consistency checking. Every `.yaml` file renders against the same merged `values.yaml` in the same pass; a `range` over an undefined values key silently renders to nothing rather than erroring. Stale/superseded template files are not automatically detected — must be found and removed manually. Read `helm template` output's `# Source:` comments to confirm which template actually produced each block.

### 4.6. Full render/apply sequence, in order

**Render time (client-side):** `Chart.yaml` validated → `values.yaml` read as defaults, `-f`/`--set` merged on top → `templates/_helpers.tpl` read → every remaining `templates/` file rendered independently against the merged values → all blocks concatenated with `# Source:` comments → `templates/NOTES.txt` rendered last (printed, never applied).

**Continuing for `helm upgrade --install` (cluster-side):** manifests validated for basic API structure → release history diffed (three-way merge) → change set submitted to the API server (the actual point of mutation).

**Runtime (asynchronous, driven by Kubernetes, not Helm):** scheduler assigns Pods → kubelet pulls images (subject to `imagePullPolicy`, 3.5) and starts containers → the container runtime executes `ENTRYPOINT` (2.6) → the application reads its env/config and behaves accordingly.

### 4.7. External Helm chart dependencies can go stale independent of the image they deploy (Phase 4)

A chart's `Chart.yaml` can declare a `dependencies:` block pointing at a third-party chart repository:

```yaml
dependencies:
  - name: timescaledb-single
    version: ">=0.35.0"
    repository: "https://charts.timescale.com"
```

`helm dependency update <chart-path>` resolves and downloads this. If the constraint doesn't match anything actually published, the error (`can't get a valid version for 1 subchart(s) ...`) reads like a simple version-typo — but the underlying repository itself may no longer be receiving new releases at all, meaning no constraint adjustment fixes it. Confirm which situation applies directly rather than guessing at a new version number:

```
helm repo add <name> <repository-url>
helm repo update <name>
helm search repo <name>/<chart-name> --versions | head -5
```

If the highest listed version is old and no newer one has appeared despite the chart clearly still being widely used (cross-check the repository's own source/issue tracker for maintenance status), treat the dependency as abandoned rather than pin-and-hope. **This is a different failure class from Section 2.7/8.2's stale-*image*-tag problem** — here, the *chart's packaging* around a still-fine underlying container image is what's abandoned; the fix (in this project's case, for both TimescaleDB and MinIO) was to drop the wrapper chart and write a hand-rolled `StatefulSet`/`Service`/`Secret` directly against the underlying image instead, removing the dependency on third-party chart maintenance entirely.

---

## 5. Command Quick-Reference

| Task | Command pattern |
|---|---|
| Find a file/directory under home | `find ~ -maxdepth N -name '<pattern>'` |
| Verify a documented repo convention against the live filesystem | `find . -maxdepth 4 -name 'Chart.yaml'` (or equivalent) |
| Write a multi-line file from the shell | `cat > file << 'EOF' ... EOF` |
| Check a systemd service | `systemctl status <name>`, `journalctl -u <name>` |
| Check actual sudo rights | `sudo -l` |
| Build a container image | `docker build -t <name>:<tag> -f Dockerfile <context>` |
| Build bypassing a broken build-network DNS | `docker build --network=host ...` |
| Get an image into k3s without a registry | `docker save <image> \| sudo k3s ctr images import -` |
| Confirm a locally-built image's actual stored name (check for Podman's `localhost/` prefix) | `sudo k3s ctr images ls \| grep <name>` |
| Peek inside an image | `docker run --rm <image> <command>` |
| Run an ad hoc command inside an image, overriding `ENTRYPOINT` | `docker run --rm --entrypoint <cmd> <image> <args>` |
| List pods in a namespace | `kubectl get pods -n <ns>` |
| Watch pods live (Ctrl+C to exit) | `kubectl get pods -n <ns> -w` |
| Get pods belonging to a specific Job's current attempt | `kubectl get pods -n <ns> -l job-name=<job>` |
| Check Job status (Complete/Failed/Running) | `kubectl get jobs -n <ns>` |
| Full object detail + events (works for CRDs too) | `kubectl describe <kind> -n <ns> <name>` |
| Application-level logs | `kubectl logs -n <ns> <name> --tail=N` |
| Logs for a specific container in a multi-container pod (e.g. an initContainer) | `kubectl logs -n <ns> <pod> -c <container-name>` |
| Run a command inside a pod | `kubectl exec -n <ns> -it <name> -- <cmd>` |
| Find a pod when its owning controller type is unknown | `kubectl get pods -n <ns> -l <label-selector>` |
| List Strimzi-managed pods specifically | `kubectl get strimzipodset -n <ns>` |
| Restart a failed Kafka Connect task | `kubectl exec -n <ns> <connect-pod> -it -- curl -s -X POST localhost:8083/connectors/<name>/tasks/0/restart` |
| Deploy/update via Helm | `helm upgrade --install <release> <chart-path> -n <ns>` |
| Render a chart locally without touching the cluster | `helm template <release> <chart-path>` |
| Add/refresh a chart repository, list actual published versions | `helm repo add <name> <url>`, `helm repo update <name>`, `helm search repo <name>/<chart> --versions` |
| Resolve a chart's own external dependencies | `helm dependency update <chart-path>` |
| Check a chart's default values and available keys before setting one blind | `helm show values <chart-ref> --version <v>` |
| Raw TCP reachability check from inside the cluster, independent of any application protocol | `kubectl run -it --rm netcheck --image=busybox -n <ns> --restart=Never -- nc -zv <host> <port>` |
| Inspect a Strimzi Kafka cluster's actual listener config (don't assume plaintext/no-auth) | `kubectl get kafka <name> -n <ns> -o yaml \| grep -A20 "listeners:"` |
| Check a Kafka consumer group's per-partition lag directly (the real "is it caught up" signal) | `kubectl exec -n <ns> <broker-pod> -it -- /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group <group>` |
| Check a pod's live CPU/memory against its configured limits | `kubectl top pod -n <ns> -l <label-selector>` (requires metrics-server) |
| Remove a resource Helm doesn't own so Helm can manage it | `kubectl delete <kind> <name> -n <ns>` then re-run `helm upgrade` |
| Create/activate a Python venv | `python3 -m venv /tmp/env && source /tmp/env/bin/activate` |

---

## 6. Python Packaging on Debian/Ubuntu

*(Sections 6.1–6.2, on `ensurepip`/venv setup and PEP 668's externally-managed-environment restriction, are unchanged from the prior revision — see the implementation-status document's problem log items 17–18 for the incidents that motivated them.)*

### 6.3. Pinning a base image's Python version isn't just about your own code — check third-party library compatibility too (Phase 4)

A Dockerfile's `FROM python:3.12-slim` looks like a reasonable, unremarkable default — newest stable Python, no reason given to question it. But a base image's Python version is also implicitly a constraint on every third-party library the image installs, and not every library keeps pace with new Python releases, especially unmaintained or infrequently-released ones.

Concretely: `kafka-python` 2.0.2 (last real release 2020) vendors its own bundled copy of the `six` compatibility library via a `sys.modules`-registration trick, and that trick breaks under Python 3.12's import-system changes — `ModuleNotFoundError: No module named 'kafka.vendor.six.moves'`, raised from deep inside the library's own import chain, with no way to work around it from calling code. This is not a bug in anything the project wrote; it is a real, open, unresolved upstream compatibility gap.

**Diagnostic signal:** a `ModuleNotFoundError` or `ImportError` pointing at a path *inside a third-party library's own internals* (not your application code, and not a missing top-level package you forgot to `pip install`) is a strong hint this is a library/interpreter-version compatibility issue, not a dependency-list mistake. Confirm via direct research (checking the library's own issue tracker/changelog for the specific error string) rather than guessing at a `pip install` fix, since the actual fix in this case was unrelated to package installation entirely.

**General rule:** when pinning a specific version of a library with a known-old or infrequent release cadence, don't default the base image to the newest available Python — check that specific library's tested/supported Python range first, and pin the base image down to match if needed. The fix here was a one-line change (`FROM python:3.12-slim` → `FROM python:3.11-slim`), but only after the actual root cause was correctly identified rather than assumed to be something else (a missing `six` package, a `requirements.txt` ordering issue, etc. — plausible-looking but wrong guesses that this specific error message would not actually support).

### 6.4. An old client library's automatic version/capability negotiation can be a more fragile dependency than the library itself (Phase 4)

Separately from 6.3's Python-version incompatibility, the same `kafka-python` 2.0.2 library hit a second, unrelated compatibility gap against this project's Kafka 4.3.1 broker: `KafkaConsumer(...)`'s constructor calls an internal `check_version()` method that probes the broker to auto-detect which protocol version to speak, and this probe failed unconditionally (`NoBrokersAvailable`) despite the broker being confirmed healthy, the network path confirmed open, and the listener confirmed correctly configured for plain unauthenticated access. DEBUG-level logging showed the client successfully connecting at the TCP level and issuing the probe, then simply never receiving a response it could parse.

**Diagnostic approach that isolated this correctly:** rule out every other layer independently before concluding it's a client/server protocol mismatch — network reachability (`nc -zv` from a disposable pod), the server's own listener configuration (reading the `Kafka` CR's `spec.listeners` directly rather than assuming), and the server's own health (checking its logs across the exact timestamps of the client's failures, not just its current `Running` status). Only once all three were independently confirmed fine did the remaining explanation — an old client unable to negotiate with a newer broker — become the leading, and eventually confirmed, hypothesis.

**The fix did not require replacing the library.** Many clients that perform automatic server-version/capability auto-detection also expose a way to skip that auto-detection and specify the target version explicitly — here, `KafkaConsumer(..., api_version=(2, 8, 0))`. This bypasses the specific broken code path (the auto-detection probe/parse) while still using a protocol version the broker fully supports for ordinary consumer operations. **General principle:** when an old, infrequently-updated client library fails against a newer server specifically during a "detect what version you're talking to" step, check for an explicit version-override parameter before assuming a full library replacement is necessary — it's a much cheaper fix to attempt first, and if it doesn't work, that failure itself is stronger evidence that a full replacement really is warranted.

---

## 7. Docker `ENTRYPOINT` Semantics

*(Unchanged from the prior revision — see Section 2.6 above, which now also covers the Phase 4 distroless/no-shell extension of this same principle.)*

---

## 8. Container Image and Chart Supply-Chain Volatility (Phase 4)

Three independent third-party distribution failures occurred in immediate succession during the Phase 4 build — one affecting a Helm chart repository, two affecting container image registries from two different vendors. Treated individually, each was solvable on its own terms (Sections 4.7, 2.7-adjacent, and the items below). Named together here because the *pattern* — a previously-working, pinned external reference silently stops resolving, for reasons entirely outside this project's own code or configuration — is common enough, and was common enough within one session, to warrant a general diagnostic posture rather than re-deriving it fresh each time.

### 8.1. A pinned dependency's failure mode tells you which layer actually broke

Three distinct error shapes were seen, and each pointed at a different actual cause:

- **`can't get a valid version for 1 subchart(s) ... version ">=X.Y.Z"`** (from `helm dependency update`) — the chart *repository itself* has stopped publishing versions matching (or beyond) what was pinned. Check `helm search repo <name>/<chart> --versions` for the real ceiling; if it's old and static, the repository is likely abandoned, not just under a stricter version policy.
- **`<specific-tag>: not found` / `ErrImagePull`** on a fully-qualified, versioned image tag — the *image itself*, at that exact tag, has been moved or removed from the registry namespace it was pinned against. This does not necessarily mean the software is abandoned — as seen with Bitnami, the *chart* was current and well-maintained; only the free public registry's tag policy changed underneath it.
- **A vendor's own current documentation/announcements confirming a distribution-model change** (found via direct search, not inferable from any single error message alone) — the deepest and most consequential case, as with MinIO ending free image distribution outright. This can't be diagnosed from `kubectl`/`docker` output alone; it requires checking whether the *entire distribution channel*, not just one tag, is still viable.

**Practical order of investigation when a previously-reliable image/chart reference suddenly fails:** (1) read the literal error string carefully — `not found` vs. an auth error vs. a version-constraint error are different problems; (2) check whether the *chart* and the *image it deploys* are the same vendor/maintainer or different ones — a chart wrapper going stale doesn't mean the image underneath it is also stale, and vice versa (this project's TimescaleDB case: chart abandoned, image fine; MinIO case: eventually, both layers were affected, but for unrelated reasons on unrelated timelines); (3) search directly for the vendor's current distribution status rather than assuming a version bump or tag change is sufficient — a wrong assumption here (as happened once this session, assuming "just find a still-current MinIO tag" would resolve what turned out to be a total distribution-model change) costs a full extra diagnostic cycle.

### 8.2. Minimal/distroless images trade a smaller attack surface for the absence of tooling you might assume is present

Chainguard-style minimal images (used in this project for `cgr.dev/chainguard/minio` and `cgr.dev/chainguard/minio-client`) intentionally ship without a shell, package manager, or other general-purpose tooling in their default/production tags, reserving that for a separate `-dev` (or similarly named) variant. This is a deliberate security posture, not an oversight — but it means any assumption carried over from a "normal" Debian/Ubuntu-based image (that `sh -c "..."` will work, that you can `docker run --rm --entrypoint sh <image>` to poke around) does not transfer. See Section 2.6 for the specific failure signature this produces (`exec: "sh": executable file not found in $PATH`, empty `kubectl logs`) and the fix (use the `-dev` tag for anything needing an actual shell; leave the production tag for the actual long-running service process, which typically has its own binary as the direct entrypoint and needs no shell at all).

### 8.3. When a wrapper chart's dependency is the actual point of failure, hand-rolling is a legitimate fix, not just a workaround

Both TimescaleDB and MinIO in this project moved from "thin Helm chart wrapping a third-party dependency chart" to "hand-rolled `StatefulSet` + `Service` + `Secret` + init-`Job`, pulling the underlying container image directly" — see implementation-status.md Section 3.6 for the full reasoning. Worth stating as a general option, not just a project-specific fallback: a hand-rolled manifest set removes the dependency on a third party's chart-maintenance commitment entirely, at the cost of taking on manifest-writing and upgrade-path work yourself. For a project whose own stated goal includes demonstrating Kubernetes depth (as here), this trade is frequently a net improvement, not merely a workaround — it was treated that way deliberately in this project's own decision record, not chosen only because the alternative was unavailable.
