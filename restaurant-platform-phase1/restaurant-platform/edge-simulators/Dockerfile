# Build from repo root:
#   docker build --network=host -t local/edge-simulator:1.0 -f edge-simulators/Dockerfile .
#
# --network=host is required here for the same reason documented for the
# Kafka Connect image build (Section 2.3 of the study guide): Podman's
# rootless build network namespace does not reliably inherit host DNS
# inside this WSL2 environment, and pip install needs to resolve PyPI.

# python:3.11-slim, not 3.12 -- kafka-python 2.0.2 (needed for the
# service-timing simulator's scenario-control consumer) vendors a copy of
# `six` that breaks under 3.12's import-system changes. Same pin used by
# every Phase 5 service and storage-consumer; see
# linux-k8s-docker-helm-study-guide.md Section 6.3.
FROM python:3.11-slim

WORKDIR /app

COPY edge-simulators/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Canonical schemas, copied from repo root -- NOT a duplicate copy
# maintained separately. If schemas/ changes, this image must be rebuilt
# and re-tagged (see imagePullPolicy note below) to pick up the change.
COPY schemas/ /app/schemas/

COPY edge-simulators/common/ /app/common/
COPY edge-simulators/simulators/ /app/simulators/
COPY edge-simulators/entrypoint.py /app/entrypoint.py

# Runs as non-root, consistent with the Kafka Connect image's practice of
# not running application code as UID 0 at runtime.
RUN useradd -u 1001 -m simuser
USER 1001

ENV SCHEMA_DIR=/app/schemas
ENV PYTHONUNBUFFERED=1

ENTRYPOINT ["python", "entrypoint.py"]