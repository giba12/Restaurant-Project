# Build from the REPO ROOT, not this directory -- same convention as
# edge-simulators/Dockerfile, so schemas/ can be COPYed directly without
# the copy-in/build/delete dance the old version of this file needed:
#   docker build --network=host -t local/storage-consumer:1.0 -f storage/consumer/Dockerfile .
# (--network=host required for pip to resolve PyPI under rootless Podman --
#  same DNS-namespace issue documented for the edge-simulator image, study
#  guide Section 2.3 / implementation-status.md problem log item 11.)
#
# Import into k3s with:
#   docker save local/storage-consumer:1.0 | sudo k3s ctr images import -

FROM docker.io/library/python:3.11-slim

RUN useradd -u 1001 -m consumer

WORKDIR /app

COPY storage/consumer/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY storage/consumer/consumer.py .
# Canonical schemas, copied from repo root -- NOT a duplicate copy
# maintained separately. Same rationale as edge-simulators/Dockerfile.
COPY schemas/ /app/schemas/

USER 1001

ENTRYPOINT ["python", "consumer.py"]
