# Build with: docker build --network=host -t local/storage-consumer:1.0 .
# (--network=host required for pip to resolve PyPI under rootless Podman --
#  same DNS-namespace issue documented for the edge-simulator image, study
#  guide Section 2.3 / implementation-status.md problem log item 11.)
#
# Import into k3s with:
#   docker save local/storage-consumer:1.0 | sudo k3s ctr images import -

FROM python:3.11-slim

RUN useradd -u 1001 -m consumer

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY consumer.py .
# schemas/ is copied in from the repo root at build time -- see the
# accompanying note in k8s/charts/storage-consumer/values.yaml build
# instructions. Do not hand-maintain a second copy of the schema files;
# the whole point of the contract-first constraint is one source of
# truth in schemas/.
COPY schemas/ /app/schemas/

USER 1001

ENTRYPOINT ["python", "consumer.py"]
