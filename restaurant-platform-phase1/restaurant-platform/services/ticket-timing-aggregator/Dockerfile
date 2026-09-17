# python:3.11-slim required -- kafka-python 2.0.2's vendored six.moves shim
# breaks on 3.12+ (see linux-k8s-docker-helm-study-guide.md, Section on
# kafka-python compatibility).
FROM docker.io/library/python:3.11-slim

RUN useradd -u 1001 -m svcuser

WORKDIR /app
COPY ticket-timing-aggregator/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Build context must be the parent `services/` directory (not this
# subdirectory), since phase5_common.py is shared across all four
# services. See PHASE5-SETUP.md step 4 for the exact build command.
COPY phase5_common.py /app/phase5_common.py
COPY ticket-timing-aggregator/aggregator.py /app/aggregator.py
# Schema files are mounted at runtime via ConfigMap to /app/schemas -- see
# k8s/ticket-timing-aggregator/templates/. Not baked into the image, so a
# schema correction does not require an image rebuild.

USER 1001
ENTRYPOINT ["python", "aggregator.py"]
