# Build from services/ (the Compose context), like every other service here:
#   docker build -t local/mqtt-kafka-bridge:1.0 -f mqtt-kafka-bridge/Dockerfile services
FROM docker.io/library/python:3.11-slim

RUN useradd -u 1001 -m svcuser

WORKDIR /app
COPY mqtt-kafka-bridge/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# phase5_common.py is only used for the shared Kafka connection settings
# (bootstrap servers, the pinned api_version, the opt-in TLS context).
COPY phase5_common.py /app/phase5_common.py
COPY mqtt-kafka-bridge/bridge.py /app/bridge.py

USER 1001
EXPOSE 8000
ENTRYPOINT ["python", "-u", "bridge.py"]
