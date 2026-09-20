# Build from the REPO ROOT (same convention as storage/consumer/Dockerfile
# and edge-simulators/Dockerfile) so the canonical schemas/ and the shared
# world model can be COPYed directly instead of duplicated:
#   docker build --network=host -t local/game-bridge:1.0 -f services/game-bridge/Dockerfile .
FROM docker.io/library/python:3.11-slim

RUN useradd -u 1001 -m svcuser

WORKDIR /app
COPY services/game-bridge/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY services/game-bridge/main.py /app/main.py
# Single source of truth for station/table IDs and RESTAURANT_ID -- the
# same module the four edge simulators import, so player events join with
# simulated ones on the same ID spaces.
COPY edge-simulators/common/world.py /app/world.py
COPY schemas/ /app/schemas/

USER 1001
EXPOSE 8001
ENTRYPOINT ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8001"]
