FROM docker.io/library/python:3.11-slim

RUN useradd -u 1001 -m svcuser

WORKDIR /app
COPY finding-narrator/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Build context must be the parent `services/` directory (same convention
# as every other Phase 5/6 service) since phase5_common.py is shared.
COPY phase5_common.py /app/phase5_common.py
COPY finding-narrator/narrator.py /app/narrator.py

USER 1001
ENTRYPOINT ["python", "narrator.py"]
