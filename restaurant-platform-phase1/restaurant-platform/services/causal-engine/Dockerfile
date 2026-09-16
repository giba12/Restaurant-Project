FROM python:3.11-slim

# DoWhy/statsmodels/pandas have C-extension build dependencies not present
# in the slim base image on some platforms; build-essential is included
# defensively. Remove if the pinned wheel versions below resolve to
# prebuilt manylinux wheels in your environment (likely on x86_64).
RUN apt-get update && apt-get install -y --no-install-recommends build-essential \
    && rm -rf /var/lib/apt/lists/*

RUN useradd -u 1001 -m svcuser

WORKDIR /app
COPY causal-engine/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY phase5_common.py /app/phase5_common.py
COPY causal-engine/causal_engine.py /app/causal_engine.py

USER 1001
ENTRYPOINT ["python", "causal_engine.py"]
