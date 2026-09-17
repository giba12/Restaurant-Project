FROM docker.io/library/python:3.11-slim

RUN useradd -u 1001 -m svcuser

WORKDIR /app
COPY anomaly-detector/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY phase5_common.py /app/phase5_common.py
COPY anomaly-detector/detector.py /app/detector.py

USER 1001
ENTRYPOINT ["python", "detector.py"]
