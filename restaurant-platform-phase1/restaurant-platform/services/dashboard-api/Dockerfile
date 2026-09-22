FROM docker.io/library/python:3.11-slim

RUN useradd -u 1001 -m svcuser

WORKDIR /app
COPY dashboard-api/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY phase5_common.py /app/phase5_common.py
COPY dashboard-api/main.py /app/main.py
COPY dashboard-api/comparison.py /app/comparison.py

USER 1001
EXPOSE 8000
ENTRYPOINT ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
