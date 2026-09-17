FROM python:3.11-slim

RUN useradd -u 1001 -m svcuser

WORKDIR /app
COPY digital-twin/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY phase5_common.py /app/phase5_common.py
COPY digital-twin/twin.py /app/twin.py

USER 1001
ENTRYPOINT ["python", "twin.py"]
