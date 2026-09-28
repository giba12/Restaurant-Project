FROM docker.io/library/python:3.11-slim

# Without this, stdout is block-buffered (not a TTY inside a container), so
# main.py's print() calls never reach `kubectl logs` until the buffer fills
# or the process exits -- confirmed live, 2026-09-28: a running, healthy pod
# produced zero log output at all, not even its own startup line.
ENV PYTHONUNBUFFERED=1

RUN useradd -u 1001 -m svcuser

WORKDIR /app
COPY main.py /app/main.py

USER 1001
ENTRYPOINT ["python", "main.py"]
