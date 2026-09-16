FROM python:3.11-slim

RUN useradd -u 1001 -m svcuser

WORKDIR /app
COPY scenario-injection-controller/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY phase5_common.py /app/phase5_common.py
COPY scenario-injection-controller/controller.py /app/controller.py

USER 1001
# No fixed ENTRYPOINT command args -- this is a CLI tool, invoked via
# `kubectl exec ... -- python controller.py <subcommand> ...` against a
# long-lived idle pod (see k8s/scenario-injection-controller/templates/),
# rather than run-to-completion as a Job, so it stays available for
# repeated ad hoc scenario runs during Phase 5 testing.
ENTRYPOINT ["python", "controller.py"]
CMD ["--help"]
