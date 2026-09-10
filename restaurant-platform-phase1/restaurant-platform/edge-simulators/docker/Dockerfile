FROM python:3.12-slim
WORKDIR /app
COPY edge-simulators/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY schemas/ /app/schemas/
COPY edge-simulators/common/simulator_base.py /app/
COPY edge-simulators/plate_waste_sim.py /app/
ENV PYTHONUNBUFFERED=1
ENV SIM_SCRIPT=plate_waste_sim.py
CMD python -u $SIM_SCRIPT
