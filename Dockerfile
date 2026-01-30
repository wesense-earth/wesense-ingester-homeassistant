# WeSense Ingester - Home Assistant
# Build context: parent directory (wesense/)
# Build: docker build -f wesense-ingester-homeassistant/Dockerfile -t wesense-ingester-homeassistant .
#
# Pulls sensor data from Home Assistant (WebSocket/REST API)
# and publishes to WeSense ecosystem (ClickHouse + MQTT).
#
# Expects wesense-ingester-core to be available at ../wesense-ingester-core
# when building with docker-compose (which sets the build context).

FROM python:3.11-slim

WORKDIR /app

# Copy dependency files first for better layer caching
COPY wesense-ingester-core/ /tmp/wesense-ingester-core/
COPY wesense-ingester-homeassistant/requirements-docker.txt .

# Install gcc, build all pip packages, then remove gcc in one layer
RUN apt-get update && \
    apt-get install -y --no-install-recommends gcc && \
    pip install --no-cache-dir /tmp/wesense-ingester-core && \
    pip install --no-cache-dir -r requirements-docker.txt && \
    apt-get purge -y --auto-remove gcc && \
    rm -rf /var/lib/apt/lists/* /tmp/wesense-ingester-core

# Copy application code
COPY wesense-ingester-homeassistant/src/ ./src/
COPY wesense-ingester-homeassistant/run.py ./

# Create directories for runtime data
RUN mkdir -p /app/logs /app/config

ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app
ENV TZ=UTC

CMD ["python", "run.py"]
