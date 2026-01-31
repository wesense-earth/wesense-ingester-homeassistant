# WeSense Ingester - Home Assistant

Pulls sensor data from Home Assistant and publishes it to the WeSense ecosystem (ClickHouse + MQTT). Any sensor that Home Assistant supports — Ecowitt, Netatmo, Aqara, Zigbee, Z-Wave, and more — becomes a WeSense data source.

> For detailed documentation, see the [Wiki](https://github.com/wesense-earth/wesense-ingester-homeassistant/wiki).
> Read on for a project overview and quick install instructions.

## Overview

This ingester connects to a Home Assistant instance via WebSocket (real-time) or REST API (polling), reads environmental sensor states, and forwards them to the WeSense network. It acts as a gateway for any sensor brand that has a Home Assistant integration.

**Key features:**
- Real-time updates via WebSocket API or polling fallback
- Loop prevention filters to exclude WeSense ESP32 devices (avoids feedback loops)
- Dual output to both MQTT and ClickHouse
- Works with any HA-supported sensor: Ecowitt, Netatmo, Aqara, Xiaomi, Zigbee, Z-Wave, etc.

Uses [wesense-ingester-core](https://github.com/wesense-earth/wesense-ingester-core) for ClickHouse writing, geocoding, and logging.

### Third-Party Sensor Gateway

The default loop prevention filter (`.*_[0-9a-f]{12}_.*`) excludes WeSense ESP32 MAC-based entity IDs but lets all third-party sensors through without any configuration:

| Sensor Brand | HA Integration | Works out of the box |
|-------------|----------------|---------------------|
| Ecowitt | `ecowitt` | Yes |
| Netatmo | `netatmo` | Yes |
| Aqara | `homekit` / `zigbee` | Yes |
| Xiaomi | `xiaomi_ble` | Yes |
| Zigbee sensors | `zha` / `zigbee2mqtt` | Yes |
| Z-Wave sensors | `zwave_js` | Yes |

## Quick Install (Recommended)

Most users should deploy via [wesense-deploy](https://github.com/wesense-earth/wesense-deploy), which orchestrates all WeSense services using Docker Compose profiles:

```bash
# Clone the deploy repo
git clone https://github.com/wesense-earth/wesense-deploy.git
cd wesense-deploy

# Configure
cp .env.sample .env
# Edit .env — set HA_URL and HA_ACCESS_TOKEN

# Start as a contributor (ingesters only, sends to remote hub)
docker compose --profile contributor up -d

# Or as a full station (includes EMQX, ClickHouse, Respiro map)
docker compose --profile station up -d
```

For Unraid or manual deployments, use the docker-run script:

```bash
./scripts/docker-run.sh station
```

See [Deployment Personas](https://github.com/wesense-earth/wesense-deploy) for all options.

## Docker (Standalone)

For running this ingester independently (e.g. on a separate host):

```bash
docker pull ghcr.io/wesense-earth/wesense-ingester-homeassistant:latest

docker run -d \
  --name wesense-ingester-homeassistant \
  --restart unless-stopped \
  -e HA_URL=http://homeassistant.local:8123 \
  -e HA_ACCESS_TOKEN=your_token_here \
  -e NODE_NAME=myhouse \
  -e LOCAL_MQTT_BROKER=mqtt.wesense.earth \
  -e LOCAL_MQTT_PORT=1883 \
  -e CLICKHOUSE_HOST=your-clickhouse-host \
  -e CLICKHOUSE_PORT=8123 \
  -e CLICKHOUSE_DATABASE=wesense \
  -v ./config:/app/config:ro \
  -v ha-ingester-logs:/app/logs \
  ghcr.io/wesense-earth/wesense-ingester-homeassistant:latest
```

### Home Assistant Access Token

1. In Home Assistant, click your profile name
2. Go to the **Security** tab
3. Scroll to **Long-lived access tokens** → **Create Token**
4. Copy the token (it won't be shown again)

## Local Development

```bash
# Install core library (from sibling directory)
pip install -e ../wesense-ingester-core

# Install adapter dependencies
pip install -r requirements.txt

# Configure
cp config/config.example.yaml config/config.yaml

# Required environment variables
export HA_URL="http://homeassistant.local:8123"
export HA_ACCESS_TOKEN="your_token"

# Run
python run.py

# MQTT-only mode (skip ClickHouse writes)
DISABLE_CLICKHOUSE=true python run.py
```

## Architecture

```
Home Assistant Instance
    ├─ WeSense ESP32 sensors (EXCLUDED — loop prevention)
    ├─ Ecowitt, Netatmo, Zigbee, Z-Wave, etc. (INCLUDED)
    │
    ▼  WebSocket / REST API
    │
Entity Filter → exclude WeSense MAC patterns, include environmental device classes
    │
    ▼  Transform HA state → WeSense reading format
    │
    ├─→ BufferedClickHouseWriter (batched inserts)
    └─→ MQTT Publisher (wesense/decoded/homeassistant/{country}/{subdiv}/{device})
```

## Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `HA_URL` | `http://homeassistant.local:8123` | Home Assistant URL |
| `HA_ACCESS_TOKEN` | | Long-lived access token |
| `NODE_NAME` | | Friendly name prefix for device IDs |
| `CLICKHOUSE_HOST` | `clickhouse` | ClickHouse server |
| `CLICKHOUSE_PORT` | `8123` | ClickHouse HTTP port |
| `CLICKHOUSE_DATABASE` | `wesense` | Database name |
| `LOCAL_MQTT_BROKER` | `emqx` | Output MQTT broker |
| `LOCAL_MQTT_PORT` | `1883` | Output MQTT port |
| `DISABLE_CLICKHOUSE` | `false` | Skip ClickHouse writes (MQTT-only) |
| `LOG_LEVEL` | `INFO` | Log level |

Filter configuration is in `config/config.yaml`. See `config/config.example.yaml` for all options.

## Loop Prevention

WeSense ESP32 entities follow the pattern `sensor.{location}_{mac}_{type}` (e.g. `sensor.office_301274c0e8fc_temperature`). The default exclusion pattern `.*_[0-9a-f]{12}_.*` matches the 12-character hex MAC address and prevents re-importing WeSense data.

## Related

- [wesense-ingester-core](https://github.com/wesense-earth/wesense-ingester-core) — Shared library
- [wesense-deploy](https://github.com/wesense-earth/wesense-deploy) — Docker Compose orchestration
- [wesense-respiro](https://github.com/wesense-earth/wesense-respiro) — Sensor map dashboard

## License

MIT
