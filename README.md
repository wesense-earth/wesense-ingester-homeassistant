# WeSense Home Assistant Ingester

Pulls sensor data from Home Assistant and publishes it to the WeSense ecosystem (ClickHouse + MQTT).

## Features

- **Real-time updates** via WebSocket API or polling fallback
- **Loop prevention** - configurable filters to prevent re-importing WeSense devices
- **Standard output format** - compatible with other WeSense ingesters
- **Dual output** - publishes to both MQTT and ClickHouse

## Quick Start

1. **Create a Home Assistant Long-Lived Access Token:**
   
   - Click your profile name in Home Assistant
   - Go to the Security tab
   - Scroll to "Long-lived access tokens" and click Create Token
   - Copy the token (it won't be shown again)

2. **Configure the ingester:**
   
   ```bash
   cp config/config.example.yaml config/config.yaml
   # Edit config.yaml with your settings
   ```

3. **Run with Docker:**
   
   ```bash
   export HA_ACCESS_TOKEN="your_token_here"
   docker-compose up -d
   ```

## Configuration

See `config/config.example.yaml` for all options. Key settings:

### Loop Prevention (Critical!)

WeSense ESP32 devices publish to Home Assistant via MQTT discovery. Without proper filtering, this ingester would re-import that data, creating a feedback loop.

The default configuration excludes entities with MAC addresses in their ID:

```yaml
filters:
  exclude_entity_patterns:
    # WeSense ESP32 devices - MAC address pattern (12 hex chars)
    - ".*_[0-9a-f]{12}_.*"
```

### Location

Home Assistant sensors typically don't have GPS coordinates. Configure a default location:

```yaml
location:
  default:
    latitude: -36.8485
    longitude: 174.7633
    country_code: "nz"
    subdivision_code: "auk"
```

## Output

### MQTT Topics

```
wesense/decoded/homeassistant/{country}/{subdivision}/{entity_id}
```

### JSON Format

```json
{
  "device_id": "homeassistant_sensor_living_room_temperature",
  "timestamp": 1732291200,
  "latitude": -36.8485,
  "longitude": 174.7633,
  "country_code": "nz",
  "subdivision_code": "auk",
  "data_source": "HOMEASSISTANT",
  "measurements": [
    {
      "reading_type": "temperature",
      "value": 22.5,
      "unit": "�C"
    }
  ]
}
```

## Local Development

```bash
# Setup
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
cp config/config.example.yaml config/config.yaml

# Required environment variables
export HA_URL="http://homeassistant.local:8123"
export HA_ACCESS_TOKEN="your_token"

# Run normally
python run.py

# MQTT-only mode (skip ClickHouse writes)
export DISABLE_CLICKHOUSE=true
python run.py

# Full dry-run (no MQTT or ClickHouse writes, just logs)
# Set dry_run: true in config.yaml
```

**Tips:**

- Set `LOG_LEVEL=DEBUG` to see which entities are filtered/processed
- Use `mode: polling` in config for less frequent updates during testing
- Run a local MQTT broker: `docker run -p 1883:1883 eclipse-mosquitto:2`

## Architecture

See [ARCHITECTURE.md](ARCHITECTURE.md) for detailed design documentation.
