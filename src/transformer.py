"""Transform Home Assistant entities to WeSense format."""

import logging
import re
import time
from datetime import datetime
from typing import TYPE_CHECKING, Any, Optional

from dateutil import parser as date_parser

from .config import Config, LocationConfig
from .utils.geocoding import get_location_info
from .utils.reading_types import (
    READING_UNITS,
    convert_unit,
    get_standard_unit,
    get_wesense_reading_type,
)

if TYPE_CHECKING:
    from .ha_client import EntityMetadata

logger = logging.getLogger(__name__)


class Transformer:
    """Transform Home Assistant state data to WeSense format."""

    def __init__(self, config: Config, metadata: Optional["EntityMetadata"] = None):
        """
        Initialize the transformer.

        Args:
            config: Application configuration
            metadata: Entity metadata from Home Assistant (for area lookup)
        """
        self.config = config
        self.location_config = config.location
        self.metadata = metadata
        self.node_name: str = ""  # Default node name (friendly name in payload)

    def transform(self, entity_id: str, state: dict[str, Any]) -> Optional[dict[str, Any]]:
        """
        Transform a Home Assistant state to WeSense format.

        Args:
            entity_id: The entity ID
            state: The Home Assistant state dict

        Returns:
            WeSense formatted dict, or None if transformation fails
        """
        try:
            attributes = state.get("attributes", {})

            # Get reading type from device class
            device_class = attributes.get("device_class", "")
            reading_type = get_wesense_reading_type(device_class)

            if not reading_type:
                # Try to infer from entity_id or unit
                reading_type = self._infer_reading_type(entity_id, attributes)

            if not reading_type:
                logger.debug(f"Could not determine reading type for {entity_id}")
                return None

            # Parse value
            try:
                value = float(state.get("state"))
            except (ValueError, TypeError):
                logger.debug(f"Non-numeric state for {entity_id}: {state.get('state')}")
                return None

            # Get and convert unit
            ha_unit = attributes.get("unit_of_measurement", "")
            standard_unit = get_standard_unit(reading_type)
            if ha_unit and standard_unit and ha_unit != standard_unit:
                value = convert_unit(value, ha_unit, standard_unit)

            # Parse timestamp - prefer last_reported (actual measurement time) if available
            # Fall back to last_changed, then last_updated
            timestamp_str = (
                state.get("last_reported")
                or state.get("last_changed")
                or state.get("last_updated")
            )
            timestamp = self._parse_timestamp(timestamp_str)

            # Get location
            location = self._get_location(entity_id, attributes)

            # Get node name (friendly name for this entity)
            node_name = self._get_node_name(entity_id)

            # Build device ID: {node_name}_{entity_suffix} for uniqueness
            device_id = self._build_device_id(entity_id, node_name)

            # Get device location (area name from Home Assistant)
            device_location = self._get_device_location(entity_id)

            # Get device info from metadata
            device_info = self._get_device_info(entity_id)

            # Build the WeSense format
            result = {
                "device_id": device_id,
                "node_name": node_name,  # Friendly name (like NODE_NAME in ESP32)
                "data_source": "HOMEASSISTANT",
                "timestamp": timestamp,
                "latitude": location["latitude"],
                "longitude": location["longitude"],
                "altitude": location["altitude"],
                "vendor": device_info.get("manufacturer") or "",
                "product_line": (device_info.get("platform") or "").upper() or "HOMEASSISTANT",
                "device_type": "SENSOR",
                "deployment_type": location["deployment_type"],
                "transport_type": self._infer_transport_type(device_info.get("platform") or ""),
                "measurements": [
                    {
                        "reading_type": reading_type,
                        "reading_type_raw": reading_type.upper(),
                        "value": round(value, 4),
                        "unit": standard_unit or ha_unit,
                        "sensor_model": device_info.get("model") or "",
                        "timestamp": timestamp,
                    }
                ],
                "country_code": location["country_code"],
                "subdivision_code": location["subdivision_code"],
                "device_location": device_location,
                # Additional metadata (stripped before publishing)
                "_meta": {
                    "entity_id": entity_id,
                    "friendly_name": attributes.get("friendly_name", entity_id),
                    "device_class": device_class,
                    "original_unit": ha_unit,
                },
            }

            return result

        except Exception as e:
            logger.error(f"Failed to transform {entity_id}: {e}")
            return None

    def _get_node_name(self, entity_id: str) -> str:
        """Get node name for an entity, checking for per-entity override."""
        # Check for entity-specific override
        if entity_id in self.location_config.overrides:
            override = self.location_config.overrides[entity_id]
            if override.node_name:
                return override.node_name

        # Use default node name
        return self.node_name

    def _build_device_id(self, entity_id: str, node_name: str) -> str:
        """Build a WeSense device ID.

        Format: {node_name}_{ha_device_identifier}
        - node_name: friendly name prefix
        - ha_device_identifier: derived from HA device (so all sensors from same
          physical device share one device_id)

        This ensures that e.g. temperature, humidity, illuminance from the same
        Shelly device all have the same device_id.
        """
        # Try to get the HA device ID from metadata
        ha_device_id = None
        if self.metadata:
            entity_info = self.metadata.get_entity_info(entity_id)
            ha_device_id = entity_info.get("device_id")

        if ha_device_id:
            # Use full HA device_id for uniqueness
            device_suffix = re.sub(r"[^a-zA-Z0-9_]", "_", ha_device_id)
        else:
            # Fallback: use entity suffix (old behavior)
            if "." in entity_id:
                device_suffix = entity_id.split(".", 1)[1]
            else:
                device_suffix = entity_id
            device_suffix = re.sub(r"[^a-zA-Z0-9_]", "_", device_suffix)

        if node_name:
            sanitized_name = re.sub(r"[^a-zA-Z0-9_]", "_", node_name)
            device_id = f"{sanitized_name}_{device_suffix}"
        else:
            device_id = f"ha_{device_suffix}"
            logger.warning(f"No node_name configured - using fallback: {device_id}")

        logger.debug(f"Built device_id: {device_id} (from entity: {entity_id})")
        return device_id

    def _get_device_location(self, entity_id: str) -> str:
        """Get device location (area name) from Home Assistant metadata."""
        if not self.metadata:
            return ""

        entity_info = self.metadata.get_entity_info(entity_id)
        return entity_info.get("area_name", "")

    def _get_device_info(self, entity_id: str) -> dict:
        """Get device info (manufacturer, model, platform) from metadata."""
        if not self.metadata:
            return {}

        return self.metadata.get_entity_info(entity_id)

    def _infer_transport_type(self, platform: str) -> str:
        """Infer transport type from integration/platform name."""
        if not platform:
            return "UNKNOWN"
        platform_lower = platform.lower()

        # WiFi-based integrations
        wifi_platforms = ["shelly", "tuya", "wiz", "esphome", "tasmota", "sonoff"]
        if any(p in platform_lower for p in wifi_platforms):
            return "WIFI"

        # Zigbee integrations
        zigbee_platforms = ["zha", "zigbee", "deconz", "zigbee2mqtt"]
        if any(p in platform_lower for p in zigbee_platforms):
            return "ZIGBEE"

        # Z-Wave integrations
        zwave_platforms = ["zwave", "ozw"]
        if any(p in platform_lower for p in zwave_platforms):
            return "ZWAVE"

        # Bluetooth integrations
        bt_platforms = ["bluetooth", "ble", "switchbot"]
        if any(p in platform_lower for p in bt_platforms):
            return "BLUETOOTH"

        # Thread/Matter
        thread_platforms = ["matter", "thread"]
        if any(p in platform_lower for p in thread_platforms):
            return "THREAD"

        # Default
        return "UNKNOWN"

    def _parse_timestamp(self, timestamp_str: Optional[str]) -> int:
        """Parse timestamp string to Unix timestamp."""
        if not timestamp_str:
            return int(time.time())

        try:
            dt = date_parser.parse(timestamp_str)
            return int(dt.timestamp())
        except (ValueError, TypeError):
            return int(time.time())

    def _get_location(self, entity_id: str, attributes: dict) -> dict[str, Any]:
        """Get location for an entity."""
        defaults = self.location_config.default

        # Check for entity-specific override
        if entity_id in self.location_config.overrides:
            override = self.location_config.overrides[entity_id]
            lat = override.latitude
            lon = override.longitude
            alt = override.altitude if override.altitude is not None else defaults.altitude
            deployment = override.deployment_type if override.deployment_type else defaults.deployment_type

            # Get country/subdivision from override or geocoding
            if override.country_code and override.subdivision_code:
                return {
                    "latitude": lat,
                    "longitude": lon,
                    "altitude": alt,
                    "country_code": override.country_code,
                    "subdivision_code": override.subdivision_code,
                    "deployment_type": deployment,
                }
            else:
                geo = get_location_info(
                    lat,
                    lon,
                    defaults.country_code,
                    defaults.subdivision_code,
                )
                return {
                    "latitude": lat,
                    "longitude": lon,
                    "altitude": alt,
                    "deployment_type": deployment,
                    **geo,
                }

        # Check for coordinates in attributes (some integrations provide this)
        if "latitude" in attributes and "longitude" in attributes:
            lat = float(attributes["latitude"])
            lon = float(attributes["longitude"])
            alt = float(attributes.get("altitude", defaults.altitude))
            geo = get_location_info(
                lat,
                lon,
                defaults.country_code,
                defaults.subdivision_code,
            )
            return {
                "latitude": lat,
                "longitude": lon,
                "altitude": alt,
                "deployment_type": defaults.deployment_type,
                **geo,
            }

        # Use defaults
        return {
            "latitude": defaults.latitude,
            "longitude": defaults.longitude,
            "altitude": defaults.altitude,
            "country_code": defaults.country_code,
            "subdivision_code": defaults.subdivision_code,
            "deployment_type": defaults.deployment_type,
        }

    def _infer_reading_type(self, entity_id: str, attributes: dict) -> Optional[str]:
        """Attempt to infer reading type from entity ID or unit."""
        lower_id = entity_id.lower()

        # Common patterns in entity IDs
        id_patterns = {
            "temperature": "temperature",
            "temp": "temperature",
            "humidity": "humidity",
            "pressure": "pressure",
            "co2": "co2",
            "carbon_dioxide": "co2",
            "pm2_5": "pm2_5",
            "pm25": "pm2_5",
            "pm10": "pm10",
            "pm1": "pm1_0",
            "voc": "voc",
            "illuminance": "light_level",
            "lux": "light_level",
            "battery": "battery_level",
            "voltage": "voltage",
            "power": "power",
            "energy": "energy",
        }

        for pattern, reading_type in id_patterns.items():
            if pattern in lower_id:
                return reading_type

        # Infer from unit of measurement
        unit = attributes.get("unit_of_measurement", "").lower()
        unit_patterns = {
            "°c": "temperature",
            "°f": "temperature",
            "%": "humidity",  # Could also be battery
            "hpa": "pressure",
            "mbar": "pressure",
            "ppm": "co2",
            "µg/m³": "pm2_5",  # Could be other PM
            "lux": "light_level",
            "v": "voltage",
            "w": "power",
            "kwh": "energy",
        }

        for pattern, reading_type in unit_patterns.items():
            if pattern in unit:
                return reading_type

        return None

    def build_mqtt_topic(self, transformed: dict[str, Any]) -> str:
        """Build the MQTT topic for publishing.

        Format: {prefix}/{data_source}/{country}/{subdivision}/{device_id}
        Matches ESP32 topic structure.
        """
        prefix = self.config.mqtt.topic_prefix
        data_source = transformed.get("data_source", "homeassistant").lower()
        country = transformed["country_code"]
        subdivision = transformed["subdivision_code"]
        device_id = transformed["device_id"]

        return f"{prefix}/{data_source}/{country}/{subdivision}/{device_id}"
