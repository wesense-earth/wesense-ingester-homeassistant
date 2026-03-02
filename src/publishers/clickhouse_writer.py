"""Storage writer for WeSense sensor data.

Routes Home Assistant readings to the WeSense storage gateway.
"""

import logging
from typing import Any, Optional

from wesense_ingester.gateway.client import GatewayClient
from wesense_ingester.gateway.config import GatewayConfig

from ..config import ClickHouseConfig as HAClickHouseConfig

logger = logging.getLogger(__name__)


class ClickHouseWriter:
    """Write sensor data via the storage gateway.

    Accepts HA data dicts, transforms to ReadingIn-compatible dicts,
    and posts to the gateway. Retains the class name for import compat.
    """

    def __init__(self, config: HAClickHouseConfig = None, dry_run: bool = False):
        self.dry_run = dry_run
        self._gateway_client: Optional[GatewayClient] = None
        self._total_written_dry = 0

    def connect(self) -> bool:
        """Connect to storage gateway."""
        if self.dry_run:
            logger.info("Storage writer in dry-run mode - not connecting")
            return True

        try:
            self._gateway_client = GatewayClient(config=GatewayConfig.from_env())
            return True
        except Exception as e:
            logger.error("Failed to create gateway client: %s", e)
            return False

    def write(self, data: dict[str, Any]) -> bool:
        """Buffer data for batch writing to the gateway."""
        try:
            if self.dry_run:
                logger.info("[DRY-RUN] Would write: %s", data.get("device_id"))
                self._total_written_dry += 1
                return True

            if not self._gateway_client:
                logger.warning("Storage not connected - data dropped")
                return False

            reading_dict = self._transform(data)
            if not reading_dict:
                return False
            self._gateway_client.add(reading_dict)
            return True

        except Exception as e:
            logger.error("Error buffering data: %s", e)
            return False

    def _transform(self, data: dict[str, Any]) -> Optional[dict]:
        """Transform HA data dict to a ReadingIn-compatible dict."""
        try:
            measurements = data.get("measurements", [])
            if not measurements:
                return None

            measurement = measurements[0]

            return {
                "timestamp": data["timestamp"],
                "device_id": data["device_id"],
                "data_source": data.get("data_source", "HOMEASSISTANT"),
                "network_source": "HOMEASSISTANT",
                "ingestion_node_id": data.get("node_name", ""),
                "reading_type": measurement["reading_type"],
                "value": float(measurement["value"]),
                "unit": measurement.get("unit", ""),
                "latitude": data.get("latitude"),
                "longitude": data.get("longitude"),
                "altitude": data.get("altitude"),
                "geo_country": data.get("country_code", ""),
                "geo_subdivision": data.get("subdivision_code", ""),
                "board_model": data.get("_meta", {}).get("device_class", ""),
                "deployment_type": data.get("deployment_type", "INDOOR"),
                "transport_type": data.get("transport_type", "UNKNOWN"),
                "node_name": data.get("node_name", ""),
            }
        except Exception as e:
            logger.error("Error transforming data: %s", e)
            return None

    def flush(self) -> int:
        """Flush buffered data."""
        if self.dry_run:
            return 0
        if self._gateway_client:
            self._gateway_client.flush()
        return 0

    async def start_periodic_flush(self):
        """No-op: gateway client handles periodic flushing internally."""
        import asyncio
        while True:
            await asyncio.sleep(3600)

    def close(self):
        """Close gateway connection and flush remaining data."""
        if self._gateway_client:
            self._gateway_client.close()
        logger.info("Storage writer closed. Total rows written: %d", self.total_written)

    @property
    def total_written(self) -> int:
        """Get total number of rows written."""
        if self.dry_run:
            return self._total_written_dry
        if self._gateway_client:
            return self._gateway_client.get_stats()["total_written"]
        return 0

    @property
    def buffer_size(self) -> int:
        """Get current buffer size."""
        if self._gateway_client:
            return self._gateway_client.get_stats()["buffer_size"]
        return 0
