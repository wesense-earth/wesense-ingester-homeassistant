"""ClickHouse writer for WeSense sensor data.

Thin wrapper around wesense-ingester-core's BufferedClickHouseWriter,
adapting the Home Assistant ingester's dict-based API to the core's
tuple-based API.
"""

import logging
from datetime import datetime, timezone
from typing import Any, Optional

from wesense_ingester import BufferedClickHouseWriter
from wesense_ingester.clickhouse.writer import ClickHouseConfig

from ..config import ClickHouseConfig as HAClickHouseConfig

logger = logging.getLogger(__name__)

# Future timestamp logger (provided by core's setup_logging with
# enable_future_timestamp_log=True)
future_timestamp_logger = logging.getLogger("wesense_ingester.future_timestamps")

# Column schema for Home Assistant data
CLICKHOUSE_COLUMNS = [
    "timestamp",
    "device_id",
    "data_source",
    "network_source",
    "ingestion_node_id",
    "reading_type",
    "value",
    "unit",
    "latitude",
    "longitude",
    "altitude",
    "geo_country",
    "geo_subdivision",
    "board_model",
    "deployment_type",
    "transport_type",
    "location_source",
    "node_name",
]


class ClickHouseWriter:
    """Write sensor data to ClickHouse database.

    Wraps core's BufferedClickHouseWriter with dict-to-tuple transformation.
    Periodic flushing and retry logic are handled by the core writer.
    """

    def __init__(self, config: HAClickHouseConfig, dry_run: bool = False):
        self.config = config
        self.dry_run = dry_run
        self._core_writer: Optional[BufferedClickHouseWriter] = None
        self._total_written_dry = 0

    def connect(self) -> bool:
        """Connect to ClickHouse."""
        if self.dry_run:
            logger.info("ClickHouse writer in dry-run mode - not connecting")
            return True

        try:
            core_config = ClickHouseConfig(
                host=self.config.host,
                port=self.config.port,
                user=self.config.user,
                password=self.config.password,
                database=self.config.database,
                table=self.config.table,
                batch_size=self.config.batch_size,
                flush_interval=self.config.flush_interval_seconds,
            )
            self._core_writer = BufferedClickHouseWriter(
                config=core_config,
                columns=CLICKHOUSE_COLUMNS,
            )
            logger.info(
                "Connected to ClickHouse at %s:%d (database: %s, table: %s)",
                self.config.host, self.config.port,
                self.config.database, self.config.table,
            )
            return True
        except Exception as e:
            logger.error("Failed to connect to ClickHouse: %s", e)
            return False

    def write(self, data: dict[str, Any]) -> bool:
        """Buffer data for batch writing to ClickHouse."""
        try:
            row = self._transform_to_row(data)
            if not row:
                return False

            if self.dry_run:
                logger.info("[DRY-RUN] Would write to ClickHouse: %s", data.get("device_id"))
                self._total_written_dry += 1
                return True

            if not self._core_writer:
                logger.warning("ClickHouse not connected - data dropped")
                return False

            self._core_writer.add(row)
            return True

        except Exception as e:
            logger.error("Error buffering data: %s", e)
            return False

    def _transform_to_row(self, data: dict[str, Any]) -> Optional[tuple]:
        """Transform WeSense data dict to ClickHouse row tuple."""
        try:
            measurements = data.get("measurements", [])
            if not measurements:
                return None

            measurement = measurements[0]

            return (
                datetime.fromtimestamp(data["timestamp"], tz=timezone.utc),
                data["device_id"],
                data.get("data_source", "HOMEASSISTANT"),
                "HOMEASSISTANT",
                data.get("node_name", ""),
                measurement["reading_type"],
                float(measurement["value"]),
                measurement.get("unit", ""),
                data.get("latitude", 0.0),
                data.get("longitude", 0.0),
                data.get("altitude", 0.0),
                data.get("country_code", ""),
                data.get("subdivision_code", ""),
                data.get("_meta", {}).get("device_class", ""),
                data.get("deployment_type", "INDOOR"),
                data.get("transport_type", "UNKNOWN"),
                "CONFIG",
                data.get("node_name", ""),
            )
        except Exception as e:
            logger.error("Error transforming row: %s", e)
            return None

    def flush(self) -> int:
        """Flush buffered data to ClickHouse."""
        if self.dry_run:
            return 0
        if self._core_writer:
            self._core_writer.flush()
            return 0  # Core doesn't return count from flush
        return 0

    async def start_periodic_flush(self):
        """No-op: core writer handles periodic flushing internally."""
        import asyncio
        # Keep the async interface for compatibility with main.py,
        # but the core writer already handles periodic flushing via its own timer.
        while True:
            await asyncio.sleep(3600)

    def close(self):
        """Close ClickHouse connection and flush remaining data."""
        if self._core_writer:
            self._core_writer.close()
        logger.info("ClickHouse writer closed. Total rows written: %d", self.total_written)

    @property
    def total_written(self) -> int:
        """Get total number of rows written."""
        if self.dry_run:
            return self._total_written_dry
        if self._core_writer:
            return self._core_writer.get_stats()["total_written"]
        return 0

    @property
    def buffer_size(self) -> int:
        """Get current buffer size."""
        if self._core_writer:
            return self._core_writer.get_stats()["buffer_size"]
        return 0
