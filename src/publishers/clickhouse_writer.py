"""ClickHouse writer for WeSense sensor data."""

import asyncio
import logging
import time
from datetime import datetime
from logging.handlers import RotatingFileHandler
from typing import Any, Optional

import clickhouse_connect

from ..config import ClickHouseConfig

logger = logging.getLogger(__name__)

# Future timestamp tolerance - reject readings more than 30 seconds in the future
FUTURE_TIMESTAMP_TOLERANCE = 30

# Set up dedicated future timestamp logger
future_timestamp_logger = logging.getLogger('future_timestamps')
future_timestamp_logger.setLevel(logging.WARNING)
_future_ts_handler = RotatingFileHandler(
    'logs/future_timestamps.log',
    maxBytes=10 * 1024 * 1024,  # 10MB
    backupCount=5
)
_future_ts_handler.setFormatter(logging.Formatter('%(asctime)s | %(message)s'))
future_timestamp_logger.addHandler(_future_ts_handler)


class ClickHouseWriter:
    """Write sensor data to ClickHouse database."""

    def __init__(self, config: ClickHouseConfig, dry_run: bool = False):
        """
        Initialize the ClickHouse writer.

        Args:
            config: ClickHouse configuration
            dry_run: If True, log writes instead of executing
        """
        self.config = config
        self.dry_run = dry_run
        self._client = None
        self._buffer: list[dict] = []
        self._last_flush = time.time()
        self._total_written = 0
        self._flush_task: Optional[asyncio.Task] = None

    def connect(self) -> bool:
        """Connect to ClickHouse."""
        if self.dry_run:
            logger.info("ClickHouse writer in dry-run mode - not connecting")
            return True

        try:
            # Debug: log connection parameters (mask password)
            logger.info(
                f"Connecting to ClickHouse: host={self.config.host}, "
                f"port={self.config.port}, database={self.config.database}, "
                f"user={self.config.user}, password={'(empty)' if not self.config.password else '(set)'}"
            )

            # Match meshtastic ingester exactly: same parameter order
            self._client = clickhouse_connect.get_client(
                host=self.config.host,
                port=self.config.port,
                database=self.config.database,
                username=self.config.user,
                password=self.config.password,
            )

            # Test connection using ping() like meshtastic ingester does
            self._client.ping()
            logger.info(f"Connected to ClickHouse at {self.config.host}:{self.config.port}")
            logger.info(f"  Database: {self.config.database}, Table: {self.config.table}")
            return True

        except Exception as e:
            logger.error(f"Failed to connect to ClickHouse: {e}")
            if "Authentication failed" in str(e):
                logger.error(
                    "Authentication failed. Check your ClickHouse server config:\n"
                    "  1. Verify 'default' user exists in users.xml/users.d/\n"
                    "  2. Check if password is required (default ClickHouse has no password)\n"
                    "  3. Check <networks> section allows connections from this host\n"
                    "  4. Try: clickhouse-client -h {host} -u default to test manually"
                    .format(host=self.config.host)
                )
            return False

    def write(self, data: dict[str, Any]) -> bool:
        """
        Buffer data for batch writing to ClickHouse.

        Args:
            data: Transformed WeSense data

        Returns:
            True if buffered successfully
        """
        try:
            # Check for future timestamps
            timestamp = data.get("timestamp", 0)
            current_time = int(time.time())
            time_delta = timestamp - current_time

            if time_delta > FUTURE_TIMESTAMP_TOLERANCE:
                device_id = data.get("device_id", "unknown")
                # Format delta for readability
                if time_delta > 86400:
                    delta_str = f"{time_delta / 86400:.1f} days"
                elif time_delta > 3600:
                    delta_str = f"{time_delta / 3600:.1f} hours"
                elif time_delta > 60:
                    delta_str = f"{time_delta / 60:.1f} minutes"
                else:
                    delta_str = f"{time_delta} seconds"

                future_timestamp_logger.warning(
                    f"FUTURE_TIMESTAMP | device_id={device_id} | "
                    f"timestamp={datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')} | "
                    f"ahead_by={delta_str} | raw_delta_seconds={time_delta}"
                )
                logger.debug(f"Skipping future timestamp for {device_id}: {delta_str} ahead")
                return False

            # Transform to ClickHouse row format
            row = self._transform_to_row(data)
            if row:
                self._buffer.append(row)

            # Check if we should flush
            if (
                len(self._buffer) >= self.config.batch_size
                or time.time() - self._last_flush >= self.config.flush_interval_seconds
            ):
                self.flush()

            return True

        except Exception as e:
            logger.error(f"Error buffering data: {e}")
            return False

    def _transform_to_row(self, data: dict[str, Any]) -> Optional[dict]:
        """Transform WeSense data to ClickHouse row format.

        Uses same column names as meshtastic ingester for schema compatibility.
        """
        try:
            # Get first measurement (Home Assistant entities have one measurement)
            measurements = data.get("measurements", [])
            if not measurements:
                return None

            measurement = measurements[0]

            return {
                # Matches meshtastic ingester schema column names
                "timestamp": datetime.fromtimestamp(data["timestamp"]),
                "device_id": data["device_id"],
                "data_source": data.get("data_source", "HOMEASSISTANT"),
                "network_source": "HOMEASSISTANT",  # Fixed value for HA ingester
                "ingestion_node_id": data.get("node_name", ""),  # Use node_name as ingestion ID
                "reading_type": measurement["reading_type"],
                "value": float(measurement["value"]),
                "unit": measurement.get("unit", ""),
                "latitude": data.get("latitude", 0.0),
                "longitude": data.get("longitude", 0.0),
                "altitude": data.get("altitude", 0.0),
                "geo_country": data.get("country_code", ""),
                "geo_subdivision": data.get("subdivision_code", ""),
                "board_model": data.get("_meta", {}).get("device_class", ""),
                "deployment_type": data.get("deployment_type", "INDOOR"),
                "transport_type": data.get("transport_type", "UNKNOWN"),
                "location_source": "CONFIG",  # Location comes from config, not GPS
                "node_name": data.get("node_name", ""),
            }

        except Exception as e:
            logger.error(f"Error transforming row: {e}")
            return None

    def flush(self) -> int:
        """
        Flush buffered data to ClickHouse.

        Returns:
            Number of rows written
        """
        if not self._buffer:
            return 0

        rows_to_write = self._buffer.copy()
        self._buffer.clear()
        self._last_flush = time.time()

        if self.dry_run:
            logger.info(f"[DRY-RUN] Would write {len(rows_to_write)} rows to ClickHouse")
            self._total_written += len(rows_to_write)
            return len(rows_to_write)

        if not self._client:
            logger.warning("ClickHouse not connected - data dropped")
            return 0

        try:
            # Column names matching meshtastic ingester schema
            columns = [
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

            # Build data matrix
            data_matrix = [
                [row.get(col, "" if isinstance(row.get(col, ""), str) else 0) for col in columns]
                for row in rows_to_write
            ]

            self._client.insert(
                self.config.table,
                data_matrix,
                column_names=columns,
            )

            self._total_written += len(rows_to_write)
            logger.debug(f"Wrote {len(rows_to_write)} rows to ClickHouse")
            return len(rows_to_write)

        except Exception as e:
            logger.error(f"Failed to write to ClickHouse: {e}")
            # Put data back in buffer for retry
            self._buffer.extend(rows_to_write)
            return 0

    async def start_periodic_flush(self):
        """Start periodic flush task."""
        while True:
            await asyncio.sleep(self.config.flush_interval_seconds)
            self.flush()

    def close(self):
        """Close ClickHouse connection and flush remaining data."""
        if self._buffer:
            self.flush()

        if self._client:
            self._client.close()

        logger.info(f"ClickHouse writer closed. Total rows written: {self._total_written}")

    @property
    def total_written(self) -> int:
        """Get total number of rows written."""
        return self._total_written

    @property
    def buffer_size(self) -> int:
        """Get current buffer size."""
        return len(self._buffer)
