#!/usr/bin/env python3
"""
WeSense Home Assistant Ingester

Pulls sensor data from Home Assistant and publishes to WeSense ecosystem.
"""

import asyncio
import logging
import signal
import sys
import time
from datetime import datetime
from typing import Any, Optional

from wesense_ingester import setup_logging as core_setup_logging

from .config import Config, load_config, validate_config
from .entity_filter import EntityFilter
from .ha_client import EntityMetadata, HomeAssistantClient
from .publishers.clickhouse_writer import ClickHouseWriter, future_timestamp_logger
from .publishers.mqtt_publisher import MQTTPublisher
from .transformer import Transformer

logger = logging.getLogger(__name__)

# Future timestamp tolerance - reject readings more than 30 seconds in the future
FUTURE_TIMESTAMP_TOLERANCE = 30


class HomeAssistantIngester:
    """Main ingester class orchestrating all components."""

    def __init__(self, config: Config):
        """
        Initialize the ingester.

        Args:
            config: Application configuration
        """
        self.config = config
        self.ha_client = HomeAssistantClient(
            url=config.homeassistant.url,
            access_token=config.homeassistant.access_token,
        )
        self.metadata = EntityMetadata()
        self.entity_filter: Optional[EntityFilter] = None
        self.transformer = Transformer(config, self.metadata)
        self.mqtt_publisher = MQTTPublisher(config.mqtt, dry_run=config.dry_run)
        # disable_clickhouse skips CH writes while still publishing to MQTT
        clickhouse_dry_run = config.dry_run or config.disable_clickhouse
        self.clickhouse_writer = ClickHouseWriter(config.clickhouse, dry_run=clickhouse_dry_run)
        self._running = False
        self._stop_event = asyncio.Event()
        self._stats = {
            "state_changes_received": 0,
            "entities_filtered": 0,
            "entities_processed": 0,
            "transform_failures": 0,
            "future_timestamps": 0,
            "missing_location": 0,
            "publish_failures": 0,
        }

    async def start(self):
        """Start the ingester."""
        logger.info("Starting Home Assistant Ingester")
        logger.info(f"Mode: {'DRY-RUN' if self.config.dry_run else 'LIVE'}")
        if self.config.disable_clickhouse:
            logger.info("ClickHouse writes DISABLED (MQTT-only mode)")
        logger.info(f"Update mode: {self.config.homeassistant.mode}")

        # Connect to MQTT
        if not self.mqtt_publisher.connect():
            logger.error("Failed to connect to MQTT broker")
            return

        # Connect to ClickHouse
        if not self.clickhouse_writer.connect():
            logger.warning("Failed to connect to ClickHouse - continuing without database writes")

        # Set node_name (friendly name like NODE_NAME in ESP32)
        if self.config.node_name:
            self.transformer.node_name = self.config.node_name
            logger.info(f"Using node_name: {self.config.node_name}")
        else:
            logger.warning("No node_name configured - device IDs will use 'ha_' prefix")

        # Initialize entity filter with empty metadata (will be populated later)
        self.entity_filter = EntityFilter(self.config.filters, self.metadata)

        # Run initial safety check (uses REST API)
        await self._safety_check()

        # Start the appropriate update mode
        self._running = True

        if self.config.homeassistant.mode == "websocket":
            await self._run_websocket_mode()
        else:
            await self._run_polling_mode()

    async def _safety_check(self):
        """Check for potential feedback loop issues."""
        logger.info("Running safety check for potential loops...")

        try:
            states = await self.ha_client.get_states()
            suspicious = self.entity_filter.find_suspicious_entities(states)

            if suspicious:
                logger.warning(
                    f"Found {len(suspicious)} potentially looping entities that will be ingested:"
                )
                for entity_id in suspicious[:10]:  # Show first 10
                    logger.warning(f"  - {entity_id}")
                if len(suspicious) > 10:
                    logger.warning(f"  ... and {len(suspicious) - 10} more")
                logger.warning(
                    "Review your exclude_entity_patterns config to prevent feedback loops!"
                )
            else:
                logger.info("Safety check passed - no obvious loop risks detected")

        except Exception as e:
            logger.warning(f"Safety check failed: {e}")

    async def _interruptible_sleep(self, seconds: float):
        """Sleep that can be interrupted by stop()."""
        try:
            await asyncio.wait_for(self._stop_event.wait(), timeout=seconds)
        except asyncio.TimeoutError:
            pass  # Normal timeout, continue

    async def _run_websocket_mode(self):
        """Run using WebSocket for real-time updates."""
        logger.info("Starting WebSocket mode")

        while self._running:
            try:
                # Connect WebSocket
                if not await self.ha_client.connect_websocket():
                    logger.error("Failed to connect WebSocket, retrying in 30s...")
                    await self._interruptible_sleep(30)
                    if not self._running:
                        break
                    continue

                # Start the WebSocket message loop in background first
                # (needed to receive responses to WebSocket messages)
                ws_task = asyncio.create_task(self.ha_client.run_websocket_loop())

                # Small delay to let the loop start
                await asyncio.sleep(0.1)

                # Load metadata now that WebSocket loop is running
                try:
                    await self.metadata.load_from_client(self.ha_client)
                    # Update entity filter with new metadata
                    self.entity_filter = EntityFilter(self.config.filters, self.metadata)
                except Exception as e:
                    logger.warning(f"Failed to load metadata: {e}")

                # Subscribe to state changes
                try:
                    await self.ha_client.subscribe_state_changes(self._handle_state_change)
                except Exception as e:
                    logger.error(f"Failed to subscribe to state changes: {e}")
                    ws_task.cancel()
                    continue

                # Start ClickHouse periodic flush
                flush_task = asyncio.create_task(
                    self.clickhouse_writer.start_periodic_flush()
                )

                # Wait for either WebSocket loop to finish OR stop signal
                stop_wait_task = asyncio.create_task(self._stop_event.wait())
                done, pending = await asyncio.wait(
                    [ws_task, stop_wait_task],
                    return_when=asyncio.FIRST_COMPLETED,
                )

                # Cancel pending tasks
                for task in pending:
                    task.cancel()
                    try:
                        await task
                    except asyncio.CancelledError:
                        pass

                flush_task.cancel()
                try:
                    await flush_task
                except asyncio.CancelledError:
                    pass

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"WebSocket error: {e}, reconnecting in 10s...")
                await self._interruptible_sleep(10)
                if not self._running:
                    break

    async def _run_polling_mode(self):
        """Run using REST API polling."""
        logger.info(
            f"Starting polling mode (interval: {self.config.homeassistant.polling_interval_seconds}s)"
        )

        # Start ClickHouse periodic flush
        flush_task = asyncio.create_task(self.clickhouse_writer.start_periodic_flush())

        try:
            await self.ha_client.poll_states(
                callback=self._handle_state_change,
                interval_seconds=self.config.homeassistant.polling_interval_seconds,
            )
        except asyncio.CancelledError:
            pass
        finally:
            flush_task.cancel()

    async def _handle_state_change(
        self,
        entity_id: str,
        old_state: Optional[dict],
        new_state: Optional[dict],
    ):
        """
        Handle a state change event.

        Args:
            entity_id: The entity ID
            old_state: Previous state (None for new entities)
            new_state: New state
        """
        self._stats["state_changes_received"] += 1

        if not new_state:
            return

        # Apply filters
        if not self.entity_filter.should_ingest(entity_id, new_state):
            self._stats["entities_filtered"] += 1
            return

        # Transform to WeSense format
        transformed = self.transformer.transform(entity_id, new_state)
        if not transformed:
            self._stats["transform_failures"] += 1
            return

        # Check for future timestamps (reject readings > 30s in the future)
        timestamp = transformed.get("timestamp", 0)
        current_time = int(time.time())
        time_delta = timestamp - current_time

        if time_delta > FUTURE_TIMESTAMP_TOLERANCE:
            self._stats["future_timestamps"] += 1
            device_id = transformed.get("device_id", "unknown")
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
            logger.debug(f"Skipping future timestamp for {entity_id}: {delta_str} ahead")
            return

        # Check for valid location (reject if missing or null island)
        lat = transformed.get("latitude", 0.0)
        lon = transformed.get("longitude", 0.0)
        if lat == 0.0 and lon == 0.0:
            self._stats["missing_location"] += 1
            logger.debug(f"Skipping {entity_id}: missing or invalid location (0, 0)")
            return

        self._stats["entities_processed"] += 1

        # Build MQTT topic
        topic = self.transformer.build_mqtt_topic(transformed)

        # Publish to MQTT
        if not self.mqtt_publisher.publish(topic, transformed):
            self._stats["publish_failures"] += 1

        # Write to ClickHouse
        self.clickhouse_writer.write(transformed)

        logger.debug(
            f"Processed {entity_id} -> {transformed['measurements'][0]['reading_type']}: "
            f"{transformed['measurements'][0]['value']}"
        )

    async def stop(self):
        """Stop the ingester gracefully."""
        logger.info("Stopping ingester...")
        self._running = False
        self._stop_event.set()  # Wake up any sleeping tasks

        await self.ha_client.close()
        self.mqtt_publisher.disconnect()
        self.clickhouse_writer.close()

        # Log final stats
        logger.info("Final statistics:")
        logger.info(f"  State changes received: {self._stats['state_changes_received']}")
        logger.info(f"  Entities filtered: {self._stats['entities_filtered']}")
        logger.info(f"  Entities processed: {self._stats['entities_processed']}")
        logger.info(f"  Transform failures: {self._stats['transform_failures']}")
        logger.info(f"  Future timestamps rejected: {self._stats['future_timestamps']}")
        logger.info(f"  Missing location rejected: {self._stats['missing_location']}")
        logger.info(f"  Publish failures: {self._stats['publish_failures']}")
        logger.info(f"  MQTT messages: {self.mqtt_publisher.message_count}")
        logger.info(f"  ClickHouse rows: {self.clickhouse_writer.total_written}")

        if self.entity_filter:
            filter_stats = self.entity_filter.get_filter_stats()
            logger.info(f"  Filter stats: {filter_stats}")


async def main():
    """Main entry point."""
    # Load configuration
    try:
        config = load_config()
    except FileNotFoundError as e:
        print(f"Config not found: {e}")
        print("Copy config/config.yaml.sample to config/config.yaml and configure it.")
        print("Exiting cleanly (will not restart).")
        sys.exit(0)
    except Exception as e:
        print(f"Failed to load configuration: {e}")
        sys.exit(1)

    # Setup logging (core provides colored console + rotating file logs)
    core_setup_logging(
        "ha_ingester",
        level=getattr(logging, config.logging.level.upper(), logging.INFO),
        enable_future_timestamp_log=True,
    )

    # Reduce noise from third-party libraries
    logging.getLogger("aiohttp").setLevel(logging.WARNING)
    logging.getLogger("websockets").setLevel(logging.WARNING)

    # Validate configuration
    warnings = validate_config(config)
    for warning in warnings:
        logger.warning(f"Config warning: {warning}")

    # Create ingester
    ingester = HomeAssistantIngester(config)

    # Handle shutdown signals
    def request_shutdown():
        logger.info("Received shutdown signal (Ctrl+C)")
        ingester._running = False
        ingester._stop_event.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, request_shutdown)

    try:
        await ingester.start()
    except Exception as e:
        logger.error(f"Error: {e}")
    finally:
        await ingester.stop()


if __name__ == "__main__":
    asyncio.run(main())
