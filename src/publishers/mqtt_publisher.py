"""MQTT publisher for WeSense data."""

import json
import logging
from typing import Any, Optional

import paho.mqtt.client as mqtt

from ..config import MQTTConfig

logger = logging.getLogger(__name__)


class MQTTPublisher:
    """Publish transformed data to MQTT."""

    def __init__(self, config: MQTTConfig, dry_run: bool = False):
        """
        Initialize the MQTT publisher.

        Args:
            config: MQTT configuration
            dry_run: If True, log messages instead of publishing
        """
        self.config = config
        self.dry_run = dry_run
        self._client: Optional[mqtt.Client] = None
        self._connected = False
        self._message_count = 0

    def connect(self) -> bool:
        """Connect to the MQTT broker."""
        if self.dry_run:
            logger.info("MQTT publisher in dry-run mode - not connecting")
            return True

        try:
            self._client = mqtt.Client(
                client_id=self.config.client_id,
                protocol=mqtt.MQTTv5,
                callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
            )

            if self.config.username and self.config.password:
                self._client.username_pw_set(self.config.username, self.config.password)

            self._client.on_connect = self._on_connect
            self._client.on_disconnect = self._on_disconnect

            logger.info(f"Connecting to MQTT broker at {self.config.broker}:{self.config.port}")
            self._client.connect(self.config.broker, self.config.port, keepalive=60)
            self._client.loop_start()

            return True

        except Exception as e:
            logger.error(f"Failed to connect to MQTT broker: {e}")
            return False

    def _on_connect(self, client, userdata, flags, reason_code, properties):
        """Handle MQTT connection."""
        if reason_code == 0:
            logger.info("Connected to MQTT broker")
            self._connected = True
        else:
            logger.error(f"MQTT connection failed with code: {reason_code}")
            self._connected = False

    def _on_disconnect(self, client, userdata, flags, reason_code, properties):
        """Handle MQTT disconnection."""
        logger.warning(f"Disconnected from MQTT broker: {reason_code}")
        self._connected = False

    def publish(self, topic: str, data: dict[str, Any]) -> bool:
        """
        Publish data to MQTT topic.

        Args:
            topic: The MQTT topic
            data: The data to publish (will be JSON encoded)

        Returns:
            True if publish succeeded
        """
        # Remove internal metadata before publishing
        publish_data = {k: v for k, v in data.items() if not k.startswith("_")}

        if self.dry_run:
            logger.info(f"[DRY-RUN] Would publish to {topic}: {json.dumps(publish_data)}")
            self._message_count += 1
            return True

        if not self._client or not self._connected:
            logger.warning("MQTT not connected - message dropped")
            return False

        try:
            payload = json.dumps(publish_data)
            result = self._client.publish(topic, payload, qos=1)

            if result.rc == mqtt.MQTT_ERR_SUCCESS:
                self._message_count += 1
                logger.debug(f"Published to {topic}")
                return True
            else:
                logger.error(f"Failed to publish to {topic}: {result.rc}")
                return False

        except Exception as e:
            logger.error(f"Error publishing to {topic}: {e}")
            return False

    def disconnect(self):
        """Disconnect from the MQTT broker."""
        if self._client:
            try:
                self._client.loop_stop()
                self._client.disconnect()
            except Exception as e:
                logger.debug(f"Error during MQTT disconnect: {e}")
            self._client = None
            self._connected = False
        logger.info(f"MQTT disconnected. Total messages published: {self._message_count}")

    @property
    def is_connected(self) -> bool:
        """Check if connected to MQTT broker."""
        return self._connected or self.dry_run

    @property
    def message_count(self) -> int:
        """Get total number of messages published."""
        return self._message_count
