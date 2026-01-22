"""Home Assistant API client supporting both REST and WebSocket APIs."""

import asyncio
import json
import logging
from typing import Any, Callable, Optional

import aiohttp

logger = logging.getLogger(__name__)


class HomeAssistantClient:
    """Client for Home Assistant REST and WebSocket APIs."""

    def __init__(self, url: str, access_token: str):
        """
        Initialize the Home Assistant client.

        Args:
            url: Home Assistant URL (e.g., http://homeassistant.local:8123)
            access_token: Long-lived access token
        """
        self.url = url.rstrip("/")
        self.access_token = access_token
        self._session: Optional[aiohttp.ClientSession] = None
        self._ws: Optional[aiohttp.ClientWebSocketResponse] = None
        self._ws_id = 0
        self._ws_callbacks: dict[int, asyncio.Future] = {}
        self._state_change_callback: Optional[Callable] = None
        self._running = False

    @property
    def headers(self) -> dict[str, str]:
        """Get authentication headers."""
        return {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json",
        }

    async def _get_session(self) -> aiohttp.ClientSession:
        """Get or create HTTP session."""
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(headers=self.headers)
        return self._session

    async def close(self):
        """Close all connections."""
        self._running = False
        if self._ws and not self._ws.closed:
            await self._ws.close()
        if self._session and not self._session.closed:
            await self._session.close()

    # -------------------------------------------------------------------------
    # REST API Methods
    # -------------------------------------------------------------------------

    async def get_states(self) -> list[dict[str, Any]]:
        """Get all entity states via REST API."""
        session = await self._get_session()
        async with session.get(f"{self.url}/api/states") as resp:
            resp.raise_for_status()
            return await resp.json()

    async def get_state(self, entity_id: str) -> dict[str, Any]:
        """Get single entity state via REST API."""
        session = await self._get_session()
        async with session.get(f"{self.url}/api/states/{entity_id}") as resp:
            resp.raise_for_status()
            return await resp.json()

    async def get_config(self) -> dict[str, Any]:
        """Get Home Assistant configuration."""
        session = await self._get_session()
        async with session.get(f"{self.url}/api/config") as resp:
            resp.raise_for_status()
            return await resp.json()

    # -------------------------------------------------------------------------
    # WebSocket API Methods
    # -------------------------------------------------------------------------

    async def connect_websocket(self) -> bool:
        """Establish WebSocket connection and authenticate."""
        try:
            session = await self._get_session()
            ws_url = self.url.replace("http://", "ws://").replace("https://", "wss://")
            self._ws = await session.ws_connect(f"{ws_url}/api/websocket")

            # Wait for auth_required message
            msg = await self._ws.receive_json()
            if msg.get("type") != "auth_required":
                logger.error(f"Unexpected message: {msg}")
                return False

            # Send authentication
            await self._ws.send_json({"type": "auth", "access_token": self.access_token})

            # Wait for auth result
            msg = await self._ws.receive_json()
            if msg.get("type") != "auth_ok":
                logger.error(f"Authentication failed: {msg}")
                return False

            logger.info("WebSocket connected and authenticated")
            return True

        except Exception as e:
            logger.error(f"WebSocket connection failed: {e}")
            return False

    async def _send_ws_message(self, message: dict) -> dict[str, Any]:
        """Send a WebSocket message and wait for response."""
        if not self._ws or self._ws.closed:
            raise RuntimeError("WebSocket not connected")

        self._ws_id += 1
        msg_id = self._ws_id
        message["id"] = msg_id

        future = asyncio.get_event_loop().create_future()
        self._ws_callbacks[msg_id] = future

        await self._ws.send_json(message)

        try:
            result = await asyncio.wait_for(future, timeout=30.0)
            return result
        finally:
            self._ws_callbacks.pop(msg_id, None)

    async def _handle_ws_message(self, msg: dict):
        """Handle incoming WebSocket message."""
        msg_id = msg.get("id")
        msg_type = msg.get("type")

        # Handle response to our requests
        if msg_id and msg_id in self._ws_callbacks:
            self._ws_callbacks[msg_id].set_result(msg)
            return

        # Handle events
        if msg_type == "event":
            event = msg.get("event", {})
            event_type = event.get("event_type")

            if event_type == "state_changed" and self._state_change_callback:
                data = event.get("data", {})
                await self._state_change_callback(
                    entity_id=data.get("entity_id"),
                    old_state=data.get("old_state"),
                    new_state=data.get("new_state"),
                )

    async def subscribe_state_changes(self, callback: Callable) -> int:
        """
        Subscribe to state change events.

        Args:
            callback: Async function called with (entity_id, old_state, new_state)

        Returns:
            Subscription ID
        """
        self._state_change_callback = callback

        result = await self._send_ws_message(
            {"type": "subscribe_events", "event_type": "state_changed"}
        )

        if not result.get("success"):
            raise RuntimeError(f"Failed to subscribe: {result}")

        logger.info("Subscribed to state changes")
        return result.get("id", 0)

    async def get_device_registry(self) -> list[dict[str, Any]]:
        """Get device registry via WebSocket."""
        result = await self._send_ws_message({"type": "config/device_registry/list"})
        return result.get("result", [])

    async def get_entity_registry(self) -> list[dict[str, Any]]:
        """Get entity registry via WebSocket."""
        result = await self._send_ws_message({"type": "config/entity_registry/list"})
        return result.get("result", [])

    async def get_area_registry(self) -> list[dict[str, Any]]:
        """Get area registry via WebSocket."""
        result = await self._send_ws_message({"type": "config/area_registry/list"})
        return result.get("result", [])

    async def get_core_config(self) -> dict[str, Any]:
        """Get core config via WebSocket (may include UUID)."""
        result = await self._send_ws_message({"type": "get_config"})
        return result.get("result", {})

    async def run_websocket_loop(self):
        """Run the WebSocket message processing loop."""
        if not self._ws:
            raise RuntimeError("WebSocket not connected")

        self._running = True
        logger.info("Starting WebSocket message loop")

        while self._running:
            try:
                # Use timeout so we can check _running periodically
                msg = await asyncio.wait_for(self._ws.receive(), timeout=1.0)

                if msg.type == aiohttp.WSMsgType.TEXT:
                    data = json.loads(msg.data)
                    await self._handle_ws_message(data)

                elif msg.type == aiohttp.WSMsgType.CLOSED:
                    logger.warning("WebSocket closed by server")
                    break

                elif msg.type == aiohttp.WSMsgType.ERROR:
                    logger.error(f"WebSocket error: {self._ws.exception()}")
                    break

            except asyncio.TimeoutError:
                continue  # Check _running and loop again
            except asyncio.CancelledError:
                logger.info("WebSocket loop cancelled")
                break
            except Exception as e:
                logger.error(f"Error in WebSocket loop: {e}")
                await asyncio.sleep(1)

        self._running = False

    async def poll_states(
        self, callback: Callable, interval_seconds: int = 60
    ):
        """
        Poll states periodically and call callback for changes.

        Args:
            callback: Async function called with (entity_id, old_state, new_state)
            interval_seconds: Polling interval
        """
        self._running = True
        previous_states: dict[str, dict] = {}

        logger.info(f"Starting polling loop (interval: {interval_seconds}s)")

        while self._running:
            try:
                states = await self.get_states()

                for state in states:
                    entity_id = state.get("entity_id")
                    if not entity_id:
                        continue

                    old_state = previous_states.get(entity_id)
                    if old_state != state:
                        await callback(
                            entity_id=entity_id,
                            old_state=old_state,
                            new_state=state,
                        )
                        previous_states[entity_id] = state

                # Sleep in 1-second chunks so we can respond to stop signals
                for _ in range(interval_seconds):
                    if not self._running:
                        break
                    await asyncio.sleep(1)

            except asyncio.CancelledError:
                logger.info("Polling loop cancelled")
                break
            except Exception as e:
                logger.error(f"Error in polling loop: {e}")
                # Sleep in chunks so we can respond to stop signals
                for _ in range(min(interval_seconds, 10)):
                    if not self._running:
                        break
                    await asyncio.sleep(1)

        self._running = False


class EntityMetadata:
    """Stores entity and device metadata from Home Assistant registries."""

    def __init__(self):
        self.entities: dict[str, dict] = {}  # entity_id -> entity registry entry
        self.devices: dict[str, dict] = {}  # device_id -> device registry entry
        self.areas: dict[str, dict] = {}  # area_id -> area registry entry

    async def load_from_client(self, client: HomeAssistantClient):
        """Load all metadata from Home Assistant.

        Note: Requires WebSocket to be connected and message loop running.
        """
        logger.info("Loading entity metadata from Home Assistant...")

        # Require WebSocket to be already connected (with message loop running)
        if not client._ws or client._ws.closed:
            logger.warning("WebSocket not connected - skipping metadata load")
            return

        try:
            entities = await client.get_entity_registry()
            for entity in entities:
                self.entities[entity.get("entity_id", "")] = entity

            devices = await client.get_device_registry()
            for device in devices:
                self.devices[device.get("id", "")] = device

            areas = await client.get_area_registry()
            for area in areas:
                self.areas[area.get("area_id", "")] = area

            logger.info(
                f"Loaded metadata: {len(self.entities)} entities, "
                f"{len(self.devices)} devices, {len(self.areas)} areas"
            )
        except Exception as e:
            logger.warning(f"Failed to load some metadata: {e}")

    def get_entity_info(self, entity_id: str) -> dict[str, Any]:
        """Get combined entity information including device and area."""
        entity = self.entities.get(entity_id, {})
        device_id = entity.get("device_id")
        device = self.devices.get(device_id, {}) if device_id else {}
        area_id = entity.get("area_id") or device.get("area_id")
        area = self.areas.get(area_id, {}) if area_id else {}

        return {
            "entity_id": entity_id,
            "platform": entity.get("platform", ""),
            "device_id": device_id,
            "manufacturer": device.get("manufacturer", ""),
            "model": device.get("model", ""),
            "area_name": area.get("name", ""),
            "disabled_by": entity.get("disabled_by"),
            "hidden_by": entity.get("hidden_by"),
        }
