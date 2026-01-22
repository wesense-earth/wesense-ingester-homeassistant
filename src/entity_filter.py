"""Entity filtering to prevent feedback loops and select relevant sensors."""

import logging
import re
from typing import Any, Optional

from .config import FilterConfig
from .ha_client import EntityMetadata

logger = logging.getLogger(__name__)


class EntityFilter:
    """
    Filter Home Assistant entities based on configuration.

    This is critical for preventing feedback loops where WeSense devices
    publish to Home Assistant and then get re-imported by this ingester.
    """

    def __init__(self, config: FilterConfig, metadata: Optional[EntityMetadata] = None):
        """
        Initialize the entity filter.

        Args:
            config: Filter configuration
            metadata: Optional entity metadata for advanced filtering
        """
        self.config = config
        self.metadata = metadata
        self._cached_decisions: dict[str, bool] = {}

        # Compile regex patterns
        self._exclude_patterns: list[re.Pattern] = []
        for pattern in config.exclude_entity_patterns:
            try:
                self._exclude_patterns.append(re.compile(pattern, re.IGNORECASE))
            except re.error as e:
                logger.error(f"Invalid regex pattern '{pattern}': {e}")

        # Convert lists to sets for faster lookups
        self._include_domains = set(config.include_domains)
        self._include_device_classes = set(dc.lower() for dc in config.include_device_classes)
        self._exclude_integrations = set(i.lower() for i in config.exclude_integrations)
        self._exclude_manufacturers = set(m.lower() for m in config.exclude_manufacturers)
        self._exclude_entities = set(config.exclude_entities)
        self._include_entities = set(config.include_entities)

    def should_ingest(self, entity_id: str, state: dict[str, Any]) -> bool:
        """
        Determine if an entity should be ingested.

        Args:
            entity_id: The entity ID (e.g., "sensor.living_room_temperature")
            state: The entity state dict from Home Assistant

        Returns:
            True if the entity should be ingested, False otherwise
        """
        # Check cache first
        if entity_id in self._cached_decisions:
            return self._cached_decisions[entity_id]

        decision = self._evaluate_filters(entity_id, state)
        self._cached_decisions[entity_id] = decision

        if not decision:
            logger.debug(f"Filtered out: {entity_id}")

        return decision

    def _evaluate_filters(self, entity_id: str, state: dict[str, Any]) -> bool:
        """Evaluate all filters for an entity."""
        attributes = state.get("attributes", {})

        # =====================================================================
        # ALLOWLIST MODE: Only explicitly included entities pass
        # =====================================================================
        if self.config.mode == "allowlist":
            return self._matches_include_list(entity_id)

        # =====================================================================
        # DENYLIST MODE: Check exclusions first, then inclusions
        # =====================================================================

        # 0. Check explicit entity inclusion (bypasses domain/device_class filters, but still validates state)
        if entity_id in self._include_entities:
            state_value = state.get("state")
            if state_value in ("unknown", "unavailable", None):
                logger.debug(f"Explicitly included but invalid state '{state_value}': {entity_id}")
                return False
            try:
                float(state_value)
                logger.debug(f"Included by explicit entity list: {entity_id}")
                return True
            except (ValueError, TypeError):
                logger.debug(f"Explicitly included but non-numeric state '{state_value}': {entity_id}")
                return False

        # 1. Check explicit entity exclusion
        if entity_id in self._exclude_entities:
            logger.debug(f"Excluded by explicit entity list: {entity_id}")
            return False

        # 2. Check regex pattern exclusions (CRITICAL for loop prevention)
        for pattern in self._exclude_patterns:
            if pattern.search(entity_id):
                logger.debug(f"Excluded by pattern '{pattern.pattern}': {entity_id}")
                return False

        # 3. Check manufacturer exclusion (if metadata available)
        if self.metadata:
            entity_info = self.metadata.get_entity_info(entity_id)
            manufacturer = entity_info.get("manufacturer", "").lower()
            if manufacturer and manufacturer in self._exclude_manufacturers:
                logger.debug(f"Excluded by manufacturer '{manufacturer}': {entity_id}")
                return False

            # 4. Check integration exclusion
            platform = entity_info.get("platform", "").lower()
            if platform and platform in self._exclude_integrations:
                logger.debug(f"Excluded by integration '{platform}': {entity_id}")
                return False

        # 5. Check domain inclusion
        domain = entity_id.split(".")[0] if "." in entity_id else ""
        if self._include_domains and domain not in self._include_domains:
            logger.debug(f"Excluded by domain '{domain}' not in include list: {entity_id}")
            return False

        # 6. Check device class inclusion
        device_class = attributes.get("device_class", "").lower()
        if self._include_device_classes:
            if not device_class or device_class not in self._include_device_classes:
                logger.debug(
                    f"Excluded by device_class '{device_class}' not in include list: {entity_id}"
                )
                return False

        # 7. Check state validity
        state_value = state.get("state")
        if state_value in ("unknown", "unavailable", None):
            logger.debug(f"Excluded by invalid state '{state_value}': {entity_id}")
            return False

        # 8. Check if state is numeric (for sensor values)
        try:
            float(state_value)
        except (ValueError, TypeError):
            # Non-numeric states are typically not sensor readings
            logger.debug(f"Excluded by non-numeric state '{state_value}': {entity_id}")
            return False

        return True

    def _matches_include_list(self, entity_id: str) -> bool:
        """Check if entity matches the allowlist (supports wildcards)."""
        if entity_id in self._include_entities:
            return True

        # Check wildcard patterns (e.g., "sensor.energy_*")
        for pattern in self._include_entities:
            if "*" in pattern:
                regex = pattern.replace(".", r"\.").replace("*", ".*")
                if re.match(regex, entity_id, re.IGNORECASE):
                    return True

        return False

    def invalidate_cache(self, entity_id: Optional[str] = None):
        """
        Invalidate cached filter decisions.

        Args:
            entity_id: Specific entity to invalidate, or None for all
        """
        if entity_id:
            self._cached_decisions.pop(entity_id, None)
        else:
            self._cached_decisions.clear()

    def get_filter_stats(self) -> dict[str, int]:
        """Get statistics about filtering decisions."""
        included = sum(1 for v in self._cached_decisions.values() if v)
        excluded = sum(1 for v in self._cached_decisions.values() if not v)
        return {
            "included": included,
            "excluded": excluded,
            "total_evaluated": len(self._cached_decisions),
        }

    def find_suspicious_entities(self, states: list[dict]) -> list[str]:
        """
        Find entities that might cause feedback loops.

        This is a safety check to warn about potential issues.
        """
        suspicious = []
        mac_pattern = re.compile(r"[0-9a-f]{12}", re.IGNORECASE)

        for state in states:
            entity_id = state.get("entity_id", "")

            # Check for MAC address patterns (common in WeSense/ESP devices)
            if mac_pattern.search(entity_id):
                if self.should_ingest(entity_id, state):
                    suspicious.append(entity_id)
                    continue

            # Check for known WeSense-related names
            lower_id = entity_id.lower()
            if any(
                kw in lower_id
                for kw in ["wesense", "meshtastic", "esp32", "esp8266", "esphome"]
            ):
                if self.should_ingest(entity_id, state):
                    suspicious.append(entity_id)

        return suspicious
