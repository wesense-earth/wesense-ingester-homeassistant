"""Configuration loading and validation for Home Assistant Ingester."""

import os
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import yaml
from dotenv import load_dotenv


@dataclass
class HomeAssistantConfig:
    """Home Assistant connection configuration."""

    url: str
    access_token: str
    mode: str = "websocket"  # "websocket" or "polling"
    polling_interval_seconds: int = 60


@dataclass
class FilterConfig:
    """Entity filtering configuration."""

    mode: str = "denylist"  # "denylist" or "allowlist"
    include_domains: list[str] = field(default_factory=lambda: ["sensor", "binary_sensor"])
    include_device_classes: list[str] = field(default_factory=list)
    exclude_entity_patterns: list[str] = field(default_factory=list)
    exclude_integrations: list[str] = field(default_factory=list)
    exclude_manufacturers: list[str] = field(default_factory=list)
    exclude_entities: list[str] = field(default_factory=list)
    include_entities: list[str] = field(default_factory=list)

    # Compiled regex patterns (populated after init)
    _compiled_patterns: list[re.Pattern] = field(default_factory=list, repr=False)

    def __post_init__(self):
        """Compile regex patterns for efficiency."""
        self._compiled_patterns = []
        for pattern in self.exclude_entity_patterns:
            try:
                self._compiled_patterns.append(re.compile(pattern, re.IGNORECASE))
            except re.error as e:
                raise ValueError(f"Invalid regex pattern '{pattern}': {e}")


# Valid deployment types matching ESP32 sensor array
DEPLOYMENT_TYPES = {"INDOOR", "OUTDOOR", "MIXED"}


@dataclass
class LocationDefault:
    """Default location configuration."""

    latitude: float
    longitude: float
    altitude: float  # meters above sea level
    country_code: str
    subdivision_code: str
    deployment_type: str = "INDOOR"  # INDOOR, OUTDOOR, or MIXED


@dataclass
class LocationOverride:
    """Per-entity location override."""

    latitude: float
    longitude: float
    altitude: Optional[float] = None  # meters above sea level
    country_code: Optional[str] = None
    subdivision_code: Optional[str] = None
    deployment_type: Optional[str] = None  # INDOOR, OUTDOOR, or MIXED
    node_name: Optional[str] = None  # Override node name for this entity


@dataclass
class LocationConfig:
    """Location configuration."""

    default: LocationDefault
    use_ha_zones: bool = True
    overrides: dict[str, LocationOverride] = field(default_factory=dict)


@dataclass
class MQTTConfig:
    """MQTT connection configuration."""

    broker: str
    port: int = 1883
    username: Optional[str] = None
    password: Optional[str] = None
    client_id: str = "ingester-homeassistant-output"
    topic_prefix: str = "wesense/decoded"


@dataclass
class ClickHouseConfig:
    """ClickHouse connection configuration."""

    host: str = "localhost"
    port: int = 8123
    user: str = "wesense"
    password: str = ""
    database: str = "wesense"
    table: str = "sensor_readings"
    batch_size: int = 100
    flush_interval_seconds: int = 10


@dataclass
class LoggingConfig:
    """Logging configuration."""

    level: str = "INFO"
    audit_file: Optional[str] = None


@dataclass
class Config:
    """Complete application configuration."""

    homeassistant: HomeAssistantConfig
    filters: FilterConfig
    location: LocationConfig
    mqtt: MQTTConfig
    clickhouse: ClickHouseConfig
    logging: LoggingConfig
    dry_run: bool = False
    disable_clickhouse: bool = False  # Skip ClickHouse writes, but still publish to MQTT
    node_name: str = ""  # Friendly device name (max 24 chars), like NODE_NAME in ESP32


def expand_env_vars(value: str) -> str:
    """Expand environment variables in string values."""
    if isinstance(value, str) and value.startswith("${") and value.endswith("}"):
        env_var = value[2:-1]
        return os.environ.get(env_var, value)
    return value


def load_config(config_path: Optional[str] = None) -> Config:
    """Load configuration from YAML file and environment variables."""
    # Load .env file if present
    load_dotenv()

    # Determine config file path
    if config_path is None:
        config_path = os.environ.get("CONFIG_PATH", "config/config.yaml")

    config_file = Path(config_path)
    if not config_file.exists():
        # Try alternative locations
        alt_paths = [
            Path("config.yaml"),
            Path("/app/config/config.yaml"),
        ]
        for alt_path in alt_paths:
            if alt_path.exists():
                config_file = alt_path
                break
        else:
            raise FileNotFoundError(
                f"Config file not found at {config_path} or alternative locations"
            )

    with open(config_file) as f:
        raw_config = yaml.safe_load(f) or {}

    # Build configuration with defaults and environment variable overrides
    ha_config = raw_config.get("homeassistant", {}) or {}
    homeassistant = HomeAssistantConfig(
        url=os.environ.get("HA_URL", expand_env_vars(ha_config.get("url", ""))),
        access_token=os.environ.get(
            "HA_ACCESS_TOKEN", expand_env_vars(ha_config.get("access_token", ""))
        ),
        mode=ha_config.get("mode", "websocket"),
        polling_interval_seconds=(ha_config.get("polling") or {}).get("interval_seconds", 60),
    )

    # Filters configuration
    filters_config = raw_config.get("filters", {}) or {}
    filters = FilterConfig(
        mode=filters_config.get("mode", "denylist"),
        include_domains=filters_config.get("include_domains") or ["sensor", "binary_sensor"],
        include_device_classes=filters_config.get("include_device_classes") or [],
        exclude_entity_patterns=filters_config.get("exclude_entity_patterns") or [],
        exclude_integrations=filters_config.get("exclude_integrations") or [],
        exclude_manufacturers=filters_config.get("exclude_manufacturers") or [],
        exclude_entities=filters_config.get("exclude_entities") or [],
        include_entities=filters_config.get("include_entities") or [],
    )

    # Location configuration
    loc_config = raw_config.get("location", {}) or {}
    loc_default = loc_config.get("default", {}) or {}
    loc_overrides = loc_config.get("overrides", {}) or {}
    location = LocationConfig(
        default=LocationDefault(
            latitude=loc_default.get("latitude", 0.0),
            longitude=loc_default.get("longitude", 0.0),
            altitude=loc_default.get("altitude", 0.0),
            country_code=loc_default.get("country_code", ""),
            subdivision_code=loc_default.get("subdivision_code", ""),
            deployment_type=loc_default.get("deployment_type", "INDOOR").upper(),
        ),
        use_ha_zones=loc_config.get("use_ha_zones", True),
        overrides={
            entity_id: LocationOverride(
                latitude=override.get("latitude"),
                longitude=override.get("longitude"),
                altitude=override.get("altitude"),
                country_code=override.get("country_code"),
                subdivision_code=override.get("subdivision_code"),
                deployment_type=override.get("deployment_type", "").upper() or None,
                node_name=override.get("node_name"),
            )
            for entity_id, override in loc_overrides.items()
        },
    )

    # MQTT configuration
    output_config = raw_config.get("output", {}) or {}
    mqtt_config = output_config.get("mqtt", {}) or {}
    mqtt = MQTTConfig(
        broker=os.environ.get("LOCAL_MQTT_BROKER", mqtt_config.get("broker", "localhost")),
        port=int(os.environ.get("LOCAL_MQTT_PORT", mqtt_config.get("port", 1883))),
        username=mqtt_config.get("username"),
        password=mqtt_config.get("password"),
        client_id=mqtt_config.get("client_id", "ingester-homeassistant-output"),
        topic_prefix=mqtt_config.get("topic_prefix", "wesense/decoded"),
    )

    # ClickHouse configuration
    ch_config = raw_config.get("clickhouse", {}) or {}
    clickhouse = ClickHouseConfig(
        host=os.environ.get("CLICKHOUSE_HOST", ch_config.get("host", "localhost")),
        port=int(os.environ.get("CLICKHOUSE_PORT", ch_config.get("port", 8123))),
        user=os.environ.get("CLICKHOUSE_USER", ch_config.get("user", "wesense")),
        password=os.environ.get("CLICKHOUSE_PASSWORD", ch_config.get("password", "")),
        database=os.environ.get("CLICKHOUSE_DATABASE", ch_config.get("database", "wesense")),
        table=os.environ.get("CLICKHOUSE_TABLE", ch_config.get("table", "sensor_readings")),
        batch_size=int(
            os.environ.get("CLICKHOUSE_BATCH_SIZE", ch_config.get("batch_size", 100))
        ),
        flush_interval_seconds=int(
            os.environ.get(
                "CLICKHOUSE_FLUSH_INTERVAL", ch_config.get("flush_interval_seconds", 10)
            )
        ),
    )

    # Logging configuration
    log_config = raw_config.get("logging", {}) or {}
    logging_cfg = LoggingConfig(
        level=os.environ.get("LOG_LEVEL", log_config.get("level", "INFO")).upper(),
        audit_file=log_config.get("audit_file"),
    )

    # Parse disable_clickhouse from env var or config
    disable_clickhouse_env = os.environ.get("DISABLE_CLICKHOUSE", "").lower()
    disable_clickhouse = disable_clickhouse_env in ("true", "1", "yes") or raw_config.get(
        "disable_clickhouse", False
    )

    # Node name - friendly device name (like NODE_NAME in ESP32)
    node_name = os.environ.get("NODE_NAME", raw_config.get("node_name", ""))

    return Config(
        homeassistant=homeassistant,
        filters=filters,
        location=location,
        mqtt=mqtt,
        clickhouse=clickhouse,
        logging=logging_cfg,
        dry_run=raw_config.get("dry_run", False),
        disable_clickhouse=disable_clickhouse,
        node_name=node_name,
    )


def validate_config(config: Config) -> list[str]:
    """Validate configuration and return list of warnings."""
    warnings = []

    # Check Home Assistant configuration
    if not config.homeassistant.url:
        raise ValueError("Home Assistant URL is required")
    if not config.homeassistant.access_token:
        raise ValueError("Home Assistant access token is required")

    # Check location defaults
    if config.location.default.latitude == 0.0 and config.location.default.longitude == 0.0:
        warnings.append(
            "Default location is (0, 0) - sensors without explicit location will be REJECTED. "
            "Configure location.default in config.yaml to set a valid default location."
        )

    # Validate deployment type
    if config.location.default.deployment_type not in DEPLOYMENT_TYPES:
        raise ValueError(
            f"Invalid deployment_type '{config.location.default.deployment_type}'. "
            f"Must be one of: {', '.join(DEPLOYMENT_TYPES)}"
        )

    # Check filters
    if config.filters.mode == "allowlist" and not config.filters.include_entities:
        warnings.append("Filter mode is 'allowlist' but no entities are included - nothing will be ingested")

    if not config.filters.exclude_entity_patterns:
        warnings.append(
            "No exclude_entity_patterns configured - WeSense devices may create loops! "
            "Recommended: add '.*_[0-9a-f]{12}_.*' to exclude ESP32 devices"
        )

    return warnings
