"""Reading type mappings between Home Assistant and WeSense."""

# Map Home Assistant device_class to WeSense reading_type
HA_TO_WESENSE_READING_TYPE = {
    # Environmental
    "temperature": "temperature",
    "humidity": "humidity",
    "pressure": "pressure",
    "atmospheric_pressure": "pressure",
    # Air quality
    "carbon_dioxide": "co2",
    "carbon_monoxide": "co",
    "pm1": "pm1_0",
    "pm10": "pm10",
    "pm25": "pm2_5",
    "volatile_organic_compounds": "voc",
    "nitrogen_dioxide": "no2",
    "ozone": "o3",
    "aqi": "aqi",
    "sulphur_dioxide": "so2",
    # Light
    "illuminance": "light_level",
    # Power
    "battery": "battery_level",
    "voltage": "voltage",
    "current": "current",
    "power": "power",
    "energy": "energy",
    # Sound
    "sound_pressure": "sound_level",
    # Other
    "signal_strength": "rssi",
    "distance": "distance",
    "speed": "speed",
    "wind_speed": "wind_speed",
    "precipitation": "precipitation",
    "precipitation_intensity": "precipitation_intensity",
}

# WeSense standard units for each reading type
READING_UNITS = {
    "temperature": "°C",
    "humidity": "%",
    "pressure": "hPa",
    "co2": "ppm",
    "co": "ppm",
    "pm1_0": "µg/m³",
    "pm2_5": "µg/m³",
    "pm10": "µg/m³",
    "voc": "index",
    "no2": "ppb",
    "o3": "ppb",
    "so2": "ppb",
    "aqi": "index",
    "light_level": "lux",
    "battery_level": "%",
    "voltage": "V",
    "current": "A",
    "power": "W",
    "energy": "kWh",
    "sound_level": "dB",
    "rssi": "dBm",
    "distance": "m",
    "speed": "m/s",
    "wind_speed": "m/s",
    "precipitation": "mm",
    "precipitation_intensity": "mm/h",
}

# Home Assistant unit conversions to WeSense standard units
UNIT_CONVERSIONS = {
    # Temperature
    ("°F", "°C"): lambda x: (x - 32) * 5 / 9,
    ("K", "°C"): lambda x: x - 273.15,
    # Pressure
    ("mbar", "hPa"): lambda x: x,  # 1:1
    ("inHg", "hPa"): lambda x: x * 33.8639,
    ("mmHg", "hPa"): lambda x: x * 1.33322,
    ("psi", "hPa"): lambda x: x * 68.9476,
    ("Pa", "hPa"): lambda x: x / 100,
    ("kPa", "hPa"): lambda x: x * 10,
    # Energy
    ("Wh", "kWh"): lambda x: x / 1000,
    ("MWh", "kWh"): lambda x: x * 1000,
    # Distance
    ("cm", "m"): lambda x: x / 100,
    ("mm", "m"): lambda x: x / 1000,
    ("km", "m"): lambda x: x * 1000,
    ("ft", "m"): lambda x: x * 0.3048,
    ("in", "m"): lambda x: x * 0.0254,
    ("mi", "m"): lambda x: x * 1609.34,
    # Speed
    ("km/h", "m/s"): lambda x: x / 3.6,
    ("mph", "m/s"): lambda x: x * 0.44704,
    ("kn", "m/s"): lambda x: x * 0.514444,
    ("ft/s", "m/s"): lambda x: x * 0.3048,
}


def convert_unit(value: float, from_unit: str, to_unit: str) -> float:
    """Convert value from one unit to another."""
    if from_unit == to_unit:
        return value

    converter = UNIT_CONVERSIONS.get((from_unit, to_unit))
    if converter:
        return converter(value)

    # No conversion available, return as-is
    return value


def get_wesense_reading_type(ha_device_class: str) -> str | None:
    """Map Home Assistant device class to WeSense reading type."""
    if not ha_device_class:
        return None
    return HA_TO_WESENSE_READING_TYPE.get(ha_device_class.lower())


def get_standard_unit(reading_type: str) -> str:
    """Get the standard WeSense unit for a reading type."""
    return READING_UNITS.get(reading_type, "")
