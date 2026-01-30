"""Geocoding utilities for reverse geocoding coordinates.

Uses wesense-ingester-core's ReverseGeocoder which has the full
ISO 3166 mapper with comprehensive subdivision coverage.
"""

import logging
from typing import Optional

from wesense_ingester import ReverseGeocoder

logger = logging.getLogger(__name__)

# Singleton geocoder instance (lazy init)
_geocoder: Optional[ReverseGeocoder] = None


def _get_geocoder() -> ReverseGeocoder:
    """Get or create the singleton ReverseGeocoder."""
    global _geocoder
    if _geocoder is None:
        _geocoder = ReverseGeocoder()
    return _geocoder


def reverse_geocode(latitude: float, longitude: float) -> Optional[dict]:
    """
    Reverse geocode coordinates to country and subdivision.

    Returns dict with keys: country_code, subdivision_code
    Returns None if geocoding fails.
    """
    try:
        geocoder = _get_geocoder()
        result = geocoder.reverse_geocode(latitude, longitude)
        if not result:
            return None

        return {
            "country_code": result["geo_country"],
            "subdivision_code": result["geo_subdivision"],
        }
    except Exception as e:
        logger.warning("Geocoding failed for (%s, %s): %s", latitude, longitude, e)
        return None


def get_location_info(
    latitude: float,
    longitude: float,
    default_country_code: str = "",
    default_subdivision_code: str = "",
) -> dict:
    """
    Get location info with fallback to defaults.

    Returns dict with country_code and subdivision_code.
    """
    result = reverse_geocode(latitude, longitude)
    if result:
        return {
            "country_code": result["country_code"],
            "subdivision_code": result["subdivision_code"],
        }
    return {
        "country_code": default_country_code,
        "subdivision_code": default_subdivision_code,
    }
