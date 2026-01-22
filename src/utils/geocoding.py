"""Geocoding utilities for reverse geocoding coordinates."""

import logging
from functools import lru_cache
from typing import Optional

logger = logging.getLogger(__name__)

# Lazy load reverse_geocoder to avoid import errors if not installed
_rg = None


def _get_reverse_geocoder():
    """Lazily load reverse_geocoder module."""
    global _rg
    if _rg is None:
        try:
            import reverse_geocoder as rg

            _rg = rg
        except ImportError:
            logger.warning("reverse_geocoder not installed - geocoding will use defaults")
            _rg = False
    return _rg


# ISO 3166-2 subdivision code mapping (GeoNames admin1 code to ISO subdivision)
# This is a subset - expand as needed
ADMIN1_TO_SUBDIVISION = {
    # New Zealand
    ("NZ", "E7"): "auk",  # Auckland
    ("NZ", "F1"): "wgn",  # Wellington
    ("NZ", "F3"): "can",  # Canterbury
    ("NZ", "G2"): "ota",  # Otago
    # USA (state FIPS codes)
    ("US", "CA"): "ca",
    ("US", "NY"): "ny",
    ("US", "TX"): "tx",
    ("US", "WA"): "wa",
    # Australia
    ("AU", "02"): "nsw",  # New South Wales
    ("AU", "07"): "vic",  # Victoria
    ("AU", "04"): "qld",  # Queensland
    # UK
    ("GB", "ENG"): "eng",
    ("GB", "SCT"): "sct",
    ("GB", "WLS"): "wls",
}


@lru_cache(maxsize=10000)
def reverse_geocode(latitude: float, longitude: float) -> Optional[dict]:
    """
    Reverse geocode coordinates to country and subdivision.

    Returns dict with keys: country_code, subdivision_code, country_name
    Returns None if geocoding fails.
    """
    rg = _get_reverse_geocoder()
    if not rg:
        return None

    try:
        results = rg.search([(latitude, longitude)], mode=1)
        if not results:
            return None

        result = results[0]
        country_code = result.get("cc", "").lower()
        admin1 = result.get("admin1", "")

        # Map admin1 to ISO 3166-2 subdivision code
        subdivision_code = ADMIN1_TO_SUBDIVISION.get(
            (result.get("cc"), admin1), admin1.lower()[:3]
        )

        return {
            "country_code": country_code,
            "subdivision_code": subdivision_code,
            "country_name": result.get("name", ""),
        }
    except Exception as e:
        logger.warning(f"Geocoding failed for ({latitude}, {longitude}): {e}")
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
