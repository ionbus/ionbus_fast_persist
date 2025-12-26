"""Fast Persist Common - Shared utilities for fast persist modules"""

from __future__ import annotations

import datetime as dt
import json
import logging
import sys

# Handle StrEnum for different Python versions
if sys.version_info >= (3, 11):
    from enum import StrEnum
else:
    from backports.strenum import StrEnum  # type: ignore

# Get logger for this module
logger = logging.getLogger("fast_persist_common")


class StorageKeys(StrEnum):
    """String enumeration for special storage dictionary keys"""

    PROCESS_NAME = "process_name"
    TIMESTAMP = "timestamp"
    STATUS = "status"
    STATUS_INT = "status_int"
    USERNAME = "username"


def setup_logger(name: str) -> logging.Logger:
    """Set up and return a logger with standard configuration.

    Args:
        name: Name for the logger

    Returns:
        Configured logger instance
    """
    logging.basicConfig(level=logging.INFO)
    return logging.getLogger(name)


def serialize_to_json(data: dict | str) -> str:
    """Safely serialize data to JSON, converting datetime/date objects to ISO strings.

    Handles datetime.datetime, datetime.date, and pandas.Timestamp objects.

    Args:
        data: Dictionary or string to serialize

    Returns:
        JSON string representation
    """
    if isinstance(data, str):
        return data

    def datetime_handler(obj):
        """Convert datetime/date objects to ISO format strings."""
        if isinstance(obj, dt.datetime):
            return obj.isoformat()
        if isinstance(obj, dt.date):
            return obj.isoformat()
        # Handle pandas Timestamp if pandas is available
        if hasattr(obj, "isoformat") and type(obj).__name__ == "Timestamp":
            return obj.isoformat()
        raise TypeError(
            f"Object of type {type(obj).__name__} is not JSON serializable"
        )

    return json.dumps(data, default=datetime_handler)


def deserialize_datetime_fields(data: dict) -> dict:
    """Recursively convert ISO datetime strings back to datetime/date objects.

    Detects and converts strings that match ISO 8601 datetime or date format.
    Preserves timezone information when present.

    Args:
        data: Dictionary that may contain ISO datetime strings

    Returns:
        Dictionary with datetime strings converted back to datetime/date objects
    """
    if not isinstance(data, dict):
        return data

    result = {}
    for key, value in data.items():
        if isinstance(value, dict):
            # Recursively process nested dicts
            result[key] = deserialize_datetime_fields(value)
        elif isinstance(value, list):
            # Process lists recursively
            result[key] = [
                deserialize_datetime_fields(item) if isinstance(item, dict) else item
                for item in value
            ]
        elif isinstance(value, str):
            # Try to parse as datetime or date
            parsed = _try_parse_datetime(value)
            result[key] = parsed if parsed is not None else value
        else:
            result[key] = value
    return result


def _try_parse_datetime(s: str) -> dt.datetime | dt.date | None:
    """Try to parse string as datetime or date, return None if not parseable.

    Args:
        s: String to parse

    Returns:
        datetime, date object, or None if not a valid ISO format
    """
    if not s:
        return None

    try:
        # Try datetime first (handles both date and datetime formats)
        parsed = dt.datetime.fromisoformat(s.replace("Z", "+00:00"))

        # Check if it's date-only (no time component)
        if parsed.time() == dt.time(0, 0, 0) and parsed.tzinfo is None:
            # Could be a date - check if original string had time component
            if "T" not in s and " " not in s and len(s) == 10:
                return parsed.date()

        # If naive datetime, assume UTC
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=dt.timezone.utc)

        return parsed
    except (ValueError, AttributeError):
        # Not a valid datetime string
        return None


def parse_timestamp(ts: str | dt.datetime | None) -> dt.datetime | None:
    """Convert timestamp string to datetime object.

    Handles ISO 8601 format with or without timezone.
    Returns None if input is None.

    If timestamp string has no timezone info, assumes UTC.

    Args:
        ts: Timestamp as string, datetime object, or None

    Returns:
        datetime object (timezone-aware) or None
    """
    if ts is None:
        return None
    if isinstance(ts, dt.datetime):
        return ts
    # Parse ISO 8601 string - fromisoformat handles timezone info
    try:
        parsed = dt.datetime.fromisoformat(ts.replace("Z", "+00:00"))
        # If naive (no timezone), assume UTC
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=dt.timezone.utc)
        return parsed
    except (ValueError, AttributeError):
        logger.warning(f"Could not parse timestamp: {ts}")
        return None
