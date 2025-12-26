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


def normalize_datetime_fields(data: dict) -> dict:
    """Convert all datetime/date/string fields to timezone-aware datetime objects.

    Recursively processes nested dicts and lists. Converts:
    - ISO datetime strings → timezone-aware datetime
    - datetime.date → timezone-aware datetime (midnight UTC)
    - pandas.Timestamp → timezone-aware datetime
    - naive datetime → timezone-aware datetime (assume UTC)

    Args:
        data: Dictionary that may contain datetime-like values

    Returns:
        Dictionary with all datetime-like values converted to timezone-aware datetime
    """
    if not isinstance(data, dict):
        return data

    result = {}
    for key, value in data.items():
        if isinstance(value, dict):
            # Recursively process nested dicts
            result[key] = normalize_datetime_fields(value)
        elif isinstance(value, list):
            # Process lists recursively
            result[key] = [
                normalize_datetime_fields(item) if isinstance(item, dict) else _normalize_single_value(item)
                for item in value
            ]
        else:
            result[key] = _normalize_single_value(value)
    return result


def _normalize_single_value(value):
    """Normalize a single value to timezone-aware datetime if applicable."""
    # Handle datetime objects
    if isinstance(value, dt.datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=dt.timezone.utc)
        return value

    # Handle date objects - convert to datetime at midnight UTC
    if isinstance(value, dt.date):
        return dt.datetime.combine(value, dt.time(0, 0, 0), tzinfo=dt.timezone.utc)

    # Handle pandas Timestamp
    if hasattr(value, "to_pydatetime") and type(value).__name__ == "Timestamp":
        pydatetime = value.to_pydatetime()
        if pydatetime.tzinfo is None:
            return pydatetime.replace(tzinfo=dt.timezone.utc)
        return pydatetime

    # Try parsing ISO strings
    if isinstance(value, str):
        try:
            parsed = dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
            # If naive, assume UTC
            if parsed.tzinfo is None:
                # Check if it's a date-only string (no time component)
                if "T" not in value and " " not in value and len(value) == 10:
                    # Date-only string - set to midnight UTC
                    return dt.datetime.combine(parsed.date(), dt.time(0, 0, 0), tzinfo=dt.timezone.utc)
                # Naive datetime string - assume UTC
                return parsed.replace(tzinfo=dt.timezone.utc)
            return parsed
        except (ValueError, AttributeError):
            # Not a datetime string, return as-is
            pass

    return value


def parse_timestamp(
    ts: str | dt.datetime | dt.date | None,
) -> dt.datetime | None:
    """Convert timestamp string/date/datetime to datetime object.

    Handles ISO 8601 format with or without timezone.
    Returns None if input is None.

    If timestamp string has no timezone info, assumes UTC.

    Args:
        ts: Timestamp as string, date, datetime object, or None

    Returns:
        datetime object (timezone-aware) or None
    """
    if ts is None:
        return None
    if isinstance(ts, dt.datetime):
        # Make sure it's timezone-aware
        if ts.tzinfo is None:
            return ts.replace(tzinfo=dt.timezone.utc)
        return ts
    if isinstance(ts, dt.date):
        # Convert date to datetime at midnight UTC
        return dt.datetime.combine(ts, dt.time(0, 0, 0), tzinfo=dt.timezone.utc)
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
