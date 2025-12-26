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
    """Safely serialize data to JSON, converting datetime objects to ISO strings.

    Args:
        data: Dictionary or string to serialize

    Returns:
        JSON string representation
    """
    if isinstance(data, str):
        return data

    def datetime_handler(obj):
        """Convert datetime objects to ISO format strings."""
        if isinstance(obj, dt.datetime):
            return obj.isoformat()
        raise TypeError(
            f"Object of type {type(obj).__name__} is not JSON serializable"
        )

    return json.dumps(data, default=datetime_handler)


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
