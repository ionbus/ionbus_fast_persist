"""Fast Persist Common - Shared utilities for fast persist modules"""

from __future__ import annotations

import datetime as dt
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


def parse_timestamp(ts: str | dt.datetime | None) -> dt.datetime | None:
    """Convert timestamp string to datetime object.

    Handles ISO 8601 format with or without timezone.
    Returns None if input is None.

    Args:
        ts: Timestamp as string, datetime object, or None

    Returns:
        datetime object or None
    """
    if ts is None:
        return None
    if isinstance(ts, dt.datetime):
        return ts
    # Parse ISO 8601 string - fromisoformat handles timezone info
    try:
        return dt.datetime.fromisoformat(ts.replace("Z", "+00:00"))
    except (ValueError, AttributeError):
        logger.warning(f"Could not parse timestamp: {ts}")
        return None
