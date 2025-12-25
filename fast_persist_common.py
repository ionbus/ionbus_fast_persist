"""Fast Persist Common - Shared utilities for fast persist modules"""

from __future__ import annotations

import logging
import sys

# Handle StrEnum for different Python versions
if sys.version_info >= (3, 11):
    from enum import StrEnum
else:
    from backports.strenum import StrEnum  # type: ignore


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
