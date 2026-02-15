"""ionbus_fast_persist - High-performance Python persistence layers.

This package provides two persistence solutions:
- WALDuckDBStorage: Date-based storage isolation
- CollectionFastPersist: Collection-based organization
"""

from __future__ import annotations

from ionbus_fast_persist.dated_fast_persist import (
    WALConfig,
    WALDuckDBStorage,
)
from ionbus_fast_persist.collection_fast_persist import (
    CollectionConfig,
    CollectionFastPersist,
)
from ionbus_fast_persist.fast_persist_common import (
    ExtraSchemaError,
    StorageKeys,
    parse_timestamp,
)

__all__ = [
    # dated_fast_persist
    "WALConfig",
    "WALDuckDBStorage",
    # collection_fast_persist
    "CollectionConfig",
    "CollectionFastPersist",
    # fast_persist_common
    "ExtraSchemaError",
    "StorageKeys",
    "parse_timestamp",
]
