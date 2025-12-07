- For storage data, there are several keys that may be there. I want these keys stored as separate fields in the duckDB database.
    - for the WAL files, nothing will change
    - Special fields
        - process_name: str
        - timestamp: datetime
            - will come in as string, should convert
            - may come in with timezone, may not
        - status: str
        - status_int: int
    - The columns above should NOT be removed from the data field when they are stored in their own columns.
        - These columns are not required so if they are missing, they should have NULL values.
        - If process_name is missing, use None as the default value
    - When stored in memory, they should be stored nested by key, then by process_name (or None).
        - Structure: cache[key][process_name] = data

## DuckDB Schema Changes
- Primary key: Composite key of (key, process_name)
- New columns to add:
    - process_name: VARCHAR (nullable, defaults to None)
    - timestamp: TIMESTAMP (nullable)
    - status: VARCHAR (nullable)
    - status_int: INTEGER (nullable)
- Keep existing columns:
    - key: VARCHAR (part of composite primary key)
    - data: JSON (keeps ALL data including special fields)
    - updated_at: TIMESTAMP
    - version: INTEGER (tracks version per (key, process_name) pair)

## API Changes

### store() method
- Signature: `store(key: str, data: Dict[str, Any], process_name: str | None = None, timestamp: str | datetime | None = None)`
- Behavior:
    - If process_name parameter is provided, use it
    - Otherwise, try to extract from data["process_name"]
    - If still not found, use None
    - Same logic for timestamp parameter vs data["timestamp"]
    - Extract status and status_int from data dict if present

### get() methods - SPLIT INTO TWO
- `get_key(key: str) -> Optional[Dict[str, Dict[str, Any]]]`
    - Returns dict of {process_name: data} for all process_names under this key
    - Returns None if key doesn't exist

- `get_key_process(key: str, process_name: str | None = None) -> Optional[Dict[str, Any]]`
    - Returns single data dict for specific (key, process_name) combination
    - Returns None if not found

### version tracking
- Version increments per (key, process_name) pair, not just per key

## StorageKeys StrEnum

Add a StrEnum class for type-safe dictionary key access:

```python
from enum import StrEnum  # or from backports.enum import StrEnum for older Python

class StorageKeys(StrEnum):
    PROCESS_NAME = "process_name"
    TIMESTAMP = "timestamp"
    STATUS = "status"
    STATUS_INT = "status_int"
```

**Usage:**
- Provides type-safe constants for dictionary keys
- Works seamlessly with FastAPI Pydantic models
- Can use either `StorageKeys.PROCESS_NAME` or plain string `"process_name"`

## FastAPI Integration

This class is designed for FastAPI servers where data comes as dictionaries:

- FastAPI endpoints receive Pydantic models
- Convert to dict with `model.model_dump()`
- Pass directly to `store()` - all special fields auto-extracted
- Timestamp strings automatically converted to datetime objects
- Supports ISO 8601 format with or without timezone

