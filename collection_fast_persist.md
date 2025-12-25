# Collection Fast Persist - Design Specification

## Overview
Create a new class called `collection_fast_persist` that is similar to `dated_fast_persist`, but instead of date-based isolation, uses collection-based organization for storing related items together.

## Shared Utilities
The logger and the StorageKeys enum should be moved to a new shared utility file (`fast_persist_common.py`) that both `dated_fast_persist` and `collection_fast_persist` will import from.

## Key Differences from dated_fast_persist

### Constructor
- **Instead of**: `date` parameter
- **Use**: `collection` parameter (type: string)
- Provides collection-based isolation instead of date-based isolation

### Storage Model
- **Instead of**: `process_name` field
- **Use**: `item_name` and `collection_name` fields
- **Additional field**: `value` (type: `int | float | str | None`) - stored in addition to the data dictionary
  - Stored in typed columns to avoid casting overhead
  - Type can change between updates without warning
  - Only one of the three value columns is populated per record
- **Retains**: All special fields (timestamp, status, status_int, username) as they are used by dated_fast_persist

### Store Function Signature
```python
def store(
    self,
    key: str,
    data: dict[str, Any],
    item_name: str | None = None,
    collection_name: str | None = None,
    value: int | float | str | None = None,
    timestamp: str | dt.datetime | None = None,
    username: str | None = None,
)
```

**Value Handling:**
- Function accepts `int | float | str | None`
- Automatically routes to appropriate column based on type:
  - `int` → `value_int` (BIGINT)
  - `float` → `value_float` (DOUBLE)
  - `str` → `value_string` (VARCHAR)
  - `None` → all value columns NULL

### In-Memory Cache Structure
```python
cache[key][collection_name][item_name] = {
    "data": data,
    "value": value,  # Stored in native Python type (int/float/str/None)
    # ... other special fields
}
```

**Retrieval:**
- Returns `value` field with the correct native type
- Type determined from which column is populated in database
- Type can change between reads without warning

**Collection Loading Behavior**: Whenever a given key and collection are used to either save or read from, that entire collection will be loaded into memory and kept there for fast access throughout the session.

## DuckDB Storage - Two Tables

### Table 1: History Table (storage_history)
- **Purpose**: Record of all updates (similar to dated_fast_persist)
- **Schema**:
  - `key` (VARCHAR)
  - `collection_name` (VARCHAR)
  - `item_name` (VARCHAR)
  - `data` (JSON)
  - `value_int` (BIGINT, nullable) - for integer values
  - `value_float` (DOUBLE, nullable) - for floating point values
  - `value_string` (VARCHAR, nullable) - for string values
  - `timestamp` (TIMESTAMP, nullable)
  - `status` (VARCHAR, nullable)
  - `status_int` (INTEGER, nullable)
  - `username` (VARCHAR, nullable)
  - `updated_at` (TIMESTAMP)
  - `version` (INTEGER)
- **Primary Key**: `(key, collection_name, item_name)`
- **Behavior**: Stores every update, updated in real-time
- **Value Storage**: Only one of `value_int`, `value_float`, or `value_string` is populated per record

### Table 2: Latest Values Table (storage_latest)
- **Purpose**: Contains only the latest value for each (key, collection_name, item_name) combination
- **Schema**: Same as history table (includes value_int, value_float, value_string)
- **Primary Key**: `(key, collection_name, item_name)`
- **Behavior**:
  - Updated at end of day only (on close or manual trigger)
  - Only updates rows that have changed during the session
  - Requires tracking which (key, collection_name, item_name) combinations have been modified
- **Value Storage**: Only one of `value_int`, `value_float`, or `value_string` is populated per record

### Change Tracking
Maintain an in-memory set/list of modified (key, collection_name, item_name) tuples to efficiently update only changed rows in the latest values table.

## WAL Files
- Similar to dated_fast_persist
- Records include: key, collection_name, item_name, data, value (in native type), and all special fields
- Value stored in native JSON type (number/string/null) for type preservation
- Used for crash recovery

## Use Cases
This class is designed for scenarios where you need to track collections of related items:
- **Form elements**: Keeping all input field values in a form
- **Seat assignments**: Managing classroom or venue seating arrangements
- **Computer assignments**: Tracking which users are assigned to different computers
- **Configuration sets**: Grouping related configuration items together
- **Inventory collections**: Organizing items by category or location

## Implementation Steps
1. Create `fast_persist_common.py` with shared StorageKeys enum and logger setup
2. Update `dated_fast_persist.py` to import from common module
3. Create `collection_fast_persist.py` based on dated_fast_persist structure
4. Implement dual-table DuckDB storage
5. Implement collection-based in-memory caching
6. Implement change tracking for efficient latest table updates
7. Create tests demonstrating collection-based usage 
