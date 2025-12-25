# Collection Fast Persist - Design Specification

## Overview
Create a new class called `collection_fast_persist` that is similar to `dated_fast_persist`, but instead of date-based isolation, uses collection-based organization for storing related items together.

## Shared Utilities
The logger and the StorageKeys enum should be moved to a new shared utility file (`fast_persist_common.py`) that both `dated_fast_persist` and `collection_fast_persist` will import from.

## Key Differences from dated_fast_persist

### Constructor
```python
def __init__(
    self,
    date: dt.date | dt.datetime | str,
    base_dir: str = "./collection_storage",
    config: CollectionConfig | None = None,
)
```

**Parameters:**
- `date`: Date for backup/WAL organization (NOT for data isolation)
  - Type: `dt.date | dt.datetime | str`
  - Used to organize WAL files and backups into date directories
  - Single instance per date (enforced via file locking)
- `base_dir`: Base directory for storage files
- `config`: Configuration object with retention settings

**Date Usage:**
- Date is used ONLY for organizing backups and WAL files
- Data organization is purely by collection/item_name (no date filtering)
- All dates' data stored in same DuckDB files
- Unlike dated_fast_persist, this does NOT provide date-based data isolation

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

## File Organization and Backup Strategy

### Directory Structure
```
./base_dir/
  ├── storage_history.duckdb          # Active history DB (all dates)
  ├── storage_latest.duckdb           # Active latest values DB
  ├── .lock                           # Instance lock file
  ├── 2025-12-20/                     # Old day (auto-deleted)
  │   ├── wal_000001.jsonl
  │   ├── storage_history.duckdb.backup
  │   └── storage_latest.duckdb.backup
  ├── 2025-12-23/                     # Yesterday
  │   ├── wal_000001.jsonl
  │   ├── storage_history.duckdb.backup
  │   └── storage_latest.duckdb.backup
  └── 2025-12-24/                     # Today (active)
      ├── wal_000001.jsonl
      └── wal_000002.jsonl
```

### WAL Files
- Located in `base_dir/{date}/wal_*.jsonl`
- Records include: key, collection_name, item_name, data, value (in native type), and all special fields
- Value stored in native JSON type (number/string/null) for type preservation
- Used for crash recovery

### DuckDB Files
**Active Files** (in `base_dir/`):
- `storage_history.duckdb` - All updates across all dates
- `storage_latest.duckdb` - Latest values across all dates

**Backup Files** (in `base_dir/{date}/`):
- `storage_history.duckdb.backup` - Copy of history DB at end of day
- `storage_latest.duckdb.backup` - Copy of latest DB at end of day

### Backup Behavior
**On Shutdown (`close()`):**
1. Flush all pending writes to DuckDB
2. Update latest values table for changed records only
3. Copy `storage_history.duckdb` → `base_dir/{date}/storage_history.duckdb.backup`
4. Copy `storage_latest.duckdb` → `base_dir/{date}/storage_latest.duckdb.backup`
5. Clean up date directories older than `retain_days`

### Startup Behavior
**If date directory exists** (indicates restart/recovery scenario):
1. Check for WAL files in `base_dir/{date}/`
2. If WAL files exist: Replay them into history DB (crash recovery)
3. If history DB won't open: Throw exception with reconstruction instructions
4. If latest DB won't open: Throw exception with reconstruction instructions
5. Load collections into memory cache as accessed

**If date directory doesn't exist** (new day):
1. Create `base_dir/{date}/` directory
2. Initialize normally

### Retention and Cleanup
- **Configuration**: `retain_days` parameter (default: 5)
- **Cleanup Trigger**: Automatic on `close()`
- **What Gets Deleted**: Entire date directories older than `retain_days`
- **What's Kept**: Current date + previous `retain_days - 1` days

Example with `retain_days=5`:
- Today: 2025-12-24
- Keeps: 2025-12-24, 2025-12-23, 2025-12-22, 2025-12-21, 2025-12-20
- Deletes: 2025-12-19 and older

## Database Reconstruction

### Manual Reconstruction Functions

These functions must be called explicitly by the user when database corruption is detected.

#### `rebuild_history_from_wal(date: dt.date | dt.datetime | str) -> int`
Reconstructs the history database from WAL files for a specific date.

**Parameters:**
- `date`: The date directory to read WAL files from

**Returns:** Number of records recovered

**Behavior:**
1. Reads all WAL files from `base_dir/{date}/wal_*.jsonl`
2. Replays entries into `storage_history.duckdb`
3. Updates in-memory cache
4. Does NOT touch `storage_latest.duckdb`

**When to Use:**
- History DB file is corrupted and won't open
- Exception message indicates history DB reconstruction needed
- Want to recover data from specific date's WAL files

#### `rebuild_latest_from_history() -> int`
Reconstructs the latest values database from the history database.

**Parameters:** None

**Returns:** Number of latest records created

**Behavior:**
1. Reads all data from `storage_history.duckdb`
2. Groups by `(key, collection_name, item_name)`
3. Takes the most recent version for each group
4. Writes to `storage_latest.duckdb`
5. Updates in-memory cache

**When to Use:**
- Latest DB file is corrupted and won't open
- Exception message indicates latest DB reconstruction needed
- Latest DB is out of sync with history

### Error Handling

**If DuckDB file won't open on startup:**
```python
Exception: CollectionFastPersistError
Message: "Failed to open storage_history.duckdb.
         Database may be corrupted. To recover:
         1. Delete the corrupted file
         2. Call rebuild_history_from_wal(date) for each date
            that needs recovery
         3. Call rebuild_latest_from_history() to rebuild latest values"
```

**Single Instance Enforcement:**
```python
Exception: CollectionFastPersistError
Message: "Another instance is already running for date {date}.
         Lock file exists at {base_dir}/.lock"
```

## Use Cases
This class is designed for scenarios where you need to track collections of related items:
- **Form elements**: Keeping all input field values in a form
- **Seat assignments**: Managing classroom or venue seating arrangements
- **Computer assignments**: Tracking which users are assigned to different computers
- **Configuration sets**: Grouping related configuration items together
- **Inventory collections**: Organizing items by category or location

## Configuration

### CollectionConfig
```python
@dataclass
class CollectionConfig:
    base_dir: str = "./collection_storage"
    max_wal_size: int = 10 * 1024 * 1024  # 10MB
    batch_size: int = 1000
    duckdb_flush_interval_seconds: int = 30
    retain_days: int = 5  # Keep 5 days of backups
```

**Configuration Options:**
| Parameter | Default | Description |
|-----------|---------|-------------|
| `base_dir` | `"./collection_storage"` | Base directory for all files |
| `max_wal_size` | `10485760` (10MB) | Max WAL file size before rotation |
| `batch_size` | `1000` | Records before batch flush |
| `duckdb_flush_interval_seconds` | `30` | Force flush interval |
| `retain_days` | `5` | Number of days to retain backups |

## Implementation Steps
1. Create `fast_persist_common.py` with shared StorageKeys enum and logger setup
2. Update `dated_fast_persist.py` to import from common module
3. Create `collection_fast_persist.py` based on dated_fast_persist structure
4. Implement dual-table DuckDB storage (history and latest)
5. Implement collection-based in-memory caching with three-level nesting
6. Implement change tracking for efficient latest table updates
7. Implement date-based WAL organization and backup strategy
8. Implement file locking for single-instance enforcement
9. Implement automatic cleanup of old date directories
10. Implement manual reconstruction functions
11. Implement typed value column handling (value_int, value_float, value_string)
12. Create tests demonstrating collection-based usage with typed values 
