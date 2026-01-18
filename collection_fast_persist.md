# collection_fast_persist

A high-performance Python persistence layer with collection-based organization, combining Write-Ahead Logs (WAL) with dual-table DuckDB storage for fast asynchronous writes, efficient latest-value queries, and reliable historical tracking.

<!-- TOC start (generated with https://github.com/derlin/bitdowntoc) -->

- [Overview](#overview)
- [Features](#features)
- [Installation](#installation)
   * [Dependencies](#dependencies)
- [Usage](#usage)
   * [Basic Example](#basic-example)
   * [Form Data Example](#form-data-example)
   * [Classroom Seating Example](#classroom-seating-example)
   * [Custom Configuration](#custom-configuration)
- [Configuration Options](#configuration-options)
- [Collection Organization](#collection-organization)
   * [Three-Level Hierarchy](#three-level-hierarchy)
   * [Lazy Loading](#lazy-loading)
- [Typed Value Support](#typed-value-support)
- [Dual-Table Architecture](#dual-table-architecture)
   * [History Table (`storage_history.duckdb`)](#history-table-storage_historyduckdb)
   * [Latest Table (`storage_latest.duckdb`)](#latest-table-storage_latestduckdb)
- [Database Health Checks](#database-health-checks)
   * [Automatic Checks (on startup)](#automatic-checks-on-startup)
   * [Manual Checks](#manual-checks)
- [File Organization](#file-organization)
   * [Directory Structure](#directory-structure)
   * [Date Usage](#date-usage)
- [Backup and Recovery](#backup-and-recovery)
   * [Automatic Backups (on close)](#automatic-backups-on-close)
   * [Crash Recovery (automatic)](#crash-recovery-automatic)
   * [Manual Reconstruction](#manual-reconstruction)
- [API Reference](#api-reference)
   * [CollectionFastPersist](#collectionfastpersist)
      + [`__init__(date, base_dir, config)`](#__init__date-base_dir-config)
      + [`store(key, data, item_name, collection_name, value, timestamp, username)`](#storekey-data-item_name-collection_name-value-timestamp-username)
      + [`get_key(key, collection_name=None)`](#get_keykey-collection_namenone)
      + [`get_item(key, collection_name, item_name)`](#get_itemkey-collection_name-item_name)
      + [`check_database_health(db_path, table_name, conn=None)`](#check_database_healthdb_path-table_name-connnone)
      + [`rebuild_history_from_wal(date)`](#rebuild_history_from_waldate)
      + [`rebuild_latest_from_history()`](#rebuild_latest_from_history)
      + [`flush_data_to_duckdb()`](#flush_data_to_duckdb)
      + [`get_stats()`](#get_stats)
      + [`close()`](#close)
- [Architecture](#architecture)
   * [Write Path](#write-path)
   * [Read Path](#read-path)
   * [Recovery Process](#recovery-process)
   * [Crash Safety](#crash-safety)
- [Thread Safety](#thread-safety)
- [Performance Characteristics](#performance-characteristics)
- [Use Cases](#use-cases)
- [Error Handling](#error-handling)
   * [Database Corruption](#database-corruption)
   * [Single Instance Violation](#single-instance-violation)
- [Testing](#testing)
- [License](#license)
- [Contributing](#contributing)

<!-- TOC end -->



## Overview

`collection_fast_persist` provides a hybrid storage system designed for applications that need to organize related items into collections:

- **Fast async writes** via Write-Ahead Logs (WAL)
- **Dual-table storage** with separate history and latest-values databases
- **Collection-based organization** using key/collection/item hierarchy
- **Typed value support** with automatic routing to int/float/string columns
- **Automatic crash recovery** from WAL files
- **In-memory caching** with lazy collection loading
- **Database health checks** for corruption detection
- **Automatic backups** with configurable retention
- **Extra schema support** for custom typed columns in DuckDB

## Features

- **Collection organization**: Three-level hierarchy (key → collection → item) for organizing related data
- **Typed values**: Separate columns for int, float, and string values to avoid casting overhead
- **Dual-table architecture**:
  - History table: All updates with full version history
  - Latest table: Only current values for fast queries
- **Change tracking**: Efficient updates to latest table using modification tracking
- **WAL-based writing**: Append-only WAL files ensure minimal write latency
- **Automatic batching**: Pending writes batched and flushed periodically
- **Crash recovery**: Automatic recovery from WAL files on restart
- **Lazy loading**: Collections loaded into memory only when accessed
- **Database health checks**: Read-only integrity checks on startup
- **File locking**: Single instance per date enforcement
- **Automatic backups**: Daily backups to date-specific directories
- **Automatic cleanup**: Removes old backup directories based on retention policy
- **Thread-safe**: Safe for concurrent access with proper locking

## Installation

### Dependencies

**Required:**
```bash
pip install duckdb pandas pyarrow
```

Or with conda:

```bash
conda install -c conda-forge duckdb pandas pyarrow
```

**For Python < 3.11:**
```bash
pip install backports.strenum
```

Or with conda:

```bash
conda install -c conda-forge backports.strenum
```

## Timestamp and Date Handling

This module automatically normalizes datetime-like values for consistency:

### In-Memory Representation (Cache)
- **`timestamp` field**: Always a timezone-aware `datetime.datetime` object (UTC if no timezone specified)
- **User data with datetime values**: Any `datetime.date` objects or ISO datetime strings in your data dict are converted to timezone-aware `datetime.datetime`

### Storage Representation (WAL/DuckDB)
- **`timestamp` field**: Serialized to ISO 8601 string in JSON (e.g., `"2025-01-15T10:30:00+00:00"`)
- **User data datetime values**: Serialized to ISO 8601 strings in JSON

### Automatic Conversions
When you store data, the system automatically converts:
- ISO datetime strings → timezone-aware `datetime.datetime` (assume UTC if naive)
- `datetime.date` objects → `datetime.datetime` at midnight UTC
- Naive `datetime.datetime` → timezone-aware (assume UTC)
- `pandas.Timestamp` → timezone-aware `datetime.datetime`

### What This Means For You
```python
# All of these work and normalize to timezone-aware datetime
storage.store("key", data, value=100, timestamp="2025-01-15T10:30:00Z")        # String
storage.store("key", data, value=100, timestamp=dt.datetime.now())             # Naive datetime → UTC
storage.store("key", data, value=100, timestamp=dt.datetime.now(dt.timezone.utc))  # Already tz-aware

# Retrieved data always has datetime objects
result = storage.get_item("key", "collection", "item")
assert isinstance(result["timestamp"], dt.datetime)  # ✓ Always True
assert result["timestamp"].tzinfo is not None        # ✓ Always timezone-aware
```

**Benefits:**
- No type confusion across restarts (always `datetime.datetime` in memory)
- Timezone-safe comparisons and arithmetic
- Consistent behavior whether data came from WAL recovery or DuckDB
- ISO strings in storage for interoperability

## Usage

### Basic Example

```python
import datetime as dt
from collection_fast_persist import (
    CollectionFastPersist,
    CollectionConfig,
)
from fast_persist_common import StorageKeys

# Configure storage
config = CollectionConfig(
    base_dir="./my_storage",
    max_wal_size=10 * 1024 * 1024,  # 10MB
    batch_size=1000,
    duckdb_flush_interval_seconds=30,
    retain_days=5,
)

# Initialize storage
storage = CollectionFastPersist(dt.date.today(), config=config)

# Store data with typed values
storage.store(
    key="user_profile",
    data={
        "label": "Age",
        StorageKeys.TIMESTAMP: dt.datetime.now().isoformat(),
        StorageKeys.STATUS: "valid",
        StorageKeys.USERNAME: "john_doe",
    },
    item_name="age",
    collection_name="personal_info",
    value=32,  # Integer value - routed to value_int column
)

storage.store(
    key="user_profile",
    data={
        "label": "Height",
        StorageKeys.TIMESTAMP: dt.datetime.now().isoformat(),
    },
    item_name="height",
    collection_name="personal_info",
    value=1.75,  # Float value - routed to value_float column
)

# Retrieve entire collection
personal_info = storage.get_key("user_profile", "personal_info")
for item_name, data in personal_info.items():
    print(f"{item_name}: {data.get('value')} ({type(data.get('value')).__name__})")

# Retrieve specific item
age_data = storage.get_item("user_profile", "personal_info", "age")
print(f"Age: {age_data.get('value')}")  # Returns: 32 (int)

# Clean shutdown (updates latest table, creates backups)
storage.close()
```

### Form Data Example

```python
# Store form field values
form_fields = {
    "full_name": "John Doe",
    "email": "john@example.com",
    "phone": "555-1234",
    "age": 32,
    "subscribe": True,
}

for field_name, field_value in form_fields.items():
    storage.store(
        key="registration_form",
        data={"label": field_name.replace("_", " ").title()},
        item_name=field_name,
        collection_name="user_input",
        value=field_value,
    )

# Retrieve all form data
form_data = storage.get_key("registration_form", "user_input")
```

### Classroom Seating Example

```python
# Assign students to seats
for seat_num in range(1, 31):
    storage.store(
        key="classroom_seating",
        data={
            "student_id": f"STU{1000 + seat_num}",
            "assigned_by": "teacher_smith",
            StorageKeys.TIMESTAMP: dt.datetime.now().isoformat(),
        },
        item_name=f"seat_{seat_num}",
        collection_name="room_a",
        value=seat_num,
    )

# Get all seats in room A
room_a_seats = storage.get_key("classroom_seating", "room_a")
```

### Custom Configuration

```python
config = CollectionConfig(
    base_dir="./collection_storage",
    max_wal_size=50 * 1024 * 1024,  # 50MB WAL files
    batch_size=5000,                 # Flush every 5000 records
    duckdb_flush_interval_seconds=60, # Or every 60 seconds
    retain_days=7,                   # Keep 7 days of backups
)

storage = CollectionFastPersist(dt.date.today(), config=config)
```

## Configuration Options

| Parameter | Default | Description |
|-----------|---------|-------------|
| `base_dir` | `"./collection_storage"` | Base directory for all storage files |
| `max_wal_size` | `10485760` (10MB) | Maximum WAL file size before rotation |
| `batch_size` | `1000` | Number of records before triggering batch flush |
| `duckdb_flush_interval_seconds` | `30` | Force flush interval in seconds |
| `retain_days` | `5` | Number of days to retain backups |
| `extra_schema` | `None` | Dict mapping column names to PyArrow type names |

## Collection Organization

### Three-Level Hierarchy

```python
cache[key][collection_name][item_name] = {
    "data": {...},           # Your custom data
    "value": <int|float|str|None>,  # Typed value
    # ... special fields (timestamp, status, etc.)
}
```

**Example:**
- **Key**: `"user_profile"` (top-level identifier)
- **Collection**: `"personal_info"`, `"contact_info"` (groups of related items)
- **Item**: `"age"`, `"email"`, `"phone"` (individual data points)

### Lazy Loading

Collections are loaded into memory only when accessed:
```python
# First access - loads entire collection from latest DB
age = storage.get_item("user_profile", "personal_info", "age")

# Subsequent access - served from cache
height = storage.get_item("user_profile", "personal_info", "height")
```

## Typed Value Support

Values are automatically routed to typed columns based on Python type:

```python
storage.store(..., value=32)        # → value_int (BIGINT)
storage.store(..., value=1.75)      # → value_float (DOUBLE)
storage.store(..., value="text")    # → value_string (VARCHAR)
storage.store(..., value=None)      # → all value columns NULL
```

**Type can change between updates:**
```python
storage.store(..., value=32)         # int
storage.store(..., value="thirty")   # now string - no warning!
```

## Dual-Table Architecture

### History Table (`storage_history.duckdb`)
- **Purpose**: Record of all updates
- **Updates**: Real-time (on every write)
- **Schema**: Includes version tracking
- **Use case**: Audit trail, historical analysis

### Latest Table (`storage_latest.duckdb`)
- **Purpose**: Current values only
- **Updates**: On shutdown (via change tracking)
- **Schema**: Same as history (version always 1)
- **Use case**: Fast current-value queries

## Extra Schema (Custom Typed Columns)

Define custom typed columns that are stored in both history and latest
DuckDB tables. Values come from the `data` dict - if missing, they're NULL.

```python
import datetime as dt
from collection_fast_persist import CollectionFastPersist, CollectionConfig

# Define extra columns using PyArrow type names
config = CollectionConfig(
    base_dir="./my_storage",
    extra_schema={
        "priority": "int32",
        "score": "float64",
        "category": "string",
    },
)

storage = CollectionFastPersist(dt.date.today(), config=config)

# Store data - extra schema values extracted from data dict
storage.store(
    key="task_1",
    data={
        "label": "High Priority Task",
        "priority": 1,           # → priority column (INTEGER)
        "score": 95.5,           # → score column (DOUBLE)
        "category": "urgent",    # → category column (VARCHAR)
    },
    item_name="item_a",
    collection_name="tasks",
    value=100,
)

# Missing extra schema values become NULL
storage.store(
    key="task_2",
    data={
        "label": "Low Priority Task",
        "priority": 5,
        # score, category missing → NULL in DB
    },
    item_name="item_b",
    collection_name="tasks",
    value=50,
)

storage.close()
```

**Supported PyArrow Types:**
| PyArrow Type | DuckDB Type |
|--------------|-------------|
| `string` | VARCHAR |
| `int64` | BIGINT |
| `int32` | INTEGER |
| `int16` | SMALLINT |
| `int8` | TINYINT |
| `uint64` | UBIGINT |
| `uint32` | UINTEGER |
| `uint16` | USMALLINT |
| `uint8` | UTINYINT |
| `float64` | DOUBLE |
| `float32` | FLOAT |
| `bool` | BOOLEAN |
| `timestamp[us]`, `timestamp[ns]`, `timestamp[ms]`, `timestamp[s]` | TIMESTAMP |
| `date32`, `date64` | DATE |

**Important Notes:**
- Extra schema only applies to newly created tables (no ALTER TABLE migrations)
- Column names must not conflict with reserved names (`key`, `collection_name`,
  `item_name`, `data`, `value_int`, `value_float`, `value_string`, `timestamp`,
  `status`, `status_int`, `username`, `updated_at`, `version`)
- Invalid column names or types raise `ExtraSchemaError` at initialization

## Database Health Checks

### Automatic Checks (on startup)
```python
# Runs automatically when initializing
storage = CollectionFastPersist(dt.date.today(), config=config)
# If corruption detected, raises CollectionFastPersistError
```

### Manual Checks
```python
# Check history database
history_ok = storage.check_database_health(
    storage.history_db_path,
    "storage_history",
    storage.history_conn  # Use existing connection
)

# Check latest database
latest_ok = storage.check_database_health(
    storage.latest_db_path,
    "storage_latest",
    storage.latest_conn
)
```

## File Organization

### Directory Structure

```
./base_dir/
  ├── storage_history.duckdb          # Active history DB (all dates)
  ├── storage_latest.duckdb           # Active latest values DB
  ├── .lock_2025-12-24               # Instance lock file
  ├── 2025-12-20/                    # Old day (auto-deleted)
  │   ├── wal_000001.jsonl
  │   ├── storage_history.duckdb.backup
  │   └── storage_latest.duckdb.backup
  ├── 2025-12-23/                    # Yesterday
  │   ├── wal_000001.jsonl
  │   ├── storage_history.duckdb.backup
  │   └── storage_latest.duckdb.backup
  └── 2025-12-24/                    # Today (active)
      ├── wal_000001.jsonl
      └── wal_000002.jsonl
```

### Date Usage

**Important**: Date is used ONLY for organizing backups and WAL files, NOT for data isolation.
- All dates' data stored in same DuckDB files
- WAL files organized in `base_dir/{date}/` directories
- Backups created in date directories on shutdown

## Backup and Recovery

### Automatic Backups (on close)

```python
storage.close()
# 1. Flushes pending writes to history DB
# 2. Updates latest table with modified records
# 3. Closes DB connections
# 4. Copies storage_history.duckdb → {date}/storage_history.duckdb.backup
# 5. Copies storage_latest.duckdb → {date}/storage_latest.duckdb.backup
# 6. Cleans up WAL files
# 7. Deletes old date directories
```

### Crash Recovery (automatic)

On startup, if WAL files exist in the date directory:
```python
# Automatically recovers from WAL
storage = CollectionFastPersist(dt.date.today(), config=config)
# Replays all WAL entries into history DB
# Updates in-memory cache
```

### Manual Reconstruction

If database becomes corrupted:

```python
# Delete corrupted file
import os
os.remove("collection_storage/storage_history.duckdb")

# Rebuild from WAL files
storage = CollectionFastPersist(dt.date.today(), config=config)
records = storage.rebuild_history_from_wal("2025-12-24")
print(f"Recovered {records} records from WAL")

# Rebuild latest from history
latest_count = storage.rebuild_latest_from_history()
print(f"Rebuilt {latest_count} latest records")
```

## API Reference

### CollectionFastPersist

#### `__init__(date, base_dir, config)`

Initialize collection storage.

**Parameters:**
- `date` (dt.date | dt.datetime | str): Date for backup/WAL organization
- `base_dir` (str, optional): Base directory for storage files
- `config` (CollectionConfig, optional): Configuration object

**Raises:**
- `CollectionFastPersistError`: If another instance is running or database is corrupted

#### `store(key, data, item_name, collection_name, value, timestamp, username)`

Store data with optional typed value.

**Parameters:**
- `key` (str): Top-level identifier
- `data` (dict): Custom data dictionary
- `item_name` (str, optional): Item identifier (default: "")
- `collection_name` (str, optional): Collection identifier (default: "")
- `value` (int | float | str | None, optional): Typed value
- `timestamp` (str | dt.datetime | None, optional): Timestamp
- `username` (str | None, optional): Username

#### `get_key(key, collection_name=None)`

Get data for a key.

**Parameters:**
- `key` (str): Key to look up
- `collection_name` (str, optional): Optional collection filter

**Returns:**
- If `collection_name` is None: `dict[str, dict[str, dict]]` (all collections)
- If `collection_name` provided: `dict[str, dict]` (items in collection)
- `None` if key doesn't exist

#### `get_item(key, collection_name, item_name)`

Get data for specific key/collection/item.

**Parameters:**
- `key` (str): Key to look up
- `collection_name` (str): Collection name
- `item_name` (str): Item name

**Returns:**
- `dict` with data and value, or `None` if not found

#### `check_database_health(db_path, table_name, conn=None)`

Check if database is healthy.

**Parameters:**
- `db_path` (Path): Path to DuckDB file
- `table_name` (str): Table to verify
- `conn` (optional): Existing connection to use

**Returns:**
- `bool`: True if healthy, False if corrupted

#### `rebuild_history_from_wal(date)`

Reconstruct history database from WAL files.

**Parameters:**
- `date` (dt.date | dt.datetime | str): Date directory to read WAL from

**Returns:**
- `int`: Number of records recovered

#### `rebuild_latest_from_history()`

Reconstruct latest database from history.

**Returns:**
- `int`: Number of latest records created

#### `flush_data_to_duckdb()`

Force immediate flush of pending data to DuckDB.

#### `get_stats()`

Get storage statistics.

**Returns:**
```python
{
    "cache_size": int,           # Number of keys in cache
    "pending_writes": int,       # Number of pending write batches
    "modified_records": int,     # Number of modified records
    "current_wal_size": int,     # Current WAL file size
    "current_wal_count": int,    # Records in current WAL
    "wal_files_count": int,      # Total WAL files
    "wal_sequence": int,         # Current WAL sequence number
}
```

#### `close()`

Clean shutdown with backup creation.

## Architecture

### Write Path

1. Data written to in-memory cache
2. Written to WAL file with fsync
3. Modification tracked in set
4. Background thread batches writes to history DB
5. On close, latest table updated with modified records

### Read Path

1. Check if collection in cache
2. If not, load entire collection from latest DB
3. Serve from cache

### Recovery Process

1. On startup, check for WAL files in date directory
2. If found, replay into history DB
3. Update in-memory cache
4. Delete processed WAL files

### Crash Safety

- Every write fsynced to WAL immediately
- WAL files are append-only
- Recovery guaranteed from WAL files
- Database backups created before shutdown

## Thread Safety

- Write operations protected by locks
- Safe for concurrent access within single process
- File locking prevents multiple instances per date

## Performance Characteristics

- **Write latency**: ~1-2ms (WAL append + fsync)
- **Read latency**: ~0.1ms (in-memory cache)
- **Collection loading**: ~10-50ms (depends on collection size)
- **Batch flush**: Background, non-blocking

## Use Cases

Ideal for:
- **Form data storage**: Track all field values in a form
- **Seat assignments**: Classroom or venue seating management
- **Computer assignments**: Track user-to-computer mappings
- **Configuration management**: Group related config items
- **Inventory systems**: Organize items by category/location
- **Settings panels**: Store user preferences by category

Not ideal for:
- Large binary blobs (use object storage)
- Frequent cross-collection queries (use relational DB)
- Real-time analytics (use time-series DB)

## Error Handling

### Database Corruption

```python
CollectionFastPersistError: Failed to open storage_history.duckdb.
Database may be corrupted. To recover:
1. Delete the corrupted file
2. Call rebuild_history_from_wal(date) for each date that needs recovery
3. Call rebuild_latest_from_history() to rebuild latest values
```

### Single Instance Violation

```python
CollectionFastPersistError: Another instance is already running for date 2025-12-24.
Lock file exists at collection_storage/.lock_2025-12-24
```

**Handling Stale Lock Files:**

If your application crashed or was terminated abnormally, the lock file may remain even though no instance is running. To recover:

1. **Verify no instance is running**: Ensure no other process is using this storage
2. **Manually remove the lock file**:
   ```bash
   # Unix/Linux/Mac:
   rm collection_storage/.lock_2025-12-24

   # Windows:
   del collection_storage\.lock_2025-12-24
   ```
3. **Restart your application**: The lock file will be recreated automatically

**Note**: Lock files are automatically removed on clean shutdown via the `close()` method. Always call `close()` when your application exits normally, or use a context manager to ensure cleanup.

## Limitations

- All data must fit in memory (lazy-loaded collections)
- Single-node only (no distributed support)
- Key/collection/item hierarchy access only (no complex queries on cached data)
- Dictionary/JSON values only
- **Automatic datetime conversion**: ISO datetime strings in user data are automatically converted to timezone-aware `datetime.datetime` objects in memory. If you need to preserve datetime values as strings, wrap them in a different structure or use a non-ISO format

## Testing

### Basic Functional Test

See [test_collection_fast_persist.py](test_collection_fast_persist.py) for comprehensive examples covering:
- Typed value storage and retrieval
- Type changes
- Collection operations
- Database health checks
- Clean shutdown and recovery

```bash
python test_collection_fast_persist.py
```

### Crash Recovery Test

Run the three-stage crash recovery test to verify WAL-based durability:

**Stage 1: Write data and simulate crash**
```bash
python test_collection_crash_recovery.py 1
```

This stage:
- Writes 24 records across user profiles, classroom seating, and forms
- Tests typed values (int, float, string)
- Leaves WAL files with unflushed data
- Simulates crash without clean shutdown

**Stage 2: Recover and verify**
```bash
python test_collection_crash_recovery.py 2
```

This stage:
- Automatically recovers all data from WAL files
- Verifies data integrity and type preservation
- Tests database health checks
- Performs clean shutdown with backup creation
- Validates backup files

**Stage 3: Test manual reconstruction**
```bash
python test_collection_crash_recovery.py 3
```

This stage:
- Tests `rebuild_latest_from_history()` function
- Verifies data remains accessible after reconstruction

**Cleaning up test artifacts:**

Tests create storage directories that persist between runs. Use the cleanup script:

```bash
python test_cleanup.py
```

Or manually remove directories:
```bash
# Unix/Linux/Mac:
rm -rf ./collection_test_storage ./crash_test_collection

# Windows:
rmdir /s /q collection_test_storage crash_test_collection
```

## License

MIT License

## Contributing

Contributions welcome! Please ensure tests pass before submitting PRs.
