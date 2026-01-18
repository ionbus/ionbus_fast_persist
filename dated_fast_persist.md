# dated_fast_persist

A high-performance Python persistence layer with date-based storage isolation, combining Write-Ahead Logs (WAL) with DuckDB for fast asynchronous writes and reliable storage.

<!-- TOC start (generated with https://bitdowntoc.derlin.ch/) -->

- [Overview](#overview)
- [Features](#features)
- [Installation](#installation)
   * [Dependencies](#dependencies)
- [Usage](#usage)
   * [Basic Example](#basic-example)
   * [FastAPI Integration Example](#fastapi-integration-example)
   * [Custom Configuration](#custom-configuration)
   * [Parquet Export](#parquet-export)
- [Configuration Options](#configuration-options)
- [Multi-Process Tracking](#multi-process-tracking)
   * [Storage Model](#storage-model)
   * [Database Schema](#database-schema)
   * [Usage Pattern](#usage-pattern)
- [Architecture](#architecture)
   * [Write Path](#write-path)
   * [Read Path](#read-path)
   * [Recovery Process](#recovery-process)
   * [Crash Safety](#crash-safety)
- [API Reference](#api-reference)
   * [StorageKeys (StrEnum)](#storagekeys-strenum)
   * [WALDuckDBStorage](#walduckdbstorage)
      + [`__init__(db_path: str, config: Optional[WALConfig] = None)`](#__init__db_path-str-config-optionalwalconfig-none)
      + [`store(key, data, process_name, timestamp)`](#storekey-str-data-dictstr-any-process_name-str--none--none-timestamp-str--datetime--none--none)
      + [`get_key(key: str)`](#get_keykey-str-optionaldictstr-dictstr-any)
      + [`get_key_process(key, process_name)`](#get_key_processkey-str-process_name-str--none--none-optionaldictstr-any)
      + [`flush_data_to_duckdb()`](#flush_data_to_duckdb)
      + [`get_stats() -> Dict[str, Any]`](#get_stats-dictstr-any)
      + [`export_to_parquet(parquet_path)`](#export_to_parquetparquet_path-str--none--none-str--none)
      + [`close()`](#close)
- [Testing and Examples](#testing-and-examples)
   * [Basic Functional Test](#basic-functional-test)
   * [Crash Recovery Test](#crash-recovery-test)
- [Thread Safety](#thread-safety)
- [Performance Characteristics](#performance-characteristics)
- [Use Cases](#use-cases)
- [Limitations](#limitations)
- [License](#license)
- [Contributing](#contributing)

<!-- TOC end -->


## Overview

`dated_fast_persist` provides a hybrid storage system designed for FastAPI servers
and high-performance applications with date-based storage isolation:

- **Fast async writes** via Write-Ahead Logs (WAL)
- **Reliable persistence** using DuckDB
- **Automatic crash recovery** from WAL files
- **In-memory caching** for quick reads
- **Background batch processing** to optimize database writes
- **FastAPI integration**: Designed to work seamlessly with dictionary
  payloads from FastAPI endpoints

## Features

- **Date-based storage isolation**: Each date gets separate subdirectories
  for WAL files and DuckDB, allowing concurrent multi-date processing
- **WAL-based writing**: All writes go to append-only WAL files first,
  ensuring minimal write latency
- **Multi-process tracking**: Track data per process with automatic
  organization by key and process_name
- **Automatic batching**: Pending writes are batched and flushed to DuckDB
  periodically or when thresholds are reached
- **Crash recovery**: Automatically recovers pending writes from WAL files
  on startup
- **Thread-safe**: Safe for concurrent access with proper locking
- **Configurable**: Tunable parameters for WAL rotation, batch sizes, and
  flush intervals
- **In-memory cache**: Fast reads from nested memory cache (key →
  process_name → data), synchronized with persistent storage
- **Special field extraction**: Automatically extracts and indexes
  process_name, timestamp, status, status_int, and username for efficient
  querying
- **Parquet export**: Export all data to Hive-partitioned Parquet files
  for analytics and data warehousing. Automatically exports on clean
  shutdown when `parquet_path` is configured
- **Extra schema support**: Define custom typed columns (via PyArrow type
  names) that are stored in DuckDB and included in Parquet exports

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

**For Python < 3.11 (StrEnum support):**
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
- **Date isolation values**: The `date` parameter remains a `datetime.date` for directory/file organization

### Storage Representation (WAL/DuckDB)
- **`timestamp` field**: Serialized to ISO 8601 string in JSON (e.g., `"2025-01-15T10:30:00+00:00"`)
- **User data datetime values**: Serialized to ISO 8601 strings in JSON
- **Date column** (Parquet exports): ISO string format (e.g., `"2025-12-26"`)

### Automatic Conversions
When you store data, the system automatically converts:
- ISO datetime strings → timezone-aware `datetime.datetime` (assume UTC if naive)
- `datetime.date` objects → `datetime.datetime` at midnight UTC
- Naive `datetime.datetime` → timezone-aware (assume UTC)
- `pandas.Timestamp` → timezone-aware `datetime.datetime`

### What This Means For You
```python
# All of these work and normalize to timezone-aware datetime
storage.store("key", data, timestamp="2025-01-15T10:30:00Z")        # String
storage.store("key", data, timestamp=dt.datetime.now())             # Naive datetime → UTC
storage.store("key", data, timestamp=dt.datetime.now(dt.timezone.utc))  # Already tz-aware

# Retrieved data always has datetime objects
result = storage.get_key_process("key", "process")
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
from dated_fast_persist import WALDuckDBStorage, WALConfig, StorageKeys

# Initialize with default configuration (date is required)
storage = WALDuckDBStorage(dt.date.today(), "data.duckdb")

# Store data using dictionary keys (typical FastAPI usage)
# StorageKeys enum provides constants for special fields
data = {
    "name": "data_processing",
    "progress": 75,
    StorageKeys.PROCESS_NAME: "worker1",  # or "process_name"
    StorageKeys.STATUS: "running",         # or "status"
    StorageKeys.USERNAME: "alice",         # or "username"
    StorageKeys.TIMESTAMP: "2025-01-15T10:30:00Z"  # Auto-converted to datetime
}
storage.store("task_status", data)

# Or use explicit parameters
storage.store(
    "task_status",
    {"name": "data_validation", "progress": 50},
    process_name="worker2",
    username="bob",
    timestamp="2025-01-15T10:35:00"  # Timezone optional, auto-converted
)

# Get all process data for a key
all_processes = storage.get_key("task_status")
# Returns: {"worker1": {...}, "worker2": {...}}

# Get specific process data
worker1_data = storage.get_key_process("task_status", "worker1")
# Returns: {"name": "data_processing", "progress": 75, ...}

# Clean shutdown (flushes all pending writes)
storage.close()
```

### FastAPI Integration Example

```python
import datetime as dt
from fastapi import FastAPI
from pydantic import BaseModel
from dated_fast_persist import WALDuckDBStorage, StorageKeys

app = FastAPI()
# Initialize with current date - each date has separate storage
storage = WALDuckDBStorage(dt.date.today(), "api_data.duckdb")

class TaskUpdate(BaseModel):
    name: str
    progress: int
    status: str
    process_name: str | None = None
    username: str | None = None
    timestamp: str | None = None

@app.post("/task/{task_id}")
async def update_task(task_id: str, update: TaskUpdate):
    # Convert Pydantic model to dict - all special fields auto-extracted
    data = update.model_dump()

    # Timestamp string automatically converted to datetime
    storage.store(task_id, data)

    return {"status": "stored", "task_id": task_id}

@app.get("/task/{task_id}")
async def get_task(task_id: str, process_name: str | None = None):
    if process_name:
        return storage.get_key_process(task_id, process_name)
    else:
        return storage.get_key(task_id)

@app.on_event("shutdown")
async def shutdown():
    storage.close()
```

### Custom Configuration

```python
import datetime as dt

config = WALConfig(
    base_dir="./wal_storage",        # Directory for WAL files
    max_wal_size=10 * 1024 * 1024,   # 10MB per WAL file
    max_wal_age_seconds=300,          # 5 minutes
    batch_size=1000,                  # Flush after 1000 records
    duckdb_flush_interval_seconds=30,        # Flush every 30 seconds
    parquet_path="./data_export"     # Optional: Parquet export path
)

storage = WALDuckDBStorage(dt.date.today(), "data.duckdb", config)
```

### Parquet Export

Export all data to Hive-partitioned Parquet files for analytics, data
warehousing, or integration with tools like Apache Spark, Presto, or AWS
Athena.

```python
import datetime as dt
from dated_fast_persist import WALDuckDBStorage, WALConfig

# Option 1: Automatic export on close (recommended)
config = WALConfig(parquet_path="./data_export")
storage = WALDuckDBStorage(dt.date.today(), "data.duckdb", config)

# Store some data
storage.store("metrics", {"cpu": 75, "memory": 60}, process_name="server1")
storage.store("metrics", {"cpu": 82, "memory": 55}, process_name="server2")

# Data will be automatically exported to parquet when close() is called
storage.close()

# Option 2: Manual export at any time
storage.export_to_parquet()  # Uses config.parquet_path

# Option 3: Specify path explicitly (overrides config)
storage.export_to_parquet("./custom_export")
```

### Extra Schema (Custom Typed Columns)

Define custom typed columns that are stored in DuckDB and included in
Parquet exports. Values come from the `data` dict - if missing, they're NULL.

```python
import datetime as dt
from dated_fast_persist import WALDuckDBStorage, WALConfig

# Define extra columns using PyArrow type names
config = WALConfig(
    parquet_path="./data_export",
    extra_schema={
        "customer_id": "int64",
        "price": "float64",
        "is_active": "bool",
        "notes": "string",
    },
)

storage = WALDuckDBStorage(dt.date.today(), "data.duckdb", config)

# Store data - extra schema values extracted from data dict
storage.store(
    "order_1",
    {
        "order_name": "Test Order",
        "customer_id": 12345,       # → customer_id column (BIGINT)
        "price": 99.99,             # → price column (DOUBLE)
        "is_active": True,          # → is_active column (BOOLEAN)
        "notes": "First order",     # → notes column (VARCHAR)
    },
    process_name="worker1",
)

# Missing extra schema values become NULL
storage.store(
    "order_2",
    {
        "order_name": "Partial",
        "customer_id": 67890,
        # price, is_active, notes are missing → NULL in DB
    },
    process_name="worker1",
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
- Column names must not conflict with reserved names (`key`, `process_name`,
  `data`, `timestamp`, `status`, `status_int`, `username`, `updated_at`, `version`)
- Invalid column names or types raise `ExtraSchemaError` at initialization

**Output Structure:** Data is partitioned by `process_name` and `date`:
```
./data_export/
├── process_name=server1/
│   └── date=2025-01-15/
│       └── data.parquet
└── process_name=server2/
    └── date=2025-01-15/
        └── data.parquet
```

**Exported Columns:**
- `key`: Primary identifier
- `process_name`: Process identifier (partition column)
- `date`: Date string (partition column)
- `data`: Full JSON data payload
- `timestamp`: Extracted timestamp
- `status`: Extracted status string
- `status_int`: Extracted status integer
- `updated_at`: Last update timestamp
- `version`: Record version number
```

## Configuration Options

| Parameter | Default | Description |
|-----------|---------|-------------|
| `base_dir` | `"./storage"` | Directory for WAL files |
| `max_wal_size` | `10485760` (10MB) | Max size before WAL rotation |
| `max_wal_age_seconds` | `300` (5 min) | Max age before WAL rotation |
| `batch_size` | `1000` | Records before batch flush |
| `duckdb_flush_interval_seconds` | `30` | Force flush interval |
| `parquet_path` | `None` | Path for Hive-partitioned Parquet export |
| `extra_schema` | `None` | Dict mapping column names to PyArrow type names |

## Date-Based Storage Isolation

`dated_fast_persist` requires a date when initializing storage, providing
complete isolation between different dates. This enables:

- **Concurrent multi-date processing**: Run separate processes for
  different dates simultaneously without conflicts
- **Clean data organization**: Each date's data stored in its own
  subdirectory structure
- **Independent recovery**: Each date can be recovered independently
- **Simple data management**: Archive or delete data by date

### Directory Structure

```
./storage/              # Base directory (configurable)
  ├── 2025-01-15/       # Date-specific subdirectory
  │   ├── wal_000001.jsonl
  │   ├── wal_000002.jsonl
  │   └── data.duckdb
  ├── 2025-01-16/
  │   ├── wal_000001.jsonl
  │   └── data.duckdb
  └── ...
```

### Usage Example

```python
import datetime as dt

# Process data for specific date
date = dt.date(2025, 1, 15)
storage = WALDuckDBStorage(date, "data.duckdb")

# Or use string format
storage = WALDuckDBStorage("2025-01-15", "data.duckdb")

# Or today's date
storage = WALDuckDBStorage(dt.date.today(), "data.duckdb")
```

## Multi-Process Tracking

`dated_fast_persist` supports tracking data from multiple processes under the
same key, making it ideal for distributed systems and parallel processing:

### Storage Model

Data is organized hierarchically:
- **Primary level**: Key (e.g., "task_status", "metrics")
- **Secondary level**: Process name (e.g., "worker1", "worker2", None)
- **Data level**: Your application data

### Database Schema

The DuckDB table uses a composite primary key `(key, process_name)` with
these columns:

| Column | Type | Description |
|--------|------|-------------|
| `key` | VARCHAR | Primary identifier (part of composite key) |
| `process_name` | VARCHAR | Process identifier (part of composite key, nullable) |
| `data` | JSON | Full data payload including all fields |
| `timestamp` | TIMESTAMP | Extracted from data, indexed (nullable) |
| `status` | VARCHAR | Extracted from data (nullable) |
| `status_int` | INTEGER | Extracted from data (nullable) |
| `username` | VARCHAR | Extracted from data (nullable) |
| `updated_at` | TIMESTAMP | Auto-updated on each write |
| `version` | INTEGER | Increments per (key, process_name) pair |

### Usage Pattern

```python
# Different processes can store under same key
storage.store("job_status", {...}, process_name="worker1")
storage.store("job_status", {...}, process_name="worker2")

# Retrieve all process data at once
all_workers = storage.get_key("job_status")
for process_name, data in all_workers.items():
    print(f"{process_name}: {data['status']}")

# Or get specific process
worker1_status = storage.get_key_process("job_status", "worker1")
```

## Architecture

### Write Path

1. Data is written to nested in-memory cache immediately:
   `cache[key][process_name] = data`
2. Special fields (process_name, timestamp, status, status_int, username)
   are extracted from data
3. Entry is appended to current WAL file (contains full data)
4. WAL is rotated when size/count/age thresholds are met
5. Background thread periodically flushes batched writes to DuckDB with
   special fields in separate columns
6. Processed WAL files are deleted after successful flush

### Read Path

1. All reads served from nested in-memory cache:
   `cache[key][process_name]`
2. Cache is synchronized with DuckDB on startup, reconstructing nested
   structure
3. Cache updated immediately on writes with proper nesting

### Recovery Process

On startup:
1. Scans for existing WAL files
2. Replays all WAL entries into cache
3. Flushes recovered data to DuckDB
4. Cleans up processed WAL files

### Crash Safety

`dated_fast_persist` is designed for crash consistency through a two-layer
protection mechanism:

**DuckDB Layer Protection:**
- DuckDB is fully ACID-compliant and designed to maintain database file
  integrity even during crashes
- Once a transaction commits (via `COMMIT`), those changes are durable
  and will survive process crashes
- Uncommitted transactions are discarded on crash - the database remains
  consistent with the last committed state
- DuckDB uses its own internal write-ahead log and `fsync` operations
  (by default) to ensure committed data reaches disk

**Application WAL Layer Protection:**
- All writes are immediately appended to application-level WAL files
  before being batched to DuckDB
- Each WAL write is followed by `fsync()` on the file to ensure data
  reaches stable storage
- On Unix-like systems (Linux, macOS), directory fsync is also performed
  after WAL file creation to ensure directory entry durability
- On Windows, directory fsync is not available; directory entry
  durability relies on filesystem behavior
- If the process crashes during a DuckDB write operation, the WAL files
  preserve any data that wasn't yet committed
- On restart, the recovery process replays all WAL entries and re-flushes
  them to DuckDB
- This protects against data loss from process crashes; power loss
  protection varies by platform and filesystem configuration

**Combined Design:**

In typical crash scenarios (process termination, system crashes):
1. The DuckDB file remains readable and consistent
2. Any uncommitted data is preserved in WAL files
3. The recovery process automatically restores all pending writes
4. The system continues operating normally

**Important Notes:**
- Durability depends on the filesystem and storage hardware honoring
  `fsync()` guarantees
- Per-write `fsync()` has significant performance cost (~1-10ms per
  write depending on storage); this is the trade-off for durability
- On Windows, directory entry durability is not explicitly ensured due
  to platform limitations
- Catastrophic failures (disk corruption, filesystem bugs) may still
  cause data loss
- For maximum durability, ensure the underlying storage is configured
  for data integrity (e.g., write-back caching disabled or
  battery-backed)

This dual-layer approach provides both the performance of async writes
and strong crash consistency under normal operating conditions.

## API Reference

### StorageKeys (StrEnum)

String enumeration providing constants for special dictionary keys used
by the storage system. Use these for type-safe dictionary access in
FastAPI and other applications.

**Constants:**
```python
class StorageKeys(StrEnum):
    PROCESS_NAME = "process_name"  # Process identifier
    TIMESTAMP = "timestamp"         # Timestamp (auto-converted from string)
    STATUS = "status"               # Status string
    STATUS_INT = "status_int"       # Status integer
    USERNAME = "username"           # Username identifier
```

**Usage:**
```python
from dated_fast_persist import StorageKeys

data = {
    "my_field": "value",
    StorageKeys.PROCESS_NAME: "worker1",
    StorageKeys.TIMESTAMP: "2025-01-15T10:30:00Z",
    StorageKeys.STATUS: "running",
    StorageKeys.USERNAME: "alice"
}
storage.store("key", data)
```

**Timestamp Conversion:**
- Accepts ISO 8601 strings with or without timezone
- Automatically converted to Python datetime objects
- Timezones preserved if provided, otherwise assumed UTC
- Examples: `"2025-01-15T10:30:00Z"`, `"2025-01-15T10:30:00"`,
  `"2025-01-15T10:30:00-05:00"`

### WALDuckDBStorage

#### `__init__(date: dt.date | dt.datetime | str, db_path: str = "data.duckdb", config: WALConfig | None = None)`

Initialize the storage system with date-based isolation.

**Parameters:**
- `date`: Date for this storage instance (required). Can be:
  - `dt.date` object (e.g., `dt.date.today()`)
  - `dt.datetime` object (date portion extracted)
  - ISO format string (e.g., `"2025-01-15"` or `"2025-01-15T10:30:00"`)
- `db_path`: Path to DuckDB database file (default: `"data.duckdb"`)
- `config`: Optional WALConfig for customization

**Date-Based Storage Isolation:**
- Each date gets its own subdirectory for WAL files: `{base_dir}/{YYYY-MM-DD}/`
- Database path handling:
  - **Relative paths** (e.g., `"data.duckdb"` or `"db/data.duckdb"`): Placed under `{base_dir}/{YYYY-MM-DD}/{db_path}` maintaining date isolation
  - **Absolute paths** (e.g., `"/shared/data.duckdb"`): Used as-is, bypassing date isolation
- **Warning**: Using absolute paths breaks date isolation - avoid reusing the same absolute path across multiple dates
- Date isolation allows running multiple dates concurrently in separate processes

#### `store(key: str, data: dict[str, Any], process_name: str | None = None, timestamp: str | dt.datetime | None = None, username: str | None = None)`

Store data with optional process and time tracking.

**Parameters:**
- `key`: Unique identifier for the data
- `data`: Dictionary to store (keeps all fields including special ones)
- `process_name`: Optional process identifier. If not provided, extracted
  from `data["process_name"]` if present, otherwise None
- `timestamp`: Optional timestamp (string or datetime). If not provided,
  extracted from `data["timestamp"]` if present
- `username`: Optional username identifier. If not provided, extracted
  from `data["username"]` if present, otherwise None

**Special Fields:**
The following fields, if present in `data`, are automatically extracted
and stored in separate indexed columns while remaining in the data dict:
- `process_name`: Process identifier
- `timestamp`: Timestamp (converted to datetime if string)
- `status`: Status string
- `status_int`: Status integer
- `username`: Username identifier

**Storage:**
Data is stored with composite key `(key, process_name)` allowing
multiple processes to track data under the same key.

#### `get_key(key: str) -> dict[str, dict[str, Any]] | None`

Retrieve all process data for a given key.

**Parameters:**
- `key`: The key to look up

**Returns:** Dictionary mapping process_name to data dict, or None if
key doesn't exist
```python
{
    "worker1": {"name": "...", "progress": 75, ...},
    "worker2": {"name": "...", "progress": 50, ...},
    None: {"name": "...", ...}  # If process_name was None
}
```

#### `get_key_process(key: str, process_name: str | None = None) -> dict[str, Any] | None`

Retrieve data for a specific key and process_name combination.

**Parameters:**
- `key`: The key to look up
- `process_name`: The process name (or None for unspecified process)

**Returns:** Data dictionary if found, None otherwise

#### `flush_data_to_duckdb()`

Force immediate flush of all pending data to DuckDB. This method rotates
the current WAL file and flushes all pending writes to the database.

#### `get_stats() -> dict[str, Any]`

Get current storage statistics.

**Returns:** Dictionary containing:
- `cache_size`: Number of cached items
- `pending_writes`: Number of pending writes
- `current_wal_size`: Current WAL file size in bytes
- `current_wal_count`: Number of records in current WAL
- `wal_files_count`: Total number of WAL files
- `wal_sequence`: Current WAL sequence number

#### `export_to_parquet(parquet_path: str | None = None) -> str | None`

Export all DuckDB data to Hive-partitioned Parquet files.

**Parameters:**
- `parquet_path`: Path to save parquet files. If None, uses
  `config.parquet_path`

**Returns:** Path where parquet was saved, or None if no data to export

**Raises:**
- `ImportError`: If pandas or pyarrow is not installed
- `ValueError`: If no parquet_path is provided or configured

**Partition Structure:** Data is partitioned by `process_name` and
`date` (in that order), creating a Hive-style directory structure:
```
parquet_path/
├── process_name=worker1/
│   └── date=2025-01-15/
│       └── *.parquet
├── process_name=worker2/
│   └── date=2025-01-15/
│       └── *.parquet
└── process_name=/
    └── date=2025-01-15/
        └── *.parquet
```

**Example:**
```python
# Using config
config = WALConfig(parquet_path="./data_export")
storage = WALDuckDBStorage(dt.date.today(), "data.duckdb", config)
storage.export_to_parquet()  # Uses config.parquet_path

# Or specify path explicitly
storage.export_to_parquet("./custom_export")
```

#### `close()`

Clean shutdown - stops background threads, flushes all pending data,
automatically exports to Parquet (if `parquet_path` is configured), and
cleans up all WAL files.

**Automatic Parquet Export:** If `config.parquet_path` is set, `close()` will
automatically call `export_to_parquet()` before shutdown. This ensures your
data is always exported when the storage system shuts down cleanly.

## Testing and Examples

### Basic Functional Test

Run the basic test to see all features in action:

```bash
python test_fast_persist.py
```

This test demonstrates:
1. Writes 250 random records with multi-process tracking
2. Shows statistics during writing
3. Forces a flush
4. Tests retrieval by key and process
5. Exports data to Hive-partitioned Parquet files
6. Restarts to test recovery
7. Verifies recovered data

### Crash Recovery Test

Run the two-stage crash recovery test to verify WAL-based durability:

**Stage 1: Write data and simulate crash**
```bash
python test_crash_recovery.py 1
```

This stage:
- Writes 9 records across 3 keys and 3 processes
- Leaves WAL files with unflushed data
- Kills process without clean shutdown (simulates crash)

**Stage 2: Recover and verify**
```bash
python test_crash_recovery.py 2
```

This stage:
- Automatically recovers all data from WAL files
- Verifies data integrity (all 9 records)
- Tests specific queries
- Exports to Parquet with Hive partitioning
- Performs clean shutdown and cleanup
- Validates Parquet structure and contents

**Expected Results:**
- ✓ All data recovered from WAL files
- ✓ In-memory cache rebuilt correctly
- ✓ Data persisted to DuckDB
- ✓ Parquet export with proper Hive structure
- ✓ All WAL files cleaned up after shutdown

**Cleaning up test artifacts:**

Tests create storage directories that persist between runs. Use the cleanup script:

```bash
python test_cleanup.py
```

Or manually remove directories:
```bash
# Unix/Linux/Mac:
rm -rf ./wal_storage ./test_output ./crash_test_output

# Windows:
rmdir /s /q wal_storage test_output crash_test_output
```

## Thread Safety

The storage system is thread-safe with the following guarantees:

- Writes are protected by `write_lock`
- Flushes are protected by `flush_lock`
- Safe for concurrent reads and writes
- Background flush thread runs safely alongside user operations

## Performance Characteristics

- **Write latency**: ~1-10ms per write (includes fsync to disk)
  - Dominated by fsync cost, varies significantly by storage hardware
  - SSDs: typically 1-3ms
  - HDDs: typically 5-10ms
  - Note: This is the durability trade-off for crash safety
- **Read latency**: ~0.1ms (in-memory cache)
- **Batch flush**: Processes 1000s of records per second to DuckDB
- **Recovery time**: Depends on WAL file count and size

## Use Cases

- High-frequency data updates (sensors, metrics, logs)
- Applications requiring fast writes with eventual consistency
- Systems needing crash recovery guarantees
- Scenarios where write throughput >> read throughput

## Limitations

- All data must fit in memory (cache stores full dataset)
- Single-node only (no distributed support)
- Key-based access only (no complex queries on cached data)
- Dictionary/JSON values only
- **Automatic datetime conversion**: ISO datetime strings in user data are automatically converted to timezone-aware `datetime.datetime` objects in memory. If you need to preserve datetime values as strings, wrap them in a different structure or use a non-ISO format

## License

See project license file.

## Contributing

Contributions welcome. Please follow PEP 8 and existing code style.
