# fast_persist

A high-performance Python persistence layer combining Write-Ahead Logs (WAL)
with DuckDB for fast asynchronous writes and reliable storage.

<!-- TOC start (generated with https://bitdowntoc.derlin.ch/) -->

- [Overview](#overview)
- [Features](#features)
- [Installation](#installation)
   * [Dependencies](#dependencies)
- [Usage](#usage)
   * [Basic Example](#basic-example)
   * [Custom Configuration](#custom-configuration)
- [Configuration Options](#configuration-options)
- [Architecture](#architecture)
   * [Write Path](#write-path)
   * [Read Path](#read-path)
   * [Recovery Process](#recovery-process)
   * [Crash Safety](#crash-safety)
- [API Reference](#api-reference)
   * [WALDuckDBStorage](#walduckdbstorage)
      + [`__init__(db_path: str, config: Optional[WALConfig] = None)`](#__init__db_path-str-config-optionalwalconfig-none)
      + [`store(key: str, data: Dict[str, Any])`](#storekey-str-data-dictstr-any)
      + [`get(key: str) -> Optional[Dict[str, Any]]`](#getkey-str-optionaldictstr-any)
      + [`force_flush()`](#force_flush)
      + [`get_stats() -> Dict[str, Any]`](#get_stats-dictstr-any)
      + [`close()`](#close)
- [Example: Running the Demo](#example-running-the-demo)
- [Thread Safety](#thread-safety)
- [Performance Characteristics](#performance-characteristics)
- [Use Cases](#use-cases)
- [Limitations](#limitations)
- [License](#license)
- [Contributing](#contributing)

<!-- TOC end -->


## Overview

`fast_persist` provides a hybrid storage system that offers:

- **Fast async writes** via Write-Ahead Logs (WAL)
- **Reliable persistence** using DuckDB
- **Automatic crash recovery** from WAL files
- **In-memory caching** for quick reads
- **Background batch processing** to optimize database writes

## Features

- **WAL-based writing**: All writes go to append-only WAL files first,
  ensuring minimal write latency
- **Automatic batching**: Pending writes are batched and flushed to DuckDB
  periodically or when thresholds are reached
- **Crash recovery**: Automatically recovers pending writes from WAL files
  on startup
- **Thread-safe**: Safe for concurrent access with proper locking
- **Configurable**: Tunable parameters for WAL rotation, batch sizes, and
  flush intervals
- **In-memory cache**: Fast reads from memory cache, synchronized with
  persistent storage

## Installation

### Dependencies

```bash
pip install duckdb
```

Or with conda:

```bash
conda install -c conda-forge duckdb
```

## Usage

### Basic Example

```python
from fast_persist_claude import WALDuckDBStorage, WALConfig

# Initialize with default configuration
storage = WALDuckDBStorage("data.duckdb")

# Store data
storage.store("user_123", {"name": "Alice", "score": 100})

# Retrieve data
data = storage.get("user_123")
print(data)  # {"name": "Alice", "score": 100}

# Clean shutdown (flushes all pending writes)
storage.close()
```

### Custom Configuration

```python
config = WALConfig(
    base_dir="./wal_storage",        # Directory for WAL files
    max_wal_size=10 * 1024 * 1024,   # 10MB per WAL file
    max_wal_age_seconds=300,          # 5 minutes
    batch_size=1000,                  # Flush after 1000 records
    flush_interval_seconds=30         # Flush every 30 seconds
)

storage = WALDuckDBStorage("data.duckdb", config)
```

## Configuration Options

| Parameter | Default | Description |
|-----------|---------|-------------|
| `base_dir` | `"./storage"` | Directory for WAL files |
| `max_wal_size` | `10485760` (10MB) | Max size before WAL rotation |
| `max_wal_age_seconds` | `300` (5 min) | Max age before WAL rotation |
| `batch_size` | `1000` | Records before batch flush |
| `flush_interval_seconds` | `30` | Force flush interval |

## Architecture

### Write Path

1. Data is written to in-memory cache immediately
2. Entry is appended to current WAL file
3. WAL is rotated when size/count/age thresholds are met
4. Background thread periodically flushes batched writes to DuckDB
5. Processed WAL files are deleted after successful flush

### Read Path

1. All reads served from in-memory cache
2. Cache is synchronized with DuckDB on startup
3. Cache updated immediately on writes

### Recovery Process

On startup:
1. Scans for existing WAL files
2. Replays all WAL entries into cache
3. Flushes recovered data to DuckDB
4. Cleans up processed WAL files

### Crash Safety

`fast_persist` is designed for crash consistency through a two-layer
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

### WALDuckDBStorage

#### `__init__(db_path: str, config: Optional[WALConfig] = None)`

Initialize the storage system.

**Parameters:**
- `db_path`: Path to DuckDB database file
- `config`: Optional WALConfig for customization

#### `store(key: str, data: Dict[str, Any])`

Store a key-value pair.

**Parameters:**
- `key`: Unique identifier
- `data`: Dictionary to store

#### `get(key: str) -> Optional[Dict[str, Any]]`

Retrieve data by key.

**Returns:** Dictionary if found, None otherwise

#### `force_flush()`

Force immediate flush of all pending writes to DuckDB.

#### `get_stats() -> Dict[str, Any]`

Get current storage statistics.

**Returns:** Dictionary containing:
- `cache_size`: Number of cached items
- `pending_writes`: Number of pending writes
- `current_wal_size`: Current WAL file size in bytes
- `current_wal_count`: Number of records in current WAL
- `wal_files_count`: Total number of WAL files
- `wal_sequence`: Current WAL sequence number

#### `close()`

Clean shutdown - stops background threads and flushes all pending data.

## Example: Running the Demo

```bash
python fast_persist_claude.py
```

This runs a demo that:
1. Writes 250 random records
2. Shows statistics during writing
3. Forces a flush
4. Restarts to test recovery
5. Verifies recovered data

## Thread Safety

The storage system is thread-safe with the following guarantees:

- Writes are protected by `write_lock`
- Flushes are protected by `flush_lock`
- Safe for concurrent reads and writes
- Background flush thread runs safely alongside user operations

## Performance Characteristics

- **Write latency**: ~1-5ms (WAL append only)
- **Read latency**: ~0.1ms (in-memory cache)
- **Batch flush**: Processes 1000s of records per second
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

## License

See project license file.

## Contributing

Contributions welcome. Please follow PEP 8 and existing code style.
