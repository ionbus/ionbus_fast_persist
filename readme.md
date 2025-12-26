# Fast Persist

High-performance Python persistence layers combining Write-Ahead Logs (WAL) with DuckDB for fast asynchronous writes and reliable storage.

<!-- TOC start (generated with https://github.com/derlin/bitdowntoc) -->

- [Choose Your Storage Model](#choose-your-storage-model)
   * [📅 dated_fast_persist](#-dated_fast_persist)
   * [📦 collection_fast_persist](#-collection_fast_persist)
- [Quick Comparison](#quick-comparison)
- [Common Features](#common-features)
- [Installation](#installation)
- [Architecture](#architecture)
- [Decision Guide](#decision-guide)
   * [Choose `dated_fast_persist` if you:](#choose-dated_fast_persist-if-you)
   * [Choose `collection_fast_persist` if you:](#choose-collection_fast_persist-if-you)
   * [Still unsure?](#still-unsure)
- [Testing](#testing)
- [Performance](#performance)
- [Thread Safety](#thread-safety)
- [License](#license)
- [Contributing](#contributing)
- [Documentation](#documentation)

<!-- TOC end -->

## Choose Your Storage Model

This repository provides two persistence solutions, each optimized for different use cases:

### [📅 dated_fast_persist](dated_fast_persist.md)

**Date-based storage isolation** - Each date gets its own isolated storage

**Best for:**
- Time-series data with date-based access patterns
- Daily processing jobs that need isolated storage per day
- Applications that query data by date ranges
- FastAPI endpoints processing date-specific requests
- Analytics pipelines that process data day-by-day
- Concurrent multi-date processing

**Key Features:**
- Date isolation (separate subdirectories per date, configurable database paths)
- Multi-process tracking (key → process_name → data)
- Automatic Parquet export for analytics
- Each date can run independently

**Example:**
```python
from dated_fast_persist import WALDuckDBStorage, WALConfig
import datetime as dt

storage = WALDuckDBStorage(dt.date.today(), "data.duckdb")
storage.store("task_1", {"value": 100}, process_name="worker1")
```

---

### [📦 collection_fast_persist](collection_fast_persist.md)

**Collection-based organization** - Group related items into collections

**Best for:**
- Form data (group all fields in a form)
- Settings/configuration (organize by category)
- Seat assignments (classroom/venue management)
- Inventory systems (organize by category/location)
- User preferences (group related settings)
- Any hierarchical data organization

**Key Features:**
- Three-level hierarchy (key → collection → item)
- Typed values (automatic int/float/string column routing)
- Dual-table architecture (history + latest values)
- Database health checks
- Automatic backups with retention
- Lazy collection loading

**Example:**
```python
from collection_fast_persist import CollectionFastPersist, CollectionConfig
import datetime as dt

storage = CollectionFastPersist(dt.date.today())
storage.store(
    key="user_profile",
    item_name="age",
    collection_name="personal_info",
    data={"label": "Age"},
    value=32  # Typed value (int)
)
```

---

## Quick Comparison

| Feature | dated_fast_persist | collection_fast_persist |
|---------|-------------------|------------------------|
| **Organization** | Date-based isolation | Collection-based hierarchy |
| **Hierarchy** | key → process_name → data | key → collection → item → data |
| **Date Usage** | Data isolation by date | Backup organization only |
| **Tables** | Single table | Dual tables (history + latest) |
| **Typed Values** | No (JSON storage) | Yes (int/float/string columns) |
| **Concurrent Dates** | Yes (isolated per date) | N/A (all dates share DBs) |
| **Latest Values** | No (query from single table) | Yes (optimized latest table) |
| **Parquet Export** | Yes (Hive-partitioned) | No |
| **Database Health** | No | Yes (automatic + manual) |
| **Backups** | No | Yes (daily with retention) |

## Common Features

Both solutions share these core capabilities:

✅ **Fast async writes** via Write-Ahead Logs (WAL)

✅ **Reliable persistence** using DuckDB

✅ **Automatic crash recovery** from WAL files

✅ **In-memory caching** for quick reads

✅ **Background batch processing** for database writes

✅ **Thread-safe** with proper locking

✅ **Configurable** WAL rotation and flush intervals

✅ **Special field support** (timestamp, status, username)

## Installation

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

## Architecture

Both solutions use a hybrid WAL + DuckDB architecture:

```
┌─────────────┐
│   Write     │
└──────┬──────┘
       │
       ▼
┌─────────────────┐   fsync   ┌──────────────┐
│  In-Memory      │──────────>│  WAL Files   │
│  Cache          │           │  (*.jsonl)   │
└─────────────────┘           └──────────────┘
       │
       │ (background batching)
       ▼
┌─────────────────┐
│    DuckDB       │
│   (persistent)  │
└─────────────────┘
```

**Write Path:**
1. Data written to in-memory cache (instant)
2. Appended to WAL file with fsync (~1-2ms)
3. Background thread batches writes to DuckDB
4. WAL files cleaned up after successful flush

**Read Path:**
1. Serve from in-memory cache (~0.1ms)
2. Cache populated from DuckDB on startup
3. Collections lazy-loaded as needed (collection_fast_persist only)

**Recovery:**
1. On startup, check for existing WAL files
2. If found, replay into DuckDB
3. Update in-memory cache
4. Delete processed WAL files

## Decision Guide

### Choose `dated_fast_persist` if you:
- Access data primarily by date
- Need isolated storage per date
- Want Parquet export for analytics
- Run multiple dates concurrently
- Have date-driven business logic

### Choose `collection_fast_persist` if you:
- Need hierarchical data organization
- Want typed value storage (int/float/string)
- Need latest-value queries (separate from history)
- Want automatic database health checks
- Need backup/restore capabilities
- Organize data by logical groups (not dates)

### Still unsure?
If your data model includes:
- "Today's metrics", "Yesterday's sales" → **dated_fast_persist**
- "User settings", "Form fields", "Seat assignments" → **collection_fast_persist**

## Testing

**Run all tests:**
```bash
python run_all_tests.py
```

This master test runner executes all 15 test suites sequentially and provides a comprehensive summary report.

**Run individual tests:**

Each module includes comprehensive tests:

**dated_fast_persist:**
```bash
python test_fast_persist.py                 # Basic functionality
python test_data_persistence.py             # Data persistence (no DROP TABLE)
python test_timestamp_username_params.py    # Timestamp/username parameters
python test_wal_metadata_recovery.py        # WAL metadata recovery
python test_process_name_none.py            # process_name=None preservation
python test_crash_recovery.py 1             # Crash simulation
python test_crash_recovery.py 2             # Recovery verification
```

**collection_fast_persist:**
```bash
python test_collection_fast_persist.py      # Full test suite
python test_history_retention.py            # History retention
python test_collection_crash_recovery.py 1  # Crash simulation
python test_collection_crash_recovery.py 2  # Recovery verification
python test_collection_crash_recovery.py 3  # Manual reconstruction
```

**Shared utilities:**
```bash
python test_parse_timestamp.py              # Timezone handling
```

**Cleaning up test artifacts:**

Tests create storage directories that persist between runs. Use the cleanup script:

```bash
python test_cleanup.py
```

Or manually remove directories:
```bash
# Unix/Linux/Mac:
rm -rf ./wal_storage ./test_output ./crash_test_output
rm -rf ./collection_test_storage ./crash_test_collection

# Windows:
rmdir /s /q wal_storage test_output crash_test_output
rmdir /s /q collection_test_storage crash_test_collection
```

## Performance

Both solutions offer similar performance characteristics:

- **Write latency**: ~1-2ms (WAL append + fsync)
- **Read latency**: ~0.1ms (in-memory cache)
- **Batch flush**: Background, non-blocking
- **Memory usage**: Proportional to cached data size

## Thread Safety

- ✅ Thread-safe within single process
- ✅ Write operations protected by locks
- ✅ Safe for concurrent readers
- ⚠️ Not designed for multi-process access (use process-level coordination)

## License

MIT License

## Contributing

Contributions welcome! Please ensure tests pass before submitting PRs.

---

## Documentation

- **[dated_fast_persist.md](dated_fast_persist.md)** - Complete API reference, examples, and architecture
- **[collection_fast_persist.md](collection_fast_persist.md)** - Complete API reference, examples, and architecture
