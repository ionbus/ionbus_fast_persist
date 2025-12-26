"""Collection Fast Persist - Collection-based persistent storage using DuckDB and Write-Ahead Logs (WAL)"""

from __future__ import annotations

import datetime as dt
import duckdb
import json
import os
import shutil
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from fast_persist_common import StorageKeys, parse_timestamp, setup_logger

logger = setup_logger("collection_fast_persist")


class CollectionFastPersistError(Exception):
    """Custom exception for collection_fast_persist errors"""

    pass


@dataclass
class CollectionConfig:
    """Configuration for collection storage behavior"""

    base_dir: str = "./collection_storage"
    max_wal_size: int = 10 * 1024 * 1024  # 10MB
    batch_size: int = 1000
    duckdb_flush_interval_seconds: int = 30
    retain_days: int = 5  # Keep 5 days of backups


class CollectionFastPersist:
    """
    Collection-based storage system using Write-Ahead Logs for fast async
    writes and DuckDB for persistent storage with dual-table architecture.

    Unlike dated_fast_persist, this uses collection/item organization
    and stores all dates' data in the same database files.
    """

    def __init__(
        self,
        date: dt.date | dt.datetime | str,
        base_dir: str = "./collection_storage",
        config: CollectionConfig | None = None,
    ):
        """Initialize collection storage.

        Args:
            date: Date for backup/WAL organization (NOT for data isolation)
            base_dir: Base directory for storage files
            config: Configuration object with retention settings
        """
        # Convert date to isoformat date string (YYYY-MM-DD)
        if isinstance(date, str):
            # Parse ISO format string to datetime
            date_obj = dt.datetime.fromisoformat(date.split("T")[0])
            self.date_str = date_obj.date().isoformat()
        elif isinstance(date, dt.datetime):
            self.date_str = date.date().isoformat()
        else:  # dt.date
            self.date_str = date.isoformat()

        # Set up configuration
        self.config = config or CollectionConfig()
        # Use config's base_dir if provided, otherwise use parameter
        if config and config.base_dir:
            self.base_dir = Path(config.base_dir)
        else:
            self.base_dir = Path(base_dir)
            self.config.base_dir = base_dir

        # Database paths (in base_dir, NOT date-specific)
        self.history_db_path = self.base_dir / "storage_history.duckdb"
        self.latest_db_path = self.base_dir / "storage_latest.duckdb"

        # Lock file path (in base_dir)
        self.lock_file_path = self.base_dir / f".lock_{self.date_str}"

        # Date-specific WAL directory
        self.wal_dir = self.base_dir / self.date_str
        self.wal_dir.mkdir(parents=True, exist_ok=True)

        # Threading primitives
        self.write_lock = threading.Lock()
        self.flush_lock = threading.Lock()
        self.stop_event = threading.Event()

        # In-memory cache: cache[key][collection_name][item_name] = {...}
        self.cache: dict[str, dict[str, dict[str, dict[str, Any]]]] = {}

        # Pending writes with same structure
        self.pending_writes: dict[
            str, dict[str, dict[str, list[dict[str, Any]]]]
        ] = {}

        # Change tracking: set of (key, collection_name, item_name) tuples
        self.modified_records: set[tuple[str, str, str]] = set()

        # WAL management
        self.current_wal = None
        self.current_wal_file = None
        self.current_wal_size = 0
        self.current_wal_count = 0
        self.current_wal_start_time = None
        self.wal_sequence = 0

        # Lock file for single instance enforcement
        self._acquire_lock()

        # Create base directory
        self.base_dir.mkdir(parents=True, exist_ok=True)

        # Check if this is a restart scenario
        self.is_restart = self.wal_dir.exists() and any(
            self.wal_dir.glob("wal_*.jsonl")
        )

        # Initialize DuckDB connections
        self._init_duckdb()

        # Recover from any existing WAL files if restart
        if self.is_restart:
            self._recover_from_wal()

        # Start background flush thread
        self.flush_thread = threading.Thread(
            target=self._background_flush, daemon=True
        )
        self.flush_thread.start()

    def _acquire_lock(self):
        """Acquire lock file to enforce single instance per date.

        Automatically cleans up stale locks from crashed processes.
        """
        if self.lock_file_path.exists():
            # Check if lock is stale (older than 5 seconds + has WAL files)
            # A fresh lock suggests another process just started
            try:
                lock_age = time.time() - self.lock_file_path.stat().st_mtime
                has_wal = bool(self._get_wal_files())

                # If lock is fresh (< 2 seconds old), another instance running
                if lock_age < 2:
                    raise CollectionFastPersistError(
                        f"Another instance is already running for date "
                        f"{self.date_str}. "
                        f"Lock file exists at {self.lock_file_path}"
                    )

                # Stale lock detected - check if crash recovery needed
                if has_wal:
                    logger.warning(
                        f"Stale lock detected (age: {lock_age:.1f}s) "
                        f"with WAL files - likely crashed process. "
                        f"Removing stale lock and will recover from WAL."
                    )
                else:
                    logger.warning(
                        f"Stale lock detected (age: {lock_age:.1f}s) "
                        f"with no WAL files. Removing stale lock."
                    )

                # Remove stale lock
                self.lock_file_path.unlink()

            except CollectionFastPersistError:
                # Re-raise instance violation
                raise
            except Exception as e:
                logger.error(f"Error checking lock file: {e}")
                raise CollectionFastPersistError(
                    f"Failed to check lock file: {e}"
                )

        # Create lock file
        try:
            self.lock_file_path.touch()
            logger.info(f"Acquired lock for date {self.date_str}")
        except Exception as e:
            raise CollectionFastPersistError(
                f"Failed to create lock file: {e}"
            )

    def _release_lock(self):
        """Release lock file."""
        try:
            if self.lock_file_path.exists():
                self.lock_file_path.unlink()
                logger.info(f"Released lock for date {self.date_str}")
        except Exception as e:
            logger.error(f"Error releasing lock file: {e}")

    def check_database_health(
        self, db_path: Path, table_name: str, conn=None
    ) -> bool:
        """Check if database file is readable and not corrupted.

        Args:
            db_path: Path to DuckDB file
            table_name: Name of table to verify
            conn: Optional existing connection to use (avoids read-only
                  conflict)

        Returns:
            True if healthy, False if corrupted
        """
        if not db_path.exists():
            # File doesn't exist yet - that's ok for initial setup
            return True

        try:
            # If connection provided, use it (already open)
            if conn is not None:
                conn.execute(
                    f"SELECT COUNT(*) FROM {table_name}"
                ).fetchone()
                conn.execute("PRAGMA database_size").fetchone()
                return True

            # Otherwise try read-only connection
            test_conn = duckdb.connect(str(db_path), read_only=True)
            test_conn.execute(
                f"SELECT COUNT(*) FROM {table_name}"
            ).fetchone()
            test_conn.execute("PRAGMA database_size").fetchone()
            test_conn.close()
            return True
        except Exception as e:
            logger.error(
                f"Database health check failed for {db_path}: {e}"
            )
            return False

    def _init_duckdb(self):
        """Initialize DuckDB connections and schema for both databases."""
        # Check health of both databases before opening
        history_healthy = self.check_database_health(
            self.history_db_path, "storage_history"
        )
        latest_healthy = self.check_database_health(
            self.latest_db_path, "storage_latest"
        )

        if not history_healthy:
            raise CollectionFastPersistError(
                f"Failed to open {self.history_db_path}.\n"
                "Database may be corrupted. To recover:\n"
                "1. Delete the corrupted file\n"
                "2. Call rebuild_history_from_wal(date) for each date\n"
                "   that needs recovery\n"
                "3. Call rebuild_latest_from_history() to rebuild "
                "latest values"
            )

        if not latest_healthy:
            raise CollectionFastPersistError(
                f"Failed to open {self.latest_db_path}.\n"
                "Database may be corrupted. To recover:\n"
                "1. Delete the corrupted file\n"
                "2. Call rebuild_latest_from_history() to rebuild from "
                "history"
            )

        # Initialize history database
        self.history_conn = duckdb.connect(str(self.history_db_path))
        self.history_conn.execute("""
            CREATE TABLE IF NOT EXISTS storage_history (
                key VARCHAR NOT NULL,
                collection_name VARCHAR NOT NULL DEFAULT '',
                item_name VARCHAR NOT NULL DEFAULT '',
                data JSON,
                value_int BIGINT,
                value_float DOUBLE,
                value_string VARCHAR,
                timestamp TIMESTAMP,
                status VARCHAR,
                status_int INTEGER,
                username VARCHAR,
                updated_at TIMESTAMP,
                version INTEGER DEFAULT 1
            )
        """)

        # Initialize latest values database
        self.latest_conn = duckdb.connect(str(self.latest_db_path))
        self.latest_conn.execute("""
            CREATE TABLE IF NOT EXISTS storage_latest (
                key VARCHAR NOT NULL,
                collection_name VARCHAR NOT NULL DEFAULT '',
                item_name VARCHAR NOT NULL DEFAULT '',
                data JSON,
                value_int BIGINT,
                value_float DOUBLE,
                value_string VARCHAR,
                timestamp TIMESTAMP,
                status VARCHAR,
                status_int INTEGER,
                username VARCHAR,
                updated_at TIMESTAMP,
                version INTEGER DEFAULT 1,
                PRIMARY KEY (key, collection_name, item_name)
            )
        """)

        logger.info("Initialized DuckDB databases (history and latest)")

    def _load_collection(self, key: str, collection_name: str):
        """Load entire collection from latest database into cache.

        Args:
            key: The key to load
            collection_name: The collection to load
        """
        # Check if already loaded
        if key in self.cache and collection_name in self.cache[key]:
            return

        # Query latest database for this key/collection
        result = self.latest_conn.execute(
            """
            SELECT item_name, data, value_int, value_float, value_string,
                   timestamp, status, status_int, username
            FROM storage_latest
            WHERE key = ? AND collection_name = ?
            """,
            [key, collection_name],
        ).fetchall()

        # Initialize cache structure
        if key not in self.cache:
            self.cache[key] = {}
        if collection_name not in self.cache[key]:
            self.cache[key][collection_name] = {}

        # Load all items in collection
        for row in result:
            (
                item_name,
                data,
                value_int,
                value_float,
                value_string,
                timestamp,
                status,
                status_int,
                username,
            ) = row

            # Deserialize JSON data
            if isinstance(data, str):
                data_dict = json.loads(data)
            else:
                data_dict = data

            # Determine value type and extract
            if value_int is not None:
                value = value_int
            elif value_float is not None:
                value = value_float
            elif value_string is not None:
                value = value_string
            else:
                value = None

            # Store in cache with value field
            data_dict["value"] = value
            if timestamp is not None:
                data_dict[StorageKeys.TIMESTAMP] = timestamp
            if status is not None:
                data_dict[StorageKeys.STATUS] = status
            if status_int is not None:
                data_dict[StorageKeys.STATUS_INT] = status_int
            if username is not None:
                data_dict[StorageKeys.USERNAME] = username

            self.cache[key][collection_name][item_name] = data_dict

        if result:
            logger.info(
                f"Loaded collection {key}/{collection_name} "
                f"with {len(result)} items"
            )

    def _get_wal_files(self):
        """Get all WAL files sorted by sequence number"""
        wal_files = sorted(
            self.wal_dir.glob("wal_*.jsonl"),
            key=lambda p: int(p.stem.split("_")[1]),
        )
        return wal_files

    def _recover_from_wal(self):
        """Recover pending data from existing WAL files"""
        wal_files = self._get_wal_files()

        if not wal_files:
            logger.info("No WAL files found for recovery")
            return

        logger.info(f"Found {len(wal_files)} WAL files to recover")

        for wal_file in wal_files:
            records = []
            try:
                with open(wal_file, "r") as f:
                    for line in f:
                        if line.strip():
                            record = json.loads(line)
                            records.append(record)

                            # Extract fields
                            key = record["key"]
                            collection_name = record.get("collection_name", "")
                            item_name = record.get("item_name", "")
                            data = record["data"]
                            value = record.get("value")

                            # Update nested in-memory cache
                            if key not in self.cache:
                                self.cache[key] = {}
                            if collection_name not in self.cache[key]:
                                self.cache[key][collection_name] = {}

                            # Add value to data
                            data["value"] = value
                            self.cache[key][collection_name][item_name] = data

                            # Update nested pending_writes (append to list)
                            if key not in self.pending_writes:
                                self.pending_writes[key] = {}
                            if (
                                collection_name
                                not in self.pending_writes[key]
                            ):
                                self.pending_writes[key][collection_name] = {}
                            if (
                                item_name
                                not in self.pending_writes[key][collection_name]
                            ):
                                self.pending_writes[key][collection_name][
                                    item_name
                                ] = []
                            # Append this update to the list
                            self.pending_writes[key][collection_name][
                                item_name
                            ].append(data)

                            # Track modification
                            self.modified_records.add(
                                (key, collection_name, item_name)
                            )

                if records:
                    logger.info(
                        f"Recovered {len(records)} records from "
                        f"{wal_file.name}"
                    )
            except Exception as e:
                logger.error(f"Error recovering from {wal_file}: {e}")

        # Flush all recovered data to history DB
        if self.pending_writes:
            self._flush_to_duckdb()
            logger.info("Flushed all recovered data to DuckDB")

    def _rotate_wal(self):
        """Rotate to a new WAL file"""
        if self.current_wal_file:
            self.current_wal_file.flush()
            os.fsync(self.current_wal_file.fileno())
            self.current_wal_file.close()

        self.wal_sequence += 1
        wal_path = self.wal_dir / f"wal_{self.wal_sequence:06d}.jsonl"
        self.current_wal = wal_path
        self.current_wal_file = open(wal_path, "a")
        self.current_wal_size = 0
        self.current_wal_count = 0
        self.current_wal_start_time = time.time()

        logger.info(f"Rotated to new WAL file: {wal_path.name}")

    def store(
        self,
        key: str,
        data: dict[str, Any],
        item_name: str | None = None,
        collection_name: str | None = None,
        value: int | float | str | None = None,
        timestamp: str | dt.datetime | None = None,
        username: str | None = None,
    ):
        """Store data with async write to WAL.

        Args:
            key: Unique identifier for the data
            data: Dictionary to store (keeps all fields)
            item_name: Item identifier (default: "")
            collection_name: Collection identifier (default: "")
            value: Optional typed value (int/float/str/None)
            timestamp: Timestamp (extracted from data if None)
            username: Username (extracted from data if None)
        """
        with self.write_lock:
            # Set defaults
            if item_name is None:
                item_name = ""
            if collection_name is None:
                collection_name = ""

            # Extract timestamp: parameter > data field > current time
            if timestamp is None:
                timestamp = data.get(StorageKeys.TIMESTAMP)
            if timestamp is None:
                timestamp = dt.datetime.now().isoformat()

            # Extract username: parameter > data field > None
            if username is None:
                username = data.get(StorageKeys.USERNAME)

            # Load collection if not already in cache
            self._load_collection(key, collection_name)

            # Add value, timestamp, and username to data dict (for cache)
            data_with_value = dict(data)
            data_with_value["value"] = value
            data_with_value[StorageKeys.TIMESTAMP] = timestamp
            if username is not None:
                data_with_value[StorageKeys.USERNAME] = username

            # Update nested in-memory cache
            if key not in self.cache:
                self.cache[key] = {}
            if collection_name not in self.cache[key]:
                self.cache[key][collection_name] = {}
            self.cache[key][collection_name][item_name] = data_with_value

            # Update nested pending_writes (append to list for full history)
            if key not in self.pending_writes:
                self.pending_writes[key] = {}
            if collection_name not in self.pending_writes[key]:
                self.pending_writes[key][collection_name] = {}
            if item_name not in self.pending_writes[key][collection_name]:
                self.pending_writes[key][collection_name][item_name] = []
            # Append this update to the list
            self.pending_writes[key][collection_name][item_name].append(
                data_with_value
            )

            # Track modification
            self.modified_records.add((key, collection_name, item_name))

            # Initialize WAL if needed
            if self.current_wal_file is None:
                self._rotate_wal()

            # Write to WAL (value stored in native JSON type)
            record = {
                "key": key,
                "collection_name": collection_name,
                "item_name": item_name,
                "data": data,
                "value": value,  # Native type preserved in JSON
                "username": username,
                "timestamp": timestamp,
            }
            wal_entry = json.dumps(record) + "\n"
            wal_bytes = wal_entry.encode("utf-8")

            if not self.current_wal_file:
                raise RuntimeError("WAL file is not initialized")
            self.current_wal_file.write(wal_entry)
            self.current_wal_file.flush()
            os.fsync(self.current_wal_file.fileno())
            self.current_wal_size += len(wal_bytes)
            self.current_wal_count += 1

            # Check if we need to rotate WAL or flush to DB
            should_rotate = (
                self.current_wal_size >= self.config.max_wal_size
                or self.current_wal_count >= self.config.batch_size
            )

            if should_rotate:
                self._rotate_wal()
                # Trigger flush in background
                threading.Thread(
                    target=self._flush_to_duckdb, daemon=True
                ).start()

    def get_key(
        self, key: str, collection_name: str | None = None
    ) -> dict[str, dict[str, dict[str, Any]]] | dict[str, dict[str, Any]] | None:
        """Get data for a given key.

        Args:
            key: The key to look up
            collection_name: Optional collection filter

        Returns:
            If collection_name is None: dict mapping collection_name to
                dict mapping item_name to data
            If collection_name is provided: dict mapping item_name to data
            None if key doesn't exist
        """
        with self.write_lock:
            if key not in self.cache:
                return None

            if collection_name is not None:
                # Load collection if needed
                self._load_collection(key, collection_name)
                return self.cache[key].get(collection_name)

            return self.cache.get(key)

    def get_item(
        self,
        key: str,
        collection_name: str,
        item_name: str,
    ) -> dict[str, Any] | None:
        """Get data for specific key/collection/item combination.

        Args:
            key: The key to look up
            collection_name: The collection name
            item_name: The item name

        Returns:
            Data dictionary if found, None otherwise
        """
        with self.write_lock:
            # Load collection if needed
            self._load_collection(key, collection_name)

            if key not in self.cache:
                return None
            if collection_name not in self.cache[key]:
                return None
            return self.cache[key][collection_name].get(item_name)

    def _flush_to_duckdb(self):
        """Flush pending writes to history DuckDB and clean up old WAL files"""
        # Copy pending writes under write_lock
        with self.write_lock:
            if not self.pending_writes:
                return
            # Deep copy nested structure
            writes_to_flush = {}
            for key, coll_dict in self.pending_writes.items():
                writes_to_flush[key] = {}
                for coll_name, item_dict in coll_dict.items():
                    writes_to_flush[key][coll_name] = dict(item_dict)
            self.pending_writes.clear()

        with self.flush_lock:
            try:
                # Prepare batch data for history table
                batch_data = []
                for key, coll_dict in writes_to_flush.items():
                    for coll_name, item_dict in coll_dict.items():
                        for item_name, data_list in item_dict.items():
                            # Get current max version for this item ONCE
                            result = self.history_conn.execute(
                                """
                                SELECT COALESCE(MAX(version), 0)
                                FROM storage_history
                                WHERE key = ? AND collection_name = ?
                                AND item_name = ?
                                """,
                                [key, coll_name, item_name],
                            ).fetchone()
                            current_version = result[0] if result else 0

                            # Process each update in the list
                            for data in data_list:
                                # Extract special fields
                                timestamp = parse_timestamp(
                                    data.get(StorageKeys.TIMESTAMP)
                                )
                                status = data.get(StorageKeys.STATUS)
                                status_int = data.get(StorageKeys.STATUS_INT)
                                username = data.get(StorageKeys.USERNAME)
                                value = data.get("value")

                                # Determine value column routing
                                value_int = None
                                value_float = None
                                value_string = None

                                if isinstance(value, int):
                                    value_int = value
                                elif isinstance(value, float):
                                    value_float = value
                                elif isinstance(value, str):
                                    value_string = value

                                # Remove value from data before storing as JSON
                                data_without_value = {
                                    k: v
                                    for k, v in data.items()
                                    if k != "value"
                                }

                                # Increment version for each update
                                current_version += 1

                                batch_data.append(
                                    (
                                        key,
                                        coll_name,
                                        item_name,
                                        json.dumps(data_without_value),
                                        value_int,
                                        value_float,
                                        value_string,
                                        timestamp,
                                        status,
                                        status_int,
                                        username,
                                        dt.datetime.now(),
                                        current_version,
                                    )
                                )

                # Batch insert to history DB (append all versions)
                self.history_conn.execute("BEGIN TRANSACTION")

                self.history_conn.executemany(
                    """
                    INSERT INTO storage_history
                    (key, collection_name, item_name, data,
                     value_int, value_float, value_string,
                     timestamp, status, status_int, username,
                     updated_at, version)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    batch_data,
                )

                self.history_conn.execute("COMMIT")

                logger.info(
                    f"Flushed {len(batch_data)} records to history DB"
                )

                # Clean up old WAL files
                self._cleanup_old_wals()

            except Exception as e:
                logger.error(f"Error flushing to DuckDB: {e}")
                self.history_conn.execute("ROLLBACK")
                # Restore failed writes
                with self.write_lock:
                    for key, coll_dict in writes_to_flush.items():
                        if key not in self.pending_writes:
                            self.pending_writes[key] = {}
                        for coll_name, item_dict in coll_dict.items():
                            if coll_name not in self.pending_writes[key]:
                                self.pending_writes[key][coll_name] = {}
                            self.pending_writes[key][coll_name].update(
                                item_dict
                            )

    def _update_latest_table(self):
        """Update latest values table with all modified records."""
        if not self.modified_records:
            logger.info("No modified records to update in latest table")
            return

        try:
            batch_data = []

            for key, collection_name, item_name in self.modified_records:
                # Get data from cache
                if key not in self.cache:
                    continue
                if collection_name not in self.cache[key]:
                    continue
                if item_name not in self.cache[key][collection_name]:
                    continue

                data = self.cache[key][collection_name][item_name]

                # Extract special fields
                timestamp = parse_timestamp(
                    data.get(StorageKeys.TIMESTAMP)
                )
                status = data.get(StorageKeys.STATUS)
                status_int = data.get(StorageKeys.STATUS_INT)
                username = data.get(StorageKeys.USERNAME)
                value = data.get("value")

                # Determine value column routing
                value_int = None
                value_float = None
                value_string = None

                if isinstance(value, int):
                    value_int = value
                elif isinstance(value, float):
                    value_float = value
                elif isinstance(value, str):
                    value_string = value

                # Remove value from data before storing as JSON
                data_without_value = {
                    k: v for k, v in data.items() if k != "value"
                }

                batch_data.append(
                    (
                        key,
                        collection_name,
                        item_name,
                        json.dumps(data_without_value),
                        value_int,
                        value_float,
                        value_string,
                        timestamp,
                        status,
                        status_int,
                        username,
                        dt.datetime.now(),
                        1,  # version always 1 in latest table
                    )
                )

            # Batch update latest DB
            self.latest_conn.execute("BEGIN TRANSACTION")

            self.latest_conn.executemany(
                """
                INSERT OR REPLACE INTO storage_latest
                (key, collection_name, item_name, data,
                 value_int, value_float, value_string,
                 timestamp, status, status_int, username,
                 updated_at, version)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                batch_data,
            )

            self.latest_conn.execute("COMMIT")

            logger.info(
                f"Updated {len(batch_data)} records in latest table"
            )

            # Clear modified records tracking
            self.modified_records.clear()

        except Exception as e:
            logger.error(f"Error updating latest table: {e}")
            self.latest_conn.execute("ROLLBACK")

    def _cleanup_old_wals(self):
        """Remove WAL files that have been successfully persisted"""
        wal_files = self._get_wal_files()

        for wal_file in wal_files:
            # Don't delete the current WAL file
            if self.current_wal and wal_file == self.current_wal:
                continue

            try:
                wal_file.unlink()
                logger.info(f"Deleted processed WAL file: {wal_file.name}")
            except Exception as e:
                logger.error(f"Error deleting WAL file {wal_file}: {e}")

    def _cleanup_old_date_directories(self):
        """Remove date directories older than retain_days."""
        try:
            # Calculate cutoff date
            cutoff_date = dt.date.today() - dt.timedelta(
                days=self.config.retain_days
            )

            # Find all date directories
            for dir_path in self.base_dir.iterdir():
                if not dir_path.is_dir():
                    continue

                # Try to parse directory name as date
                try:
                    dir_date = dt.date.fromisoformat(dir_path.name)
                    if dir_date < cutoff_date:
                        # Delete entire directory
                        shutil.rmtree(dir_path)
                        logger.info(
                            f"Deleted old date directory: {dir_path.name}"
                        )
                except (ValueError, OSError):
                    # Not a date directory, skip
                    continue

        except Exception as e:
            logger.error(f"Error cleaning up old directories: {e}")

    def _background_flush(self):
        """Background thread to periodically flush to DuckDB"""
        while not self.stop_event.is_set():
            time.sleep(self.config.duckdb_flush_interval_seconds)

            with self.write_lock:
                if self.pending_writes:
                    # Rotate WAL before flushing
                    if self.current_wal_file:
                        self._rotate_wal()

            self._flush_to_duckdb()

    def _backup_databases(self):
        """Copy database files to date-specific backup location."""
        try:
            backup_dir = self.wal_dir
            backup_dir.mkdir(parents=True, exist_ok=True)

            # Copy history database
            if self.history_db_path.exists():
                backup_path = backup_dir / "storage_history.duckdb.backup"
                shutil.copy2(self.history_db_path, backup_path)
                logger.info(f"Backed up history DB to {backup_path}")

            # Copy latest database
            if self.latest_db_path.exists():
                backup_path = backup_dir / "storage_latest.duckdb.backup"
                shutil.copy2(self.latest_db_path, backup_path)
                logger.info(f"Backed up latest DB to {backup_path}")

        except Exception as e:
            logger.error(f"Error backing up databases: {e}")

    def flush_data_to_duckdb(self):
        """Force an immediate flush of pending data to DuckDB"""
        with self.write_lock:
            if self.current_wal_file:
                self._rotate_wal()

        self._flush_to_duckdb()

    def get_stats(self) -> dict[str, Any]:
        """Get storage statistics"""
        with self.write_lock:
            wal_files = self._get_wal_files()

            return {
                "cache_size": len(self.cache),
                "pending_writes": len(self.pending_writes),
                "modified_records": len(self.modified_records),
                "current_wal_size": self.current_wal_size,
                "current_wal_count": self.current_wal_count,
                "wal_files_count": len(wal_files),
                "wal_sequence": self.wal_sequence,
            }

    def rebuild_history_from_wal(
        self, date: dt.date | dt.datetime | str
    ) -> int:
        """Reconstruct history database from WAL files for specific date.

        Args:
            date: The date directory to read WAL files from

        Returns:
            Number of records recovered
        """
        # Convert date to string
        if isinstance(date, str):
            date_str = date.split("T")[0]
        elif isinstance(date, dt.datetime):
            date_str = date.date().isoformat()
        else:
            date_str = date.isoformat()

        wal_dir = self.base_dir / date_str
        if not wal_dir.exists():
            logger.warning(f"No WAL directory found for {date_str}")
            return 0

        wal_files = sorted(
            wal_dir.glob("wal_*.jsonl"),
            key=lambda p: int(p.stem.split("_")[1]),
        )

        if not wal_files:
            logger.warning(f"No WAL files found in {date_str}")
            return 0

        logger.info(
            f"Rebuilding history from {len(wal_files)} WAL files "
            f"in {date_str}"
        )

        records_recovered = 0

        for wal_file in wal_files:
            try:
                with open(wal_file, "r") as f:
                    for line in f:
                        if not line.strip():
                            continue

                        record = json.loads(line)

                        # Extract fields
                        key = record["key"]
                        collection_name = record.get("collection_name", "")
                        item_name = record.get("item_name", "")
                        data = record["data"]
                        value = record.get("value")
                        username = record.get("username")
                        timestamp = parse_timestamp(
                            record.get("timestamp")
                        )

                        # Determine value columns
                        value_int = None
                        value_float = None
                        value_string = None

                        if isinstance(value, int):
                            value_int = value
                        elif isinstance(value, float):
                            value_float = value
                        elif isinstance(value, str):
                            value_string = value

                        # Get current max version for this item
                        result = self.history_conn.execute(
                            """
                            SELECT COALESCE(MAX(version), 0)
                            FROM storage_history
                            WHERE key = ? AND collection_name = ?
                            AND item_name = ?
                            """,
                            [key, collection_name, item_name],
                        ).fetchone()
                        current_version = result[0] if result else 0

                        # Insert into history DB (append new version)
                        self.history_conn.execute(
                            """
                            INSERT INTO storage_history
                            (key, collection_name, item_name, data,
                             value_int, value_float, value_string,
                             timestamp, status, status_int, username,
                             updated_at, version)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                            """,
                            (
                                key,
                                collection_name,
                                item_name,
                                json.dumps(data),
                                value_int,
                                value_float,
                                value_string,
                                timestamp,
                                data.get(StorageKeys.STATUS),
                                data.get(StorageKeys.STATUS_INT),
                                username,
                                dt.datetime.now(),
                                current_version + 1,
                            ),
                        )

                        # Update cache
                        if key not in self.cache:
                            self.cache[key] = {}
                        if collection_name not in self.cache[key]:
                            self.cache[key][collection_name] = {}

                        data_with_value = dict(data)
                        data_with_value["value"] = value
                        self.cache[key][collection_name][
                            item_name
                        ] = data_with_value

                        records_recovered += 1

            except Exception as e:
                logger.error(f"Error recovering from {wal_file}: {e}")

        logger.info(f"Recovered {records_recovered} records from WAL files")
        return records_recovered

    def rebuild_latest_from_history(self) -> int:
        """Reconstruct latest values database from history database.

        Returns:
            Number of latest records created
        """
        logger.info("Rebuilding latest table from history table")

        try:
            # Query history for latest version of each record
            result = self.history_conn.execute("""
                SELECT key, collection_name, item_name, data,
                       value_int, value_float, value_string,
                       timestamp, status, status_int, username,
                       updated_at, version
                FROM storage_history
                WHERE (key, collection_name, item_name, version) IN (
                    SELECT key, collection_name, item_name, MAX(version)
                    FROM storage_history
                    GROUP BY key, collection_name, item_name
                )
            """).fetchall()

            # Clear latest table
            self.latest_conn.execute("DELETE FROM storage_latest")

            # Insert latest values
            batch_data = []
            for row in result:
                (
                    key,
                    collection_name,
                    item_name,
                    data,
                    value_int,
                    value_float,
                    value_string,
                    timestamp,
                    status,
                    status_int,
                    username,
                    updated_at,
                    version,
                ) = row

                batch_data.append(
                    (
                        key,
                        collection_name,
                        item_name,
                        data,
                        value_int,
                        value_float,
                        value_string,
                        timestamp,
                        status,
                        status_int,
                        username,
                        updated_at,
                        1,  # version always 1 in latest
                    )
                )

                # Update cache
                if key not in self.cache:
                    self.cache[key] = {}
                if collection_name not in self.cache[key]:
                    self.cache[key][collection_name] = {}

                # Deserialize and add value
                if isinstance(data, str):
                    data_dict = json.loads(data)
                else:
                    data_dict = data

                # Determine value
                if value_int is not None:
                    value = value_int
                elif value_float is not None:
                    value = value_float
                elif value_string is not None:
                    value = value_string
                else:
                    value = None

                data_dict["value"] = value
                self.cache[key][collection_name][item_name] = data_dict

            self.latest_conn.executemany(
                """
                INSERT INTO storage_latest
                (key, collection_name, item_name, data,
                 value_int, value_float, value_string,
                 timestamp, status, status_int, username,
                 updated_at, version)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                batch_data,
            )

            logger.info(
                f"Rebuilt latest table with {len(batch_data)} records"
            )
            return len(batch_data)

        except Exception as e:
            logger.error(f"Error rebuilding latest table: {e}")
            raise

    def close(self):
        """Clean shutdown"""
        logger.info("Shutting down collection storage system...")

        # Stop background thread
        self.stop_event.set()
        if self.flush_thread.is_alive():
            self.flush_thread.join(timeout=5)

        # Final flush to history DB
        self.flush_data_to_duckdb()

        # Update latest values table
        self._update_latest_table()

        # Close DuckDB connections BEFORE backing up
        # (Windows locks files while they're open)
        self.history_conn.close()
        self.latest_conn.close()

        # Now backup databases to date directory
        self._backup_databases()

        # Close WAL file
        if self.current_wal_file:
            self.current_wal_file.flush()
            os.fsync(self.current_wal_file.fileno())
            self.current_wal_file.close()

        # Clean up all remaining WAL files
        wal_files = self._get_wal_files()
        for wal_file in wal_files:
            try:
                wal_file.unlink()
                logger.info(f"Deleted WAL file on close: {wal_file.name}")
            except Exception as e:
                logger.error(f"Error deleting WAL file {wal_file}: {e}")

        # Clean up old date directories
        self._cleanup_old_date_directories()

        # Release lock
        self._release_lock()

        logger.info("Collection storage system shut down complete")
