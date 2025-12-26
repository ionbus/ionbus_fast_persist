"""Dated Fast Persist - Date-based persistent storage using DuckDB and Write-Ahead Logs (WAL)"""

from __future__ import annotations

import datetime as dt
import duckdb
import json
import os
import pandas as pd
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from fast_persist_common import StorageKeys, parse_timestamp, setup_logger

logger = setup_logger("dated_fast_persist")


@dataclass
class WALConfig:
    """Configuration for WAL behavior"""

    base_dir: str = "./storage"
    max_wal_size: int = 10 * 1024 * 1024  # 10MB per WAL file
    max_wal_age_seconds: int = 300  # 5 minutes
    batch_size: int = 1000  # Records before triggering batch write
    duckdb_flush_interval_seconds: int = 30  # Force flush every 30 seconds
    parquet_path: str | None = None  # Path for Hive-partitioned parquet


class WALDuckDBStorage:
    """
    Hybrid storage system using Write-Ahead Logs for fast async writes
    and DuckDB for persistent storage with batch processing.
    """

    def __init__(
        self,
        date: dt.date | dt.datetime | str,
        db_path: str = "data.duckdb",
        config: WALConfig | None = None,
    ):
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
        self.config = config or WALConfig()

        # Create date-specific directory (don't modify config.base_dir!)
        self.wal_dir = os.path.join(self.config.base_dir, self.date_str)

        # Create date-specific database path (in same dir as WAL files)
        db_name = os.path.basename(db_path)
        self.db_path = os.path.join(self.wal_dir, db_name)

        # Create date-specific storage directories
        Path(self.wal_dir).mkdir(parents=True, exist_ok=True)

        # Threading primitives
        self.write_lock = threading.Lock()
        self.flush_lock = threading.Lock()
        self.stop_event = threading.Event()

        # In-memory cache
        self.cache = {}
        self.pending_writes = {}

        # WAL management
        self.current_wal = None
        self.current_wal_file = None
        self.current_wal_size = 0
        self.current_wal_count = 0
        self.current_wal_start_time = None
        self.wal_sequence = 0

        # Initialize DuckDB
        self._init_duckdb()

        # Recover from any existing WAL files
        self._recover_from_wal()

        # Start background flush thread
        self.flush_thread = threading.Thread(
            target=self._background_flush, daemon=True
        )
        self.flush_thread.start()

    def _init_duckdb(self):
        """Initialize DuckDB connection and schema"""
        self.conn = duckdb.connect(self.db_path)

        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS storage (
                key VARCHAR NOT NULL,
                process_name VARCHAR DEFAULT NULL,
                data JSON,
                timestamp TIMESTAMP,
                status VARCHAR,
                status_int INTEGER,
                username VARCHAR,
                updated_at TIMESTAMP,
                version INTEGER DEFAULT 1,
                UNIQUE (key, process_name)
            )
        """)

        # Migrate existing tables: allow NULL for process_name
        # Check if the table has the old schema (NOT NULL constraint)
        schema_info = self.conn.execute(
            "PRAGMA table_info('storage')"
        ).fetchall()

        # Find process_name column and check if it's NOT NULL
        needs_migration = False
        for col_info in schema_info:
            # col_info format: (cid, name, type, notnull, default_value, pk)
            if col_info[1] == "process_name" and col_info[3] == 1:  # notnull=1
                needs_migration = True
                break

        if needs_migration:
            self.conn.execute("BEGIN TRANSACTION")
            try:
                # Create temporary table with new schema
                self.conn.execute("""
                    CREATE TABLE storage_new (
                        key VARCHAR NOT NULL,
                        process_name VARCHAR DEFAULT NULL,
                        data JSON,
                        timestamp TIMESTAMP,
                        status VARCHAR,
                        status_int INTEGER,
                        username VARCHAR,
                        updated_at TIMESTAMP,
                        version INTEGER DEFAULT 1,
                        UNIQUE (key, process_name)
                    )
                """)
                # Copy data
                self.conn.execute("""
                    INSERT INTO storage_new
                    SELECT * FROM storage
                """)
                # Drop old table and rename new one
                self.conn.execute("DROP TABLE storage")
                self.conn.execute(
                    "ALTER TABLE storage_new RENAME TO storage"
                )
                self.conn.execute("COMMIT")
                logger.info(
                    "Migrated storage table to allow NULL process_name"
                )
            except Exception as e:
                self.conn.execute("ROLLBACK")
                logger.error(f"Migration failed: {e}")
                raise

        # Load existing data into nested cache structure
        result = self.conn.execute(
            "SELECT key, process_name, data FROM storage"
        ).fetchall()
        for key, process_name, data in result:
            # Deserialize JSON string back to dict
            if isinstance(data, str):
                data_dict = json.loads(data)
            else:
                data_dict = data

            # Initialize nested structure
            if key not in self.cache:
                self.cache[key] = {}
            self.cache[key][process_name] = data_dict

        # Count total records across all keys and processes
        total_records = sum(len(procs) for procs in self.cache.values())
        logger.info(
            f"Loaded {total_records} records from DuckDB "
            f"({len(self.cache)} keys)"
        )

    def _get_wal_files(self):
        """Get all WAL files sorted by sequence number"""
        wal_dir = Path(self.wal_dir)
        wal_files = sorted(
            wal_dir.glob("wal_*.jsonl"), key=lambda p: int(p.stem.split("_")[1])
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

                            # Extract key, process_name, and data
                            key = record["key"]
                            process_name = record.get("process_name", "")
                            data = record["data"]

                            # Update nested in-memory cache
                            if key not in self.cache:
                                self.cache[key] = {}
                            self.cache[key][process_name] = data

                            # Update nested pending_writes
                            if key not in self.pending_writes:
                                self.pending_writes[key] = {}
                            self.pending_writes[key][process_name] = data

                if records:
                    logger.info(
                        f"Recovered {len(records)} records from {wal_file.name}"
                    )
            except Exception as e:
                logger.error(f"Error recovering from {wal_file}: {e}")

        # Flush all recovered data to DuckDB
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
        wal_path = Path(self.wal_dir) / f"wal_{self.wal_sequence:06d}.jsonl"
        self.current_wal = wal_path
        self.current_wal_file = open(wal_path, "a")
        self.current_wal_size = 0
        self.current_wal_count = 0
        self.current_wal_start_time = time.time()

        # Fsync directory to ensure new file entry is durable
        # (Windows doesn't support directory fsync, skip on that platform)
        if os.name != "nt":
            try:
                dir_fd = os.open(self.wal_dir, os.O_RDONLY)
                try:
                    os.fsync(dir_fd)
                finally:
                    os.close(dir_fd)
            except (OSError, PermissionError) as e:
                logger.warning(f"Could not fsync directory {self.wal_dir}: {e}")

        logger.info(f"Rotated to new WAL file: {wal_path.name}")

    def store(
        self,
        key: str,
        data: dict[str, Any],
        process_name: str | None = None,
        timestamp: str | dt.datetime | None = None,
        username: str | None = None,
    ):
        """Store data with async write to WAL.

        Args:
            key: Unique identifier for the data
            data: Dictionary to store (keeps all fields)
            process_name: Process identifier (extracted from data if None)
            timestamp: Timestamp (extracted from data if None)
            username: Username (extracted from data if None)
        """
        with self.write_lock:
            # Extract process_name: parameter > data field > None
            if process_name is None:
                process_name = data.get(StorageKeys.PROCESS_NAME)

            # Extract timestamp: parameter > data field > current time
            if timestamp is None:
                timestamp = data.get(StorageKeys.TIMESTAMP)
            if timestamp is None:
                timestamp = dt.datetime.now().isoformat()

            # Extract username: parameter > data field > None
            if username is None:
                username = data.get(StorageKeys.USERNAME)

            # Add timestamp and username to data dict (for cache/storage)
            data_with_metadata = dict(data)
            data_with_metadata[StorageKeys.TIMESTAMP] = timestamp
            if username is not None:
                data_with_metadata[StorageKeys.USERNAME] = username
            data_with_metadata[StorageKeys.PROCESS_NAME] = process_name

            # Update nested in-memory cache immediately
            if key not in self.cache:
                self.cache[key] = {}
            self.cache[key][process_name] = data_with_metadata

            # Update nested pending_writes
            if key not in self.pending_writes:
                self.pending_writes[key] = {}
            self.pending_writes[key][process_name] = data_with_metadata

            # Initialize WAL if needed
            if self.current_wal_file is None:
                self._rotate_wal()

            # Write to WAL (includes process_name and username in record)
            record = {
                "key": key,
                "process_name": process_name,
                "data": data,
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
            wal_age = (
                time.time() - self.current_wal_start_time
                if self.current_wal_start_time
                else 0
            )
            should_rotate = (
                self.current_wal_size >= self.config.max_wal_size
                or self.current_wal_count >= self.config.batch_size
                or wal_age >= self.config.max_wal_age_seconds
            )

            if should_rotate:
                self._rotate_wal()
                # Trigger flush in background
                threading.Thread(
                    target=self._flush_to_duckdb, daemon=True
                ).start()

    def get_key(self, key: str) -> dict[str, dict[str, Any]] | None:
        """Get all process data for a given key.

        Args:
            key: The key to look up

        Returns:
            Dictionary mapping process_name to data dict, or None if
            key doesn't exist
        """
        with self.write_lock:
            return self.cache.get(key)

    def get_key_process(
        self, key: str, process_name: str | None = None
    ) -> dict[str, Any] | None:
        """Get data for specific key and process_name combination.

        Args:
            key: The key to look up
            process_name: The process name (or None)

        Returns:
            Data dictionary if found, None otherwise
        """
        with self.write_lock:
            if key not in self.cache:
                return None
            return self.cache[key].get(process_name)

    def _flush_to_duckdb(self):
        """Flush pending writes to DuckDB and clean up old WAL files"""
        # Copy pending writes under write_lock to avoid race condition
        with self.write_lock:
            if not self.pending_writes:
                return
            # Deep copy nested structure
            writes_to_flush = {}
            for key, process_dict in self.pending_writes.items():
                writes_to_flush[key] = dict(process_dict)
            self.pending_writes.clear()

        with self.flush_lock:
            try:
                # Prepare batch data with special field extraction
                batch_data = []
                for key, process_dict in writes_to_flush.items():
                    for process_name, data in process_dict.items():
                        # Extract special fields from data
                        timestamp = parse_timestamp(
                            data.get(StorageKeys.TIMESTAMP)
                        )
                        status = data.get(StorageKeys.STATUS)
                        status_int = data.get(StorageKeys.STATUS_INT)
                        username = data.get(StorageKeys.USERNAME)

                        batch_data.append(
                            (
                                key,
                                process_name,
                                json.dumps(data)
                                if not isinstance(data, str)
                                else data,
                                timestamp,
                                status,
                                status_int,
                                username,
                                dt.datetime.now(),
                                key,
                                process_name,
                            )
                        )

                # Batch insert/update to DuckDB
                self.conn.execute("BEGIN TRANSACTION")

                # Get current versions for all keys/processes being updated
                version_map = {}
                for key, process_dict in writes_to_flush.items():
                    for process_name in process_dict.keys():
                        result = self.conn.execute(
                            """
                            SELECT COALESCE(MAX(version), 0)
                            FROM storage
                            WHERE key = ?
                            AND process_name IS NOT DISTINCT FROM ?
                            """,
                            [key, process_name],
                        ).fetchone()
                        version_map[(key, process_name)] = (
                            result[0] + 1 if result else 1
                        )

                # Prepare batch with calculated versions
                batch_with_versions = []
                for (
                    key,
                    process_name,
                    data_json,
                    timestamp,
                    status,
                    status_int,
                    username,
                    updated_at,
                    _key,
                    _process,
                ) in batch_data:
                    version = version_map[(key, process_name)]
                    batch_with_versions.append(
                        (
                            key,
                            process_name,
                            data_json,
                            timestamp,
                            status,
                            status_int,
                            username,
                            updated_at,
                            version,
                        )
                    )

                # Use INSERT OR REPLACE for upsert behavior
                self.conn.executemany(
                    """
                    INSERT OR REPLACE INTO storage
                    (key, process_name, data, timestamp, status,
                     status_int, username, updated_at, version)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    batch_with_versions,
                )

                self.conn.execute("COMMIT")

                logger.info(f"Flushed {len(batch_data)} records to DuckDB")

                # Clean up old WAL files (keep current one)
                self._cleanup_old_wals()

            except Exception as e:
                logger.error(f"Error flushing to DuckDB: {e}")
                self.conn.execute("ROLLBACK")
                # Restore failed writes back to pending_writes
                with self.write_lock:
                    for key, process_dict in writes_to_flush.items():
                        if key not in self.pending_writes:
                            self.pending_writes[key] = {}
                        self.pending_writes[key].update(process_dict)

    def _cleanup_old_wals(self):
        """Remove WAL files that have been successfully persisted"""
        wal_files = self._get_wal_files()

        for wal_file in wal_files:
            # Don't delete the current WAL file
            if self.current_wal and wal_file == self.current_wal:
                continue

            # Check if this WAL has been processed (no pending data from it)
            # In this implementation, we can safely delete after successful
            # flush
            try:
                wal_file.unlink()
                logger.info(f"Deleted processed WAL file: {wal_file.name}")
            except Exception as e:
                logger.error(f"Error deleting WAL file {wal_file}: {e}")

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
                "current_wal_size": self.current_wal_size,
                "current_wal_count": self.current_wal_count,
                "wal_files_count": len(wal_files),
                "wal_sequence": self.wal_sequence,
            }

    def export_to_parquet(self, parquet_path: str | None = None) -> str | None:
        """Export all DuckDB data to Hive-partitioned Parquet file.

        Args:
            parquet_path: Path to save parquet files. If None, uses
                         config.parquet_path

        Returns:
            Path where parquet was saved, or None if no path configured

        Raises:
            ValueError: If no parquet_path is provided or configured
        """
        # Determine parquet path
        export_path = parquet_path or self.config.parquet_path
        if not export_path:
            raise ValueError(
                "No parquet_path provided. Either pass it as argument "
                "or set it in WALConfig"
            )

        # Query all data from DuckDB with date column
        query = """
            SELECT
                key,
                process_name,
                data,
                timestamp,
                status,
                status_int,
                username,
                updated_at,
                version,
                ? as date
            FROM storage
        """

        df = self.conn.execute(query, [self.date_str]).df()

        if df.empty:
            logger.warning("No data to export to parquet")
            return None

        # Write as Hive-partitioned parquet
        # Partition by process_name and date (in that order)
        df.to_parquet(
            export_path,
            engine="pyarrow",
            partition_cols=["process_name", "date"],
            index=False,
        )

        logger.info(
            f"Exported {len(df)} records to Hive-partitioned "
            f"parquet at {export_path}"
        )
        return export_path

    def close(self):
        """Clean shutdown"""
        logger.info("Shutting down storage system...")

        # Stop background thread
        self.stop_event.set()
        if self.flush_thread.is_alive():
            self.flush_thread.join(timeout=5)

        # Final flush
        self.flush_data_to_duckdb()

        # Export to parquet if path is configured
        if self.config.parquet_path:
            try:
                self.export_to_parquet()
            except Exception as e:
                logger.error(f"Error exporting to parquet on close: {e}")

        # Close WAL file
        if self.current_wal_file:
            self.current_wal_file.flush()
            os.fsync(self.current_wal_file.fileno())
            self.current_wal_file.close()

        # Clean up all remaining WAL files
        # (data is now safely persisted in DuckDB)
        wal_files = self._get_wal_files()
        for wal_file in wal_files:
            try:
                wal_file.unlink()
                logger.info(f"Deleted WAL file on close: {wal_file.name}")
            except Exception as e:
                logger.error(f"Error deleting WAL file {wal_file}: {e}")

        # Close DuckDB connection
        self.conn.close()

        logger.info("Storage system shut down complete")
