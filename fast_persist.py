"""Claude fast persistent storage using DuckDB and Write-Ahead Logs (WAL)"""

from __future__ import annotations

import datetime as dt
import duckdb
import json
import os
import sys
import time
import threading
from pathlib import Path
from typing import Any
from dataclasses import dataclass
import logging

# Handle StrEnum for different Python versions
if sys.version_info >= (3, 11):
    from enum import StrEnum
else:
    from backports.strenum import StrEnum  # type: ignore

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class StorageKeys(StrEnum):
    """String enumeration for special storage dictionary keys"""

    PROCESS_NAME = "process_name"
    TIMESTAMP = "timestamp"
    STATUS = "status"
    STATUS_INT = "status_int"


def _parse_timestamp(ts: str | dt.datetime | None) -> dt.datetime | None:
    """Convert timestamp string to datetime object.

    Handles ISO 8601 format with or without timezone.
    Returns None if input is None.
    """
    if ts is None:
        return None
    if isinstance(ts, dt.datetime):
        return ts
    # Parse ISO 8601 string - fromisoformat handles timezone info
    try:
        return dt.datetime.fromisoformat(ts.replace("Z", "+00:00"))
    except (ValueError, AttributeError):
        logger.warning(f"Could not parse timestamp: {ts}")
        return None


@dataclass
class WALConfig:
    """Configuration for WAL behavior"""

    base_dir: str = "./storage"
    max_wal_size: int = 10 * 1024 * 1024  # 10MB per WAL file
    max_wal_age_seconds: int = 300  # 5 minutes
    batch_size: int = 1000  # Records before triggering batch write
    flush_interval_seconds: int = 30  # Force flush every 30 seconds


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

        # Update config to use date-specific directory
        self.config = config or WALConfig()
        # Append date subdirectory to base_dir
        self.config.base_dir = os.path.join(self.config.base_dir, self.date_str)

        # Create date-specific database path
        db_dir = os.path.dirname(db_path) if os.path.dirname(db_path) else "."
        db_name = os.path.basename(db_path)
        self.db_path = os.path.join(db_dir, self.date_str, db_name)

        # Create date-specific storage directories
        Path(self.config.base_dir).mkdir(parents=True, exist_ok=True)
        Path(os.path.dirname(self.db_path)).mkdir(parents=True, exist_ok=True)

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

        # Check if old schema exists and drop it
        try:
            self.conn.execute("DROP TABLE IF EXISTS storage")
        except Exception:
            pass

        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS storage (
                key VARCHAR NOT NULL,
                process_name VARCHAR NOT NULL DEFAULT '',
                data JSON,
                timestamp TIMESTAMP,
                status VARCHAR,
                status_int INTEGER,
                updated_at TIMESTAMP,
                version INTEGER DEFAULT 1,
                PRIMARY KEY (key, process_name)
            )
        """)

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
        wal_dir = Path(self.config.base_dir)
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
        wal_path = (
            Path(self.config.base_dir) / f"wal_{self.wal_sequence:06d}.jsonl"
        )
        self.current_wal = wal_path
        self.current_wal_file = open(wal_path, "a")
        self.current_wal_size = 0
        self.current_wal_count = 0
        self.current_wal_start_time = time.time()

        # Fsync directory to ensure new file entry is durable
        # (Windows doesn't support directory fsync, skip on that platform)
        if os.name != "nt":
            try:
                dir_fd = os.open(self.config.base_dir, os.O_RDONLY)
                try:
                    os.fsync(dir_fd)
                finally:
                    os.close(dir_fd)
            except (OSError, PermissionError) as e:
                logger.warning(
                    f"Could not fsync directory {self.config.base_dir}: {e}"
                )

        logger.info(f"Rotated to new WAL file: {wal_path.name}")

    def store(
        self,
        key: str,
        data: dict[str, Any],
        process_name: str | None = None,
        timestamp: str | dt.datetime | None = None,
    ):
        """Store data with async write to WAL.

        Args:
            key: Unique identifier for the data
            data: Dictionary to store (keeps all fields)
            process_name: Process identifier (extracted from data if None)
            timestamp: Timestamp (extracted from data if None)
        """
        with self.write_lock:
            # Extract process_name: parameter > data field > ""
            if process_name is None:
                process_name = data.get(StorageKeys.PROCESS_NAME, "")

            # Extract timestamp: parameter > data field > None
            if timestamp is None:
                timestamp = data.get(StorageKeys.TIMESTAMP)

            # Update nested in-memory cache immediately
            if key not in self.cache:
                self.cache[key] = {}
            self.cache[key][process_name] = data

            # Update nested pending_writes
            if key not in self.pending_writes:
                self.pending_writes[key] = {}
            self.pending_writes[key][process_name] = data

            # Initialize WAL if needed
            if self.current_wal_file is None:
                self._rotate_wal()

            # Write to WAL (includes process_name in record)
            record = {
                "key": key,
                "process_name": process_name,
                "data": data,
                "timestamp": dt.datetime.now().isoformat(),
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
                        # Ensure process_name is not None
                        if process_name is None:
                            process_name = ""

                        # Extract special fields from data
                        timestamp = _parse_timestamp(
                            data.get(StorageKeys.TIMESTAMP)
                        )
                        status = data.get(StorageKeys.STATUS)
                        status_int = data.get(StorageKeys.STATUS_INT)

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
                                dt.datetime.now(),
                                key,
                                process_name,
                            )
                        )

                # Batch insert/update to DuckDB
                self.conn.execute("BEGIN TRANSACTION")

                # Use INSERT OR REPLACE for upsert behavior
                self.conn.executemany(
                    """
                    INSERT OR REPLACE INTO storage
                    (key, process_name, data, timestamp, status,
                     status_int, updated_at, version)
                    VALUES (?, ?, ?, ?, ?, ?, ?,
                        COALESCE(
                            (SELECT version + 1 FROM storage
                             WHERE key = ? AND process_name IS NOT DISTINCT FROM ?),
                            1
                        )
                    )
                """,  # noqa: E501
                    batch_data,
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
            time.sleep(self.config.flush_interval_seconds)

            with self.write_lock:
                if self.pending_writes:
                    # Rotate WAL before flushing
                    if self.current_wal_file:
                        self._rotate_wal()

            self._flush_to_duckdb()

    def force_flush(self):
        """Force an immediate flush to DuckDB"""
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

    def close(self):
        """Clean shutdown"""
        logger.info("Shutting down storage system...")

        # Stop background thread
        self.stop_event.set()
        if self.flush_thread.is_alive():
            self.flush_thread.join(timeout=5)

        # Final flush
        self.force_flush()

        # Close WAL file
        if self.current_wal_file:
            self.current_wal_file.flush()
            os.fsync(self.current_wal_file.fileno())
            self.current_wal_file.close()

        # Close DuckDB connection
        self.conn.close()

        logger.info("Storage system shut down complete")
