"""Claude fast persistent storage using DuckDB and Write-Ahead Logs (WAL)"""

from __future__ import annotations

import duckdb
import json
import time
import threading
from pathlib import Path
from typing import Any, Dict, Optional
from dataclasses import dataclass
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


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
        self, db_path: str = "data.duckdb", config: Optional[WALConfig] = None
    ):
        self.config = config or WALConfig()
        self.db_path = db_path

        # Create storage directory
        Path(self.config.base_dir).mkdir(parents=True, exist_ok=True)

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
                key VARCHAR PRIMARY KEY,
                data JSON,
                updated_at TIMESTAMP,
                version INTEGER DEFAULT 1
            )
        """)

        # Load existing data into cache
        result = self.conn.execute("SELECT key, data FROM storage").fetchall()
        for key, data in result:
            self.cache[key] = data

        logger.info(f"Loaded {len(self.cache)} existing records from DuckDB")

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
                            # Update in-memory cache
                            self.cache[record["key"]] = record["data"]
                            self.pending_writes[record["key"]] = record["data"]

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
            self.current_wal_file.close()

        self.wal_sequence += 1
        wal_path = (
            Path(self.config.base_dir) / f"wal_{self.wal_sequence:06d}.jsonl"
        )
        self.current_wal = wal_path
        self.current_wal_file = open(wal_path, "a")
        self.current_wal_size = 0
        self.current_wal_count = 0

        logger.info(f"Rotated to new WAL file: {wal_path.name}")

    def store(self, key: str, data: Dict[str, Any]):
        """Store data with async write to WAL"""
        with self.write_lock:
            # Update in-memory cache immediately
            self.cache[key] = data
            self.pending_writes[key] = data

            # Initialize WAL if needed
            if self.current_wal_file is None:
                self._rotate_wal()

            # Write to WAL
            record = {
                "key": key,
                "data": data,
                "timestamp": datetime.now().isoformat(),
            }
            wal_entry = json.dumps(record) + "\n"
            wal_bytes = wal_entry.encode("utf-8")

            if not self.current_wal_file:
                raise RuntimeError("WAL file is not initialized")
            self.current_wal_file.write(wal_entry)
            self.current_wal_file.flush()
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

    def get(self, key: str) -> Optional[Dict[str, Any]]:
        """Get data from cache"""
        with self.write_lock:
            return self.cache.get(key)

    def _flush_to_duckdb(self):
        """Flush pending writes to DuckDB and clean up old WAL files"""
        with self.flush_lock:
            if not self.pending_writes:
                return

            try:
                # Prepare batch data
                batch_data = []
                for key, data in self.pending_writes.items():
                    batch_data.append(
                        (
                            key,
                            json.dumps(data)
                            if not isinstance(data, str)
                            else data,
                            datetime.now(),
                        )
                    )

                # Batch insert/update to DuckDB
                self.conn.execute("BEGIN TRANSACTION")

                # Use INSERT OR REPLACE for upsert behavior
                self.conn.executemany(
                    """
                    INSERT OR REPLACE INTO storage (key, data, updated_at, version)
                    VALUES (?, ?, ?, 
                        COALESCE((SELECT version + 1 FROM storage WHERE key = ?), 1))
                """,  # noqa: E501
                    [(k, d, t, k) for k, d, t in batch_data],
                )

                self.conn.execute("COMMIT")

                logger.info(f"Flushed {len(batch_data)} records to DuckDB")

                # Clear pending writes
                self.pending_writes.clear()

                # Clean up old WAL files (keep current one)
                self._cleanup_old_wals()

            except Exception as e:
                logger.error(f"Error flushing to DuckDB: {e}")
                self.conn.execute("ROLLBACK")

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

    def get_stats(self) -> Dict[str, Any]:
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
            self.current_wal_file.close()

        # Close DuckDB connection
        self.conn.close()

        logger.info("Storage system shut down complete")


# Example usage
if __name__ == "__main__":
    import random
    import string

    # Configure the storage system
    config = WALConfig(
        base_dir="./wal_storage",
        max_wal_size=1024 * 1024,  # 1MB per WAL
        batch_size=100,  # Flush every 100 records
        flush_interval_seconds=10,  # Or every 10 seconds
    )

    # Initialize storage
    storage = WALDuckDBStorage("data.duckdb", config)

    # Simulate async updates
    def generate_update():
        key = "".join(random.choices(string.ascii_letters, k=10))
        data = {
            "value": random.randint(1, 1000),
            "timestamp": datetime.now().isoformat(),
            "metadata": {"source": "simulator"},
        }
        return key, data

    # Test writes
    print("Writing test data...")
    for i in range(250):
        key, data = generate_update()
        storage.store(key, data)

        if i % 50 == 0:
            stats = storage.get_stats()
            print(f"After {i} writes: {stats}")

    # Force final flush
    storage.force_flush()

    # Check final stats
    final_stats = storage.get_stats()
    print(f"Final stats: {final_stats}")

    # Clean shutdown
    storage.close()

    print("\nRestarting to test recovery...")

    # Test recovery by creating new instance
    storage2 = WALDuckDBStorage("data.duckdb", config)
    recovery_stats = storage2.get_stats()
    print(f"After recovery: {recovery_stats}")

    # Verify data
    sample_key = list(storage2.cache.keys())[0] if storage2.cache else None
    if sample_key:
        print(
            f"Sample recovered data: {sample_key} = {storage2.get(sample_key)}"
        )

    storage2.close()
