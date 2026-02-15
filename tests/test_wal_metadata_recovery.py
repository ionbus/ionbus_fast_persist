"""Test that WAL recovery preserves timestamp/username metadata.

This test verifies that when recovering from WAL files after a crash,
the timestamp and username metadata are correctly restored (not just
the data payload).
"""

from __future__ import annotations

import datetime as dt
import os
import site
import time
from pathlib import Path

parent_dir = Path(__file__).parent.parent.parent
site.addsitedir(str(parent_dir))

from ionbus_fast_persist import WALConfig, WALDuckDBStorage

if __name__ == "__main__":
    print("Testing WAL metadata recovery...")
    print("=" * 60)

    config = WALConfig(base_dir="./test_wal_metadata_recovery")

    # Session 1: Write data with explicit metadata
    print("\n[Session 1] Writing data with metadata...")
    storage1 = WALDuckDBStorage(dt.date.today(), "data.duckdb", config)

    custom_timestamp = "2025-01-15T10:30:00Z"
    custom_username = "alice"

    storage1.store(
        key="test_key",
        data={"value": 100, "description": "test data"},
        process_name="worker1",
        timestamp=custom_timestamp,
        username=custom_username,
    )

    print(f"  Stored with timestamp: {custom_timestamp}")
    print(f"  Stored with username: {custom_username}")

    # Simulate crash: close WITHOUT flushing to DB
    # This leaves data only in WAL files
    if storage1.current_wal_file:
        storage1.current_wal_file.close()
        storage1.current_wal_file = None

    print("  Simulated crash (data only in WAL, not flushed to DB)")

    # Session 2: Recover from WAL and verify metadata
    print("\n[Session 2] Recovering from WAL...")
    storage2 = WALDuckDBStorage(dt.date.today(), "data.duckdb", config)

    result = storage2.get_key_process("test_key", "worker1")

    # Verify all metadata was recovered
    success = True

    if result is None:
        print("  [FAIL] FAILED: No data recovered")
        success = False
    else:
        # Check timestamp (now a datetime object)
        recovered_ts = result.get("timestamp")
        expected_dt = dt.datetime.fromisoformat(custom_timestamp.replace("Z", "+00:00"))
        if recovered_ts == expected_dt:
            print(f"  [OK] Timestamp recovered: {recovered_ts}")
        else:
            print(
                f"  [FAIL] FAILED: Expected timestamp {expected_dt}, "
                f"got {recovered_ts}"
            )
            success = False

        # Check username
        recovered_user = result.get("username")
        if recovered_user == custom_username:
            print(f"  [OK] Username recovered: {recovered_user}")
        else:
            print(
                f"  [FAIL] FAILED: Expected username {custom_username}, "
                f"got {recovered_user}"
            )
            success = False

        # Check process_name
        recovered_proc = result.get("process_name")
        if recovered_proc == "worker1":
            print(f"  [OK] Process name recovered: {recovered_proc}")
        else:
            print(
                f"  [FAIL] FAILED: Expected process_name 'worker1', "
                f"got {recovered_proc}"
            )
            success = False

        # Check original data
        if result.get("value") == 100:
            print(f"  [OK] Original data intact: value={result.get('value')}")
        else:
            print(f"  [FAIL] FAILED: Original data corrupted")
            success = False

    # Clean up
    storage2.close()

    print("\n" + "=" * 60)
    if success:
        print("[OK] SUCCESS: WAL recovery preserves all metadata!")
    else:
        print("[FAIL] FAILED: WAL recovery lost metadata")
