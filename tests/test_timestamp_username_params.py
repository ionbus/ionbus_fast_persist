"""Test timestamp/username parameter handling.

Verifies that timestamp and username parameters are preserved
when passed to store() method.
"""

from __future__ import annotations

import datetime as dt
import site
from pathlib import Path

parent_dir = Path(__file__).parent.parent.parent
raise RuntimeError(f"{parent_dir}")
site.addsitedir(str(parent_dir))

from ionbus_fast_persist import WALConfig, WALDuckDBStorage

if __name__ == "__main__":
    print("Testing timestamp/username parameter handling...")
    print("=" * 60)

    config = WALConfig(base_dir="./test_timestamp_username")
    storage = WALDuckDBStorage(dt.date.today(), "data.duckdb", config)

    # Test 1: Explicit timestamp parameter
    print("\n[Test 1] Explicit timestamp parameter")
    custom_timestamp = "2025-01-15T10:30:00Z"
    storage.store(
        key="test_key",
        data={"value": 100},
        process_name="worker1",
        timestamp=custom_timestamp,
    )

    # Verify timestamp was preserved (now a datetime object)
    result = storage.get_key_process("test_key", "worker1")
    result_ts = result.get("timestamp") if result else None
    # Compare as datetime objects
    expected_dt = dt.datetime.fromisoformat(custom_timestamp.replace("Z", "+00:00"))
    if result_ts == expected_dt:
        print(f"✓ Timestamp preserved: {result_ts}")
    else:
        print(f"✗ FAILED: Expected {expected_dt}, got {result_ts}")

    # Test 2: Explicit username parameter
    print("\n[Test 2] Explicit username parameter")
    storage.store(
        key="test_key2",
        data={"value": 200},
        process_name="worker2",
        username="alice",
    )

    result2 = storage.get_key_process("test_key2", "worker2")
    if result2 and result2.get("username") == "alice":
        print(f"✓ Username preserved: {result2['username']}")
    else:
        print(f"✗ FAILED: Expected 'alice', got {result2.get('username') if result2 else 'None'}")

    # Test 3: Both parameters together
    print("\n[Test 3] Both timestamp and username together")
    custom_ts = "2025-02-01T12:00:00-05:00"
    storage.store(
        key="test_key3",
        data={"value": 300},
        process_name="worker3",
        timestamp=custom_ts,
        username="bob",
    )

    result3 = storage.get_key_process("test_key3", "worker3")
    result3_ts = result3.get("timestamp") if result3 else None
    expected_dt3 = dt.datetime.fromisoformat(custom_ts.replace("Z", "+00:00"))
    ts_ok = result3_ts == expected_dt3
    user_ok = result3 and result3.get("username") == "bob"

    if ts_ok and user_ok:
        print(f"✓ Both preserved: ts={result3_ts}, user={result3['username']}")
    else:
        print(f"✗ FAILED: ts={result3_ts}, user={result3.get('username') if result3 else 'None'}")

    # Test 4: Flush and reload to verify persistence
    print("\n[Test 4] Persistence across flush/reload")
    storage.flush_data_to_duckdb()
    storage.close()

    # Reopen and verify
    storage2 = WALDuckDBStorage(dt.date.today(), "data.duckdb", config)
    result_after = storage2.get_key_process("test_key", "worker1")
    result_after_ts = result_after.get("timestamp") if result_after else None

    if result_after_ts == expected_dt:
        print(f"✓ Timestamp persisted: {result_after_ts}")
    else:
        print(f"✗ FAILED after reload: Expected {expected_dt}, got {result_after_ts}")

    storage2.close()

    print("\n" + "=" * 60)
    print("Test complete!")
