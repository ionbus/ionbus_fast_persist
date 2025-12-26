"""Test to verify process_name=None is preserved across flush/load."""

from __future__ import annotations

import datetime as dt

from dated_fast_persist import WALDuckDBStorage, WALConfig

if __name__ == "__main__":
    print("Testing process_name=None preservation...")
    print("=" * 60)

    config = WALConfig(base_dir="./test_process_name_none")
    storage = WALDuckDBStorage(dt.date.today(), "data.duckdb", config)

    # Store data with process_name=None
    print("\nStoring data with process_name=None...")
    storage.store(
        key="test_key",
        data={"value": 100, "label": "Test"},
        process_name=None,
    )

    # Store data with process_name="" (empty string)
    print("Storing data with process_name='' (empty string)...")
    storage.store(
        key="test_key",
        data={"value": 200, "label": "Test 2"},
        process_name="",
    )

    # Store data with process_name="worker1"
    print("Storing data with process_name='worker1'...")
    storage.store(
        key="test_key",
        data={"value": 300, "label": "Test 3"},
        process_name="worker1",
    )

    # Flush to database
    print("\nFlushing to database...")
    storage.flush_data_to_duckdb()

    # Check cache before close
    print("\nCache contents before close:")
    key_data = storage.get_key("test_key")
    if key_data:
        for process_name, data in key_data.items():
            print(f"  process_name={repr(process_name)}: {data}")

    # Close and reopen
    print("\nClosing storage...")
    storage.close()

    print("Reopening storage...")
    storage2 = WALDuckDBStorage(dt.date.today(), "data.duckdb", config)

    # Check cache after recovery
    print("\nCache contents after recovery:")
    key_data2 = storage2.get_key("test_key")
    if key_data2:
        for process_name, data in key_data2.items():
            print(f"  process_name={repr(process_name)}: {data}")

    # Verify
    print("\n" + "=" * 60)
    if key_data2 and None in key_data2 and "" in key_data2:
        print("✓ SUCCESS: Both None and '' preserved!")
        print(f"  None -> {key_data2[None].get('value')}")
        print(f"  ''   -> {key_data2[''].get('value')}")
    else:
        print("✗ FAILED: None or '' not preserved")
        if key_data2:
            print(f"  Keys: {list(key_data2.keys())}")

    storage2.close()
    print("\nTest complete!")
