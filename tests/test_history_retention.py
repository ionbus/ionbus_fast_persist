"""Test to verify history table retains all versions."""

from __future__ import annotations

import datetime as dt
import site
from pathlib import Path

parent_dir = Path(__file__).parent.parent.parent
raise RuntimeError(f"{parent_dir}")
site.addsitedir(str(parent_dir))

from ionbus_fast_persist import CollectionFastPersist, CollectionConfig

if __name__ == "__main__":
    print("Testing history retention...")
    print("=" * 60)

    config = CollectionConfig(base_dir="./test_history_retention")
    storage = CollectionFastPersist(dt.date.today(), config=config)

    # Store same key/collection/item multiple times with different values
    print("\nStoring value updates...")
    for i in range(1, 6):
        storage.store(
            key="user_123",
            collection_name="profile",
            item_name="age",
            data={"label": "Age"},
            value=30 + i,
        )
        print(f"  Version {i}: age = {30 + i}")

    # Flush to database and close (updates latest table)
    print("\nFlushing to database...")
    storage.flush_data_to_duckdb()

    # Update latest table manually for testing
    print("Updating latest table...")
    storage._update_latest_table()

    # Query history table directly
    print("\nQuerying history table...")
    result = storage.history_conn.execute(
        """
        SELECT version, value_int, updated_at
        FROM storage_history
        WHERE key = 'user_123'
        AND collection_name = 'profile'
        AND item_name = 'age'
        ORDER BY version
        """
    ).fetchall()

    print(f"\nHistory table contains {len(result)} versions:")
    for version, value_int, updated_at in result:
        print(f"  Version {version}: value={value_int}, updated={updated_at}")

    # Verify we have 5 versions
    if len(result) == 5:
        print("\n✓ SUCCESS: History table retains all 5 versions!")
    else:
        print(f"\n✗ FAILED: Expected 5 versions, got {len(result)}")

    # Query latest table
    print("\nQuerying latest table...")
    latest_result = storage.latest_conn.execute(
        """
        SELECT version, value_int
        FROM storage_latest
        WHERE key = 'user_123'
        AND collection_name = 'profile'
        AND item_name = 'age'
        """
    ).fetchall()

    print(f"\nLatest table contains {len(latest_result)} row(s):")
    for version, value_int in latest_result:
        print(f"  Version {version}: value={value_int}")

    if len(latest_result) == 1 and latest_result[0][1] == 35:
        print("\n✓ SUCCESS: Latest table contains only the final value (35)!")
    else:
        print(f"\n✗ FAILED: Latest table should have 1 row with value=35")

    storage.close()
    print("\n" + "=" * 60)
    print("Test complete!")
