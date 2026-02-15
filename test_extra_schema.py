"""Test extra_schema feature for both dated and collection fast persist."""

from __future__ import annotations

import datetime as dt
import shutil
import sys
from pathlib import Path

from fast_persist_common import ExtraSchemaError
from dated_fast_persist import WALDuckDBStorage, WALConfig
from collection_fast_persist import (
    CollectionFastPersist,
    CollectionConfig,
)


def cleanup_test_dirs():
    """Remove test directories."""
    dirs_to_remove = [
        "./extra_schema_test_dated",
        "./extra_schema_test_collection",
    ]
    for dir_path in dirs_to_remove:
        if Path(dir_path).exists():
            shutil.rmtree(dir_path)


def test_dated_extra_schema():
    """Test extra_schema with dated_fast_persist."""
    print("\n" + "=" * 60)
    print("Testing dated_fast_persist extra_schema")
    print("=" * 60)

    config = WALConfig(
        base_dir="./extra_schema_test_dated",
        batch_size=10,
        duckdb_flush_interval_seconds=60,
        extra_schema={
            "customer_id": "int64",
            "price": "float64",
            "is_active": "bool",
            "notes": "string",
        },
    )

    storage = WALDuckDBStorage(dt.date.today(), "data.duckdb", config)

    # Test 1: Store data with all extra columns populated
    print("\n[Test 1] Storing data with all extra columns...")
    storage.store(
        "order_1",
        {
            "order_name": "Test Order",
            "customer_id": 12345,
            "price": 99.99,
            "is_active": True,
            "notes": "First order",
        },
        process_name="worker1",
    )

    # Test 2: Store data with some extra columns missing (should be NULL)
    print("[Test 2] Storing data with some extra columns missing...")
    storage.store(
        "order_2",
        {
            "order_name": "Partial Order",
            "customer_id": 67890,
            # price, is_active, notes are missing
        },
        process_name="worker1",
    )

    # Force flush to DuckDB
    storage.flush_data_to_duckdb()

    # Verify data in DuckDB
    print("\n[Verification] Checking DuckDB contents...")
    result = storage.conn.execute(
        "SELECT key, customer_id, price, is_active, notes FROM storage"
    ).fetchall()

    print(f"  Found {len(result)} records:")
    for row in result:
        print(f"    {row}")

    # Verify order_1 has all values
    order_1 = [r for r in result if r[0] == "order_1"][0]
    assert order_1[1] == 12345, f"Expected customer_id=12345, got {order_1[1]}"
    assert order_1[2] == 99.99, f"Expected price=99.99, got {order_1[2]}"
    assert order_1[3] is True, f"Expected is_active=True, got {order_1[3]}"
    assert order_1[4] == "First order", f"Expected notes, got {order_1[4]}"
    print("  order_1 verification passed!")

    # Verify order_2 has NULL for missing columns
    order_2 = [r for r in result if r[0] == "order_2"][0]
    assert order_2[1] == 67890, f"Expected customer_id=67890, got {order_2[1]}"
    assert order_2[2] is None, f"Expected price=None, got {order_2[2]}"
    assert order_2[3] is None, f"Expected is_active=None, got {order_2[3]}"
    assert order_2[4] is None, f"Expected notes=None, got {order_2[4]}"
    print("  order_2 verification passed!")

    storage.close()
    print("\n[PASSED] dated_fast_persist extra_schema tests")


def test_dated_parquet_export():
    """Test that extra columns appear in Parquet export."""
    print("\n" + "=" * 60)
    print("Testing dated_fast_persist Parquet export with extra_schema")
    print("=" * 60)

    config = WALConfig(
        base_dir="./extra_schema_test_dated",
        batch_size=10,
        duckdb_flush_interval_seconds=60,
        parquet_path="./extra_schema_test_dated/parquet_output",
        extra_schema={
            "customer_id": "int64",
            "price": "float64",
        },
    )

    storage = WALDuckDBStorage(dt.date.today(), "data2.duckdb", config)

    storage.store(
        "order_export",
        {
            "order_name": "Export Test",
            "customer_id": 11111,
            "price": 50.00,
        },
        process_name="exporter",
    )

    storage.flush_data_to_duckdb()

    # Export to parquet
    parquet_path = storage.export_to_parquet()
    print(f"  Exported to: {parquet_path}")

    # Read parquet and verify columns exist
    import pandas as pd

    df = pd.read_parquet(parquet_path)
    print(f"  Parquet columns: {list(df.columns)}")

    assert "customer_id" in df.columns, "customer_id not in parquet"
    assert "price" in df.columns, "price not in parquet"
    assert df["customer_id"].iloc[0] == 11111
    assert df["price"].iloc[0] == 50.00

    print("  Parquet verification passed!")

    storage.close()
    print("\n[PASSED] dated_fast_persist Parquet export test")


def test_collection_extra_schema():
    """Test extra_schema with collection_fast_persist."""
    print("\n" + "=" * 60)
    print("Testing collection_fast_persist extra_schema")
    print("=" * 60)

    config = CollectionConfig(
        base_dir="./extra_schema_test_collection",
        batch_size=10,
        duckdb_flush_interval_seconds=60,
        extra_schema={
            "priority": "int32",
            "score": "float64",
            "category": "string",
        },
    )

    storage = CollectionFastPersist(dt.date.today(), config=config)

    # Test 1: Store data with all extra columns populated
    print("\n[Test 1] Storing data with all extra columns...")
    storage.store(
        key="task_1",
        data={
            "label": "High Priority Task",
            "priority": 1,
            "score": 95.5,
            "category": "urgent",
        },
        item_name="item_a",
        collection_name="tasks",
        value=100,
    )

    # Test 2: Store data with some extra columns missing
    print("[Test 2] Storing data with some extra columns missing...")
    storage.store(
        key="task_2",
        data={
            "label": "Low Priority Task",
            "priority": 5,
            # score, category missing
        },
        item_name="item_b",
        collection_name="tasks",
        value=50,
    )

    # Force flush
    storage.flush_data_to_duckdb()

    # Verify data in history DuckDB
    print("\n[Verification] Checking history DuckDB contents...")
    result = storage.history_conn.execute(
        "SELECT key, item_name, priority, score, category "
        "FROM storage_history"
    ).fetchall()

    print(f"  Found {len(result)} records:")
    for row in result:
        print(f"    {row}")

    # Verify task_1 has all values
    task_1 = [r for r in result if r[0] == "task_1"][0]
    assert task_1[2] == 1, f"Expected priority=1, got {task_1[2]}"
    assert task_1[3] == 95.5, f"Expected score=95.5, got {task_1[3]}"
    assert task_1[4] == "urgent", f"Expected category=urgent, got {task_1[4]}"
    print("  task_1 verification passed!")

    # Verify task_2 has NULL for missing columns
    task_2 = [r for r in result if r[0] == "task_2"][0]
    assert task_2[2] == 5, f"Expected priority=5, got {task_2[2]}"
    assert task_2[3] is None, f"Expected score=None, got {task_2[3]}"
    assert task_2[4] is None, f"Expected category=None, got {task_2[4]}"
    print("  task_2 verification passed!")

    storage.close()

    # Verify latest table also has extra columns after close
    print("\n[Verification] Checking latest table after close...")
    import duckdb

    latest_conn = duckdb.connect(
        str(Path("./extra_schema_test_collection") / "storage_latest.duckdb"),
        read_only=True,
    )
    result = latest_conn.execute(
        "SELECT key, item_name, priority, score, category "
        "FROM storage_latest"
    ).fetchall()
    latest_conn.close()

    print(f"  Found {len(result)} records in latest table:")
    for row in result:
        print(f"    {row}")

    print("\n[PASSED] collection_fast_persist extra_schema tests")


def test_name_conflict_detection():
    """Test that name conflicts raise ExtraSchemaError."""
    print("\n" + "=" * 60)
    print("Testing name conflict detection")
    print("=" * 60)

    # Test dated_fast_persist conflict
    print("\n[Test 1] dated_fast_persist with reserved column name...")
    try:
        config = WALConfig(
            base_dir="./extra_schema_test_dated",
            extra_schema={"timestamp": "string"},  # Conflict!
        )
        storage = WALDuckDBStorage(dt.date.today(), "conflict.duckdb", config)
        storage.close()
        print("  ERROR: Should have raised ExtraSchemaError!")
        sys.exit(1)
    except ExtraSchemaError as e:
        print(f"  Correctly raised ExtraSchemaError: {e}")

    # Test collection_fast_persist conflict
    print("\n[Test 2] collection_fast_persist with reserved column name...")
    try:
        config = CollectionConfig(
            base_dir="./extra_schema_test_collection",
            extra_schema={"collection_name": "string"},  # Conflict!
        )
        storage = CollectionFastPersist(dt.date.today(), config=config)
        storage.close()
        print("  ERROR: Should have raised ExtraSchemaError!")
        sys.exit(1)
    except ExtraSchemaError as e:
        print(f"  Correctly raised ExtraSchemaError: {e}")

    print("\n[PASSED] Name conflict detection tests")


def test_invalid_type_detection():
    """Test that invalid PyArrow types raise ExtraSchemaError."""
    print("\n" + "=" * 60)
    print("Testing invalid type detection")
    print("=" * 60)

    print("\n[Test 1] Invalid PyArrow type name...")
    try:
        config = WALConfig(
            base_dir="./extra_schema_test_dated",
            extra_schema={"my_col": "invalid_type"},
        )
        storage = WALDuckDBStorage(dt.date.today(), "invalid.duckdb", config)
        storage.close()
        print("  ERROR: Should have raised ExtraSchemaError!")
        sys.exit(1)
    except ExtraSchemaError as e:
        print(f"  Correctly raised ExtraSchemaError: {e}")

    print("\n[PASSED] Invalid type detection tests")


if __name__ == "__main__":
    # Clean up before tests
    cleanup_test_dirs()

    try:
        test_name_conflict_detection()
        test_invalid_type_detection()
        test_dated_extra_schema()
        test_dated_parquet_export()
        test_collection_extra_schema()

        print("\n" + "=" * 60)
        print("ALL EXTRA_SCHEMA TESTS PASSED!")
        print("=" * 60)

    finally:
        # Clean up after tests
        cleanup_test_dirs()
