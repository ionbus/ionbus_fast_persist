"""Two-stage crash recovery test for fast_persist.

Stage 1: Write data and kill process while WAL files exist
Stage 2: Recover from WAL, verify data, clean shutdown, verify parquet
"""

from __future__ import annotations

import datetime as dt
import os
import sys

from fast_persist import WALDuckDBStorage, WALConfig, StorageKeys


def stage1_write_and_crash():
    """Stage 1: Write data and simulate crash before clean shutdown."""
    print("=" * 60)
    print("STAGE 1: Writing data and simulating crash")
    print("=" * 60)

    config = WALConfig(
        base_dir="./crash_test_wal",
        max_wal_size=1024 * 512,  # 512KB - small for testing
        batch_size=50,  # Small batch to ensure multiple WAL files
        duckdb_flush_interval_seconds=60,  # Long interval to keep data in WAL
        parquet_path="./crash_test_output",
    )

    storage = WALDuckDBStorage(dt.date.today(), "crash_test.duckdb", config)

    # Write test data across multiple processes
    test_data = [
        ("metrics", {"cpu": 75, "memory": 60, "disk": 45}, "server1"),
        ("metrics", {"cpu": 82, "memory": 55, "disk": 50}, "server2"),
        ("metrics", {"cpu": 68, "memory": 70, "disk": 38}, "server3"),
        ("status", {"state": "running", "uptime": 3600}, "server1"),
        ("status", {"state": "running", "uptime": 7200}, "server2"),
        ("status", {"state": "degraded", "uptime": 1800}, "server3"),
        ("alerts", {"level": "warning", "count": 5}, "server1"),
        ("alerts", {"level": "info", "count": 12}, "server2"),
        ("alerts", {"level": "critical", "count": 2}, "server3"),
    ]

    print("\nWriting initial data...")
    for key, data, process in test_data:
        data[StorageKeys.TIMESTAMP] = dt.datetime.now().isoformat()
        data[StorageKeys.STATUS] = "active"
        data[StorageKeys.STATUS_INT] = 1
        storage.store(key, data, process_name=process)
        print(f"  Stored: key={key}, process={process}")

    # Check stats to see WAL files
    stats = storage.get_stats()
    print(f"\nStats before crash: {stats}")
    print(f"  WAL files: {stats['wal_files_count']}")
    print(f"  Pending writes: {stats['pending_writes']}")
    print(f"  Cache size: {stats['cache_size']}")

    # Verify data in cache
    print("\nData in cache before crash:")
    for key in ["metrics", "status", "alerts"]:
        data = storage.get_key(key)
        if data:
            print(f"  {key}: {len(data)} processes")

    # SIMULATE CRASH - exit without calling close()
    print("\n" + "!" * 60)
    print("SIMULATING CRASH - Exiting without clean shutdown!")
    print(f"Leaving {stats['wal_files_count']} WAL file(s) behind")
    print("!" * 60)

    # Force exit without cleanup
    sys.exit(0)


def stage2_recover_and_verify():
    """Stage 2: Recover from WAL files and verify data integrity."""
    print("=" * 60)
    print("STAGE 2: Recovering from crash")
    print("=" * 60)

    config = WALConfig(
        base_dir="./crash_test_wal",
        max_wal_size=1024 * 512,
        batch_size=50,
        duckdb_flush_interval_seconds=60,
        parquet_path="./crash_test_output",
    )

    print("\nInitializing storage (will trigger recovery)...")
    storage = WALDuckDBStorage(dt.date.today(), "crash_test.duckdb", config)

    # Check recovery stats
    stats = storage.get_stats()
    print(f"\nStats after recovery: {stats}")
    print(f"  Cache size: {stats['cache_size']}")
    print(f"  Pending writes: {stats['pending_writes']}")
    print(f"  WAL files: {stats['wal_files_count']}")

    # Verify all data was recovered
    print("\n" + "=" * 60)
    print("VERIFYING RECOVERED DATA")
    print("=" * 60)

    expected_data = {
        "metrics": ["server1", "server2", "server3"],
        "status": ["server1", "server2", "server3"],
        "alerts": ["server1", "server2", "server3"],
    }

    all_verified = True
    for key, expected_processes in expected_data.items():
        recovered_data = storage.get_key(key)
        if recovered_data is None:
            print(f"\n✗ FAILED: Key '{key}' not found!")
            all_verified = False
            continue

        print(f"\n✓ Key '{key}' recovered with {len(recovered_data)} processes")

        for process in expected_processes:
            if process in recovered_data:
                data = recovered_data[process]
                print(f"  ✓ Process '{process}': {list(data.keys())}")
            else:
                print(f"  ✗ FAILED: Process '{process}' missing!")
                all_verified = False

    # Test specific queries
    print("\n" + "=" * 60)
    print("TESTING SPECIFIC QUERIES")
    print("=" * 60)

    server1_metrics = storage.get_key_process("metrics", "server1")
    if server1_metrics:
        print(f"\n✓ server1 metrics: cpu={server1_metrics.get('cpu')}, "
              f"memory={server1_metrics.get('memory')}")
    else:
        print("\n✗ FAILED: Could not retrieve server1 metrics")
        all_verified = False

    server3_status = storage.get_key_process("status", "server3")
    if server3_status:
        print(f"✓ server3 status: state={server3_status.get('state')}, "
              f"uptime={server3_status.get('uptime')}")
    else:
        print("✗ FAILED: Could not retrieve server3 status")
        all_verified = False

    # Clean shutdown (will automatically export to parquet)
    print("\n" + "=" * 60)
    print("CLEAN SHUTDOWN (with automatic parquet export)")
    print("=" * 60)
    storage.close()
    print("✓ Clean shutdown completed")

    stats_after_close = storage.get_stats()
    print(f"Final stats: {stats_after_close}")

    # Verify parquet structure
    print("\n" + "=" * 60)
    print("VERIFYING PARQUET STRUCTURE")
    print("=" * 60)
    verify_parquet_structure(config.parquet_path)

    # Final result
    print("\n" + "=" * 60)
    if all_verified:
        print("✓ ALL TESTS PASSED!")
    else:
        print("✗ SOME TESTS FAILED!")
    print("=" * 60)


def verify_parquet_structure(parquet_path: str):
    """Verify the Hive-partitioned parquet structure."""
    import pandas as pd
    from pathlib import Path

    base_path = Path(parquet_path)

    if not base_path.exists():
        print(f"✗ FAILED: Parquet path does not exist: {parquet_path}")
        return

    # Find all parquet files
    parquet_files = list(base_path.rglob("*.parquet"))
    print(f"\nFound {len(parquet_files)} parquet file(s)")

    # Check partition structure
    expected_processes = ["server1", "server2", "server3"]
    date_str = dt.date.today().isoformat()

    for process in expected_processes:
        partition_path = base_path / f"process_name={process}" / f"date={date_str}"
        if partition_path.exists():
            files = list(partition_path.glob("*.parquet"))
            print(f"✓ Partition process_name={process}, date={date_str}: "
                  f"{len(files)} file(s)")
        else:
            print(f"✗ FAILED: Missing partition for process={process}")

    # Read and verify parquet data
    if parquet_files:
        print("\nReading parquet data to verify contents...")
        df = pd.read_parquet(parquet_path)
        print(f"✓ Total records in parquet: {len(df)}")
        print(f"✓ Columns: {list(df.columns)}")
        print(f"✓ Unique keys: {df['key'].unique().tolist()}")
        print(f"✓ Unique processes: {df['process_name'].unique().tolist()}")

        # Show sample data
        print("\nSample data from parquet:")
        print(df[["key", "process_name", "timestamp", "status"]].head(10))


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage:")
        print("  Stage 1 (write and crash): python test_crash_recovery.py 1")
        print("  Stage 2 (recover): python test_crash_recovery.py 2")
        sys.exit(1)

    stage = sys.argv[1]

    if stage == "1":
        stage1_write_and_crash()
    elif stage == "2":
        stage2_recover_and_verify()
    else:
        print(f"Invalid stage: {stage}. Use '1' or '2'")
        sys.exit(1)
