"""Test and demonstration script for fast_persist."""

from __future__ import annotations

import datetime as dt
import random

from fast_persist_claude import WALDuckDBStorage, WALConfig, StorageKeys


if __name__ == "__main__":
    # Configure the storage system
    config = WALConfig(
        base_dir="./wal_storage",
        max_wal_size=1024 * 1024,  # 1MB per WAL
        batch_size=100,  # Flush every 100 records
        flush_interval_seconds=10,  # Or every 10 seconds
    )

    # Initialize storage with today's date
    storage = WALDuckDBStorage(dt.date.today(), "data.duckdb", config)

    # Simulate async updates with multi-process tracking
    def generate_update():
        task_id = random.choice(["task_1", "task_2", "task_3"])
        worker_id = random.choice(["worker1", "worker2", "worker3", None])
        data = {
            "value": random.randint(1, 1000),
            StorageKeys.TIMESTAMP: dt.datetime.now().isoformat(),
            StorageKeys.STATUS: random.choice(
                ["running", "completed", "failed"]
            ),
            StorageKeys.STATUS_INT: random.randint(0, 2),
            "metadata": {"source": "simulator"},
        }
        if worker_id:
            data[StorageKeys.PROCESS_NAME] = worker_id
        return task_id, data, worker_id

    # Test writes
    print("Writing test data...")
    for i in range(250):
        key, data, worker_id = generate_update()
        storage.store(key, data)

        if i % 50 == 0:
            stats = storage.get_stats()
            print(f"After {i} writes: {stats}")

    # Force final flush
    storage.force_flush()

    # Check final stats
    final_stats = storage.get_stats()
    print(f"Final stats: {final_stats}")

    # Test retrieval
    print("\nTesting multi-process retrieval:")
    all_task1 = storage.get_key("task_1")
    if all_task1:
        print(f"task_1 has {len(all_task1)} processes:")
        for proc_name, data in all_task1.items():
            print(f"  {proc_name}: status={data.get('status')}")

    # Test specific process retrieval
    worker1_task1 = storage.get_key_process("task_1", "worker1")
    if worker1_task1:
        print(f"\ntask_1/worker1 data: {worker1_task1}")

    # Clean shutdown
    storage.close()

    print("\nRestarting to test recovery...")

    # Test recovery by creating new instance
    storage2 = WALDuckDBStorage(dt.date.today(), "data.duckdb", config)
    recovery_stats = storage2.get_stats()
    print(f"After recovery: {recovery_stats}")

    # Verify data
    if storage2.cache:
        sample_key = list(storage2.cache.keys())[0]
        all_processes = storage2.get_key(sample_key)
        print(f"\nSample recovered key: {sample_key}")
        if all_processes:
            print(f"  Processes: {list(all_processes.keys())}")
            sample_proc = list(all_processes.keys())[0]
            print(
                f"  Sample data for {sample_proc}: "
                f"{storage2.get_key_process(sample_key, sample_proc)}"
            )

    storage2.close()
