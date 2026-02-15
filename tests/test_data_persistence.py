"""Test that data persists across restarts (no DROP TABLE bug).

This test verifies that the catastrophic DROP TABLE bug is fixed
by ensuring data written in one session is available in the next.
"""

from __future__ import annotations

import datetime as dt

from ionbus_fast_persist import WALConfig, WALDuckDBStorage

if __name__ == "__main__":
    print("Testing data persistence across restarts...")
    print("=" * 60)

    config = WALConfig(base_dir="./test_data_persistence")

    # Session 1: Write data
    print("\n[Session 1] Writing initial data...")
    storage1 = WALDuckDBStorage(dt.date.today(), "data.duckdb", config)

    storage1.store(
        key="user_1",
        data={"name": "Alice", "score": 100},
        process_name="game",
    )
    storage1.store(
        key="user_2",
        data={"name": "Bob", "score": 200},
        process_name="game",
    )

    print("  Stored: user_1 (Alice, score=100)")
    print("  Stored: user_2 (Bob, score=200)")

    # Flush and close
    storage1.flush_data_to_duckdb()
    storage1.close()
    print("  Closed session 1")

    # Session 2: Verify data still exists
    print("\n[Session 2] Reopening and verifying data...")
    storage2 = WALDuckDBStorage(dt.date.today(), "data.duckdb", config)

    user1 = storage2.get_key_process("user_1", "game")
    user2 = storage2.get_key_process("user_2", "game")

    # Verify data
    success = True

    if user1 and user1.get("name") == "Alice" and user1.get("score") == 100:
        print("  ✓ user_1 data intact: Alice, score=100")
    else:
        print(f"  ✗ FAILED: user_1 = {user1}")
        success = False

    if user2 and user2.get("name") == "Bob" and user2.get("score") == 200:
        print("  ✓ user_2 data intact: Bob, score=200")
    else:
        print(f"  ✗ FAILED: user_2 = {user2}")
        success = False

    # Session 3: Add more data and verify both old and new exist
    print("\n[Session 3] Adding new data alongside existing...")
    storage2.store(
        key="user_3",
        data={"name": "Charlie", "score": 300},
        process_name="game",
    )
    print("  Stored: user_3 (Charlie, score=300)")

    # Verify all three exist
    all_users = storage2.get_key("user_1")
    if all_users:
        print(f"  Cache contains: {len(all_users)} process(es)")

    user1_check = storage2.get_key_process("user_1", "game")
    user3_check = storage2.get_key_process("user_3", "game")

    if user1_check and user3_check:
        print("  ✓ Both old (user_1) and new (user_3) data coexist")
    else:
        print("  ✗ FAILED: Old or new data missing")
        success = False

    storage2.flush_data_to_duckdb()
    storage2.close()

    # Final verification
    print("\n[Session 4] Final verification after multiple cycles...")
    storage3 = WALDuckDBStorage(dt.date.today(), "data.duckdb", config)

    final_user1 = storage3.get_key_process("user_1", "game")
    final_user2 = storage3.get_key_process("user_2", "game")
    final_user3 = storage3.get_key_process("user_3", "game")

    if final_user1 and final_user2 and final_user3:
        print("  ✓ All 3 users persist across multiple sessions!")
        print(f"    user_1: {final_user1.get('name')}")
        print(f"    user_2: {final_user2.get('name')}")
        print(f"    user_3: {final_user3.get('name')}")
    else:
        print("  ✗ FAILED: Some data lost")
        success = False

    storage3.close()

    print("\n" + "=" * 60)
    if success:
        print("✓ SUCCESS: Data persists correctly (no DROP TABLE bug)!")
    else:
        print("✗ FAILED: Data persistence issues detected")
