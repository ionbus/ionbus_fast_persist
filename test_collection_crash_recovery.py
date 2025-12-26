"""Two-stage crash recovery test for collection_fast_persist.

Stage 1: Write data and kill process while WAL files exist
Stage 2: Recover from WAL, verify data, clean shutdown, verify backups
"""

from __future__ import annotations

import datetime as dt
import sys
from pathlib import Path

from collection_fast_persist import (
    CollectionConfig,
    CollectionFastPersist,
)
from fast_persist_common import StorageKeys


def stage1_write_and_crash():
    """Stage 1: Write data and simulate crash before clean shutdown."""
    print("=" * 60)
    print("STAGE 1: Writing data and simulating crash")
    print("=" * 60)

    config = CollectionConfig(
        base_dir="./crash_test_collection",
        max_wal_size=1024 * 512,  # 512KB - small for testing
        batch_size=50,  # Small batch to ensure multiple WAL files
        duckdb_flush_interval_seconds=60,  # Long interval to keep data in WAL
        retain_days=3,
    )

    storage = CollectionFastPersist(dt.date.today(), config=config)

    # Test data: User profiles with typed values
    print("\nWriting user profile data...")
    user_profiles = [
        ("john_doe", "personal_info", "age", 32, "Age"),
        ("john_doe", "personal_info", "height", 1.75, "Height (m)"),
        ("john_doe", "personal_info", "name", "John Doe", "Full Name"),
        ("john_doe", "contact_info", "email", "john@example.com", "Email"),
        ("john_doe", "contact_info", "phone", "555-1234", "Phone"),
        ("jane_smith", "personal_info", "age", 28, "Age"),
        ("jane_smith", "personal_info", "height", 1.68, "Height (m)"),
        ("jane_smith", "personal_info", "name", "Jane Smith", "Full Name"),
        ("jane_smith", "contact_info", "email", "jane@example.com", "Email"),
        ("jane_smith", "contact_info", "phone", "555-5678", "Phone"),
    ]

    for user_key, collection, item, value, label in user_profiles:
        storage.store(
            key=user_key,
            data={
                "label": label,
                StorageKeys.TIMESTAMP: dt.datetime.now().isoformat(),
                StorageKeys.STATUS: "active",
                StorageKeys.STATUS_INT: 1,
                StorageKeys.USERNAME: "admin",
            },
            item_name=item,
            collection_name=collection,
            value=value,
        )
        print(
            f"  Stored: {user_key}/{collection}/{item} = {value} "
            f"({type(value).__name__})"
        )

    # Test data: Classroom seating
    print("\nWriting classroom seating data...")
    for seat_num in range(1, 11):
        storage.store(
            key="classroom_seating",
            data={
                "student_id": f"STU{1000 + seat_num}",
                "assigned_by": "teacher_smith",
                StorageKeys.TIMESTAMP: dt.datetime.now().isoformat(),
                StorageKeys.STATUS: "occupied",
            },
            item_name=f"seat_{seat_num}",
            collection_name="room_a",
            value=seat_num,
        )
        print(f"  Assigned: room_a/seat_{seat_num} to STU{1000 + seat_num}")

    # Test data: Form fields with mixed types
    print("\nWriting form data...")
    form_data = [
        ("registration_form", "user_input", "subscribe", 1, "Subscribe"),
        ("registration_form", "user_input", "age_verified", 1, "Age Verified"),
        ("registration_form", "user_input", "terms_accepted", 1, "Terms"),
        (
            "registration_form",
            "user_input",
            "comments",
            "Looking forward!",
            "Comments",
        ),
    ]

    for key, collection, item, value, label in form_data:
        storage.store(
            key=key,
            data={
                "label": label,
                StorageKeys.TIMESTAMP: dt.datetime.now().isoformat(),
            },
            item_name=item,
            collection_name=collection,
            value=value,
        )
        print(f"  Form: {item} = {value}")

    # Check stats to see WAL files
    stats = storage.get_stats()
    print(f"\nStats before crash: {stats}")
    print(f"  WAL files: {stats['wal_files_count']}")
    print(f"  Pending writes: {stats['pending_writes']}")
    print(f"  Cache size: {stats['cache_size']}")
    print(f"  Modified records: {stats['modified_records']}")

    # Verify data in cache
    print("\nData in cache before crash:")
    john_personal = storage.get_key("john_doe", "personal_info")
    if john_personal:
        print(f"  john_doe/personal_info: {len(john_personal)} items")

    room_a = storage.get_key("classroom_seating", "room_a")
    if room_a:
        print(f"  classroom_seating/room_a: {len(room_a)} seats")

    form_input = storage.get_key("registration_form", "user_input")
    if form_input:
        print(f"  registration_form/user_input: {len(form_input)} fields")

    # SIMULATE CRASH - exit without calling close()
    print("\n" + "!" * 60)
    print("SIMULATING CRASH - Exiting without clean shutdown!")
    print(f"Leaving {stats['wal_files_count']} WAL file(s) behind")
    print(f"Latest table NOT updated ({stats['modified_records']} changes)")
    print("!" * 60)

    # Force exit without cleanup
    sys.exit(0)


def stage2_recover_and_verify():
    """Stage 2: Recover from WAL files and verify data integrity."""
    print("=" * 60)
    print("STAGE 2: Recovering from crash")
    print("=" * 60)

    config = CollectionConfig(
        base_dir="./crash_test_collection",
        max_wal_size=1024 * 512,
        batch_size=50,
        duckdb_flush_interval_seconds=60,
        retain_days=3,
    )

    # Check for stale lock file (should be auto-cleaned now)
    base_path = Path(config.base_dir)
    date_str = dt.date.today().isoformat()
    lock_file = base_path / f".lock_{date_str}"
    if lock_file.exists():
        print(f"\nStale lock file detected: {lock_file}")
        print("  (Will be automatically removed during initialization)")

    print("\nInitializing storage (will trigger recovery)...")
    print("  - Automatic stale lock detection enabled")
    storage = CollectionFastPersist(dt.date.today(), config=config)

    # Check recovery stats
    stats = storage.get_stats()
    print(f"\nStats after recovery: {stats}")
    print(f"  Cache size: {stats['cache_size']}")
    print(f"  Pending writes: {stats['pending_writes']}")
    print(f"  WAL files: {stats['wal_files_count']}")
    print(f"  Modified records: {stats['modified_records']}")

    # Verify all data was recovered
    print("\n" + "=" * 60)
    print("VERIFYING RECOVERED DATA")
    print("=" * 60)

    all_verified = True

    # Verify user profiles
    print("\n[Test 1] User Profiles")
    john_personal = storage.get_key("john_doe", "personal_info")
    if john_personal and len(john_personal) == 3:
        print(f"✓ john_doe/personal_info: {len(john_personal)} items")
        age = john_personal.get("age", {}).get("value")
        height = john_personal.get("height", {}).get("value")
        name = john_personal.get("name", {}).get("value")
        if isinstance(age, int) and age == 32:
            print(f"  ✓ age: {age} (int)")
        else:
            print(f"  ✗ FAILED: age = {age} (expected 32 int)")
            all_verified = False
        if isinstance(height, float) and abs(height - 1.75) < 0.01:
            print(f"  ✓ height: {height} (float)")
        else:
            print(f"  ✗ FAILED: height = {height} (expected 1.75 float)")
            all_verified = False
        if isinstance(name, str) and name == "John Doe":
            print(f"  ✓ name: {name} (str)")
        else:
            print(f"  ✗ FAILED: name = {name}")
            all_verified = False
    else:
        print(f"✗ FAILED: john_doe/personal_info missing or incomplete")
        all_verified = False

    john_contact = storage.get_key("john_doe", "contact_info")
    if john_contact and len(john_contact) == 2:
        print(f"✓ john_doe/contact_info: {len(john_contact)} items")
    else:
        print(f"✗ FAILED: john_doe/contact_info missing or incomplete")
        all_verified = False

    jane_personal = storage.get_key("jane_smith", "personal_info")
    if jane_personal and len(jane_personal) == 3:
        print(f"✓ jane_smith/personal_info: {len(jane_personal)} items")
    else:
        print(f"✗ FAILED: jane_smith/personal_info missing or incomplete")
        all_verified = False

    # Verify classroom seating
    print("\n[Test 2] Classroom Seating")
    room_a = storage.get_key("classroom_seating", "room_a")
    if room_a and len(room_a) == 10:
        print(f"✓ classroom_seating/room_a: {len(room_a)} seats")
        # Check specific seat
        seat_5 = room_a.get("seat_5", {})
        if seat_5.get("value") == 5:
            print(f"  ✓ seat_5: {seat_5.get('value')}")
        else:
            print(f"  ✗ FAILED: seat_5 value incorrect")
            all_verified = False
    else:
        print(f"✗ FAILED: classroom_seating/room_a missing or incomplete")
        all_verified = False

    # Verify form data
    print("\n[Test 3] Form Data")
    form_input = storage.get_key("registration_form", "user_input")
    if form_input and len(form_input) == 4:
        print(f"✓ registration_form/user_input: {len(form_input)} fields")
        comments = form_input.get("comments", {}).get("value")
        if comments == "Looking forward!":
            print(f"  ✓ comments: {comments}")
        else:
            print(f"  ✗ FAILED: comments = {comments}")
            all_verified = False
    else:
        print(f"✗ FAILED: registration_form/user_input missing or incomplete")
        all_verified = False

    # Test specific item retrieval
    print("\n" + "=" * 60)
    print("TESTING SPECIFIC ITEM QUERIES")
    print("=" * 60)

    jane_age = storage.get_item("jane_smith", "personal_info", "age")
    if jane_age and jane_age.get("value") == 28:
        print(
            f"\n✓ jane_smith age: {jane_age.get('value')} "
            f"(type={type(jane_age.get('value')).__name__})"
        )
    else:
        print(f"\n✗ FAILED: Could not retrieve jane_smith age correctly")
        all_verified = False

    seat_1 = storage.get_item("classroom_seating", "room_a", "seat_1")
    if seat_1:
        print(
            f"✓ room_a/seat_1: Student {seat_1.get('student_id')}, "
            f"Seat #{seat_1.get('value')}"
        )
    else:
        print(f"✗ FAILED: Could not retrieve seat_1")
        all_verified = False

    # Database health checks
    print("\n" + "=" * 60)
    print("DATABASE HEALTH CHECKS")
    print("=" * 60)

    history_healthy = storage.check_database_health(
        storage.history_db_path, "storage_history", storage.history_conn
    )
    latest_healthy = storage.check_database_health(
        storage.latest_db_path, "storage_latest", storage.latest_conn
    )

    print(f"\n✓ History DB health: {'OK' if history_healthy else 'FAILED'}")
    print(f"✓ Latest DB health: {'OK' if latest_healthy else 'FAILED'}")

    if not history_healthy or not latest_healthy:
        all_verified = False

    # Clean shutdown (will update latest table and create backups)
    print("\n" + "=" * 60)
    print("CLEAN SHUTDOWN")
    print("=" * 60)
    print("  - Flushing pending writes to history DB")
    print("  - Updating latest values table")
    print("  - Closing database connections")
    print("  - Creating database backups")
    print("  - Cleaning up WAL files")
    storage.close()
    print("✓ Clean shutdown completed")

    stats_after_close = storage.get_stats()
    print(f"Final stats: {stats_after_close}")

    # Verify backup files were created
    print("\n" + "=" * 60)
    print("VERIFYING BACKUPS")
    print("=" * 60)
    verify_backups(config.base_dir)

    # Final result
    print("\n" + "=" * 60)
    if all_verified:
        print("✓ ALL TESTS PASSED!")
    else:
        print("✗ SOME TESTS FAILED!")
    print("=" * 60)


def verify_backups(base_dir: str):
    """Verify backup files were created."""
    from pathlib import Path

    base_path = Path(base_dir)
    date_str = dt.date.today().isoformat()
    backup_dir = base_path / date_str

    if not backup_dir.exists():
        print(f"✗ FAILED: Backup directory does not exist: {backup_dir}")
        return

    print(f"\nBackup directory: {backup_dir}")

    # Check for backup files
    history_backup = backup_dir / "storage_history.duckdb.backup"
    latest_backup = backup_dir / "storage_latest.duckdb.backup"

    if history_backup.exists():
        size = history_backup.stat().st_size
        print(f"✓ History backup exists: {size:,} bytes")
    else:
        print(f"✗ FAILED: History backup missing")

    if latest_backup.exists():
        size = latest_backup.stat().st_size
        print(f"✓ Latest backup exists: {size:,} bytes")
    else:
        print(f"✗ FAILED: Latest backup missing")

    # Check that WAL files were cleaned up
    wal_files = list(backup_dir.glob("wal_*.jsonl"))
    if len(wal_files) == 0:
        print(f"✓ WAL files cleaned up (0 remaining)")
    else:
        print(
            f"✗ FAILED: {len(wal_files)} WAL files still present "
            f"(should be 0)"
        )


def test_reconstruction():
    """Stage 3: Test manual reconstruction functions."""
    print("\n" + "=" * 60)
    print("STAGE 3: Testing Manual Reconstruction")
    print("=" * 60)

    config = CollectionConfig(
        base_dir="./crash_test_collection",
        max_wal_size=1024 * 512,
        batch_size=50,
        duckdb_flush_interval_seconds=60,
        retain_days=3,
    )

    # Initialize storage
    storage = CollectionFastPersist(dt.date.today(), config=config)

    # Test rebuild_latest_from_history
    print("\nTesting rebuild_latest_from_history()...")
    count = storage.rebuild_latest_from_history()
    print(f"✓ Rebuilt {count} records in latest table")

    # Verify data still works
    john_age = storage.get_item("john_doe", "personal_info", "age")
    if john_age and john_age.get("value") == 32:
        print(f"✓ Data verification after rebuild: age = {john_age.get('value')}")
    else:
        print(f"✗ FAILED: Data verification after rebuild")

    storage.close()
    print("\n✓ Reconstruction test completed")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage:")
        print(
            "  Stage 1 (write and crash): "
            "python test_collection_crash_recovery.py 1"
        )
        print(
            "  Stage 2 (recover): "
            "python test_collection_crash_recovery.py 2"
        )
        print(
            "  Stage 3 (test reconstruction): "
            "python test_collection_crash_recovery.py 3"
        )
        sys.exit(1)

    stage = sys.argv[1]

    if stage == "1":
        stage1_write_and_crash()
    elif stage == "2":
        stage2_recover_and_verify()
    elif stage == "3":
        test_reconstruction()
    else:
        print(f"Invalid stage: {stage}. Use '1', '2', or '3'")
        sys.exit(1)
