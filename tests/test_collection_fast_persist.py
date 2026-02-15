"""Test and demonstration script for collection_fast_persist."""

from __future__ import annotations

import datetime as dt
import site
from pathlib import Path

parent_dir = Path(__file__).parent.parent.parent
site.addsitedir(str(parent_dir))

from ionbus_fast_persist import (
    CollectionConfig,
    CollectionFastPersist,
    StorageKeys,
)


if __name__ == "__main__":
    # Configure the storage system
    config = CollectionConfig(
        base_dir="./collection_test_storage",
        max_wal_size=1024 * 512,  # 512KB for testing
        batch_size=50,  # Small batch for testing
        duckdb_flush_interval_seconds=10,
        retain_days=3,
    )

    # Initialize storage with today's date
    storage = CollectionFastPersist(dt.date.today(), config=config)

    print("=" * 60)
    print("Testing Collection Fast Persist")
    print("=" * 60)

    # Test 1: Store form data with typed values
    print("\n[Test 1] Storing form data with typed values...")

    # Form: user_profile, Collection: personal_info
    storage.store(
        key="user_profile",
        data={
            "label": "Full Name",
            StorageKeys.TIMESTAMP: dt.datetime.now().isoformat(),
            StorageKeys.STATUS: "valid",
            StorageKeys.USERNAME: "john_doe",
        },
        item_name="full_name",
        collection_name="personal_info",
        value="John Doe",  # String value
    )

    storage.store(
        key="user_profile",
        data={
            "label": "Age",
            StorageKeys.TIMESTAMP: dt.datetime.now().isoformat(),
            StorageKeys.STATUS: "valid",
            StorageKeys.USERNAME: "john_doe",
        },
        item_name="age",
        collection_name="personal_info",
        value=32,  # Integer value
    )

    storage.store(
        key="user_profile",
        data={
            "label": "Height (meters)",
            StorageKeys.TIMESTAMP: dt.datetime.now().isoformat(),
            StorageKeys.STATUS: "valid",
            StorageKeys.USERNAME: "john_doe",
        },
        item_name="height",
        collection_name="personal_info",
        value=1.75,  # Float value
    )

    # Form: user_profile, Collection: contact_info
    storage.store(
        key="user_profile",
        data={
            "label": "Email",
            StorageKeys.TIMESTAMP: dt.datetime.now().isoformat(),
            StorageKeys.STATUS: "verified",
            StorageKeys.USERNAME: "john_doe",
        },
        item_name="email",
        collection_name="contact_info",
        value="john@example.com",
    )

    storage.store(
        key="user_profile",
        data={
            "label": "Phone",
            StorageKeys.TIMESTAMP: dt.datetime.now().isoformat(),
            StorageKeys.STATUS: "pending",
            StorageKeys.USERNAME: "john_doe",
        },
        item_name="phone",
        collection_name="contact_info",
        value="555-1234",
    )

    print("[OK] Stored 5 items across 2 collections")

    # Test 2: Retrieve entire collections
    print("\n[Test 2] Retrieving collections...")

    personal_info = storage.get_key("user_profile", "personal_info")
    if personal_info:
        print(f"\nPersonal Info Collection ({len(personal_info)} items):")
        for item_name, data in personal_info.items():
            value = data.get("value")
            value_type = type(value).__name__
            print(
                f"  {item_name}: value={value} (type={value_type}), "
                f"label={data.get('label')}"
            )

    contact_info = storage.get_key("user_profile", "contact_info")
    if contact_info:
        print(f"\nContact Info Collection ({len(contact_info)} items):")
        for item_name, data in contact_info.items():
            status = data.get(StorageKeys.STATUS)
            print(
                f"  {item_name}: {data.get('value')} "
                f"(status={status})"
            )

    # Test 3: Retrieve specific items
    print("\n[Test 3] Retrieving specific items...")

    age_data = storage.get_item("user_profile", "personal_info", "age")
    if age_data:
        print(
            f"[OK] Age: {age_data.get('value')} "
            f"(type={type(age_data.get('value')).__name__})"
        )

    email_data = storage.get_item("user_profile", "contact_info", "email")
    if email_data:
        print(
            f"[OK] Email: {email_data.get('value')} "
            f"(status={email_data.get(StorageKeys.STATUS)})"
        )

    # Test 4: Update values with type changes
    print("\n[Test 4] Updating values (with type changes)...")

    # Update age from int to string
    storage.store(
        key="user_profile",
        data={
            "label": "Age",
            StorageKeys.TIMESTAMP: dt.datetime.now().isoformat(),
            StorageKeys.STATUS: "valid",
            StorageKeys.USERNAME: "john_doe",
        },
        item_name="age",
        collection_name="personal_info",
        value="thirty-two",  # Changed from int to string!
    )

    age_data_updated = storage.get_item(
        "user_profile", "personal_info", "age"
    )
    if age_data_updated:
        print(
            f"[OK] Updated age: {age_data_updated.get('value')} "
            f"(type={type(age_data_updated.get('value')).__name__})"
        )

    # Test 5: Seat assignment use case
    print("\n[Test 5] Seat assignment use case...")

    # Classroom A seats
    for seat_num in range(1, 6):
        storage.store(
            key="classroom_seating",
            data={
                "student_id": f"STU{1000 + seat_num}",
                "assigned_by": "teacher_smith",
                StorageKeys.TIMESTAMP: dt.datetime.now().isoformat(),
                StorageKeys.STATUS: "occupied",
            },
            item_name=f"seat_{seat_num}",
            collection_name="classroom_a",
            value=seat_num,  # Seat number as int
        )

    # Classroom B seats
    for seat_num in range(1, 4):
        storage.store(
            key="classroom_seating",
            data={
                "student_id": f"STU{2000 + seat_num}",
                "assigned_by": "teacher_jones",
                StorageKeys.TIMESTAMP: dt.datetime.now().isoformat(),
                StorageKeys.STATUS: "occupied",
            },
            item_name=f"seat_{seat_num}",
            collection_name="classroom_b",
            value=seat_num,
        )

    print("[OK] Assigned 8 seats across 2 classrooms")

    # Retrieve classroom A seats
    classroom_a = storage.get_key("classroom_seating", "classroom_a")
    if classroom_a:
        print(f"\nClassroom A ({len(classroom_a)} seats):")
        for seat, data in classroom_a.items():
            print(
                f"  {seat}: Student {data.get('student_id')} "
                f"(seat #{data.get('value')})"
            )

    # Force flush to database
    print("\n[Test 6] Flushing to database...")
    storage.flush_data_to_duckdb()
    stats = storage.get_stats()
    print(f"Stats after flush: {stats}")

    # Test 7: Database health check
    print("\n[Test 7] Database health checks...")

    # Use existing connections to avoid read-only conflict
    history_healthy = storage.check_database_health(
        storage.history_db_path, "storage_history", storage.history_conn
    )
    latest_healthy = storage.check_database_health(
        storage.latest_db_path, "storage_latest", storage.latest_conn
    )

    print(f"[OK] History DB health: {'OK' if history_healthy else 'FAILED'}")
    print(f"[OK] Latest DB health: {'OK' if latest_healthy else 'FAILED'}")

    # Clean shutdown (updates latest table and creates backups)
    print("\n[Test 8] Clean shutdown...")
    print("  - Flushing pending writes")
    print("  - Updating latest values table")
    print("  - Creating database backups")
    print("  - Cleaning up WAL files")
    storage.close()

    print("\n" + "=" * 60)
    print("Testing Recovery")
    print("=" * 60)

    # Test 9: Restart and verify data
    print("\n[Test 9] Restarting and verifying data...")
    storage2 = CollectionFastPersist(dt.date.today(), config=config)

    # Verify data survived restart
    age_after_restart = storage2.get_item(
        "user_profile", "personal_info", "age"
    )
    if age_after_restart:
        print(
            f"[OK] Age after restart: {age_after_restart.get('value')} "
            f"(type={type(age_after_restart.get('value')).__name__})"
        )

    email_after_restart = storage2.get_item(
        "user_profile", "contact_info", "email"
    )
    if email_after_restart:
        print(
            f"[OK] Email after restart: {email_after_restart.get('value')}"
        )

    classroom_a_after = storage2.get_key(
        "classroom_seating", "classroom_a"
    )
    if classroom_a_after:
        print(
            f"[OK] Classroom A after restart: {len(classroom_a_after)} seats"
        )

    stats_after = storage2.get_stats()
    print(f"\nStats after restart: {stats_after}")

    storage2.close()

    print("\n" + "=" * 60)
    print("[OK] All tests completed successfully!")
    print("=" * 60)
