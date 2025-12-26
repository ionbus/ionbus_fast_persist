"""Test parse_timestamp timezone handling."""

from __future__ import annotations

import datetime as dt

from fast_persist_common import parse_timestamp

if __name__ == "__main__":
    print("Testing parse_timestamp timezone handling...")
    print("=" * 60)

    # Test with timezone
    ts_with_tz = "2025-01-15T10:30:00-05:00"
    result1 = parse_timestamp(ts_with_tz)
    print(f"\nWith TZ: {result1}")
    print(f"  tzinfo: {result1.tzinfo}")

    # Test without timezone (should assume UTC)
    ts_no_tz = "2025-01-15T10:30:00"
    result2 = parse_timestamp(ts_no_tz)
    print(f"\nWithout TZ: {result2}")
    print(f"  tzinfo: {result2.tzinfo}")

    # Test with Z (should convert to UTC)
    ts_with_z = "2025-01-15T10:30:00Z"
    result3 = parse_timestamp(ts_with_z)
    print(f"\nWith Z: {result3}")
    print(f"  tzinfo: {result3.tzinfo}")

    # Verify
    print("\n" + "=" * 60)
    if result2.tzinfo == dt.timezone.utc:
        print("✓ SUCCESS: Naive timestamp assumed UTC!")
    else:
        print(f"✗ FAILED: Expected UTC, got {result2.tzinfo}")

    if result3.tzinfo == dt.timezone.utc:
        print("✓ SUCCESS: Z converted to UTC!")
    else:
        print(f"✗ FAILED: Expected UTC, got {result3.tzinfo}")

    print("\nTest complete!")
