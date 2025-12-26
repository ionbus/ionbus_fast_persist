"""Master test runner for all fast_persist tests.

Runs all test suites in sequence and provides a summary report.
"""

from __future__ import annotations

import subprocess
import sys
import time
from pathlib import Path


def run_test(test_name: str, command: list[str]) -> tuple[bool, float, str]:
    """Run a single test and return results.

    Args:
        test_name: Display name for the test
        command: Command to execute as list

    Returns:
        Tuple of (success, duration, output)
    """
    print(f"\n{'=' * 70}")
    print(f"Running: {test_name}")
    print(f"{'=' * 70}")

    start_time = time.time()
    try:
        result = subprocess.run(
            command,
            capture_output=True,
            text=True,
            timeout=120,  # 2 minute timeout per test
        )
        duration = time.time() - start_time

        # Print output
        if result.stdout:
            print(result.stdout)
        if result.stderr:
            print("STDERR:", result.stderr, file=sys.stderr)

        success = result.returncode == 0
        status = "✓ PASSED" if success else "✗ FAILED"
        print(f"\n{status} ({duration:.2f}s)")

        return success, duration, result.stdout + result.stderr

    except subprocess.TimeoutExpired:
        duration = time.time() - start_time
        print(f"\n✗ TIMEOUT ({duration:.2f}s)")
        return False, duration, "Test timed out"
    except Exception as e:
        duration = time.time() - start_time
        print(f"\n✗ ERROR: {e}")
        return False, duration, str(e)


def main():
    """Run all test suites."""
    print("Fast Persist - Master Test Runner")
    print("=" * 70)

    # Detect Python command (conda environment aware)
    python_cmd = sys.executable

    # Define all tests
    tests = [
        # Cleanup first
        ("Cleanup: Remove old test artifacts", [python_cmd, "test_cleanup.py"]),
        # Core functionality tests
        (
            "dated_fast_persist: Basic functionality",
            [python_cmd, "test_fast_persist.py"],
        ),
        (
            "collection_fast_persist: Full test suite",
            [python_cmd, "test_collection_fast_persist.py"],
        ),
        # Specific feature tests
        (
            "collection_fast_persist: History retention",
            [python_cmd, "test_history_retention.py"],
        ),
        (
            "dated_fast_persist: process_name=None preservation",
            [python_cmd, "test_process_name_none.py"],
        ),
        (
            "Shared utilities: parse_timestamp timezone handling",
            [python_cmd, "test_parse_timestamp.py"],
        ),
        # Crash recovery tests
        (
            "dated_fast_persist: Crash simulation (Stage 1)",
            [python_cmd, "test_crash_recovery.py", "1"],
        ),
        (
            "dated_fast_persist: Recovery verification (Stage 2)",
            [python_cmd, "test_crash_recovery.py", "2"],
        ),
        (
            "collection_fast_persist: Crash simulation (Stage 1)",
            [python_cmd, "test_collection_crash_recovery.py", "1"],
        ),
        (
            "collection_fast_persist: Recovery verification (Stage 2)",
            [python_cmd, "test_collection_crash_recovery.py", "2"],
        ),
        (
            "collection_fast_persist: Manual reconstruction (Stage 3)",
            [python_cmd, "test_collection_crash_recovery.py", "3"],
        ),
        # Final cleanup
        ("Cleanup: Remove test artifacts", [python_cmd, "test_cleanup.py"]),
    ]

    # Run all tests
    results = []
    total_duration = 0

    for test_name, command in tests:
        success, duration, output = run_test(test_name, command)
        results.append((test_name, success, duration))
        total_duration += duration

        # Add delay between crash recovery stages to ensure lock detection
        if "Crash simulation" in test_name:
            print("\nWaiting 3 seconds before recovery stage...")
            time.sleep(3)

        # Stop on first failure for crash recovery tests
        # (subsequent stages depend on previous ones)
        if not success and "Crash" in test_name:
            print(
                f"\n⚠ Skipping remaining crash recovery stages "
                f"due to failure"
            )
            # Skip to next non-crash test
            continue

    # Print summary
    print("\n" + "=" * 70)
    print("TEST SUMMARY")
    print("=" * 70)

    passed = sum(1 for _, success, _ in results if success)
    failed = len(results) - passed

    for test_name, success, duration in results:
        status = "✓" if success else "✗"
        print(f"{status} {test_name:55s} ({duration:.2f}s)")

    print("=" * 70)
    print(f"Total: {len(results)} tests")
    print(f"Passed: {passed}")
    print(f"Failed: {failed}")
    print(f"Duration: {total_duration:.2f}s")
    print("=" * 70)

    if failed == 0:
        print("\n🎉 All tests passed!")
        return 0
    else:
        print(f"\n❌ {failed} test(s) failed")
        return 1


if __name__ == "__main__":
    sys.exit(main())
