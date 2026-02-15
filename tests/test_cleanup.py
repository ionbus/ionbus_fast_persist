"""Cleanup utility for test artifacts.

Run standalone: python test_cleanup.py
Or import: from test_cleanup import cleanup_test_artifacts
"""

from __future__ import annotations

import os
import shutil
import stat
import sys
from pathlib import Path


def _handle_remove_readonly(func, path, exc):
    """Error handler for Windows readonly files."""
    # Clear readonly bit and try again
    try:
        os.chmod(path, stat.S_IWRITE)
        func(path)
    except Exception:
        # If we still can't delete, just skip it
        pass


def cleanup_test_artifacts(verbose: bool = True) -> int:
    """Remove all test storage directories.

    Args:
        verbose: Print what's being deleted

    Returns:
        Number of directories removed
    """
    # Test directories created by various test scripts
    test_dirs = [
        "./wal_storage",
        "./test_output",
        "./crash_test_output",
        "./collection_test_storage",
        "./crash_test_collection",
        "./test_history_retention",
        "./test_process_name_none",
        "./test_timestamp_username",
        "./test_data_persistence",
        "./test_wal_metadata_recovery",
    ]

    removed_count = 0

    for dir_path in test_dirs:
        path = Path(dir_path)
        if path.exists():
            try:
                # Use onerror handler for Windows readonly files
                shutil.rmtree(path, onerror=_handle_remove_readonly)
                removed_count += 1
                if verbose:
                    print(f"[OK] Removed: {dir_path}")
            except Exception as e:
                if verbose:
                    print(f"[FAIL] Failed to remove {dir_path}: {e}")
        elif verbose:
            # Only show if verbose and nothing to clean
            pass

    return removed_count


if __name__ == "__main__":
    print("Cleaning up test artifacts...")
    count = cleanup_test_artifacts(verbose=True)

    if count == 0:
        print("No test artifacts found.")
        sys.exit(0)
    else:
        print(f"\nCleaned up {count} director{'y' if count == 1 else 'ies'}.")
        sys.exit(0)
