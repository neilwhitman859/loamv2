#!/usr/bin/env python3
"""
Overnight TTB image download orchestrator.
Waits for the C:->D: move to finish, then downloads remaining labels + scans.

Usage: python overnight_download.py
"""

import shutil
import subprocess
import sys
import time
from pathlib import Path

SRC_LABELS = Path(r"C:\Users\neilw\Desktop\Label Images\labels")
DST_LABELS = Path(r"D:\TTB Label Images\labels")


def wait_for_move():
    """Wait until C: source directory is empty (move complete)."""
    print("Waiting for C: -> D: move to complete...")
    while True:
        if not SRC_LABELS.exists():
            print("  Source directory gone — move complete!")
            return

        # Fast check: count year directories remaining (not individual files)
        year_dirs = [d for d in SRC_LABELS.iterdir() if d.is_dir()]
        if not year_dirs:
            print("  No year directories left — move complete!")
            return

        c_free = shutil.disk_usage("C:/").free / 1e9
        d_used = (shutil.disk_usage("D:/").total - shutil.disk_usage("D:/").free) / 1e9
        years_left = sorted([d.name for d in year_dirs])
        print(f"  C: {c_free:.0f} GB free | D: {d_used:.0f} GB used | {len(years_left)} years left: {years_left[0]}..{years_left[-1]}")

        time.sleep(60)  # Check every minute


def run_download(download_type, concurrent):
    """Run the TTB image downloader."""
    print(f"\n{'='*60}")
    print(f"Starting {download_type} download (concurrent={concurrent})")
    print(f"{'='*60}\n")

    cmd = [
        sys.executable, "-m", "pipeline.fetch.ttb_image_downloader",
        "--type", download_type,
        "--concurrent", str(concurrent),
    ]
    result = subprocess.run(cmd, cwd=r"C:\Users\neilw\Documents\GitHub\loamv2")
    return result.returncode


def main():
    print("=== Overnight TTB Download Orchestrator ===\n")

    # Step 1: Finish moving remaining files from C: to D:
    print("--- Phase 0: Finishing C: -> D: move ---")
    import subprocess as sp
    sp.run([sys.executable, "-m", "pipeline.analyze.move_images"],
           cwd=r"C:\Users\neilw\Documents\GitHub\loamv2")

    # Step 2: Download remaining labels
    print("\n--- Phase 1: Remaining Labels ---")
    run_download("labels", 35)

    # Step 3: Download scans
    print("\n--- Phase 2: Scans ---")
    run_download("scans", 35)

    print("\n=== All done! ===")
    d_used = (shutil.disk_usage("D:/").total - shutil.disk_usage("D:/").free) / 1e9
    print(f"D: {d_used:.0f} GB used")


if __name__ == "__main__":
    main()
