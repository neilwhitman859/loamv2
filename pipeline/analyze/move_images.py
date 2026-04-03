#!/usr/bin/env python3
"""Move TTB label images from C: to D: with progress tracking."""

import shutil
import time
from pathlib import Path

SRC = Path(r"C:\Users\neilw\Desktop\Label Images\labels")
DST = Path(r"D:\TTB Label Images\labels")


def main():
    years = sorted([y for y in SRC.iterdir() if y.is_dir()])
    print(f"Found {len(years)} year directories to move")

    total_moved = 0
    total_size = 0
    start = time.time()

    for year_dir in years:
        year = year_dir.name
        dst_year = DST / year
        dst_year.mkdir(parents=True, exist_ok=True)

        colas = sorted([c for c in year_dir.iterdir() if c.is_dir()])
        year_start = time.time()
        year_count = 0

        for cola_dir in colas:
            try:
                dst_cola = dst_year / cola_dir.name
                if dst_cola.exists():
                    # Already transferred (test copy or partial), just delete source
                    shutil.rmtree(cola_dir, ignore_errors=True)
                else:
                    shutil.copytree(cola_dir, dst_cola)
                    shutil.rmtree(cola_dir, ignore_errors=True)
            except Exception as e:
                print(f"\n  WARN: {cola_dir.name}: {e}")
                continue

            year_count += 1
            total_moved += 1

            if year_count % 500 == 0:
                elapsed = time.time() - start
                rate = total_moved / elapsed if elapsed > 0 else 0
                print(
                    f"  {year}: {year_count:,}/{len(colas):,}  "
                    f"Total: {total_moved:,}  {rate:.0f} COLAs/sec",
                    end="\r",
                )

        year_elapsed = time.time() - year_start
        elapsed = time.time() - start
        print(
            f"  {year}: {len(colas):,} COLAs moved in {year_elapsed:.0f}s  "
            f"(Total: {total_moved:,}, {elapsed/60:.0f}min elapsed)          "
        )

        # Remove empty year directory from source
        try:
            year_dir.rmdir()
        except OSError:
            pass

    elapsed = time.time() - start
    print(f"\nDone! {total_moved:,} COLAs moved in {elapsed/60:.1f} min")

    c_free = shutil.disk_usage("C:/").free / 1e9
    d_used = (shutil.disk_usage("D:/").total - shutil.disk_usage("D:/").free) / 1e9
    print(f"C: {c_free:.0f} GB free | D: {d_used:.0f} GB used")


if __name__ == "__main__":
    main()
