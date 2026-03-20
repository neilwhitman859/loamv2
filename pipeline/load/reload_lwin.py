#!/usr/bin/env python3
"""
Reloads source_lwin staging table from data/lwin_database.csv.
Filters to wine types only (Wine, Fortified Wine, Champagne), skips Deleted rows.

Usage:
    python -m pipeline.load.reload_lwin
"""

import csv
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from pipeline.lib.db import get_supabase

DATA_DIR = Path(__file__).resolve().parents[2] / "data"
BATCH_SIZE = 500
ALLOWED_TYPES = {"Wine", "Fortified Wine", "Champagne"}


def na_val(val: str | None) -> str | None:
    if not val or val == "NA":
        return None
    return val


def map_row(row: dict) -> dict | None:
    lwin = row.get("LWIN", "")
    wine_type = row.get("TYPE", "")
    status = row.get("STATUS", "")

    if wine_type not in ALLOWED_TYPES:
        return None
    if status == "Deleted":
        return None

    return {
        "lwin": lwin,
        "lwin_7": lwin[:7] or None,
        "lwin_11": lwin[:11] if len(lwin) >= 11 else None,
        "lwin_18": lwin if len(lwin) >= 18 else None,
        "display_name": na_val(row.get("DISPLAY_NAME")),
        "producer_name": na_val(row.get("PRODUCER_NAME")),
        "wine_name": na_val(row.get("WINE")),
        "country": na_val(row.get("COUNTRY")),
        "region": na_val(row.get("REGION")),
        "sub_region": na_val(row.get("SUB_REGION")),
        "appellation": na_val(row.get("DESIGNATION")),
        "colour": na_val(row.get("COLOUR")),
        "wine_type": wine_type,
        "designation": na_val(row.get("DESIGNATION")),
        "classification": na_val(row.get("CLASSIFICATION")),
        "vintage": na_val(row.get("VINTAGE_CONFIG")),
    }


def main():
    csv_path = DATA_DIR / "lwin_database.csv"
    print(f"Reading CSV from: {csv_path}")

    sb = get_supabase()
    batch: list[dict] = []
    total_read = 0
    total_inserted = 0
    total_skipped = 0
    errors = 0

    with open(csv_path, encoding="utf-8") as f:
        reader = csv.DictReader(f)
        print(f"CSV columns: {', '.join(reader.fieldnames or [])}")

        for row in reader:
            total_read += 1
            mapped = map_row(row)

            if not mapped:
                total_skipped += 1
                continue

            batch.append(mapped)

            if len(batch) >= BATCH_SIZE:
                try:
                    sb.table("source_lwin").insert(batch).execute()
                    total_inserted += len(batch)
                except Exception as e:
                    print(f"  Error at row {total_read}: {e}")
                    errors += 1
                batch = []

                if total_read % 10000 < BATCH_SIZE:
                    print(f"  Progress: {total_read:,} read, {total_inserted:,} inserted, {total_skipped:,} skipped")

    # Final batch
    if batch:
        try:
            sb.table("source_lwin").insert(batch).execute()
            total_inserted += len(batch)
        except Exception as e:
            print(f"  Error on final batch: {e}")
            errors += 1

    print(f"\nDone.")
    print(f"  Total CSV rows read: {total_read:,}")
    print(f"  Inserted: {total_inserted:,}")
    print(f"  Skipped (wrong type/deleted): {total_skipped:,}")
    print(f"  Batch errors: {errors}")


if __name__ == "__main__":
    main()
