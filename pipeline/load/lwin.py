#!/usr/bin/env python3
"""
Load LWIN database Excel into source_lwin staging table.
Source: data/LWINdatabase.xlsx (25MB, ~211K records)
Filters to: STATUS=Live, TYPE=Wine or Fortified Wine

Usage:
    python -m pipeline.load.lwin [--dry-run] [--include-fortified]
"""

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import openpyxl

from pipeline.lib.db import get_supabase, batch_insert

DATA_DIR = Path(__file__).resolve().parents[2] / "data"


def clean(val) -> str | None:
    if val is None:
        return None
    s = str(val).strip()
    return None if s in ("", "NA", "N/A") else s


def map_record(r: dict) -> dict:
    lwin = str(r.get("LWIN", ""))
    return {
        "lwin": lwin,
        "lwin_7": lwin[:7] if len(lwin) >= 7 else lwin,
        "lwin_11": None,
        "lwin_18": None,
        "display_name": clean(r.get("DISPLAY_NAME")),
        "producer_name": clean(r.get("PRODUCER_NAME")),
        "wine_name": clean(r.get("WINE")),
        "country": clean(r.get("COUNTRY")),
        "region": clean(r.get("REGION")),
        "sub_region": clean(r.get("SUB_REGION")),
        "appellation": clean(r.get("SITE")) or clean(r.get("DESIGNATION")),
        "colour": clean(r.get("COLOUR")),
        "wine_type": clean(r.get("SUB_TYPE")) or clean(r.get("TYPE")),
        "designation": clean(r.get("DESIGNATION")),
        "classification": clean(r.get("CLASSIFICATION")),
        "vintage": clean(r.get("VINTAGE_CONFIG")),
    }


def read_xlsx(path: Path) -> list[dict]:
    """Read xlsx to list of dicts using openpyxl."""
    wb = openpyxl.load_workbook(path, read_only=True)
    ws = wb.active
    rows = list(ws.iter_rows(values_only=True))
    wb.close()
    if not rows:
        return []
    headers = [str(h) if h else f"col_{i}" for i, h in enumerate(rows[0])]
    return [dict(zip(headers, row)) for row in rows[1:]]


def main():
    parser = argparse.ArgumentParser(description="Load LWIN database into staging")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--include-fortified", action="store_true")
    args = parser.parse_args()

    print("Reading LWIN database...")
    raw = read_xlsx(DATA_DIR / "LWINdatabase.xlsx")
    print(f"Total records: {len(raw)}")

    valid_types = {"Wine"}
    if args.include_fortified:
        valid_types.add("Fortified Wine")

    wines = [r for r in raw if r.get("STATUS") == "Live" and r.get("TYPE") in valid_types]
    inc_label = "including" if args.include_fortified else "excluding"
    print(f"Live wine records: {len(wines)} ({inc_label} fortified)")

    mapped = [map_record(r) for r in wines]

    if args.dry_run:
        print("\n--- DRY RUN ---")
        print("Sample records:")
        for r in mapped[:5]:
            print(json.dumps(r, indent=2))

        stats = {
            "with_producer": sum(1 for r in mapped if r["producer_name"]),
            "with_wine": sum(1 for r in mapped if r["wine_name"]),
            "with_country": sum(1 for r in mapped if r["country"]),
            "with_region": sum(1 for r in mapped if r["region"]),
            "with_sub_region": sum(1 for r in mapped if r["sub_region"]),
            "with_colour": sum(1 for r in mapped if r["colour"]),
            "with_classification": sum(1 for r in mapped if r["classification"]),
            "with_designation": sum(1 for r in mapped if r["designation"]),
        }
        print("\nField fill rates:")
        for k, v in stats.items():
            print(f"  {k}: {v} ({v / len(mapped) * 100:.1f}%)")

        print(f"\nWould insert {len(mapped)} records.")
        return

    sb = get_supabase()

    # Clear existing data in batches
    print("Clearing existing source_lwin data...")
    deleted = 0
    while True:
        result = sb.table("source_lwin").select("lwin").limit(5000).execute()
        if not result.data:
            break
        ids = [r["lwin"] for r in result.data]
        sb.table("source_lwin").delete().in_("lwin", ids).execute()
        deleted += len(ids)
        if deleted % 10000 == 0:
            print(f"  Deleted {deleted} rows...")
    print(f"Cleared {deleted} existing rows.")

    inserted = 0
    errors = 0
    BATCH = 500
    for i in range(0, len(mapped), BATCH):
        batch = mapped[i:i + BATCH]
        try:
            result = sb.table("source_lwin").insert(batch).execute()
            inserted += len(result.data) if result.data else len(batch)
        except Exception as e:
            for row in batch:
                try:
                    sb.table("source_lwin").insert(row).execute()
                    inserted += 1
                except Exception as row_err:
                    errors += 1
                    if errors <= 5:
                        print(f"  Row error ({row['lwin']} {row['display_name']}): {row_err}")
        if (i + BATCH) % 10000 == 0 or i + BATCH >= len(mapped):
            print(f"  {min(i + BATCH, len(mapped))}/{len(mapped)} processed ({inserted} inserted, {errors} errors)")

    print(f"\nDone. Inserted {inserted} records, {errors} errors.")

    result = sb.table("source_lwin").select("*", count="exact").limit(0).execute()
    print(f"Table row count: {result.count}")


if __name__ == "__main__":
    main()
