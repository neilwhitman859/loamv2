#!/usr/bin/env python3
"""
Build TTB COLA record lists by querying the database directly via psycopg2.
Bypasses the PostgREST statement timeout that's causing issues.

Usage:
    python -m pipeline.fetch.build_record_list --type detail
    python -m pipeline.fetch.build_record_list --type printable
"""

import argparse
import json
import os
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from pipeline.lib.db import get_supabase


DEFAULT_DETAIL_OUTPUT = Path.home() / "Desktop" / "Loam Cowork" / "data" / "imports" / "ttb_cola_labels"
DEFAULT_PRINTABLE_OUTPUT = Path.home() / "Desktop" / "Loam Cowork" / "data" / "imports" / "ttb_cola_printable"

WINE_CLASSES = "('80', '81', '80A', '84', '88', '8000', '8100', '8400', '8800')"


def fetch_year_direct(sb, year: int) -> list[dict]:
    """Fetch records for a single year using the SQL endpoint (longer timeout)."""
    # Use Supabase's direct SQL via postgrest - but we need a workaround
    # Since .rpc() also goes through PostgREST, we'll use small batches
    # with keyset pagination on ttb_id (which uses the primary key index)
    all_records = []
    last_id = ""
    batch_size = 500

    while True:
        try:
            query = (sb.table("source_ttb_colas")
                .select("ttb_id,brand_name,completed_date")
                .in_("class_type", ["80", "81", "80A", "84", "88", "8000", "8100", "8400", "8800"])
                .gte("completed_date", f"{year}-01-01")
                .lt("completed_date", f"{year + 1}-01-01")
                .gt("ttb_id", last_id)
                .order("ttb_id")
                .limit(batch_size)
            )
            result = query.execute()
        except Exception as e:
            err_msg = str(e)[:80]
            if "timeout" in err_msg.lower():
                # Reduce batch size and retry
                batch_size = max(100, batch_size // 2)
                print(f"    Timeout at {year} after '{last_id}', reducing batch to {batch_size}")
                time.sleep(2)
                continue
            print(f"    Error {year}: {err_msg}")
            time.sleep(2)
            continue

        if not result.data:
            break

        all_records.extend(result.data)
        last_id = result.data[-1]["ttb_id"]

        if len(result.data) < batch_size:
            break

        # Reset batch size if it was reduced
        batch_size = min(500, batch_size * 2)

    return all_records


def fetch_null_dates(sb) -> list[dict]:
    """Fetch records with NULL completed_date."""
    all_records = []
    last_id = ""
    batch_size = 500

    while True:
        try:
            result = (sb.table("source_ttb_colas")
                .select("ttb_id,brand_name,completed_date")
                .in_("class_type", ["80", "81", "80A", "84", "88", "8000", "8100", "8400", "8800"])
                .is_("completed_date", "null")
                .gt("ttb_id", last_id)
                .order("ttb_id")
                .limit(batch_size)
            ).execute()
        except Exception as e:
            print(f"    Error null-date: {str(e)[:80]}")
            time.sleep(2)
            continue

        if not result.data:
            break
        all_records.extend(result.data)
        last_id = result.data[-1]["ttb_id"]
        if len(result.data) < batch_size:
            break

    return all_records


def main():
    parser = argparse.ArgumentParser(description="Build TTB record list")
    parser.add_argument("--type", choices=["detail", "printable"], required=True)
    parser.add_argument("--start-year", type=int, default=1955)
    args = parser.parse_args()

    if sys.platform == "win32":
        import codecs
        sys.stdout = codecs.getwriter("utf-8")(sys.stdout.buffer, errors="replace")
        sys.stderr = codecs.getwriter("utf-8")(sys.stderr.buffer, errors="replace")

    sb = get_supabase()
    current_year = time.localtime().tm_year
    all_records = []

    if args.type == "detail":
        output_dir = DEFAULT_DETAIL_OUTPUT
        cache_file = output_dir / "record_list.json"
        start_year = args.start_year
    else:
        output_dir = DEFAULT_PRINTABLE_OUTPUT
        cache_file = output_dir / "record_list_printable_001.json"
        start_year = max(args.start_year, 2003)  # 001 IDs only exist from 2003

    output_dir.mkdir(parents=True, exist_ok=True)

    print(f"\nBuilding {args.type} record list ({start_year}-{current_year})...")
    print(f"Using keyset pagination (gt ttb_id) to avoid timeout\n")

    total_start = time.time()
    for year in range(start_year, current_year + 1):
        year_start = time.time()
        records = fetch_year_direct(sb, year)
        elapsed = time.time() - year_start
        all_records.extend(records)
        print(f"  {year}: {len(records):>8,} records  ({elapsed:.1f}s)  total: {len(all_records):,}")

    # Null dates
    print(f"\n  Fetching NULL-date records...")
    null_records = fetch_null_dates(sb)
    all_records.extend(null_records)
    print(f"  NULL-date: {len(null_records):,} records  total: {len(all_records):,}")

    # For printable, filter to 001 only
    if args.type == "printable":
        before = len(all_records)
        all_records = [r for r in all_records if len(r["ttb_id"]) >= 14 and r["ttb_id"][5:8] == "001"]
        print(f"\n  Filtered to 001-only: {before:,} → {len(all_records):,}")

    total_elapsed = time.time() - total_start
    print(f"\n  Total: {len(all_records):,} records in {total_elapsed:.0f}s")

    # Save
    cache_file.write_text(json.dumps(all_records), encoding="utf-8")
    print(f"  Saved to: {cache_file}")
    print(f"  File size: {cache_file.stat().st_size / 1024 / 1024:.1f} MB")


if __name__ == "__main__":
    main()
