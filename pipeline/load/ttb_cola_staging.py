#!/usr/bin/env python3
"""
Load TTB COLA harvest data into source_ttb_colas staging table.

Source: ttb_cola_catalog.jsonl (3.28M records, ~2.2GB JSONL)
       Streamed line-by-line to handle the large file.

Usage:
    python -m pipeline.load.ttb_cola_staging --file PATH_TO_JSONL [--batch-size 500] [--dry-run] [--analyze]

Examples:
    # Analyze the file without loading
    python -m pipeline.load.ttb_cola_staging --file "C:/Users/neilw/Desktop/Loam Cowork/data/imports/ttb_cola_catalog.jsonl" --analyze

    # Dry run (parse + transform, no DB writes)
    python -m pipeline.load.ttb_cola_staging --file "C:/Users/neilw/Desktop/Loam Cowork/data/imports/ttb_cola_catalog.jsonl" --dry-run

    # Full load
    python -m pipeline.load.ttb_cola_staging --file "C:/Users/neilw/Desktop/Loam Cowork/data/imports/ttb_cola_catalog.jsonl"
"""

import argparse
import html
import json
import re
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from pipeline.lib.db import get_supabase


BATCH_SIZE_DEFAULT = 2000


def parse_date(s: str) -> str | None:
    """Convert MM/DD/YYYY to YYYY-MM-DD, or return None."""
    if not s:
        return None
    m = re.match(r"^(\d{2})/(\d{2})/(\d{4})$", s.strip())
    if m:
        return f"{m.group(3)}-{m.group(1)}-{m.group(2)}"
    return None


def clean_text(s: str | None) -> str | None:
    """Strip whitespace, remove null bytes, decode HTML entities, return None for empty."""
    if not s:
        return None
    s = s.replace("\x00", "")
    s = html.unescape(s)
    s = s.strip()
    return s if s else None


def transform_record(raw: dict) -> dict:
    """Map JSONL fields to source_ttb_colas columns."""
    grape = clean_text(raw.get("grape_varietals"))
    if grape and grape.upper() in ("N/A", "NA", "NONE", ""):
        grape = None

    return {
        "ttb_id": raw["ttb_id"].strip(),
        "permit_no": clean_text(raw.get("permit_no")),
        "serial_number": clean_text(raw.get("serial_number")),
        "completed_date": parse_date(raw.get("completed_date", "")),
        "brand_name": clean_text(raw.get("brand_name")),
        "fanciful_name": clean_text(raw.get("fanciful_name")),
        "origin_code": clean_text(raw.get("origin_code")),
        "origin_desc": clean_text(raw.get("origin_desc")),
        "class_type": clean_text(raw.get("class_type_code")),
        "class_type_desc": clean_text(raw.get("class_type_desc")),
        "grape_varietals": grape,
        "status": clean_text(raw.get("status")),
        "vendor_code": clean_text(raw.get("vendor_code")),
        "type_of_application": clean_text(raw.get("type_of_application")),
        "total_bottle_capacity": clean_text(raw.get("bottle_capacity")),
        "qualifications": clean_text(raw.get("qualifications")),
        "approval_date": clean_text(raw.get("approval_date")),
        "formula": clean_text(raw.get("formula")),
        "for_sale_in": clean_text(raw.get("for_sale_in")),
        "permit_principal": clean_text(raw.get("permit_principal")),
        "permit_other": clean_text(raw.get("permit_other")),
    }


def analyze(filepath: Path):
    """Print stats about the JSONL file without loading."""
    from collections import Counter

    print(f"=== Analyzing {filepath.name} ===\n")

    total = 0
    statuses = Counter()
    class_types = Counter()
    origins = Counter()
    has_fanciful = 0
    has_grape = 0
    has_quals = 0
    dupes = set()
    dupe_count = 0

    with open(filepath, "r", encoding="utf-8") as f:
        for line in f:
            d = json.loads(line)
            total += 1
            ttb_id = d.get("ttb_id", "").strip()
            if ttb_id in dupes:
                dupe_count += 1
            dupes.add(ttb_id)

            statuses[d.get("status", "")] += 1
            class_types[d.get("class_type_code", "") + " " + d.get("class_type_desc", "")] += 1
            origins[d.get("origin_desc", "")] += 1
            if clean_text(d.get("fanciful_name")):
                has_fanciful += 1
            g = clean_text(d.get("grape_varietals"))
            if g and g.upper() not in ("N/A", "NA", "NONE"):
                has_grape += 1
            if clean_text(d.get("qualifications")):
                has_quals += 1

            if total % 500_000 == 0:
                print(f"  {total:,} scanned...", end="\r")

    print(f"Total records: {total:,}")
    print(f"Duplicate TTB IDs: {dupe_count:,}")
    print(f"Unique TTB IDs: {len(dupes):,}")
    print(f"\nHas fanciful_name: {has_fanciful:,} ({has_fanciful*100/total:.1f}%)")
    print(f"Has grape_varietals: {has_grape:,} ({has_grape*100/total:.1f}%)")
    print(f"Has qualifications: {has_quals:,} ({has_quals*100/total:.1f}%)")

    print(f"\nStatus:")
    for k, v in statuses.most_common(10):
        print(f"  {v:>10,}  {k}")

    print(f"\nTop class types:")
    for k, v in class_types.most_common(15):
        print(f"  {v:>10,}  {k}")

    print(f"\nTop origins:")
    for k, v in origins.most_common(20):
        print(f"  {v:>10,}  {k}")


def load(filepath: Path, batch_size: int, dry_run: bool, truncate: bool = False):
    """Stream JSONL and upsert into source_ttb_colas."""
    sb = None if dry_run else get_supabase()

    print(f"=== TTB COLA Staging Loader ===")
    print(f"File: {filepath}")
    print(f"Batch size: {batch_size}")
    print(f"Mode: {'DRY RUN' if dry_run else 'LIVE'}")
    if truncate:
        print(f"TRUNCATE: will clear table first")
    print()

    # Dedup by ttb_id — keep last occurrence (most complete from later scrape passes)
    print("Pass 1: Deduplicating by TTB ID...")
    by_ttb: dict[str, dict] = {}
    no_ttb = 0
    total = 0
    t0 = time.time()

    with open(filepath, "r", encoding="utf-8") as f:
        for line in f:
            total += 1
            raw = json.loads(line)
            ttb_id = (raw.get("ttb_id") or "").strip()
            if not ttb_id:
                no_ttb += 1
                continue
            by_ttb[ttb_id] = raw

            if total % 500_000 == 0:
                elapsed = time.time() - t0
                print(f"  {total:,} read ({elapsed:.0f}s)...", end="\r")

    elapsed = time.time() - t0
    print(f"  Read {total:,} lines in {elapsed:.0f}s")
    print(f"  Unique TTB IDs: {len(by_ttb):,}")
    print(f"  No TTB ID: {no_ttb:,}")
    print(f"  Duplicates: {total - len(by_ttb) - no_ttb:,}\n")

    # Transform
    print("Pass 2: Transforming and loading...")
    records = []
    for raw in by_ttb.values():
        records.append(transform_record(raw))

    del by_ttb  # free memory

    if dry_run:
        print(f"  {len(records):,} records ready (dry run, no DB writes)")
        print(f"\nSample records:")
        for r in records[:3]:
            print(f"  {r['ttb_id']}: {r['brand_name']} / {r['fanciful_name']} ({r['origin_desc']}) [{r['class_type_desc']}]")
        return

    # Truncate if requested
    if truncate:
        print("Truncating source_ttb_colas...")
        sb.table("source_ttb_colas").delete().neq("ttb_id", "").execute()
        print("  Done.\n")

    # Batch upsert
    inserted = 0
    errors = 0
    t0 = time.time()

    for i in range(0, len(records), batch_size):
        batch = records[i:i + batch_size]
        try:
            result = sb.table("source_ttb_colas").upsert(
                batch, on_conflict="ttb_id"
            ).execute()
            inserted += len(result.data) if result.data else len(batch)
        except Exception as e:
            errors += 1
            err_msg = str(e)[:200]
            print(f"\n  Batch error at {i:,}: {err_msg}")
            # Retry one-by-one
            for row in batch:
                try:
                    sb.table("source_ttb_colas").upsert(
                        row, on_conflict="ttb_id"
                    ).execute()
                    inserted += 1
                except Exception as row_err:
                    print(f"  Row error {row['ttb_id']}: {str(row_err)[:100]}")

        done = min(i + batch_size, len(records))
        if done % 50_000 < batch_size or done >= len(records):
            elapsed = time.time() - t0
            rate = done / elapsed if elapsed > 0 else 0
            eta = (len(records) - done) / rate if rate > 0 else 0
            print(f"  {done:,}/{len(records):,} ({done*100/len(records):.1f}%) — "
                  f"{rate:.0f} rows/s — ETA {eta/60:.0f}m", end="\r")

    elapsed = time.time() - t0
    print(f"\n\nDone in {elapsed/60:.1f}m: {inserted:,} upserted, {errors} batch errors")

    # Verify
    result = sb.table("source_ttb_colas").select("*", count="exact", head=True).execute()
    print(f"DB total: {result.count:,}" if result.count else "DB total: unknown")


def main():
    parser = argparse.ArgumentParser(description="Load TTB COLA harvest into staging")
    parser.add_argument("--file", required=True, help="Path to ttb_cola_catalog.jsonl")
    parser.add_argument("--batch-size", type=int, default=BATCH_SIZE_DEFAULT)
    parser.add_argument("--dry-run", action="store_true", help="Parse and transform without DB writes")
    parser.add_argument("--analyze", action="store_true", help="Print stats only")
    parser.add_argument("--truncate", action="store_true", help="Truncate table before loading")
    args = parser.parse_args()

    filepath = Path(args.file)
    if not filepath.exists():
        print(f"File not found: {filepath}")
        sys.exit(1)

    if args.analyze:
        analyze(filepath)
    else:
        load(filepath, args.batch_size, args.dry_run, args.truncate)


if __name__ == "__main__":
    main()
