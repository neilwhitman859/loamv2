#!/usr/bin/env python3
"""
TTB UPC/QR Promoter

Reads barcode scan JSON results, joins to source_ttb_colas.canonical_wine_id,
and writes to external_ids.

Handles both labels and scans JSON files, merges and deduplicates.

Usage:
    python -m pipeline.promote.ttb_upc_promote                    # dry run
    python -m pipeline.promote.ttb_upc_promote --execute          # write to DB
    python -m pipeline.promote.ttb_upc_promote --execute --qr     # include QR codes
"""

import argparse
import json
import os
import re
import sys
import time
from collections import Counter
from pathlib import Path

# Force UTF-8 on Windows
if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass
    os.environ.setdefault("PYTHONIOENCODING", "utf-8")

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from pipeline.lib.db import get_conn

# ─── Constants ───────────────────────────────────────────────────────────────

DATA_DIR = Path(__file__).resolve().parents[2] / "data" / "imports"
LABELS_JSON = DATA_DIR / "ttb_barcode_results.json"
SCANS_JSON = DATA_DIR / "ttb_barcode_results_scans.json"

# QR codes shorter than this are likely decoder noise
MIN_QR_LENGTH = 8
URL_RE = re.compile(r'^https?://', re.IGNORECASE)


# ─── Load & Merge ────────────────────────────────────────────────────────────

def load_and_merge() -> dict[str, dict]:
    """Load labels + scans JSONs, merge by TTB ID, deduplicate codes."""
    merged = {}  # ttb_id -> {upcs: set, qrs: set}

    for path in [LABELS_JSON, SCANS_JSON]:
        if not path.exists():
            print(f"  WARN: {path.name} not found, skipping")
            continue

        print(f"  Loading {path.name}...")
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)

        count = 0
        for rec in data["results"]:
            tid = rec["ttb_id"]
            if tid not in merged:
                merged[tid] = {"upcs": set(), "qrs": set()}

            merged[tid]["upcs"].update(rec.get("upcs", []))
            merged[tid]["qrs"].update(rec.get("qr_codes", []))
            count += 1

        print(f"    {count:,} COLAs loaded from {path.name}")

    return merged


def filter_qr_codes(qrs: set[str]) -> list[str]:
    """Filter QR codes to meaningful values. Returns list of (system, value) tuples."""
    results = []
    for qr in qrs:
        qr = qr.strip()
        if len(qr) < MIN_QR_LENGTH:
            continue
        results.append(qr)
    return results


# ─── Promote ─────────────────────────────────────────────────────────────────

def promote(merged: dict, include_qr: bool, dry_run: bool):
    """Join TTB IDs to canonical wines, write UPCs + QRs to external_ids."""
    conn = get_conn()
    cur = conn.cursor()

    # Step 1: Get TTB -> canonical_wine_id mapping
    print("\n  Fetching TTB -> wine mapping from source_ttb_colas...")
    ttb_ids_with_codes = [tid for tid, codes in merged.items()
                          if codes["upcs"] or (include_qr and codes["qrs"])]

    # Batch fetch in chunks of 5000
    wine_map = {}  # ttb_id -> canonical_wine_id
    batch_size = 5000
    for i in range(0, len(ttb_ids_with_codes), batch_size):
        batch = ttb_ids_with_codes[i:i + batch_size]
        placeholders = ",".join(["%s"] * len(batch))
        cur.execute(
            f"SELECT ttb_id, canonical_wine_id FROM source_ttb_colas "
            f"WHERE ttb_id IN ({placeholders}) AND canonical_wine_id IS NOT NULL",
            batch
        )
        for row in cur.fetchall():
            wine_map[row[0]] = row[1]

        if (i + batch_size) % 50000 == 0 or i + batch_size >= len(ttb_ids_with_codes):
            print(f"    {min(i + batch_size, len(ttb_ids_with_codes)):,}/{len(ttb_ids_with_codes):,} TTB IDs checked")

    print(f"  Mapped: {len(wine_map):,} TTB IDs -> canonical wines")
    print(f"  Unmapped: {len(ttb_ids_with_codes) - len(wine_map):,} (no canonical_wine_id)")

    # Step 2: Build insert rows: (entity_type, entity_id, system, external_id)
    rows_upc = []
    rows_qr_url = []
    rows_qr = []

    # Track per-wine to count distinct wines getting UPCs
    wines_getting_upc = set()
    wines_getting_qr = set()

    for ttb_id, codes in merged.items():
        wine_id = wine_map.get(ttb_id)
        if not wine_id:
            continue

        for upc in codes["upcs"]:
            # Skip bogus UPCs (all zeros, too short)
            if not upc or len(upc) < 6 or upc == "0" * len(upc):
                continue
            rows_upc.append(("wine", str(wine_id), "upc", upc))
            wines_getting_upc.add(str(wine_id))

        if include_qr:
            for qr in filter_qr_codes(codes["qrs"]):
                if URL_RE.match(qr):
                    rows_qr_url.append(("wine", str(wine_id), "qr_url", qr))
                else:
                    rows_qr.append(("wine", str(wine_id), "qr", qr))
                wines_getting_qr.add(str(wine_id))

    print(f"\n  UPC rows to insert: {len(rows_upc):,} (across {len(wines_getting_upc):,} wines)")
    if include_qr:
        print(f"  QR URL rows: {len(rows_qr_url):,}")
        print(f"  QR other rows: {len(rows_qr):,}")
        print(f"  QR wines: {len(wines_getting_qr):,}")

    # Deduplicate (same wine might get same UPC from multiple COLAs)
    all_rows = list(set(rows_upc + rows_qr_url + rows_qr))
    print(f"  After dedup: {len(all_rows):,} unique rows")

    if dry_run:
        print("\n  DRY RUN — no changes written.")

        # Show sample
        print("\n  Sample UPC rows (first 10):")
        for r in rows_upc[:10]:
            print(f"    wine={r[1][:8]}... system={r[2]} value={r[3]}")

        if include_qr and rows_qr_url:
            print("\n  Sample QR URL rows (first 5):")
            for r in rows_qr_url[:5]:
                print(f"    wine={r[1][:8]}... system={r[2]} value={r[3][:80]}")

        # UPC length distribution
        upc_lens = Counter(len(r[3]) for r in rows_upc)
        print(f"\n  UPC length distribution:")
        for l in sorted(upc_lens.keys()):
            print(f"    {l} digits: {upc_lens[l]:,}")

        conn.close()
        return

    # Step 3: Insert with ON CONFLICT DO NOTHING
    print(f"\n  Inserting {len(all_rows):,} rows into external_ids...")
    inserted = 0
    skipped = 0

    for i in range(0, len(all_rows), 1000):
        batch = all_rows[i:i + 1000]
        for row in batch:
            try:
                cur.execute(
                    """INSERT INTO external_ids (entity_type, entity_id, system, external_id)
                       VALUES (%s, %s, %s, %s)
                       ON CONFLICT (entity_type, entity_id, system, external_id) DO NOTHING""",
                    row
                )
                if cur.rowcount > 0:
                    inserted += 1
                else:
                    skipped += 1
            except Exception as e:
                print(f"    ERROR on {row[3]}: {e}")
                conn.rollback()
                continue

        conn.commit()
        if (i + 1000) % 10000 == 0 or i + 1000 >= len(all_rows):
            print(f"    {min(i + 1000, len(all_rows)):,}/{len(all_rows):,} — inserted {inserted:,}, skipped {skipped:,}")

    conn.commit()
    conn.close()

    print(f"\n  Done! Inserted: {inserted:,}, Already existed: {skipped:,}")


# ─── Main ────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Promote TTB barcode scan UPCs/QRs to external_ids")
    parser.add_argument("--execute", action="store_true", help="Write to DB (default is dry run)")
    parser.add_argument("--qr", action="store_true", help="Include QR codes (URLs and data)")
    args = parser.parse_args()

    dry_run = not args.execute

    print("=== TTB UPC/QR Promoter ===")
    print(f"  Mode: {'EXECUTE' if not dry_run else 'DRY RUN'}")
    print(f"  QR codes: {'included' if args.qr else 'UPC only'}")
    print()

    # Load and merge
    merged = load_and_merge()

    total_with_upc = sum(1 for v in merged.values() if v["upcs"])
    total_with_qr = sum(1 for v in merged.values() if v["qrs"])
    all_upcs = set()
    for v in merged.values():
        all_upcs.update(v["upcs"])

    print(f"\n  Merged: {len(merged):,} COLAs")
    print(f"  With UPC: {total_with_upc:,}")
    print(f"  With QR: {total_with_qr:,}")
    print(f"  Unique UPCs: {len(all_upcs):,}")

    # Promote
    promote(merged, include_qr=args.qr, dry_run=dry_run)

    if dry_run:
        print("\n  To execute: python -m pipeline.promote.ttb_upc_promote --execute")
        print("  To include QRs: python -m pipeline.promote.ttb_upc_promote --execute --qr")


if __name__ == "__main__":
    main()
