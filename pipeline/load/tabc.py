#!/usr/bin/env python3
"""
Load TX TABC wine data into source_tabc staging table.
Source: data/imports/tx_tabc_wines.json (201K wines from Socrata API)

Usage:
    python -m pipeline.load.tabc
"""

import json
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from pipeline.lib.db import get_supabase, batch_upsert

DATA_DIR = Path(__file__).resolve().parents[2] / "data" / "imports"
BATCH_SIZE = 500


def main():
    print("=== TX TABC Staging Loader ===\n")

    raw = json.loads((DATA_DIR / "tx_tabc_wines.json").read_text(encoding="utf-8"))
    print(f"Raw records: {len(raw):,}")

    # Dedup by TTB number -- keep richest record
    by_ttb: dict[str, dict] = {}
    no_ttb = 0
    for r in raw:
        ttb = (r.get("ttb_number") or "").strip()
        if not ttb or not re.match(r"^\d{10,}$", ttb):
            no_ttb += 1
            continue
        existing = by_ttb.get(ttb)
        if not existing or len(r) > len(existing):
            by_ttb[ttb] = r

    unique = list(by_ttb.values())
    print(f"Unique TTB numbers: {len(unique):,}")
    print(f"No valid TTB: {no_ttb:,}\n")

    sb = get_supabase()
    inserted = 0
    errors = 0

    for i in range(0, len(unique), BATCH_SIZE):
        batch = []
        for r in unique[i:i + BATCH_SIZE]:
            batch.append({
                "ttb_number": r["ttb_number"].strip(),
                "brand_name": r.get("brand_name") or None,
                "trade_name": r.get("trade_name") or None,
                "alcohol_content": float(r["alcohol_content_by_volume"]) if r.get("alcohol_content_by_volume") else None,
                "approval_date": r.get("approval_date") or None,
                "tabc_certificate": r.get("tabc_certificate_number") or None,
                "permit_license": r.get("permit_license_number") or None,
                "product_type": r.get("type") or "WINE",
            })

        try:
            result = sb.table("source_tabc").upsert(batch, on_conflict="ttb_number").execute()
            inserted += len(result.data) if result.data else len(batch)
        except Exception as e:
            print(f"  Batch error at {i}: {e}")
            errors += 1

        if (i + BATCH_SIZE) % 10000 == 0 or i + BATCH_SIZE >= len(unique):
            print(f"  {min(i + BATCH_SIZE, len(unique)):,}/{len(unique):,} loaded")

    print(f"\n\nDone: {inserted:,} upserted, {errors} errors")
    result = sb.table("source_tabc").select("*", count="exact").limit(0).execute()
    print(f"DB total: {result.count:,}" if result.count else "DB total: unknown")


if __name__ == "__main__":
    main()
