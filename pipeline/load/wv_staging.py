#!/usr/bin/env python3
"""
Load WV ABCA wine data into source_wv_abca staging table.

Source: data/imports/wv_wines_list.json (55K wines from REST API)

Usage:
    python -m pipeline.load.wv_staging
"""

import json
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from pipeline.lib.db import get_supabase

DATA_DIR = Path(__file__).resolve().parents[2] / "data" / "imports"
BATCH_SIZE = 500


def main():
    sb = get_supabase()

    print("=== WV ABCA Staging Loader ===\n")

    # Try both possible filenames
    wv_path = DATA_DIR / "wv_wines_list.json"
    if not wv_path.exists():
        wv_path = DATA_DIR / "wv_abca_wines.json"

    raw = json.loads(wv_path.read_text(encoding="utf-8"))
    print(f"Raw records: {len(raw):,}")

    # Dedup by LabelID
    by_label: dict[str, dict] = {}
    for r in raw:
        label_id = r.get("LabelID")
        if not label_id:
            continue
        by_label[label_id] = r

    unique = list(by_label.values())
    print(f"Unique LabelIDs: {len(unique):,}\n")

    inserted = 0
    errors = 0
    for i in range(0, len(unique), BATCH_SIZE):
        batch = []
        for r in unique[i:i + BATCH_SIZE]:
            # TTB field: sometimes numeric COLA, sometimes text
            ttb_raw = (r.get("TTB") or "").strip()
            ttb = ttb_raw if re.match(r"^\d{10,}$", ttb_raw) else None

            batch.append({
                "label_id": r["LabelID"],
                "ttb": ttb,
                "brand_name": r.get("BrandName") or None,
                "fanciful_name": (r.get("FancifulName") or None) if ttb else (r.get("FancifulName") or ttb_raw or None),
                "class_text": r.get("ClassText") or None,
                "alcohol_percentage": r.get("AlcoholPercentage") or None,
                "vintage": str(r["Vintage"]) if r.get("Vintage") else None,
                "winery_name": r.get("WineryName") or None,
            })

        try:
            result = sb.table("source_wv_abca").upsert(
                batch, on_conflict="label_id"
            ).execute()
            inserted += len(result.data) if result.data else len(batch)
        except Exception as e:
            print(f"  Batch error at {i}: {e}")
            errors += 1

        if (i + BATCH_SIZE) % 10000 == 0 or i + BATCH_SIZE >= len(unique):
            print(f"  {min(i + BATCH_SIZE, len(unique)):,}/{len(unique):,} loaded", end="\r")

    print(f"\n\nDone: {inserted:,} upserted, {errors} errors")
    result = sb.table("source_wv_abca").select("*", count="exact", head=True).execute()
    print(f"DB total: {result.count:,}" if result.count else "DB total: unknown")

    # Stats
    has_ttb = sb.table("source_wv_abca").select("*", count="exact", head=True).not_.is_("ttb", "null").execute()
    print(f"With COLA: {has_ttb.count:,}" if has_ttb.count else "With COLA: unknown")


if __name__ == "__main__":
    main()
