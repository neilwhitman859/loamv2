#!/usr/bin/env python3
"""
Load UPC barcode sources into staging tables.
Sources: Open Food Facts, Horizon Beverage, WineDeals

Usage:
    python -m pipeline.load.upc_staging
"""

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from pipeline.lib.db import get_supabase

DATA_DIR = Path(__file__).resolve().parents[2] / "data" / "imports"
BATCH = 500


def load_batches(sb, table: str, records: list[dict], conflict: str) -> dict:
    inserted = 0
    errors = 0
    for i in range(0, len(records), BATCH):
        batch = records[i:i + BATCH]
        try:
            result = sb.table(table).upsert(batch, on_conflict=conflict).execute()
            inserted += len(result.data) if result.data else len(batch)
        except Exception as e:
            print(f"  Error at {i}: {e}")
            errors += 1
        if (i + BATCH) % 2000 == 0 or i + BATCH >= len(records):
            print(f"  {min(i + BATCH, len(records)):,}/{len(records):,}", end="\r")
    return {"inserted": inserted, "errors": errors}


def main():
    sb = get_supabase()

    # === Open Food Facts ===
    print("=== Open Food Facts ===")
    off_raw = json.loads((DATA_DIR / "openfoodfacts_wines.json").read_text(encoding="utf-8"))
    off_wines = off_raw.get("wines", off_raw) if isinstance(off_raw, dict) else off_raw
    off_wines = [w for w in off_wines if w.get("barcode")]
    print(f"Records: {len(off_wines)}")
    off_batch = [{
        "barcode": r["barcode"],
        "name": r.get("name") or None,
        "brand": r.get("brand") or None,
        "country": r.get("countries") or None,
        "categories": r.get("categories") or None,
        "abv": float(r["abv"]) if r.get("abv") else None,
        "color": r.get("color") or None,
        "origins": r.get("origins") or None,
        "labels": r.get("labels") or None,
        "quantity": r.get("quantity") or None,
    } for r in off_wines]
    off_result = load_batches(sb, "source_openfoodfacts", off_batch, "barcode")
    print(f"\n  Done: {off_result['inserted']} upserted, {off_result['errors']} errors\n")

    # === Horizon Beverage ===
    print("=== Horizon Beverage ===")
    hz_raw = json.loads((DATA_DIR / "horizon_beverage_wines.json").read_text(encoding="utf-8"))
    hz_wines = hz_raw.get("wines", hz_raw) if isinstance(hz_raw, dict) else hz_raw
    hz_wines = [w for w in hz_wines if w.get("upc")]
    print(f"Records: {len(hz_wines)}")
    hz_batch = [{
        "upc": r["upc"],
        "name": r.get("name") or None,
        "brand": r.get("producer") or None,
        "category": r.get("category") or None,
        "subcategory": r.get("style") or None,
        "country": r.get("country") or None,
        "region": r.get("region") or None,
        "varietal": ", ".join(r["grapes"]) if r.get("grapes") else None,
        "size": r.get("size_raw") or None,
    } for r in hz_wines]
    hz_result = load_batches(sb, "source_horizon", hz_batch, "upc")
    print(f"\n  Done: {hz_result['inserted']} upserted, {hz_result['errors']} errors\n")

    # === WineDeals ===
    print("=== WineDeals ===")
    wd_raw = json.loads((DATA_DIR / "winedeals_catalog.json").read_text(encoding="utf-8"))
    wd_wines = wd_raw if isinstance(wd_raw, list) else wd_raw.get("wines", [])
    print(f"Records: {len(wd_wines)}")
    wd_batch = [{
        "upc": r.get("upc") or None,
        "name": r.get("name") or None,
        "producer": r.get("producer") or None,
        "country": r.get("country") or None,
        "region": r.get("region") or None,
        "appellation": r.get("appellation") or None,
        "vintage": r.get("vintage") or None,
        "abv": float(str(r["abv"]).replace("%", "")) if r.get("abv") else None,
        "color": r.get("color") or None,
        "price_usd": float(str(r["price"]).replace("$", "")) if r.get("price") else None,
        "compare_at_price_usd": float(str(r["compare_at_price"]).replace("$", "")) if r.get("compare_at_price") else None,
        "url": r.get("url") or None,
        "item_number": r.get("item_number") or r.get("sku") or None,
    } for r in wd_wines]
    wd_result = load_batches(sb, "source_winedeals", wd_batch, "item_number")
    print(f"\n  Done: {wd_result['inserted']} upserted, {wd_result['errors']} errors\n")

    # === Summary ===
    print("=== UPC TOTALS ===")
    for table, label in [("source_openfoodfacts", "OFF"), ("source_horizon", "Horizon"), ("source_winedeals", "WineDeals")]:
        result = sb.table(table).select("*", count="exact", head=True).execute()
        print(f"  {label}: {result.count:,}" if result.count else f"  {label}: unknown")


if __name__ == "__main__":
    main()
