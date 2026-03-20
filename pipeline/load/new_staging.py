#!/usr/bin/env python3
"""
Load LCBO, Systembolaget, Polaner, FirstLeaf into staging tables.

Usage:
    python -m pipeline.load.new_staging
"""

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from pipeline.lib.db import get_supabase, batch_insert

DATA_DIR = Path(__file__).resolve().parents[2] / "data" / "imports"


def load_lcbo():
    print("\n=== LCBO ===")
    data = json.loads((DATA_DIR / "lcbo_catalog.json").read_text(encoding="utf-8"))
    print(f"  {len(data)} wines from JSON")

    rows = [{
        "sku": w["sku"],
        "name": w["name"],
        "upc": w.get("upc") or None,
        "producer": w.get("producer") or None,
        "country": w.get("country") or None,
        "region": w.get("region") or None,
        "abv": w.get("abv") or None,
        "price_cad_cents": w.get("price_cad_cents") or None,
        "category": w.get("category") or None,
        "description": w.get("description") or None,
        "volume_ml": w.get("volume_ml") or None,
        "is_vqa": w.get("is_vqa") or False,
        "is_kosher": w.get("is_kosher") or False,
        "updated_at_source": w.get("updated_at") or None,
    } for w in data]

    n = batch_insert("source_lcbo", rows, batch_size=500)
    print(f"  Inserted {n} rows")


def load_systembolaget():
    print("\n=== Systembolaget ===")
    raw = json.loads((DATA_DIR / "systembolaget_raw.json").read_text(encoding="utf-8"))
    wines = [p for p in raw if (p.get("categoryLevel1") or "").lower() == "vin"]
    print(f"  {len(wines)} wines from {len(raw)} total products")

    rows = [{
        "product_id": w.get("productId") or None,
        "product_number": w.get("productNumber") or None,
        "name_bold": w.get("productNameBold") or None,
        "name_thin": w.get("productNameThin") or None,
        "producer": w.get("producerName") or None,
        "country": w.get("country") or None,
        "origin_level1": w.get("originLevel1") or None,
        "origin_level2": w.get("originLevel2") or None,
        "category_level1": w.get("categoryLevel1") or None,
        "category_level2": w.get("categoryLevel2") or None,
        "color": w.get("color") or None,
        "grapes": w["grapes"] if w.get("grapes") and len(w["grapes"]) > 0 else None,
        "vintage": str(w["vintage"]) if w.get("vintage") else None,
        "abv": w.get("alcoholPercentage") or None,
        "price_sek": w.get("price") or None,
        "volume_ml": w.get("volume") or None,
        "sugar_g_per_100ml": w.get("sugarContentGramPer100ml") or None,
        "taste_body": w.get("tasteClockBody") or None,
        "taste_sweetness": w.get("tasteClockSweetness") or None,
        "taste_fruitacid": w.get("tasteClockFruitacid") or None,
        "taste_bitterness": w.get("tasteClockBitter") or None,
        "taste_roughness": w.get("tasteClockRoughness") or None,
        "taste_smokiness": w.get("tasteClockSmokiness") or None,
        "is_organic": w.get("isOrganic") or False,
        "is_kosher": w.get("isKosher") or False,
        "is_ethical": w.get("isEthical") or False,
        "description": w.get("taste") or None,
    } for w in wines]

    n = batch_insert("source_systembolaget", rows, batch_size=500)
    print(f"  Inserted {n} rows")


def load_polaner():
    sb = get_supabase()
    print("\n=== Polaner ===")
    result = sb.table("source_polaner").select("id", count="exact").limit(0).execute()
    if result.count and result.count > 0:
        print(f"  Already has {result.count} rows, skipping")
        return

    data = json.loads((DATA_DIR / "polaner_catalog.json").read_text(encoding="utf-8"))
    print(f"  {len(data)} wines from JSON")

    rows = [{
        "title": w.get("title") or w.get("name") or None,
        "country": w.get("country") or None,
        "region": w.get("region") or None,
        "appellation": w.get("appellation") or None,
        "url": w.get("url") or None,
        "metadata": w.get("metadata") or None,
    } for w in data]

    n = batch_insert("source_polaner", rows, batch_size=500)
    print(f"  Inserted {n} rows")


def load_firstleaf():
    print("\n=== FirstLeaf ===")
    data = json.loads((DATA_DIR / "firstleaf_catalog.json").read_text(encoding="utf-8"))
    print(f"  {len(data)} products from JSON")

    rows = [{
        "title": w.get("title") or None,
        "handle": w.get("handle") or None,
        "vendor": w.get("vendor") or None,
        "product_type": w.get("product_type") or None,
        "tags": w["tags"] if w.get("tags") and len(w["tags"]) > 0 else None,
        "price_usd": float(w["price"]) if w.get("price") else None,
        "image_url": (w.get("image") or {}).get("src") or None,
        "metadata": w.get("metadata") or None,
    } for w in data]

    n = batch_insert("source_firstleaf", rows, batch_size=500)
    print(f"  Inserted {n} rows")


def main():
    load_lcbo()
    load_systembolaget()
    load_polaner()
    load_firstleaf()
    print("\nDone!")


if __name__ == "__main__":
    main()
