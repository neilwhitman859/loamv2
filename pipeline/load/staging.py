#!/usr/bin/env python3
"""
Staging table loader — loads raw JSON catalog files into per-source staging tables.

Usage:
    python -m pipeline.load.staging --source polaner
    python -m pipeline.load.staging --source all
    python -m pipeline.load.staging --source kl,skurnik,polaner
"""

import argparse
import json
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from pipeline.lib.db import get_supabase, batch_insert

DATA_DIR = Path(__file__).resolve().parents[2] / "data" / "imports"


def load_json(filename: str) -> list | dict:
    path = DATA_DIR / filename
    return json.loads(path.read_text(encoding="utf-8"))


def parse_vintage_from_title(title: str) -> str | None:
    m = re.search(r"\b(19|20)\d{2}\b", title or "")
    return m.group(0) if m else None


def parse_tag_value(tags: list | None, key: str) -> str | None:
    if not tags or not isinstance(tags, list):
        return None
    for tag in tags:
        if isinstance(tag, str) and tag.startswith(f"{key}:"):
            return tag[len(key) + 1:].strip()
    return None


# ── Source Loaders ──────────────────────────────────────────

def load_polaner() -> int:
    print("\n=== Loading Polaner ===")
    wines = load_json("polaner_catalog.json")
    rows = [{
        "wp_id": str(w.get("wp_id", "")),
        "slug": w.get("slug"),
        "title": w.get("title"),
        "url": w.get("url"),
        "source_url": w.get("_source"),
        "country": w.get("country"),
        "region": w.get("region"),
        "appellation": w.get("appellation"),
        "certifications": w.get("certifications"),
    } for w in wines]
    count = batch_insert("source_polaner", rows)
    print(f"  Inserted {count}/{len(wines)} rows")
    return count


def load_kermit_lynch() -> int:
    print("\n=== Loading Kermit Lynch ===")
    catalog = load_json("kermit_lynch_catalog.json")

    grower_rows = [{
        "kl_id": str(g.get("kl_id", "")),
        "name": g.get("name"),
        "slug": g.get("slug"),
        "country": g.get("country"),
        "region": g.get("region"),
        "farming": g.get("farming"),
        "winemaker": g.get("winemaker"),
        "founded_year": g["founded_year"] if isinstance(g.get("founded_year"), int) else None,
        "website": g.get("website"),
        "location": g.get("location"),
        "annual_production": g.get("annual_production"),
        "viticulture_notes": g.get("viticulture_notes"),
        "about": g.get("about"),
    } for g in catalog.get("growers", [])]
    gc = batch_insert("source_kermit_lynch_growers", grower_rows)
    print(f"  Inserted {gc}/{len(grower_rows)} growers")

    wine_rows = [{
        "kl_id": str(w.get("kl_id", "")),
        "sku": w.get("sku"),
        "wine_name": w.get("wine_name"),
        "grower_name": w.get("grower_name"),
        "grower_kl_id": str(w["grower_kl_id"]) if w.get("grower_kl_id") else None,
        "country": w.get("country"),
        "region": w.get("region"),
        "wine_type": w.get("wine_type"),
        "blend": w.get("blend"),
        "soil": w.get("soil"),
        "vine_age": w.get("vine_age"),
        "vineyard_area": w.get("vineyard_area"),
        "vinification": w.get("vinification"),
        "farming": w.get("farming"),
    } for w in catalog.get("wines", [])]
    wc = batch_insert("source_kermit_lynch", wine_rows)
    print(f"  Inserted {wc}/{len(wine_rows)} wines")
    return wc


def load_skurnik() -> int:
    print("\n=== Loading Skurnik ===")
    wines = load_json("skurnik_catalog.json")
    rows = [{
        "url": w.get("url"),
        "source_url": w.get("_source"),
        "producer_slug": w.get("producer_slug"),
        "producer": w.get("producer") or (w.get("extra_fields") or {}).get("producer"),
        "name": w.get("name"),
        "vintage": w.get("vintage"),
        "country": w.get("country"),
        "region": w.get("region"),
        "appellation": w.get("appellation"),
        "grape": w.get("grape"),
        "color": w.get("color"),
        "sku": w.get("sku"),
        "bottle_format": w.get("bottle_format"),
        "farming": w.get("farming"),
        "description": w.get("description"),
        "notes": w.get("notes"),
        "image_url": w.get("image_url"),
        "extra_fields": w.get("extra_fields"),
    } for w in wines]
    count = batch_insert("source_skurnik", rows)
    print(f"  Inserted {count}/{len(wines)} rows")
    return count


def load_winebow() -> int:
    print("\n=== Loading Winebow ===")
    wines = load_json("winebow_catalog.json")
    rows = []
    for w in wines:
        name = w.get("name")
        if not name and w.get("url"):
            parts = w["url"].rstrip("/").split("/")
            last = parts[-1]
            name_part = parts[-2] if re.match(r"^\d{4}$", last) else last
            name = name_part.replace("-", " ").title()
        if not name:
            name = w.get("varietal_display") or "Unknown"

        rows.append({
            "url": w.get("url"),
            "source_url": w.get("_source"),
            "brand_slug": w.get("brand_slug"),
            "producer": w.get("producer"),
            "name": name,
            "varietal_display": w.get("varietal_display"),
            "vintage": w.get("vintage"),
            "appellation": w.get("appellation"),
            "vineyard": w.get("vineyard"),
            "vineyard_size": w.get("vineyard_size"),
            "soil": w.get("soil"),
            "training_method": w.get("training_method"),
            "elevation": w.get("elevation"),
            "vines_per_acre": w.get("vines_per_acre"),
            "yield_per_acre": w.get("yield_per_acre"),
            "exposure": w.get("exposure"),
            "production": w.get("production"),
            "grape": w.get("grape"),
            "maceration": w.get("maceration"),
            "malolactic": w.get("malolactic"),
            "aging_vessel_size": w.get("aging_vessel_size"),
            "oak_type": w.get("oak_type"),
            "ph": float(w["ph"]) if w.get("ph") else None,
            "acidity": float(w["acidity"]) if w.get("acidity") else None,
            "abv": float(w["abv"]) if w.get("abv") else None,
            "residual_sugar": float(w["residual_sugar"]) if w.get("residual_sugar") is not None else None,
            "scores": w.get("scores"),
            "description": w.get("description"),
            "vineyard_description": w.get("vineyard_description"),
        })
    count = batch_insert("source_winebow", rows)
    print(f"  Inserted {count}/{len(wines)} rows")
    return count


def load_empson() -> int:
    print("\n=== Loading Empson ===")
    wines = load_json("empson_catalog.json")
    rows = [{
        "url": w.get("url"),
        "source_url": w.get("_source"),
        "name": w.get("name"),
        "producer": w.get("producer"),
        "producer_slug": w.get("producer_slug"),
        "grape": w.get("grape"),
        "fermentation_container": w.get("fermentation_container"),
        "fermentation_duration": w.get("fermentation_duration"),
        "fermentation_temp": w.get("fermentation_temp"),
        "yeast_type": w.get("yeast_type"),
        "maceration_duration": w.get("maceration_duration"),
        "maceration_technique": w.get("maceration_technique"),
        "malolactic": w.get("malolactic"),
        "aging_container": w.get("aging_container"),
        "aging_container_size": w.get("aging_container_size"),
        "aging_duration": w.get("aging_duration"),
        "oak_type": w.get("oak_type"),
        "closure": w.get("closure"),
        "vineyard_location": w.get("vineyard_location"),
        "soil": w.get("soil"),
        "training_method": w.get("training_method"),
        "altitude": w.get("altitude"),
        "vine_density": w.get("vine_density"),
        "exposure": w.get("exposure"),
        "vine_age": w.get("vine_age"),
        "vineyard_size": w.get("vineyard_size"),
        "yield": w.get("yield"),
        "tasting_notes": w.get("tasting_notes"),
        "serving_temp": w.get("serving_temp"),
        "food_pairings": w.get("food_pairings"),
        "aging_potential": w.get("aging_potential"),
        "abv": w.get("abv"),
        "winemaker": w.get("winemaker"),
        "description": w.get("description"),
        "production": w.get("production"),
        "harvest_time": w.get("harvest_time"),
        "bottling_period": w.get("bottling_period"),
        "first_vintage": w.get("first_vintage"),
        "extra_fields": w.get("extra_fields"),
    } for w in wines]
    count = batch_insert("source_empson", rows)
    print(f"  Inserted {count}/{len(wines)} rows")
    return count


def load_european_cellars() -> int:
    print("\n=== Loading European Cellars ===")
    wines = load_json("european_cellars_catalog.json")
    rows = []
    for w in wines:
        name = w.get("name")
        if not name and w.get("url_slug"):
            name = w["url_slug"].replace("-", " ").title()
        if not name:
            name = w.get("grape") or "Unknown"
        rows.append({
            "url": w.get("url"),
            "source_url": w.get("_source"),
            "producer": w.get("producer"),
            "name": name,
            "color": w.get("color"),
            "certifications": w.get("certifications"),
            "appellation": w.get("appellation"),
            "grape": w.get("grape"),
            "vine_age": w.get("vine_age"),
            "farming": w.get("farming"),
            "soil": w.get("soil"),
            "altitude": w.get("altitude"),
            "vinification": w.get("vinification"),
            "aging": w.get("aging"),
            "scores": w.get("scores"),
        })
    count = batch_insert("source_european_cellars", rows)
    print(f"  Inserted {count}/{len(wines)} rows")
    return count


def load_last_bottle() -> int:
    print("\n=== Loading Last Bottle ===")
    products = load_json("last_bottle_raw.json")
    wine_products = [p for p in products if p.get("product_type") and
                     ("Wine" in p["product_type"] or "Champagne" in p["product_type"])]
    rows = [{
        "shopify_id": str(p.get("shopify_id", "")),
        "shopify_handle": p.get("handle"),
        "title": p.get("title"),
        "producer": None,
        "wine_name": None,
        "country": None,
        "region": None,
        "appellation": None,
        "color": {"Red Wine": "red", "White Wine": "white", "Rosé Wine": "rose"}.get(p.get("product_type")),
        "wine_type": "sparkling" if p.get("product_type") == "Champagne" else None,
        "grape": None,
        "vintage": parse_vintage_from_title(p.get("title", "")),
        "price_usd": p.get("price"),
        "compare_at_price_usd": p.get("compare_at_price"),
        "description": p.get("body_html"),
        "tags": p.get("tags"),
        "metadata": {"product_type": p.get("product_type"), "vendor": p.get("vendor"),
                      "created_at": p.get("created_at")},
    } for p in wine_products]
    count = batch_insert("source_last_bottle", rows)
    print(f"  Inserted {count}/{len(wine_products)} rows")
    return count


def load_best_wine_store() -> int:
    print("\n=== Loading Best Wine Store ===")
    products = load_json("best_wine_store_raw.json")
    rows = [{
        "shopify_id": str(p.get("shopify_id", "")),
        "shopify_handle": p.get("handle"),
        "title": p.get("title"),
        "producer": p.get("vendor"),
        "wine_name": None,
        "country": None,
        "region": None,
        "appellation": None,
        "color": None,
        "wine_type": "sparkling" if p.get("product_type") == "Champagne" else None,
        "grape": None,
        "vintage": parse_vintage_from_title(p.get("title", "")),
        "price_usd": p.get("price"),
        "description": p.get("body_html"),
        "tags": p.get("tags"),
        "metadata": {"product_type": p.get("product_type"), "vendor": p.get("vendor"),
                      "created_at": p.get("created_at")},
    } for p in products]
    count = batch_insert("source_best_wine_store", rows)
    print(f"  Inserted {count}/{len(products)} rows")
    return count


def load_domestique() -> int:
    print("\n=== Loading Domestique ===")
    products = load_json("domestique_wine_raw.json")
    rows = [{
        "shopify_id": str(p.get("shopify_id", "")),
        "shopify_handle": p.get("handle"),
        "title": p.get("title"),
        "producer": p.get("vendor"),
        "wine_name": None,
        "country": parse_tag_value(p.get("tags"), "country"),
        "region": parse_tag_value(p.get("tags"), "region"),
        "appellation": None,
        "color": None,
        "wine_type": (parse_tag_value(p.get("tags"), "type") or "").lower() or None,
        "grape": parse_tag_value(p.get("tags"), "grape"),
        "vintage": parse_tag_value(p.get("tags"), "vintage") or parse_vintage_from_title(p.get("title", "")),
        "price_usd": p.get("price"),
        "description": p.get("body_html"),
        "tags": p.get("tags"),
        "metadata": {"product_type": p.get("product_type"), "vendor": p.get("vendor"),
                      "created_at": p.get("created_at"),
                      "certified": parse_tag_value(p.get("tags"), "certified")},
    } for p in products]
    count = batch_insert("source_domestique", rows)
    print(f"  Inserted {count}/{len(products)} rows")
    return count


LOADERS = {
    "polaner": load_polaner,
    "kl": load_kermit_lynch,
    "skurnik": load_skurnik,
    "winebow": load_winebow,
    "empson": load_empson,
    "ec": load_european_cellars,
    "last-bottle": load_last_bottle,
    "best-wine-store": load_best_wine_store,
    "domestique": load_domestique,
}


def main():
    parser = argparse.ArgumentParser(description="Load raw catalog JSON into staging tables")
    parser.add_argument("--source", required=True, help="Source name or 'all'")
    args = parser.parse_args()

    sources = list(LOADERS.keys()) if args.source == "all" else [s.strip() for s in args.source.split(",")]

    total = 0
    for source in sources:
        if source not in LOADERS:
            print(f"Unknown source: {source}. Available: {', '.join(LOADERS.keys())}")
            continue
        try:
            count = LOADERS[source]()
            total += count
        except Exception as e:
            print(f"Error loading {source}: {e}")

    print(f"\n=== Done. Total rows loaded: {total} ===")


if __name__ == "__main__":
    main()
