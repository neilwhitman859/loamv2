#!/usr/bin/env python3
"""
Fetch wine products from Open Food Facts.

Paginates through the OFF search API for wine categories,
filters for actual wines (with barcodes, alcohol content, etc.),
and saves structured data for staging import.

Usage:
    python -m pipeline.fetch.openfoodfacts
    python -m pipeline.fetch.openfoodfacts --limit 500

Output: data/imports/openfoodfacts_wines.json
"""

import argparse
import json
import sys
import time
from pathlib import Path

import httpx

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

OUTPUT_FILE = Path("data/imports/openfoodfacts_wines.json")
PAGE_SIZE = 100  # max allowed by OFF API
DELAY_S = 2.0  # be polite -- OFF is volunteer-run
USER_AGENT = "LoamWineDB/1.0 (neil@loam.wine)"

WINE_CATEGORIES = [
    "en:wines", "en:red-wines", "en:white-wines", "en:rose-wines",
    "en:sparkling-wines", "en:champagnes", "en:dessert-wines", "en:fortified-wines",
]

EXCLUDE_PATTERNS = [
    "vinegar", "vinaigre", "juice", "jus", "jam", "confiture",
    "marmelade", "sauce", "cooking", "cuisine", "non-alcoholic",
    "sans-alcool", "alcohol-free", "dealcoholized",
]

FIELDS = ",".join([
    "code", "product_name", "product_name_en", "product_name_fr",
    "brands", "categories_tags", "countries_tags", "labels_tags",
    "quantity", "alcohol_100g", "alcohol_value", "alcohol_unit",
    "origins", "origins_tags", "manufacturing_places",
    "nutriments", "nutrition_grades_tags",
    "image_front_url", "url", "states_tags",
])


def is_actual_wine(product: dict) -> bool:
    cats = " ".join(product.get("categories_tags", [])).lower()
    name = (product.get("product_name") or product.get("product_name_en")
            or product.get("product_name_fr") or "").lower()

    code = product.get("code", "")
    if not code or len(code) < 8:
        return False

    for pat in EXCLUDE_PATTERNS:
        if pat in cats or pat in name:
            return False

    has_wine_cat = any(
        any(kw in c for kw in ("wine", "vin", "champagne", "prosecco", "cava", "porto", "sherry", "madeira"))
        for c in product.get("categories_tags", [])
    )
    return has_wine_cat


def extract_wine_data(p: dict) -> dict:
    name = p.get("product_name") or p.get("product_name_en") or p.get("product_name_fr")
    nutriments = p.get("nutriments") or {}

    abv = None
    if p.get("alcohol_value"):
        abv = float(p["alcohol_value"])
    elif p.get("alcohol_100g"):
        abv = float(p["alcohol_100g"])
    elif nutriments.get("alcohol_100g"):
        abv = float(nutriments["alcohol_100g"])

    countries = []
    for c in p.get("countries_tags", []):
        c_clean = c.replace("en:", "").replace("-", " ")
        countries.append(c_clean[0].upper() + c_clean[1:] if c_clean else c_clean)

    cats = ",".join(p.get("categories_tags", []))
    color = None
    if "red-wine" in cats or "vins-rouges" in cats:
        color = "red"
    elif "white-wine" in cats or "vins-blancs" in cats:
        color = "white"
    elif "rose-wine" in cats or "vins-roses" in cats:
        color = "rose"

    wine_type = "table"
    if any(kw in cats for kw in ("sparkling", "champagne", "prosecco", "cava", "cremant")):
        wine_type = "sparkling"
    elif any(kw in cats for kw in ("dessert", "sweet-wine")):
        wine_type = "dessert"
    elif any(kw in cats for kw in ("fortified", "porto", "sherry", "madeira")):
        wine_type = "fortified"

    labels = [l.replace("en:", "").replace("-", " ") for l in p.get("labels_tags", [])]

    return {
        "barcode": p.get("code"),
        "name": name,
        "brand": p.get("brands") or None,
        "countries": countries,
        "color": color,
        "wine_type": wine_type,
        "abv": abv if abv and abv > 0 else None,
        "categories": p.get("categories_tags", []),
        "labels": labels,
        "origins": p.get("origins") or None,
        "origins_tags": p.get("origins_tags", []),
        "quantity": p.get("quantity") or None,
        "manufacturing_places": p.get("manufacturing_places") or None,
        "energy_kcal": nutriments.get("energy-kcal_100g"),
        "sugars_g": nutriments.get("sugars_100g"),
        "off_url": p.get("url") or f"https://world.openfoodfacts.org/product/{p.get('code')}",
    }


def fetch_page(client: httpx.Client, category: str, page: int) -> dict | None:
    url = (
        f"https://world.openfoodfacts.org/cgi/search.pl?"
        f"action=process&tagtype_0=categories&tag_contains_0=contains&tag_0={category}"
        f"&fields={FIELDS}&sort_by=unique_scans_n&page_size={PAGE_SIZE}&page={page}&json=1"
    )
    resp = client.get(url)
    if resp.status_code != 200:
        print(f"  HTTP {resp.status_code} for {category} page {page}")
        return None
    return resp.json()


def main():
    parser = argparse.ArgumentParser(description="Fetch Open Food Facts wines")
    parser.add_argument("--limit", type=int, default=None)
    args = parser.parse_args()
    limit = args.limit or float("inf")

    print("=== Open Food Facts Wine Fetcher ===\n")

    all_wines: dict[str, dict] = {}  # barcode -> wine data (dedup)
    total_fetched = 0
    total_filtered = 0

    with httpx.Client(timeout=30.0, headers={"User-Agent": USER_AGENT}) as client:
        for category in WINE_CATEGORIES:
            print(f"\nCategory: {category}")
            page = 1
            has_more = True

            while has_more and len(all_wines) < limit:
                data = fetch_page(client, category, page)
                if not data or not data.get("products"):
                    has_more = False
                    break

                total_fetched += len(data["products"])

                for p in data["products"]:
                    if is_actual_wine(p):
                        wine = extract_wine_data(p)
                        if wine["barcode"] not in all_wines:
                            all_wines[wine["barcode"]] = wine
                    else:
                        total_filtered += 1

                total_count = data.get("count", "?")
                if page % 5 == 0 or len(data["products"]) < PAGE_SIZE:
                    print(f"  Page {page} -- {len(all_wines)} wines ({total_count} total in category)")

                if len(data["products"]) < PAGE_SIZE:
                    has_more = False
                if page >= 100:
                    print(f"  Hit page 100 limit for {category}")
                    has_more = False
                if len(all_wines) >= limit:
                    break

                page += 1
                time.sleep(DELAY_S)

    wines = list(all_wines.values())
    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_FILE.write_text(json.dumps(wines, indent=2, ensure_ascii=False))

    print(f"\n=== Results ===")
    print(f"  Total API results: {total_fetched}")
    print(f"  Filtered out: {total_filtered}")
    print(f"  Unique wines: {len(wines)}")
    print(f"  Saved to: {OUTPUT_FILE}")

    total = len(wines) or 1
    def stat(label, fn):
        n = sum(1 for w in wines if fn(w))
        print(f"  {label}: {n}/{len(wines)} ({n/total*100:.1f}%)")

    print("")
    stat("Has name", lambda w: w.get("name"))
    stat("Has brand", lambda w: w.get("brand"))
    stat("Has ABV", lambda w: w.get("abv"))
    stat("Has country", lambda w: len(w.get("countries", [])) > 0)
    stat("Has color", lambda w: w.get("color"))
    stat("Has origins", lambda w: w.get("origins"))
    stat("Has labels", lambda w: len(w.get("labels", [])) > 0)
    stat("Has nutrition", lambda w: w.get("energy_kcal"))

    country_counts: dict[str, int] = {}
    for w in wines:
        for c in w.get("countries", []):
            country_counts[c] = country_counts.get(c, 0) + 1
    top_countries = sorted(country_counts.items(), key=lambda x: -x[1])[:15]
    print("\n  Top countries:")
    for c, n in top_countries:
        print(f"    {c}: {n}")


if __name__ == "__main__":
    main()
