#!/usr/bin/env python3
"""
BC Liquor Stores Wine Catalog Fetcher.

Source: bcliquorstores.com — British Columbia state monopoly
API:    GET /ajax/browse?type=wine&page=N&limit=N (Elasticsearch, no auth)
Fields: UPC barcode, country, region, subRegion, grapeType, alcoholPercentage,
        sweetness, price, tasting description, color, organic/kosher/VQA flags

Usage:
    python -m pipeline.fetch.bc_liquor
    python -m pipeline.fetch.bc_liquor --limit 100
"""

import argparse
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import httpx

API_URL = "https://www.bcliquorstores.com/ajax/browse"
OUTPUT_FILE = Path("data/imports/bc_liquor_wines.json")
PAGE_SIZE = 100
DELAY_S = 1.0
USER_AGENT = "LoamWineDB/1.0 (neil@loam.wine)"


def fetch_page(client: httpx.Client, page: int) -> dict:
    params = {"type": "wine", "page": page, "size": PAGE_SIZE}
    resp = client.get(API_URL, params=params)
    resp.raise_for_status()
    return resp.json()


def extract_from_es_hit(hit: dict) -> dict:
    """Extract the product source from an Elasticsearch hit."""
    return hit.get("_source", hit)


def normalize_product(p: dict) -> dict:
    """Extract wine fields from BC Liquor product."""
    # UPC can be a list
    upc_raw = p.get("upc")
    if isinstance(upc_raw, list):
        upc = upc_raw[0] if upc_raw else None
    else:
        upc = upc_raw

    # Grape type: use specific varietal if available, else generic grapeType
    grape = p.get("redVarietal") or p.get("whiteVarietal") or p.get("grapeType")

    return {
        "sku": p.get("sku"),
        "name": p.get("name"),
        "upc": upc,
        "country": p.get("countryName"),
        "country_code": p.get("countryCode"),
        "region": p.get("region"),
        "sub_region": p.get("subRegion"),
        "grape_type": grape,
        "abv": p.get("alcoholPercentage"),
        "sweetness": p.get("sweetness"),
        "price": p.get("currentPrice"),
        "regular_price": p.get("regularPrice"),
        "color": p.get("color"),
        "product_type": p.get("productType"),
        "description": p.get("tastingDescription"),
        "volume_ml": float(p["volume"]) * 1000 if p.get("volume") else None,
        "organic": p.get("isOrganic"),
        "kosher": p.get("isKosher"),
        "vqa": p.get("isVQA"),
        "rating": p.get("consumerRating"),
        "votes": p.get("votes"),
        "category": p.get("category", {}).get("description") if isinstance(p.get("category"), dict) else None,
        "sub_category": p.get("subCategory", {}).get("description") if isinstance(p.get("subCategory"), dict) else None,
        "image_url": p.get("image"),
    }


def main():
    parser = argparse.ArgumentParser(description="Fetch BC Liquor Stores wines")
    parser.add_argument("--limit", type=int, help="Max products to fetch")
    args = parser.parse_args()

    print("=== BC Liquor Stores Wine Catalog Fetcher ===")
    print(f"API: {API_URL}")
    print(f"Output: {OUTPUT_FILE}")

    all_products = []

    with httpx.Client(timeout=30.0, headers={"User-Agent": USER_AGENT}) as client:
        # First page to get total count
        print("\nFetching page 1...")
        first = fetch_page(client, 0)

        # Elasticsearch response: hits.total, hits.hits[]._source
        if not isinstance(first, dict) or "hits" not in first:
            print("Unexpected response format. Keys:", list(first.keys()) if isinstance(first, dict) else type(first))
            return

        hits_obj = first["hits"]
        total_count = hits_obj.get("total", 0)
        if isinstance(total_count, dict):
            total_count = total_count.get("value", 0)

        es_hits = hits_obj.get("hits", [])
        products = [extract_from_es_hit(h) for h in es_hits]
        all_products.extend(products)
        total_pages = (total_count + PAGE_SIZE - 1) // PAGE_SIZE
        print(f"  Page 1: {len(products)} products (total in index: {total_count}, {total_pages} pages)")

        page = 1
        while page < total_pages:
            if args.limit and len(all_products) >= args.limit:
                break

            time.sleep(DELAY_S)
            try:
                data = fetch_page(client, page)
            except Exception as err:
                print(f"  ERROR on page {page + 1}: {err}")
                time.sleep(3.0)
                try:
                    data = fetch_page(client, page)
                except Exception:
                    break

            if not isinstance(data, dict) or "hits" not in data:
                break

            es_hits = data["hits"].get("hits", [])
            if not es_hits:
                print(f"  Page {page + 1}: empty, done")
                break

            page_products = [extract_from_es_hit(h) for h in es_hits]
            all_products.extend(page_products)

            if (page + 1) % 10 == 0 or len(page_products) < PAGE_SIZE:
                print(f"  Page {page + 1}/{total_pages}: {len(page_products)} products ({len(all_products)} total)")

            if len(page_products) < PAGE_SIZE:
                break

            page += 1

    if args.limit:
        all_products = all_products[:args.limit]

    wines = [normalize_product(p) for p in all_products]

    # Stats
    total = len(wines)
    has_upc = sum(1 for w in wines if w.get("upc"))
    has_country = sum(1 for w in wines if w.get("country"))
    has_region = sum(1 for w in wines if w.get("region"))
    has_grape = sum(1 for w in wines if w.get("grape_type"))
    has_abv = sum(1 for w in wines if w.get("abv"))
    has_price = sum(1 for w in wines if w.get("price"))
    has_description = sum(1 for w in wines if w.get("description"))

    country_counts: dict[str, int] = {}
    for w in wines:
        c = w.get("country") or "unknown"
        country_counts[c] = country_counts.get(c, 0) + 1
    top_countries = dict(sorted(country_counts.items(), key=lambda x: -x[1])[:15])

    print(f"\n=== RESULTS ===")
    print(f"Total wines: {total}")
    if total:
        print(f"Has UPC: {has_upc}/{total} ({has_upc/total*100:.1f}%)")
        print(f"Has country: {has_country}/{total} ({has_country/total*100:.1f}%)")
        print(f"Has region: {has_region}/{total} ({has_region/total*100:.1f}%)")
        print(f"Has grape: {has_grape}/{total} ({has_grape/total*100:.1f}%)")
        print(f"Has ABV: {has_abv}/{total} ({has_abv/total*100:.1f}%)")
        print(f"Has price: {has_price}/{total} ({has_price/total*100:.1f}%)")
        print(f"Has description: {has_description}/{total} ({has_description/total*100:.1f}%)")
        print(f"\nTop countries: {top_countries}")

    output = {
        "metadata": {
            "source": "BC Liquor Stores",
            "url": "https://www.bcliquorstores.com",
            "fetched_at": datetime.now(timezone.utc).isoformat(),
            "stats": {
                "total": total,
                "has_upc": has_upc,
                "has_country": has_country,
                "has_region": has_region,
                "has_grape": has_grape,
                "has_abv": has_abv,
                "has_price": has_price,
                "has_description": has_description,
                "top_countries": top_countries,
            },
        },
        "wines": wines,
    }

    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_FILE.write_text(json.dumps(output, indent=2, ensure_ascii=False), encoding="utf-8")
    file_size_mb = OUTPUT_FILE.stat().st_size / 1024 / 1024
    print(f"\nSaved to {OUTPUT_FILE} ({file_size_mb:.1f} MB)")


if __name__ == "__main__":
    main()
