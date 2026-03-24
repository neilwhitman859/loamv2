#!/usr/bin/env python3
"""
Wally's Wine & Spirits Catalog Fetcher.

Source: wallywine.com — major LA retailer, 42K wine products
API:    Sanity CMS (completely open, no auth) + Shopify products.json
Fields: name, grape varietal, region, country, vintage, vendor (distributor),
        price, product type, tags

Usage:
    python -m pipeline.fetch.wallys
    python -m pipeline.fetch.wallys --analyze
    python -m pipeline.fetch.wallys --limit 500
"""

import argparse
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import httpx

# Shopify is simpler for bulk product listing
SHOPIFY_BASE = "https://cbbc83-3.myshopify.com"
OUTPUT_FILE = Path("data/imports/wallys_wines.json")
DELAY_S = 1.0
USER_AGENT = "LoamWineDB/1.0 (neil@loam.wine)"

# Wine collection handles to fetch
WINE_COLLECTIONS = [
    "domestic",     # US wines
    "imported",     # International wines
    "sparkling",    # Sparkling/Champagne
]


def fetch_products_page(client: httpx.Client, page: int, collection: str | None = None) -> list[dict]:
    """Fetch a page of products from Shopify."""
    if collection:
        url = f"{SHOPIFY_BASE}/collections/{collection}/products.json?limit=250&page={page}"
    else:
        url = f"{SHOPIFY_BASE}/products.json?limit=250&page={page}"
    resp = client.get(url)
    resp.raise_for_status()
    data = resp.json()
    return data.get("products", [])


def fetch_all_products(client: httpx.Client, limit: int | None = None) -> list[dict]:
    """Fetch all wine products."""
    all_products = []
    seen_ids: set[int] = set()

    # Try fetching all products without collection filter first
    page = 1
    while True:
        if limit and len(all_products) >= limit:
            break

        try:
            products = fetch_products_page(client, page)
        except Exception as err:
            print(f"  ERROR on page {page}: {err}")
            break

        if not products:
            break

        new = 0
        for p in products:
            pid = p.get("id")
            if pid and pid not in seen_ids:
                seen_ids.add(pid)
                all_products.append(p)
                new += 1

        if page % 20 == 0 or not products or len(products) < 250:
            print(f"  Page {page}: {new} new ({len(all_products)} total)")

        if len(products) < 250:
            break

        page += 1
        time.sleep(DELAY_S)

    return all_products


def extract_tag_value(tags: list[str], prefix: str) -> str | None:
    """Extract a tag value by prefix (e.g., 'Country:France' -> 'France')."""
    for tag in tags:
        if tag.lower().startswith(prefix.lower()):
            val = tag[len(prefix):].strip().strip(":")
            if val:
                return val
    return None


def extract_tags_multi(tags: list[str], prefix: str) -> list[str]:
    """Extract all tag values matching a prefix."""
    results = []
    for tag in tags:
        if tag.lower().startswith(prefix.lower()):
            val = tag[len(prefix):].strip().strip(":")
            if val:
                results.append(val)
    return results


def normalize_product(p: dict) -> dict:
    """Normalize a Shopify product into our standard format."""
    tags = p.get("tags", [])
    if isinstance(tags, str):
        tags = [t.strip() for t in tags.split(",")]

    # Extract structured tag data
    country = extract_tag_value(tags, "Country:")
    region = extract_tag_value(tags, "Region:")
    grapes = extract_tags_multi(tags, "Grape Variety:")
    vintage = extract_tag_value(tags, "Vintage:")
    producer = extract_tag_value(tags, "Producer:")
    sweetness = extract_tag_value(tags, "Sweetness:")
    body = extract_tag_value(tags, "Body:")
    pairing = extract_tags_multi(tags, "Pairing:")
    size = extract_tag_value(tags, "Size:")

    # Get price from first variant
    variants = p.get("variants", [])
    price = None
    sku = None
    if variants:
        price = variants[0].get("price")
        sku = variants[0].get("sku")

    return {
        "shopify_id": p.get("id"),
        "title": p.get("title"),
        "vendor": p.get("vendor"),  # Often the distributor (e.g., "BREAKTHRU Beverage")
        "product_type": p.get("product_type"),
        "handle": p.get("handle"),
        "country": country,
        "region": region,
        "grapes": grapes if grapes else None,
        "vintage": vintage,
        "producer": producer,
        "sweetness": sweetness,
        "body": body,
        "pairings": pairing if pairing else None,
        "size": size,
        "price": price,
        "sku": sku,
        "tags": tags,
        "created_at": p.get("created_at"),
    }


def is_wine_product(p: dict) -> bool:
    """Check if a product is wine (not spirits/beer/accessories)."""
    ptype = (p.get("product_type") or "").lower()
    title = (p.get("title") or "").lower()
    tags_str = " ".join(p.get("tags", [])).lower() if isinstance(p.get("tags"), list) else (p.get("tags") or "").lower()

    non_wine = ["spirit", "whiskey", "vodka", "tequila", "rum", "gin", "bourbon",
                "scotch", "beer", "ale", "lager", "stout", "cider", "seltzer",
                "mixer", "accessori", "gift", "glass", "opener", "decant"]

    for nw in non_wine:
        if nw in ptype or nw in title:
            return False

    wine_types = ["red wine", "white wine", "rosé", "rose wine", "sparkling",
                  "champagne", "pinot", "cabernet", "merlot", "chardonnay",
                  "domestic", "imported", "wine"]

    for wt in wine_types:
        if wt in ptype or wt in tags_str:
            return True

    # Default: include if product_type contains "wine" or is empty
    return "wine" in ptype or not ptype


def main():
    parser = argparse.ArgumentParser(description="Fetch Wally's Wine catalog")
    parser.add_argument("--analyze", action="store_true", help="Show tag analysis only")
    parser.add_argument("--limit", type=int, help="Max products to fetch")
    args = parser.parse_args()

    print("=== Wally's Wine & Spirits Catalog Fetcher ===")
    print(f"Shopify: {SHOPIFY_BASE}")
    print(f"Output: {OUTPUT_FILE}")

    with httpx.Client(timeout=30.0, headers={"User-Agent": USER_AGENT}) as client:
        if args.analyze:
            print("\nFetching sample page to analyze tags...")
            products = fetch_products_page(client, 1)
            print(f"\n{len(products)} products on page 1")
            if products:
                p = products[0]
                print(f"\nSample product:")
                print(f"  Title: {p.get('title')}")
                print(f"  Vendor: {p.get('vendor')}")
                print(f"  Product type: {p.get('product_type')}")
                print(f"  Tags: {p.get('tags')}")
                print(f"  Variants: {len(p.get('variants', []))}")
                if p.get("variants"):
                    v = p["variants"][0]
                    print(f"  First variant: price={v.get('price')}, sku={v.get('sku')}, barcode={v.get('barcode')}")

            # Collect tag prefixes from first few pages
            tag_prefixes: dict[str, int] = {}
            product_types: dict[str, int] = {}
            for page in range(1, 4):
                prods = fetch_products_page(client, page)
                for p in prods:
                    pt = p.get("product_type") or "none"
                    product_types[pt] = product_types.get(pt, 0) + 1
                    tags = p.get("tags", [])
                    if isinstance(tags, str):
                        tags = [t.strip() for t in tags.split(",")]
                    for tag in tags:
                        if ":" in tag:
                            prefix = tag.split(":")[0] + ":"
                            tag_prefixes[prefix] = tag_prefixes.get(prefix, 0) + 1
                time.sleep(DELAY_S)

            print(f"\nProduct types: {dict(sorted(product_types.items(), key=lambda x: -x[1]))}")
            print(f"\nTag prefixes: {dict(sorted(tag_prefixes.items(), key=lambda x: -x[1])[:30])}")
            return

        print(f"\nFetching all products (limit={args.limit or 'none'})...")
        raw_products = fetch_all_products(client, limit=args.limit)

    # Filter to wine
    wine_products = [p for p in raw_products if is_wine_product(p)]
    filtered = len(raw_products) - len(wine_products)
    print(f"\nFiltered {filtered} non-wine products ({len(wine_products)} wines remain)")

    wines = [normalize_product(p) for p in wine_products]

    # Stats
    total = len(wines)
    has_country = sum(1 for w in wines if w.get("country"))
    has_region = sum(1 for w in wines if w.get("region"))
    has_grapes = sum(1 for w in wines if w.get("grapes"))
    has_vintage = sum(1 for w in wines if w.get("vintage"))
    has_producer = sum(1 for w in wines if w.get("producer"))
    has_price = sum(1 for w in wines if w.get("price"))
    has_vendor = sum(1 for w in wines if w.get("vendor"))

    country_counts: dict[str, int] = {}
    vendor_counts: dict[str, int] = {}
    type_counts: dict[str, int] = {}
    for w in wines:
        c = w.get("country") or "unknown"
        country_counts[c] = country_counts.get(c, 0) + 1
        v = w.get("vendor") or "unknown"
        vendor_counts[v] = vendor_counts.get(v, 0) + 1
        t = w.get("product_type") or "unknown"
        type_counts[t] = type_counts.get(t, 0) + 1

    top_countries = dict(sorted(country_counts.items(), key=lambda x: -x[1])[:15])
    top_vendors = dict(sorted(vendor_counts.items(), key=lambda x: -x[1])[:15])

    print(f"\n=== RESULTS ===")
    print(f"Total wines: {total}")
    if total:
        print(f"Has country: {has_country}/{total} ({has_country/total*100:.1f}%)")
        print(f"Has region: {has_region}/{total} ({has_region/total*100:.1f}%)")
        print(f"Has grapes: {has_grapes}/{total} ({has_grapes/total*100:.1f}%)")
        print(f"Has vintage: {has_vintage}/{total} ({has_vintage/total*100:.1f}%)")
        print(f"Has producer: {has_producer}/{total} ({has_producer/total*100:.1f}%)")
        print(f"Has price: {has_price}/{total} ({has_price/total*100:.1f}%)")
        print(f"Has vendor/distributor: {has_vendor}/{total} ({has_vendor/total*100:.1f}%)")
        print(f"\nProduct types: {type_counts}")
        print(f"\nTop countries: {top_countries}")
        print(f"\nTop vendors/distributors: {top_vendors}")

    output = {
        "metadata": {
            "source": "Wally's Wine & Spirits",
            "url": "https://wallywine.com",
            "shopify": SHOPIFY_BASE,
            "fetched_at": datetime.now(timezone.utc).isoformat(),
            "stats": {
                "total": total,
                "raw_products": len(raw_products),
                "filtered_non_wine": filtered,
                "has_country": has_country,
                "has_region": has_region,
                "has_grapes": has_grapes,
                "has_vintage": has_vintage,
                "has_producer": has_producer,
                "has_price": has_price,
                "product_types": type_counts,
                "top_countries": top_countries,
                "top_vendors": top_vendors,
            },
        },
        "wines": wines,
    }

    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_FILE.write_text(json.dumps(output, indent=2, ensure_ascii=False))
    file_size_mb = OUTPUT_FILE.stat().st_size / 1024 / 1024
    print(f"\nSaved to {OUTPUT_FILE} ({file_size_mb:.1f} MB)")


if __name__ == "__main__":
    main()
