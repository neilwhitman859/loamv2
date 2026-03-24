#!/usr/bin/env python3
"""
Spec's Wine & Fine Foods Catalog Fetcher.

Source: specsonline.com — major TX retailer, ~80K products (~20K wine)
UPC:    SKU field contains UPC barcode (e.g., 063444500052)
API:    WooCommerce Store API v1 (public, no auth)
Fields: name, UPC (via SKU), price, wine category, wine origin, wine size,
        images, rating, review count

Usage:
    python -m pipeline.fetch.specs
    python -m pipeline.fetch.specs --analyze
    python -m pipeline.fetch.specs --limit 1000
    python -m pipeline.fetch.specs --resume
"""

import argparse
import json
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import httpx

API_URL = "https://specsonline.com/wp-json/wc/store/v1/products"
OUTPUT_FILE = Path("data/imports/specs_wines.json")
CHECKPOINT_FILE = Path("data/imports/specs_checkpoint.json")
PAGE_SIZE = 100  # WooCommerce Store API max per_page
DELAY_S = 1.0
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"


def fetch_page(client: httpx.Client, page: int) -> tuple[list[dict], int]:
    """Fetch a page of products. Returns (products, total_pages)."""
    resp = client.get(API_URL, params={"per_page": PAGE_SIZE, "page": page})
    resp.raise_for_status()
    total_pages = int(resp.headers.get("X-WP-TotalPages", 0))
    return resp.json(), total_pages


def get_attribute(product: dict, attr_name: str) -> str | None:
    """Get an attribute value by name from product attributes."""
    for attr in product.get("attributes", []):
        if attr.get("name", "").lower() == attr_name.lower():
            terms = attr.get("terms", [])
            if terms:
                return terms[0].get("name")
    return None


def is_wine_product(product: dict) -> bool:
    """Check if a product is wine based on category and permalink."""
    permalink = (product.get("permalink") or "").lower()
    name = (product.get("name") or "").lower()
    category = get_attribute(product, "Wine Category") or ""

    # Skip non-wine by URL path
    non_wine = ["/beer/", "/spirits/", "/liquor/", "/whiskey/", "/vodka/",
                "/tequila/", "/rum/", "/gin/", "/bourbon/", "/scotch/",
                "/mixers/", "/accessories/", "/food/", "/gift/", "/cigar/",
                "/tobacco/", "/bar-tools/", "/glassware/"]
    for nw in non_wine:
        if nw in permalink:
            return False

    # Wine by URL
    if "/wine/" in permalink or "/shop/wine" in permalink:
        return True

    # Wine by category attribute
    if category and "wine" in category.lower():
        return True

    return False


def normalize_product(p: dict) -> dict:
    """Normalize a WooCommerce Store API product."""
    sku = p.get("sku", "")
    # Validate SKU as UPC (8-14 digits)
    upc = None
    if sku and re.match(r'^\d{8,14}$', sku):
        upc = sku

    # Price is in cents
    prices = p.get("prices", {})
    price_cents = prices.get("price")
    price = float(price_cents) / 100 if price_cents else None

    # Get image URL
    images = p.get("images", [])
    image_url = images[0].get("src") if images else None

    return {
        "specs_id": p.get("id"),
        "name": p.get("name"),
        "slug": p.get("slug"),
        "upc": upc,
        "sku": sku,
        "price": price,
        "permalink": p.get("permalink"),
        "wine_category": get_attribute(p, "Wine Category"),
        "wine_origin": get_attribute(p, "Wine Origin"),
        "wine_size": get_attribute(p, "Wine Size"),
        "rating": p.get("average_rating"),
        "review_count": p.get("review_count"),
        "image_url": image_url,
        "in_stock": p.get("is_in_stock"),
    }


def load_checkpoint() -> dict:
    if CHECKPOINT_FILE.exists():
        return json.loads(CHECKPOINT_FILE.read_text(encoding="utf-8"))
    return {"page": 1, "products": []}


def save_checkpoint(page: int, products: list[dict]):
    CHECKPOINT_FILE.parent.mkdir(parents=True, exist_ok=True)
    CHECKPOINT_FILE.write_text(json.dumps({"page": page, "count": len(products)}), encoding="utf-8")


def main():
    parser = argparse.ArgumentParser(description="Fetch Spec's wine catalog via WooCommerce Store API")
    parser.add_argument("--analyze", action="store_true", help="Fetch first page to analyze structure")
    parser.add_argument("--limit", type=int, help="Max products to fetch")
    parser.add_argument("--resume", action="store_true", help="Resume from checkpoint")
    args = parser.parse_args()

    print("=== Spec's Wine Catalog Fetcher (WooCommerce Store API) ===")
    print(f"API: {API_URL}")
    print(f"Output: {OUTPUT_FILE}")

    with httpx.Client(timeout=30.0, headers={"User-Agent": USER_AGENT}, follow_redirects=True) as client:
        if args.analyze:
            print("\nFetching first page to analyze...")
            products, total_pages = fetch_page(client, 1)
            print(f"\n{len(products)} products on page 1, {total_pages} total pages")
            if products:
                p = products[0]
                print(f"\nSample product:")
                print(f"  Name: {p.get('name')}")
                print(f"  SKU/UPC: {p.get('sku')}")
                print(f"  Permalink: {p.get('permalink')}")
                print(f"  Attributes:")
                for attr in p.get("attributes", []):
                    terms = [t.get("name") for t in attr.get("terms", [])]
                    print(f"    {attr.get('name')}: {terms}")
            return

        all_products = []
        seen_ids: set[int] = set()
        start_page = 1

        if args.resume:
            cp = load_checkpoint()
            start_page = cp.get("page", 1)
            print(f"Resuming from page {start_page}")

        page = start_page
        total_pages = None

        while True:
            if args.limit and len(all_products) >= args.limit:
                break

            try:
                products, tp = fetch_page(client, page)
                if total_pages is None:
                    total_pages = tp
                    print(f"\nTotal pages: {total_pages}")
            except httpx.HTTPStatusError as e:
                if e.response.status_code == 400:
                    # Past last page
                    break
                print(f"  ERROR page {page}: HTTP {e.response.status_code}")
                save_checkpoint(page, all_products)
                time.sleep(5.0)
                page += 1
                continue
            except Exception as err:
                print(f"  ERROR page {page}: {err}")
                save_checkpoint(page, all_products)
                time.sleep(5.0)
                page += 1
                continue

            if not products:
                break

            for p in products:
                pid = p.get("id")
                if pid and pid not in seen_ids:
                    seen_ids.add(pid)
                    if is_wine_product(p):
                        all_products.append(normalize_product(p))

            if page % 50 == 0 or page == 1:
                print(f"  Page {page}/{total_pages}: {len(all_products)} wine products")
                save_checkpoint(page + 1, all_products)

            if total_pages and page >= total_pages:
                break

            page += 1
            time.sleep(DELAY_S)

    if args.limit:
        all_products = all_products[:args.limit]

    # Stats
    total = len(all_products)
    has_upc = sum(1 for p in all_products if p.get("upc"))
    has_price = sum(1 for p in all_products if p.get("price"))
    has_category = sum(1 for p in all_products if p.get("wine_category"))
    has_origin = sum(1 for p in all_products if p.get("wine_origin"))

    upc_lengths: dict[int, int] = {}
    for p in all_products:
        if p.get("upc"):
            length = len(p["upc"])
            upc_lengths[length] = upc_lengths.get(length, 0) + 1

    category_counts: dict[str, int] = {}
    origin_counts: dict[str, int] = {}
    for p in all_products:
        cat = p.get("wine_category") or "unknown"
        category_counts[cat] = category_counts.get(cat, 0) + 1
        origin = p.get("wine_origin") or "unknown"
        origin_counts[origin] = origin_counts.get(origin, 0) + 1

    top_categories = dict(sorted(category_counts.items(), key=lambda x: -x[1])[:20])
    top_origins = dict(sorted(origin_counts.items(), key=lambda x: -x[1])[:20])

    print(f"\n=== RESULTS ===")
    print(f"Total wine products: {total}")
    if total:
        print(f"Has UPC: {has_upc}/{total} ({has_upc/total*100:.1f}%)")
        print(f"Has price: {has_price}/{total} ({has_price/total*100:.1f}%)")
        print(f"Has category: {has_category}/{total} ({has_category/total*100:.1f}%)")
        print(f"Has origin: {has_origin}/{total} ({has_origin/total*100:.1f}%)")
        print(f"UPC lengths: {upc_lengths}")
        print(f"\nTop categories: {top_categories}")
        print(f"\nTop origins: {top_origins}")

    output = {
        "metadata": {
            "source": "Spec's Wine & Fine Foods",
            "url": "https://specsonline.com",
            "api": API_URL,
            "fetched_at": datetime.now(timezone.utc).isoformat(),
            "stats": {
                "total": total,
                "has_upc": has_upc,
                "has_price": has_price,
                "has_category": has_category,
                "has_origin": has_origin,
                "upc_lengths": upc_lengths,
                "top_categories": top_categories,
                "top_origins": top_origins,
            },
        },
        "wines": all_products,
    }

    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_FILE.write_text(json.dumps(output, indent=2, ensure_ascii=False), encoding="utf-8")
    file_size_mb = OUTPUT_FILE.stat().st_size / 1024 / 1024
    print(f"\nSaved to {OUTPUT_FILE} ({file_size_mb:.1f} MB)")

    if CHECKPOINT_FILE.exists():
        CHECKPOINT_FILE.unlink()


if __name__ == "__main__":
    main()
