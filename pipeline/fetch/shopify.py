#!/usr/bin/env python3
"""
Generic Shopify catalog fetcher — paginates /products.json for any Shopify wine store.

Usage:
    python -m pipeline.fetch.shopify --url "https://store.example.com" --output data/imports/store_raw.json
    python -m pipeline.fetch.shopify --url "https://store.example.com" --limit 50
"""

import argparse
import json
import sys
import time
from pathlib import Path

import httpx


def fetch_shopify_catalog(base_url: str, limit: int | None = None, delay: float = 1.0) -> list[dict]:
    """
    Fetch all products from a Shopify store via /products.json.

    Args:
        base_url: Store URL (e.g., "https://store.example.com")
        limit: Max products to fetch (None = all)
        delay: Seconds between requests

    Returns:
        List of product dicts with shopify_id added
    """
    base_url = base_url.rstrip("/")
    all_products = []
    page = 1
    per_page = 250  # Shopify max

    with httpx.Client(timeout=30.0) as client:
        while True:
            url = f"{base_url}/products.json?limit={per_page}&page={page}"
            print(f"  Fetching page {page}...", end="", flush=True)

            resp = client.get(url)
            resp.raise_for_status()
            data = resp.json()
            products = data.get("products", [])

            if not products:
                print(" (empty, done)")
                break

            for p in products:
                p["shopify_id"] = p.pop("id", None)

            all_products.extend(products)
            print(f" {len(products)} products (total: {len(all_products)})")

            if limit and len(all_products) >= limit:
                all_products = all_products[:limit]
                break

            if len(products) < per_page:
                break

            page += 1
            time.sleep(delay)

    return all_products


def main():
    parser = argparse.ArgumentParser(description="Fetch Shopify wine store catalog")
    parser.add_argument("--url", required=True, help="Shopify store URL")
    parser.add_argument("--output", required=True, help="Output JSON file path")
    parser.add_argument("--limit", type=int, help="Max products to fetch")
    parser.add_argument("--delay", type=float, default=1.0, help="Seconds between requests")
    args = parser.parse_args()

    print(f"=== Fetching Shopify catalog from {args.url} ===\n")
    products = fetch_shopify_catalog(args.url, limit=args.limit, delay=args.delay)

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(products, indent=2, ensure_ascii=False), encoding="utf-8")

    print(f"\n=== Done. {len(products)} products saved to {args.output} ===")


if __name__ == "__main__":
    main()
