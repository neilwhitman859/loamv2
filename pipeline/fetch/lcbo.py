#!/usr/bin/env python3
"""
Fetch LCBO wine catalog via GraphQL API.

Free API, no auth needed. Rate limit: 60 req/min.
Each product has UPC barcode, producer, country, region, ABV, price.

Usage:
    python -m pipeline.fetch.lcbo
"""

import argparse
import json
import sys
import time
from pathlib import Path

import httpx

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

API = "https://api.lcbo.dev/graphql"
CATEGORIES = ["red-wine", "white-wine", "rose-wine", "sparkling-wine", "champagne", "fortified-wine", "dessert-wine"]
PAGE_SIZE = 50
OUTPUT_FILE = Path("data/imports/lcbo_catalog.json")

QUERY = """
query FetchWines($category: String!, $after: String) {
  products(
    filters: { categorySlug: $category }
    pagination: { first: %d, after: $after }
  ) {
    totalCount
    pageInfo {
      hasNextPage
      endCursor
    }
    edges {
      node {
        sku
        name
        upcNumber
        producerName
        countryOfManufacture
        regionName
        alcoholPercent
        priceInCents
        primaryCategory
        shortDescription
        unitVolumeMl
        sellingPackage
        isVqa
        isKosher
        isSeasonal
        updatedAt
      }
    }
  }
}
""" % PAGE_SIZE


def gql(client: httpx.Client, query: str, variables: dict) -> dict:
    resp = client.post(API, json={"query": query, "variables": variables})
    resp.raise_for_status()
    data = resp.json()
    if data.get("errors"):
        raise RuntimeError("; ".join(e["message"] for e in data["errors"]))
    return data["data"]


def main():
    parser = argparse.ArgumentParser(description="Fetch LCBO wine catalog")
    parser.parse_args()

    all_wines: list[dict] = []

    with httpx.Client(timeout=30.0, headers={"Content-Type": "application/json"}) as client:
        for cat in CATEGORIES:
            after = None
            page_num = 0

            print(f"\nFetching {cat}...")
            while True:
                data = gql(client, QUERY, {"category": cat, "after": after})
                products = data["products"]

                if page_num == 0:
                    print(f"  {products['totalCount']} products")

                for edge in products["edges"]:
                    n = edge["node"]
                    all_wines.append({
                        "sku": n.get("sku"),
                        "name": n.get("name"),
                        "upc": n.get("upcNumber"),
                        "producer": n.get("producerName"),
                        "country": n.get("countryOfManufacture"),
                        "region": n.get("regionName"),
                        "abv": n.get("alcoholPercent"),
                        "price_cad_cents": n.get("priceInCents"),
                        "category": n.get("primaryCategory"),
                        "description": n.get("shortDescription"),
                        "volume_ml": n.get("unitVolumeMl"),
                        "selling_package": n.get("sellingPackage"),
                        "is_vqa": n.get("isVqa"),
                        "is_kosher": n.get("isKosher"),
                        "updated_at": n.get("updatedAt"),
                    })

                page_num += 1
                if not products["pageInfo"]["hasNextPage"]:
                    break
                after = products["pageInfo"]["endCursor"]
                time.sleep(1.1)  # Stay under 60/min

            print(f"  Fetched {len(all_wines)} total so far")

    total = len(all_wines)
    has_upc = sum(1 for w in all_wines if w.get("upc"))
    has_producer = sum(1 for w in all_wines if w.get("producer"))
    has_abv = sum(1 for w in all_wines if w.get("abv"))
    has_country = sum(1 for w in all_wines if w.get("country"))
    has_region = sum(1 for w in all_wines if w.get("region"))

    print(f"\n=== LCBO Fetch Complete ===")
    print(f"Total wines: {total}")
    if total:
        print(f"Has UPC: {has_upc} ({has_upc/total*100:.1f}%)")
        print(f"Has producer: {has_producer} ({has_producer/total*100:.1f}%)")
        print(f"Has ABV: {has_abv} ({has_abv/total*100:.1f}%)")
        print(f"Has country: {has_country} ({has_country/total*100:.1f}%)")
        print(f"Has region: {has_region} ({has_region/total*100:.1f}%)")

    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_FILE.write_text(json.dumps(all_wines, indent=2, ensure_ascii=False))
    print(f"\nSaved to {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
