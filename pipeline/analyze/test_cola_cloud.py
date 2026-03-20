#!/usr/bin/env python3
"""
COLA Cloud API test -- explore data quality on free tier.
Uses 3-5 of our 500 monthly requests.

Usage:
    python -m pipeline.analyze.test_cola_cloud
"""

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import httpx

from pipeline.lib.db import get_env

API_KEY = get_env("COLA_CLOUD_API_KEY")
BASE = "https://app.colacloud.us/api/v1"
DATA_DIR = Path(__file__).resolve().parents[2] / "data" / "imports"


def api_get(path: str, params: dict | None = None) -> dict:
    url = f"{BASE}{path}"
    print(f"  GET {path}")
    r = httpx.get(url, params=params or {}, headers={"X-API-Key": API_KEY}, timeout=30)
    r.raise_for_status()
    remaining = r.headers.get("x-ratelimit-remaining")
    if remaining:
        print(f"  -> Requests remaining: {remaining}")
    return r.json()


def main():
    print("\n=== COLA Cloud API Test ===\n")

    # Test 1: Total wine COLA count
    print("--- Test 1: Total wine COLA count ---")
    wines = api_get("/colas", {
        "product_type": "wine", "per_page": 1,
        "approval_date_from": "2015-01-01", "approval_date_to": "2026-12-31",
    })
    pagination = wines.get("pagination", {})
    print(f"Total wine COLAs: {pagination.get('total', 'unknown'):,}")
    print(f"Pages: {pagination.get('pages', 'unknown'):,}")

    # Test 2: Sample wine COLAs
    print("\n--- Test 2: Sample wine COLAs (100 records) ---")
    sample = api_get("/colas", {
        "product_type": "wine", "per_page": 100,
        "approval_date_from": "2025-01-01",
    })

    fields = {
        "brand_name": 0, "product_name": 0, "abv": 0,
        "wine_appellation": 0, "grape_varietals": 0,
        "wine_vintage_year": 0, "barcode_value": 0,
        "origin_name": 0, "class_name": 0,
    }
    for cola in sample.get("data", []):
        for field in fields:
            val = cola.get(field)
            if val is not None and val != "" and not (isinstance(val, list) and len(val) == 0):
                fields[field] += 1

    print("\nField coverage (out of 100):")
    for field, count in fields.items():
        print(f"  {field}: {count}%")

    print("\n--- Sample Records ---")
    for i, c in enumerate(sample.get("data", [])[:3]):
        print(f"\n[{i + 1}] {c.get('brand_name')} -- {c.get('product_name')}")
        print(f"    TTB ID: {c.get('ttb_id')}")
        print(f"    Class: {c.get('class_name')}")
        print(f"    Origin: {c.get('origin_name')} ({c.get('domestic_or_imported')})")
        print(f"    ABV: {c.get('abv')}")

    # Test 3: Detail record
    with_grapes = next(
        (c for c in sample.get("data", []) if c.get("grape_varietals") and len(c["grape_varietals"]) > 0),
        sample.get("data", [{}])[0] if sample.get("data") else {}
    )
    ttb_id = with_grapes.get("ttb_id")
    if ttb_id:
        print(f"\n--- Test 3: Detail for {ttb_id} ---")
        detail = api_get(f"/colas/{ttb_id}")
        d = detail.get("data", {})
        print(f"  Brand: {d.get('brand_name')}")
        print(f"  Product: {d.get('product_name')}")
        print(f"  Grapes: {json.dumps(d.get('grape_varietals'))}")
        print(f"  Appellation: {d.get('wine_appellation')}")
        print(f"  Barcode: {d.get('barcode_value')} ({d.get('barcode_type')})")

    # Test 4: Search for Ridge
    print('\n--- Test 4: Search for "Ridge" ---')
    ridge = api_get("/colas", {
        "product_type": "wine", "brand_name": "Ridge",
        "per_page": 10, "approval_date_from": "2015-01-01",
    })
    print(f"Ridge COLAs found: {ridge.get('pagination', {}).get('total')}")
    for c in ridge.get("data", [])[:5]:
        print(f"  {c.get('ttb_id')}: {c.get('brand_name')} -- {c.get('product_name')} ({c.get('abv')}% ABV)")

    # Test 5: Search for Opus One
    print('\n--- Test 5: Search for "Opus One" ---')
    opus = api_get("/colas", {
        "product_type": "wine", "q": "Opus One",
        "per_page": 10, "approval_date_from": "2015-01-01",
    })
    print(f"Opus One COLAs found: {opus.get('pagination', {}).get('total')}")
    for c in opus.get("data", [])[:5]:
        print(f"  {c.get('ttb_id')}: {c.get('brand_name')} -- {c.get('product_name')} | origin: {c.get('origin_name')}")

    # Save sample
    output = {
        "sample": sample.get("data", []),
        "ridge": ridge.get("data", []),
        "opus": opus.get("data", []),
        "detail": detail.get("data") if ttb_id else None,
        "total_wine_colas": pagination.get("total"),
    }
    out_path = DATA_DIR / "cola_cloud_sample.json"
    out_path.write_text(json.dumps(output, indent=2), encoding="utf-8")
    print(f"\nFull sample saved to {out_path}")
    print("Total API requests used: 5 of 500")


if __name__ == "__main__":
    main()
