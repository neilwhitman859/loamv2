#!/usr/bin/env python3
"""
COLA Cloud API test round 2 -- match against known wines.
Uses ~10 of remaining requests.

Usage:
    python -m pipeline.analyze.test_cola_cloud_2
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

request_count = 0


def api_get(path: str, params: dict | None = None) -> dict:
    global request_count
    url = f"{BASE}{path}"
    print(f"  GET {path}")
    r = httpx.get(url, params=params or {}, headers={"X-API-Key": API_KEY}, timeout=30)
    request_count += 1
    r.raise_for_status()
    remaining = r.headers.get("x-ratelimit-remaining")
    if remaining:
        print(f"  -> Remaining: {remaining}")
    return r.json()


def search_and_detail(label: str, search_params: dict) -> dict | None:
    print(f"\n=== {label} ===")
    results = api_get("/colas", {
        "product_type": "wine", "per_page": 5,
        "approval_date_from": "2015-01-01", **search_params,
    })
    print(f"  Found: {results.get('pagination', {}).get('total')} COLAs")

    data = results.get("data", [])
    if not data:
        print("  (no results)")
        return None

    for c in data[:3]:
        print(f"  [search] {c.get('ttb_id')}: {c.get('brand_name')} -- {c.get('product_name')} | {c.get('origin_name')} | ABV {c.get('abv')}%")

    detail = api_get(f"/colas/{data[0]['ttb_id']}")
    d = detail.get("data", {})
    print(f"  [detail] Grapes: {json.dumps(d.get('grape_varietals'))}")
    print(f"  [detail] Appellation: {d.get('wine_appellation')}")
    print(f"  [detail] Vintage: {d.get('wine_vintage_year')}")
    print(f"  [detail] Barcode: {d.get('barcode_value')} ({d.get('barcode_type')})")
    print(f"  [detail] Designation: {d.get('llm_wine_designation')}")
    desc = d.get("llm_product_description") or ""
    print(f"  [detail] Description: {desc[:200]}")

    return d


def main():
    print("\n=== COLA Cloud Test Round 2: Known Wine Matching ===")

    details = []
    details.append(search_and_detail("Ridge Monte Bello", {"q": "Ridge Monte Bello"}))
    details.append(search_and_detail("Lopez de Heredia", {"q": "Lopez de Heredia"}))
    details.append(search_and_detail("Antinori Tignanello", {"q": "Tignanello"}))
    details.append(search_and_detail("Louis Roederer Cristal", {"q": "Cristal Roederer"}))
    details.append(search_and_detail("Chateau d'Yquem", {"q": "Yquem"}))

    # Barcode lookup
    print("\n=== Barcode Lookup Test ===")
    barcode = api_get("/barcodes/3554770154090")
    bc_data = barcode.get("data", {})
    print(f"Barcode lookup result:")
    print(f"  Total COLAs with this barcode: {bc_data.get('total_colas')}")

    # Grape coverage spot check
    print("\n=== Grape Coverage Spot Check (5 random details) ===")
    random_sample = api_get("/colas", {
        "product_type": "wine", "per_page": 100,
        "approval_date_from": "2024-06-01", "approval_date_to": "2024-12-31",
    })

    grape_count = app_count = vintage_count = barcode_count = 0
    for c in random_sample.get("data", [])[:5]:
        det = api_get(f"/colas/{c['ttb_id']}")
        d = det.get("data", {})
        if d.get("grape_varietals") and len(d["grape_varietals"]) > 0:
            grape_count += 1
        if d.get("wine_appellation"):
            app_count += 1
        if d.get("wine_vintage_year"):
            vintage_count += 1
        if d.get("barcode_value"):
            barcode_count += 1
        print(f"  {d.get('brand_name')} {d.get('product_name')}: "
              f"grapes={json.dumps(d.get('grape_varietals'))} | "
              f"app={d.get('wine_appellation')} | vintage={d.get('wine_vintage_year')} | "
              f"barcode={'yes' if d.get('barcode_value') else 'no'}")

    print(f"\nDetail field coverage (5 samples):")
    print(f"  Grapes: {grape_count}/5")
    print(f"  Appellation: {app_count}/5")
    print(f"  Vintage: {vintage_count}/5")
    print(f"  Barcode: {barcode_count}/5")

    # Save
    output = {
        "known_wine_details": [d for d in details if d],
        "barcode_lookup": bc_data,
    }
    out_path = DATA_DIR / "cola_cloud_test2.json"
    out_path.write_text(json.dumps(output, indent=2), encoding="utf-8")
    print(f"\nSaved to {out_path}")
    print(f"Total requests this run: {request_count}")
    print(f"Estimated remaining: {495 - request_count}")


if __name__ == "__main__":
    main()
