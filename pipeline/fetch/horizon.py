#!/usr/bin/env python3
"""
Horizon Beverage (Southern Glazer's) Wine Catalog Fetcher.

Source: horizonbeverage.com -- MA/RI wholesale distributor
API:    POST /api/products/GetProducts (JSON, no auth)
Fields: producerName, productName, rawMaterialNames (grapes!),
        countryName, regionName, styleName, upc, size, caseSize

Usage:
    python -m pipeline.fetch.horizon
"""

import argparse
import json
import sys
import time
from pathlib import Path

import httpx

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

API_URL = "https://www.horizonbeverage.com/api/products/GetProducts"
OUTPUT_FILE = Path("data/imports/horizon_beverage_wines.json")
DELAY_S = 0.5


def fetch_page(client: httpx.Client, state: str, page: int) -> dict:
    body = {
        "Page": page, "State": state, "Category": "Wine",
        "Type": [], "Style": [], "Country": [], "Region": [], "Keywords": "",
    }
    resp = client.post(API_URL, json=body)
    resp.raise_for_status()
    return resp.json()


def fetch_all_for_state(client: httpx.Client, state: str) -> list[dict]:
    print(f"\nFetching {state} wines...")
    first = fetch_page(client, state, 1)
    total_items = first["totalItems"]
    total_pages = first["totalPages"]
    print(f"  {state}: {total_items} wines across {total_pages} pages")

    all_products = list(first["products"])
    for page in range(2, total_pages + 1):
        time.sleep(DELAY_S)
        try:
            data = fetch_page(client, state, page)
            all_products.extend(data["products"])
            if page % 20 == 0 or page == total_pages:
                print(f"  {state}: page {page}/{total_pages} ({len(all_products)} products)")
        except Exception as err:
            print(f"  ERROR on {state} page {page}: {err}")
            time.sleep(3.0)
            try:
                data = fetch_page(client, state, page)
                all_products.extend(data["products"])
            except Exception as err2:
                print(f"  RETRY FAILED on {state} page {page}: {err2}")

    print(f"  {state}: {len(all_products)} total products fetched")
    return all_products


def parse_size(size, unit) -> float | None:
    if not size or not unit:
        return None
    try:
        val = float(size)
    except (ValueError, TypeError):
        return None
    if unit == "mL":
        return val
    if unit == "L":
        return val * 1000
    return val


def normalize_product(p: dict) -> dict:
    grapes_raw = p.get("rawMaterialNames") or ""
    grapes = [g.strip() for g in grapes_raw.split(";") if g.strip()] if grapes_raw else []
    return {
        "state": p.get("stateName"),
        "item_number": p.get("itemNumber"),
        "category": p.get("productTypeName"),
        "style": p.get("styleName"),
        "producer": p.get("producerName"),
        "name": p.get("productName"),
        "grapes": grapes,
        "country": p.get("countryName"),
        "region": p.get("regionName"),
        "size_ml": parse_size(p.get("size"), p.get("sizeUnit")),
        "size_raw": f"{p['size']} {p['sizeUnit']}" if p.get("size") and p.get("sizeUnit") else None,
        "case_size": int(p["caseSize"]) if p.get("caseSize") else None,
        "upc": p["upc"].strip() if p.get("upc") else None,
    }


def main():
    parser = argparse.ArgumentParser(description="Fetch Horizon Beverage wines")
    parser.parse_args()

    print("=== Horizon Beverage Wine Catalog Fetcher ===")
    print(f"API: {API_URL}")
    print(f"Output: {OUTPUT_FILE}")

    with httpx.Client(timeout=30.0, headers={"Content-Type": "application/json; charset=utf-8"}) as client:
        ma_products = fetch_all_for_state(client, "MA")
        ri_products = fetch_all_for_state(client, "RI")

    ma_norm = [normalize_product(p) for p in ma_products]
    ri_norm = [normalize_product(p) for p in ri_products]

    by_upc: dict[str, dict] = {}
    no_upc: list[dict] = []

    for p in ma_norm:
        if p["upc"]:
            by_upc[p["upc"]] = {**p, "states": ["MA"]}
        else:
            no_upc.append({**p, "states": ["MA"]})

    ri_new = 0
    ri_dup = 0
    for p in ri_norm:
        if p["upc"] and p["upc"] in by_upc:
            by_upc[p["upc"]]["states"].append("RI")
            ri_dup += 1
        elif p["upc"]:
            by_upc[p["upc"]] = {**p, "states": ["RI"]}
            ri_new += 1
        else:
            no_upc.append({**p, "states": ["RI"]})

    all_wines = list(by_upc.values()) + no_upc

    non_wine_styles = {"Fruit", "Sangria", "Wine Cooler", "Specialty"}
    wines = [w for w in all_wines if w.get("style") not in non_wine_styles]
    filtered = len(all_wines) - len(wines)

    total = len(wines)
    has_upc = sum(1 for w in wines if w.get("upc"))
    has_grapes = sum(1 for w in wines if w.get("grapes"))
    has_country = sum(1 for w in wines if w.get("country"))
    has_region = sum(1 for w in wines if w.get("region"))

    print("\n=== RESULTS ===")
    print(f"MA wines: {len(ma_norm)}")
    print(f"RI wines: {len(ri_norm)}")
    print(f"RI unique (not in MA): {ri_new}")
    print(f"RI duplicates: {ri_dup}")
    print(f"Filtered non-wine: {filtered}")
    print(f"Total unique wines: {total}")
    if total:
        print(f"Has UPC: {has_upc}/{total} ({has_upc/total*100:.1f}%)")
        print(f"Has grapes: {has_grapes}/{total} ({has_grapes/total*100:.1f}%)")
        print(f"Has country: {has_country}/{total} ({has_country/total*100:.1f}%)")
        print(f"Has region: {has_region}/{total} ({has_region/total*100:.1f}%)")

    style_dist: dict[str, int] = {}
    country_dist: dict[str, int] = {}
    for w in wines:
        style_dist[w.get("style") or "unknown"] = style_dist.get(w.get("style") or "unknown", 0) + 1
        if w.get("country"):
            country_dist[w["country"]] = country_dist.get(w["country"], 0) + 1
    top_countries = dict(sorted(country_dist.items(), key=lambda x: -x[1])[:15])
    print(f"\nStyles: {style_dist}")
    print(f"\nTop countries: {top_countries}")

    from datetime import datetime, timezone
    output = {
        "metadata": {
            "source": "Horizon Beverage (Southern Glazer's)",
            "url": "https://www.horizonbeverage.com/our-products",
            "fetched_at": datetime.now(timezone.utc).isoformat(),
            "states": ["MA", "RI"],
            "stats": {
                "total": total, "ma_total": len(ma_norm), "ri_total": len(ri_norm),
                "ri_unique": ri_new, "ri_duplicate": ri_dup, "no_upc": len(no_upc),
                "filtered_non_wine": filtered, "has_upc": has_upc, "has_grapes": has_grapes,
                "has_country": has_country, "has_region": has_region,
                "styles": style_dist, "top_countries": top_countries,
            },
        },
        "wines": wines,
    }

    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_FILE.write_text(json.dumps(output, indent=2, ensure_ascii=False))
    file_size_mb = OUTPUT_FILE.stat().st_size / 1024 / 1024
    print(f"\nSaved to {OUTPUT_FILE}")
    print(f"File size: {file_size_mb:.1f} MB")


if __name__ == "__main__":
    main()
