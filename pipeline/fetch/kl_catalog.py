#!/usr/bin/env python3
"""
Fetch Kermit Lynch full wine catalog via their JSON API.

Endpoints:
    /api/v1?action=getWines         -> wine list with IDs/SKUs
    /api/v1?action=getWine&id=N     -> wine detail (blend, soil, vine_age, viticulture)
    /api/v1?action=getGrowers       -> grower list
    /api/v1?action=getGrower&slug=X -> grower profile (about, founded, website, coords)
    /api/v1?action=getRegions       -> regions
    /api/v1?action=getFarming       -> farming types
    /api/v1?action=getWineTypes     -> wine types

Usage:
    python -m pipeline.fetch.kl_catalog
"""

import argparse
import json
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import httpx

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

BASE = "https://kermitlynch.com/api/v1"
CACHE_FILE = Path("data/imports/kl_wine_details_cache.json")
OUT_FILE = Path("data/imports/kermit_lynch_catalog.json")
DELAY_S = 0.1  # 100ms between requests
HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; Loam/1.0)",
    "Accept": "application/json",
}


def strip_html(html: str) -> str:
    if not html:
        return ""
    s = re.sub(r"<em[^>]*>", "", html)
    s = re.sub(r"</em>", "", s)
    s = re.sub(r"<p[^>]*>", "\n", s)
    s = re.sub(r"</p>", "", s)
    s = re.sub(r"<br\s*/?>", "\n", s)
    s = re.sub(r"<[^>]+>", "", s)
    s = s.replace("&bull;", "\u2022").replace("&amp;", "&")
    s = s.replace("&quot;", '"').replace("&#039;", "'")
    s = s.replace("&lt;", "<").replace("&gt;", ">")
    s = re.sub(r"\n{3,}", "\n\n", s)
    return s.strip()


def clean_wine_name(name: str) -> str:
    return re.sub(r"\s+", " ", strip_html(name)).strip()


def api_fetch(client: httpx.Client, action: str, params: str = "") -> dict | list | None:
    url = f"{BASE}?action={action}{params}"
    resp = client.get(url)
    text = resp.text
    if not text:
        return None
    return json.loads(text)


def main():
    parser = argparse.ArgumentParser(description="Fetch Kermit Lynch catalog")
    parser.parse_args()

    print("=== Kermit Lynch Full Catalog Extraction ===\n")

    with httpx.Client(timeout=20.0, headers=HEADERS) as client:
        # 1. Fetch reference data
        print("Fetching reference data...")
        wines_list = api_fetch(client, "getWines") or []
        growers_list = api_fetch(client, "getGrowers") or []
        regions = api_fetch(client, "getRegions") or []
        farming = api_fetch(client, "getFarming") or []
        wine_types = api_fetch(client, "getWineTypes") or []

        region_map = {r["id"]: r["name"] for r in regions}
        farming_map = {f["id"]: f["name"] for f in farming}
        wine_type_map = {t["id"]: t["value"] for t in wine_types}
        country_map = {1: "France", 2: "Italy"}

        print(f"  Wines: {len(wines_list)}")
        print(f"  Growers: {len(growers_list)}")
        print(f"  Regions: {len(regions)}")

        # Filter to actual wines
        wine_type_ids = {
            t["id"] for t in wine_types
            if t.get("value") in ("Red", "White", "Rosé", "Sparkling", "Dessert")
        }
        actual_wines = [w for w in wines_list if w.get("wine_type") in wine_type_ids]
        print(f"  Actual wines (excluding grocery/spirits): {len(actual_wines)}")

        # 2. Load cache
        detail_cache: dict = {}
        if CACHE_FILE.exists():
            detail_cache = json.loads(CACHE_FILE.read_text())
            print(f"\n  Loaded {len(detail_cache)} cached wine details")

        # 3. Fetch wine details
        need_fetch = [w for w in actual_wines if str(w["id"]) not in detail_cache]
        print(f"\n  Need to fetch {len(need_fetch)} wine details...\n")

        fetched = 0
        for wine in need_fetch:
            try:
                time.sleep(DELAY_S)
                detail = api_fetch(client, "getWine", f"&id={wine['id']}")
                if detail:
                    detail_cache[str(wine["id"])] = detail
                    fetched += 1
                    if fetched % 50 == 0:
                        print(f"  Fetched {fetched}/{len(need_fetch)} details...")
                        CACHE_FILE.parent.mkdir(parents=True, exist_ok=True)
                        CACHE_FILE.write_text(json.dumps(detail_cache, indent=2, ensure_ascii=False))
            except Exception as e:
                print(f"  Error fetching wine {wine['id']}: {e}")

        if fetched > 0:
            CACHE_FILE.write_text(json.dumps(detail_cache, indent=2, ensure_ascii=False))
            print(f"  Cached {fetched} new details")

        # 4. Fetch grower profiles
        print("\nFetching grower profiles...")
        grower_profiles: dict = {}
        growers_fetched = 0
        for grower in growers_list:
            try:
                time.sleep(DELAY_S)
                profile = api_fetch(client, "getGrower", f"&slug={grower['slug']}")
                if profile:
                    grower_profiles[str(grower["id"])] = profile
                    growers_fetched += 1
                    if growers_fetched % 20 == 0:
                        print(f"  Fetched {growers_fetched}/{len(growers_list)} grower profiles...")
            except Exception as e:
                print(f"  Error fetching grower {grower.get('slug')}: {e}")
        print(f"  Fetched {growers_fetched} grower profiles")

        # 5. Assemble catalog
        print("\nAssembling catalog...")

        growers = []
        for g in growers_list:
            profile = grower_profiles.get(str(g["id"]))
            farming_ids = [int(x) for x in str(g.get("farming", "")).split(",") if x.strip().isdigit()]
            growers.append({
                "kl_id": g["id"],
                "name": g["name"],
                "slug": g["slug"],
                "country": country_map.get(g.get("country"), f"country_{g.get('country')}"),
                "region": region_map.get(g.get("region")),
                "farming": [farming_map[fid] for fid in farming_ids if fid in farming_map],
                "winemaker": profile.get("producer") if profile else None,
                "founded_year": profile.get("founded") if profile else None,
                "website": profile.get("www") if profile else None,
                "location": profile.get("location") if profile else None,
                "annual_production": profile.get("annual_production") if profile else None,
                "viticulture_notes": strip_html(profile.get("viticulture", "")) if profile else None,
                "about": strip_html(profile.get("about", ""))[:500] if profile else None,
            })

        wines = []
        for w in actual_wines:
            detail = detail_cache.get(str(w["id"]), {})
            farming_ids = [int(x) for x in str(w.get("farming", "")).split(",") if x.strip().isdigit()]
            wines.append({
                "kl_id": w["id"],
                "sku": w.get("sku"),
                "wine_name": clean_wine_name(w.get("name", "")),
                "grower_name": w.get("grower"),
                "grower_kl_id": w.get("grower_id"),
                "country": country_map.get(w.get("country"), f"country_{w.get('country')}"),
                "region": region_map.get(w.get("region")),
                "wine_type": wine_type_map.get(w.get("wine_type")),
                "blend": detail.get("blend"),
                "soil": detail.get("soil"),
                "vine_age": detail.get("vine_age"),
                "vineyard_area": detail.get("vineyard_area"),
                "vinification": strip_html(detail.get("viticulture", "")) if detail.get("viticulture") else None,
                "farming": [farming_map[fid] for fid in farming_ids if fid in farming_map],
            })

        catalog = {
            "source": "kermitlynch.com",
            "extracted": datetime.now(timezone.utc).isoformat(),
            "summary": {
                "total_wines": len(wines),
                "total_growers": len(growers),
                "regions": sorted({w["region"] for w in wines if w.get("region")}),
                "countries": sorted({w["country"] for w in wines}),
                "wine_types": sorted({w["wine_type"] for w in wines if w.get("wine_type")}),
            },
            "growers": growers,
            "wines": wines,
        }

        OUT_FILE.parent.mkdir(parents=True, exist_ok=True)
        OUT_FILE.write_text(json.dumps(catalog, indent=2, ensure_ascii=False))
        print(f"\nSaved catalog to {OUT_FILE}")
        print(f"  {len(wines)} wines from {len(growers)} growers")
        print(f"  Regions: {', '.join(catalog['summary']['regions'])}")

        # Stats
        has_blend = sum(1 for w in wines if w.get("blend"))
        has_soil = sum(1 for w in wines if w.get("soil"))
        has_vine_age = sum(1 for w in wines if w.get("vine_age"))
        has_vinification = sum(1 for w in wines if w.get("vinification"))
        total = len(wines) or 1
        print(f"\nData completeness:")
        print(f"  Blend: {has_blend}/{total} ({100*has_blend//total}%)")
        print(f"  Soil: {has_soil}/{total} ({100*has_soil//total}%)")
        print(f"  Vine age: {has_vine_age}/{total} ({100*has_vine_age//total}%)")
        print(f"  Vinification: {has_vinification}/{total} ({100*has_vinification//total}%)")


if __name__ == "__main__":
    main()
