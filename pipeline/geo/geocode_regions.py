"""
Geocodes regions via Nominatim with query overrides from data/region_nominatim_queries.json.
Uses progressive simplification for polygons.

Usage:
  python -m pipeline.geo.geocode_regions --dry-run    # preview (default)
  python -m pipeline.geo.geocode_regions --apply       # apply changes
"""

import sys
import json
import time
import argparse
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import httpx
from pipeline.lib.db import get_supabase
from pipeline.geo.helpers import (
    nominatim_search, has_polygon, simplify_precision, progressive_simplify,
    RATE_LIMIT_S, fetch_all_paginated,
)

PROJECT_ROOT = Path(__file__).resolve().parents[2]


def main():
    parser = argparse.ArgumentParser(description="Geocode regions via Nominatim")
    parser.add_argument("--apply", action="store_true", help="Apply changes (default is dry-run)")
    parser.add_argument("--dry-run", action="store_true", help="Preview only (default)")
    args = parser.parse_args()

    apply = args.apply
    sb = get_supabase()
    client = httpx.Client(timeout=30)
    mode = "APPLY" if apply else "DRY RUN"
    print(f"\n=== Geocode Regions ({mode}) ===\n")

    # Load query overrides
    overrides_path = PROJECT_ROOT / "data" / "region_nominatim_queries.json"
    overrides = {}
    if overrides_path.exists():
        overrides = json.loads(overrides_path.read_text(encoding="utf-8"))
        print(f"Loaded {len(overrides)} query overrides")

    # Get all regions with country
    regions = fetch_all_paginated(sb, "regions", "id, slug, name, country_id, is_catch_all")

    # Get countries
    countries = sb.table("countries").select("id, name").execute().data
    country_by_id = {c["id"]: c["name"] for c in countries}

    # Get existing boundaries
    existing = fetch_all_paginated(sb, "geographic_boundaries", "id, region_id")
    existing_set = {e["region_id"] for e in existing if e.get("region_id")}

    # Filter to non-catch-all regions without boundaries
    todo = [r for r in regions if not r.get("is_catch_all") and r["id"] not in existing_set]
    print(f"Total regions: {len(regions)}")
    print(f"To geocode: {len(todo)}\n")

    fixed = 0
    failed = 0

    for i, r in enumerate(todo):
        country_name = country_by_id.get(r["country_id"], "Unknown")
        print(f"  [{i + 1}/{len(todo)}] {r['name']} ({country_name})")

        # Build queries
        if r["slug"] in overrides:
            queries = overrides[r["slug"]]
        else:
            queries = [
                f"{r['name']}, {country_name}",
                f"{r['name']} wine region, {country_name}",
            ]

        if not apply:
            print(f"    Would try: {' -> '.join(queries)}")
            continue

        result = None
        for query in queries:
            time.sleep(RATE_LIMIT_S)
            try:
                results = nominatim_search(client, query)
                if results:
                    result = results[0]
                    if has_polygon(result):
                        break
            except Exception as e:
                print(f"    Error: {e}")

        if not result:
            print(f"    FAIL no results")
            failed += 1
            continue

        lat = float(result["lat"])
        lng = float(result["lon"])
        geojson = result.get("geojson")
        osm_type = result.get("osm_type", "")
        osm_id = result.get("osm_id", "")
        source_id = f"nominatim/{osm_type}/{osm_id}"
        is_poly = geojson and geojson.get("type") in ("Polygon", "MultiPolygon")

        if is_poly:
            simplified = progressive_simplify(geojson)
            size_kb = len(json.dumps(simplified)) / 1024
            print(f"    ok Polygon ({simplified['type']}, {size_kb:.1f} KB)")
            sb.rpc("upsert_region_boundary", {
                "p_region_id": r["id"],
                "p_geojson": json.dumps(simplified),
                "p_source_id": source_id,
                "p_confidence": "approximate",
            }).execute()
        else:
            print(f"    ok Centroid ({lat:.4f}, {lng:.4f})")
            sb.rpc("upsert_region_boundary", {
                "p_region_id": r["id"],
                "p_lat": lat, "p_lng": lng,
                "p_source_id": source_id,
                "p_confidence": "geocoded",
            }).execute()
        fixed += 1

    client.close()
    print(f"\n=== Summary ===")
    print(f"Fixed: {fixed}")
    print(f"Failed: {failed}")


if __name__ == "__main__":
    main()
