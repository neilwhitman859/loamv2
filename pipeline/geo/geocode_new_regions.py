"""
Targeted Nominatim geocoding for specific newly created regions.

Usage:
  python -m pipeline.geo.geocode_new_regions --dry-run
  python -m pipeline.geo.geocode_new_regions --apply
"""

import sys
import json
import time
import argparse
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import httpx
from pipeline.lib.db import get_supabase
from pipeline.geo.helpers import nominatim_search, simplify_precision, RATE_LIMIT_S

REGIONS = [
    {"slug": "niagara-peninsula-region",
     "queries": ["Niagara Peninsula, Ontario, Canada", "Niagara Region, Ontario, Canada"]},
    {"slug": "okanagan-valley-region",
     "queries": ["Okanagan Valley, British Columbia, Canada", "Regional District of Central Okanagan, British Columbia"]},
    {"slug": "klein-karoo-region",
     "queries": ["Klein Karoo, South Africa", "Little Karoo, South Africa", "Klein Karoo, Western Cape"]},
    {"slug": "olifants-river-region",
     "queries": ["Olifants River Valley, South Africa", "Citrusdal, Western Cape, South Africa"]},
    {"slug": "scotland",
     "queries": ["Scotland, United Kingdom", "Scotland"]},
]


def main():
    parser = argparse.ArgumentParser(description="Geocode new regions")
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    apply = args.apply
    sb = get_supabase()
    client = httpx.Client(timeout=30)
    mode = "APPLY" if apply else "DRY RUN"
    print(f"\n=== Geocode New Regions ({mode}) ===\n")

    slugs = [r["slug"] for r in REGIONS]
    result = sb.table("regions").select("id, slug, name").in_("slug", slugs).is_(
        "deleted_at", "null"
    ).execute()
    region_by_slug = {r["slug"]: r for r in result.data}

    fixed = 0
    failed = 0

    for entry in REGIONS:
        region = region_by_slug.get(entry["slug"])
        if not region:
            print(f"  WARNING Region not found: {entry['slug']}")
            continue

        print(f"  {region['name']} ({entry['slug']})")

        if not apply:
            print(f"    Would try: {' -> '.join(entry['queries'])}")
            continue

        result_data = None
        for query in entry["queries"]:
            time.sleep(RATE_LIMIT_S)
            print(f"    Trying: \"{query}\"")
            try:
                results = nominatim_search(client, query)
                if results:
                    r = results[0]
                    geojson = r.get("geojson")
                    is_poly = geojson and geojson.get("type") in ("Polygon", "MultiPolygon")
                    if is_poly:
                        result_data = r
                        break
                    result_data = r  # keep as fallback
            except Exception as e:
                print(f"    Error: {e}")

        if not result_data:
            print(f"    FAIL All queries failed")
            failed += 1
            continue

        lat = float(result_data["lat"])
        lng = float(result_data["lon"])
        geojson = result_data.get("geojson")
        source_id = f"nominatim/{result_data.get('osm_type')}/{result_data.get('osm_id')}"
        is_poly = geojson and geojson.get("type") in ("Polygon", "MultiPolygon")

        if is_poly:
            simplified = simplify_precision(geojson)
            size_kb = len(json.dumps(simplified)) / 1024
            print(f"    ok Polygon ({simplified['type']}, {size_kb:.1f} KB)")
            sb.rpc("upsert_region_boundary", {
                "p_region_id": region["id"],
                "p_geojson": json.dumps(simplified),
                "p_source_id": source_id,
                "p_confidence": "approximate",
            }).execute()
        else:
            print(f"    ok Centroid ({lat:.4f}, {lng:.4f})")
            sb.rpc("upsert_region_boundary", {
                "p_region_id": region["id"],
                "p_lat": lat, "p_lng": lng,
                "p_source_id": source_id,
                "p_confidence": "geocoded",
            }).execute()
        fixed += 1

    client.close()
    print(f"\n=== Summary: Fixed={fixed}, Failed={failed} ===")


if __name__ == "__main__":
    main()
