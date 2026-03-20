"""
Fixes specific geocoding issues: Swiss cantons and Argentine provinces.

Usage:
  python -m pipeline.geo.fix_region_geocodes --dry-run
  python -m pipeline.geo.fix_region_geocodes --apply
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

RETRIES = {
    "fribourg": ["Fribourg, Schweiz", "Kanton Freiburg"],
    "graubunden": ["Graubunden, Schweiz", "Kanton Graubunden"],
    "luzern": ["Luzern, Schweiz", "Kanton Luzern"],
    "schaffhausen": ["Schaffhausen, Schweiz", "Kanton Schaffhausen"],
    "st-gallen": ["Sankt Gallen, Schweiz", "St. Gallen, Switzerland"],
    "thurgau": ["Thurgau, Schweiz", "Kanton Thurgau"],
    "ticino": ["Ticino, Svizzera", "Canton Ticino, Switzerland"],
    "valais": ["Valais, Suisse", "Wallis, Schweiz"],
    "buenos-aires": ["Provincia de Buenos Aires, Argentina"],
    "salta": ["Provincia de Salta, Argentina"],
    "san-juan": ["Provincia de San Juan, Argentina"],
    "la-rioja": ["Provincia de La Rioja, Argentina"],
    "cordoba": ["Provincia de Cordoba, Argentina"],
    "patagonia": ["Provincia de Rio Negro, Argentina"],
}


def main():
    parser = argparse.ArgumentParser(description="Fix region geocodes")
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    apply = args.apply
    sb = get_supabase()
    client = httpx.Client(timeout=30)
    mode = "APPLY" if apply else "DRY RUN"
    print(f"\n=== Fix Region Geocodes ({mode}) ===\n")

    slugs = list(RETRIES.keys())
    regions_result = sb.table("regions").select("id, slug, name").in_("slug", slugs).is_(
        "deleted_at", "null"
    ).execute()
    region_by_slug = {r["slug"]: r for r in regions_result.data}

    fixed = 0
    failed = 0

    for slug, queries in RETRIES.items():
        region = region_by_slug.get(slug)
        if not region:
            print(f"  WARNING Region not found: {slug}")
            continue

        print(f"  {region['name']} ({slug})")

        if not apply:
            print(f"    Would try: {' -> '.join(queries)}")
            continue

        result = None
        for query in queries:
            time.sleep(RATE_LIMIT_S)
            print(f"    Trying: \"{query}\"")
            try:
                results = nominatim_search(client, query)
                if results:
                    result = results[0]
                    break
            except Exception as e:
                print(f"    Error: {e}")

        if not result:
            print(f"    FAIL All queries failed")
            failed += 1
            continue

        lat = float(result["lat"])
        lng = float(result["lon"])
        geojson = result.get("geojson")
        source_id = f"nominatim/{result.get('osm_type')}/{result.get('osm_id')}"
        is_poly = geojson and geojson.get("type") in ("Polygon", "MultiPolygon")

        # Delete existing bad entry
        sb.table("geographic_boundaries").delete().eq("region_id", region["id"]).execute()

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
