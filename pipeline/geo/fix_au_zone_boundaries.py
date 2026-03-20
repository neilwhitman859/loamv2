"""
Fixes Australian zone boundary polygon import.
The original populate_au_containment script called upsert_appellation_boundary
with p_boundary_geojson but the actual function signature uses p_geojson.

Reads data/geo/wine_australia_zones.geojson, matches each zone feature to an
existing zone appellation in the DB, and calls the RPC with correct parameter names.

Usage: python -m pipeline.geo.fix_au_zone_boundaries [--dry-run]
"""

import sys
import json
import argparse
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from pipeline.lib.db import get_supabase
from pipeline.geo.helpers import (
    simplify_precision, simplify_geometry, geo_slugify,
)

PROJECT_ROOT = Path(__file__).resolve().parents[2]


def main():
    parser = argparse.ArgumentParser(description="Fix AU zone boundary RPC parameter")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    sb = get_supabase()
    mode = "(DRY RUN)" if args.dry_run else ""
    print(f"\n=== Fix AU Zone Boundaries {mode} ===\n")

    # 1. Get Australia country ID
    au = sb.table("countries").select("id").eq("iso_code", "AU").single().execute().data
    au_id = au["id"]

    # 2. Load existing AU zone appellations
    zone_apps = (
        sb.table("appellations")
        .select("id, name, classification_level")
        .eq("country_id", au_id)
        .eq("classification_level", "zone")
        .execute()
        .data
    )
    app_by_name = {a["name"]: a for a in zone_apps}
    print(f"Found {len(zone_apps)} AU zone appellations in DB")

    # 3. Check which already have boundaries
    zone_ids = [a["id"] for a in zone_apps]
    existing_bounds = (
        sb.table("geographic_boundaries")
        .select("appellation_id, boundary")
        .in_("appellation_id", zone_ids)
        .execute()
        .data
    )
    has_polygon = {gb["appellation_id"] for gb in (existing_bounds or []) if gb.get("boundary")}
    print(f"Zones already with boundary polygon: {len(has_polygon)}")

    # 4. Load zone GeoJSON
    geo_path = PROJECT_ROOT / "data" / "geo" / "wine_australia_zones.geojson"
    zones_geo = json.loads(geo_path.read_text(encoding="utf-8"))
    print(f"GeoJSON features: {len(zones_geo['features'])}\n")

    # 5. Match and import
    imported = 0
    skipped_already = 0
    skipped_no_match = 0

    for feature in zones_geo["features"]:
        gi_name = feature["properties"]["GI_NAME"]
        geom = feature["geometry"]

        app = app_by_name.get(gi_name)
        if not app:
            print(f"  SKIP (no DB match): {gi_name}")
            skipped_no_match += 1
            continue

        if app["id"] in has_polygon:
            print(f"  SKIP (already has polygon): {gi_name}")
            skipped_already += 1
            continue

        # Simplify: round precision, then Douglas-Peucker if still > 1MB
        simplified = simplify_precision(geom)
        geojson_str = json.dumps(simplified)
        orig_size_kb = len(geojson_str) / 1024

        if len(geojson_str) > 1_000_000:
            simplified = simplify_geometry(simplified, 0.005)
            geojson_str = json.dumps(simplified)
            new_kb = len(geojson_str) / 1024
            print(f"  [SIMPLIFY] {gi_name}: {orig_size_kb:.1f} KB -> {new_kb:.1f} KB")

        size_kb = len(geojson_str) / 1024
        source_id = f"wine-australia-zone/{geo_slugify(gi_name)}"
        dry_tag = "[dry-run] " if args.dry_run else ""
        geom_type = simplified["type"]
        app_id = app["id"]
        print(f"  {dry_tag}Import: {gi_name} ({geom_type}, {size_kb:.1f} KB) -> {app_id}")

        if not args.dry_run:
            try:
                sb.rpc("upsert_appellation_boundary", {
                    "p_appellation_id": app["id"],
                    "p_geojson": geojson_str,
                    "p_source_id": source_id,
                    "p_confidence": "official",
                }).execute()
            except Exception as e:
                print(f"    ERROR: {e}")
                continue
        imported += 1

    print(f"\n--- Summary ---")
    print(f"Imported: {imported}")
    print(f"Skipped (already has polygon): {skipped_already}")
    print(f"Skipped (no DB match): {skipped_no_match}")
    n_features = len(zones_geo["features"])
    print(f"Total features processed: {n_features}")
    print("\nDone!")


if __name__ == "__main__":
    main()
