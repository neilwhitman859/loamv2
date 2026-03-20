"""
Imports Australian GI wine regions and subregions from official Wine Australia GeoJSON.

Usage:
  python -m pipeline.geo.import_australia_gi [--dry-run] [--boundaries-only]
"""

import sys
import json
import time
import argparse
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from pipeline.lib.db import get_supabase
from pipeline.geo.helpers import (
    compute_centroid, simplify_precision, simplify_geometry, geo_slugify,
    fetch_all_paginated,
)

PROJECT_ROOT = Path(__file__).resolve().parents[2]

STATE_TO_REGION = {
    "SA": "South Australia", "VIC": "Victoria", "NSW": "New South Wales",
    "WA": "Western Australia", "TAS": "Tasmania", "QLD": None, "ACT": None,
}

NAME_MAP = {"Mclaren Vale": "McLaren Vale"}


def main():
    parser = argparse.ArgumentParser(description="Import Australian GI boundaries")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--boundaries-only", action="store_true")
    args = parser.parse_args()

    sb = get_supabase()
    print("=== Wine Australia GI Import ===")
    if args.dry_run:
        print("[DRY RUN MODE]")

    # Phase 0: Load GeoJSON
    print("\n--- Phase 0: Loading data ---")
    regions_gj = json.loads((PROJECT_ROOT / "data/geo/wine_australia_regions.geojson").read_text())
    subregions_gj = json.loads((PROJECT_ROOT / "data/geo/wine_australia_subregions.geojson").read_text())
    print(f"  Regions: {len(regions_gj['features'])} features")
    print(f"  Subregions: {len(subregions_gj['features'])} features")

    all_gis = []
    for f in regions_gj["features"]:
        name = NAME_MAP.get(f["properties"]["GI_NAME"], f["properties"]["GI_NAME"])
        all_gis.append({
            "name": name, "gi_number": f["properties"]["GI_NUMBER"],
            "gi_type": f["properties"].get("GI_TYPE", "region"),
            "state": f["properties"].get("STATE"),
            "gi_url": f["properties"].get("GI_URL"),
            "year_registered": f["properties"].get("YEAR_REGISTERED") or f["properties"].get("YEAR_REGISTER"),
            "geometry": f["geometry"],
        })
    for f in subregions_gj["features"]:
        name = NAME_MAP.get(f["properties"]["GI_NAME"], f["properties"]["GI_NAME"])
        all_gis.append({
            "name": name, "gi_number": f["properties"]["GI_NUMBER"],
            "gi_type": f["properties"].get("GI_TYPE", "subregion"),
            "state": f["properties"].get("STATE"),
            "gi_url": f["properties"].get("GI_URL"),
            "year_registered": f["properties"].get("YEAR_REGISTERED") or f["properties"].get("YEAR_REGISTER"),
            "geometry": f["geometry"],
        })
    print(f"  Total GIs to process: {len(all_gis)}")

    # Load DB reference data
    australia = sb.table("countries").select("id").eq("iso_code", "AU").single().execute().data
    au_id = australia["id"]
    au_regions = sb.table("regions").select("id, name, is_catch_all").eq("country_id", au_id).execute().data
    region_by_name = {r["name"].lower(): r for r in au_regions}
    catch_all = next((r for r in au_regions if r["is_catch_all"]), None)

    existing_apps = fetch_all_paginated(sb, "appellations", "id, name, slug", {"country_id": au_id})
    app_by_name = {a["name"].lower(): a for a in existing_apps}
    app_by_slug = {a["slug"]: a for a in existing_apps}
    print(f"  {len(existing_apps)} existing Australian appellations")

    stats = {"matched": 0, "created": 0, "boundaries": 0, "errors": 0}
    gi_to_app = {}

    # Phase 1
    if not args.boundaries_only:
        print("\n--- Phase 1: Matching appellations ---")
        for gi in all_gis:
            app = app_by_name.get(gi["name"].lower()) or app_by_slug.get(geo_slugify(gi["name"]))
            if app:
                gi_to_app[gi["gi_number"]] = app["id"]
                stats["matched"] += 1
                continue

            state_region = STATE_TO_REGION.get(gi["state"])
            region_id = catch_all["id"] if catch_all else None
            if state_region:
                r = region_by_name.get(state_region.lower())
                if r:
                    region_id = r["id"]

            centroid = compute_centroid(gi["geometry"]) if gi["geometry"] else None
            new_app = {
                "name": gi["name"], "slug": geo_slugify(gi["name"]),
                "country_id": au_id, "region_id": region_id, "designation_type": "GI",
                "latitude": round(centroid["lat"], 5) if centroid else None,
                "longitude": round(centroid["lng"], 5) if centroid else None,
                "hemisphere": "south", "growing_season_start_month": 10,
                "growing_season_end_month": 4,
                "established_year": gi.get("year_registered"),
                "regulatory_url": gi.get("gi_url"),
            }

            if args.dry_run:
                print(f"  [DRY] Would create: {gi['name']}")
                stats["created"] += 1
                continue

            try:
                result = sb.table("appellations").insert(new_app).select("id").single().execute()
                gi_to_app[gi["gi_number"]] = result.data["id"]
                app_by_name[gi["name"].lower()] = {"id": result.data["id"], **new_app}
                stats["created"] += 1
            except Exception as e:
                print(f"  [ERROR] {gi['name']}: {e}")
                stats["errors"] += 1

        print(f"\n  Phase 1: {stats['matched']} matched, {stats['created']} created, {stats['errors']} errors")

    # Phase 2: Boundaries
    print("\n--- Phase 2: Importing boundaries ---")
    for gi in all_gis:
        app_id = gi_to_app.get(gi["gi_number"])
        if not app_id:
            app = app_by_name.get(gi["name"].lower()) or app_by_slug.get(geo_slugify(gi["name"]))
            if app:
                app_id = app["id"]
        if not app_id or not gi["geometry"]:
            continue

        try:
            simplified = simplify_precision(gi["geometry"])
            geo_str = json.dumps(simplified)
            if len(geo_str) > 1_000_000:
                simplified = simplify_geometry(simplified, 0.005)
                simplified = simplify_precision(simplified)

            if args.dry_run:
                print(f"  [DRY] {gi['name']} ({simplified['type']})")
                stats["boundaries"] += 1
                continue

            sb.rpc("upsert_appellation_boundary", {
                "p_appellation_id": app_id,
                "p_geojson": json.dumps(simplified),
                "p_source_id": f"wine-australia/{gi['gi_number']}",
                "p_confidence": "official",
            }).execute()
            stats["boundaries"] += 1
        except Exception as e:
            print(f"  [ERROR] {gi['name']}: {e}")
            stats["errors"] += 1

    print(f"\n=== Complete: {stats['matched']} matched, {stats['created']} created, "
          f"{stats['boundaries']} boundaries, {stats['errors']} errors ===")


if __name__ == "__main__":
    main()
