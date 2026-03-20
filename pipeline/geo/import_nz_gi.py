"""
Imports New Zealand GI boundaries from IPONZ Esri JSON files.

Usage: python -m pipeline.geo.import_nz_gi [--dry-run] [--boundaries-only]
"""

import sys
import json
import argparse
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from pipeline.lib.db import get_supabase
from pipeline.geo.helpers import (
    compute_centroid, simplify_precision, progressive_simplify,
    geo_slugify, esri_rings_to_geojson, fetch_all_paginated,
)

PROJECT_ROOT = Path(__file__).resolve().parents[2]

GI_TO_REGION = {
    "Auckland": "Auckland",
    "Canterbury": "Canterbury",
    "Central Otago": "Central Otago",
    "Gisborne": "Gisborne",
    "Hawke's Bay": "Hawke's Bay",
    "Marlborough": "Marlborough",
    "Nelson": "Nelson",
    "North Canterbury": "Canterbury",
    "Wairarapa": "Wairarapa",
    "Waikato": None,
    "Northland": None,
}


def main():
    parser = argparse.ArgumentParser(description="Import NZ GI boundaries from IPONZ Esri JSON")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--boundaries-only", action="store_true")
    args = parser.parse_args()

    sb = get_supabase()
    print("=== New Zealand GI Import ===")

    nz = sb.table("countries").select("id").eq("iso_code", "NZ").single().execute().data
    nz_id = nz["id"]

    nz_regions = sb.table("regions").select("id, name, is_catch_all").eq("country_id", nz_id).execute().data
    region_by_name = {r["name"].lower(): r for r in nz_regions}
    catch_all = next((r for r in nz_regions if r["is_catch_all"]), None)

    existing_apps = fetch_all_paginated(sb, "appellations", "id, name, slug", {"country_id": nz_id})
    app_by_name = {a["name"].lower(): a for a in existing_apps}

    # Load Esri JSON files
    gi_dir = PROJECT_ROOT / "data" / "geo" / "nz_gi"
    gi_files = list(gi_dir.glob("*.json")) if gi_dir.exists() else []
    print(f"  Found {len(gi_files)} GI files")

    stats = {"matched": 0, "created": 0, "boundaries": 0, "errors": 0}

    for gi_file in gi_files:
        data = json.loads(gi_file.read_text())
        features = data.get("features", [])
        if not features:
            continue

        for feature in features:
            name = feature.get("attributes", {}).get("NAME") or gi_file.stem.replace("_", " ").title()
            rings = feature.get("geometry", {}).get("rings", [])

            if not rings:
                continue

            geojson = esri_rings_to_geojson(rings)
            simplified = progressive_simplify(geojson)
            centroid = compute_centroid(simplified)

            # Match or create appellation
            app = app_by_name.get(name.lower())
            if app:
                app_id = app["id"]
                stats["matched"] += 1
            elif not args.boundaries_only:
                region_name = GI_TO_REGION.get(name)
                region_id = catch_all["id"] if catch_all else None
                if region_name:
                    r = region_by_name.get(region_name.lower())
                    if r:
                        region_id = r["id"]

                new_app = {
                    "name": name, "slug": geo_slugify(name),
                    "country_id": nz_id, "region_id": region_id, "designation_type": "GI",
                    "latitude": round(centroid["lat"], 5) if centroid else None,
                    "longitude": round(centroid["lng"], 5) if centroid else None,
                    "hemisphere": "south",
                }

                if args.dry_run:
                    print(f"  [DRY] Would create: {name}")
                    stats["created"] += 1
                    continue

                try:
                    result = sb.table("appellations").insert(new_app).select("id").single().execute()
                    app_id = result.data["id"]
                    app_by_name[name.lower()] = {"id": app_id, **new_app}
                    stats["created"] += 1
                except Exception as e:
                    print(f"  [ERROR] {name}: {e}")
                    stats["errors"] += 1
                    continue
            else:
                continue

            # Import boundary
            if args.dry_run:
                print(f"  [DRY] {name} ({simplified['type']})")
                stats["boundaries"] += 1
                continue

            try:
                sb.rpc("upsert_appellation_boundary", {
                    "p_appellation_id": app_id,
                    "p_geojson": json.dumps(simplified),
                    "p_source_id": f"iponz/{gi_file.stem}",
                    "p_confidence": "official",
                }).execute()
                stats["boundaries"] += 1
            except Exception as e:
                print(f"  [ERROR] boundary {name}: {e}")
                stats["errors"] += 1

    print(f"\n=== Complete: {stats['matched']} matched, {stats['created']} created, "
          f"{stats['boundaries']} boundaries, {stats['errors']} errors ===")


if __name__ == "__main__":
    main()
