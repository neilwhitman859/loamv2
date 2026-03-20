"""
Imports appellations for multiple countries from data/appellations_global.json.
Geocodes via Nominatim (centroids + optional boundary polygons).

Usage:
    python -m pipeline.geo.import_global_appellations [--dry-run] [--country=ZA]
"""

import sys
import json
import time
import argparse
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from pipeline.lib.db import get_supabase
from pipeline.geo.helpers import (
    nominatim_search, geo_slugify, simplify_geometry, simplify_precision,
    has_polygon, RATE_LIMIT_S, fetch_all_paginated,
)
import httpx

TARGET_SIZE = 250_000

def main():
    parser = argparse.ArgumentParser(description="Import global appellations from JSON")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--country", type=str, default=None)
    args = parser.parse_args()

    sb = get_supabase()
    project_root = Path(__file__).resolve().parents[2]

    print("=== Global Appellation Import ===")
    if args.dry_run:
        print("[DRY RUN MODE]")
    if args.country:
        print(f"[FILTERING TO: {args.country}]")

    all_data = json.loads((project_root / "data" / "appellations_global.json").read_text())

    print("\n--- Loading DB reference data ---")
    countries = sb.table("countries").select("id, name, iso_code").execute().data
    country_by_code = {c["iso_code"]: c for c in countries}

    all_regions = fetch_all_paginated(sb, "regions", "id, name, is_catch_all, country_id")
    region_index = {}
    catch_all_index = {}
    for r in all_regions:
        region_index.setdefault(r["country_id"], {})[r["name"].lower()] = r
        if r["is_catch_all"]:
            catch_all_index[r["country_id"]] = r

    existing_apps = fetch_all_paginated(sb, "appellations", "id, name, slug, country_id")
    app_index = {}
    slug_index = {}
    for a in existing_apps:
        app_index[f"{a['country_id']}::{a['name'].lower()}"] = a
        slug_index[f"{a['country_id']}::{a['slug']}"] = a

    print(f"  {len(countries)} countries, {len(all_regions)} regions, {len(existing_apps)} existing appellations")

    stats = {"matched": 0, "created": 0, "boundaries": 0, "geocoded": 0, "errors": 0}
    iso_keys = [args.country] if args.country else list(all_data.keys())

    client = httpx.Client(timeout=30.0)
    try:
        for iso in iso_keys:
            country_data = all_data.get(iso)
            if not country_data:
                print(f"\n[SKIP] No data for {iso}")
                continue
            country = country_by_code.get(iso)
            if not country:
                print(f"\n[SKIP] Country {iso} not in DB")
                continue

            regions = region_index.get(country["id"], {})
            catch_all = catch_all_index.get(country["id"])

            apps = country_data.get("appellations", [])
            print(f"\n=== {country['name']} ({iso}) -- {len(apps)} appellations ===")

            for app_def in apps:
                name_key = f"{country['id']}::{app_def['name'].lower()}"
                slug_key = f"{country['id']}::{geo_slugify(app_def['name'])}"
                existing = app_index.get(name_key) or slug_index.get(slug_key)

                if existing:
                    stats["matched"] += 1
                    continue

                region_id = catch_all["id"] if catch_all else None
                if app_def.get("region"):
                    reg = regions.get(app_def["region"].lower())
                    if reg:
                        region_id = reg["id"]

                lat, lng, boundary = None, None, None
                if app_def.get("geo"):
                    time.sleep(RATE_LIMIT_S)
                    results = nominatim_search(client, app_def["geo"])
                    if results:
                        r0 = results[0]
                        lat = round(float(r0["lat"]), 5)
                        lng = round(float(r0["lon"]), 5)
                        stats["geocoded"] += 1
                        if has_polygon(r0):
                            boundary = r0["geojson"]

                new_app = {
                    "name": app_def["name"],
                    "slug": geo_slugify(app_def["name"]),
                    "country_id": country["id"],
                    "region_id": region_id,
                    "designation_type": app_def.get("type"),
                    "latitude": lat,
                    "longitude": lng,
                    "hemisphere": country_data.get("hemisphere"),
                    "growing_season_start_month": country_data.get("growing_start"),
                    "growing_season_end_month": country_data.get("growing_end"),
                }

                if args.dry_run:
                    b_str = f" +boundary({boundary['type']})" if boundary else ""
                    print(f"  [DRY] Would create: {app_def['name']} ({lat}, {lng}){b_str}")
                    stats["created"] += 1
                    continue

                try:
                    result = sb.table("appellations").insert(new_app).execute()
                    inserted_id = result.data[0]["id"]
                    if boundary:
                        _import_boundary(sb, inserted_id, app_def["name"], boundary, iso, stats)
                    stats["created"] += 1
                except Exception as e:
                    print(f"  [ERROR] {app_def['name']}: {e}")
                    stats["errors"] += 1
    finally:
        client.close()

    print("\n=== Import Complete ===")
    for k, v in stats.items():
        print(f"  {k}: {v}")
    if args.dry_run:
        print("\n  [DRY RUN - no changes made]")


def _import_boundary(sb, app_id, name, boundary, iso, stats):
    try:
        geo_str = json.dumps(boundary)
        simplified = boundary
        for tol in [0.001, 0.002, 0.005, 0.01]:
            if len(geo_str) <= TARGET_SIZE:
                break
            simplified = simplify_geometry(simplified, tol)
            geo_str = json.dumps(simplified)
        if len(geo_str) > TARGET_SIZE * 2:
            return
        sb.rpc("upsert_appellation_boundary", {
            "p_appellation_id": app_id,
            "p_geojson": geo_str,
            "p_source_id": f"nominatim/{iso}/{geo_slugify(name)}",
            "p_confidence": "geocoded",
        }).execute()
        stats["boundaries"] += 1
    except Exception:
        pass


if __name__ == "__main__":
    main()