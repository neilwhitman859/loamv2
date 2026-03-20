"""
Import high-accuracy boundary polygons from authoritative sources:
1. UC Davis AVA Project -- official US AVA boundaries (CC0 license)
2. Natural Earth -- all country boundaries (public domain)

Usage:
  python -m pipeline.geo.import_accurate_boundaries [--dry-run] [--countries-only] [--avas-only]
"""

import sys
import json
import argparse
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from pipeline.lib.db import get_supabase

PROJECT_ROOT = Path(__file__).resolve().parents[2]

AVA_NAME_MAP = {
    "Mount Veeder": "Mt. Veeder",
    "Moon Mountain District": "Moon Mountain District Sonoma County",
    "San Luis Obispo": "San Luis Obispo Coast",
    "San Benito County": "San Benito",
    "San Luis Obispo County": "San Luis Obispo Coast",
    "Contra Costa County": "Contra Costa",
    "Mendocino County": "Mendocino",
}

COUNTRY_NAME_MAP = {
    "United States": "United States of America",
    "Czech Republic": "Czechia",
    "South Korea": "South Korea",
}


def main():
    parser = argparse.ArgumentParser(description="Import accurate boundary polygons")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--countries-only", action="store_true")
    parser.add_argument("--avas-only", action="store_true")
    args = parser.parse_args()

    sb = get_supabase()
    print(f"\n=== Accurate Boundary Importer ===")
    print(f"Mode: {'DRY RUN' if args.dry_run else 'LIVE'}")

    # Part 1: US AVA Boundaries
    if not args.countries_only:
        print(f"\n--- US AVA Boundaries (UC Davis) ---")
        ava_path = PROJECT_ROOT / "avas_ucdavis.geojson"
        ava_data = None
        try:
            ava_data = json.loads(ava_path.read_text(encoding="utf-8"))
        except FileNotFoundError:
            print("ERROR: avas_ucdavis.geojson not found.")
            if not args.avas_only:
                print("Skipping AVAs, continuing to countries...")

        if ava_data:
            ava_lookup = {}
            for f in ava_data["features"]:
                ava_lookup[f["properties"]["name"].lower()] = f
                aka = f["properties"].get("aka")
                if aka:
                    for a in aka.split("|"):
                        ava_lookup[a.strip().lower()] = f
            print(f"Loaded {len(ava_data['features'])} AVA boundaries")

            # Get US appellations
            us_apps = []
            offset = 0
            while True:
                result = sb.table("appellations").select(
                    "id, name, regions!inner(countries!inner(name))"
                ).eq("regions.countries.name", "United States").range(
                    offset, offset + 999
                ).execute()
                us_apps.extend(result.data)
                if len(result.data) < 1000:
                    break
                offset += 1000

            app_ids = [a["id"] for a in us_apps]
            boundaries = []
            for i in range(0, len(app_ids), 100):
                chunk = app_ids[i:i + 100]
                result = sb.table("geographic_boundaries").select(
                    "id, appellation_id"
                ).in_("appellation_id", chunk).execute()
                boundaries.extend(result.data)
            boundary_by_app = {b["appellation_id"]: b["id"] for b in boundaries}

            ava_success = 0
            ava_failed = 0
            ava_skipped = 0
            ava_failures = []

            for app in us_apps:
                boundary_id = boundary_by_app.get(app["id"])
                if not boundary_id:
                    ava_skipped += 1
                    continue

                ava_feature = ava_lookup.get(app["name"].lower())
                if not ava_feature and app["name"] in AVA_NAME_MAP:
                    ava_feature = ava_lookup.get(AVA_NAME_MAP[app["name"]].lower())

                if not ava_feature:
                    ava_failed += 1
                    ava_failures.append(app["name"])
                    continue

                geojson = ava_feature["geometry"]
                if not geojson or geojson.get("type") not in ("Polygon", "MultiPolygon"):
                    ava_failed += 1
                    ava_failures.append(f"{app['name']} (not a polygon: {geojson.get('type') if geojson else 'None'})")
                    continue

                print(f"  {app['name']} -> {ava_feature['properties']['name']}...", end="", flush=True)

                if args.dry_run:
                    print(f" ok ({geojson['type']})")
                    ava_success += 1
                    continue

                geojson_str = json.dumps(geojson)
                sb.rpc("update_boundary_polygon", {
                    "p_boundary_id": boundary_id,
                    "p_geojson": geojson_str,
                    "p_source_id": f"ucdavis-ava/{ava_feature['properties'].get('ava_id', '')}",
                }).execute()
                print(" ok")
                ava_success += 1

            print(f"\nAVA Results: {ava_success} success, {ava_failed} failed, {ava_skipped} skipped")
            if ava_failures:
                print(f"Failed: {', '.join(ava_failures)}")

    # Part 2: Country Boundaries
    if not args.avas_only:
        print(f"\n--- Country Boundaries (Natural Earth) ---")
        country_path = PROJECT_ROOT / "countries_naturalearth.geojson"
        try:
            country_data = json.loads(country_path.read_text(encoding="utf-8"))
        except FileNotFoundError:
            print("ERROR: countries_naturalearth.geojson not found.")
            return

        country_lookup = {}
        for f in country_data["features"]:
            country_lookup[f["properties"]["name"].lower()] = f
            iso = f["properties"].get("ISO3166-1-Alpha-2")
            if iso:
                country_lookup[iso.lower()] = f
        print(f"Loaded {len(country_data['features'])} country boundaries")

        countries_result = sb.table("countries").select("id, name, iso_code").execute()
        countries = countries_result.data or []

        country_success = 0
        country_failed = 0

        for c in countries:
            feature = None
            if c.get("iso_code"):
                feature = country_lookup.get(c["iso_code"].lower())
            if not feature:
                feature = country_lookup.get(c["name"].lower())
            if not feature and c["name"] in COUNTRY_NAME_MAP:
                feature = country_lookup.get(COUNTRY_NAME_MAP[c["name"]].lower())

            if not feature:
                country_failed += 1
                print(f"  {c['name']} FAIL not found in Natural Earth")
                continue

            geojson = feature.get("geometry")
            if not geojson:
                country_failed += 1
                print(f"  {c['name']} FAIL no geometry")
                continue

            print(f"  {c['name']} ({c.get('iso_code', '??')}) -> {feature['properties']['name']}...", end="", flush=True)

            if args.dry_run:
                print(f" ok {geojson['type']}")
                country_success += 1
                continue

            sb.rpc("insert_country_boundary", {
                "p_country_id": c["id"],
                "p_geojson": json.dumps(geojson),
                "p_source_id": "natural-earth",
            }).execute()
            print(" ok")
            country_success += 1

        print(f"\nCountry Results: {country_success} success, {country_failed} failed")

    print(f"\n=== Done ===")


if __name__ == "__main__":
    main()
