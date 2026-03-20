"""
Imports all 276 US AVAs from the UC Davis GeoJSON into the Loam database.
Three phases:
  Phase 0: Create missing state-level regions + fetch Nominatim polygons
  Phase 1: Create missing appellation records
  Phase 2: Import AVA boundary polygons from UC Davis GeoJSON

Usage:
  python -m pipeline.geo.import_us_avas [--dry-run] [--skip-regions] [--skip-boundaries]
"""

import sys
import json
import time
import argparse
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import httpx
from pipeline.lib.db import get_supabase
from pipeline.lib.normalize import slugify

PROJECT_ROOT = Path(__file__).resolve().parents[2]
USER_AGENT = "LoamWineApp/1.0 (contact@loam.wine)"

STATE_NAMES = {
    "AL": "Alabama", "AK": "Alaska", "AZ": "Arizona", "AR": "Arkansas",
    "CA": "California", "CO": "Colorado", "CT": "Connecticut", "DE": "Delaware",
    "FL": "Florida", "GA": "Georgia", "HI": "Hawaii", "ID": "Idaho",
    "IL": "Illinois", "IN": "Indiana", "IA": "Iowa", "KS": "Kansas",
    "KY": "Kentucky", "LA": "Louisiana", "ME": "Maine", "MD": "Maryland",
    "MA": "Massachusetts", "MI": "Michigan", "MN": "Minnesota", "MS": "Mississippi",
    "MO": "Missouri", "MT": "Montana", "NE": "Nebraska", "NV": "Nevada",
    "NH": "New Hampshire", "NJ": "New Jersey", "NM": "New Mexico", "NY": "New York",
    "NC": "North Carolina", "ND": "North Dakota", "OH": "Ohio", "OK": "Oklahoma",
    "OR": "Oregon", "PA": "Pennsylvania", "RI": "Rhode Island", "SC": "South Carolina",
    "SD": "South Dakota", "TN": "Tennessee", "TX": "Texas", "UT": "Utah",
    "VT": "Vermont", "VA": "Virginia", "WA": "Washington", "WV": "West Virginia",
    "WI": "Wisconsin", "WY": "Wyoming",
}

DB_TO_GEOJSON = {
    "Mount Veeder": "Mt. Veeder",
    "Moon Mountain District": "Moon Mountain District Sonoma County",
    "San Luis Obispo": "San Luis Obispo Coast",
    "San Benito County": "San Benito",
    "Contra Costa County": "Contra Costa",
    "Mendocino County": "Mendocino",
    "San Luis Obispo County": "San Luis Obispo Coast",
}


def compute_centroid(geojson: dict) -> dict | None:
    total_lat = 0.0
    total_lng = 0.0
    count = 0

    def extract(coords):
        nonlocal total_lat, total_lng, count
        if isinstance(coords[0], (int, float)):
            total_lng += coords[0]
            total_lat += coords[1]
            count += 1
        else:
            for c in coords:
                extract(c)

    if geojson.get("type") == "GeometryCollection":
        for g in geojson.get("geometries", []):
            extract(g["coordinates"])
    else:
        extract(geojson["coordinates"])
    return {"lat": total_lat / count, "lng": total_lng / count} if count > 0 else None


def primary_state_code(state_field: str | None) -> str | None:
    if not state_field:
        return None
    for code, name in STATE_NAMES.items():
        if state_field.lower() == name.lower():
            return code
    first = state_field.split("|")[0].split(",")[0].strip().replace(" ", "")
    if len(first) == 2 and first.upper() in STATE_NAMES:
        return first.upper()
    return None


def nominatim_search(client: httpx.Client, query: str, retries: int = 3) -> list[dict]:
    for attempt in range(retries):
        resp = client.get(
            "https://nominatim.openstreetmap.org/search",
            params={"q": query, "format": "json", "polygon_geojson": "1", "limit": "1"},
            headers={"User-Agent": USER_AGENT},
        )
        if resp.status_code == 429:
            wait = 5 * (attempt + 1)
            print(f"[rate-limited {wait}s] ", end="", flush=True)
            time.sleep(wait)
            continue
        resp.raise_for_status()
        return resp.json()
    raise Exception("Rate limited after retries")


def main():
    parser = argparse.ArgumentParser(description="Import US AVAs from UC Davis GeoJSON")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--skip-regions", action="store_true")
    parser.add_argument("--skip-boundaries", action="store_true")
    args = parser.parse_args()

    sb = get_supabase()
    http = httpx.Client(timeout=30)
    dry = args.dry_run

    print(f"\n=== US AVA Importer ===")
    print(f"Mode: {'DRY RUN' if dry else 'LIVE'}\n")

    geo_path = PROJECT_ROOT / "avas_ucdavis.geojson"
    geo_data = json.loads(geo_path.read_text(encoding="utf-8"))
    print(f"Loaded {len(geo_data['features'])} AVA features from GeoJSON")

    us_country = sb.table("countries").select("id").eq("name", "United States").single().execute()
    us_country_id = us_country.data["id"]
    print(f"US country ID: {us_country_id}")

    states_with_avas = set()
    for f in geo_data["features"]:
        code = primary_state_code(f["properties"].get("state"))
        if code:
            states_with_avas.add(code)
    print(f"States with AVAs: {', '.join(sorted(states_with_avas))} ({len(states_with_avas)} states)")

    # Phase 0: State regions
    if not args.skip_regions:
        print(f"\n--- Phase 0: State-Level Regions ---")
        existing_regions = sb.table("regions").select("id, name, parent_id").eq(
            "country_id", us_country_id
        ).execute().data or []
        region_by_name = {r["name"].lower(): r for r in existing_regions}

        # Texas special case
        texas_hc = region_by_name.get("texas hill country")
        texas_region = region_by_name.get("texas")

        if not texas_region and "TX" in states_with_avas:
            print("  Creating 'Texas' parent region...")
            if not dry:
                result = sb.table("regions").insert({
                    "name": "Texas", "slug": "texas",
                    "country_id": us_country_id, "is_catch_all": False,
                }).select("id").single().execute()
                texas_region = {"id": result.data["id"], "name": "Texas"}
                region_by_name["texas"] = texas_region
                if texas_hc and not texas_hc.get("parent_id"):
                    sb.table("regions").update({"parent_id": result.data["id"]}).eq(
                        "id", texas_hc["id"]
                    ).execute()
                    print("    Re-parented 'Texas Hill Country' under 'Texas'")

        missing_states = []
        for code in states_with_avas:
            state_name = STATE_NAMES.get(code)
            if not state_name or state_name.lower() in region_by_name:
                continue
            missing_states.append({"code": code, "name": state_name})

        print(f"  Missing state regions: {len(missing_states)}")
        for state in missing_states:
            print(f"  Creating '{state['name']}' ({state['code']})... ", end="", flush=True)
            if not dry:
                result = sb.table("regions").insert({
                    "name": state["name"], "slug": slugify(state["name"]),
                    "country_id": us_country_id, "is_catch_all": False,
                }).select("id").single().execute()
                region_by_name[state["name"].lower()] = {"id": result.data["id"], "name": state["name"]}
                print(f"ok ({result.data['id']})")
            else:
                print("[dry-run]")

        # Fetch Nominatim boundaries
        all_region_ids = [r["id"] for r in region_by_name.values()]
        existing_bounds = []
        for i in range(0, len(all_region_ids), 200):
            chunk = all_region_ids[i:i + 200]
            result = sb.table("geographic_boundaries").select("id, region_id").in_(
                "region_id", chunk
            ).execute()
            existing_bounds.extend(result.data or [])
        regions_with_boundary = {b["region_id"] for b in existing_bounds}

        needs_boundary = []
        for code in states_with_avas:
            state_name = STATE_NAMES.get(code)
            if not state_name:
                continue
            region = region_by_name.get(state_name.lower())
            if not region or region["id"] in regions_with_boundary:
                continue
            needs_boundary.append({"code": code, "name": state_name, "id": region["id"]})

        print(f"  State regions needing boundaries: {len(needs_boundary)}")
        for i, state in enumerate(needs_boundary):
            print(f"  [{i + 1}/{len(needs_boundary)}] {state['name']}... ", end="", flush=True)
            time.sleep(1.1)
            try:
                results = nominatim_search(http, f"{state['name']}, United States")
                if not results:
                    print("no results")
                    continue
                result = results[0]
                geojson = result.get("geojson")
                has_poly = geojson and geojson.get("type") in ("Polygon", "MultiPolygon")
                if not dry:
                    rpc_params = {
                        "p_region_id": state["id"],
                        "p_source_id": f"nominatim/{result.get('osm_type')}/{result.get('osm_id')}",
                        "p_confidence": "approximate" if has_poly else "geocoded",
                    }
                    if has_poly:
                        rpc_params["p_geojson"] = json.dumps(geojson)
                    else:
                        rpc_params["p_lat"] = float(result["lat"])
                        rpc_params["p_lng"] = float(result["lon"])
                    sb.rpc("upsert_region_boundary", rpc_params).execute()
                    print(f"ok {geojson['type'] if has_poly else 'centroid'}")
                else:
                    print(f"[dry-run] {geojson['type'] if has_poly else 'centroid'}")
            except Exception as e:
                print(f"FAIL {e}")

    # Phase 1: Appellation records
    print(f"\n--- Phase 1: Appellation Records ---")
    us_regions = sb.table("regions").select("id, name").eq("country_id", us_country_id).execute().data or []
    region_by_name = {r["name"].lower(): r for r in us_regions}

    state_to_region_id = {}
    for code, name in STATE_NAMES.items():
        region = region_by_name.get(name.lower())
        if region:
            state_to_region_id[code] = region["id"]
    print(f"  State->Region mappings: {len(state_to_region_id)}")

    existing_apps = []
    offset = 0
    while True:
        result = sb.table("appellations").select("id, name").eq(
            "country_id", us_country_id
        ).range(offset, offset + 999).execute()
        existing_apps.extend(result.data)
        if len(result.data) < 1000:
            break
        offset += 1000

    app_by_name = {a["name"].lower(): a for a in existing_apps}
    print(f"  Existing US appellations: {len(app_by_name)}")

    geo_lookup = {}
    for f in geo_data["features"]:
        geo_lookup[f["properties"]["name"].lower()] = f
        aka = f["properties"].get("aka")
        if aka:
            for a in aka.split("|"):
                geo_lookup[a.strip().lower()] = f

    matched_geo_names = set()
    existing_matched = 0
    for name, app in app_by_name.items():
        geo_feature = geo_lookup.get(name)
        if not geo_feature and app["name"] in DB_TO_GEOJSON:
            geo_feature = geo_lookup.get(DB_TO_GEOJSON[app["name"]].lower())
        if geo_feature:
            matched_geo_names.add(geo_feature["properties"]["name"].lower())
            existing_matched += 1
    print(f"  Existing appellations matched to GeoJSON: {existing_matched}")

    to_create = []
    for f in geo_data["features"]:
        geo_name = f["properties"]["name"]
        if geo_name.lower() in matched_geo_names or geo_name.lower() in app_by_name:
            continue
        state_code = primary_state_code(f["properties"].get("state"))
        region_id = state_to_region_id.get(state_code) if state_code else None
        if not region_id:
            us_region = region_by_name.get("united states")
            if us_region:
                to_create.append({"feature": f, "region_id": us_region["id"]})
            continue
        to_create.append({"feature": f, "region_id": region_id})

    print(f"  AVAs to create: {len(to_create)}")
    created = 0
    create_errors = 0

    for item in to_create:
        feature = item["feature"]
        name = feature["properties"]["name"]
        centroid = compute_centroid(feature["geometry"])
        print(f"  + {name}... ", end="", flush=True)

        if not dry:
            try:
                result = sb.table("appellations").insert({
                    "name": name, "slug": slugify(name),
                    "designation_type": "AVA", "country_id": us_country_id,
                    "region_id": item["region_id"], "hemisphere": "north",
                    "latitude": round(centroid["lat"], 3) if centroid else None,
                    "longitude": round(centroid["lng"], 3) if centroid else None,
                    "growing_season_start_month": 3, "growing_season_end_month": 10,
                }).select("id").single().execute()
                app_by_name[name.lower()] = {"id": result.data["id"], "name": name}
                print("ok")
                created += 1
            except Exception as e:
                print(f"FAIL {e}")
                create_errors += 1
        else:
            print("[dry-run]")
            created += 1

    print(f"\n  Phase 1 Results: {created} created, {create_errors} errors, {existing_matched} already existed")

    # Phase 2: Boundary polygons
    if not args.skip_boundaries:
        print(f"\n--- Phase 2: AVA Boundary Polygons ---")

        all_apps = []
        offset = 0
        while True:
            result = sb.table("appellations").select("id, name").eq(
                "country_id", us_country_id
            ).range(offset, offset + 999).execute()
            all_apps.extend(result.data)
            if len(result.data) < 1000:
                break
            offset += 1000
        app_lookup = {a["name"].lower(): a for a in all_apps}

        app_ids = [a["id"] for a in all_apps]
        existing_bounds = []
        for i in range(0, len(app_ids), 200):
            chunk = app_ids[i:i + 200]
            result = sb.table("geographic_boundaries").select("id, appellation_id").in_(
                "appellation_id", chunk
            ).execute()
            existing_bounds.extend(result.data or [])
        boundary_by_app = {b["appellation_id"]: b["id"] for b in existing_bounds}
        print(f"  Existing boundary rows: {len(boundary_by_app)}")

        poly_success = 0
        poly_failed = 0
        poly_skipped = 0

        for feature in geo_data["features"]:
            geo_name = feature["properties"]["name"]
            geojson = feature.get("geometry")
            if not geojson or geojson.get("type") not in ("Polygon", "MultiPolygon"):
                poly_skipped += 1
                continue

            app = app_lookup.get(geo_name.lower())
            if not app and feature["properties"].get("aka"):
                for aka in feature["properties"]["aka"].split("|"):
                    app = app_lookup.get(aka.strip().lower())
                    if app:
                        break
            if not app:
                for db_name, geo_mapped in DB_TO_GEOJSON.items():
                    if geo_mapped.lower() == geo_name.lower():
                        app = app_lookup.get(db_name.lower())
                        if app:
                            break

            if not app:
                print(f"  WARNING No DB match for '{geo_name}'")
                poly_failed += 1
                continue

            boundary_id = boundary_by_app.get(app["id"])
            print(f"  {app['name']}... ", end="", flush=True)

            if dry:
                print(f"[dry-run] {'update' if boundary_id else 'create'} {geojson['type']}")
                poly_success += 1
                continue

            try:
                sb.rpc("upsert_appellation_boundary", {
                    "p_appellation_id": app["id"],
                    "p_geojson": json.dumps(geojson),
                    "p_source_id": f"ucdavis-ava/{feature['properties'].get('ava_id', '')}",
                    "p_confidence": "official",
                }).execute()
                print(f"ok {'updated' if boundary_id else 'created'}")
                poly_success += 1
            except Exception as e:
                print(f"FAIL {e}")
                poly_failed += 1

        print(f"\n  Phase 2 Results: {poly_success} polygons set, {poly_failed} failed, {poly_skipped} skipped")

    print(f"\n========================================")
    print(f"   US AVA IMPORT COMPLETE")
    print(f"========================================")

    http.close()


if __name__ == "__main__":
    main()
