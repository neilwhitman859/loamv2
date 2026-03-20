"""
Fetch boundary polygons for ALL wine regions & appellations worldwide
from OpenStreetMap Nominatim API, then store in geographic_boundaries.

Uses centroid distance validation to avoid false matches.
Skips entities that already have a polygon.

Usage: python -m pipeline.geo.fetch_global_boundaries [--dry-run] [--limit N] [--country "France"]
"""

import sys
import json
import math
import time
import argparse
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import httpx
from pipeline.lib.db import get_supabase

NOMINATIM_BASE = "https://nominatim.openstreetmap.org/search"
RATE_LIMIT_MS = 1100
MAX_DISTANCE_KM = 100
USER_AGENT = "Loam Wine Platform (neil@loam.wine)"


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    R = 6371
    d_lat = (lat2 - lat1) * math.pi / 180
    d_lon = (lon2 - lon1) * math.pi / 180
    a = (math.sin(d_lat / 2) ** 2 +
         math.cos(lat1 * math.pi / 180) * math.cos(lat2 * math.pi / 180) *
         math.sin(d_lon / 2) ** 2)
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def nominatim_search(client: httpx.Client, query: str) -> list[dict]:
    params = {"q": query, "format": "json", "polygon_geojson": "1", "limit": "1"}
    resp = client.get(NOMINATIM_BASE, params=params, headers={"User-Agent": USER_AGENT})
    resp.raise_for_status()
    return resp.json()


def has_polygon(result: dict | None) -> bool:
    if not result or "geojson" not in result:
        return False
    return result["geojson"].get("type") in ("Polygon", "MultiPolygon")


def build_queries(name: str, region_name: str | None, country_name: str,
                  designation_type: str | None, is_region: bool) -> list[str]:
    queries = []
    if is_region:
        queries.append(f"{name}, {country_name}")
        queries.append(f"{name} wine region, {country_name}")
        if country_name == "United States":
            queries.append(f"{name} County, {country_name}")
    else:
        if region_name:
            queries.append(f"{name}, {region_name}, {country_name}")
        queries.append(f"{name}, {country_name}")
        if country_name == "United States":
            queries.append(f"{name} AVA, {country_name}")
        elif country_name == "France":
            queries.append(f"{name} wine region, France")
        elif country_name == "Italy":
            queries.append(f"{name} wine, Italy")
        elif country_name == "Spain":
            queries.append(f"{name} denominacion, Spain")
        elif country_name == "Germany":
            queries.append(f"{name} Anbaugebiet, Germany")
        queries.append(f"{name} wine region, {country_name}")
    return queries


def main():
    parser = argparse.ArgumentParser(description="Fetch global boundary polygons via Nominatim")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--country", type=str, default=None)
    args = parser.parse_args()

    sb = get_supabase()
    print(f"\n=== Global Boundary Polygon Fetcher ===")
    print(f"Mode: {'DRY RUN' if args.dry_run else 'LIVE'}")
    if args.country:
        print(f"Country filter: {args.country}")

    # Get boundary points and polygons via RPCs
    bp_result = sb.rpc("get_boundary_points").execute()
    rows = bp_result.data or []

    poly_result = sb.rpc("get_boundary_polygons").execute()
    has_poly_set = set(p["entity_name"] for p in (poly_result.data or []))

    # Get all regions
    all_regions = []
    offset = 0
    while True:
        result = sb.table("regions").select("id, name, countries!inner(name)").range(
            offset, offset + 999
        ).execute()
        all_regions.extend(result.data)
        if len(result.data) < 1000:
            break
        offset += 1000

    work = []

    # Add regions without polygons
    for r in all_regions:
        country_name = r.get("countries", {}).get("name")
        if not country_name:
            continue
        if args.country and country_name != args.country:
            continue
        if r["name"] in has_poly_set:
            continue
        bp = next((p for p in rows if p["entity_name"] == r["name"]
                    and p["entity_type"] == "region"), None)
        if not bp:
            continue
        work.append({
            "type": "region", "id": r["id"], "name": r["name"],
            "regionName": None, "countryName": country_name,
            "designationType": None, "lat": bp["lat"], "lng": bp["lng"],
        })

    # Add appellations without polygons
    for p in rows:
        if p["entity_type"] != "appellation":
            continue
        if p["entity_name"] in has_poly_set:
            continue
        if args.country and p.get("country_name") != args.country:
            continue
        work.append({
            "type": "appellation", "boundaryId": p["id"],
            "name": p["entity_name"], "regionName": p.get("region_name"),
            "countryName": p.get("country_name"), "designationType": None,
            "lat": p["lat"], "lng": p["lng"],
        })

    print(f"Found {len(work)} entities without polygons")

    to_process = work[:args.limit] if args.limit else work
    success = 0
    failed = 0
    too_far = 0
    failures = []
    successes = []
    too_far_list = []

    client = httpx.Client(timeout=30)

    for i, item in enumerate(to_process):
        is_region = item["type"] == "region"
        queries = build_queries(item["name"], item.get("regionName"),
                                item["countryName"], item.get("designationType"), is_region)

        print(f"[{i + 1}/{len(to_process)}] {item['countryName']}/{item['name']}...", end="", flush=True)

        polygon = None
        source_id = None

        for q in queries:
            time.sleep(RATE_LIMIT_MS / 1000)
            try:
                results = nominatim_search(client, q)
                if results and has_polygon(results[0]):
                    match_lat = float(results[0]["lat"])
                    match_lng = float(results[0]["lon"])
                    dist = haversine_km(item["lat"], item["lng"], match_lat, match_lng)
                    if dist > MAX_DISTANCE_KM:
                        print(f" REJECT polygon too far ({round(dist)}km, query: \"{q}\")")
                        too_far += 1
                        too_far_list.append(f"{item['name']} ({round(dist)}km)")
                        continue
                    polygon = results[0]["geojson"]
                    source_id = f"osm/{results[0]['osm_type']}/{results[0]['osm_id']}"
                    print(f" ok {polygon['type']} ({round(dist)}km, query: \"{q}\")")
                    break
            except Exception as err:
                print(f" error: {err}")

        if not polygon:
            if item["name"] not in [t.split(" (")[0] for t in too_far_list]:
                print(" FAIL no polygon found")
            failed += 1
            failures.append(item["name"])
            continue

        if args.dry_run:
            success += 1
            successes.append({"name": item["name"], "country": item["countryName"],
                              "type": polygon["type"]})
            continue

        geojson_str = json.dumps(polygon)
        if is_region:
            rpc_result = sb.rpc("insert_boundary_polygon", {
                "p_region_id": item["id"], "p_geojson": geojson_str, "p_source_id": source_id,
            }).execute()
        else:
            rpc_result = sb.rpc("update_boundary_polygon", {
                "p_boundary_id": item["boundaryId"], "p_geojson": geojson_str,
                "p_source_id": source_id,
            }).execute()

        success += 1
        successes.append({"name": item["name"], "country": item["countryName"],
                          "type": polygon["type"]})

    client.close()

    print(f"\n=== Summary ===")
    print(f"Success: {success}  Failed: {failed}  Too far: {too_far}")
    if failures:
        print(f"\nFailed ({len(failures)}):")
        for f in failures:
            print(f"  - {f}")
    if too_far_list:
        print(f"\nRejected (too far from centroid):")
        for f in too_far_list:
            print(f"  - {f}")
    if successes:
        label = "Would store" if args.dry_run else "Stored"
        print(f"\n{label} ({len(successes)}):")
        by_country = {}
        for s in successes:
            by_country[s["country"]] = by_country.get(s["country"], 0) + 1
        for c, n in sorted(by_country.items(), key=lambda x: -x[1]):
            print(f"  {c}: {n}")


if __name__ == "__main__":
    main()
