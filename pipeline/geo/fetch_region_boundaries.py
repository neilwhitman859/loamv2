"""
Geocodes + fetches polygon boundaries for all regions via Nominatim.
Uses upsert_region_boundary RPC to insert/update geographic_boundaries.

Usage:
  python -m pipeline.geo.fetch_region_boundaries              # live run
  python -m pipeline.geo.fetch_region_boundaries --dry-run    # preview only
  python -m pipeline.geo.fetch_region_boundaries --limit 10
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

USER_AGENT = "LoamWineApp/1.0 (contact@loam.wine)"


def haversine(lat1: float, lng1: float, lat2: float, lng2: float) -> float:
    R = 6371
    d_lat = (lat2 - lat1) * math.pi / 180
    d_lng = (lng2 - lng1) * math.pi / 180
    a = (math.sin(d_lat / 2) ** 2 +
         math.cos(lat1 * math.pi / 180) * math.cos(lat2 * math.pi / 180) *
         math.sin(d_lng / 2) ** 2)
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def compute_centroid(geojson: dict) -> dict | None:
    total_lat = 0
    total_lng = 0
    count = 0

    def extract_coords(coords):
        nonlocal total_lat, total_lng, count
        if isinstance(coords[0], (int, float)):
            total_lng += coords[0]
            total_lat += coords[1]
            count += 1
        else:
            for c in coords:
                extract_coords(c)

    if geojson.get("type") == "GeometryCollection":
        for g in geojson.get("geometries", []):
            extract_coords(g["coordinates"])
    else:
        extract_coords(geojson["coordinates"])

    return {"lat": total_lat / count, "lng": total_lng / count} if count > 0 else None


def nominatim_search(client: httpx.Client, query: str, retries: int = 3) -> list[dict]:
    for attempt in range(retries):
        resp = client.get(
            "https://nominatim.openstreetmap.org/search",
            params={"q": query, "format": "json", "polygon_geojson": "1", "limit": "1"},
            headers={"User-Agent": USER_AGENT},
        )
        if resp.status_code == 429:
            wait = 5 * (attempt + 1)
            print(f"[rate-limited, waiting {wait}s] ", end="", flush=True)
            time.sleep(wait)
            continue
        resp.raise_for_status()
        return resp.json()
    raise Exception("Rate limited after retries")


def main():
    parser = argparse.ArgumentParser(description="Fetch region boundary polygons via Nominatim")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--limit", type=int, default=None)
    args = parser.parse_args()

    sb = get_supabase()
    print(f"\n=== Region Boundary Fetcher ===")
    print(f"Mode: {'DRY RUN' if args.dry_run else 'LIVE'}")

    # Fetch all regions with country name
    all_regions = []
    offset = 0
    while True:
        result = sb.table("regions").select(
            "id, name, country:countries(id, name)"
        ).order("name").range(offset, offset + 999).execute()
        all_regions.extend(result.data)
        if len(result.data) < 1000:
            break
        offset += 1000

    # Get existing boundary rows for regions
    existing = []
    offset = 0
    while True:
        result = sb.table("geographic_boundaries").select("id, region_id").not_(
            "region_id", "is", "null"
        ).range(offset, offset + 999).execute()
        existing.extend(result.data)
        if len(result.data) < 1000:
            break
        offset += 1000
    existing_map = {e["region_id"]: e["id"] for e in existing}

    # Get average appellation centroids per region for validation
    centroid_map = {}
    bp_result = sb.rpc("get_boundary_points").execute()
    bp_data = bp_result.data or []
    if bp_data:
        region_points = {}
        for pt in bp_data:
            if pt.get("entity_type") != "appellation" or not pt.get("region_name"):
                continue
            if not pt.get("lat") or not pt.get("lng"):
                continue
            key = f"{pt['region_name']}|{pt['country_name']}"
            if key not in region_points:
                region_points[key] = {"sum_lat": 0, "sum_lng": 0, "count": 0}
            region_points[key]["sum_lat"] += pt["lat"]
            region_points[key]["sum_lng"] += pt["lng"]
            region_points[key]["count"] += 1

        for r in all_regions:
            country_name = r.get("country", {}).get("name", "")
            key = f"{r['name']}|{country_name}"
            if key in region_points:
                p = region_points[key]
                centroid_map[r["id"]] = {
                    "lat": p["sum_lat"] / p["count"],
                    "lng": p["sum_lng"] / p["count"],
                }
        print(f"Computed expected centroids for {len(centroid_map)} regions from appellation data")

    # Filter to non-catch-all regions without existing boundaries
    todo = [r for r in all_regions if r["name"] != r.get("country", {}).get("name")]
    to_process = [r for r in todo if r["id"] not in existing_map]

    print(f"Total regions: {len(all_regions)}")
    print(f"Skipping {len(all_regions) - len(todo)} catch-all regions")
    print(f"Already have boundaries: {len(existing_map)}")
    print(f"To process: {len(to_process)}")

    limit = min(len(to_process), args.limit) if args.limit else len(to_process)
    success = 0
    failed = 0
    stored = 0
    country_counts = {}

    client = httpx.Client(timeout=30)

    for i in range(limit):
        r = to_process[i]
        country_name = r.get("country", {}).get("name", "Unknown")
        print(f"[{i + 1}/{limit}] {country_name}/{r['name']}... ", end="", flush=True)

        time.sleep(3)  # Conservative rate limit

        queries = [f"{r['name']}, {country_name}", r["name"]]
        found = False

        for query in queries:
            try:
                results = nominatim_search(client, query)
                if not results:
                    continue

                result = results[0]
                lat = float(result["lat"])
                lng = float(result["lon"])
                geojson = result.get("geojson")
                has_poly = geojson and geojson.get("type") in ("Polygon", "MultiPolygon")

                # Validate distance
                expected = centroid_map.get(r["id"])
                if expected:
                    dist = haversine(lat, lng, expected["lat"], expected["lng"])
                    if dist > 200:
                        print(f"REJECT too far ({round(dist)}km, query: \"{query}\")")
                        continue

                if has_poly:
                    centroid = compute_centroid(geojson)
                    if centroid and expected:
                        poly_dist = haversine(centroid["lat"], centroid["lng"],
                                              expected["lat"], expected["lng"])
                        if poly_dist > 300:
                            print(f"REJECT polygon too far ({round(poly_dist)}km)")
                            continue

                if not args.dry_run:
                    source_id = f"nominatim/{result.get('osm_type')}/{result.get('osm_id')}"
                    if has_poly:
                        sb.rpc("upsert_region_boundary", {
                            "p_region_id": r["id"],
                            "p_geojson": json.dumps(geojson),
                            "p_source_id": source_id,
                            "p_confidence": "approximate",
                        }).execute()
                    else:
                        sb.rpc("upsert_region_boundary", {
                            "p_region_id": r["id"],
                            "p_lat": lat, "p_lng": lng,
                            "p_source_id": source_id,
                            "p_confidence": "geocoded",
                        }).execute()

                type_label = geojson["type"] if has_poly else "centroid"
                print(f"ok {type_label} (query: \"{query}\")")
                success += 1
                stored += 1
                country_counts[country_name] = country_counts.get(country_name, 0) + 1
                found = True
                break
            except Exception as e:
                print(f"FAIL error: {e}")
                time.sleep(2)

        if not found:
            print("FAIL no result found")
            failed += 1

    client.close()

    print(f"\n=== Summary ===")
    print(f"Success: {success}  Failed: {failed}")
    print(f"\nStored ({stored}):")
    for c, n in sorted(country_counts.items(), key=lambda x: -x[1]):
        print(f"  {c}: {n}")


if __name__ == "__main__":
    main()
