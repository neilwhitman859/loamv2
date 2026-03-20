"""
Fetch boundary polygons for California wine regions & appellations
from OpenStreetMap Nominatim API, then store in geographic_boundaries.

Usage: python -m pipeline.geo.fetch_california_boundaries [--dry-run] [--limit N]
"""

import sys
import json
import time
import argparse
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import httpx
from pipeline.lib.db import get_supabase

NOMINATIM_BASE = "https://nominatim.openstreetmap.org/search"
RATE_LIMIT_MS = 1100
USER_AGENT = "Loam Wine Platform (neil@loam.wine)"

CA_REGIONS = [
    "Napa Valley", "Sonoma County", "California", "Central Coast",
    "Paso Robles", "Monterey", "Mendocino", "Santa Barbara County",
    "Sierra Foothills", "Lodi",
]


def nominatim_search(client: httpx.Client, query: str) -> list[dict]:
    params = {"q": query, "format": "json", "polygon_geojson": "1", "limit": "1"}
    resp = client.get(NOMINATIM_BASE, params=params, headers={"User-Agent": USER_AGENT})
    resp.raise_for_status()
    return resp.json()


def has_polygon(result: dict | None) -> bool:
    if not result or "geojson" not in result:
        return False
    return result["geojson"].get("type") in ("Polygon", "MultiPolygon")


def build_queries(name: str, region_name: str | None, is_region: bool) -> list[str]:
    queries = []
    if is_region:
        queries.append(f"{name} wine region, California, United States")
        queries.append(f"{name}, California, United States")
        queries.append(f"{name} County, California, United States")
        queries.append(f"{name} AVA, California")
    else:
        queries.append(f"{name} AVA, California, United States")
        queries.append(f"{name} wine region, {region_name}, California")
        queries.append(f"{name}, {region_name}, California, United States")
        queries.append(f"{name}, California, United States")
        if "County" in name:
            queries.append(f"{name}, California")
    return queries


def main():
    parser = argparse.ArgumentParser(description="Fetch California boundary polygons")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--limit", type=int, default=None)
    args = parser.parse_args()

    sb = get_supabase()
    print(f"\n=== California Boundary Polygon Fetcher ===")
    print(f"Mode: {'DRY RUN' if args.dry_run else 'LIVE'}")

    # Get California appellations
    app_result = sb.table("appellations").select(
        "id, name, regions!inner(id, name, countries!inner(name))"
    ).in_("regions.name", CA_REGIONS).eq(
        "regions.countries.name", "United States"
    ).execute()
    appellations = app_result.data or []

    # Get California regions
    reg_result = sb.table("regions").select(
        "id, name, countries!inner(name)"
    ).in_("name", CA_REGIONS).eq("countries.name", "United States").execute()
    regions = reg_result.data or []

    work = []
    for r in regions:
        work.append({"type": "region", "id": r["id"], "name": r["name"], "regionName": None})
    for a in appellations:
        work.append({
            "type": "appellation", "id": a["id"], "name": a["name"],
            "regionName": a.get("regions", {}).get("name"),
        })

    print(f"Found {len(work)} entities ({len(regions)} regions, {len(appellations)} appellations)")

    to_process = work[:args.limit] if args.limit else work
    success = 0
    failed = 0
    failures = []
    successes = []

    client = httpx.Client(timeout=30)

    for i, item in enumerate(to_process):
        is_region = item["type"] == "region"
        queries = build_queries(item["name"], item.get("regionName"), is_region)

        print(f"[{i + 1}/{len(to_process)}] {item['type']}: {item['name']}...", end="", flush=True)

        polygon = None
        source_id = None

        for q in queries:
            time.sleep(RATE_LIMIT_MS / 1000)
            try:
                results = nominatim_search(client, q)
                if results and has_polygon(results[0]):
                    polygon = results[0]["geojson"]
                    source_id = f"osm/{results[0]['osm_type']}/{results[0]['osm_id']}"
                    print(f" ok polygon ({polygon['type']}, query: \"{q}\")")
                    break
            except Exception as err:
                print(f" error: {err}")

        if not polygon:
            print(" FAIL no polygon found")
            failed += 1
            failures.append(item["name"])
            continue

        if args.dry_run:
            success += 1
            successes.append({"name": item["name"], "type": polygon["type"]})
            continue

        geojson_str = json.dumps(polygon)
        if is_region:
            sb.rpc("insert_boundary_polygon", {
                "p_region_id": item["id"], "p_geojson": geojson_str, "p_source_id": source_id,
            }).execute()
        else:
            gb_result = sb.table("geographic_boundaries").select("id").eq(
                "appellation_id", item["id"]
            ).single().execute()
            if not gb_result.data:
                print("  no boundary row found")
                failed += 1
                failures.append(f"{item['name']} (no boundary row)")
                continue
            sb.rpc("update_boundary_polygon", {
                "p_boundary_id": gb_result.data["id"], "p_geojson": geojson_str,
                "p_source_id": source_id,
            }).execute()

        success += 1
        successes.append({"name": item["name"], "type": polygon["type"]})

    client.close()

    print(f"\n=== Summary ===")
    print(f"Success: {success}  Failed: {failed}")
    if failures:
        print(f"\nFailed:")
        for f in failures:
            print(f"  - {f}")
    if args.dry_run and successes:
        print(f"\nWould store:")
        for s in successes:
            print(f"  - {s['name']} ({s['type']})")


if __name__ == "__main__":
    main()
