"""
Geocodes all appellations that don't yet have a geographic_boundaries row.
Uses OpenStreetMap Nominatim (free, 1 req/sec rate limit).
Inserts centroid-only rows with confidence='geocoded', source='nominatim'.

Usage: python -m pipeline.geo.geocode_appellations [--dry-run] [--limit N] [--verbose]
"""

import sys
import re
import time
import argparse
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import httpx
from pipeline.lib.db import get_supabase

NOMINATIM_URL = "https://nominatim.openstreetmap.org/search"
RATE_LIMIT_MS = 1100
USER_AGENT = "Loam Wine Intelligence (loam.onrender.com)"

DESIGNATION_PREFIXES = [
    "AOC ", "AOP ", "DOC ", "DOCG ", "DO ", "AVA ", "IGP ", "IGT ",
    "VdP ", "GI ", "IG ", "PDO ", "PGI ", "DOP ", "WO ",
]

PLACE_EXTRACTORS = [
    re.compile(r"\bdi\s+(.+)$", re.IGNORECASE),
    re.compile(r"\bdel\s+(.+)$", re.IGNORECASE),
    re.compile(r"\bdella\s+(.+)$", re.IGNORECASE),
    re.compile(r"\bdelle\s+(.+)$", re.IGNORECASE),
    re.compile(r"\bdei\s+(.+)$", re.IGNORECASE),
    re.compile(r"\bd'\s*(.+)$", re.IGNORECASE),
    re.compile(r"\bde\s+(.+)$", re.IGNORECASE),
]

QUALITY_SUFFIXES = [
    "Superiore", "Riserva", "Classico", "Gran Selezione", "Passito",
    "Spumante", "Frizzante", "Liquoroso", "Novello",
]


def clean_appellation_name(name: str) -> str:
    cleaned = name
    for prefix in DESIGNATION_PREFIXES:
        if cleaned.startswith(prefix):
            cleaned = cleaned[len(prefix):]
    return cleaned.strip()


def strip_quality_suffix(name: str) -> str:
    cleaned = name
    for suffix in QUALITY_SUFFIXES:
        if cleaned.endswith(f" {suffix}"):
            cleaned = cleaned[:-(len(suffix) + 1)]
    return cleaned


def extract_place_names(name: str) -> list[str]:
    places = []
    seen = set()
    for variant in [name, strip_quality_suffix(name)]:
        for regex in PLACE_EXTRACTORS:
            match = regex.search(variant)
            if match:
                place = match.group(1).strip()
                if place not in seen:
                    seen.add(place)
                    places.append(place)
                break
    return places


def build_search_queries(appellation: str, region: str, country: str) -> list[str]:
    clean_name = clean_appellation_name(appellation)
    stripped_name = strip_quality_suffix(clean_name)
    place_names = extract_place_names(clean_name)
    queries = []
    seen = set()

    def add(q: str):
        if q not in seen:
            seen.add(q)
            queries.append(q)

    add(f"{clean_name}, {region}, {country}")
    if stripped_name != clean_name:
        add(f"{stripped_name}, {region}, {country}")
    add(f"{clean_name} wine region, {country}")
    for place in place_names:
        add(f"{place}, {region}, {country}")
        add(f"{place}, {country}")
    if not place_names:
        add(f"{clean_name}, {country}")
    if clean_name.lower() != region.lower():
        add(f"{region} wine region, {country}")
        add(f"{region}, {country}")
    return queries


def geocode_query(client: httpx.Client, query: str) -> dict | None:
    params = {"q": query, "format": "json", "limit": "1", "addressdetails": "0"}
    resp = client.get(NOMINATIM_URL, params=params, headers={
        "User-Agent": USER_AGENT, "Accept-Language": "en",
    })
    resp.raise_for_status()
    data = resp.json()
    if data:
        return {
            "lat": float(data[0]["lat"]),
            "lng": float(data[0]["lon"]),
            "display_name": data[0]["display_name"],
            "osm_type": data[0].get("osm_type"),
            "osm_id": data[0].get("osm_id"),
        }
    return None


def geocode_appellation(client: httpx.Client, appellation: str, region: str,
                        country: str, verbose: bool = False) -> dict | None:
    queries = build_search_queries(appellation, region, country)
    region_fallback_query = f"{region} wine region, {country}"

    for query in queries:
        time.sleep(RATE_LIMIT_MS / 1000)
        if verbose:
            print(f"     trying: \"{query}\" ... ", end="", flush=True)
        result = geocode_query(client, query)
        if result:
            if verbose:
                print("ok")
            is_region_fallback = query == region_fallback_query
            return {**result, "query": query, "regionFallback": is_region_fallback}
        if verbose:
            print("no result")
    return None


def main():
    parser = argparse.ArgumentParser(description="Geocode appellations via Nominatim")
    parser.add_argument("--dry-run", action="store_true", help="Preview only, no DB writes")
    parser.add_argument("--limit", type=int, default=None, help="Max appellations to process")
    parser.add_argument("--verbose", action="store_true", help="Show each query attempt")
    args = parser.parse_args()

    sb = get_supabase()
    print(f"Geocoding appellations{' (DRY RUN)' if args.dry_run else ''}")
    print(f"   Rate limit: {RATE_LIMIT_MS}ms per request\n")

    # Fetch appellations with region/country via join
    all_apps = []
    offset = 0
    while True:
        result = sb.table("appellations").select(
            "id, name, designation_type, regions!inner(name, countries!inner(name))"
        ).order("name").range(offset, offset + 999).execute()
        all_apps.extend(result.data)
        if len(result.data) < 1000:
            break
        offset += 1000

    # Check which already have boundaries
    existing_ids = set()
    offset = 0
    while True:
        result = sb.table("geographic_boundaries").select("appellation_id").not_(
            "appellation_id", "is", "null"
        ).range(offset, offset + 999).execute()
        for row in result.data:
            existing_ids.add(row["appellation_id"])
        if len(result.data) < 1000:
            break
        offset += 1000

    to_geocode = [a for a in all_apps if a["id"] not in existing_ids]

    print(f"   Total appellations: {len(all_apps)}")
    print(f"   Already geocoded: {len(existing_ids)}")
    print(f"   To geocode: {len(to_geocode)}")
    if args.limit:
        print(f"   Limit: {args.limit}")
    print()

    batch = to_geocode[:args.limit] if args.limit else to_geocode
    success = 0
    region_fallbacks = 0
    failed = 0
    failures = []

    client = httpx.Client(timeout=30)

    for i, a in enumerate(batch):
        region_name = a["regions"]["name"]
        country_name = a["regions"]["countries"]["name"]
        progress = f"[{i + 1}/{len(batch)}]"

        try:
            result = geocode_appellation(client, a["name"], region_name, country_name, args.verbose)

            if result:
                fallback_tag = " [region fallback]" if result["regionFallback"] else ""
                if result["regionFallback"]:
                    region_fallbacks += 1
                print(f"{progress} ok {a['name']} ({region_name}, {country_name}) -> "
                      f"{result['lat']:.4f}, {result['lng']:.4f}{fallback_tag}")

                if not args.dry_run:
                    point_wkt = f"SRID=4326;POINT({result['lng']} {result['lat']})"
                    source_id = (f"{result['osm_type']}/{result['osm_id']}"
                                 if result.get("osm_id") else None)

                    insert_result = sb.table("geographic_boundaries").insert({
                        "appellation_id": a["id"],
                        "centroid": point_wkt,
                        "boundary_confidence": "geocoded",
                        "boundary_source": "nominatim",
                        "boundary_source_id": source_id,
                    }).execute()

                    if hasattr(insert_result, "error") and insert_result.error:
                        print(f"   Insert failed: {insert_result.error}")
                        failed += 1
                        failures.append({
                            "name": a["name"], "region": region_name,
                            "country": country_name, "error": str(insert_result.error),
                        })
                        continue
                success += 1
            else:
                print(f"{progress} FAIL {a['name']} ({region_name}, {country_name}) -- no results")
                failed += 1
                failures.append({
                    "name": a["name"], "region": region_name,
                    "country": country_name, "error": "no geocode results",
                })
        except Exception as err:
            print(f"{progress} FAIL {a['name']} -- {err}")
            failed += 1
            failures.append({
                "name": a["name"], "region": region_name,
                "country": country_name, "error": str(err),
            })

    client.close()

    print(f"\n--- Summary ---")
    print(f"Geocoded:          {success}")
    print(f"  Region fallback: {region_fallbacks}")
    print(f"Failed:            {failed}")
    if failures:
        print(f"\nFailed appellations:")
        for f in failures:
            print(f"  - {f['name']} ({f['region']}, {f['country']}): {f['error']}")


if __name__ == "__main__":
    main()
