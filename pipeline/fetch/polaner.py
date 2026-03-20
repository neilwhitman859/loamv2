#!/usr/bin/env python3
"""
Fetch Polaner Selections catalog via WordPress REST API.

Polaner is a WordPress site with custom 'wine' post type and taxonomies.
The REST API exposes: wine title, country, region, appellation,
biodynamic/organic/natural certifications.

Usage:
    python -m pipeline.fetch.polaner
    python -m pipeline.fetch.polaner --limit 100
"""

import argparse
import html
import json
import re
import sys
import time
from pathlib import Path

import httpx

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

OUTPUT_FILE = Path("data/imports/polaner_catalog.json")
BASE_API = "https://www.polanerselections.com/wp-json/wp/v2"
DELAY_S = 0.5
HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}


def decode_entities(s: str) -> str:
    """Decode HTML entities including numeric character references."""
    s = html.unescape(s)
    return s


def fetch_all_terms(client: httpx.Client, taxonomy: str) -> dict[int, dict]:
    """Fetch all terms for a WordPress taxonomy."""
    terms: dict[int, dict] = {}
    page = 1
    while True:
        url = f"{BASE_API}/{taxonomy}?per_page=100&page={page}&_fields=id,name,slug"
        resp = client.get(url)
        if resp.status_code != 200:
            break
        data = resp.json()
        if not data:
            break
        for t in data:
            terms[t["id"]] = {"name": decode_entities(t["name"]), "slug": t["slug"]}
        total_pages = int(resp.headers.get("x-wp-totalpages", 0))
        if not total_pages or page >= total_pages:
            break
        page += 1
        time.sleep(DELAY_S)
    return terms


def main():
    parser = argparse.ArgumentParser(description="Fetch Polaner Selections catalog")
    parser.add_argument("--limit", type=int, default=None, help="Max wines to fetch")
    args = parser.parse_args()
    limit = args.limit or float("inf")

    with httpx.Client(timeout=15.0, headers=HEADERS, follow_redirects=True) as client:
        # Step 1: Fetch all taxonomies
        print("Step 1: Fetching taxonomies...")
        countries = fetch_all_terms(client, "country")
        regions = fetch_all_terms(client, "region")
        appellations = fetch_all_terms(client, "appellations")
        biodynamics = fetch_all_terms(client, "biodynamics")
        organics = fetch_all_terms(client, "organics")
        green_props = fetch_all_terms(client, "green_properties")

        print(f"  Countries: {len(countries)}")
        print(f"  Regions: {len(regions)}")
        print(f"  Appellations: {len(appellations)}")
        print(f"  Biodynamics: {len(biodynamics)}")
        print(f"  Organics: {len(organics)}")
        print(f"  Green properties: {len(green_props)}")

        # Step 2: Fetch all wines
        print("\nStep 2: Fetching wines...")
        wines: list[dict] = []
        page = 1

        while len(wines) < limit:
            url = (
                f"{BASE_API}/wine?per_page=100&page={page}"
                f"&_fields=id,slug,title,link,appellations,country,region,biodynamics,organics,green_properties"
            )
            resp = client.get(url)
            if resp.status_code != 200:
                print(f"  Page {page}: status {resp.status_code}")
                break

            data = resp.json()
            if not data:
                break

            if page == 1:
                total_wines = int(resp.headers.get("x-wp-total", 0))
                print(f"  Total wines available: {total_wines}")

            for w in data:
                if len(wines) >= limit:
                    break

                raw_title = w.get("title", "")
                if isinstance(raw_title, dict):
                    raw_title = raw_title.get("rendered", "")
                title = decode_entities(raw_title)

                wine: dict = {
                    "wp_id": w["id"],
                    "slug": w.get("slug", ""),
                    "title": title,
                    "url": w.get("link", ""),
                    "_source": "polaner",
                }

                # Resolve taxonomies
                country_ids = w.get("country") or []
                if country_ids:
                    c = countries.get(country_ids[0])
                    if c:
                        wine["country"] = c["name"]

                region_ids = w.get("region") or []
                if region_ids:
                    r = regions.get(region_ids[0])
                    if r:
                        wine["region"] = r["name"]

                app_ids = w.get("appellations") or []
                if app_ids:
                    apps = [appellations[aid]["name"] for aid in app_ids if aid in appellations]
                    if apps:
                        wine["appellation"] = apps[0]
                    if len(apps) > 1:
                        wine["appellations_all"] = apps

                # Certifications
                certs: list[str] = []
                for bid in w.get("biodynamics") or []:
                    term = biodynamics.get(bid)
                    if term:
                        certs.append(f"biodynamic:{term['name']}")
                for oid in w.get("organics") or []:
                    term = organics.get(oid)
                    if term:
                        certs.append(f"organic:{term['name']}")
                for gid in w.get("green_properties") or []:
                    term = green_props.get(gid)
                    if term:
                        certs.append(f"green:{term['name']}")
                if certs:
                    wine["certifications"] = certs

                wines.append(wine)

            print(f"  Page {page}: {len(data)} wines (total: {len(wines)})")
            page += 1
            time.sleep(DELAY_S)

    # Step 3: Save
    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_FILE.write_text(json.dumps(wines, indent=2, ensure_ascii=False))
    print(f"\nDone. {len(wines)} wines saved to {OUTPUT_FILE}")

    if wines:
        print("\nSample wine:")
        print(json.dumps(wines[0], indent=2, ensure_ascii=False))

    # Stats
    total = len(wines)
    if total:
        stats = {
            "total": total,
            "withCountry": sum(1 for w in wines if w.get("country")),
            "withRegion": sum(1 for w in wines if w.get("region")),
            "withAppellation": sum(1 for w in wines if w.get("appellation")),
            "withCerts": sum(1 for w in wines if w.get("certifications")),
        }
        print("\nField coverage:")
        for k, v in stats.items():
            pct = f"{v / total * 100:.1f}" if total else "0"
            print(f"  {k}: {v} ({pct}%)")

        # Country distribution
        country_dist: dict[str, int] = {}
        for w in wines:
            c = w.get("country", "unknown")
            country_dist[c] = country_dist.get(c, 0) + 1
        print("\nCountry distribution:")
        for k, v in sorted(country_dist.items(), key=lambda x: -x[1]):
            print(f"  {k}: {v}")

        # Certification distribution
        cert_dist: dict[str, int] = {}
        for w in wines:
            for c in w.get("certifications", []):
                cert_dist[c] = cert_dist.get(c, 0) + 1
        if cert_dist:
            print("\nCertification distribution:")
            for k, v in sorted(cert_dist.items(), key=lambda x: -x[1]):
                print(f"  {k}: {v}")


if __name__ == "__main__":
    main()
