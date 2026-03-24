#!/usr/bin/env python3
"""
EnofileOnline Wine Competition Aggregator Fetcher.

Source: enofileonline.com — aggregates 74+ US wine competitions (2012-2026)
API:    GET /search.aspx?fn=dosearch&... (JSON, no auth, 300 results/page)
Fields: brand, wineType (varietal), vintage, appellation, designation, price, award, competition

Usage:
    python -m pipeline.fetch.enofileonline
    python -m pipeline.fetch.enofileonline --analyze
    python -m pipeline.fetch.enofileonline --limit 1000
"""

import argparse
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import httpx

API_URL = "https://enofileonline.com/search.aspx"
OUTPUT_FILE = Path("data/imports/enofileonline_wines.json")
PAGE_SIZE = 300
DELAY_S = 0.5
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"

HEADERS = {
    "User-Agent": USER_AGENT,
    "Referer": "https://enofileonline.com/search.aspx",
    "X-Requested-With": "XMLHttpRequest",
}


def fetch_competitions(client: httpx.Client) -> list[dict]:
    """Fetch the list of available competitions."""
    resp = client.get(API_URL, params={"fn": "getcomps"})
    resp.raise_for_status()
    return resp.json()


def fetch_competition_results(client: httpx.Client, comp_id: int, page: int = 1) -> dict:
    """Fetch results for a specific competition. Returns {count, results}."""
    params = {
        "fn": "dosearch",
        "page": page,
        "pagesize": PAGE_SIZE,
        "brand": "",
        "varietal": "",
        "award": "",
        "competitionID": str(comp_id),
        "appellation": "",
    }
    resp = client.get(API_URL, params=params)
    resp.raise_for_status()
    data = resp.json()
    if isinstance(data, dict):
        return data
    # Fallback for unexpected format
    return {"count": len(data) if isinstance(data, list) else 0, "results": data if isinstance(data, list) else []}


def fetch_all(client: httpx.Client, limit: int | None = None) -> list[dict]:
    """Fetch all competition results by iterating through each competition.

    Note: The API's pagination is broken — it always returns the same 300 results
    regardless of page number. We fetch one page per competition (300 max per comp).
    For competitions with >300 entries, we only get a subset.
    """
    comps = fetch_competitions(client)
    print(f"  {len(comps)} competitions found")

    all_results = []
    seen_ids: set[int] = set()
    truncated_comps = []

    for comp in comps:
        if limit and len(all_results) >= limit:
            break

        comp_id = comp["id"]
        comp_name = comp["name"]
        comp_year = comp.get("year", "?")

        try:
            data = fetch_competition_results(client, comp_id)
        except Exception as err:
            print(f"  ERROR: {comp_name} ({comp_year}): {err}")
            time.sleep(3.0)
            continue

        results = data.get("results", [])
        total_count = data.get("count", 0)

        if not results:
            continue

        new_count = 0
        for r in results:
            rid = r.get("id")
            if rid and rid not in seen_ids:
                seen_ids.add(rid)
                all_results.append(r)
                new_count += 1

        truncated = total_count > len(results)
        if truncated:
            truncated_comps.append(f"{comp_name} ({comp_year}): got {len(results)}/{total_count}")

        print(f"  {comp_name} ({comp_year}): {new_count} wines" + (f" [TRUNCATED: {len(results)}/{total_count}]" if truncated else "") + f" ({len(all_results)} total)")
        time.sleep(DELAY_S)

    if truncated_comps:
        print(f"\n  Note: {len(truncated_comps)} competitions truncated at 300 results (API pagination broken)")

    return all_results


def normalize_result(r: dict) -> dict:
    """Normalize an EnofileOnline result into our standard format."""
    return {
        "enofile_id": r.get("id"),
        "year": r.get("year"),
        "competition": r.get("competition") or r.get("competitionName"),
        "brand": r.get("brand"),
        "varietal": r.get("wineType") or r.get("varietal"),
        "vintage": r.get("vintage"),
        "appellation": r.get("appellation"),
        "designation": r.get("designation"),
        "addl_designation": r.get("addlDesignation") or r.get("addl_designation"),
        "price": r.get("price"),
        "award": r.get("award"),
        "website": r.get("website"),
    }


def main():
    parser = argparse.ArgumentParser(description="Fetch EnofileOnline competition results")
    parser.add_argument("--analyze", action="store_true", help="Fetch competitions list only")
    parser.add_argument("--limit", type=int, help="Max results to fetch")
    args = parser.parse_args()

    print("=== EnofileOnline Wine Competition Fetcher ===")
    print(f"API: {API_URL}")
    print(f"Output: {OUTPUT_FILE}")

    with httpx.Client(timeout=30.0, headers=HEADERS) as client:
        if args.analyze:
            print("\nFetching competition list...")
            comps = fetch_competitions(client)
            print(f"\n{len(comps)} competitions:")
            for c in comps:
                print(f"  {c}")
            return

        print(f"\nFetching all results (limit={args.limit or 'none'})...")
        raw_results = fetch_all(client, limit=args.limit)

    wines = [normalize_result(r) for r in raw_results]

    # Stats
    total = len(wines)
    has_brand = sum(1 for w in wines if w.get("brand"))
    has_varietal = sum(1 for w in wines if w.get("varietal"))
    has_vintage = sum(1 for w in wines if w.get("vintage"))
    has_appellation = sum(1 for w in wines if w.get("appellation"))
    has_price = sum(1 for w in wines if w.get("price"))

    comp_counts: dict[str, int] = {}
    award_counts: dict[str, int] = {}
    for w in wines:
        comp = w.get("competition") or "unknown"
        comp_counts[comp] = comp_counts.get(comp, 0) + 1
        award = w.get("award") or "unknown"
        award_counts[award] = award_counts.get(award, 0) + 1

    top_comps = dict(sorted(comp_counts.items(), key=lambda x: -x[1])[:20])

    print(f"\n=== RESULTS ===")
    print(f"Total wines: {total}")
    if total:
        print(f"Has brand: {has_brand}/{total} ({has_brand/total*100:.1f}%)")
        print(f"Has varietal: {has_varietal}/{total} ({has_varietal/total*100:.1f}%)")
        print(f"Has vintage: {has_vintage}/{total} ({has_vintage/total*100:.1f}%)")
        print(f"Has appellation: {has_appellation}/{total} ({has_appellation/total*100:.1f}%)")
        print(f"Has price: {has_price}/{total} ({has_price/total*100:.1f}%)")
        print(f"\nAward distribution: {award_counts}")
        print(f"\nTop competitions:")
        for comp, n in top_comps.items():
            print(f"  {comp}: {n}")

    output = {
        "metadata": {
            "source": "EnofileOnline",
            "url": "https://enofileonline.com",
            "fetched_at": datetime.now(timezone.utc).isoformat(),
            "stats": {
                "total": total,
                "has_brand": has_brand,
                "has_varietal": has_varietal,
                "has_vintage": has_vintage,
                "has_appellation": has_appellation,
                "has_price": has_price,
                "competitions": len(comp_counts),
                "award_distribution": award_counts,
                "top_competitions": top_comps,
            },
        },
        "wines": wines,
    }

    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_FILE.write_text(json.dumps(output, indent=2, ensure_ascii=False), encoding="utf-8")
    file_size_mb = OUTPUT_FILE.stat().st_size / 1024 / 1024
    print(f"\nSaved to {OUTPUT_FILE} ({file_size_mb:.1f} MB)")


if __name__ == "__main__":
    main()
