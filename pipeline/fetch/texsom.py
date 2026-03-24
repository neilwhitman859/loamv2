#!/usr/bin/env python3
"""
TEXSOM International Wine Awards Fetcher.

Source: texsom.com — 40+ years of competition results
API:    Static JSON files at /wp-content/plugins/wine-seeker/data/json-data-{YEAR}.json
Fields: producer, wine name, appellation, country, vintage, medal

Usage:
    python -m pipeline.fetch.texsom
    python -m pipeline.fetch.texsom --years 2024,2025
"""

import argparse
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import httpx

BASE_URL = "https://texsom.com/wp-content/plugins/wine-seeker/data/json-data-{year}.json"
OUTPUT_FILE = Path("data/imports/texsom_wines.json")
DELAY_S = 1.0
USER_AGENT = "LoamWineDB/1.0 (neil@loam.wine)"

# Try a wide range of years
DEFAULT_YEARS = list(range(1985, 2027))


def fetch_year(client: httpx.Client, year: int) -> list[list] | None:
    """Fetch results for a single year. Returns None if not found."""
    url = BASE_URL.format(year=year)
    try:
        resp = client.get(url)
        if resp.status_code == 404:
            return None
        resp.raise_for_status()
        text = resp.text.strip()

        # Skip HTML responses (old years return a webpage)
        if text.startswith("<!DOCTYPE") or text.startswith("<html"):
            return None

        # Response formats:
        # A) all_posts_wine_data_YYYY = [[...],...]  (2015, 2022+)
        # B) ['2017','Producer','Wine',...],['2017',...]  (bare arrays, 2016-2021)
        # Both use single quotes and escaped apostrophes

        # Strip variable assignment if present
        if text.startswith("all_posts_wine_data"):
            eq_idx = text.index("=")
            text = text[eq_idx + 1:].strip()
            if text.endswith(";"):
                text = text[:-1]
        else:
            # Bare arrays — wrap in outer brackets
            text = "[" + text.strip() + "]"
            if text.endswith(",]"):
                text = text[:-2] + "]"

        # Convert JS single-quoted arrays to valid JSON
        text = text.replace("\\'", "\u2019")  # curly apostrophe placeholder
        text = text.replace("'", '"')
        text = text.replace("\u2019", "'")

        data = json.loads(text)
        if isinstance(data, list) and len(data) > 0:
            return data
        return None
    except (ValueError, json.JSONDecodeError) as err:
        print(f"  {year}: parse error ({err})")
        return None
    except Exception as err:
        print(f"  {year}: error ({err})")
        return None


def parse_result(row: list, year: int) -> dict:
    """
    Parse a TEXSOM result row (array format).
    Observed format: [year, producer, wine_name, appellation, country, vintage, ?, medal, ?, wine_name2, ?]
    Field positions may vary — we map what we can.
    """
    def safe_get(idx: int) -> str | None:
        if idx < len(row):
            val = row[idx]
            if val and str(val).strip():
                return str(val).strip()
        return None

    # Try to identify fields based on position
    result = {
        "year": safe_get(0) or str(year),
        "producer": safe_get(1),
        "wine_name": safe_get(2),
        "appellation": safe_get(3),
        "country": safe_get(4),
        "vintage": safe_get(5),
    }

    # Medal is typically position 7
    medal = safe_get(7)
    if medal and any(m in medal.lower() for m in ["gold", "silver", "bronze", "best", "grand"]):
        result["award"] = medal
    elif safe_get(6) and any(m in str(safe_get(6)).lower() for m in ["gold", "silver", "bronze", "best", "grand"]):
        result["award"] = safe_get(6)
    else:
        # Try all remaining positions for medal
        for i in range(6, len(row)):
            val = safe_get(i)
            if val and any(m in val.lower() for m in ["gold", "silver", "bronze", "best", "grand"]):
                result["award"] = val
                break

    if "award" not in result:
        result["award"] = safe_get(7)

    # Capture any remaining fields
    extras = []
    for i in range(len(row)):
        if i not in (0, 1, 2, 3, 4, 5, 7):
            val = safe_get(i)
            if val:
                extras.append(val)
    if extras:
        result["extra_fields"] = extras

    return result


def main():
    parser = argparse.ArgumentParser(description="Fetch TEXSOM competition results")
    parser.add_argument("--years", help="Comma-separated years (default: 1985-2026)")
    args = parser.parse_args()

    if args.years:
        years = [int(y.strip()) for y in args.years.split(",")]
    else:
        years = DEFAULT_YEARS

    print("=== TEXSOM International Wine Awards Fetcher ===")
    print(f"Output: {OUTPUT_FILE}")
    print(f"Years to try: {years[0]}-{years[-1]}")

    all_wines = []
    year_counts: dict[int, int] = {}

    with httpx.Client(timeout=30.0, headers={"User-Agent": USER_AGENT}) as client:
        for year in years:
            data = fetch_year(client, year)
            if data is None:
                continue

            wines_for_year = [parse_result(row, year) for row in data if isinstance(row, list)]
            year_counts[year] = len(wines_for_year)
            all_wines.extend(wines_for_year)
            print(f"  {year}: {len(wines_for_year)} wines")
            time.sleep(DELAY_S)

    # Stats
    total = len(all_wines)
    has_producer = sum(1 for w in all_wines if w.get("producer"))
    has_wine_name = sum(1 for w in all_wines if w.get("wine_name"))
    has_appellation = sum(1 for w in all_wines if w.get("appellation"))
    has_country = sum(1 for w in all_wines if w.get("country"))
    has_vintage = sum(1 for w in all_wines if w.get("vintage"))
    has_award = sum(1 for w in all_wines if w.get("award"))

    award_counts: dict[str, int] = {}
    for w in all_wines:
        award = w.get("award") or "unknown"
        award_counts[award] = award_counts.get(award, 0) + 1

    print(f"\n=== RESULTS ===")
    print(f"Total wines: {total}")
    print(f"Years with data: {sorted(year_counts.keys())}")
    if total:
        print(f"Has producer: {has_producer}/{total} ({has_producer/total*100:.1f}%)")
        print(f"Has wine_name: {has_wine_name}/{total} ({has_wine_name/total*100:.1f}%)")
        print(f"Has appellation: {has_appellation}/{total} ({has_appellation/total*100:.1f}%)")
        print(f"Has country: {has_country}/{total} ({has_country/total*100:.1f}%)")
        print(f"Has vintage: {has_vintage}/{total} ({has_vintage/total*100:.1f}%)")
        print(f"Has award: {has_award}/{total} ({has_award/total*100:.1f}%)")
        print(f"\nAward distribution: {award_counts}")
        print(f"\nPer-year counts:")
        for year in sorted(year_counts.keys()):
            print(f"  {year}: {year_counts[year]}")

    output = {
        "metadata": {
            "source": "TEXSOM International Wine Awards",
            "url": "https://texsom.com/results",
            "fetched_at": datetime.now(timezone.utc).isoformat(),
            "stats": {
                "total": total,
                "years": sorted(year_counts.keys()),
                "year_counts": {str(k): v for k, v in sorted(year_counts.items())},
                "has_producer": has_producer,
                "has_appellation": has_appellation,
                "has_country": has_country,
                "has_vintage": has_vintage,
                "award_distribution": award_counts,
            },
        },
        "wines": all_wines,
    }

    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_FILE.write_text(json.dumps(output, indent=2, ensure_ascii=False), encoding="utf-8")
    file_size_mb = OUTPUT_FILE.stat().st_size / 1024 / 1024
    print(f"\nSaved to {OUTPUT_FILE} ({file_size_mb:.1f} MB)")


if __name__ == "__main__":
    main()
