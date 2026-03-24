#!/usr/bin/env python3
"""
Berliner Wine Trophy Results Fetcher.

Source: results.wine-trophy.com — 74K awarded wines, OIV-patronized
Access: Server-rendered HTML, paginated (200/page, ~370 pages)
Fields: wine name, producer, origin (country+region), grape varieties,
        award (medal + competition + year), photo URL

Also covers Asia Wine Trophy and Portugal Wine Trophy.

Usage:
    python -m pipeline.fetch.berliner_wine_trophy
    python -m pipeline.fetch.berliner_wine_trophy --year 2025
    python -m pipeline.fetch.berliner_wine_trophy --limit 1000
    python -m pipeline.fetch.berliner_wine_trophy --resume
"""

import argparse
import json
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import httpx

BASE_URL = "https://results.wine-trophy.com/"
OUTPUT_FILE = Path("data/imports/berliner_wine_trophy.json")
CHECKPOINT_FILE = Path("data/imports/berliner_checkpoint.json")
PAGE_SIZE = 200
DELAY_S = 2.0  # polite — this is a real website
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"


def fetch_page(client: httpx.Client, page: int, year: int | None = None) -> str:
    """Fetch a results page as HTML."""
    params = {"size": PAGE_SIZE, "page": page}
    if year:
        params["sf"] = f"trophy_year:{year}"
    resp = client.get(BASE_URL, params=params)
    resp.raise_for_status()
    return resp.text


def parse_total_pages(html: str) -> int:
    """Extract total page count from the page."""
    # Look for "Page X of Y" pattern
    m = re.search(r'Page\s+\d+\s+of\s+([\d,]+)', html)
    if m:
        return int(m.group(1).replace(",", ""))
    # Alternative: look for last page number in pagination
    pages = re.findall(r'pageSelect\((\d+)\)', html)
    if pages:
        return max(int(p) for p in pages)
    return 0


def parse_results(html: str) -> list[dict]:
    """Parse wine results from HTML page.

    Structure: <div class="search-result-item"> contains:
      - <h3><a href="/en/wine/{code}/{slug}">Wine Name</a></h3>
      - <div>Producer: ...</div>
      - <div>Origin: ...</div>
      - <div>Grape varieties: ...</div>
      - <div class="award">Award: Medal - Competition Year</div>
    """
    wines = []
    import html as html_mod

    # Split on search-result-item divs
    blocks = re.split(r'<div\s+class="search-result-item">', html)

    for block in blocks[1:]:  # Skip preamble
        wine = {}

        # Wine URL and name from <h3><a href="...">Name</a></h3>
        link_match = re.search(r'<h3>\s*<a\s+href="(/en/wine/([^"]+))">(.+?)</a>', block)
        if link_match:
            path = link_match.group(1).split("?")[0]  # strip backUrl param
            code_slug = link_match.group(2).split("?")[0]
            wine["wine_name"] = html_mod.unescape(link_match.group(3).strip())
            wine["url"] = f"https://results.wine-trophy.com{path}"
            # Trophy code is first part of path
            parts = code_slug.split("/")
            if parts:
                wine["trophy_code"] = parts[0]
            if len(parts) > 1:
                wine["slug"] = parts[1]

        # Producer
        prod_match = re.search(r'<div>Producer:\s*(.+?)</div>', block)
        if prod_match:
            wine["producer"] = html_mod.unescape(prod_match.group(1).strip())

        # Origin
        origin_match = re.search(r'<div>Origin:\s*(.+?)</div>', block)
        if origin_match:
            origin_raw = html_mod.unescape(origin_match.group(1).strip())
            wine["origin"] = origin_raw
            parts = [p.strip() for p in origin_raw.split(",")]
            if len(parts) >= 2:
                wine["country"] = parts[-1]
                wine["region"] = ", ".join(parts[:-1])
            else:
                wine["country"] = origin_raw

        # Grape varieties
        grape_match = re.search(r'<div>Grape variet(?:y|ies):\s*(.+?)</div>', block)
        if grape_match:
            wine["grapes"] = html_mod.unescape(grape_match.group(1).strip())

        # Award (inside <div class="award">)
        award_match = re.search(r'Award:\s*(.+?)(?:</div>|$)', block)
        if award_match:
            award_raw = html_mod.unescape(award_match.group(1).strip())
            # Remove any remaining HTML tags
            award_raw = re.sub(r'<[^>]+>', '', award_raw).strip()
            wine["award_raw"] = award_raw
            # Parse "Gold - Berliner Wein Trophy 2026"
            award_parts = award_raw.split(" - ", 1)
            if len(award_parts) == 2:
                wine["medal"] = award_parts[0].strip()
                wine["competition"] = award_parts[1].strip()
                yr_match = re.search(r'(\d{4})', award_parts[1])
                if yr_match:
                    wine["competition_year"] = int(yr_match.group(1))
            else:
                wine["medal"] = award_raw

        # Photo URL
        photo_match = re.search(r"src='(/en/Service/WinePhoto\?[^']+)'", block)
        if photo_match:
            wine["photo_url"] = f"https://results.wine-trophy.com{html_mod.unescape(photo_match.group(1))}"

        if wine.get("wine_name") or wine.get("producer"):
            wines.append(wine)

    return wines


def load_checkpoint() -> dict:
    """Load checkpoint for resume support."""
    if CHECKPOINT_FILE.exists():
        return json.loads(CHECKPOINT_FILE.read_text())
    return {"page": 1, "wines": []}


def save_checkpoint(page: int, wines: list[dict]):
    """Save checkpoint for resume support."""
    CHECKPOINT_FILE.parent.mkdir(parents=True, exist_ok=True)
    CHECKPOINT_FILE.write_text(json.dumps({"page": page, "wines_count": len(wines)}, indent=2))


def main():
    parser = argparse.ArgumentParser(description="Fetch Berliner Wine Trophy results")
    parser.add_argument("--year", type=int, help="Filter by competition year")
    parser.add_argument("--limit", type=int, help="Max wines to fetch")
    parser.add_argument("--resume", action="store_true", help="Resume from checkpoint")
    parser.add_argument("--pages", type=int, help="Max pages to fetch")
    args = parser.parse_args()

    print("=== Berliner Wine Trophy Results Fetcher ===")
    print(f"Output: {OUTPUT_FILE}")
    if args.year:
        print(f"Year filter: {args.year}")

    all_wines = []
    start_page = 1

    if args.resume:
        checkpoint = load_checkpoint()
        start_page = checkpoint.get("page", 1)
        # Load existing partial results if they exist
        if OUTPUT_FILE.exists():
            existing = json.loads(OUTPUT_FILE.read_text())
            all_wines = existing.get("wines", [])
            print(f"Resuming from page {start_page} ({len(all_wines)} wines loaded)")

    seen_codes: set[str] = {w.get("trophy_code", "") for w in all_wines if w.get("trophy_code")}

    with httpx.Client(timeout=30.0, headers={
        "User-Agent": USER_AGENT,
        "Accept": "text/html,application/xhtml+xml",
        "Accept-Language": "en-US,en;q=0.9",
    }) as client:
        # Get first page to determine total
        print(f"\nFetching page {start_page}...")
        html = fetch_page(client, start_page, year=args.year)
        total_pages = parse_total_pages(html)
        print(f"Total pages: {total_pages}")

        wines = parse_results(html)
        new_count = 0
        for w in wines:
            code = w.get("url", "") or f"{w.get('wine_name','')}-{w.get('producer','')}"
            if code not in seen_codes:
                seen_codes.add(code)
                all_wines.append(w)
                new_count += 1
        print(f"  Page {start_page}: {new_count} new wines (total: {len(all_wines)})")

        max_page = total_pages
        if args.pages:
            max_page = min(total_pages, start_page + args.pages - 1)

        for page in range(start_page + 1, max_page + 1):
            if args.limit and len(all_wines) >= args.limit:
                break

            time.sleep(DELAY_S)
            try:
                html = fetch_page(client, page, year=args.year)
                wines = parse_results(html)
            except Exception as err:
                print(f"  ERROR on page {page}: {err}")
                save_checkpoint(page, all_wines)
                time.sleep(5.0)
                try:
                    html = fetch_page(client, page, year=args.year)
                    wines = parse_results(html)
                except Exception as err2:
                    print(f"  RETRY FAILED on page {page}: {err2}")
                    save_checkpoint(page, all_wines)
                    continue

            new_count = 0
            for w in wines:
                code = w.get("url", "") or f"{w.get('wine_name','')}-{w.get('producer','')}"
                if code not in seen_codes:
                    seen_codes.add(code)
                    all_wines.append(w)
                    new_count += 1

            if page % 25 == 0 or page == max_page:
                print(f"  Page {page}/{max_page}: {new_count} new ({len(all_wines)} total)")
                save_checkpoint(page + 1, all_wines)

                # Periodic save
                _save_output(all_wines, args.year, partial=True)

    if args.limit:
        all_wines = all_wines[:args.limit]

    _save_output(all_wines, args.year, partial=False)

    # Clean up checkpoint on successful completion
    if CHECKPOINT_FILE.exists():
        CHECKPOINT_FILE.unlink()


def _save_output(wines: list[dict], year: int | None, partial: bool = False):
    """Save results to output file."""
    total = len(wines)
    has_producer = sum(1 for w in wines if w.get("producer"))
    has_grapes = sum(1 for w in wines if w.get("grapes"))
    has_country = sum(1 for w in wines if w.get("country"))
    has_medal = sum(1 for w in wines if w.get("medal"))

    medal_counts: dict[str, int] = {}
    country_counts: dict[str, int] = {}
    comp_counts: dict[str, int] = {}
    for w in wines:
        medal = w.get("medal") or "unknown"
        medal_counts[medal] = medal_counts.get(medal, 0) + 1
        country = w.get("country") or "unknown"
        country_counts[country] = country_counts.get(country, 0) + 1
        comp = w.get("competition") or "unknown"
        comp_counts[comp] = comp_counts.get(comp, 0) + 1

    top_countries = dict(sorted(country_counts.items(), key=lambda x: -x[1])[:20])

    if not partial:
        print(f"\n=== RESULTS ===")
        print(f"Total wines: {total}")
        if total:
            print(f"Has producer: {has_producer}/{total} ({has_producer/total*100:.1f}%)")
            print(f"Has grapes: {has_grapes}/{total} ({has_grapes/total*100:.1f}%)")
            print(f"Has country: {has_country}/{total} ({has_country/total*100:.1f}%)")
            print(f"Has medal: {has_medal}/{total} ({has_medal/total*100:.1f}%)")
            print(f"\nMedal distribution: {medal_counts}")
            print(f"Competitions: {comp_counts}")
            print(f"\nTop countries: {top_countries}")

    output = {
        "metadata": {
            "source": "Berliner Wine Trophy",
            "url": "https://results.wine-trophy.com",
            "fetched_at": datetime.now(timezone.utc).isoformat(),
            "partial": partial,
            "year_filter": year,
            "stats": {
                "total": total,
                "has_producer": has_producer,
                "has_grapes": has_grapes,
                "has_country": has_country,
                "has_medal": has_medal,
                "medal_distribution": medal_counts,
                "competitions": comp_counts,
                "top_countries": top_countries,
            },
        },
        "wines": wines,
    }

    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_FILE.write_text(json.dumps(output, indent=2, ensure_ascii=False), encoding="utf-8")
    file_size_mb = OUTPUT_FILE.stat().st_size / 1024 / 1024
    if not partial:
        print(f"\nSaved to {OUTPUT_FILE} ({file_size_mb:.1f} MB)")


if __name__ == "__main__":
    main()
