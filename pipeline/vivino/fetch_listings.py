"""
Paginates through Vivino's explore API and saves raw wine listings to JSON.

Usage:
    python -m pipeline.vivino.fetch_listings --pages 42
    python -m pipeline.vivino.fetch_listings --pages 5 --delay-ms 2000
"""

import sys
import json
import re
import time
import argparse
from pathlib import Path

import httpx

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

BASE_URL = "https://www.vivino.com/api/explore/explore"
DEFAULT_PARAMS = {
    "country_code": "US",
    "currency_code": "USD",
    "min_rating": "1",
    "order_by": "ratings_count",
    "order": "desc",
    "price_range_min": "0",
    "price_range_max": "500",
}
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36"
)


def fetch_page(client: httpx.Client, page: int) -> dict:
    params = {**DEFAULT_PARAMS, "page": str(page)}
    resp = client.get(BASE_URL, params=params)
    resp.raise_for_status()
    return resp.json()["explore_vintage"]


def extract_listing(match: dict) -> dict:
    v = match.get("vintage") or {}
    wine = v.get("wine") or {}
    winery = wine.get("winery") or {}
    region = wine.get("region") or {}
    country = region.get("country") or {}
    stats = v.get("statistics") or {}
    price = match.get("price")

    price_per_bottle = None
    merchant_name = None
    source_url = None
    bottle_qty = 1
    if price:
        bottle_qty = price.get("bottle_quantity") or 1
        amt = price.get("amount")
        if amt is not None:
            price_per_bottle = round(amt / bottle_qty, 2)
        merchant_name = price.get("merchant_name")
        source_url = price.get("url")

    vintage_year = None
    year = v.get("year")
    if year and year > 1900:
        vintage_year = year
    else:
        m = re.search(r"-(\d{4})$", v.get("seo_name") or "")
        if m:
            vintage_year = int(m.group(1))

    return {
        "vivino_wine_id": wine.get("id"),
        "vivino_vintage_id": v.get("id"),
        "winery_name": winery.get("name"),
        "wine_name": wine.get("name"),
        "vintage_year": vintage_year,
        "region_name": region.get("name"),
        "country_name": country.get("name"),
        "country_code": country.get("code"),
        "wine_type_id": wine.get("type_id"),
        "rating_average": stats.get("ratings_average"),
        "rating_count": stats.get("ratings_count") or 0,
        "price_usd": price_per_bottle,
        "price_raw": price["amount"] if price else None,
        "bottle_quantity": bottle_qty,
        "merchant_name": merchant_name,
        "source_url": source_url,
        "is_natural": wine.get("is_natural") or False,
    }


def main():
    parser = argparse.ArgumentParser(description="Fetch Vivino wine listings")
    parser.add_argument("--pages", type=int, default=42, help="Max pages to fetch")
    parser.add_argument("--delay-ms", type=int, default=1500, help="Delay between requests in ms")
    parser.add_argument("--output", default="vivino_listings.json", help="Output file")
    args = parser.parse_args()

    max_pages = args.pages
    delay_s = args.delay_ms / 1000.0
    output_file = args.output

    print(f"Fetching up to {max_pages} pages ({max_pages * 24} wines) from Vivino...")
    print(f"Delay: {args.delay_ms}ms between requests\n")

    all_listings = []
    total_available = None
    consecutive_errors = 0

    client = httpx.Client(headers={"User-Agent": USER_AGENT, "Accept": "application/json"}, timeout=30)

    try:
        for page in range(1, max_pages + 1):
            try:
                result = fetch_page(client, page)

                if page == 1:
                    total_available = result.get("records_matched", 0)
                    print(f"Total wines available on Vivino: {total_available:,}")

                matches = result.get("matches") or []
                if not matches:
                    print(f"Page {page}: No more results. Stopping.")
                    break

                listings = [extract_listing(m) for m in matches]
                all_listings.extend(listings)
                consecutive_errors = 0

                with_price = sum(1 for l in listings if l["price_usd"] is not None)
                print(
                    f"\r  Page {page}/{max_pages} -- {len(all_listings)} wines "
                    f"({with_price}/{len(listings)} with price)",
                    end="", flush=True,
                )

                if page < max_pages:
                    time.sleep(delay_s)

            except Exception as err:
                print(f"\n  ERROR page {page}: {err}")
                consecutive_errors += 1
                if consecutive_errors >= 3:
                    print("  3 consecutive errors -- stopping.")
                    break
                time.sleep(delay_s * 2)
    finally:
        client.close()

    print(f"\n\nFetched {len(all_listings)} wine listings.")

    # Stats
    with_price = [l for l in all_listings if l["price_usd"] is not None]
    prices = sorted(l["price_usd"] for l in with_price)
    pct = round(len(with_price) / len(all_listings) * 100) if all_listings else 0
    print(f"  With price: {len(with_price)} ({pct}%)")
    if prices:
        print(f"  Price range: ${prices[0]} -- ${prices[-1]}")
        print(f"  Median price: ${prices[len(prices) // 2]}")

    countries: dict[str, int] = {}
    for l in all_listings:
        c = l.get("country_name") or "Unknown"
        countries[c] = countries.get(c, 0) + 1
    print("\n  Top countries:")
    for c, n in sorted(countries.items(), key=lambda x: -x[1])[:10]:
        print(f"    {c}: {n}")

    Path(output_file).write_text(json.dumps(all_listings, indent=2), encoding="utf-8")
    print(f"\nSaved to {output_file}")


if __name__ == "__main__":
    main()
