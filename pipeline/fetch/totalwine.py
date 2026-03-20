#!/usr/bin/env python3
"""
Slow scraper for Total Wine Lexington Green store inventory.

Fetches server-rendered HTML pages, parses wine product cards,
and writes results to JSONL.

Usage:
    python -m pipeline.fetch.totalwine
    python -m pipeline.fetch.totalwine --start-page 5
"""

import argparse
import json
import re
import sys
import time
from pathlib import Path

import httpx

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

STORE_ID = "2102"  # Lexington Green, KY
PAGE_SIZE = 120
BASE_URL = "https://www.totalwine.com/wine/c/c0020"
OUTPUT_FILE = Path("totalwine_lexington_green.jsonl")
PROGRESS_FILE = Path("totalwine_progress.json")
DELAY_S = 20.0  # 20 seconds between pages -- go slow

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
)


def load_progress() -> dict:
    if PROGRESS_FILE.exists():
        return json.loads(PROGRESS_FILE.read_text())
    return {"lastPage": 0, "totalProducts": 0}


def save_progress(page: int, total_products: int):
    PROGRESS_FILE.write_text(json.dumps({
        "lastPage": page,
        "totalProducts": total_products,
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }))


def parse_categories_from_url(url: str) -> list[str]:
    """Parse category path from product URL."""
    if not url:
        return []
    categories: list[str] = []
    cleaned = re.sub(r"\?.*$", "", url).replace("/wine/", "", 1).lstrip("/")
    parts = cleaned.split("/")
    skip = {"deals", "gift-center", "p"}
    for idx, part in enumerate(parts):
        if part in skip:
            continue
        if re.match(r"^\d+$", part):
            continue
        if "-p-" in part:
            continue
        # Last segment before /p/ is the product slug, skip it
        if idx == len(parts) - 1:
            continue
        if idx == len(parts) - 3 and len(parts) >= 3 and parts[-2] == "p":
            continue
        readable = " ".join(w.capitalize() for w in part.split("-"))
        categories.append(readable)
    return categories


def parse_products(html: str, page_num: int) -> list[dict]:
    """Parse wine products from HTML article blocks."""
    products: list[dict] = []

    for match in re.finditer(r"<article[^>]*>([\s\S]*?)</article>", html, re.IGNORECASE):
        block = match.group(1)

        # Product name from h2 > a
        name_match = re.search(r"<h2[^>]*>[\s\S]*?<a[^>]*>([\s\S]*?)</a>", block, re.IGNORECASE)
        if not name_match:
            continue
        name = re.sub(r"<[^>]+>", "", name_match.group(1)).strip()
        if not name or len(name) < 3:
            continue

        # Product URL
        href_match = re.search(r'<h2[^>]*>[\s\S]*?<a[^>]*href="([^"]*)"[^>]*>', block, re.IGNORECASE)
        raw_url = href_match.group(1).replace("&amp;", "&") if href_match else ""
        url = raw_url.split("?")[0]

        # SKU
        sku_match = re.search(r'data-sku="([^"]+)"', block)
        sku = sku_match.group(1) if sku_match else ""

        # Size
        size_match = re.search(r"(750ml|1\.5L|375ml|3L|1L|500ml|187ml|4\s*Pack)", block, re.IGNORECASE)
        size = size_match.group(0) if size_match else ""

        # Price
        price_match = re.search(r'class="price[^"]*">\$([0-9]+\.?\d*)', block)
        price = f"${price_match.group(1)}" if price_match else ""
        if not price:
            fallback = re.search(r"\$\d+\.\d{2}", block)
            price = fallback.group(0) if fallback else ""

        # Star rating
        star_match = re.search(r"([\d.]+)<!-- --> out of 5 stars", block)
        star_rating = star_match.group(1) if star_match else ""

        # Reviews count
        rev_match = re.search(r"([\d,]+)<!-- --> <span[^>]*>reviews?</span>", block, re.IGNORECASE)
        reviews = rev_match.group(1).replace(",", "") if rev_match else ""

        # Winery Direct badge
        winery_direct = bool(re.search(r"WINERY DIRECT", block, re.IGNORECASE))

        categories = parse_categories_from_url(url)

        products.append({
            "name": name,
            "url": url,
            "sku": sku,
            "size": size,
            "price": price,
            "starRating": star_rating,
            "reviews": reviews,
            "wineryDirect": winery_direct,
            "categories": categories,
            "page": page_num,
        })

    return products


def fetch_page(client: httpx.Client, page_num: int) -> str:
    url = f"{BASE_URL}?page={page_num}&pageSize={PAGE_SIZE}&aty=1,0,0,0"
    resp = client.get(url)
    resp.raise_for_status()
    return resp.text


def main():
    parser = argparse.ArgumentParser(description="Scrape Total Wine store inventory")
    parser.add_argument("--start-page", type=int, default=1)
    args = parser.parse_args()

    progress = load_progress()
    resume_page = max(args.start_page, progress["lastPage"] + 1)

    print(f"Starting from page {resume_page} ({PAGE_SIZE} items/page)")
    print(f"Output: {OUTPUT_FILE}")
    print(f"Delay: {DELAY_S}s between pages\n")

    if resume_page <= 1:
        OUTPUT_FILE.write_text("")

    total_products = progress.get("totalProducts", 0)
    total_pages = 43  # Will be updated from first page
    consecutive_empty = 0
    retries = 0

    headers = {
        "User-Agent": USER_AGENT,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Cookie": f"STORE_ID={STORE_ID}; shoppingMethod=INSTORE_PICKUP",
    }

    with httpx.Client(timeout=30.0, headers=headers) as client:
        page = resume_page
        while page <= total_pages:
            try:
                ts = time.strftime("%H:%M:%S")
                print(f"[{ts}] Fetching page {page}/{total_pages}...")

                html = fetch_page(client, page)

                # Try to get actual total from the HTML
                total_match = re.search(r"of\s+([\d,]+)\s*results", html)
                if total_match:
                    total = int(total_match.group(1).replace(",", ""))
                    total_pages = (total + PAGE_SIZE - 1) // PAGE_SIZE

                products = parse_products(html, page)
                print(f"  Found {len(products)} wines (total so far: {total_products + len(products)})")

                if len(products) == 0:
                    consecutive_empty += 1
                    print(f"  WARNING: Empty page ({consecutive_empty} consecutive)")
                    if consecutive_empty >= 3:
                        print("  3 consecutive empty pages -- stopping.")
                        break
                else:
                    consecutive_empty = 0

                # Append to JSONL
                if products:
                    lines = "\n".join(json.dumps(p, ensure_ascii=False) for p in products)
                    with open(OUTPUT_FILE, "a", encoding="utf-8") as f:
                        f.write(lines + "\n")

                total_products += len(products)
                save_progress(page, total_products)

                if products:
                    sample = products[0]
                    print(f'  Sample: "{sample["name"]}" -- {sample["price"]}')

                if page < total_pages:
                    print(f"  Waiting {DELAY_S}s...")
                    time.sleep(DELAY_S)

                retries = 0
                page += 1

            except Exception as err:
                print(f"  ERROR on page {page}: {err}")
                retries += 1
                if retries >= 3:
                    print(f"  3 retries failed on page {page} -- waiting 2 minutes then continuing")
                    time.sleep(120)
                    retries = 0
                    page += 1
                    continue
                print(f"  Waiting 60s before retry (attempt {retries}/3)...")
                time.sleep(60)
                # Retry same page (don't increment)

    print(f"\nDone! {total_products} total wines saved to {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
