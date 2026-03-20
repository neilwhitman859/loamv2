#!/usr/bin/env python3
"""
Scrape winedeals.com wine catalog using Playwright.

Two-pass approach:
  Pass 1: Paginate listing pages, collect all product URLs
  Pass 2: Visit each product page, extract structured attributes

Captures: UPC, grapes, ABV, scores with review text + critic + date,
food pairings, compare-at price, producer, and all More Information fields.

Resume-safe: skips already-scraped URLs on restart.

Usage:
    python -m pipeline.fetch.winedeals
    python -m pipeline.fetch.winedeals --urls-only
    python -m pipeline.fetch.winedeals --scrape-only
"""

import argparse
import json
import re
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

URLS_FILE = Path("data/imports/winedeals_urls.json")
CATALOG_FILE = Path("data/imports/winedeals_catalog.json")
BASE = "https://www.winedeals.com"
LISTING_URL = f"{BASE}/wine.html?product_list_limit=44&p="
DELAY_S = 1.5


# -- Pass 1: Collect product URLs --
def collect_urls(browser) -> list[str]:
    print("\n=== Pass 1: Collecting product URLs ===")
    page = browser.new_page()
    page.add_init_script("Object.defineProperty(navigator, 'webdriver', { get: () => false })")
    page.set_extra_http_headers({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
    })

    all_urls: set[str] = set()
    page_num = 1
    total_pages: int | None = None
    consecutive_empty = 0

    while True:
        url = LISTING_URL + str(page_num)
        try:
            page.goto(url, wait_until="networkidle", timeout=45000)
            try:
                page.wait_for_selector("a.product-item-link", timeout=10000)
            except Exception:
                pass
        except Exception as err:
            print(f"  Page {page_num} load error: {err}")
            consecutive_empty += 1
            if consecutive_empty >= 3:
                break
            page_num += 1
            time.sleep(DELAY_S)
            continue

        if total_pages is None:
            total_pages = page.evaluate("""() => {
                const pageLinks = document.querySelectorAll('ul.items.pages-items li a, .pages-items .item a');
                let max = 1;
                pageLinks.forEach(a => {
                    const text = a.textContent.trim().replace('Page', '').trim();
                    const num = parseInt(text);
                    if (!isNaN(num) && num > max) max = num;
                });
                return max;
            }""")
            print(f"  Total pages: {total_pages}")

        urls = page.evaluate("""() => {
            return [...document.querySelectorAll('a.product-item-link')]
                .map(a => a.href)
                .filter(h => h.includes('.html') && (h.includes('/wine/') || h.includes('/spirits/')));
        }""")

        if len(urls) == 0:
            consecutive_empty += 1
            if consecutive_empty >= 3:
                print(f"  3 consecutive empty pages at page {page_num}, stopping.")
                break
        else:
            consecutive_empty = 0

        for u in urls:
            all_urls.add(u)

        if page_num % 10 == 0 or (total_pages and page_num == total_pages):
            print(f"  Page {page_num}/{total_pages} -- {len(all_urls)} URLs collected")

        if total_pages and page_num >= total_pages:
            break
        page_num += 1
        time.sleep(DELAY_S)

    page.close()

    url_list = sorted(all_urls)
    URLS_FILE.parent.mkdir(parents=True, exist_ok=True)
    URLS_FILE.write_text(json.dumps(url_list, indent=2))
    print(f"  Saved {len(url_list)} URLs to {URLS_FILE}")
    return url_list


# -- Pass 2: Scrape each product page --
def scrape_products(browser, urls: list[str]):
    print("\n=== Pass 2: Scraping product pages ===")

    catalog: list[dict] = []
    scraped: set[str] = set()
    if CATALOG_FILE.exists():
        catalog = json.loads(CATALOG_FILE.read_text())
        for w in catalog:
            scraped.add(w.get("url", ""))
        print(f"  Resuming: {len(catalog)} already scraped")

    remaining = [u for u in urls if u not in scraped]
    print(f"  {len(remaining)} URLs to scrape")

    page = browser.new_page()
    page.add_init_script("Object.defineProperty(navigator, 'webdriver', { get: () => false })")
    page.set_extra_http_headers({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
    })

    count = 0
    errors = 0

    for url in remaining:
        try:
            page.goto(url, wait_until="networkidle", timeout=45000)
            try:
                page.wait_for_selector(".page-title", timeout=10000)
            except Exception:
                pass

            data = page.evaluate("""() => {
                const result = {};

                result.name = document.querySelector('.page-title span, .page-title')?.textContent?.trim() || null;

                const oldPrice = document.querySelector('.old-price .price');
                const finalPrice = document.querySelector('.final-price .price, .price-wrapper .price, .special-price .price');
                const anyPrice = document.querySelector('.price');
                result.compare_at_price = oldPrice?.textContent?.trim() || null;
                result.price = finalPrice?.textContent?.trim() || anyPrice?.textContent?.trim() || null;

                const itemText = document.body.innerText.match(/Item#:\\s*(\\d+)/);
                result.item_number = itemText ? itemText[1] : null;

                const badgeSection = document.querySelector('.product-info-main');
                if (badgeSection) {
                    const badgeText = badgeSection.textContent;
                    const badgeMatch = badgeText.match(/(WE|SP|RP|JS|WS|VI|JD)\\s*(\\d{2,3})\\s*(?:pts?\\.?|points?)?/i);
                    if (badgeMatch) {
                        result.badge_publication = badgeMatch[1].toUpperCase();
                        result.badge_score = parseInt(badgeMatch[2]);
                    }
                }

                const infoMain = document.querySelector('.product-info-main');
                if (infoMain) {
                    const text = infoMain.textContent;
                    const allGrapesMatch = text.match(/All Grapes?:\\s*([^\\n]+)/i);
                    result.all_grapes = allGrapesMatch ? allGrapesMatch[1].trim() : null;
                    const foodMatch = text.match(/Food Pairings?:\\s*([^\\n]+)/i);
                    result.food_pairings = foodMatch ? foodMatch[1].trim() : null;
                    const producerLink = infoMain.querySelector('a[href*="brand"]');
                    if (!producerLink) {
                        const moreFrom = text.match(/More from this Producer:\\s*(.+)/i);
                        result.producer = moreFrom ? moreFrom[1].trim() : null;
                    } else {
                        result.producer = producerLink.textContent.trim();
                    }
                }

                result.reviews = [];
                result.description = document.querySelector('.product.attribute.description .value')?.textContent?.trim() || null;

                const attributes = {};
                const tables = document.querySelectorAll('table');
                tables.forEach(table => {
                    const rows = table.querySelectorAll('tr');
                    rows.forEach(row => {
                        const th = row.querySelector('th');
                        const td = row.querySelector('td');
                        if (th && td) {
                            const label = th.textContent.trim();
                            const value = td.textContent.trim();
                            if (label && value) attributes[label] = value;
                        }
                    });
                });
                result.attributes = attributes;

                return result;
            }""")

            attrs = data.get("attributes", {})

            catalog.append({
                "url": url,
                "name": data.get("name"),
                "item_number": data.get("item_number"),
                "price": data.get("price"),
                "compare_at_price": data.get("compare_at_price"),
                "description": data.get("description"),
                "producer": data.get("producer") or attrs.get("Wine/Spirit Brand"),
                "sku": attrs.get("SKU"),
                "upc": attrs.get("UPC"),
                "country": attrs.get("Country"),
                "region": attrs.get("Region"),
                "district": attrs.get("District"),
                "appellation": attrs.get("Appellation"),
                "abv": attrs.get("Proof/Alcohol by Volume"),
                "vintage": attrs.get("Vintage"),
                "grapes": attrs.get("Grape(s)"),
                "primary_grape": attrs.get("Primary Grape"),
                "all_grapes": data.get("all_grapes") or attrs.get("Grape(s)"),
                "brand": attrs.get("Wine/Spirit Brand"),
                "wine_type": attrs.get("Wine Type"),
                "color": attrs.get("Wine - Color"),
                "package_size": attrs.get("Package Size"),
                "product_type": attrs.get("Product Type"),
                "bottles_per_case": attrs.get("Bottles per Case"),
                "alternate_name": attrs.get("Alternate Name"),
                "can_ship": attrs.get("Can it Be Shipped"),
                "awards": attrs.get("Awards"),
                "food_pairing": data.get("food_pairings") or attrs.get("Food Pairing"),
                "occasion_pairing": attrs.get("Occasion Pairing"),
                "spec_designation": attrs.get("Spec. Designation"),
                "dollar_sale": attrs.get("Dollar Sale (Y/N)"),
                "reviews": data.get("reviews", []),
                "avg_rating": data.get("avg_rating"),
                "num_ratings": data.get("num_ratings"),
                "all_attributes": attrs,
            })

            count += 1
        except Exception as err:
            print(f"  Error scraping {url}: {err}")
            errors += 1

        if count % 50 == 0 and count > 0:
            CATALOG_FILE.write_text(json.dumps(catalog, indent=2, ensure_ascii=False))
            total = len(catalog)
            with_scores = sum(1 for w in catalog if w.get("reviews"))
            print(f"  {total}/{len(urls)} -- {count} new, {errors} errors, {with_scores} with scores")

        time.sleep(DELAY_S)

    page.close()

    # Final save
    CATALOG_FILE.write_text(json.dumps(catalog, indent=2, ensure_ascii=False))
    print(f"\n  Complete: {len(catalog)} total wines saved")
    print(f"  Errors: {errors}")

    total = len(catalog)
    if total == 0:
        return

    def stat(label, fn):
        n = sum(1 for w in catalog if fn(w))
        print(f"  {label}: {n}/{total} ({n / total * 100:.1f}%)")

    print("")
    stat("UPC", lambda w: w.get("upc"))
    stat("Grapes", lambda w: w.get("grapes") or w.get("all_grapes"))
    stat("ABV", lambda w: w.get("abv"))
    stat("Vintage", lambda w: w.get("vintage"))
    stat("Country", lambda w: w.get("country"))
    stat("Appellation", lambda w: w.get("appellation"))
    stat("Producer", lambda w: w.get("producer"))
    stat("Scores", lambda w: w.get("reviews") and len(w["reviews"]) > 0)
    stat("Food pairing", lambda w: w.get("food_pairing"))
    stat("Compare-at price", lambda w: w.get("compare_at_price"))
    stat("Description", lambda w: w.get("description"))


def main():
    parser = argparse.ArgumentParser(description="Scrape winedeals.com wine catalog")
    parser.add_argument("--urls-only", action="store_true", help="Pass 1 only")
    parser.add_argument("--scrape-only", action="store_true", help="Pass 2 only (requires urls file)")
    args = parser.parse_args()

    from playwright.sync_api import sync_playwright

    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-setuid-sandbox", "--disable-blink-features=AutomationControlled"],
        )

        try:
            if args.scrape_only:
                if not URLS_FILE.exists():
                    print("No URLs file found. Run without --scrape-only first.")
                    sys.exit(1)
                urls = json.loads(URLS_FILE.read_text())
                print(f"Loaded {len(urls)} URLs from {URLS_FILE}")
            else:
                urls = collect_urls(browser)
                if args.urls_only:
                    print("URLs collected. Run with --scrape-only to scrape products.")
                    return

            scrape_products(browser, urls)
        finally:
            browser.close()


if __name__ == "__main__":
    main()
