#!/usr/bin/env python3
"""
Fetch European Cellars (Eric Solomon) catalog via sitemap + HTML scraping.

WordPress site with 718 wine pages. Clean dt/dd structure for technical data.
Robots.txt requests 10-second crawl delay.

Usage:
    python -m pipeline.fetch.european_cellars
    python -m pipeline.fetch.european_cellars --limit 50
    python -m pipeline.fetch.european_cellars --resume
"""

import argparse
import html as html_mod
import json
import re
import sys
import time
from pathlib import Path

import httpx

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

OUTPUT_FILE = Path("data/imports/european_cellars_catalog.json")
PROGRESS_FILE = Path("data/imports/european_cellars_progress.json")
DELAY_S = 10.0  # respecting robots.txt crawl-delay
BASE_URL = "https://www.europeancellars.com"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}


def decode_entities(s: str) -> str:
    return html_mod.unescape(s)


def clean_text(h: str) -> str:
    return re.sub(r"\s+", " ", decode_entities(re.sub(r"<[^>]+>", "", h))).strip()


def parse_wine_page(body: str, url: str) -> dict:
    wine: dict = {"url": url, "_source": "european_cellars"}

    title_m = re.search(r"<h1[^>]*>([^<]+)</h1>", body)
    if title_m:
        wine["name"] = clean_text(title_m.group(1))

    producer_m = re.search(
        r'<h3 class="producer-header"[^>]*>(?:<a[^>]*>)?([^<]+)(?:</a>)?</h3>', body
    )
    if producer_m:
        wine["producer"] = clean_text(producer_m.group(1))

    type_m = re.search(r'class="[^"]*wine-(red|white|rose|sparkling|dessert|cider)[^"]*"', body)
    if type_m:
        wine["color"] = type_m.group(1)

    certs: list[str] = []
    if "wine-certified-organic" in body:
        certs.append("certified_organic")
    if "wine-biodynamic" in body:
        certs.append("biodynamic")
    if "wine-vegan" in body:
        certs.append("vegan")
    if certs:
        wine["certifications"] = certs

    # Technical information (dt/dd pairs)
    tech_field_map = {
        "appellation": "appellation", "variety": "grape", "age of vines": "vine_age",
        "farming": "farming", "soil": "soil", "altitude": "altitude",
        "fermentation": "vinification", "aging": "aging", "vineyard size": "vineyard_size",
        "winemaker": "winemaker", "proprietor": "proprietor",
    }
    tech_m = re.search(r'<dl class="technical-information">([\s\S]*?)</dl>', body)
    if tech_m:
        for pair in re.finditer(r"<dt>([^<]+)</dt>\s*\n?\s*<dd>([\s\S]*?)</dd>", tech_m.group(1)):
            label = pair.group(1).strip().lower()
            value = clean_text(pair.group(2))
            if not value:
                continue
            key = tech_field_map.get(label)
            if key:
                wine[key] = value
            else:
                wine.setdefault("extra_fields", {})[label] = value

    # Location information
    loc_m = re.search(r'<dl class="location-information">([\s\S]*?)</dl>', body)
    if loc_m:
        for pair in re.finditer(r"<h6>([^<]+)</h6>\s*\n?\s*<dd>([\s\S]*?)</dd>", loc_m.group(1)):
            label = pair.group(1).strip().lower()
            value = clean_text(pair.group(2))
            if not value:
                continue
            if label == "location":
                wine["location"] = value
            elif label == "appellation" and "appellation" not in wine:
                wine["appellation"] = value
            elif label == "proprietor":
                wine["proprietor"] = value
            elif label == "winemaker":
                wine["winemaker"] = value
            elif label == "size / elevation":
                wine["size_elevation"] = value

    # Scores
    ratings_m = re.search(r"Ratings &amp; Reviews([\s\S]*?)(?:Other Wines|Downloads|<footer)", body)
    if ratings_m:
        scores = []
        for sp in re.finditer(
            r'<h2[^>]*>(\d{2,3})\+?</h2>\s*[\s\S]*?<h6[^>]*>([^<]+)</h6>',
            ratings_m.group(1),
        ):
            score = int(sp.group(1))
            label = clean_text(sp.group(2))
            entry: dict = {"score": score}
            vm = re.match(r"^(\d{4})\s+(.+)", label)
            if vm:
                entry["vintage"] = vm.group(1)
                entry["wine_name"] = vm.group(2)
            else:
                entry["label"] = label
            scores.append(entry)
        if scores:
            wine["scores"] = scores

    slug_part = url.split("/wine/")
    wine["url_slug"] = slug_part[1].rstrip("/") if len(slug_part) > 1 else ""
    return wine


def main():
    parser = argparse.ArgumentParser(description="Fetch European Cellars catalog")
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--resume", action="store_true")
    args = parser.parse_args()
    limit = args.limit or float("inf")

    with httpx.Client(timeout=20.0, headers=HEADERS, follow_redirects=True) as client:
        print("Step 1: Fetching wine URLs from sitemap...")
        all_urls: list[str] = []

        if args.resume and PROGRESS_FILE.exists():
            progress = json.loads(PROGRESS_FILE.read_text())
            all_urls = progress["allUrls"]
            print(f"  Resumed with {len(all_urls)} URLs from progress file")
        else:
            resp = client.get(f"{BASE_URL}/wp-sitemap-posts-wine-1.xml")
            all_urls = [m.group(1) for m in re.finditer(r"<loc>([^<]+)</loc>", resp.text)]
            print(f"  {len(all_urls)} wine URLs from sitemap")

        cap = int(min(limit, len(all_urls)))
        print(f"\nStep 2: Scraping {cap} pages (delay: {DELAY_S}s -- respecting robots.txt)...")

        wines: list[dict] = []
        start_idx = 0
        if args.resume and OUTPUT_FILE.exists():
            wines = json.loads(OUTPUT_FILE.read_text())
            start_idx = len(wines)
            print(f"  Resuming from index {start_idx}")

        wine_count = 0
        error_count = 0

        for i in range(start_idx, cap):
            url = all_urls[i]
            try:
                resp = client.get(url)
                if resp.status_code != 200:
                    print(f"  [{i+1}/{cap}] {resp.status_code}: {url}")
                    error_count += 1
                    continue
                wine = parse_wine_page(resp.text, url)
                wines.append(wine)
                wine_count += 1
                if (i + 1) % 25 == 0:
                    print(f"  [{i+1}/{cap}] wines: {wine_count}, errors: {error_count}")
                    OUTPUT_FILE.write_text(json.dumps(wines, indent=2, ensure_ascii=False))
                    PROGRESS_FILE.write_text(json.dumps({"allUrls": all_urls, "lastIndex": i}))
            except Exception as e:
                print(f"  [{i+1}/{cap}] Error: {e} -- {url}")
                error_count += 1
            time.sleep(DELAY_S)

    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_FILE.write_text(json.dumps(wines, indent=2, ensure_ascii=False))
    print(f"\nDone. {len(wines)} wines saved to {OUTPUT_FILE}")
    print(f"   {error_count} errors")

    if wines:
        print("\nSample wine:")
        print(json.dumps(wines[0], indent=2, ensure_ascii=False))

    total = len(wines)
    if total:
        fields = [
            ("withProducer", "producer"), ("withGrape", "grape"), ("withAppellation", "appellation"),
            ("withSoil", "soil"), ("withAltitude", "altitude"), ("withVineAge", "vine_age"),
            ("withFarming", "farming"), ("withVinification", "vinification"),
            ("withAging", "aging"),
        ]
        print("\nField coverage:")
        print(f"  total: {total} (100.0%)")
        for label, key in fields:
            n = sum(1 for w in wines if w.get(key))
            print(f"  {label}: {n} ({n/total*100:.1f}%)")
        n_scores = sum(1 for w in wines if w.get("scores"))
        n_certs = sum(1 for w in wines if w.get("certifications"))
        print(f"  withScores: {n_scores} ({n_scores/total*100:.1f}%)")
        print(f"  withCerts: {n_certs} ({n_certs/total*100:.1f}%)")

    if cap >= len(all_urls) and PROGRESS_FILE.exists():
        PROGRESS_FILE.unlink()


if __name__ == "__main__":
    main()
