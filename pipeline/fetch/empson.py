#!/usr/bin/env python3
"""
Fetch Empson & Co. Italian wine catalog via sitemap + HTML scraping.

WordPress site with ~279 wine pages. Excellent per-wine technical data.
Uses h5/p column pairs for 25+ structured fields.

Usage:
    python -m pipeline.fetch.empson
    python -m pipeline.fetch.empson --limit 50
    python -m pipeline.fetch.empson --resume
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

OUTPUT_FILE = Path("data/imports/empson_catalog.json")
PROGRESS_FILE = Path("data/imports/empson_progress.json")
DELAY_S = 3.0
BASE_URL = "https://www.empson.com"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}


def decode_entities(s: str) -> str:
    return html_mod.unescape(s)


def clean_text(h: str) -> str:
    return decode_entities(re.sub(r"<[^>]+>", "", h).strip())


def clean_whitespace(s: str) -> str:
    return re.sub(r"\s+", " ", s).strip()


def parse_wine_page(body: str, url: str) -> dict:
    wine: dict = {"url": url, "_source": "empson"}

    title_m = re.search(r"<h1[^>]*>([^<]+)</h1>", body)
    if title_m:
        wine["name"] = clean_whitespace(clean_text(title_m.group(1)))

    producer_link = re.search(
        r'<a[^>]*href="[^"]*\/wine_producer\/([^"\/]+)\/"[^>]*>([^<]+)</a>', body
    )
    if producer_link:
        wine["producer"] = clean_whitespace(clean_text(producer_link.group(2)))
        wine["producer_slug"] = producer_link.group(1)
    else:
        for m in re.finditer(r"<h2[^>]*>([^<]+)</h2>", body):
            text = clean_whitespace(clean_text(m.group(1)))
            if text != "Most recent awards" and 1 < len(text) < 100:
                wine["producer"] = text
                break

    # Extract h5 label + next sibling p value pairs
    field_map = {
        "grape varieties": "grape",
        "fermentation container": "fermentation_container",
        "length of alcoholic fermentation": "fermentation_duration",
        "type of yeast": "yeast_type",
        "fermentation temperature": "fermentation_temp",
        "maceration technique": "maceration_technique",
        "length of maceration": "maceration_duration",
        "aging containers": "aging_container",
        "container size": "aging_container_size",
        "type of oak": "oak_type",
        "aging before bottling": "aging_duration",
        "closure": "closure",
        "vineyard location": "vineyard_location",
        "vineyard size": "vineyard_size",
        "soil composition": "soil",
        "vine training": "training_method",
        "altitude": "altitude",
        "vine density": "vine_density",
        "yield": "yield",
        "exposure": "exposure",
        "age of vines": "vine_age",
        "time of harvest": "harvest_time",
        "total yearly production (in bottles)": "production",
        "tasting notes": "tasting_notes",
        "food pairings": "food_pairings",
        "aging potential": "aging_potential",
        "alcohol": "abv",
        "winemaker": "winemaker",
        "malolactic fermentation": "malolactic",
        "bottling period": "bottling_period",
        "serving temperature": "serving_temp",
        "first vintage of this wine": "first_vintage",
    }

    for m in re.finditer(
        r"<h5>([^<]+)</h5>\s*</div>\s*<div[^>]*>\s*<p>([\s\S]*?)</p>", body
    ):
        label = m.group(1).replace(":", "").strip().lower()
        value = clean_whitespace(clean_text(m.group(2)))
        if not value:
            continue
        key = field_map.get(label)
        if key:
            wine[key] = value
        else:
            wine.setdefault("extra_fields", {})[label] = value

    # Description
    desc_m = re.search(
        r'<div class="col-md-8">\s*\n?\s*\n?\s*<div><p>([\s\S]*?)</p>', body
    )
    if desc_m:
        text = clean_whitespace(clean_text(desc_m.group(1)))
        if len(text) > 20:
            wine["description"] = text

    # Scores/awards
    awards_m = re.search(r"Most recent awards([\s\S]*?)(?:You may also like|<footer)", body)
    if awards_m:
        clean_awards = re.sub(r"<svg[\s\S]*?</svg>", "", awards_m.group(1))
        clean_awards = re.sub(r"<style[\s\S]*?</style>", "", clean_awards)
        scores = []
        for sm in re.finditer(r"(\d{4})\s*\|\s*([A-Za-z][A-Za-z\s.]+?)\s+(\d{2,3})\b", clean_awards):
            scores.append({
                "vintage": sm.group(1),
                "publication": sm.group(2).strip(),
                "score": int(sm.group(3)),
            })
        if scores:
            wine["scores"] = scores

    slug_part = url.split("/wine/")
    wine["url_slug"] = slug_part[1].rstrip("/") if len(slug_part) > 1 else ""
    return wine


def main():
    parser = argparse.ArgumentParser(description="Fetch Empson catalog")
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--resume", action="store_true")
    args = parser.parse_args()
    limit = args.limit or float("inf")

    with httpx.Client(timeout=20.0, headers=HEADERS, follow_redirects=True) as client:
        # Step 1: Get wine URLs from sitemap
        print("Step 1: Fetching wine URLs from sitemap...")
        all_urls: list[str] = []

        if args.resume and PROGRESS_FILE.exists():
            progress = json.loads(PROGRESS_FILE.read_text())
            all_urls = progress["allUrls"]
            print(f"  Resumed with {len(all_urls)} URLs from progress file")
        else:
            resp = client.get(f"{BASE_URL}/wine-sitemap.xml")
            for m in re.finditer(r"<loc>(?:<!\[CDATA\[)?(.*?)(?:\]\]>)?</loc>", resp.text):
                all_urls.append(m.group(1))
            print(f"  {len(all_urls)} wine URLs from sitemap")

        # Step 2: Scrape each page
        cap = int(min(limit, len(all_urls)))
        print(f"\nStep 2: Scraping {cap} pages (delay: {DELAY_S}s)...")

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

    # Final save
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
            ("withProducer", "producer"), ("withGrape", "grape"), ("withSoil", "soil"),
            ("withAltitude", "altitude"), ("withVineAge", "vine_age"), ("withAbv", "abv"),
            ("withProduction", "production"), ("withTastingNotes", "tasting_notes"),
            ("withFoodPairings", "food_pairings"), ("withWinemaker", "winemaker"),
            ("withFermentContainer", "fermentation_container"), ("withOakType", "oak_type"),
        ]
        print("\nField coverage:")
        print(f"  total: {total} (100.0%)")
        for label, key in fields:
            n = sum(1 for w in wines if w.get(key))
            print(f"  {label}: {n} ({n/total*100:.1f}%)")
        n_scores = sum(1 for w in wines if w.get("scores"))
        print(f"  withScores: {n_scores} ({n_scores/total*100:.1f}%)")

    # Cleanup progress on completion
    if cap >= len(all_urls) and PROGRESS_FILE.exists():
        PROGRESS_FILE.unlink()


if __name__ == "__main__":
    main()
