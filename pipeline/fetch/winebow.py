#!/usr/bin/env python3
"""
Fetch Winebow catalog from sitemap + HTML scraping.

Drupal site with ~153 brand pages, each listing wines. Wine detail pages
have excellent structured data: 19 Drupal Views fields, scores, descriptions.

Usage:
    python -m pipeline.fetch.winebow
    python -m pipeline.fetch.winebow --limit 50
    python -m pipeline.fetch.winebow --resume
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

OUTPUT_FILE = Path("data/imports/winebow_catalog.json")
PROGRESS_FILE = Path("data/imports/winebow_progress.json")
DELAY_S = 2.0
BASE_URL = "https://www.winebow.com"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}

SPIRIT_TERMS = [
    "nardini", "poli-distillery", "four-pillars", "diplomatico",
    "lustau", "topo-chico", "fever-tree", "regans", "bitter-truth",
    "combier", "tempus-fugit", "clear-creek", "st-george-spirits",
]


def decode_entities(s: str) -> str:
    return html_mod.unescape(s)


def clean_text(h: str) -> str:
    return re.sub(r"\s+", " ", decode_entities(re.sub(r"<[^>]+>", "", h))).strip()


def parse_wine_page(body: str, url: str, brand_slug: str) -> dict:
    wine: dict = {"url": url, "_source": "winebow", "brand_slug": brand_slug}

    brand_m = re.search(r"vintage__(?:mobile-)?brand-name[^>]*>([^<]+)", body)
    if brand_m:
        wine["producer"] = clean_text(brand_m.group(1))

    prod_m = re.search(r"vintage__(?:mobile-)?product-name[^>]*>([^<]+)", body)
    if prod_m:
        wine["name"] = clean_text(prod_m.group(1))

    varietal_m = re.search(r"vintage__mobile-varietal[^>]*>\s*([\s\S]*?)</div>", body)
    if varietal_m:
        wine["varietal_display"] = clean_text(varietal_m.group(1))

    year_m = re.search(r"/(\d{4})/?$", url)
    if year_m:
        wine["vintage"] = year_m.group(1)
    if "vintage" not in wine:
        yd = re.search(r"vintage__mobile-year-label[^>]*>([^<]*\d{4}[^<]*)", body)
        if yd:
            ym = re.search(r"(\d{4})", yd.group(1))
            if ym:
                wine["vintage"] = ym.group(1)
    if "vintage" not in wine:
        vt = re.search(r"Vintage[:\s]*(\d{4})", body, re.IGNORECASE)
        if vt:
            wine["vintage"] = vt.group(1)

    # Drupal Views fields
    view_field_map = {
        "field-vintage-appellation": "appellation",
        "field-vintage-vineyard-name": "vineyard",
        "field-vintage-vineyard-size": "vineyard_size",
        "field-vintage-soil-composition": "soil",
        "field-vintage-training-method": "training_method",
        "field-vintage-elevation": "elevation",
        "field-vintage-vines-acre": "vines_per_acre",
        "field-vintage-yield-acre": "yield_per_acre",
        "field-vintage-exposure": "exposure",
        "field-vintage-bottles-produced": "production",
        "field-vintage-varietal-comp": "grape",
        "field-vintage-maceration": "maceration",
        "field-vintage-malolactic-ferm": "malolactic",
        "field-vintage-size-aging": "aging_vessel_size",
        "field-vintage-oak": "oak_type",
        "field-vintage-ph-level": "ph",
        "field-vintage-acidity": "acidity",
        "field-vintage-alcohol": "abv",
        "field-vintage-residual-sugar": "residual_sugar",
    }
    for m in re.finditer(
        r'<div class="views-field views-field-([a-z0-9-]+)">\s*([\s\S]*?)</div>', body
    ):
        field = m.group(1)
        value = clean_text(m.group(2))
        value = re.sub(r"^[^:]+:\s*", "", value)
        if not value:
            continue
        key = view_field_map.get(field)
        if key:
            wine[key] = value

    # Scores
    scores: list[dict] = []
    acclaim_m = re.search(r"acclaim-container([\s\S]*?)(?=esg-section|</main|$)", body)
    if acclaim_m:
        slides = acclaim_m.group(1).split("vintage__acclaim-slide")[1:]
        for slide in slides:
            score_entry: dict = {}
            sm = re.search(r"rating-score[^>]*>(\d+)", slide)
            if sm:
                score_entry["score"] = int(sm.group(1))
            pm = re.search(r"acclaim-publication-name[^>]*>([^<]+)", slide)
            if pm:
                score_entry["publication"] = clean_text(pm.group(1))
            qm = re.search(r"acclaim-quote[^>]*>([\s\S]*?)</div>", slide)
            if qm:
                score_entry["note"] = clean_text(qm.group(1))
            if score_entry.get("score") or score_entry.get("publication"):
                scores.append(score_entry)
    if scores:
        wine["scores"] = scores

    desc_m = re.search(r"vintage__section-content[^>]*>([\s\S]*?)</div>", body)
    if desc_m:
        text = clean_text(desc_m.group(1))
        if len(text) > 20:
            wine["description"] = text

    about_m = re.search(r"vintage__about-content[^>]*>([\s\S]*?)</div>", body)
    if about_m:
        text = clean_text(about_m.group(1))
        if len(text) > 20:
            wine["vineyard_description"] = text

    return wine


def main():
    parser = argparse.ArgumentParser(description="Fetch Winebow catalog")
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--resume", action="store_true")
    args = parser.parse_args()
    limit = args.limit or float("inf")

    with httpx.Client(timeout=20.0, headers=HEADERS, follow_redirects=True, max_redirects=5) as client:
        print("Step 1: Fetching brand URLs from sitemap...")
        all_wine_urls: list[dict] = []

        if args.resume and PROGRESS_FILE.exists():
            progress = json.loads(PROGRESS_FILE.read_text())
            all_wine_urls = progress["allWineUrls"]
            print(f"  Resumed with {len(all_wine_urls)} wine URLs from progress file")
        else:
            resp = client.get(f"{BASE_URL}/sitemap.xml")
            sm_urls = [m.group(1) for m in re.finditer(r"<loc>([^<]+)</loc>", resp.text)]
            brand_urls = [
                u.replace("http:", "https:") for u in sm_urls
                if "/our-brands/" in u and not u.endswith("/our-brands")
            ]
            print(f"  {len(brand_urls)} brand pages found")

            print("\nStep 2: Collecting wine URLs from brand pages...")
            for i, brand_url in enumerate(brand_urls):
                brand_slug = (brand_url.split("/our-brands/")[1] or "").rstrip("/") if "/our-brands/" in brand_url else ""
                if any(t in (brand_slug or "") for t in SPIRIT_TERMS):
                    print(f"  [{i+1}/{len(brand_urls)}] Skipping spirits: {brand_slug}")
                    continue
                try:
                    resp = client.get(brand_url)
                    if resp.status_code != 200:
                        print(f"  [{i+1}/{len(brand_urls)}] {resp.status_code}: {brand_url}")
                        continue
                    wine_links: list[str] = []
                    for m in re.finditer(r'<a href="(/our-brands/[^"]+/[^"]+)"', resp.text):
                        wu = f"{BASE_URL}{m.group(1)}"
                        if wu not in wine_links:
                            wine_links.append(wu)
                    if wine_links:
                        for wu in wine_links:
                            all_wine_urls.append({"url": wu, "brandSlug": brand_slug})
                        print(f"  [{i+1}/{len(brand_urls)}] {brand_slug}: {len(wine_links)} wines")
                    else:
                        print(f"  [{i+1}/{len(brand_urls)}] {brand_slug}: 0 wines (spirit/other?)")
                except Exception as e:
                    print(f"  [{i+1}/{len(brand_urls)}] Error: {e} -- {brand_url}")
                time.sleep(1.0)
            print(f"  Total: {len(all_wine_urls)} wine URLs")
            PROGRESS_FILE.parent.mkdir(parents=True, exist_ok=True)
            PROGRESS_FILE.write_text(json.dumps({"allWineUrls": all_wine_urls}))

        # Step 3: Scrape each wine page
        cap = int(min(limit, len(all_wine_urls)))
        print(f"\nStep 3: Scraping {cap} wine pages (delay: {DELAY_S}s)...")

        wines: list[dict] = []
        start_idx = 0
        if args.resume and OUTPUT_FILE.exists():
            wines = json.loads(OUTPUT_FILE.read_text())
            start_idx = len(wines)
            print(f"  Resuming from index {start_idx}")

        wine_count = 0
        error_count = 0

        for i in range(start_idx, cap):
            entry = all_wine_urls[i]
            url = entry["url"]
            brand_slug = entry["brandSlug"]
            try:
                resp = client.get(url)
                if resp.status_code != 200:
                    print(f"  [{i+1}/{cap}] {resp.status_code}: {url}")
                    error_count += 1
                    continue
                wine = parse_wine_page(resp.text, str(resp.url), brand_slug)
                wines.append(wine)
                wine_count += 1
                if (i + 1) % 25 == 0:
                    print(f"  [{i+1}/{cap}] wines: {wine_count}, errors: {error_count}")
                    OUTPUT_FILE.write_text(json.dumps(wines, indent=2, ensure_ascii=False))
                    PROGRESS_FILE.write_text(json.dumps({"allWineUrls": all_wine_urls, "lastIndex": i}))
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
            ("withVintage", "vintage"), ("withGrape", "grape"), ("withAppellation", "appellation"),
            ("withSoil", "soil"), ("withAbv", "abv"), ("withAcidity", "acidity"),
            ("withRS", "residual_sugar"), ("withPH", "ph"), ("withProduction", "production"),
            ("withVineyard", "vineyard"),
        ]
        print("\nField coverage:")
        print(f"  total: {total} (100.0%)")
        for label, key in fields:
            n = sum(1 for w in wines if w.get(key))
            print(f"  {label}: {n} ({n/total*100:.1f}%)")
        n_scores = sum(1 for w in wines if w.get("scores"))
        n_desc = sum(1 for w in wines if w.get("description"))
        print(f"  withScores: {n_scores} ({n_scores/total*100:.1f}%)")
        print(f"  withDescription: {n_desc} ({n_desc/total*100:.1f}%)")

    if cap >= len(all_wine_urls) and PROGRESS_FILE.exists():
        PROGRESS_FILE.unlink()


if __name__ == "__main__":
    main()
