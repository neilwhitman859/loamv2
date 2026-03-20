#!/usr/bin/env python3
"""
Fetch Skurnik Wines catalog via FacetWP REST API + detail scraping.

Two-phase approach:
  Phase 1: FacetWP API bulk listing (270 pages x 20 wines = ~5,394 wines)
  Phase 2: Individual SKU page scraping for enrichment detail

Usage:
    python -m pipeline.fetch.skurnik
    python -m pipeline.fetch.skurnik --phase1
    python -m pipeline.fetch.skurnik --phase2
    python -m pipeline.fetch.skurnik --limit 50
    python -m pipeline.fetch.skurnik --resume
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

OUTPUT_FILE = Path("data/imports/skurnik_catalog.json")
PROGRESS_FILE = Path("data/imports/skurnik_progress.json")
API_DELAY_S = 1.5
DETAIL_DELAY_S = 2.0
BASE_URL = "https://www.skurnik.com"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}


def decode_entities(s: str) -> str:
    return html_mod.unescape(s)


def clean_text(h: str) -> str:
    return re.sub(r"\s+", " ", decode_entities(re.sub(r"<[^>]+>", "", h))).strip()


def parse_listing_cards(template_html: str) -> list[dict]:
    wines: list[dict] = []
    card_blocks = re.split(r'class="sku-list-item\b', template_html)[1:]

    for card in card_blocks:
        wine: dict = {"_source": "skurnik"}

        pm = re.search(r'href="[^"]*\/producer\/([^"\/]+)\/"[^>]*>\s*([\s\S]*?)\s*</a>', card)
        if pm:
            wine["producer_slug"] = pm.group(1)
            wine["producer"] = decode_entities(re.sub(r"\s+", " ", pm.group(2)).strip())

        tm = re.search(r'sku-title[^>]*><a href="([^"]+)"[^>]*>([^<]+)</a>', card)
        if tm:
            href = tm.group(1)
            wine["url"] = href if href.startswith("http") else f"{BASE_URL}{href}"
            wine["url_slug"] = re.sub(r"^.*/sku/", "", href).rstrip("/")
            wine["name"] = decode_entities(tm.group(2).strip())

        im = re.search(r'src="([^"]+\.(jpg|png|webp)[^"]*)"', card, re.IGNORECASE)
        if im:
            wine["image_url"] = im.group(1)

        label_field_map = {
            "sku": "sku", "vintage": "vintage", "country": "country",
            "region": "region", "appellation": "appellation", "variety": "grape",
            "color": "color", "farming practice": "farming",
        }
        for lm in re.finditer(
            r'list-label[^"]*"[^>]*>([^<]+)</div>\s*(?:</div>)?\s*(?:<div[^>]*>)?\s*<div class="list-desc[^"]*"[^>]*>([^<]+)</div>',
            card,
        ):
            label = re.sub(r"[:#]", "", lm.group(1)).strip().lower()
            value = decode_entities(lm.group(2).strip())
            if not value:
                continue
            key = label_field_map.get(label)
            if key:
                wine[key] = value
            else:
                wine.setdefault("extra_fields", {})[label] = value

        if wine.get("name"):
            wines.append(wine)
    return wines


def fetch_phase1(client: httpx.Client, existing_wines: list[dict], start_page: int, limit: float) -> list[dict]:
    print("Phase 1: Fetching wine catalog via FacetWP API...")
    wines = list(existing_wines)
    page = start_page
    total_pages = None
    consecutive_empty = 0

    while True:
        if len(wines) >= limit:
            print(f"  Reached limit of {int(limit)} wines")
            break

        payload = {
            "action": "facetwp_refresh",
            "data": {
                "facets": {
                    "color": [], "country": [], "region": [], "appellation": [],
                    "producer": [], "varietal": [], "vintage": [],
                    "wine_farming_practice": [], "kosher_type": [],
                },
                "frozen_facets": {},
                "http_params": {"uri": "portfolio-wine", "lang": ""},
                "template": "our_wines_22",
                "extras": {"sort": "default"},
                "soft_refresh": 0, "is_preload": 0, "first_load": 0,
                "paged": page,
            },
        }

        try:
            resp = client.post(
                f"{BASE_URL}/wp-json/facetwp/v1/refresh",
                json=payload,
                headers={**HEADERS, "Content-Type": "application/json", "Referer": f"{BASE_URL}/portfolio-wine/"},
            )
            if resp.status_code != 200:
                print(f"  Page {page}: HTTP {resp.status_code}")
                consecutive_empty += 1
                if consecutive_empty >= 3:
                    break
                page += 1
                time.sleep(API_DELAY_S)
                continue

            data = resp.json()
            settings = data.get("settings", {}).get("pager", {})
            if settings:
                total_pages = settings.get("total_pages")
                total_rows = settings.get("total_rows")
                if page == start_page:
                    print(f"  Total: {total_rows} wines across {total_pages} pages")

            template = data.get("template", "")
            page_wines = parse_listing_cards(template)

            if not page_wines:
                if len(template) < 100:
                    print(f"  Page {page}: empty template ({len(template)} chars)")
                    consecutive_empty += 1
                    if consecutive_empty >= 3:
                        break
                else:
                    if page <= 3:
                        debug_path = Path(f"data/imports/skurnik_debug_page{page}.html")
                        debug_path.write_text(template)
                        print(f"  Page {page}: {len(template)} chars but 0 wines parsed -- saved debug HTML")
                    consecutive_empty += 1
                    if consecutive_empty >= 5:
                        break
            else:
                consecutive_empty = 0
                wines.extend(page_wines)

            print(f"  [Page {page}/{total_pages or '?'}] {len(page_wines)} wines (total: {len(wines)})")

            if page % 10 == 0:
                OUTPUT_FILE.write_text(json.dumps(wines, indent=2, ensure_ascii=False))
                PROGRESS_FILE.write_text(json.dumps({"phase": 1, "page": page, "totalPages": total_pages}))

            if total_pages and page >= total_pages:
                break
            page += 1

        except Exception as e:
            print(f"  Page {page}: Error -- {e}")
            consecutive_empty += 1
            if consecutive_empty >= 3:
                break
            page += 1

        time.sleep(API_DELAY_S)

    OUTPUT_FILE.write_text(json.dumps(wines, indent=2, ensure_ascii=False))
    print(f"\n  Phase 1 complete: {len(wines)} wines")
    return wines


def parse_detail_page(body: str, wine: dict) -> dict:
    detail = dict(wine)

    # Structured details
    details_m = re.search(r"<!-- SKU DETAILS START -->([\s\S]*?)<!-- SKU DETAILS END -->", body)
    if details_m:
        details = details_m.group(1)
        detail_field_map = {
            "vintage": "vintage", "country": "country", "region": "region",
            "appellation": "appellation", "variety": "grape", "color": "color",
            "farming practice": "farming", "soil": "soil", "vineyard": "vineyard",
        }
        for pm in re.finditer(
            r'<div class="list-label[^"]*">([^<]+)</div>\s*\n?\s*<div class="list-desc[^"]*">(?:<a[^>]*>)?([^<]+)(?:</a>)?</div>',
            details,
        ):
            label = pm.group(1).replace(":", "").strip().lower()
            value = decode_entities(pm.group(2).strip())
            key = detail_field_map.get(label)
            if key:
                detail[key] = value
            else:
                detail.setdefault("extra_fields", {})[label] = value

        gd = re.search(r"grape[^>]*>([^<]*\d+%[^<]*)", details, re.IGNORECASE)
        if gd:
            detail["grape_detail"] = decode_entities(gd.group(1).strip())

        abv_m = re.search(r"(\d{1,2}(?:\.\d+)?)\s*%\s*(?:ABV|alc)", details, re.IGNORECASE)
        if abv_m:
            detail["abv"] = abv_m.group(1)

        cases_m = re.search(r"(\d[\d,]+)\s*cases?\s*produced", details, re.IGNORECASE)
        if cases_m:
            detail["production"] = cases_m.group(1).replace(",", "")

        sku_m = re.search(r"<td>([A-Z]{2}-[A-Z]+-[\w-]+)</td>", details)
        if sku_m:
            detail["sku"] = sku_m.group(1)

        fmt_m = re.search(r"<td>(\d+/\d+ml)</td>", details)
        if fmt_m:
            detail["bottle_format"] = fmt_m.group(1)

    # Bullet list (winemaking notes)
    content_m = re.search(r"<!-- POST CONTENT START -->([\s\S]*?)<!-- POST CONTENT END -->", body)
    if content_m:
        content = content_m.group(1)
        bullets = [clean_text(m.group(1)) for m in re.finditer(r"<li>([\s\S]*?)</li>", content)]
        if bullets:
            detail["notes"] = bullets
        paras = [clean_text(m.group(1)) for m in re.finditer(r"<p>([\s\S]*?)</p>", content)]
        paras = [p for p in paras if len(p) > 10]
        if paras:
            detail["description"] = "\n".join(paras)

    # Scores
    scores: list[dict] = []
    for rs in re.finditer(
        r"tr_section_header[^>]*>([\s\S]*?)</div>\s*[\s\S]*?tr_section_content[^>]*>([\s\S]*?)</div>",
        body,
    ):
        header = clean_text(rs.group(1))
        content = clean_text(rs.group(2))
        sm = re.match(r"(.+?)\s+(\d{2,3})(?:\s*[-\u2013]\s*(\d{2,3}))?\s*(?:pts?|points?)?$", header, re.IGNORECASE)
        if sm:
            entry: dict = {"publication": sm.group(1).strip(), "score": int(sm.group(2))}
            if sm.group(3):
                entry["score_high"] = int(sm.group(3))
            if len(content) > 10:
                entry["note"] = content
            dw = re.search(r"(?:drink|drinking)\s+(\d{4})\s*[-\u2013]\s*(\d{4})", content, re.IGNORECASE)
            if dw:
                entry["drinking_window_start"] = dw.group(1)
                entry["drinking_window_end"] = dw.group(2)
            scores.append(entry)
    if scores:
        detail["scores"] = scores

    pdf_m = re.search(r'href="([^"]+\.pdf)"', body, re.IGNORECASE)
    if pdf_m:
        detail["tech_sheet_url"] = pdf_m.group(1)

    detail["_detail_scraped"] = True
    return detail


def fetch_phase2(client: httpx.Client, wines: list[dict], start_idx: int, limit: float):
    cap = int(min(limit, len(wines)))
    print(f"\nPhase 2: Scraping {cap} detail pages (delay: {DETAIL_DELAY_S}s)...")

    enriched = 0
    skipped = 0
    error_count = 0

    for i in range(start_idx, cap):
        wine = wines[i]
        if wine.get("_detail_scraped"):
            skipped += 1
            continue
        try:
            resp = client.get(wine["url"])
            if resp.status_code != 200:
                print(f"  [{i+1}/{cap}] {resp.status_code}: {wine['url']}")
                error_count += 1
                continue
            wines[i] = parse_detail_page(resp.text, wine)
            enriched += 1
            if (i + 1) % 50 == 0:
                print(f"  [{i+1}/{cap}] enriched: {enriched}, skipped: {skipped}, errors: {error_count}")
                OUTPUT_FILE.write_text(json.dumps(wines, indent=2, ensure_ascii=False))
                PROGRESS_FILE.write_text(json.dumps({"phase": 2, "lastIndex": i}))
        except Exception as e:
            print(f"  [{i+1}/{cap}] Error: {e} -- {wine.get('url', '?')}")
            error_count += 1
        time.sleep(DETAIL_DELAY_S)

    OUTPUT_FILE.write_text(json.dumps(wines, indent=2, ensure_ascii=False))
    print(f"  Phase 2 complete: {enriched} enriched, {skipped} already done, {error_count} errors")


def main():
    parser = argparse.ArgumentParser(description="Fetch Skurnik catalog")
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--resume", action="store_true")
    parser.add_argument("--phase1", action="store_true", help="Phase 1 only")
    parser.add_argument("--phase2", action="store_true", help="Phase 2 only")
    args = parser.parse_args()
    limit = args.limit or float("inf")

    wines: list[dict] = []
    phase1_start = 1
    phase2_start = 0

    if args.resume and PROGRESS_FILE.exists():
        progress = json.loads(PROGRESS_FILE.read_text())
        if OUTPUT_FILE.exists():
            wines = json.loads(OUTPUT_FILE.read_text())
        if progress.get("phase") == 1:
            phase1_start = progress["page"] + 1
            print(f"Resuming Phase 1 from page {phase1_start} ({len(wines)} wines so far)")
        elif progress.get("phase") == 2:
            phase2_start = (progress.get("lastIndex") or 0) + 1
            print(f"Resuming Phase 2 from index {phase2_start} ({len(wines)} wines)")

    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)

    with httpx.Client(timeout=20.0, headers=HEADERS, follow_redirects=True) as client:
        # Phase 1
        if not args.phase2:
            if args.resume and PROGRESS_FILE.exists():
                progress = json.loads(PROGRESS_FILE.read_text())
                if progress.get("phase") == 2:
                    print("Phase 1 already complete, skipping to Phase 2")
                else:
                    wines = fetch_phase1(client, wines, phase1_start, limit)
            else:
                wines = fetch_phase1(client, [], 1, limit)
        else:
            if OUTPUT_FILE.exists():
                wines = json.loads(OUTPUT_FILE.read_text())
                print(f"Loaded {len(wines)} wines from Phase 1")
            else:
                print("No catalog file found. Run Phase 1 first.")
                sys.exit(1)

        # Phase 2
        if not args.phase1 and wines:
            PROGRESS_FILE.write_text(json.dumps({"phase": 2, "lastIndex": phase2_start - 1}))
            fetch_phase2(client, wines, phase2_start, limit)

    print(f"\nDone. {len(wines)} wines saved to {OUTPUT_FILE}")

    if wines:
        print("\nSample wine:")
        print(json.dumps(wines[0], indent=2, ensure_ascii=False))

    total = len(wines)
    if total:
        fields = [
            ("withProducer", "producer"), ("withVintage", "vintage"), ("withGrape", "grape"),
            ("withAppellation", "appellation"), ("withRegion", "region"), ("withCountry", "country"),
            ("withFarming", "farming"), ("withSoil", "soil"), ("withAbv", "abv"),
        ]
        print("\nField coverage:")
        print(f"  total: {total} (100.0%)")
        for label, key in fields:
            n = sum(1 for w in wines if w.get(key))
            print(f"  {label}: {n} ({n/total*100:.1f}%)")
        n_scores = sum(1 for w in wines if w.get("scores"))
        n_notes = sum(1 for w in wines if w.get("notes"))
        n_desc = sum(1 for w in wines if w.get("description"))
        n_detail = sum(1 for w in wines if w.get("_detail_scraped"))
        print(f"  withScores: {n_scores} ({n_scores/total*100:.1f}%)")
        print(f"  withNotes: {n_notes} ({n_notes/total*100:.1f}%)")
        print(f"  withDescription: {n_desc} ({n_desc/total*100:.1f}%)")
        print(f"  withDetailScraped: {n_detail} ({n_detail/total*100:.1f}%)")

        country_dist: dict[str, int] = {}
        for w in wines:
            c = w.get("country", "unknown")
            country_dist[c] = country_dist.get(c, 0) + 1
        print("\nCountry distribution (top 10):")
        for k, v in sorted(country_dist.items(), key=lambda x: -x[1])[:10]:
            print(f"  {k}: {v}")

    if not args.phase1 and PROGRESS_FILE.exists():
        PROGRESS_FILE.unlink()


if __name__ == "__main__":
    main()
