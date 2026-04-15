#!/usr/bin/env python3
"""Scrape 10 rich tech sheet pages to supplement the Task 2 test set.

Targets producers known for publishing detailed technical data:
ABV, blend percentages, chemistry (pH, TA), oak program, cases produced.
"""

import json
import os
import re
import time
from html.parser import HTMLParser
from pathlib import Path

import requests

DATA_DIR = Path("bakeoff/data/task2")

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
}

# 10 pages from producers with rich, server-rendered tech sheets
PAGES = [
    # Tablas Creek — famous for complete tech sheets
    ("Tablas Creek", "Esprit de Tablas", "https://tablascreek.com/wines/2023_esprit_de_tablas"),
    ("Tablas Creek", "Mourvedre", "https://tablascreek.com/wines/2023_mourvedre"),
    ("Tablas Creek", "Patelin de Tablas", "https://tablascreek.com/wines/2023_patelin_de_tablas"),
    ("Tablas Creek", "Dianthus Rose", "https://tablascreek.com/wines/2025_dianthus"),
    # Ridge — already have Lytton Springs; get Three Valleys
    ("Ridge Vineyards", "Three Valleys", "https://www.ridgewine.com/wines/three-valleys/"),
    # J. Lohr — detailed tech sheets
    ("J. Lohr", "Hilltop Cabernet Sauvignon", "https://www.jlohr.com/wines/2023-hilltop-cabernet-sauvignon"),
    ("J. Lohr", "Arroyo Vista Chardonnay", "https://www.jlohr.com/wines/2024-arroyo-vista-chardonnay"),
    ("J. Lohr", "Seven Oaks Cabernet Sauvignon", "https://www.jlohr.com/wines/2023-seven-oaks-cabernet-sauvignon"),
    # Bonny Doon — Randall Grahm publishes everything
    ("Bonny Doon", "Le Cigare Volant", "https://www.bonnydoonvineyard.com/product/2021-le-cigare-volant/"),
    # Cline Family — detailed tech notes
    ("Cline Family Cellars", "Ancient Vines Zinfandel", "https://www.clinecellars.com/wines/Ancient-Vines-Zinfandel"),
]


class TextExtractor(HTMLParser):
    def __init__(self):
        super().__init__()
        self.text = []
        self.skip = False

    def handle_starttag(self, tag, attrs):
        if tag in ("script", "style", "noscript"):
            self.skip = True

    def handle_endtag(self, tag):
        if tag in ("script", "style", "noscript"):
            self.skip = False

    def handle_data(self, data):
        if not self.skip:
            t = data.strip()
            if t:
                self.text.append(t)


def extract_text(html):
    ext = TextExtractor()
    ext.feed(html)
    return " ".join(ext.text)


def check_richness(text):
    """Check what structured data is present in the text."""
    found = {}
    # ABV
    m = re.search(r"(\d{1,2}\.?\d?)\s*%?\s*(?:ABV|[Aa]lcohol|[Aa]lc\.?\s+[Bb]y)", text)
    if m:
        found["abv"] = m.group(1)
    # pH
    m = re.search(r"(?:pH|ph)\s*[:=]?\s*(\d\.\d{1,2})", text, re.I)
    if m:
        found["ph"] = m.group(1)
    # TA
    m = re.search(r"(?:TA|[Tt]otal [Aa]cid|[Aa]cidity)\s*[:=]?\s*(\d+\.?\d*)", text)
    if m:
        found["ta"] = m.group(1)
    # Cases
    m = re.search(r"(\d[\d,]+)\s*(?:[Cc]ases|cs)\b", text)
    if m:
        found["cases"] = m.group(1)
    # Blend percentages
    pcts = re.findall(r"(\d{1,3})\s*%\s*([A-Z][a-z]+(?:\s+[A-Z][a-z]+)?)", text)
    if pcts:
        found["blend"] = pcts[:6]
    # Oak
    if re.search(r"(?:oak|barrel|foudre|barrique)", text, re.I):
        found["oak"] = True
    return found


def main():
    # Start numbering after existing 50 pages
    start_num = 51
    results = []

    for i, (producer, wine, url) in enumerate(PAGES):
        page_num = start_num + i
        page_id = f"page_{page_num:03d}"
        print(f"\n[{i+1}/{len(PAGES)}] {producer} - {wine}")
        print(f"  URL: {url}")

        try:
            resp = requests.get(url, headers=headers, timeout=30)
            print(f"  Status: {resp.status_code}, Size: {len(resp.text):,}")

            if resp.status_code != 200:
                print(f"  FAILED: HTTP {resp.status_code}")
                results.append({"page_id": page_id, "producer": producer, "wine": wine,
                                "url": url, "status": f"fail_{resp.status_code}"})
                continue

            html = resp.text
            text = extract_text(html)
            richness = check_richness(text)
            print(f"  Text: {len(text):,} chars")
            print(f"  Found: {richness}")

            if not richness:
                print(f"  SKIP: No structured data found (SPA or marketing-only)")
                results.append({"page_id": page_id, "producer": producer, "wine": wine,
                                "url": url, "status": "skip_no_data"})
                continue

            # Save HTML
            html_file = DATA_DIR / f"final_page_{page_num:03d}.html"
            html_file.write_text(html, encoding="utf-8")

            # Save meta
            meta = {
                "page_id": page_id, "producer": producer, "wine": wine,
                "url": url, "final_url": resp.url,
                "status_code": resp.status_code, "content_length": len(html),
            }
            meta_file = DATA_DIR / f"final_page_{page_num:03d}_meta.json"
            meta_file.write_text(json.dumps(meta, indent=2), encoding="utf-8")

            # Build ground truth from the text (Opus inline extraction)
            # Parse ABV
            abv = float(richness["abv"]) if "abv" in richness else None

            # Parse blend
            blend = None
            if "blend" in richness:
                blend = [{"grape": g, "pct": int(p)} for p, g in richness["blend"]]

            # Parse chemistry
            chem = None
            ph = float(richness["ph"]) if "ph" in richness else None
            ta = float(richness["ta"]) if "ta" in richness else None
            if ph or ta:
                chem = {"ph": ph, "ta_g_l": ta, "rs_g_l": None, "so2_total_mg_l": None}

            # Parse cases
            cases = None
            if "cases" in richness:
                cases = int(richness["cases"].replace(",", ""))

            # Farming
            farming = None
            text_lower = text.lower()
            if "biodynamic" in text_lower:
                farming = "biodynamic"
            elif "organic" in text_lower:
                farming = "organic"
            elif "sustainable" in text_lower or "sustainability" in text_lower:
                farming = "sustainable"

            # Oak details (basic)
            oak = None
            if richness.get("oak"):
                oak_text = text_lower
                vessel = None
                if "foudre" in oak_text:
                    vessel = "foudre"
                elif "barrel" in oak_text or "barrique" in oak_text:
                    vessel = "barrel"
                origin = None
                if "french oak" in oak_text:
                    origin = "French"
                elif "american oak" in oak_text:
                    origin = "American"
                months = None
                m = re.search(r"(\d{1,2})\s*months?\s*(?:in|of)?\s*(?:oak|barrel|aging|ageing|foudre)", text, re.I)
                if m:
                    months = int(m.group(1))
                new_pct = None
                m = re.search(r"(\d{1,3})\s*%\s*new\s*(?:oak|barrel|french|american)", text, re.I)
                if m:
                    new_pct = float(m.group(1))
                if vessel or origin or months or new_pct:
                    oak = {"vessel": vessel, "months": months, "new_pct": new_pct, "origin": origin}

            # Winemaker
            winemaker = None
            m = re.search(r"(?:[Ww]inemaker|[Ww]ine\s*[Mm]aker)\s*[:=]?\s*([A-Z][a-z]+(?:\s+[A-Z][a-z]+)+)", text)
            if m:
                winemaker = m.group(1)

            # Vintage year
            vintage = None
            m = re.search(r"\b(20[12]\d)\b", wine) or re.search(r"\b(20[12]\d)\b", text[:200])
            if m:
                vintage = int(m.group(1))

            truth = {
                "page_id": page_id,
                "url": url,
                "producer": producer,
                "wine_name": wine,
                "extraction": {
                    "name": wine,
                    "producer": producer,
                    "vintage_year": vintage,
                    "abv_pct": abv,
                    "blend": blend,
                    "oak": oak,
                    "cases_produced": cases,
                    "chemistry": chem,
                    "winemaker": winemaker,
                    "farming": farming,
                },
                "data_richness": sum(1 for v in [vintage, abv, blend, oak, cases, chem, winemaker, farming] if v is not None),
                "total_fields": 8,
                "text_length": len(text),
            }
            truth_file = DATA_DIR / f"truth_{page_id}.json"
            truth_file.write_text(json.dumps(truth, indent=2, ensure_ascii=False), encoding="utf-8")

            print(f"  Saved: {html_file.name} + truth ({truth['data_richness']} fields)")
            results.append({"page_id": page_id, "producer": producer, "wine": wine,
                            "url": url, "status": "ok", "richness": truth["data_richness"],
                            "fields": richness})

        except Exception as e:
            print(f"  ERROR: {e}")
            results.append({"page_id": page_id, "producer": producer, "wine": wine,
                            "url": url, "status": f"error_{type(e).__name__}"})

        time.sleep(1)

    # Summary
    ok = [r for r in results if r["status"] == "ok"]
    failed = [r for r in results if r["status"] != "ok"]
    print(f"\n\n=== SUMMARY ===")
    print(f"Scraped: {len(ok)} pages, Failed: {len(failed)}")
    for r in ok:
        print(f"  [{r['richness']}] {r['producer']} - {r['wine']}: {list(r['fields'].keys())}")
    for r in failed:
        print(f"  FAIL: {r['producer']} - {r['wine']}: {r['status']}")


if __name__ == "__main__":
    main()
