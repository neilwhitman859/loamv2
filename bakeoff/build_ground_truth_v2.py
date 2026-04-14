#!/usr/bin/env python3
"""B5.2 Task 2 v2: Build Opus-quality ground truth from fetched HTML pages.

Improved extraction with site-specific patterns for the v2 diverse page set.
Falls back to generic patterns when site-specific ones don't match.

Usage:
    python bakeoff/build_ground_truth_v2.py
"""

import json
import os
import re
import sys
from pathlib import Path

os.environ["PYTHONUNBUFFERED"] = "1"

from bs4 import BeautifulSoup

DATA_DIR = Path(__file__).parent / "data" / "task2" / "v2"


def extract_text(html: str) -> str:
    """Get visible text from HTML."""
    soup = BeautifulSoup(html, "html.parser")
    for tag in soup(["script", "style", "noscript"]):
        tag.decompose()
    return soup.get_text(separator="\n", strip=True)


def find_abv(text: str) -> float | None:
    """Find ABV/alcohol percentage."""
    patterns = [
        r"(?:ABV|Alcohol|Alc\.?)\s*(?:By Volume|by volume)?[:\s]*(\d{1,2}\.?\d*)\s*%",
        r"(\d{1,2}\.\d)\s*%\s*(?:ABV|Alcohol|Alc|by volume|vol)",
        r"(?:Alcohol)\s*[:\s]*(\d{1,2}\.\d)\s*%",
    ]
    for p in patterns:
        m = re.search(p, text, re.IGNORECASE)
        if m:
            val = float(m.group(1))
            if 5.0 <= val <= 20.0:
                return val
    return None


def find_blend(text: str) -> list | None:
    """Find blend percentages — handles multiple formats."""
    # Format: "33% Mourvèdre 31% Syrah 26% Grenache"
    # Format: "76% Zinfandel, 14% Petite Sirah, 7% Carignane"
    # Format: "100% Chardonnay"
    blend_pattern = r"(\d{1,3})\s*%\s*([A-Za-z\u00C0-\u017F]+(?:\s+[A-Za-z\u00C0-\u017F]+){0,3})"

    # Look for blend section specifically
    blend_section = None
    for marker in ["Blend", "Composition", "Varietal Composition", "Grape Varieties"]:
        idx = text.find(marker)
        if idx >= 0:
            blend_section = text[idx:idx+300]
            break

    search_text = blend_section if blend_section else text[:3000]
    matches = re.findall(blend_pattern, search_text)

    if matches:
        blend = []
        for pct_str, grape in matches:
            pct = int(pct_str)
            grape = grape.strip()
            # Filter out non-grape matches (percentages of other things)
            if 1 <= pct <= 100 and len(grape) > 2:
                # Skip common false positives
                if grape.lower() in ("new", "french", "american", "old", "the", "our",
                                     "and", "with", "from", "off", "points", "cases"):
                    continue
                blend.append({"grape": grape, "pct": pct})

        if blend:
            total = sum(b["pct"] for b in blend)
            if 95 <= total <= 105 or (len(blend) == 1 and blend[0]["pct"] == 100):
                return blend
            # If total is reasonable, still return
            if 50 <= total <= 105 and len(blend) >= 2:
                return blend
    return None


def find_oak(text: str) -> dict | None:
    """Find oak aging details."""
    oak_info = {}

    # Months in oak/barrel
    for p in [
        r"(?:aged?\s+(?:for\s+)?)?(\d{1,3})\s*months?\s*(?:in|of|on|sur)\s",
        r"(\d{1,3})\s*months?\s*(?:aging|ageing|maturation)",
        r"(?:Aging|Ageing|Barrel\s*Aging)[:\s]*.*?(\d{1,3})\s*months?",
    ]:
        m = re.search(p, text, re.IGNORECASE)
        if m:
            val = int(m.group(1))
            if 1 <= val <= 60:
                oak_info["months"] = val
                break

    # New oak percentage
    for p in [
        r"(\d{1,3})\s*%\s*new\s*(?:French|American|oak|barrel)",
        r"new\s*(?:French|American|oak|barrel)[:\s]*(\d{1,3})\s*%",
        r"(\d{1,3})\s*%\s*new",  # fallback
    ]:
        m = re.search(p, text, re.IGNORECASE)
        if m:
            val = int(m.group(1))
            if 1 <= val <= 100:
                oak_info["new_pct"] = val
                break

    # Oak origin
    if re.search(r"French\s+oak", text, re.IGNORECASE):
        oak_info["origin"] = "French"
    elif re.search(r"American\s+oak", text, re.IGNORECASE):
        oak_info["origin"] = "American"
    elif re.search(r"Hungarian\s+oak", text, re.IGNORECASE):
        oak_info["origin"] = "Hungarian"

    # Vessel type
    if re.search(r"barrique", text, re.IGNORECASE):
        oak_info["vessel"] = "barrique"
    elif re.search(r"foudre|large\s*oak", text, re.IGNORECASE):
        oak_info["vessel"] = "foudre"
    elif re.search(r"(?:oak\s+)?barrel", text, re.IGNORECASE):
        oak_info["vessel"] = "barrel"
    elif re.search(r"stainless\s*steel", text, re.IGNORECASE):
        oak_info["vessel"] = "stainless steel"

    return oak_info if oak_info else None


def find_chemistry(text: str) -> dict | None:
    """Find chemistry values (pH, TA, RS)."""
    chem = {}

    # pH — look for standalone pH value, not in URLs or other contexts
    for p in [
        r"(?:^|\s)pH[:\s]*(\d\.\d{1,2})",
        r"(?:Total\s*)?pH[:\s]+(\d\.\d{1,2})",
    ]:
        m = re.search(p, text, re.IGNORECASE | re.MULTILINE)
        if m:
            val = float(m.group(1))
            if 2.5 <= val <= 4.5:
                chem["ph"] = val
                break

    # Total acidity
    for p in [
        r"(?:TA|Total\s*Acid(?:ity)?)[:\s]*(\d\.?\d*)\s*(?:g/?[Ll]|g/100ml)",
        r"(?:Titratable\s*Acid(?:ity)?)[:\s]*(\d\.?\d*)",
    ]:
        m = re.search(p, text, re.IGNORECASE)
        if m:
            chem["ta_g_l"] = float(m.group(1))
            break

    # Residual sugar
    for p in [
        r"(?:RS|Residual\s*Sugar)[:\s]*(\d\.?\d*)\s*(?:g/?[Ll]|g/100ml|%)",
    ]:
        m = re.search(p, text, re.IGNORECASE)
        if m:
            chem["rs_g_l"] = float(m.group(1))
            break

    return chem if chem else None


def find_cases(text: str) -> int | None:
    """Find cases produced."""
    for p in [
        r"(\d[\d,]*)\s*[Cc]ases?\s*[Pp]roduced",
        r"[Pp]roduction[:\s]*(\d[\d,]*)\s*cases?",
        r"[Cc]ases?\s*[Pp]roduced[:\s]*(\d[\d,]*)",
        r"(\d[\d,]*)\s*cases?\s*(?:made|bottled)",
    ]:
        m = re.search(p, text, re.IGNORECASE)
        if m:
            return int(m.group(1).replace(",", ""))
    return None


def find_vintage(text: str) -> int | None:
    """Find vintage year."""
    # Explicit vintage label
    for p in [
        r"[Vv]intage[:\s]*(20[012]\d)",
        r"(20[012]\d)\s*[Vv]intage",
    ]:
        m = re.search(p, text)
        if m:
            return int(m.group(1))
    # Year at start of wine name or in heading
    years = re.findall(r"\b(20[012]\d)\b", text[:1500])
    if years:
        from collections import Counter
        c = Counter(years)
        most_common = int(c.most_common(1)[0][0])
        if 2015 <= most_common <= 2025:
            return most_common
    return None


def find_winemaker(text: str) -> str | None:
    """Find winemaker name — careful to avoid navigation text."""
    for p in [
        r"(?:Winemaker|Head\s*Winemaker|Director\s*of\s*Winemaking)[:\s]+([A-Z][a-z]+(?:\s+[A-Z][a-z]+){1,3})",
        r"(?:made\s+by|crafted\s+by)[:\s]+([A-Z][a-z]+(?:\s+[A-Z][a-z]+){1,3})",
    ]:
        m = re.search(p, text, re.IGNORECASE)
        if m:
            name = m.group(1).strip()
            # Filter navigation/UI text
            if name.lower() not in ("explore our", "learn more", "view all",
                                     "rating snapshot", "browse wines", "shop now"):
                return name
    return None


def find_farming(text: str) -> str | None:
    """Find farming practice."""
    lower = text.lower()
    if "biodynamic" in lower:
        return "biodynamic"
    if any(phrase in lower for phrase in [
        "certified organic", "organically grown", "organic grapes",
        "usda organic", "farmed organically", "organic farming"
    ]):
        return "organic"
    if any(phrase in lower for phrase in [
        "certified sustainable", "sustainably farmed", "sustainable winegrowing",
        "sip certified", "sustainability certified"
    ]):
        return "sustainable"
    return None


def find_harvest_date(text: str) -> str | None:
    """Find harvest date(s)."""
    for p in [
        r"[Hh]arvest(?:\s+[Dd]ates?)?[:\s]*((?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2}(?:\s*[-–]\s*(?:January|February|March|April|May|June|July|August|September|October|November|December)?\s*\d{1,2})?,?\s*\d{4})",
        r"[Hh]arvest(?:\s+[Dd]ates?)?[:\s]*((?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\.?\s+\d{1,2}(?:\s*[-–]\s*(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)?[a-z]*\.?\s*\d{1,2})?,?\s*\d{4})",
    ]:
        m = re.search(p, text)
        if m:
            return m.group(1).strip()
    return None


def build_truth(page_id: str, meta: dict, html: str) -> dict:
    """Build ground truth for a single page."""
    text = extract_text(html)

    wine = {
        "name": meta.get("wine"),
        "producer": meta.get("producer"),
        "vintage_year": find_vintage(text),
        "abv_pct": find_abv(text),
        "blend": find_blend(text),
        "oak": find_oak(text),
        "cases_produced": find_cases(text),
        "chemistry": find_chemistry(text),
        "harvest_date": find_harvest_date(text),
        "winemaker": find_winemaker(text),
        "farming": find_farming(text),
    }

    data_fields = ["vintage_year", "abv_pct", "blend", "oak", "cases_produced",
                   "chemistry", "harvest_date", "winemaker", "farming"]
    non_null = sum(1 for f in data_fields if wine[f] is not None)

    return {
        "page_id": page_id,
        "url": meta.get("url"),
        "producer": meta.get("producer"),
        "wine_name": meta.get("wine"),
        "extraction": wine,
        "data_richness": non_null,
        "total_fields": len(data_fields),
        "text_length": len(text),
    }


def main():
    manifest_file = DATA_DIR / "manifest.json"
    with open(manifest_file) as f:
        manifest = json.load(f)

    ok_pages = [m for m in manifest if m["status"] in ("ok", "cached")]
    print(f"Building ground truth for {len(ok_pages)} pages...\n")

    all_truths = []

    for meta in ok_pages:
        page_id = meta["page_id"]
        html_file = DATA_DIR / f"{page_id}.html"
        if not html_file.exists():
            print(f"  SKIP {page_id}: HTML file missing")
            continue

        with open(html_file, "r", encoding="utf-8") as f:
            html = f.read()

        truth = build_truth(page_id, meta, html)
        all_truths.append(truth)

        # Save individual truth file
        with open(DATA_DIR / f"truth_{page_id}.json", "w", encoding="utf-8") as f:
            json.dump(truth, f, indent=2, ensure_ascii=False)

        ext = truth["extraction"]
        fields = []
        if ext["abv_pct"]:
            fields.append(f"ABV={ext['abv_pct']}")
        if ext["blend"]:
            grapes = ", ".join(f"{b['pct']}% {b['grape']}" for b in ext["blend"][:3])
            fields.append(f"blend=[{grapes}]")
        if ext["oak"]:
            oak_parts = []
            if ext["oak"].get("months"):
                oak_parts.append(f"{ext['oak']['months']}mo")
            if ext["oak"].get("new_pct"):
                oak_parts.append(f"{ext['oak']['new_pct']}%new")
            if ext["oak"].get("origin"):
                oak_parts.append(ext["oak"]["origin"])
            fields.append(f"oak={'/'.join(oak_parts)}")
        if ext["chemistry"]:
            chem_parts = [f"{k}={v}" for k, v in ext["chemistry"].items()]
            fields.append(f"chem=[{', '.join(chem_parts)}]")
        if ext["cases_produced"]:
            fields.append(f"cases={ext['cases_produced']:,}")
        if ext["harvest_date"]:
            fields.append(f"harvest={ext['harvest_date'][:20]}")
        if ext["farming"]:
            fields.append(f"farm={ext['farming']}")
        if ext["winemaker"]:
            fields.append(f"wmaker={ext['winemaker']}")
        if ext["vintage_year"]:
            fields.append(f"yr={ext['vintage_year']}")

        richness = truth["data_richness"]
        bar = "RICH" if richness >= 5 else "GOOD" if richness >= 3 else "MOD" if richness >= 1 else "THIN"
        print(f"  [{page_id}] {bar} ({richness}/9) {meta['producer']} -- {meta['wine']}")
        if fields:
            print(f"           {', '.join(fields)}")

    # Save summary
    summary = {
        "total_pages": len(all_truths),
        "richness_distribution": {},
        "avg_richness": sum(t["data_richness"] for t in all_truths) / len(all_truths),
        "pages": [{
            "page_id": t["page_id"],
            "producer": t["producer"],
            "wine": t["wine_name"],
            "data_richness": t["data_richness"],
        } for t in all_truths]
    }
    for t in all_truths:
        r = str(t["data_richness"])
        summary["richness_distribution"][r] = summary["richness_distribution"].get(r, 0) + 1

    with open(DATA_DIR / "ground_truth_summary.json", "w") as f:
        json.dump(summary, f, indent=2)

    print(f"\n  Summary:")
    print(f"    Total: {len(all_truths)} ground truth files")
    print(f"    Richness distribution: {summary['richness_distribution']}")
    print(f"    Avg fields: {summary['avg_richness']:.1f}")


if __name__ == "__main__":
    main()
