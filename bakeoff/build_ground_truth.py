#!/usr/bin/env python3
"""B5.2 Task 2: Build ground truth JSON from fetched HTML pages.

Extracts visible text from each page, then uses rule-based parsing
to identify structured wine data (ABV, blend, oak, chemistry, etc).

Usage:
    python -m bakeoff.build_ground_truth
"""

import json
import os
import re
import sys
from pathlib import Path

os.environ["PYTHONUNBUFFERED"] = "1"

from bs4 import BeautifulSoup

DATA_DIR = Path(__file__).parent / "data" / "task2"


def extract_text(html: str) -> str:
    """Get visible text from HTML."""
    soup = BeautifulSoup(html, "html.parser")
    # Remove script/style
    for tag in soup(["script", "style", "noscript"]):
        tag.decompose()
    return soup.get_text(separator="\n", strip=True)


def find_abv(text: str) -> float | None:
    """Find ABV/alcohol percentage."""
    patterns = [
        r"(?:ABV|Alcohol|Alc)[:\s]*(\d{1,2}\.?\d*)\s*%",
        r"(\d{1,2}\.\d)\s*%\s*(?:ABV|Alcohol|Alc|by volume)",
        r"(\d{1,2}\.\d)\s*%\s*(?:vol|alcohol)",
    ]
    for p in patterns:
        m = re.search(p, text, re.IGNORECASE)
        if m:
            val = float(m.group(1))
            if 5.0 <= val <= 20.0:  # Sanity check
                return val
    return None


def find_blend(text: str) -> list | None:
    """Find blend percentages."""
    # Pattern: 75% Cabernet Sauvignon, 20% Merlot
    blend_pattern = r"(\d{1,3})\s*%\s*([A-Z][a-zéèêà]+(?:\s+[A-Z][a-zéèêà]+)*)"
    matches = re.findall(blend_pattern, text)
    if matches and len(matches) >= 2:
        blend = []
        for pct, grape in matches:
            pct_val = int(pct)
            if 1 <= pct_val <= 100:
                blend.append({"grape": grape.strip(), "pct": pct_val})
        if blend and sum(b["pct"] for b in blend) >= 90:
            return blend
    return None


def find_oak(text: str) -> dict | None:
    """Find oak aging details."""
    oak_info = {}

    # Months in oak/barrel
    m = re.search(r"(\d{1,3})\s*months?\s*(?:in|of)\s*(?:French|American|oak|barrel)",
                  text, re.IGNORECASE)
    if m:
        oak_info["months"] = int(m.group(1))

    # New oak percentage
    m = re.search(r"(\d{1,3})\s*%\s*(?:new)\s*(?:French|American|oak|barrel)",
                  text, re.IGNORECASE)
    if not m:
        m = re.search(r"(?:new)\s*(?:French|American|oak|barrel)[:\s]*(\d{1,3})\s*%",
                      text, re.IGNORECASE)
    if m:
        oak_info["new_pct"] = int(m.group(1))

    # Vessel type
    if re.search(r"French\s+oak", text, re.IGNORECASE):
        oak_info["origin"] = "French"
    elif re.search(r"American\s+oak", text, re.IGNORECASE):
        oak_info["origin"] = "American"
    elif re.search(r"Hungarian\s+oak", text, re.IGNORECASE):
        oak_info["origin"] = "Hungarian"

    # Barrel type
    if re.search(r"barrique", text, re.IGNORECASE):
        oak_info["vessel"] = "barrique"
    elif re.search(r"barrel", text, re.IGNORECASE):
        oak_info["vessel"] = "barrel"
    elif re.search(r"stainless", text, re.IGNORECASE):
        oak_info["vessel"] = "stainless steel"

    return oak_info if oak_info else None


def find_chemistry(text: str) -> dict | None:
    """Find chemistry values (pH, TA, RS)."""
    chem = {}

    # pH
    m = re.search(r"pH[:\s]*(\d\.\d{1,2})", text, re.IGNORECASE)
    if m:
        val = float(m.group(1))
        if 2.5 <= val <= 4.5:
            chem["ph"] = val

    # Total acidity
    m = re.search(r"(?:TA|Total\s*Acid(?:ity)?)[:\s]*(\d\.?\d*)\s*(?:g/[Ll]|g/100ml)",
                  text, re.IGNORECASE)
    if m:
        chem["ta_g_l"] = float(m.group(1))

    # Residual sugar
    m = re.search(r"(?:RS|Residual\s*Sugar)[:\s]*(\d\.?\d*)\s*(?:g/[Ll]|g/100ml)",
                  text, re.IGNORECASE)
    if m:
        chem["rs_g_l"] = float(m.group(1))

    return chem if chem else None


def find_cases(text: str) -> int | None:
    """Find cases produced."""
    patterns = [
        r"(\d[\d,]*)\s*cases?\s*(?:produced|made|bottled)",
        r"(?:Production|Cases)[:\s]*(\d[\d,]*)\s*cases?",
    ]
    for p in patterns:
        m = re.search(p, text, re.IGNORECASE)
        if m:
            return int(m.group(1).replace(",", ""))
    return None


def find_vintage(text: str) -> int | None:
    """Find vintage year."""
    # Look for 4-digit year in wine name context
    m = re.search(r"(?:vintage|harvest)\s*(?:year)?[:\s]*((?:19|20)\d{2})", text, re.IGNORECASE)
    if m:
        return int(m.group(1))
    # Year near top of page
    years = re.findall(r"\b(20[012]\d)\b", text[:2000])
    if years:
        # Return most common year in first 2000 chars
        from collections import Counter
        c = Counter(years)
        return int(c.most_common(1)[0][0])
    return None


def find_winemaker(text: str) -> str | None:
    """Find winemaker name."""
    patterns = [
        r"(?:Winemaker|Winemaking\s*Team|Head\s*Winemaker)[:\s]*([A-Z][a-z]+(?:\s+[A-Z][a-z]+){1,3})",
        r"(?:made\s+by|crafted\s+by)[:\s]*([A-Z][a-z]+(?:\s+[A-Z][a-z]+){1,3})",
    ]
    for p in patterns:
        m = re.search(p, text, re.IGNORECASE)
        if m:
            return m.group(1).strip()
    return None


def find_farming(text: str) -> str | None:
    """Find farming practice."""
    lower = text.lower()
    if "biodynamic" in lower:
        return "biodynamic"
    if "organic" in lower and ("certified organic" in lower or "organically grown" in lower
                                or "organic grapes" in lower or "usda organic" in lower):
        return "organic"
    if "sustainab" in lower:
        return "sustainable"
    return None


def build_ground_truth(page_id: str, meta: dict, html: str) -> dict:
    """Build ground truth JSON for a single page."""
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
        "winemaker": find_winemaker(text),
        "farming": find_farming(text),
    }

    # Count non-null fields for data richness scoring
    data_fields = ["vintage_year", "abv_pct", "blend", "oak", "cases_produced",
                   "chemistry", "winemaker", "farming"]
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
    manifest_file = DATA_DIR / "manifest_final.json"
    if not manifest_file.exists():
        print("ERROR: manifest_final.json not found. Run build_task2.py --fetch first.")
        return

    with open(manifest_file) as f:
        manifest = json.load(f)

    print(f"Building ground truth for {len(manifest)} pages...\n")

    all_truths = []
    richness_counts = {0: 0, 1: 0, 2: 0, 3: 0, 4: 0}

    for meta in manifest:
        page_id = meta["page_id"]
        # Try final_* first, then original
        html_file = DATA_DIR / f"final_{page_id}.html"
        if not html_file.exists():
            html_file = DATA_DIR / f"{meta.get('original_page_id', page_id)}.html"
        if not html_file.exists():
            print(f"  SKIP {page_id}: HTML file not found")
            continue

        with open(html_file, "r", encoding="utf-8") as f:
            html = f.read()

        truth = build_ground_truth(page_id, meta, html)
        all_truths.append(truth)

        richness = truth["data_richness"]
        bucket = min(richness, 4)
        richness_counts[bucket] = richness_counts.get(bucket, 0) + 1

        ext = truth["extraction"]
        fields = []
        if ext["abv_pct"]:
            fields.append(f"ABV={ext['abv_pct']}")
        if ext["blend"]:
            fields.append(f"blend={len(ext['blend'])} grapes")
        if ext["oak"]:
            fields.append(f"oak={ext['oak']}")
        if ext["chemistry"]:
            fields.append(f"chem={ext['chemistry']}")
        if ext["cases_produced"]:
            fields.append(f"cases={ext['cases_produced']}")
        if ext["farming"]:
            fields.append(f"farm={ext['farming']}")
        if ext["winemaker"]:
            fields.append(f"wmaker={ext['winemaker']}")

        status = "RICH" if richness >= 3 else "MOD" if richness >= 1 else "THIN"
        print(f"  [{page_id}] {status} ({richness}/{truth['total_fields']}) "
              f"{meta['producer']} — {meta['wine']}")
        if fields:
            print(f"           {', '.join(fields)}")

    # Save all ground truth
    for truth in all_truths:
        page_id = truth["page_id"]
        with open(DATA_DIR / f"truth_{page_id}.json", "w", encoding="utf-8") as f:
            json.dump(truth, f, indent=2, ensure_ascii=False)

    # Save summary
    with open(DATA_DIR / "ground_truth_summary.json", "w") as f:
        json.dump({
            "total_pages": len(all_truths),
            "richness_distribution": richness_counts,
            "avg_richness": sum(t["data_richness"] for t in all_truths) / len(all_truths) if all_truths else 0,
            "pages": [{
                "page_id": t["page_id"],
                "producer": t["producer"],
                "wine": t["wine_name"],
                "data_richness": t["data_richness"],
            } for t in all_truths]
        }, f, indent=2)

    print(f"\n  Summary:")
    print(f"    Total: {len(all_truths)} ground truth files")
    print(f"    Richness: {richness_counts}")
    print(f"    Avg fields: {sum(t['data_richness'] for t in all_truths) / len(all_truths):.1f}")


if __name__ == "__main__":
    main()
