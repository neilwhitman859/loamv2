#!/usr/bin/env python3
"""
Analyze Kansas active brands JSON data.

Usage:
    python -m pipeline.analyze.analyze_kansas
"""

import json
import sys
from collections import Counter
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

DATA_DIR = Path(__file__).resolve().parents[2] / "data" / "imports"

LABELS = {
    "a": "COLA Number", "b": "KS License", "c": "Brand Name", "d": "Fanciful Name",
    "e": "Type", "f": "ABV", "g": "unknown_g", "h": "Size", "i": "Unit", "j": "Vintage",
    "k": "Appellation", "l": "Expiration", "m": "unknown_m", "n": "Container",
    "o": "unknown_o", "p": "unknown_p", "q": "Distributor1", "r": "Distributor2",
}


def main():
    data = json.loads((DATA_DIR / "kansas_active_brands.json").read_text(encoding="utf-8"))
    print("=== KANSAS ACTIVE BRANDS ANALYSIS ===")
    print(f"Total records: {len(data)}")

    print("\n--- Field Fill Rates ---")
    for k in "abcdefghijklmnopqr":
        filled = sum(1 for r in data if r.get(k) and str(r[k]).strip())
        pct = filled / len(data) * 100
        print(f"  {k} ({LABELS.get(k, '?')}): {filled} ({pct:.1f}%)")

    print("\n--- Type Breakdown ---")
    types = Counter(r.get("e") for r in data)
    for k, v in types.most_common():
        print(f"  {k}: {v}")

    wines = [r for r in data if "Wine" in (r.get("e") or "") or "wine" in (r.get("e") or "")]
    print(f"\n--- Wine Records ---")
    print(f"Wine-type records: {len(wines)} of {len(data)} ({len(wines) / len(data) * 100:.1f}%)")

    wines_with_cola = [r for r in wines if r.get("a") and str(r["a"]).strip()]
    print(f"Wines with COLA number: {len(wines_with_cola)} ({len(wines_with_cola) / len(wines) * 100:.1f}%)")

    wines_with_vintage = [r for r in wines if r.get("j") and str(r["j"]).strip() and str(r["j"]).strip() != "0"]
    print(f"Wines with vintage: {len(wines_with_vintage)} ({len(wines_with_vintage) / len(wines) * 100:.1f}%)")

    wines_with_app = [r for r in wines if r.get("k") and str(r["k"]).strip()]
    print(f"Wines with appellation: {len(wines_with_app)} ({len(wines_with_app) / len(wines) * 100:.1f}%)")

    wines_with_abv = [r for r in wines if r.get("f") and float(r["f"] or 0) > 0]
    print(f"Wines with ABV: {len(wines_with_abv)} ({len(wines_with_abv) / len(wines) * 100:.1f}%)")

    print("\n--- Sample Wine Records ---")
    for w in wines[:5]:
        print(json.dumps(w))

    print("\n--- Top 30 Wine Appellations ---")
    app_counter = Counter((r.get("k") or "").strip() for r in wines if (r.get("k") or "").strip())
    for k, v in app_counter.most_common(30):
        print(f"  {k}: {v}")

    print("\n--- Top 20 Wine Brands ---")
    brand_counter = Counter((r.get("c") or "").strip() for r in wines if (r.get("c") or "").strip())
    for k, v in brand_counter.most_common(20):
        print(f"  {k}: {v}")

    unique_brands = {(r.get("c") or "").strip().upper() for r in wines}
    unique_colas = {r["a"].strip() for r in wines_with_cola}
    print(f"\n--- Unique Counts (wines only) ---")
    print(f"Unique brands: {len(unique_brands)}")
    print(f"Unique COLA numbers: {len(unique_colas)}")


if __name__ == "__main__":
    main()
