#!/usr/bin/env python3
"""
Stats on scraped Ridge JSONL data.

Usage:
    python -m pipeline.analyze.ridge_stats
"""

import json
import sys
from collections import Counter
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

JSONL_FILE = Path(__file__).resolve().parents[2] / "ridge_wines.jsonl"


def main():
    lines = JSONL_FILE.read_text(encoding="utf-8").strip().split("\n")
    wines = [json.loads(l) for l in lines]

    print("=== RIDGE SCRAPE STATS ===\n")
    print(f"Total entries: {len(wines)}")

    names = {w.get("wineName") for w in wines}
    print(f"Unique wines: {len(names)}")

    years = [w["vintage"] for w in wines if w.get("vintage")]
    if years:
        print(f"Vintage range: {min(years)} - {max(years)}")

    print(f"\nData Completeness:")
    has_grapes = sum(1 for w in wines if w.get("grapes") and len(w["grapes"]) > 0)
    has_scores = sum(1 for w in wines if w.get("scores") and len(w["scores"]) > 0)
    has_wm_notes = sum(1 for w in wines if w.get("winemakerNotes"))
    has_v_notes = sum(1 for w in wines if w.get("vintageNotes"))
    has_abv = sum(1 for w in wines if w.get("abv"))
    has_winemaking = sum(1 for w in wines if w.get("winemaking"))
    has_growing = sum(1 for w in wines if w.get("growingSeason"))
    has_ph = sum(1 for w in wines if (w.get("winemaking") or {}).get("ph"))
    has_brix = sum(1 for w in wines if (w.get("winemaking") or {}).get("brix"))
    members_only = sum(1 for w in wines if w.get("membersOnly"))
    n = len(wines)

    print(f"  With grapes: {has_grapes} ({has_grapes / n * 100:.1f}%)")
    print(f"  With scores: {has_scores} ({has_scores / n * 100:.1f}%)")
    print(f"  With winemaker notes: {has_wm_notes} ({has_wm_notes / n * 100:.1f}%)")
    print(f"  With vintage notes: {has_v_notes} ({has_v_notes / n * 100:.1f}%)")
    print(f"  With ABV: {has_abv} ({has_abv / n * 100:.1f}%)")
    print(f"  With winemaking: {has_winemaking} ({has_winemaking / n * 100:.1f}%)")
    print(f"  With growing season: {has_growing} ({has_growing / n * 100:.1f}%)")
    print(f"  With pH: {has_ph}")
    print(f"  With Brix: {has_brix}")
    print(f"  Members only: {members_only}")

    total_scores = sum(len(w.get("scores", [])) for w in wines)
    print(f"\nTotal scores: {total_scores}")

    pubs: Counter = Counter()
    for w in wines:
        for s in w.get("scores", []):
            pubs[s.get("publication") or "(no publication)"] += 1
    print(f"\nPublications ({len(pubs)}):")
    for p, c in pubs.most_common():
        print(f"  {p}: {c} scores")

    apps: Counter = Counter()
    for w in wines:
        apps[w.get("appellation") or "(none)"] += 1
    print(f"\nAppellations ({len(apps)}):")
    for a, c in apps.most_common():
        print(f"  {a}: {c} vintages")

    grape_set: set[str] = set()
    for w in wines:
        for g in w.get("grapes", []):
            grape_set.add(g.get("grape", ""))
    print(f"\nUnique grapes ({len(grape_set)}): {', '.join(sorted(grape_set))}")


if __name__ == "__main__":
    main()
