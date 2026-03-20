"""
Audits slug_match entries, substring matches, and duplicate winery mappings
from producer_winery_map.jsonl.

Usage:
    python -m pipeline.vivino.audit
"""

import sys
import json
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))


def main():
    # 1. Check slug_match entries
    print("=== AUDIT: slug_match entries (highest risk of false positives) ===\n")
    slug_matches = []
    all_resolved = []

    with open("producer_winery_map.jsonl", "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                j = json.loads(line)
            except json.JSONDecodeError:
                continue
            if j.get("match_confidence") == "slug_match":
                slug_matches.append(j)
            if j.get("vivino_winery_id"):
                all_resolved.append(j)

    print(f"slug_match entries: {len(slug_matches)}")
    print("Sample slug_match (potential false positives):")
    for m in slug_matches[:20]:
        match = "ok" if m["producer_name"] == m["vivino_winery_name"] else "MISMATCH"
        print(f'  {match} Loam: "{m["producer_name"]}" -> Vivino: "{m["vivino_winery_name"]}" ({m["vivino_winery_id"]}, {m.get("wines_count", 0)} wines)')

    # 2. Check substring matches
    print("\n=== AUDIT: substring entries ===\n")
    substring_matches = []
    with open("producer_winery_map.jsonl", "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                j = json.loads(line)
            except json.JSONDecodeError:
                continue
            if j.get("match_confidence") == "substring":
                substring_matches.append(j)

    print(f"substring entries: {len(substring_matches)}")
    print("Sample substring matches:")
    for m in substring_matches[:20]:
        print(f'  Loam: "{m["producer_name"]}" -> Vivino: "{m["vivino_winery_name"]}" ({m.get("wines_count", 0)} wines)')

    # 3. Check for duplicate vivino_winery_ids
    print("\n=== AUDIT: Duplicate Vivino winery mappings ===\n")
    winery_to_producers: dict[str, list] = {}
    for m in all_resolved:
        winery_to_producers.setdefault(m["vivino_winery_id"], []).append(m)

    dupes = [(wid, prods) for wid, prods in winery_to_producers.items() if len(prods) > 1]
    print(f"Wineries mapped to multiple Loam producers: {len(dupes)}")
    for wid, prods in dupes[:10]:
        names = ", ".join(f'"{p["producer_name"]}"' for p in prods)
        print(f'  Vivino {wid} "{prods[0]["vivino_winery_name"]}" -> {names}')

    # 4. Check exact matches with high wine counts
    print("\n=== AUDIT: 'exact' matches with high wine counts ===\n")
    exact_big = [m for m in all_resolved if m.get("match_confidence") == "exact" and m.get("wines_count", 0) > 100]
    print(f"Exact matches with >100 wines: {len(exact_big)}")
    for m in exact_big[:10]:
        print(f'  "{m["producer_name"]}" -> "{m["vivino_winery_name"]}" ({m.get("wines_count", 0)} wines)')


if __name__ == "__main__":
    main()
