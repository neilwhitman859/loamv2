"""
Analyzes slug_match/substring/suffix_stripped match quality from JSONL.

Reads producer_winery_map.jsonl and categorizes matches by quality:
- slug_match: safe (encoding) vs false positives (genuinely different names)
- substring: contained (safe) vs risky (partial overlap)
- suffix_stripped: all entries

Usage:
    python -m pipeline.vivino.analyze_cleanup
"""

import sys
import json
import unicodedata
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))


def normalize(s):
    """Normalize handling literal \\u0026 sequences and &amp;"""
    out = s.replace("\\u0026", "&").replace("&amp;", "&")
    return unicodedata.normalize("NFC", out).lower().strip()


def main():
    slug_matches = []
    substring_matches = []
    suffix_stripped = []
    all_entries = []

    with open("producer_winery_map.jsonl", "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                j = json.loads(line)
            except json.JSONDecodeError:
                continue
            all_entries.append(j)
            mc = j.get("match_confidence")
            if mc == "slug_match":
                slug_matches.append(j)
            elif mc == "substring":
                substring_matches.append(j)
            elif mc == "suffix_stripped":
                suffix_stripped.append(j)

    # Normalization test
    test_entry = next((m for m in slug_matches if "0026" in m.get("vivino_winery_name", "")), None)
    if test_entry:
        print("=== NORMALIZATION TEST ===")
        print(f"Raw vivino name: {json.dumps(test_entry['vivino_winery_name'])}")
        print(f"Normalized: {normalize(test_entry['vivino_winery_name'])}")
        print(f"Producer normalized: {normalize(test_entry['producer_name'])}")
        print(f"Match? {normalize(test_entry['producer_name']) == normalize(test_entry['vivino_winery_name'])}")
        print()

    # SLUG_MATCH ANALYSIS
    slug_safe = []
    slug_false_positives = []
    for m in slug_matches:
        if normalize(m["producer_name"]) == normalize(m["vivino_winery_name"]):
            slug_safe.append(m)
        else:
            slug_false_positives.append(m)

    print("=== SLUG_MATCH ANALYSIS ===")
    print(f"Total: {len(slug_matches)}")
    print(f"Encoding-safe (names match after normalizing): {len(slug_safe)}")
    print(f"FALSE POSITIVES (names genuinely different): {len(slug_false_positives)}")
    print()
    if slug_false_positives:
        print("All false positive slug_matches:")
        for m in slug_false_positives:
            print(f'  x "{m["producer_name"]}" -> "{normalize(m["vivino_winery_name"])}" (winery {m["vivino_winery_id"]}, {m.get("wines_count", 0)} wines)')

    # SUFFIX_STRIPPED ANALYSIS
    print()
    print("=== SUFFIX_STRIPPED ANALYSIS ===")
    print(f"Total: {len(suffix_stripped)}")
    for m in suffix_stripped:
        print(f'  "{m["producer_name"]}" -> "{normalize(m["vivino_winery_name"])}" (winery {m["vivino_winery_id"]}, {m.get("wines_count", 0)} wines)')

    # SUBSTRING ANALYSIS
    sub_safe = []
    sub_risky = []
    for m in substring_matches:
        a = normalize(m["producer_name"])
        b = normalize(m["vivino_winery_name"])
        if a in b or b in a:
            sub_safe.append(m)
        else:
            sub_risky.append(m)

    print()
    print("=== SUBSTRING ANALYSIS ===")
    print(f"Total: {len(substring_matches)}")
    print(f"Contained (one name inside the other): {len(sub_safe)}")
    print(f"Risky (partial/weak overlap): {len(sub_risky)}")
    print()
    if sub_risky:
        print("Risky substring matches:")
        for m in sub_risky:
            print(f'  ? "{m["producer_name"]}" -> "{normalize(m["vivino_winery_name"])}" (winery {m["vivino_winery_id"]}, {m.get("wines_count", 0)} wines)')

    # SUMMARY
    total_resolved = sum(1 for e in all_entries if e.get("vivino_winery_id"))
    total_none = sum(1 for e in all_entries if e.get("match_confidence") == "none")
    total_exact = sum(1 for e in all_entries if e.get("match_confidence") == "exact")

    print()
    print("=== SUMMARY ===")
    print(f"Total producers: {len(all_entries)}")
    print(f"Exact matches (safe): {total_exact}")
    print(f"Slug_match encoding-safe: {len(slug_safe)}")
    print(f"Slug_match FALSE POSITIVES: {len(slug_false_positives)}")
    print(f"Suffix_stripped: {len(suffix_stripped)}")
    print(f"Substring contained (likely safe): {len(sub_safe)}")
    print(f"Substring risky: {len(sub_risky)}")
    print(f"Unresolved: {total_none}")
    print()
    delete_count = len(slug_false_positives) + len(sub_risky)
    keep_count = total_exact + len(slug_safe) + len(sub_safe) + len(suffix_stripped)
    print(f"ENTRIES TO DELETE: {len(slug_false_positives)} slug_match FPs + {len(sub_risky)} risky substring = {delete_count}")
    print(f"ENTRIES TO KEEP: {total_exact} exact + {len(slug_safe)} slug safe + {len(sub_safe)} substring safe + {len(suffix_stripped)} suffix = {keep_count}")


if __name__ == "__main__":
    main()
