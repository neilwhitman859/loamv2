"""
TTB Producer Re-linking: Fix unlinked TTB brand names via suffix stripping.

Many TTB brand_name values are slight variations of canonical producers:
  "LOUIS JADOT" → "Maison Louis Jadot" (prefix: Maison)
  "CHATEAU LYNCH BAGES" → "Château Lynch-Bages" (accent + hyphen)
  "FAIVELEY" → "Domaine Faiveley" (prefix: Domaine)
  "BOUCHARD PERE & FILS" → "Bouchard Père & Fils" (accent)

This script re-links using:
  Tier 1: Exact normalized name (catches accent differences)
  Tier 2: Suffix-stripped match (catches Domaine/Château/Maison prefixes)
  Tier 3: Producer alias match

Then cascades wine linking for newly-linked producers.

Usage:
    python -m pipeline.promote.ttb_producer_relink [--dry-run]
"""

import sys
import time
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from pipeline.lib.db import get_supabase
from pipeline.lib.normalize import normalize, normalize_producer


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    sb = get_supabase()

    # Load canonical producers into multiple indexes
    print("Loading canonical producers...")
    prod_by_norm = {}        # name_normalized -> {id, name}
    prod_by_stripped = {}    # normalize_producer(name) -> [{id, name}]
    prod_by_upper = {}       # UPPER(name) -> id (already used by cola_pass1)
    offset = 0
    while True:
        result = (sb.table("producers")
                  .select("id,name,name_normalized")
                  .is_("deleted_at", "null")
                  .range(offset, offset + 999)
                  .execute())
        for p in result.data:
            norm = p.get("name_normalized") or normalize(p["name"])
            prod_by_norm[norm] = p
            prod_by_upper[p["name"].upper()] = p["id"]
            stripped = normalize_producer(p["name"])
            if stripped and stripped != norm:
                if stripped not in prod_by_stripped:
                    prod_by_stripped[stripped] = []
                prod_by_stripped[stripped].append(p)
        if len(result.data) < 1000:
            break
        offset += 1000
    print(f"  {len(prod_by_norm):,} by norm, {len(prod_by_stripped):,} by stripped")

    # Load aliases
    prod_by_alias = {}
    offset = 0
    while True:
        result = sb.table("producer_aliases").select("producer_id,name_normalized").range(offset, offset + 999).execute()
        for a in result.data:
            prod_by_alias[a["name_normalized"]] = a["producer_id"]
        if len(result.data) < 1000:
            break
        offset += 1000
    print(f"  {len(prod_by_alias):,} aliases")

    # Get unlinked TTB brand names with counts
    print("\nLoading unlinked brand names...")
    brand_counts = defaultdict(int)
    offset = 0
    batch_n = 0
    while True:
        result = (sb.table("source_ttb_colas")
                  .select("brand_name")
                  .is_("canonical_producer_id", "null")
                  .in_("class_type", ["80", "81", "80A", "84", "88"])
                  .range(offset, offset + 999)
                  .execute())
        for r in result.data:
            if r.get("brand_name"):
                brand_counts[r["brand_name"]] += 1
        if len(result.data) < 1000:
            break
        offset += 1000
        batch_n += 1
        if batch_n % 500 == 0:
            print(f"  ... scanned {offset:,} rows, {len(brand_counts):,} distinct brands")

    print(f"  {len(brand_counts):,} distinct unlinked brand names")

    # Match brands to producers
    matches = {}  # brand_name -> (producer_id, tier)
    for brand, cnt in brand_counts.items():
        # Already checked by cola_pass1 (exact UPPER match) — skip those
        if brand in prod_by_upper:
            continue

        norm = normalize(brand)

        # Tier 1: Exact normalized match (catches accents)
        p = prod_by_norm.get(norm)
        if p:
            matches[brand] = (p["id"], 1, p["name"], cnt)
            continue

        # Tier 2: Alias match
        pid = prod_by_alias.get(norm)
        if pid:
            matches[brand] = (pid, 2, norm, cnt)
            continue

        # Tier 3: Suffix-stripped match
        stripped = normalize_producer(brand)
        if stripped and stripped != norm:
            candidates = prod_by_stripped.get(stripped, [])
            if len(candidates) == 1:
                matches[brand] = (candidates[0]["id"], 3, candidates[0]["name"], cnt)
            elif len(candidates) > 1:
                # Ambiguous — skip
                pass

    # Sort by record count
    sorted_matches = sorted(matches.items(), key=lambda x: -x[1][3])
    total_records = sum(v[3] for v in matches.values())

    print(f"\n  Matched: {len(matches):,} brands ({total_records:,} TTB records)")
    by_tier = defaultdict(int)
    for _, (_, tier, _, cnt) in matches.items():
        by_tier[tier] += cnt
    print(f"  Tier 1 (normalized): {by_tier[1]:,} records")
    print(f"  Tier 2 (alias): {by_tier[2]:,} records")
    print(f"  Tier 3 (stripped): {by_tier[3]:,} records")

    print("\n  Top 30 matches:")
    for brand, (pid, tier, pname, cnt) in sorted_matches[:30]:
        print(f"    {brand:40s} → {pname:35s} (T{tier}, {cnt:,})")

    if args.dry_run:
        print("\n  DRY RUN — no writes")
        return

    # Apply the links
    print(f"\nLinking {len(matches):,} brands to producers...")
    total_linked = 0
    errors = 0

    for i, (brand, (pid, tier, pname, cnt)) in enumerate(sorted_matches):
        try:
            result = (sb.table("source_ttb_colas")
                      .update({"canonical_producer_id": pid})
                      .eq("brand_name", brand)
                      .is_("canonical_producer_id", "null")
                      .execute())
            linked = len(result.data) if result.data else 0
            total_linked += linked
        except Exception as e:
            errors += 1
            if errors <= 5:
                print(f"  Error on {brand}: {e}")

        if (i + 1) % 200 == 0:
            print(f"  {i + 1:,}/{len(matches):,} brands, {total_linked:,} records linked")

    print(f"\n  Total linked: {total_linked:,}")
    print(f"  Errors: {errors}")


if __name__ == "__main__":
    main()
