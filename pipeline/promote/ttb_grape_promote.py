"""
Promote grape data from TTB records to wine_grapes table.

For each canonical wine that lacks grape data, finds TTB records via
canonical_wine_id index and resolves the grape_varietals string.

Usage:
    python -m pipeline.promote.ttb_grape_promote [--limit 10000] [--dry-run]
"""

import re
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from pipeline.lib.db import get_supabase, batch_insert
from pipeline.lib.resolve import ReferenceResolver


def parse_grape_string(s):
    """Parse TTB grape_varietals string into [(name, percentage), ...].

    Examples:
        "CABERNET SAUVIGNON" → [("Cabernet Sauvignon", None)]
        "100% PINOT NOIR" → [("Pinot Noir", 100)]
        "75% CABERNET SAUVIGNON, 25% MERLOT" → [("Cabernet Sauvignon", 75), ("Merlot", 25)]
        "CHARDONNAY/PINOT NOIR" → [("Chardonnay", None), ("Pinot Noir", None)]
    """
    if not s:
        return []

    s = s.strip()

    # Split on comma or slash
    parts = re.split(r'[,/]', s)

    grapes = []
    for part in parts:
        part = part.strip()
        if not part:
            continue

        # Extract percentage
        pct_match = re.match(r'^(\d+)%?\s+(.+)$', part)
        if pct_match:
            pct = int(pct_match.group(1))
            name = pct_match.group(2).strip()
            if pct > 100:
                continue  # invalid
            grapes.append((name.title(), pct))
        else:
            # No percentage
            name = re.sub(r'\s+', ' ', part).strip()
            if len(name) < 2:
                continue
            grapes.append((name.title(), None))

    return grapes


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=50000)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    sb = get_supabase()

    # Load grape resolver
    print("Loading grape resolver...")
    resolver = ReferenceResolver(verbose=True)
    resolver.init_sync()

    # Get wine IDs that need grapes (have no wine_grapes entries)
    # Use a SQL approach: wines NOT IN wine_grapes, created recently (Tier C)
    print("Finding wines without grape data...")
    wines_needing_grapes = []
    offset = 0
    while True:
        result = (sb.table("wines")
                  .select("id")
                  .is_("deleted_at", "null")
                  .gte("created_at", "2026-04-02")  # Tier C wines
                  .range(offset, offset + 999)
                  .execute())
        wines_needing_grapes.extend([r["id"] for r in result.data])
        if len(result.data) < 1000:
            break
        offset += 1000
    print(f"  {len(wines_needing_grapes)} Tier C wines")

    # Filter to those without grapes
    # Check in batches
    print("Filtering to wines without grapes...")
    wines_without_grapes = []
    for i in range(0, len(wines_needing_grapes), 200):
        batch = wines_needing_grapes[i:i + 200]
        result = (sb.table("wine_grapes")
                  .select("wine_id")
                  .in_("wine_id", batch)
                  .execute())
        has_grapes = {r["wine_id"] for r in result.data}
        wines_without_grapes.extend([wid for wid in batch if wid not in has_grapes])

    print(f"  {len(wines_without_grapes)} wines without grapes")

    if args.limit and len(wines_without_grapes) > args.limit:
        wines_without_grapes = wines_without_grapes[:args.limit]
        print(f"  Limited to {args.limit}")

    # For each wine, get grape data from TTB via canonical_wine_id index
    print("Loading grape data from TTB...")
    grape_inserts = []
    wines_resolved = 0
    grapes_resolved = 0
    grapes_unresolved = 0
    unresolved_names = {}

    for i, wine_id in enumerate(wines_without_grapes):
        # Query TTB record for this wine (uses the canonical_wine_id index)
        result = (sb.table("source_ttb_colas")
                  .select("grape_varietals")
                  .eq("canonical_wine_id", wine_id)
                  .not_.is_("grape_varietals", "null")
                  .limit(1)
                  .execute())

        if not result.data:
            continue

        grape_string = result.data[0]["grape_varietals"]
        parsed = parse_grape_string(grape_string)

        if not parsed:
            continue

        wine_grapes = []
        for name, pct in parsed:
            grape = resolver.resolve_grape(name)
            if grape:
                wine_grapes.append({
                    "wine_id": wine_id,
                    "grape_id": grape["id"],
                    "percentage": pct,
                })
                grapes_resolved += 1
            else:
                grapes_unresolved += 1
                unresolved_names[name] = unresolved_names.get(name, 0) + 1

        if wine_grapes:
            grape_inserts.extend(wine_grapes)
            wines_resolved += 1

        if (i + 1) % 2000 == 0:
            print(f"  {i + 1}/{len(wines_without_grapes)} wines processed, "
                  f"{wines_resolved} resolved, {len(grape_inserts)} grape links")

    print(f"\nResults:")
    print(f"  {wines_resolved} wines with resolved grapes")
    print(f"  {len(grape_inserts)} grape links to insert")
    print(f"  {grapes_resolved} grapes resolved, {grapes_unresolved} unresolved")

    if unresolved_names:
        top_unresolved = sorted(unresolved_names.items(), key=lambda x: -x[1])[:15]
        print(f"\n  Top unresolved grape names:")
        for name, cnt in top_unresolved:
            safe_name = name.encode('ascii', 'replace').decode('ascii')
            print(f"    {safe_name:30s} {cnt}")

    if args.dry_run:
        print("\n  DRY RUN")
        return

    if grape_inserts:
        print(f"\nInserting {len(grape_inserts)} grape links...")
        inserted = batch_insert("wine_grapes", grape_inserts, batch_size=200)
        print(f"  Inserted: {inserted}")


if __name__ == "__main__":
    main()
