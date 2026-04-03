"""
Promote grapes from _grape_pending helper table.

Reads pre-extracted (wine_id, grape_string) pairs from the SQL helper table,
resolves grape names, and batch-inserts into wine_grapes.

Much faster than per-wine TTB lookups since _grape_pending is only 100K rows.

Usage:
    python -m pipeline.promote.grape_from_helper [--dry-run] [--limit 0]
"""

import re
import sys
import time
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from pipeline.lib.db import get_supabase, batch_insert
from pipeline.lib.resolve import ReferenceResolver


def parse_grape_string(s):
    """Parse TTB grape_varietals into [(name, percentage), ...]."""
    if not s:
        return []
    s = s.strip()
    parts = re.split(r'[,/]', s)
    grapes = []
    for part in parts:
        part = part.strip()
        if not part or len(part) < 2:
            continue
        pct_match = re.match(r'^(\d+)%?\s+(.+)$', part)
        if pct_match:
            pct = int(pct_match.group(1))
            name = pct_match.group(2).strip()
            if 0 < pct <= 100:
                grapes.append((name.title(), pct))
        else:
            name = re.sub(r'\s+', ' ', part).strip()
            grapes.append((name.title(), None))
    return grapes


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--limit", type=int, default=0, help="0 = no limit")
    args = parser.parse_args()

    sb = get_supabase()

    # Load resolver
    print("Loading grape resolver...")
    resolver = ReferenceResolver(verbose=True)
    resolver.init_sync()

    # Load existing wine_grapes to skip dupes
    print("Loading existing wine_grapes...")
    existing = set()
    offset = 0
    while True:
        result = sb.table("wine_grapes").select("wine_id").range(offset, offset + 999).execute()
        for r in result.data:
            existing.add(r["wine_id"])
        if len(result.data) < 1000:
            break
        offset += 1000
    print(f"  {len(existing)} wines already have grapes")

    # Load from _grape_pending
    print("Loading _grape_pending...")
    pending = []
    offset = 0
    while True:
        result = sb.table("_grape_pending").select("wine_id,grape_string").range(offset, offset + 999).execute()
        pending.extend(result.data)
        if len(result.data) < 1000:
            break
        offset += 1000
    print(f"  {len(pending)} rows loaded")

    # Filter out wines that already have grapes
    pending = [r for r in pending if r["wine_id"] not in existing]
    print(f"  {len(pending)} wines need grapes")

    if args.limit > 0:
        pending = pending[:args.limit]

    # Resolve grapes
    grape_inserts = []
    wines_resolved = 0
    unresolved = defaultdict(int)
    t0 = time.time()

    for i, row in enumerate(pending):
        parsed = parse_grape_string(row["grape_string"])
        if not parsed:
            continue

        wine_grapes = []
        for name, pct in parsed:
            grape = resolver.resolve_grape(name)
            if grape:
                wine_grapes.append({
                    "wine_id": row["wine_id"],
                    "grape_id": grape["id"],
                    "percentage": pct,
                })
            else:
                unresolved[name] += 1

        if wine_grapes:
            grape_inserts.extend(wine_grapes)
            wines_resolved += 1

        if (i + 1) % 10000 == 0:
            elapsed = time.time() - t0
            print(f"  {i+1}/{len(pending)} processed, {wines_resolved} resolved, "
                  f"{len(grape_inserts)} links ({elapsed:.0f}s)")

    print(f"\nResults:")
    print(f"  {wines_resolved} wines with resolved grapes")
    print(f"  {len(grape_inserts)} grape links to insert")
    print(f"  {sum(unresolved.values())} unresolved instances")

    if unresolved:
        top = sorted(unresolved.items(), key=lambda x: -x[1])[:20]
        print(f"\n  Top unresolved:")
        for name, cnt in top:
            safe = name.encode('ascii', 'replace').decode('ascii')
            print(f"    {safe:30s} {cnt}")

    if args.dry_run:
        print("\n  DRY RUN")
        return

    if grape_inserts:
        print(f"\nInserting {len(grape_inserts)} grape links...")
        inserted = batch_insert("wine_grapes", grape_inserts, batch_size=200)
        print(f"  Inserted: {inserted}")


if __name__ == "__main__":
    main()
