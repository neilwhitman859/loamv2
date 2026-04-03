"""
Promote grapes directly from source_ttb_colas, bypassing _grape_pending.

Paginates through source_ttb_colas in batches where:
- canonical_wine_id is set
- grape_varietals is not null/empty
- the wine doesn't already have grapes in wine_grapes

This avoids the need for the _grape_pending helper table which times out
on CREATE TABLE AS with 3.28M rows.

Usage:
    python -m pipeline.promote.grape_from_ttb_direct [--dry-run] [--limit 0]
"""

import re
import sys
import time
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import pipeline.lib.db as db_mod
from pipeline.lib.db import batch_insert
from pipeline.lib.resolve import ReferenceResolver


JUNK_STRINGS = {
    "the status is approved.", "the status is approved",
    "red wine", "white wine", "rose wine", "table wine",
    "wine", "red", "white", "rose", "blended wine",
}

# TTB encodes accented chars as '?' — map common corrupted forms
TTB_GRAPE_FIXES = {
    "mourv?dre": "Mourvedre", "mourv dre": "Mourvedre",
    "albari?o": "Albarino", "albari o": "Albarino",
    "gew?rztraminer": "Gewurztraminer", "gew rztraminer": "Gewurztraminer",
    "gr?ner veltliner": "Gruner Veltliner", "gr ner veltliner": "Gruner Veltliner",
    "carmen?re": "Carmenere", "carmen re": "Carmenere",
    "aligot?": "Aligote", "aligot": "Aligote",
    "sp?tburgunder": "Spatburgunder", "sp tburgunder": "Spatburgunder",
    "valdigu?": "Valdiguie", "valdigu": "Valdiguie",
    "m?ller-thurgau": "Muller-Thurgau", "m ller-thurgau": "Muller-Thurgau",
    "m?ller thurgau": "Muller-Thurgau",
    "blaufr?nkisch": "Blaufrankisch",
    "torront?s": "Torrontes",
    "catarratto": "Catarratto Bianco Comune",
    "montepulciano d'abruzzo": "Montepulciano",
    "picpoul blanc": "Piquepoul Blanc", "picpoul": "Piquepoul Blanc",
    "pineau d'aunis": "Pineau d'Aunis",
    "chianti": "Sangiovese",
}


def parse_grape_string(s):
    """Parse TTB grape_varietals into [(name, percentage), ...]."""
    if not s:
        return []
    s = s.strip()
    # Skip junk
    if s.lower() in JUNK_STRINGS:
        return []

    grapes = []
    # Split on comma, semicolon, ampersand, " and ", " / "
    parts = re.split(r'[,;/&]|\band\b', s, flags=re.IGNORECASE)
    for part in parts:
        part = part.strip()
        if not part:
            continue
        # Try "Grape Name (NN%)" or "NN% Grape Name"
        m = re.match(r'^(.+?)\s*\(?\s*(\d{1,3})\s*%\s*\)?$', part)
        if m:
            name = _fix_grape_name(m.group(1).strip())
            pct = float(m.group(2))
            if name and pct > 0:
                grapes.append((name, pct))
            continue
        m = re.match(r'^(\d{1,3})\s*%\s*(.+)$', part)
        if m:
            name = _fix_grape_name(m.group(2).strip())
            pct = float(m.group(1))
            if name and pct > 0:
                grapes.append((name, pct))
            continue
        # Plain name
        name = _fix_grape_name(part)
        if name:
            grapes.append((name, None))
    return grapes


def _fix_grape_name(name):
    """Fix TTB encoding corruption and known misspellings."""
    if not name:
        return None
    # Strip non-ASCII question marks that replace accented chars
    cleaned = name.replace('?', '').strip()
    # Check TTB fixes map (case-insensitive)
    lower = name.lower()
    if lower in TTB_GRAPE_FIXES:
        return TTB_GRAPE_FIXES[lower]
    cleaned_lower = cleaned.lower()
    if cleaned_lower in TTB_GRAPE_FIXES:
        return TTB_GRAPE_FIXES[cleaned_lower]
    if cleaned_lower in JUNK_STRINGS:
        return None
    # Return title-cased version
    result = cleaned.title() if cleaned == cleaned.upper() else cleaned
    return result if len(result) >= 2 else None


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--limit", type=int, default=0, help="0 = no limit")
    args = parser.parse_args()

    sb = db_mod.get_supabase()

    # Load resolver
    print("Loading grape resolver...")
    resolver = ReferenceResolver(verbose=True)
    resolver.init_sync()

    # Load existing wine_grapes to skip dupes
    print("Loading existing wine_grapes (wine IDs with grapes)...")
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

    # Approach: query only canonical_wine_id (simpler filter, uses index)
    # and filter grape_varietals in Python. The canonical_wine_id index
    # is much more selective (~360K of 3.28M rows).
    print("Loading grape data from source_ttb_colas (cursor-based, minimal filter)...")
    pending = []
    seen_wines = set()  # dedup: only take first grape string per wine
    cursor = ""  # empty string sorts before all ttb_ids
    total_scanned = 0
    total_with_grapes = 0
    retries = 0
    max_retries = 5
    t0 = time.time()

    while True:
        try:
            q = (sb.table("source_ttb_colas")
                 .select("ttb_id,canonical_wine_id,grape_varietals")
                 .not_.is_("canonical_wine_id", "null")
                 .gt("ttb_id", cursor)
                 .order("ttb_id")
                 .limit(1000))
            result = q.execute()
            retries = 0  # reset on success
        except Exception as e:
            retries += 1
            if retries > max_retries:
                print(f"  Max retries exceeded at cursor {cursor}")
                break
            print(f"  Error at cursor {cursor}: {e}")
            print(f"  Retry {retries}/{max_retries} after 5s...")
            time.sleep(5)
            db_mod.get_supabase.cache_clear()
            sb = db_mod.get_supabase()
            continue

        if not result.data:
            break

        for row in result.data:
            wine_id = row["canonical_wine_id"]
            grape_str = row.get("grape_varietals") or ""
            grape_str = grape_str.strip()

            # Skip empty grapes or already-processed wines
            if not grape_str or wine_id in existing or wine_id in seen_wines:
                continue
            total_with_grapes += 1
            seen_wines.add(wine_id)
            pending.append({
                "wine_id": wine_id,
                "grape_string": grape_str
            })

        cursor = result.data[-1]["ttb_id"]
        total_scanned += len(result.data)

        if total_scanned % 50000 == 0:
            elapsed = time.time() - t0
            print(f"  {total_scanned} TTB rows scanned, "
                  f"{len(pending)} wines pending, {total_with_grapes} w/grapes ({elapsed:.0f}s)")

        if len(result.data) < 1000:
            break

        if args.limit > 0 and len(pending) >= args.limit:
            pending = pending[:args.limit]
            break

    elapsed = time.time() - t0
    print(f"  Scanned {total_scanned} TTB rows in {elapsed:.0f}s")
    print(f"  {total_with_grapes} had grape data, {len(pending)} wines need grapes")

    if not pending:
        print("Nothing to do.")
        return

    # Resolve grapes
    print("\nResolving grapes...")
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
