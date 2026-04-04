"""
Grape blend splitter for TTB grape_varietals strings.

Handles space-separated blend strings like "Pinot Noir Chardonnay" which the
base ttb_grape_promote.py skips because it only splits on commas and slashes.

Algorithm:
  For each unresolved grape string, try to segment it into known grape names
  using a greedy left-to-right approach (window sizes 3, 2, 1 words).
  Also handles percentage-prefixed blends: "50% Chardonnay 50% Pinot Noir".

Usage:
    python -m pipeline.promote.grape_blend_promote [--dry-run] [--limit 10000]
"""

import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from psycopg2.extras import execute_values

from pipeline.lib.db import get_conn
from pipeline.lib.resolve import ReferenceResolver
from pipeline.promote.ttb_grape_promote import (
    BLEND_BLACKLIST,
    JUNK_STRINGS,
    _fix_grape_name,
)


def _parse_pct_blend(s, resolver):
    """
    Handle strings like '50% Chardonnay 50% Pinot Noir' or 'Merlot 30% Cab Sauv'.
    Returns [(grape_id, pct_or_None), ...] or None if pattern doesn't match.
    """
    # Find all 'NN% GrapeName' segments
    tokens = re.split(r'\s+', s.strip())
    result = []
    i = 0
    while i < len(tokens):
        # Try matching a percentage token
        pct_tok = tokens[i]
        pct = None
        if re.match(r'^\d+%$', pct_tok):
            pct = int(pct_tok[:-1])
            i += 1
        # Greedily consume grape name (up to 3 words)
        found = None
        for window in (3, 2, 1):
            if i + window > len(tokens):
                continue
            candidate = " ".join(tokens[i:i + window])
            fixed = _fix_grape_name(candidate)
            if not fixed:
                continue
            grape = resolver.resolve_grape(fixed)
            if grape:
                found = (grape["id"], pct)
                i += window
                break
        if found:
            result.append(found)
        elif pct is not None:
            # Had a percentage but couldn't resolve the grape — give up
            return None
        else:
            # Couldn't resolve at this position — give up
            return None
    return result if len(result) >= 2 else None


def _split_blend_greedy(s, resolver):
    """
    Split a space-separated grape blend string into individual grape names.
    E.g. 'Pinot Noir Chardonnay' → [Pinot Noir id, Chardonnay id]
    Returns [(grape_id, None), ...] or None if can't fully segment.
    """
    s = s.strip()
    s_lower = s.lower()

    # Skip known junk/blacklist
    if s_lower in JUNK_STRINGS or s_lower in BLEND_BLACKLIST:
        return None

    # If it has percentage tokens, use the pct parser
    if re.search(r'\d+%', s):
        # Normalize inline percentages attached to words:
        # "Noir20%" → "Noir 20%", "72%Bobal" → "72% Bobal"
        s_norm = re.sub(r'([A-Za-z])(\d+%)', r'\1 \2', s)   # word-digit%
        s_norm = re.sub(r'(\d+%)([A-Za-z])', r'\1 \2', s_norm)  # digit%-word
        return _parse_pct_blend(s_norm, resolver)

    # Normalize dash separators: "Cabernet Sauvignon - Merlot" → "Cabernet Sauvignon Merlot"
    s = re.sub(r'\s*-\s*', ' ', s)

    # Handle parenthetical synonyms: "Spatburgunder (Pinot Noir)" → try both parts
    paren_match = re.match(r'^(.+?)\s*\((.+?)\)\s*$', s)
    if paren_match:
        # Try the primary name first, then the parenthetical
        primary = paren_match.group(1).strip()
        fixed = _fix_grape_name(primary)
        if fixed:
            grape = resolver.resolve_grape(fixed)
            if grape:
                return [(grape["id"], None)]
        # Try the parenthetical
        secondary = paren_match.group(2).strip()
        fixed = _fix_grape_name(secondary)
        if fixed:
            grape = resolver.resolve_grape(fixed)
            if grape:
                return [(grape["id"], None)]

    words = s.split()
    if len(words) < 2:
        return None  # Single word — handled by ttb_grape_promote

    result = []
    i = 0
    while i < len(words):
        found = None
        for window in (3, 2, 1):
            if i + window > len(words):
                continue
            candidate = " ".join(words[i:i + window])
            fixed = _fix_grape_name(candidate)
            if not fixed:
                continue
            grape = resolver.resolve_grape(fixed)
            if grape:
                found = (grape["id"], None)
                i += window
                break
        if found:
            result.append(found)
        else:
            return None  # Can't segment from position i — give up

    return result if len(result) >= 2 else None


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Promote blend grape strings to wine_grapes")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--limit", type=int, default=10000)
    args = parser.parse_args()

    print("Loading grape resolver...")
    resolver = ReferenceResolver(verbose=True)
    resolver.init_sync()

    conn = get_conn()
    cur = conn.cursor()

    # Step 1: Find wines without grape data (from canonical side — fast)
    print("Finding wines without grape data...")
    cur.execute("""
        SELECT w.id
        FROM wines w
        LEFT JOIN wine_grapes wg ON wg.wine_id = w.id
        WHERE w.deleted_at IS NULL AND wg.wine_id IS NULL
        LIMIT %s
    """, (args.limit,))
    wine_ids = [row[0] for row in cur.fetchall()]
    print(f"  {len(wine_ids)} wines without grapes")

    if not wine_ids:
        print("No wines need grape data.")
        cur.close()
        conn.close()
        return

    # Step 2: Find TTB grape data for those wines (using temp table for efficient join)
    print("Loading TTB grape data via temp table...")
    cur.execute("CREATE TEMP TABLE _blend_targets (wine_id uuid)")
    execute_values(
        cur,
        "INSERT INTO _blend_targets (wine_id) VALUES %s",
        [(wid,) for wid in wine_ids],
        page_size=5000,
    )
    cur.execute("""
        SELECT DISTINCT ON (t.canonical_wine_id)
            t.canonical_wine_id AS wine_id,
            t.grape_varietals
        FROM source_ttb_colas t
        JOIN _blend_targets bt ON bt.wine_id = t.canonical_wine_id
        WHERE t.grape_varietals IS NOT NULL
        ORDER BY t.canonical_wine_id, t.ttb_id
    """)
    rows = cur.fetchall()
    cur.execute("DROP TABLE IF EXISTS _blend_targets")
    conn.commit()
    print(f"  {len(rows)} wines with TTB grape data but no grape links")

    wines_resolved = 0
    wines_skipped_junk = 0
    wines_unsplittable = 0
    grape_inserts = []
    blend_counts = {}

    for wine_id, grape_string in rows:
        s_lower = grape_string.lower().replace('\ufffd', '?').strip()

        # Skip pure junk/blacklist (these are handled by ttb_grape_promote)
        if s_lower in JUNK_STRINGS or s_lower in BLEND_BLACKLIST:
            wines_skipped_junk += 1
            continue

        # Try to split as a blend
        parts = _split_blend_greedy(grape_string, resolver)
        if parts:
            for grape_id, pct in parts:
                grape_inserts.append((wine_id, grape_id, pct))
            wines_resolved += 1
            key = grape_string.strip()
            blend_counts[key] = blend_counts.get(key, 0) + 1
        else:
            wines_unsplittable += 1

    print(f"\nResults:")
    print(f"  {wines_resolved} wines resolved via blend splitting")
    print(f"  {len(grape_inserts)} grape links to insert")
    print(f"  {wines_skipped_junk} wines skipped (junk/blacklist)")
    print(f"  {wines_unsplittable} wines still unsplittable")

    if blend_counts:
        print(f"\n  Top resolved blend strings:")
        for name, cnt in sorted(blend_counts.items(), key=lambda x: -x[1])[:15]:
            print(f"    {name!r:50s} {cnt}")

    if not grape_inserts or args.dry_run:
        if args.dry_run:
            print("\n[DRY RUN] No writes made.")
        cur.close()
        conn.close()
        return

    print(f"\nInserting {len(grape_inserts)} grape links...")
    execute_values(
        cur,
        """INSERT INTO wine_grapes (wine_id, grape_id, percentage)
           VALUES %s ON CONFLICT (wine_id, grape_id) DO NOTHING""",
        [(wid, gid, pct) for wid, gid, pct in grape_inserts],
        page_size=1000,
    )
    conn.commit()
    print(f"  Done. {cur.rowcount} rows inserted.")

    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
