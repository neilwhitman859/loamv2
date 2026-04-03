"""
Promote grape data from TTB records to wine_grapes table.

For each canonical wine that lacks grape data, finds TTB records via
canonical_wine_id index and resolves the grape_varietals string.

Uses direct Postgres (psycopg2) instead of Supabase REST API to avoid
HTTP/2 ConnectionTerminated errors on the 3.28M-row source_ttb_colas table.

Usage:
    python -m pipeline.promote.ttb_grape_promote [--limit 10000] [--dry-run]
"""

import re
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from pipeline.lib.db import get_conn
from pipeline.lib.resolve import ReferenceResolver


def parse_grape_string(s):
    """Parse TTB grape_varietals string into [(name, percentage), ...].

    Examples:
        "CABERNET SAUVIGNON" -> [("Cabernet Sauvignon", None)]
        "100% PINOT NOIR" -> [("Pinot Noir", 100)]
        "75% CABERNET SAUVIGNON, 25% MERLOT" -> [("Cabernet Sauvignon", 75), ("Merlot", 25)]
        "CHARDONNAY/PINOT NOIR" -> [("Chardonnay", None), ("Pinot Noir", None)]
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
    from psycopg2.extras import execute_values

    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=50000)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    conn = get_conn()
    cur = conn.cursor()

    # Load grape resolver (uses REST API internally for light reference reads -- that's fine)
    print("Loading grape resolver...")
    resolver = ReferenceResolver(verbose=True)
    resolver.init_sync()

    # Find ALL wines without grape data using a single SQL query with LEFT JOIN
    # No created_at filter -- processes the entire catalog
    print("Finding wines without grape data...")
    cur.execute("""
        SELECT w.id
        FROM wines w
        LEFT JOIN wine_grapes wg ON wg.wine_id = w.id
        WHERE w.deleted_at IS NULL
          AND wg.wine_id IS NULL
    """)
    wines_without_grapes = [row[0] for row in cur.fetchall()]
    print(f"  {len(wines_without_grapes)} wines without grapes")

    if args.limit and len(wines_without_grapes) > args.limit:
        wines_without_grapes = wines_without_grapes[:args.limit]
        print(f"  Limited to {args.limit}")

    if not wines_without_grapes:
        print("No wines need grape data.")
        cur.close()
        conn.close()
        return

    # Load TTB grape data for target wines using a single JOIN query
    # Uses a temp table to pass the wine ID list efficiently
    print("Loading grape data from TTB...")
    cur.execute("""
        CREATE TEMP TABLE _target_wines (wine_id uuid)
    """)
    execute_values(
        cur,
        "INSERT INTO _target_wines (wine_id) VALUES %s",
        [(wid,) for wid in wines_without_grapes],
        page_size=5000,
    )
    cur.execute("""
        SELECT DISTINCT ON (t.canonical_wine_id)
            t.canonical_wine_id, t.grape_varietals
        FROM source_ttb_colas t
        JOIN _target_wines tw ON tw.wine_id = t.canonical_wine_id
        WHERE t.grape_varietals IS NOT NULL
          AND t.canonical_wine_id IS NOT NULL
        ORDER BY t.canonical_wine_id, t.id
    """)
    wine_grape_map = {}
    for row in cur:
        wine_grape_map[row[0]] = row[1]

    cur.execute("DROP TABLE IF EXISTS _target_wines")
    conn.commit()

    print(f"  {len(wine_grape_map)} wines have TTB grape data")

    # Resolve grapes in memory
    print("Resolving grape names...")
    grape_inserts = []
    wines_resolved = 0
    grapes_resolved = 0
    grapes_unresolved = 0
    unresolved_names = {}

    for i, (wine_id, grape_string) in enumerate(wine_grape_map.items()):
        parsed = parse_grape_string(grape_string)

        if not parsed:
            continue

        wine_grapes = []
        for name, pct in parsed:
            grape = resolver.resolve_grape(name)
            if grape:
                wine_grapes.append((wine_id, grape["id"], pct))
                grapes_resolved += 1
            else:
                grapes_unresolved += 1
                unresolved_names[name] = unresolved_names.get(name, 0) + 1

        if wine_grapes:
            grape_inserts.extend(wine_grapes)
            wines_resolved += 1

        if (i + 1) % 5000 == 0:
            print(f"  {i + 1}/{len(wine_grape_map)} wines processed, "
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
        cur.close()
        conn.close()
        return

    if grape_inserts:
        print(f"\nInserting {len(grape_inserts)} grape links...")
        execute_values(
            cur,
            """
            INSERT INTO wine_grapes (wine_id, grape_id, percentage)
            VALUES %s
            ON CONFLICT DO NOTHING
            """,
            grape_inserts,
            page_size=1000,
        )
        conn.commit()
        inserted = cur.rowcount
        print(f"  Inserted: {inserted}")

    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
