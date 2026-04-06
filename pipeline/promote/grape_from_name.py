"""
Grape backfill from wine names.

For wines with 0 wine_grapes links, parses unambiguous grape variety names
from the wine's name_normalized field and creates the link.

Strictly definitional: only links grapes whose names literally appear
in the wine name as word-bounded tokens. Uses greedy longest-match
to handle multi-word grapes (e.g., "Cabernet Sauvignon" before "Cabernet").

Usage:
    python -m pipeline.promote.grape_from_name --dry-run
    python -m pipeline.promote.grape_from_name --execute
    python -m pipeline.promote.grape_from_name --execute --limit 1000
"""

import argparse
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from pipeline.lib.db import get_conn
from pipeline.lib.normalize import normalize


def main():
    parser = argparse.ArgumentParser(description="Backfill wine_grapes from wine names")
    parser.add_argument("--dry-run", action="store_true", help="Preview without writing")
    parser.add_argument("--execute", action="store_true", help="Execute the backfill")
    parser.add_argument("--limit", type=int, default=0, help="Limit wines processed (0=all)")
    args = parser.parse_args()

    if not args.dry_run and not args.execute:
        print("Specify --dry-run or --execute")
        sys.exit(1)

    conn = get_conn()
    cur = conn.cursor()

    # Load grape reference, restricted to commercially significant varieties
    # to avoid false positives (e.g., "Columbia" grape matching Columbia Valley)
    cur.execute("SELECT id, display_name FROM grapes WHERE deleted_at IS NULL")
    grapes_raw = {normalize(row[1]): (str(row[0]), row[1]) for row in cur.fetchall()}

    # Curated set of unambiguous commercial grape names
    SAFE_GRAPES = {
        # Major reds
        "cabernet sauvignon", "pinot noir", "merlot", "syrah", "shiraz",
        "zinfandel", "malbec", "tempranillo", "sangiovese", "nebbiolo",
        "cabernet franc", "petit verdot", "grenache", "mourvedre", "carignan",
        "barbera", "primitivo", "gamay", "petite sirah", "carmenere",
        "tannat", "touriga nacional", "pinotage", "aglianico", "nero davola",
        "corvina", "montepulciano", "dolcetto", "bonarda", "negroamaro",
        "cinsault", "counoise", "graciano", "mencia", "blaufrankisch",
        "zweigelt", "st laurent", "trollinger", "dornfelder", "teroldego",
        "lagrein", "sagrantino", "nerello mascalese", "frappato", "cannonau",
        "xinomavro", "agiorgitiko",
        # Major whites
        "chardonnay", "sauvignon blanc", "riesling", "pinot grigio", "pinot gris",
        "pinot blanc", "gewurztraminer", "viognier", "chenin blanc",
        "gruner veltliner", "albarino", "torrontes", "verdejo", "vermentino",
        "trebbiano", "garganega", "arneis", "cortese", "fiano", "greco",
        "falanghina", "malvasia", "moscato", "muscat", "roussanne", "marsanne",
        "semillon", "melon de bourgogne", "muller thurgau", "silvaner",
        "scheurebe", "kerner", "furmint", "harslevelu", "welschriesling",
        "grillo", "catarratto", "assyrtiko", "godello", "txakoli",
        "colombard", "ugni blanc", "picpoul", "clairette",
        # Sparkling-related
        "glera",
        # Common blends referenced by name
        "cabernet", "grenache blanc", "grenache noir",
    }

    grape_lookup = {}
    for norm_name in SAFE_GRAPES:
        if norm_name in grapes_raw:
            grape_lookup[norm_name] = grapes_raw[norm_name]
        else:
            # Try without accents or slight variations
            for gn, val in grapes_raw.items():
                if gn == norm_name or gn.replace(" ", "") == norm_name.replace(" ", ""):
                    grape_lookup[norm_name] = val
                    break

    print(f"Matched {len(grape_lookup)}/{len(SAFE_GRAPES)} curated grapes to DB")

    # Sort by length descending for greedy matching
    grape_names_sorted = sorted(grape_lookup.keys(), key=len, reverse=True)

    # Build regex patterns for word-bounded matching
    grape_patterns = []
    for gn in grape_names_sorted:
        # Escape for regex and require word boundaries
        escaped = re.escape(gn)
        grape_patterns.append((gn, re.compile(r'\b' + escaped + r'\b')))

    # Fetch wines with 0 grape links
    limit_clause = f"LIMIT {args.limit}" if args.limit > 0 else ""
    cur.execute(f"""
        SELECT w.id, w.name, w.name_normalized
        FROM wines w
        WHERE w.deleted_at IS NULL
        AND NOT EXISTS (SELECT 1 FROM wine_grapes wg WHERE wg.wine_id = w.id)
        AND w.name_normalized IS NOT NULL
        ORDER BY w.created_at
        {limit_clause}
    """)
    wines = cur.fetchall()
    print(f"Wines with 0 grape links: {len(wines):,}")

    # Match grapes from wine names
    matches = []  # (wine_id, grape_id, grape_display_name, wine_name)
    for wine_id, wine_name, name_norm in wines:
        if not name_norm:
            continue

        # Greedy longest-match: find all grape names in the wine name
        remaining = name_norm
        found_grapes = []
        for gn, pattern in grape_patterns:
            if pattern.search(remaining):
                grape_id, display = grape_lookup[gn]
                found_grapes.append((grape_id, display))
                # Remove matched grape from remaining to prevent substring re-match
                remaining = pattern.sub('', remaining)

        for grape_id, display in found_grapes:
            matches.append((str(wine_id), grape_id, display, wine_name))

    # Deduplicate (wine_id, grape_id)
    seen = set()
    unique_matches = []
    for wine_id, grape_id, display, wine_name in matches:
        key = (wine_id, grape_id)
        if key not in seen:
            seen.add(key)
            unique_matches.append((wine_id, grape_id, display, wine_name))

    print(f"Grape links to create: {len(unique_matches):,}")
    print(f"Distinct wines affected: {len(set(m[0] for m in unique_matches)):,}")

    # Show distribution
    grape_dist = {}
    for _, _, display, _ in unique_matches:
        grape_dist[display] = grape_dist.get(display, 0) + 1
    print("\nTop grapes found:")
    for grape, count in sorted(grape_dist.items(), key=lambda x: -x[1])[:20]:
        print(f"  {grape:<25} {count:>6,}")

    if args.dry_run:
        # Show sample matches
        print("\nSample matches (first 15):")
        for wine_id, grape_id, display, wine_name in unique_matches[:15]:
            print(f"  {wine_name[:60]:<60} -> {display}")
        return

    if not args.execute:
        return

    # Execute: insert wine_grapes rows using batch VALUES
    print("\nInserting wine_grapes links...")
    inserted = 0
    batch_size = 1000
    for start in range(0, len(unique_matches), batch_size):
        batch = unique_matches[start:start + batch_size]
        values = [(wid, gid) for wid, gid, _, _ in batch]
        try:
            from psycopg2.extras import execute_values
            execute_values(
                cur,
                "INSERT INTO wine_grapes (wine_id, grape_id) VALUES %s ON CONFLICT (wine_id, grape_id) DO NOTHING",
                values,
                page_size=1000,
            )
            inserted += cur.rowcount
            conn.commit()
        except Exception as e:
            conn.rollback()
            print(f"  Batch error at {start}: {e}")

        if (start + batch_size) % 10000 < batch_size:
            print(f"  Processed {min(start + batch_size, len(unique_matches)):,}/{len(unique_matches):,}...")

    print(f"\nInserted: {inserted:,} wine_grapes links")


if __name__ == "__main__":
    main()
