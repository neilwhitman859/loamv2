"""
Promote grape data from importer + retailer + competition staging sources to wine_grapes.

Reads grape columns from matched staging rows (canonical_wine_id set), splits on common
delimiters, resolves via ReferenceResolver (aliases/synonyms/VIVC names), and bulk-inserts
into wine_grapes with ON CONFLICT DO NOTHING.

Direct source data only — no inference. Every insert traces back to a staging row's
grape/grapes/grape_type/varietal column value.

Usage:
    python -m pipeline.promote.importer_grape_promote [--dry-run]
"""

import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from pipeline.lib.db import get_conn
from pipeline.lib.resolve import ReferenceResolver
from psycopg2.extras import execute_values


# (source_table, grape_column, column_is_array)
SOURCES = [
    ("source_skurnik", "grape", False),
    ("source_bc_liquor", "grape_type", False),
    ("source_enofile", "varietal", False),
    ("source_flatiron", "grapes", True),
    ("source_berliner", "grapes", False),
    ("source_systembolaget", "grapes", True),
    ("source_winebow", "grape", False),
    ("source_european_cellars", "grape", False),
    ("source_empson", "grape", False),
    ("source_domestique", "grape", False),
]


# Split string on common delimiters, ignoring percentages
SPLIT_RE = re.compile(r"[,;/|&]|\s+and\s+|\s*\+\s*", re.IGNORECASE)
PCT_RE = re.compile(r"\d+\s*%")


def parse_grape_string(s):
    """Parse a free-text grape string into a list of raw grape name strings."""
    if not s:
        return []
    if isinstance(s, list):
        parts = [p for p in s if p]
    else:
        parts = SPLIT_RE.split(str(s))
    out = []
    for p in parts:
        p = PCT_RE.sub("", p).strip()
        p = re.sub(r"\s+", " ", p)
        p = p.strip(" .()")
        if len(p) >= 2:
            out.append(p)
    return out


def main():
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    print("Loading grape resolver...")
    resolver = ReferenceResolver(verbose=True)
    resolver.init_sync()

    conn = get_conn()
    cur = conn.cursor()

    all_inserts = set()
    source_stats = {}

    for table, col, is_array in SOURCES:
        print(f"\n{table}.{col} ...")
        cur.execute(f"""
            SELECT canonical_wine_id, {col}
            FROM {table}
            WHERE canonical_wine_id IS NOT NULL
              AND {col} IS NOT NULL
              {"AND array_length(" + col + ", 1) > 0" if is_array else "AND TRIM(" + col + "::text) != ''"}
        """)
        rows = cur.fetchall()
        print(f"  {len(rows)} matched rows with grape data")

        resolved_count = 0
        unresolved = {}
        inserts_from_source = 0

        for wine_id, grape_val in rows:
            names = parse_grape_string(grape_val)
            for name in names:
                g = resolver.resolve_grape(name)
                if g and g.get("id"):
                    key = (wine_id, g["id"])
                    if key not in all_inserts:
                        all_inserts.add(key)
                        inserts_from_source += 1
                    resolved_count += 1
                else:
                    unresolved[name] = unresolved.get(name, 0) + 1

        source_stats[table] = {
            "rows": len(rows),
            "resolved": resolved_count,
            "unique_inserts": inserts_from_source,
            "unresolved": sum(unresolved.values()),
        }
        print(f"  resolved: {resolved_count}, unique new grape links: {inserts_from_source}, unresolved: {sum(unresolved.values())}")
        if unresolved:
            top = sorted(unresolved.items(), key=lambda x: -x[1])[:5]
            for n, c in top:
                safe = n.encode("ascii", "replace").decode("ascii")
                print(f"    unresolved: {safe} ({c})")

    print(f"\nTotal unique (wine_id, grape_id) pairs: {len(all_inserts)}")
    print(f"Source stats:")
    for s, stats in source_stats.items():
        print(f"  {s}: {stats}")

    if args.dry_run:
        print("\nDRY RUN")
        cur.close()
        conn.close()
        return

    if not all_inserts:
        print("\nNothing to insert")
        cur.close()
        conn.close()
        return

    print(f"\nInserting {len(all_inserts)} grape links (ON CONFLICT DO NOTHING)...")
    # Get pre-count
    cur.execute("SELECT COUNT(*) FROM wine_grapes")
    pre = cur.fetchone()[0]

    execute_values(
        cur,
        "INSERT INTO wine_grapes (wine_id, grape_id) VALUES %s ON CONFLICT DO NOTHING",
        list(all_inserts),
        page_size=1000,
    )
    conn.commit()

    cur.execute("SELECT COUNT(*) FROM wine_grapes")
    post = cur.fetchone()[0]
    print(f"  wine_grapes: {pre} -> {post} (+{post - pre})")

    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
