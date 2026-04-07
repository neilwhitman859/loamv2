"""
Fill wines.color from TTB class_type codes.

TTB class_type is a regulatory code that encodes wine color:
  - Codes starting with '80' prefix = red table wine
  - Codes starting with '81' prefix = white table wine
  - Codes starting with '82' prefix = rosé table wine

This is definitional — TTB assigns class_type based on the actual product
submitted for label approval. Not inference.

Usage:
    python -m pipeline.promote.ttb_color_fill --dry-run
    python -m pipeline.promote.ttb_color_fill --execute
"""

import argparse
import sys
from pipeline.lib.db import get_conn


# TTB class_type prefix → wine color mapping
# Source: 27 CFR Part 4 — Labeling and Advertising of Wine
# Class 80x = red, 81x = white, 82x = rosé
# 2-digit codes (80, 81, 82) and 3+ digit codes (800-809, 810-819, 820-829,
# 8000-8009, 8100-8199, etc.) follow the same prefix rule.
TTB_COLOR_MAP = {
    '80': 'red',
    '81': 'white',
    '82': 'rose',
}


def determine_color_from_class_type(class_type: str) -> str | None:
    """Map a TTB class_type code to a wine color."""
    if not class_type:
        return None
    ct = class_type.strip()
    # Check 2-char prefix
    if len(ct) >= 2:
        prefix = ct[:2]
        return TTB_COLOR_MAP.get(prefix)
    return None


def analyze(conn):
    """Analyze the TTB class_type → color opportunity."""
    cur = conn.cursor()

    # First: what class_type codes exist and how do they distribute?
    print("=== TTB class_type code distribution (linked wines only) ===")
    cur.execute("""
        SELECT
            LEFT(class_type, 2) as prefix,
            count(*) as ttb_records,
            count(DISTINCT canonical_wine_id) as distinct_wines
        FROM source_ttb_colas
        WHERE canonical_wine_id IS NOT NULL
          AND class_type IS NOT NULL
        GROUP BY LEFT(class_type, 2)
        ORDER BY count(*) DESC
    """)
    rows = cur.fetchall()
    for prefix, ttb_count, wine_count in rows:
        color = TTB_COLOR_MAP.get(prefix, '???')
        print(f"  Prefix {prefix} ({color:>8s}): {ttb_count:>8,} TTB records, {wine_count:>7,} distinct wines")

    # Now: how many colorless wines can we fill?
    print("\n=== Fillable colorless wines ===")
    cur.execute("""
        WITH ttb_colors AS (
            SELECT
                canonical_wine_id,
                LEFT(class_type, 2) as prefix,
                count(*) as ttb_count
            FROM source_ttb_colas
            WHERE canonical_wine_id IS NOT NULL
              AND class_type IS NOT NULL
              AND LEFT(class_type, 2) IN ('80', '81', '82')
            GROUP BY canonical_wine_id, LEFT(class_type, 2)
        ),
        -- wines where ALL TTB records agree on the same color prefix
        consistent_colors AS (
            SELECT
                canonical_wine_id,
                CASE WHEN count(DISTINCT prefix) = 1 THEN min(prefix) ELSE NULL END as agreed_prefix
            FROM ttb_colors
            GROUP BY canonical_wine_id
        )
        SELECT
            CASE agreed_prefix
                WHEN '80' THEN 'red'
                WHEN '81' THEN 'white'
                WHEN '82' THEN 'rose'
            END as color,
            count(*) as fillable
        FROM consistent_colors cc
        JOIN wines w ON w.id = cc.canonical_wine_id
        WHERE w.color IS NULL
          AND cc.agreed_prefix IS NOT NULL
        GROUP BY agreed_prefix
        ORDER BY count(*) DESC
    """)
    rows = cur.fetchall()
    total = 0
    for color, count in rows:
        print(f"  {color:>8s}: {count:>7,} wines")
        total += count
    print(f"  {'TOTAL':>8s}: {total:>7,} wines")

    # Check for conflicts (wines with mixed TTB color codes)
    cur.execute("""
        WITH ttb_colors AS (
            SELECT
                canonical_wine_id,
                LEFT(class_type, 2) as prefix
            FROM source_ttb_colas
            WHERE canonical_wine_id IS NOT NULL
              AND class_type IS NOT NULL
              AND LEFT(class_type, 2) IN ('80', '81', '82')
            GROUP BY canonical_wine_id, LEFT(class_type, 2)
        )
        SELECT count(DISTINCT canonical_wine_id) as conflicting_wines
        FROM ttb_colors
        WHERE canonical_wine_id IN (
            SELECT canonical_wine_id
            FROM ttb_colors
            GROUP BY canonical_wine_id
            HAVING count(DISTINCT prefix) > 1
        )
    """)
    conflicts = cur.fetchone()[0]
    print(f"\n  Wines with conflicting TTB color codes (skipped): {conflicts:,}")

    cur.close()
    return total


def execute(conn):
    """Fill wines.color from TTB class_type codes."""
    cur = conn.cursor()

    # Build the update: only fill where ALL TTB records agree on color
    cur.execute("""
        WITH ttb_colors AS (
            SELECT
                canonical_wine_id,
                LEFT(class_type, 2) as prefix
            FROM source_ttb_colas
            WHERE canonical_wine_id IS NOT NULL
              AND class_type IS NOT NULL
              AND LEFT(class_type, 2) IN ('80', '81', '82')
            GROUP BY canonical_wine_id, LEFT(class_type, 2)
        ),
        consistent_colors AS (
            SELECT
                canonical_wine_id,
                min(prefix) as agreed_prefix
            FROM ttb_colors
            GROUP BY canonical_wine_id
            HAVING count(DISTINCT prefix) = 1
        )
        UPDATE wines w
        SET color = CASE cc.agreed_prefix
            WHEN '80' THEN 'red'
            WHEN '81' THEN 'white'
            WHEN '82' THEN 'rose'
        END
        FROM consistent_colors cc
        WHERE w.id = cc.canonical_wine_id
          AND w.color IS NULL
    """)

    updated = cur.rowcount
    conn.commit()
    cur.close()
    return updated


def main():
    parser = argparse.ArgumentParser(description="Fill wines.color from TTB class_type codes")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--dry-run', action='store_true', help='Analyze only, no changes')
    group.add_argument('--execute', action='store_true', help='Apply color fills')
    args = parser.parse_args()

    conn = get_conn()
    try:
        total = analyze(conn)

        if args.execute and total > 0:
            print(f"\n=== Executing color fill ===")
            updated = execute(conn)
            print(f"Updated {updated:,} wines with color from TTB class_type codes")

            # Verify new coverage
            cur = conn.cursor()
            cur.execute("""
                SELECT count(*) as total, count(color) as has_color,
                       round(100.0 * count(color) / count(*), 1) as pct
                FROM wines
            """)
            total_w, has_color, pct = cur.fetchone()
            print(f"\nColor coverage: {has_color:,} / {total_w:,} ({pct}%)")
            cur.close()
        elif args.dry_run:
            print("\n(dry run — no changes made)")
    finally:
        conn.close()


if __name__ == '__main__':
    main()
