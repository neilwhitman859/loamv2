"""
COLA-keyed deterministic merge: link state database records to canonical wines
via shared TTB COLA numbers.

State databases (PRO Platform, TABC, Kansas, WV) store TTB COLA numbers.
TTB COLA records are already linked to canonical wines. This script bridges
state DB records → TTB → canonical wines via pure key matching.

Then promotes depth data (vintages, appellations, ABV) from the newly linked records.

Usage:
    python -m pipeline.promote.cola_merge [--dry-run] [--source pro,tabc,kansas,wv]
"""

import argparse
import re
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from pipeline.lib.db import get_conn, get_supabase
from pipeline.lib.normalize import normalize
from pipeline.lib.resolve import ReferenceResolver


SOURCE_CONFIG = {
    'pro': {
        'table': 'source_pro_platform',
        'cola_col': 'cola_number',
        'has_vintage': True,
        'vintage_col': 'vintage',
        'has_appellation': True,
        'appellation_col': 'appellation',
        'has_abv': True,
        'abv_col': 'abv',
    },
    'tabc': {
        'table': 'source_tabc',
        'cola_col': 'ttb_number',
        'has_vintage': False,
        'has_appellation': False,
        'has_abv': True,
        'abv_col': 'alcohol_content',
    },
    'kansas': {
        'table': 'source_kansas_brands',
        'cola_col': 'cola_number',
        'has_vintage': False,
        'has_appellation': False,
        'has_abv': False,
    },
    'wv': {
        'table': 'source_wv_abca',
        'cola_col': 'ttb',
        'has_vintage': False,
        'has_appellation': False,
        'has_abv': False,
    },
}


def link_source(conn, source_key, config, dry_run=False):
    """Link a state source to canonical wines via TTB COLA keys."""
    cur = conn.cursor()
    table = config['table']
    cola_col = config['cola_col']

    print(f"\n{'=' * 60}")
    print(f"{source_key.upper()} COLA MERGE")
    print(f"{'=' * 60}")

    # Step 1: Link canonical_wine_id via TTB
    print("  Linking wines via TTB COLA keys...")
    if dry_run:
        cur.execute(f"""
            SELECT count(*)
            FROM {table} s
            JOIN source_ttb_colas t ON t.ttb_id = s.{cola_col}
            WHERE s.canonical_wine_id IS NULL
              AND t.canonical_wine_id IS NOT NULL
        """)
        count = cur.fetchone()[0]
        print(f"    {count} records would be linked (DRY RUN)")
        cur.close()
        return count

    cur.execute(f"""
        UPDATE {table} s
        SET canonical_wine_id = t.canonical_wine_id,
            canonical_producer_id = COALESCE(s.canonical_producer_id, t.canonical_producer_id)
        FROM source_ttb_colas t
        WHERE t.ttb_id = s.{cola_col}
          AND s.canonical_wine_id IS NULL
          AND t.canonical_wine_id IS NOT NULL
    """)
    linked = cur.rowcount
    conn.commit()
    print(f"    {linked:,} records linked")

    cur.close()
    return linked


def promote_pro_platform_depth(conn, sb, resolver, dry_run=False):
    """Promote vintages, appellations, and ABV from newly linked PRO Platform records."""
    cur = conn.cursor()

    print("\n  Promoting PRO Platform depth data...")

    # Get newly linked records with vintage/appellation/ABV
    cur.execute("""
        SELECT pp.canonical_wine_id, pp.vintage, pp.appellation, pp.abv
        FROM source_pro_platform pp
        WHERE pp.canonical_wine_id IS NOT NULL
    """)
    rows = cur.fetchall()
    print(f"    {len(rows):,} linked records to process")

    # Load existing vintages
    cur.execute("SELECT wine_id, vintage_year FROM wine_vintages")
    existing_vintages = set((r[0], r[1]) for r in cur.fetchall())
    print(f"    {len(existing_vintages):,} existing vintages")

    # Collect new vintages
    vintage_inserts = []
    seen = set()
    for wine_id, vintage_str, appellation_str, abv_str in rows:
        if not vintage_str:
            continue
        try:
            vy = int(str(vintage_str)[:4])
            if not (1900 <= vy <= 2030):
                continue
        except (ValueError, TypeError):
            continue

        key = (wine_id, vy)
        if key in existing_vintages or key in seen:
            continue
        seen.add(key)

        row_data = {'wine_id': wine_id, 'vintage_year': vy}
        if abv_str:
            try:
                abv = float(abv_str)
                if 3.0 <= abv <= 25.0:
                    row_data['abv'] = abv
            except (ValueError, TypeError):
                pass
        vintage_inserts.append(row_data)

    print(f"    {len(vintage_inserts):,} new vintages to create")

    if not dry_run and vintage_inserts:
        from psycopg2.extras import execute_values
        # Batch insert vintages
        vals = [(v['wine_id'], v['vintage_year'], v.get('abv'))
                for v in vintage_inserts]
        execute_values(
            cur,
            """INSERT INTO wine_vintages (wine_id, vintage_year, abv)
               VALUES %s ON CONFLICT (wine_id, vintage_year) DO NOTHING""",
            vals,
            page_size=2000,
        )
        inserted = cur.rowcount
        conn.commit()
        print(f"    Vintages inserted: {inserted:,}")

    # Promote appellations: update wines missing appellation_id
    if resolver:
        print("  Promoting appellations...")
        cur.execute("""
            SELECT DISTINCT pp.canonical_wine_id, pp.appellation
            FROM source_pro_platform pp
            JOIN wines w ON w.id = pp.canonical_wine_id
            WHERE pp.canonical_wine_id IS NOT NULL
              AND pp.appellation IS NOT NULL
              AND w.appellation_id IS NULL
              AND w.deleted_at IS NULL
        """)
        appellation_rows = cur.fetchall()
        print(f"    {len(appellation_rows):,} wines need appellation")

        appellation_updates = 0
        for wine_id, app_str in appellation_rows:
            resolved = resolver.resolve_appellation(app_str)
            if resolved:
                if not dry_run:
                    cur.execute(
                        "UPDATE wines SET appellation_id = %s WHERE id = %s AND appellation_id IS NULL",
                        (resolved['id'], wine_id)
                    )
                appellation_updates += 1

        if not dry_run:
            conn.commit()
        print(f"    Appellations set: {appellation_updates:,}")

    cur.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--dry-run', action='store_true')
    parser.add_argument('--source', default='pro,tabc,kansas,wv')
    args = parser.parse_args()

    sources = [s.strip() for s in args.source.split(',')]
    conn = get_conn()
    sb = get_supabase()

    # Load resolver for appellation matching
    resolver = None
    if 'pro' in sources:
        print("Loading reference resolver...")
        resolver = ReferenceResolver(verbose=True)
        resolver.init_sync()

    total_linked = 0
    for source_key in sources:
        if source_key not in SOURCE_CONFIG:
            print(f"Unknown source: {source_key}")
            continue
        config = SOURCE_CONFIG[source_key]
        linked = link_source(conn, source_key, config, args.dry_run)
        total_linked += linked

    # Promote depth data from PRO Platform (richest state source)
    if 'pro' in sources:
        promote_pro_platform_depth(conn, sb, resolver, args.dry_run)

    print(f"\n{'=' * 60}")
    print(f"TOTAL: {total_linked:,} records linked across {len(sources)} sources")
    if args.dry_run:
        print("  ** DRY RUN — no writes **")

    conn.close()


if __name__ == '__main__':
    main()
