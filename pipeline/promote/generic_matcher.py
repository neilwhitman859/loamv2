"""
Generic batch matcher for staging tables without dedicated batch_matcher adapters.

Uses in-memory normalized producer name matching against canonical producers,
then creates wines and links staging rows.

Usage:
    python -m pipeline.promote.generic_matcher --source enofile,polaner,... [--dry-run]
"""

import argparse
import sys
import time
import uuid
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from pipeline.lib.db import get_conn, get_supabase
from pipeline.lib.normalize import normalize, normalize_producer, slugify


# Source configs: table, producer column, wine name column
SOURCES = {
    'enofile': {
        'table': 'source_enofile',
        'producer_col': 'brand',
        'wine_name_col': None,  # brand IS the name for competitions
        'has_upc': False,
    },
    'polaner': {
        'table': 'source_polaner',
        'producer_col': 'producer',
        'wine_name_col': 'wine_name',
        'has_upc': False,
    },
    'firstleaf': {
        'table': 'source_firstleaf',
        'producer_col': 'vendor',
        'wine_name_col': 'title',
        'has_upc': False,
    },
    'best_wine_store': {
        'table': 'source_best_wine_store',
        'producer_col': 'producer',
        'wine_name_col': 'wine_name',
        'has_upc': False,
    },
    'domestique': {
        'table': 'source_domestique',
        'producer_col': 'producer',
        'wine_name_col': 'wine_name',
        'has_upc': False,
    },
    'last_bottle': {
        'table': 'source_last_bottle',
        'producer_col': 'producer',
        'wine_name_col': 'wine_name',
        'has_upc': False,
    },
    'horizon': {
        'table': 'source_horizon',
        'producer_col': 'brand',
        'wine_name_col': 'name',
        'has_upc': True,
        'upc_col': 'upc',
    },
    'openfoodfacts': {
        'table': 'source_openfoodfacts',
        'producer_col': 'brand',
        'wine_name_col': 'name',
        'has_upc': True,
        'upc_col': 'barcode',
    },
    'winedeals': {
        'table': 'source_winedeals',
        'producer_col': 'producer',
        'wine_name_col': 'name',
        'has_upc': True,
        'upc_col': 'upc',
    },
}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--source', default='enofile,polaner,firstleaf,best_wine_store,domestique,last_bottle,horizon,openfoodfacts,winedeals')
    parser.add_argument('--dry-run', action='store_true')
    args = parser.parse_args()

    sources = [s.strip() for s in args.source.split(',')]
    conn = get_conn()
    cur = conn.cursor()
    sb = get_supabase()

    # Load canonical producers into memory: normalized_name -> (id, country_id)
    print("Loading canonical producers...")
    cur.execute("""
        SELECT id, name_normalized, country_id FROM producers
        WHERE deleted_at IS NULL AND name_normalized IS NOT NULL
    """)
    prod_map = {}
    for pid, norm, cid in cur.fetchall():
        if norm not in prod_map:
            prod_map[norm] = (pid, cid)
    print(f"  {len(prod_map):,} producers in memory")

    # Also build suffix-stripped variants for matching
    # "Domaine de la Romanée-Conti" → "romanee conti"
    SUFFIXES = ['winery', 'vineyards', 'vineyard', 'wines', 'wine', 'cellars', 'cellar',
                'estate', 'estates', 'chateau', 'domaine', 'bodega', 'bodegas',
                'cantina', 'tenuta', 'casa', 'vinicola', 'azienda', 'maison']
    stripped_map = {}
    for norm, val in prod_map.items():
        words = norm.split()
        stripped = ' '.join(w for w in words if w not in SUFFIXES)
        if stripped and stripped != norm and len(stripped) > 2:
            if stripped not in stripped_map:
                stripped_map[stripped] = val
    print(f"  {len(stripped_map):,} suffix-stripped variants")

    total_matched = 0
    total_created = 0

    for source_key in sources:
        if source_key not in SOURCES:
            print(f"Unknown source: {source_key}")
            continue

        config = SOURCES[source_key]
        table = config['table']
        producer_col = config['producer_col']
        wine_name_col = config['wine_name_col']

        print(f"\n{'=' * 60}")
        print(f"{source_key.upper()}")
        print(f"{'=' * 60}")

        # Load unlinked records
        cols = f"id,{producer_col}"
        if wine_name_col:
            cols += f",{wine_name_col}"
        cur.execute(f"""
            SELECT {cols} FROM {table}
            WHERE canonical_producer_id IS NULL
        """)
        rows = cur.fetchall()
        print(f"  {len(rows):,} unlinked records")

        if not rows:
            continue

        # Match producers
        matched = 0
        unmatched_producers = set()
        updates = []  # (staging_id, producer_id, country_id)

        for row in rows:
            staging_id = row[0]
            producer_name = row[1]
            if not producer_name:
                continue

            norm = normalize_producer(producer_name)
            if not norm:
                continue

            # Try exact match first, then suffix-stripped
            match = prod_map.get(norm) or stripped_map.get(norm)
            if match:
                updates.append((staging_id, match[0], match[1]))
                matched += 1
            else:
                unmatched_producers.add(norm)

        print(f"  Producer matches: {matched:,} / {len(rows):,}")
        print(f"  Unmatched unique producers: {len(unmatched_producers):,}")

        if not args.dry_run and updates:
            # Batch update staging rows with canonical_producer_id
            from psycopg2.extras import execute_batch
            execute_batch(
                cur,
                f"UPDATE {table} SET canonical_producer_id = %s WHERE id = %s",
                [(pid, sid) for sid, pid, _ in updates],
                page_size=1000,
            )
            conn.commit()
            print(f"  Producers linked: {matched:,}")

        total_matched += matched

        # Now create wines from matched records using the REST API approach
        # (similar to retail_wine_create but simpler)
        if wine_name_col and matched > 0:
            print("  Creating wines from matched records...")
            wine_created = 0
            wine_linked = 0

            # Group by producer
            producer_wines = {}
            for row in rows:
                staging_id = row[0]
                producer_name = row[1]
                wine_name = row[2] if len(row) > 2 else producer_name

                if not producer_name or not wine_name:
                    continue

                norm_prod = normalize_producer(producer_name)
                match = prod_map.get(norm_prod) or stripped_map.get(norm_prod)
                if not match:
                    continue

                pid = match[0]
                cid = match[1]
                name_norm = normalize(wine_name)
                if not name_norm or len(name_norm) < 2:
                    continue

                if pid not in producer_wines:
                    producer_wines[pid] = {}
                if name_norm not in producer_wines[pid]:
                    producer_wines[pid][name_norm] = {
                        'display': wine_name,
                        'country_id': cid,
                        'staging_ids': [],
                    }
                producer_wines[pid][name_norm]['staging_ids'].append(staging_id)

            # Check existing wines
            unique_count = sum(len(w) for w in producer_wines.values())
            print(f"  {unique_count:,} unique wine names")

            # Load existing wine names per producer (batch)
            existing = set()
            all_pids = list(producer_wines.keys())
            for i in range(0, len(all_pids), 100):
                batch_pids = all_pids[i:i+100]
                placeholders = ','.join(['%s'] * len(batch_pids))
                cur.execute(f"""
                    SELECT producer_id, name_normalized FROM wines
                    WHERE producer_id IN ({placeholders}) AND deleted_at IS NULL
                """, batch_pids)
                for p, n in cur.fetchall():
                    existing.add((p, n))

            # Create wines and link
            for pid, wines in producer_wines.items():
                for name_norm, data in wines.items():
                    wine_id = None
                    if (pid, name_norm) in existing:
                        # Find existing wine ID
                        cur.execute("""
                            SELECT id FROM wines
                            WHERE producer_id = %s AND name_normalized = %s AND deleted_at IS NULL
                            LIMIT 1
                        """, (pid, name_norm))
                        row = cur.fetchone()
                        if row:
                            wine_id = row[0]
                    else:
                        # Create new wine
                        if not args.dry_run:
                            short_id = uuid.uuid4().hex[:8]
                            display = data['display']
                            if display == display.upper():
                                display = display.title()
                            new_id = str(uuid.uuid4())
                            try:
                                cur.execute("""
                                    INSERT INTO wines (id, name, name_normalized, slug, producer_id, country_id, wine_type, effervescence)
                                    VALUES (%s, %s, %s, %s, %s, %s, 'table', 'still')
                                    ON CONFLICT DO NOTHING
                                    RETURNING id
                                """, (
                                    new_id, display, name_norm,
                                    slugify(display)[:180] + f'-gm-{short_id}',
                                    pid, data['country_id'],
                                ))
                                result = cur.fetchone()
                                if result:
                                    wine_id = result[0]
                                    wine_created += 1
                                    existing.add((pid, name_norm))
                            except Exception as e:
                                if wine_created < 3:
                                    print(f"    Error: {str(e)[:100]}")
                        else:
                            wine_created += 1

                    # Link staging rows
                    if wine_id and not args.dry_run:
                        for sid in data['staging_ids']:
                            cur.execute(f"""
                                UPDATE {table} SET canonical_wine_id = %s WHERE id = %s
                            """, (wine_id, sid))
                            wine_linked += 1

            if not args.dry_run:
                conn.commit()

            print(f"  Wines created: {wine_created:,}, rows linked: {wine_linked:,}")
            total_created += wine_created

    print(f"\n{'=' * 60}")
    print(f"TOTAL: {total_matched:,} producers matched, {total_created:,} wines created")
    if args.dry_run:
        print("  ** DRY RUN **")

    cur.close()
    conn.close()


if __name__ == '__main__':
    main()
