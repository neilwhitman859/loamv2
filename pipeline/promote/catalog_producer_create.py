"""
Create canonical producers from curated catalog sources with explicit producer columns.

For staging sources like Enofile, Systembolaget, WineDeals, etc., the producer name
is a real, vetted brand from a legitimate catalog. We create canonical producers directly
without requiring TTB confirmation (many are European producers TTB will never have).

Matches against existing canonical producers first (normalized name). Only creates
new producers for names that don't already exist.

Usage:
    python -m pipeline.promote.catalog_producer_create [--dry-run] [--source enofile,systembolaget,...]
"""

import sys
import time
import uuid
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from pipeline.lib.db import get_conn
from pipeline.lib.normalize import normalize, slugify


SOURCE_CONFIG = {
    'enofile': {
        'table': 'source_enofile',
        'producer_col': 'brand',
        'price_col': 'price',
    },
    'systembolaget': {
        'table': 'source_systembolaget',
        'producer_col': 'producer',
        'price_col': 'price_sek',
    },
    'winedeals': {
        'table': 'source_winedeals',
        'producer_col': 'producer',
        'price_col': 'price_usd',
    },
    'best_wine_store': {
        'table': 'source_best_wine_store',
        'producer_col': 'producer',
        'price_col': 'price_usd',
    },
    'domestique': {
        'table': 'source_domestique',
        'producer_col': 'producer',
        'price_col': 'price_usd',
    },
    'flatiron': {
        'table': 'source_flatiron',
        'producer_col': 'producer',
        'price_col': 'price',
    },
    'pa': {
        'table': 'source_pa',
        'producer_col': 'brand_name',
        'price_col': 'retail_price',
    },
}


def title_case_brand(name):
    """Convert ALL-CAPS name to title case with wine-word awareness."""
    if not name or name != name.upper() or len(name) <= 3:
        return name
    words = name.title().split()
    lowercase_words = {'De', 'La', 'Le', 'Les', 'Du', 'Des', 'Di', 'Del', 'Della',
                       'Delle', 'Dei', 'Da', 'Das', 'Do', 'Dos', 'El', 'En', 'Et',
                       'Von', 'Van', 'Der', 'Den', 'Und', 'The', 'And', 'Of', 'In'}
    result = []
    for i, w in enumerate(words):
        if i > 0 and w in lowercase_words:
            result.append(w.lower())
        else:
            result.append(w)
    return ' '.join(result)


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--dry-run', action='store_true')
    parser.add_argument('--source', default=','.join(SOURCE_CONFIG.keys()),
                        help="Comma-separated source keys")
    args = parser.parse_args()

    sources = [s.strip() for s in args.source.split(',')]
    conn = get_conn()
    cur = conn.cursor()
    t0 = time.time()

    # Load existing canonical producer index
    print("Loading canonical producer index...")
    cur.execute("SELECT id, name_normalized FROM producers WHERE deleted_at IS NULL")
    producer_by_norm = {}
    for pid, pnorm in cur.fetchall():
        if pnorm:
            producer_by_norm[pnorm] = pid
    print(f"  {len(producer_by_norm)} canonical producers loaded")

    total_linked = 0
    total_created = 0
    total_rows = 0

    for source_key in sources:
        config = SOURCE_CONFIG.get(source_key)
        if not config:
            print(f"Unknown source: {source_key}")
            continue

        table = config['table']
        prod_col = config['producer_col']
        price_col = config['price_col']

        print(f"\n{'=' * 60}")
        print(f"{source_key.upper()}")
        print(f"{'=' * 60}")

        # Load unmatched rows with prices and producer names
        cur.execute(f"""
            SELECT id, {prod_col} as producer_name
            FROM {table}
            WHERE canonical_producer_id IS NULL
              AND {price_col} IS NOT NULL
              AND {prod_col} IS NOT NULL
              AND TRIM({prod_col}) != ''
        """)
        rows = cur.fetchall()
        print(f"  {len(rows)} unmatched rows with prices")

        if not rows:
            continue

        # Group by normalized producer name
        producer_groups = defaultdict(list)  # norm -> [(row_id, raw_name)]
        for row_id, raw_name in rows:
            norm = normalize(raw_name.strip())
            if norm and len(norm) >= 3:  # minimum 3 chars for a producer name
                producer_groups[norm].append((row_id, raw_name))

        print(f"  {len(producer_groups)} distinct producer names")

        linked_existing = 0
        created_new = 0
        rows_updated = 0
        skipped = 0

        for norm, group_rows in producer_groups.items():
            canonical_pid = None
            display_name = group_rows[0][1].strip()

            # Check if canonical producer already exists
            existing_pid = producer_by_norm.get(norm)
            if existing_pid:
                canonical_pid = existing_pid
                linked_existing += 1
            else:
                # Create new canonical producer
                if args.dry_run:
                    created_new += 1
                    rows_updated += len(group_rows)
                    producer_by_norm[norm] = 'dry-run'
                    continue

                # Clean display name
                if display_name == display_name.upper():
                    display_name = title_case_brand(display_name)

                slug = slugify(display_name)[:180] + f'-cp-{uuid.uuid4().hex[:8]}'
                name_norm = normalize(display_name)

                try:
                    cur.execute("""
                        INSERT INTO producers (name, name_normalized, slug, producer_type)
                        VALUES (%s, %s, %s, 'estate')
                        ON CONFLICT DO NOTHING
                        RETURNING id
                    """, (display_name, name_norm, slug))
                    result = cur.fetchone()
                    if result:
                        canonical_pid = result[0]
                        created_new += 1
                        producer_by_norm[name_norm] = canonical_pid
                    else:
                        # Conflict — find existing
                        cur.execute("SELECT id FROM producers WHERE name_normalized = %s AND deleted_at IS NULL LIMIT 1",
                                    (name_norm,))
                        r = cur.fetchone()
                        if r:
                            canonical_pid = r[0]
                            linked_existing += 1
                            producer_by_norm[name_norm] = canonical_pid
                        else:
                            skipped += len(group_rows)
                            continue
                except Exception as e:
                    conn.rollback()
                    skipped += len(group_rows)
                    if created_new + linked_existing < 3:
                        print(f"    Error creating '{display_name}': {str(e)[:100]}")
                    continue

            if canonical_pid and not args.dry_run:
                row_ids = [r[0] for r in group_rows]
                for i in range(0, len(row_ids), 500):
                    batch = row_ids[i:i + 500]
                    placeholders = ','.join(['%s'] * len(batch))
                    cur.execute(f"""
                        UPDATE {table}
                        SET canonical_producer_id = %s
                        WHERE id IN ({placeholders})
                          AND canonical_producer_id IS NULL
                    """, [canonical_pid] + batch)
                    rows_updated += cur.rowcount

                conn.commit()
            elif canonical_pid:
                rows_updated += len(group_rows)

        if not args.dry_run:
            conn.commit()

        print(f"  Linked to existing: {linked_existing}, Created new: {created_new}, "
              f"Rows updated: {rows_updated}, Skipped: {skipped}")
        total_linked += linked_existing
        total_created += created_new
        total_rows += rows_updated

    elapsed = time.time() - t0
    print(f"\n{'=' * 60}")
    print(f"TOTAL: {total_linked} linked to existing, {total_created} new producers, "
          f"{total_rows} staging rows updated ({elapsed:.0f}s)")
    if args.dry_run:
        print("  ** DRY RUN — no writes **")

    conn.close()


if __name__ == '__main__':
    main()
