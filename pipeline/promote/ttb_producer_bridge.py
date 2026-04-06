"""
TTB Producer Bridge: Link unmatched staging rows to canonical producers via TTB brand matching.

For staging sources with explicit producer columns, normalizes the producer name and matches
against TTB brand_name. If the TTB brand has a canonical_producer_id, links directly.
If the TTB brand exists but has no canonical producer, creates one (TTB confirms it's real).

Conservative: only creates producers confirmed by TTB COLA registry.

Usage:
    python -m pipeline.promote.ttb_producer_bridge [--dry-run]
"""

import re
import sys
import time
import uuid
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from pipeline.lib.db import get_conn
from pipeline.lib.normalize import normalize, slugify


# Sources with explicit producer columns and price data
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
    'pa': {
        'table': 'source_pa',
        'producer_col': 'brand_name',
        'price_col': 'retail_price',
    },
}


def normalize_brand(name):
    """Normalize a brand/producer name for matching (same as batch_matcher)."""
    if not name:
        return ''
    return normalize(name.strip())


def title_case_brand(name):
    """Convert ALL-CAPS TTB brand name to title case, preserving common wine words."""
    if not name:
        return name
    if name != name.upper():
        return name  # Already mixed case
    # Title-case with exceptions
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
    args = parser.parse_args()

    conn = get_conn()
    cur = conn.cursor()
    t0 = time.time()

    # Step 1: Load TTB brand index (normalized -> canonical_producer_id + display name)
    print("Loading TTB brand index...")
    cur.execute("""
        SELECT DISTINCT ON (regexp_replace(public.unaccent(LOWER(TRIM(brand_name))), '[^a-z0-9]', '', 'g'))
            brand_name,
            regexp_replace(public.unaccent(LOWER(TRIM(brand_name))), '[^a-z0-9]', '', 'g') as norm,
            canonical_producer_id
        FROM source_ttb_colas
        WHERE brand_name IS NOT NULL
        ORDER BY regexp_replace(public.unaccent(LOWER(TRIM(brand_name))), '[^a-z0-9]', '', 'g'),
                 canonical_producer_id NULLS LAST
    """)
    ttb_index = {}  # norm -> {brand_name, canonical_producer_id}
    for brand_name, norm, cpid in cur.fetchall():
        if norm and len(norm) >= 2:
            ttb_index[norm] = {'brand_name': brand_name, 'canonical_producer_id': cpid}
    print(f"  {len(ttb_index)} distinct TTB brands loaded")

    # Step 2: Load existing canonical producers index
    print("Loading canonical producer index...")
    cur.execute("SELECT id, name, name_normalized, country_id FROM producers WHERE deleted_at IS NULL")
    producer_by_norm = {}  # name_normalized -> {id, name, country_id}
    for pid, pname, pnorm, cid in cur.fetchall():
        if pnorm:
            producer_by_norm[pnorm] = {'id': pid, 'name': pname, 'country_id': cid}
    print(f"  {len(producer_by_norm)} canonical producers loaded")

    total_linked = 0
    total_created = 0
    total_rows = 0

    for source_key, config in SOURCE_CONFIG.items():
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
            norm = normalize_brand(raw_name)
            if norm and len(norm) >= 2:
                producer_groups[norm].append((row_id, raw_name))

        print(f"  {len(producer_groups)} distinct producer names")

        linked_existing = 0
        created_new = 0
        rows_updated = 0
        skipped = 0

        for norm, group_rows in producer_groups.items():
            # Check TTB brand index
            ttb_entry = ttb_index.get(norm)
            if not ttb_entry:
                skipped += len(group_rows)
                continue

            canonical_pid = None
            display_name = group_rows[0][1].strip()  # Use source's display name (better casing than TTB)

            # Path A: TTB brand already linked to canonical producer
            if ttb_entry['canonical_producer_id']:
                canonical_pid = ttb_entry['canonical_producer_id']
                linked_existing += 1
            else:
                # Path B: Check if canonical producer exists with this normalized name
                existing = producer_by_norm.get(norm)
                if existing:
                    canonical_pid = existing['id']
                    linked_existing += 1
                else:
                    # Path C: Create new canonical producer (TTB confirms brand is real)
                    if args.dry_run:
                        created_new += 1
                        rows_updated += len(group_rows)
                        continue

                    # Use source display name if mixed case, else title-case the TTB name
                    if display_name == display_name.upper():
                        display_name = title_case_brand(display_name)

                    slug = slugify(display_name)[:180] + f'-tb-{uuid.uuid4().hex[:8]}'
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
                            producer_by_norm[name_norm] = {'id': canonical_pid, 'name': display_name, 'country_id': None}
                        else:
                            # Conflict — try to find it
                            cur.execute("SELECT id FROM producers WHERE name_normalized = %s AND deleted_at IS NULL LIMIT 1", (name_norm,))
                            r = cur.fetchone()
                            if r:
                                canonical_pid = r[0]
                                linked_existing += 1
                            else:
                                skipped += len(group_rows)
                                continue
                    except Exception as e:
                        conn.rollback()
                        skipped += len(group_rows)
                        if created_new + linked_existing < 3:
                            print(f"    Error creating producer '{display_name}': {str(e)[:100]}")
                        continue

            if canonical_pid and not args.dry_run:
                # Update staging rows with canonical_producer_id
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
              f"Rows updated: {rows_updated}, Skipped (no TTB match): {skipped}")
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
