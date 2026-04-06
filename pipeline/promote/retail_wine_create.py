"""
Create canonical wines from producer-matched retail staging records.

For each retail source, finds records that have a canonical_producer_id but no
canonical_wine_id. Extracts the wine name, normalizes it, checks for duplicates
within the producer, and creates new wines.

Then links the staging record back to the new wine (sets canonical_wine_id).

Usage:
    python -m pipeline.promote.retail_wine_create [--dry-run] [--source specs,wallys,...]
"""

import argparse
import re
import sys
import time
import uuid
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from pipeline.lib.db import get_conn
from pipeline.lib.normalize import normalize, slugify, normalize_wine_name


def extract_wine_from_retail(title):
    """Extract wine name from a retail product title, stripping vintage, size."""
    if not title:
        return ''
    name = title.strip()
    # Strip trailing bottle size: "750ml", "1.5L", etc.
    name = re.sub(r'\s*\d+\s*ml\b.*$', '', name, flags=re.I)
    name = re.sub(r'\s*\d+(\.\d+)?\s*[Ll]\b.*$', '', name)
    # Strip leading vintage year
    name = re.sub(r'^\d{4}\s+', '', name)
    # Strip trailing vintage year
    name = re.sub(r'\s+\d{4}\s*$', '', name)
    return name.strip()


def strip_producer_prefix(wine_title, producer_name):
    """Strip producer name from the beginning of a wine title."""
    if not wine_title or not producer_name:
        return wine_title or ''
    title_lower = wine_title.lower().strip()
    prod_lower = producer_name.lower().strip()
    if title_lower.startswith(prod_lower):
        remainder = wine_title[len(producer_name):].lstrip(' ,:-')
        return remainder.strip() if remainder.strip() else wine_title
    # Also try with possessives removed: "Jacob's Creek" vs "Jacobs Creek"
    prod_noposs = prod_lower.replace("'s ", "s ").replace("'", "")
    title_noposs = title_lower.replace("'s ", "s ").replace("'", "")
    if title_noposs.startswith(prod_noposs):
        remainder = wine_title[len(producer_name):].lstrip(' ,:-')
        # May be off by a char or two due to apostrophe differences, be safe
        if remainder.strip():
            return remainder.strip()
    return wine_title


# Source configs: table name, columns to select, how to extract wine name
SOURCE_CONFIG = {
    'specs': {
        'table': 'source_specs',
        'select': 'id, canonical_producer_id, name',
        'needs_producer_strip': True,
    },
    'wallys': {
        'table': 'source_wallys',
        'select': 'id, canonical_producer_id, title',
        'name_col': 'title',
        'needs_producer_strip': True,
    },
    'systembolaget': {
        'table': 'source_systembolaget',
        'select': 'id, canonical_producer_id, name_bold, name_thin, producer',
        'needs_producer_strip': False,  # name_thin is already wine-only
    },
    'lcbo': {
        'table': 'source_lcbo',
        'select': 'id, canonical_producer_id, name',
        'needs_producer_strip': True,
    },
    'flatiron': {
        'table': 'source_flatiron',
        'select': 'id, canonical_producer_id, title, producer',
        'needs_producer_strip': True,
    },
    'bc_liquor': {
        'table': 'source_bc_liquor',
        'select': 'id, canonical_producer_id, name',
        'needs_producer_strip': True,
    },
    'enofile': {
        'table': 'source_enofile',
        'select': 'id, canonical_producer_id, varietal, designation, addl_designation',
        'needs_producer_strip': False,  # wine name composed from varietal+designation
    },
    'pa': {
        'table': 'source_pa',
        'select': 'id, canonical_producer_id, item_description',
        'name_col': 'item_description',
        'needs_producer_strip': True,
    },
    'best_wine_store': {
        'table': 'source_best_wine_store',
        'select': 'id, canonical_producer_id, title, producer',
        'needs_producer_strip': True,
    },
    'domestique': {
        'table': 'source_domestique',
        'select': 'id, canonical_producer_id, wine_name, title, producer',
        'needs_producer_strip': True,
    },
    'winedeals': {
        'table': 'source_winedeals',
        'select': 'id, canonical_producer_id, name, producer',
        'needs_producer_strip': True,
    },
    'firstleaf': {
        'table': 'source_firstleaf',
        'select': 'id, canonical_producer_id, title',
        'name_col': 'title',
        'needs_producer_strip': True,
    },
}


def get_raw_wine_name(row, source_key):
    """Extract raw wine name from a staging row before producer stripping."""
    if source_key == 'systembolaget':
        # Systembolaget: name_thin is the wine name, name_bold often has producer
        name = row.get('name_thin') or ''
        if not name:
            bold = row.get('name_bold') or ''
            producer = row.get('producer') or ''
            if producer and bold.lower().startswith(producer.lower()):
                name = bold[len(producer):].strip()
            else:
                name = bold
        return name
    elif source_key == 'bc_liquor':
        # BC Liquor format: "WINE_DESC - PRODUCER [VINTAGE]"
        name = row.get('name') or ''
        if ' - ' in name:
            parts = name.rsplit(' - ', 1)
            return parts[0].strip()  # wine part is before the dash
        return name
    elif source_key == 'flatiron':
        title = row.get('title') or ''
        producer = row.get('producer') or ''
        if producer and title.lower().startswith(producer.lower()):
            return title[len(producer):].lstrip(' ,:-').strip()
        return title
    elif source_key == 'wallys':
        return row.get('title') or ''
    elif source_key == 'enofile':
        # Compose wine name from varietal + designation + addl_designation
        parts = []
        varietal = (row.get('varietal') or '').strip()
        designation = (row.get('designation') or '').strip()
        addl = (row.get('addl_designation') or '').strip()
        if varietal:
            parts.append(varietal)
        if designation:
            parts.append(designation)
        if addl:
            parts.append(addl)
        return ' '.join(parts)
    elif source_key == 'pa':
        return row.get('item_description') or ''
    elif source_key == 'best_wine_store':
        title = row.get('title') or ''
        producer = row.get('producer') or ''
        if producer and title.lower().startswith(producer.lower()):
            return title[len(producer):].lstrip(' ,:-').strip()
        return title
    elif source_key == 'domestique':
        return row.get('wine_name') or row.get('title') or ''
    elif source_key == 'winedeals':
        name = row.get('name') or ''
        producer = row.get('producer') or ''
        if producer and name.lower().startswith(producer.lower()):
            return name[len(producer):].lstrip(' ,:-').strip()
        return name
    elif source_key == 'firstleaf':
        return row.get('title') or ''
    else:
        return row.get('name') or ''


def process_source(conn, source_key, config, dry_run=False):
    """Process one retail source: find unlinked records, create wines, link back."""
    table = config['table']
    select_cols = config['select']
    needs_strip = config.get('needs_producer_strip', True)

    print(f"\n{'=' * 60}")
    print(f"{source_key.upper()}")
    print(f"{'=' * 60}")

    cur = conn.cursor()

    # Load records with producer but no wine
    cur.execute(f"""
        SELECT {select_cols}
        FROM {table}
        WHERE canonical_producer_id IS NOT NULL
          AND canonical_wine_id IS NULL
    """)
    col_names = [desc[0] for desc in cur.description]
    rows = [dict(zip(col_names, row)) for row in cur.fetchall()]

    print(f"  {len(rows)} records with producer but no wine")
    if not rows:
        return 0, 0

    # Load producer names for stripping (keyed by producer_id)
    producer_ids = list(set(r['canonical_producer_id'] for r in rows))
    producer_names = {}
    for i in range(0, len(producer_ids), 500):
        batch = producer_ids[i:i + 500]
        placeholders = ','.join(['%s'] * len(batch))
        cur.execute(f"SELECT id, name, country_id FROM producers WHERE id IN ({placeholders})", batch)
        for pid, pname, cid in cur.fetchall():
            producer_names[pid] = {'name': pname, 'country_id': cid}

    # Group by producer, extract unique wine names
    producer_wines = defaultdict(dict)  # producer_id -> {name_norm: (display_name, [row_ids])}
    skipped_empty = 0

    for row in rows:
        pid = row['canonical_producer_id']
        raw_name = get_raw_wine_name(row, source_key)
        raw_name = extract_wine_from_retail(raw_name)  # strip vintage/size

        # Strip producer prefix if needed
        if needs_strip and pid in producer_names:
            raw_name = strip_producer_prefix(raw_name, producer_names[pid]['name'])

        if not raw_name or len(raw_name.strip()) < 2:
            skipped_empty += 1
            continue

        # Clean display name
        display = raw_name.strip()
        if display == display.upper() and len(display) > 3:
            display = display.title()

        name_norm = normalize(display)
        if not name_norm or len(name_norm) < 2:
            skipped_empty += 1
            continue

        if name_norm not in producer_wines[pid]:
            producer_wines[pid][name_norm] = (display, [])
        producer_wines[pid][name_norm][1].append(row['id'])

    total_unique = sum(len(wines) for wines in producer_wines.values())
    print(f"  {total_unique} unique wine names across {len(producer_wines)} producers")
    if skipped_empty:
        print(f"  {skipped_empty} rows skipped (empty/short name)")

    # Load existing wines for these producers (dedup check)
    existing = set()  # (producer_id, name_normalized)
    existing_wine_ids = {}  # (producer_id, name_normalized) -> wine_id
    for i in range(0, len(producer_ids), 500):
        batch = producer_ids[i:i + 500]
        placeholders = ','.join(['%s'] * len(batch))
        cur.execute(f"""
            SELECT id, producer_id, name_normalized
            FROM wines
            WHERE producer_id IN ({placeholders}) AND deleted_at IS NULL
        """, batch)
        for wid, wpid, wnorm in cur.fetchall():
            key = (wpid, wnorm)
            existing.add(key)
            existing_wine_ids[key] = wid

    wines_created = 0
    wines_linked_existing = 0
    rows_linked = 0
    errors = 0

    for pid, wines in producer_wines.items():
        country_id = producer_names.get(pid, {}).get('country_id')

        for name_norm, (display_name, row_ids) in wines.items():
            key = (pid, name_norm)

            if key in existing:
                # Wine already exists — link staging rows to it
                wine_id = existing_wine_ids.get(key)
                if wine_id and not dry_run:
                    for rid in row_ids:
                        try:
                            cur.execute(f"""
                                UPDATE {table}
                                SET canonical_wine_id = %s
                                WHERE id = %s AND canonical_wine_id IS NULL
                            """, (wine_id, rid))
                            rows_linked += cur.rowcount
                        except Exception:
                            pass
                else:
                    rows_linked += len(row_ids)
                wines_linked_existing += 1
                continue

            # Create new wine
            short_id = uuid.uuid4().hex[:8]
            slug = slugify(display_name)[:180] + f'-rt-{short_id}'

            if dry_run:
                wines_created += 1
                rows_linked += len(row_ids)
                existing.add(key)
                continue

            try:
                cur.execute("""
                    INSERT INTO wines (name, name_normalized, slug, producer_id, country_id, wine_type, effervescence)
                    VALUES (%s, %s, %s, %s, %s, 'table', 'still')
                    RETURNING id
                """, (display_name, name_norm, slug, pid, country_id))
                result = cur.fetchone()
                if result:
                    wine_id = result[0]
                    wines_created += 1
                    existing.add(key)
                    existing_wine_ids[key] = wine_id

                    # Link staging rows
                    for rid in row_ids:
                        cur.execute(f"""
                            UPDATE {table}
                            SET canonical_wine_id = %s
                            WHERE id = %s AND canonical_wine_id IS NULL
                        """, (wine_id, rid))
                        rows_linked += cur.rowcount

            except Exception as e:
                err = str(e)
                conn.rollback()  # reset from failed insert
                if '23505' in err:  # unique constraint violation
                    existing.add(key)
                    wines_linked_existing += 1
                else:
                    errors += 1
                    if errors <= 5:
                        print(f"    Error creating '{display_name}': {err[:120]}")

        # Commit per producer to avoid giant transactions
        if not dry_run:
            conn.commit()

    if not dry_run:
        conn.commit()

    print(f"  Results: {wines_created} wines created, {wines_linked_existing} linked to existing, "
          f"{rows_linked} staging rows linked, {errors} errors")
    return wines_created, rows_linked


def main():
    parser = argparse.ArgumentParser(description="Create canonical wines from producer-matched staging records")
    parser.add_argument('--dry-run', action='store_true', help="Show what would be created without writing")
    parser.add_argument('--source', default='lcbo,systembolaget,flatiron,specs,wallys,bc_liquor,enofile,pa,best_wine_store,domestique,winedeals,firstleaf',
                        help="Comma-separated source keys")
    args = parser.parse_args()

    sources = [s.strip() for s in args.source.split(',')]
    conn = get_conn()

    total_created = 0
    total_linked = 0
    t0 = time.time()

    for source_key in sources:
        if source_key not in SOURCE_CONFIG:
            print(f"Unknown source: {source_key}")
            continue
        created, linked = process_source(conn, source_key, SOURCE_CONFIG[source_key], args.dry_run)
        total_created += created
        total_linked += linked

    elapsed = time.time() - t0
    print(f"\n{'=' * 60}")
    print(f"TOTAL: {total_created} wines created, {total_linked} rows linked ({elapsed:.0f}s)")
    if args.dry_run:
        print("  ** DRY RUN — no writes **")

    conn.close()


if __name__ == '__main__':
    main()
