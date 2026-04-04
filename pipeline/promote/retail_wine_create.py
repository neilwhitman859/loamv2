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
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from pipeline.lib.db import get_supabase
from pipeline.lib.normalize import normalize, slugify


# Source configs: table name, name column, how to extract wine name from product title
SOURCE_CONFIG = {
    'specs': {
        'table': 'source_specs',
        'name_col': 'name',
        'extract_wine_name': lambda row, _: extract_wine_from_retail(row.get('name', '')),
    },
    'wallys': {
        'table': 'source_wallys',
        'name_col': 'title',
        'extract_wine_name': lambda row, _: extract_wine_from_retail(row.get('title', '')),
    },
    'systembolaget': {
        'table': 'source_systembolaget',
        'name_col': 'name_thin',
        'extract_wine_name': lambda row, _: row.get('name_thin') or row.get('name_bold') or '',
    },
    'lcbo': {
        'table': 'source_lcbo',
        'name_col': 'name',
        'extract_wine_name': lambda row, _: extract_wine_from_retail(row.get('name', '')),
    },
    'flatiron': {
        'table': 'source_flatiron',
        'name_col': 'title',
        'extract_wine_name': lambda row, _: extract_wine_from_retail(row.get('title', '')),
    },
    'bc_liquor': {
        'table': 'source_bc_liquor',
        'name_col': 'name',
        'extract_wine_name': lambda row, _: extract_wine_from_retail(row.get('name', '')),
    },
    # Unlinked sources that need batch matching first
    'enofile': {
        'table': 'source_enofile',
        'name_col': 'brand',
        'extract_wine_name': lambda row, _: row.get('brand', ''),
    },
    'openfoodfacts': {
        'table': 'source_openfoodfacts',
        'name_col': 'name',
        'extract_wine_name': lambda row, _: row.get('name', ''),
    },
    'winedeals': {
        'table': 'source_winedeals',
        'name_col': 'name',
        'extract_wine_name': lambda row, _: row.get('name', ''),
    },
    'horizon': {
        'table': 'source_horizon',
        'name_col': 'name',
        'extract_wine_name': lambda row, _: row.get('name', ''),
    },
    'polaner': {
        'table': 'source_polaner',
        'name_col': 'wine_name',
        'extract_wine_name': lambda row, _: row.get('wine_name') or row.get('title', ''),
    },
    'firstleaf': {
        'table': 'source_firstleaf',
        'name_col': 'title',
        'extract_wine_name': lambda row, _: row.get('title', ''),
    },
    'best_wine_store': {
        'table': 'source_best_wine_store',
        'name_col': 'wine_name',
        'extract_wine_name': lambda row, _: row.get('wine_name') or row.get('title', ''),
    },
    'domestique': {
        'table': 'source_domestique',
        'name_col': 'wine_name',
        'extract_wine_name': lambda row, _: row.get('wine_name') or row.get('title', ''),
    },
    'last_bottle': {
        'table': 'source_last_bottle',
        'name_col': 'wine_name',
        'extract_wine_name': lambda row, _: row.get('wine_name') or row.get('title', ''),
    },
}


def extract_wine_from_retail(title):
    """Extract wine name from a retail product title, stripping vintage, size, producer prefix."""
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


def process_source(sb, source_key, config, dry_run=False):
    """Process one retail source: find unlinked records, create wines, link back."""
    table = config['table']
    name_col = config['name_col']

    print(f"\n{'=' * 60}")
    print(f"{source_key.upper()}")
    print(f"{'=' * 60}")

    # Load records with producer but no wine
    rows = []
    offset = 0
    select_cols = f"id,canonical_producer_id,{name_col}"
    # Add extra cols if available
    if source_key == 'systembolaget':
        select_cols = "id,canonical_producer_id,name_bold,name_thin"

    while True:
        try:
            result = (sb.table(table)
                      .select(select_cols)
                      .not_.is_('canonical_producer_id', 'null')
                      .is_('canonical_wine_id', 'null')
                      .range(offset, offset + 999)
                      .execute())
            rows.extend(result.data)
            if len(result.data) < 1000:
                break
            offset += 1000
        except Exception as e:
            print(f"  Error loading: {e}")
            break

    print(f"  {len(rows)} records with producer but no wine")
    if not rows:
        return 0, 0

    # Group by producer, extract unique wine names
    producer_wines = {}  # producer_id -> {name_norm: (display_name, [row_ids])}
    for row in rows:
        pid = row['canonical_producer_id']
        wine_name = config['extract_wine_name'](row, pid)
        if not wine_name or len(wine_name) < 2:
            continue
        name_norm = normalize(wine_name)
        if not name_norm or len(name_norm) < 2:
            continue
        if pid not in producer_wines:
            producer_wines[pid] = {}
        if name_norm not in producer_wines[pid]:
            producer_wines[pid][name_norm] = (wine_name, [])
        producer_wines[pid][name_norm][1].append(row['id'])

    total_unique = sum(len(wines) for wines in producer_wines.values())
    print(f"  {total_unique} unique wine names across {len(producer_wines)} producers")

    # Load existing wines for these producers (dedup check)
    producer_ids = list(producer_wines.keys())
    existing = set()  # (producer_id, name_normalized)
    for i in range(0, len(producer_ids), 50):
        batch = producer_ids[i:i + 50]
        try:
            result = (sb.table('wines')
                      .select('producer_id,name_normalized')
                      .in_('producer_id', batch)
                      .is_('deleted_at', 'null')
                      .execute())
            for w in result.data:
                existing.add((w['producer_id'], w['name_normalized']))
        except Exception as e:
            if i == 0:
                print(f"  Error loading existing: {e}")

    wines_created = 0
    wines_skipped = 0
    rows_linked = 0
    errors = 0

    for pid, wines in producer_wines.items():
        for name_norm, (display_name, row_ids) in wines.items():
            if (pid, name_norm) in existing:
                # Wine exists — try to link the staging rows to it
                try:
                    result = (sb.table('wines')
                              .select('id')
                              .eq('producer_id', pid)
                              .eq('name_normalized', name_norm)
                              .is_('deleted_at', 'null')
                              .limit(1)
                              .execute())
                    if result.data:
                        wine_id = result.data[0]['id']
                        if not dry_run:
                            for rid in row_ids:
                                try:
                                    sb.table(table).update({
                                        'canonical_wine_id': wine_id
                                    }).eq('id', rid).execute()
                                    rows_linked += 1
                                except:
                                    pass
                        else:
                            rows_linked += len(row_ids)
                except:
                    pass
                wines_skipped += 1
                continue

            # Create new wine
            if dry_run:
                wines_created += 1
                rows_linked += len(row_ids)
                existing.add((pid, name_norm))
                continue

            display = display_name.title() if display_name == display_name.upper() else display_name
            short_id = uuid.uuid4().hex[:8]
            try:
                result = sb.table('wines').insert({
                    'name': display,
                    'name_normalized': name_norm,
                    'slug': slugify(display)[:180] + f'-rt-{short_id}',
                    'producer_id': pid,
                    'wine_type': 'table',
                    'effervescence': 'still',
                }).execute()

                if result.data:
                    wine_id = result.data[0]['id']
                    wines_created += 1
                    existing.add((pid, name_norm))

                    # Link staging rows
                    for rid in row_ids:
                        try:
                            sb.table(table).update({
                                'canonical_wine_id': wine_id
                            }).eq('id', rid).execute()
                            rows_linked += 1
                        except:
                            pass
            except Exception as e:
                err = str(e)
                if '23505' in err:  # duplicate
                    existing.add((pid, name_norm))
                    wines_skipped += 1
                else:
                    errors += 1
                    if errors <= 3:
                        print(f"    Error: {err[:120]}")

    print(f"  Results: {wines_created} wines created, {wines_skipped} existing, "
          f"{rows_linked} rows linked, {errors} errors")
    return wines_created, rows_linked


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--dry-run', action='store_true')
    parser.add_argument('--source', default='specs,wallys,systembolaget,lcbo,flatiron,bc_liquor')
    args = parser.parse_args()

    sources = [s.strip() for s in args.source.split(',')]
    sb = get_supabase()

    total_created = 0
    total_linked = 0
    t0 = time.time()

    for source_key in sources:
        if source_key not in SOURCE_CONFIG:
            print(f"Unknown source: {source_key}")
            continue
        created, linked = process_source(sb, source_key, SOURCE_CONFIG[source_key], args.dry_run)
        total_created += created
        total_linked += linked

    elapsed = time.time() - t0
    print(f"\n{'=' * 60}")
    print(f"TOTAL: {total_created} wines created, {total_linked} rows linked ({elapsed:.0f}s)")
    if args.dry_run:
        print("  ** DRY RUN — no writes **")


if __name__ == '__main__':
    main()
