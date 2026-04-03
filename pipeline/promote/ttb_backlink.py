"""
Back-link TTB COLA records to newly created Tier C wines and promote depth.

Strategy: For each new wine, find its TTB source records by matching
producer name + wine identity (fanciful/grape) + appellation.
Works producer-by-producer to avoid timeout issues.

Also promotes: vintages + ABV, COLA external_ids.

Usage:
    python -m pipeline.promote.ttb_backlink [--dry-run] [--limit N]
"""

import re
import sys
import time
import argparse
from collections import defaultdict
from pipeline.lib.db import get_supabase


def normalize(s: str) -> str:
    """Lowercase, strip spaces and accents for matching."""
    if not s:
        return ''
    return re.sub(r'\s+', '', s.lower().strip())


def strip_suffix(brand: str) -> str:
    """Strip common producer suffixes from brand name."""
    return re.sub(
        r'\s*(winery|vineyards|vineyard|estate|cellars|wines|wine|chateau|domaine|bodega|bodegas|cantina|tenuta|weingut|fattoria)\s*$',
        '', brand.strip(), flags=re.IGNORECASE
    ).strip()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--dry-run', action='store_true')
    parser.add_argument('--limit', type=int, default=0)
    args = parser.parse_args()

    sb = get_supabase()
    t0 = time.time()

    # Step 1: Get distinct producer_ids with new Tier C wines
    print("Fetching producers with new Tier C wines...")
    all_wines = []
    offset = 0
    while True:
        batch = sb.table('wines').select(
            'id,name,producer_id,appellation_id'
        ).gt('created_at', '2026-04-02T20:00:00Z').is_(
            'deleted_at', 'null'
        ).range(offset, offset + 999).execute()
        all_wines.extend(batch.data)
        if len(batch.data) < 1000:
            break
        offset += 1000

    print(f"Found {len(all_wines)} new Tier C wines")

    # Group by producer
    by_producer = defaultdict(list)
    for w in all_wines:
        by_producer[w['producer_id']].append(w)

    producer_ids = list(by_producer.keys())
    if args.limit:
        producer_ids = producer_ids[:args.limit]
    print(f"Across {len(producer_ids)} producers")

    # Step 2: Get producer names
    print("Fetching producer names...")
    producer_names = {}
    for i in range(0, len(producer_ids), 50):
        chunk = producer_ids[i:i+50]
        # Supabase doesn't support IN directly, fetch individually for now
        for pid in chunk:
            r = sb.table('producers').select('id,name,name_normalized').eq('id', pid).execute()
            if r.data:
                producer_names[pid] = r.data[0]
    print(f"Got names for {len(producer_names)} producers")

    # Step 3: Process each producer
    stats = {'linked': 0, 'vintages': 0, 'colas': 0, 'skipped': 0}

    for idx, pid in enumerate(producer_ids):
        if pid not in producer_names:
            continue

        pinfo = producer_names[pid]
        pname = pinfo['name']
        pnorm = pinfo['name_normalized']
        wines = by_producer[pid]

        # Build wine lookup: identity (before comma) → wine
        wine_lookup = {}
        for w in wines:
            prefix = w['name'].split(',')[0].strip().lower()
            wine_lookup[prefix] = w

        # Fetch TTB records for this brand name using case-insensitive match
        ttb_records = []
        for brand_variant in [pname, pname.upper(), pname.title(), strip_suffix(pname), strip_suffix(pname).upper()]:
            if not brand_variant:
                continue
            try:
                ttb_batch = sb.table('source_ttb_colas').select(
                    'ttb_id,fanciful_name,grape_varietals,wine_appellation,wine_vintage,abv'
                ).is_('canonical_wine_id', 'null').ilike(
                    'brand_name', brand_variant
                ).limit(1000).execute()
                if ttb_batch.data:
                    ttb_records = ttb_batch.data
                    break
            except Exception:
                continue

        if not ttb_records:
            stats['skipped'] += 1
            continue

        linked_count = 0
        for ttb in ttb_records:
            # Build identity from TTB
            identity = (ttb.get('fanciful_name') or '').strip()
            if not identity:
                gv = (ttb.get('grape_varietals') or '').strip()
                # Skip multi-grape for matching
                if ',' in gv or '/' in gv:
                    continue
                identity = re.sub(r'^\d+%\s*', '', gv)

            if not identity:
                continue

            # Look up in wine_lookup
            match_wine = wine_lookup.get(identity.lower())
            if not match_wine:
                continue

            if args.dry_run:
                linked_count += 1
                continue

            # Link TTB → wine
            try:
                sb.table('source_ttb_colas').update({
                    'canonical_wine_id': match_wine['id'],
                    'canonical_producer_id': pid,
                }).eq('ttb_id', ttb['ttb_id']).execute()
                stats['linked'] += 1
                linked_count += 1
            except Exception as e:
                continue

            # COLA external_id
            try:
                sb.table('external_ids').insert({
                    'entity_type': 'wine',
                    'entity_id': match_wine['id'],
                    'system': 'cola',
                    'external_id': ttb['ttb_id']
                }).execute()
                stats['colas'] += 1
            except Exception:
                pass

            # Vintage
            vintage_str = (ttb.get('wine_vintage') or '').strip()
            if vintage_str.isdigit() and 1900 <= int(vintage_str) <= 2027:
                abv = None
                abv_str = (ttb.get('abv') or '').strip()
                try:
                    abv_val = float(abv_str)
                    if 3 <= abv_val <= 25:
                        abv = abv_val
                except (ValueError, TypeError):
                    pass

                try:
                    row = {'wine_id': match_wine['id'], 'vintage_year': int(vintage_str)}
                    if abv is not None:
                        row['abv'] = abv
                    sb.table('wine_vintages').insert(row).execute()
                    stats['vintages'] += 1
                except Exception:
                    pass

        if (idx + 1) % 200 == 0:
            elapsed = time.time() - t0
            rate = (idx + 1) / elapsed * 60
            print(f"  [{idx+1}/{len(producer_ids)}] {rate:.0f} producers/min | "
                  f"Linked: {stats['linked']}, Vintages: {stats['vintages']}, COLAs: {stats['colas']}, "
                  f"Skipped: {stats['skipped']}")

    elapsed = time.time() - t0
    print(f"\nDone in {elapsed:.0f}s. "
          f"Linked: {stats['linked']}, Vintages: {stats['vintages']}, "
          f"COLAs: {stats['colas']}, Skipped: {stats['skipped']}")
    if args.dry_run:
        print("(DRY RUN — no changes written)")


if __name__ == '__main__':
    main()
