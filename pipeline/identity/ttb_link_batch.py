"""
Standalone TTB linker: link canonical wines to TTB COLA records for depth data.

For each producer, searches source_ttb_colas by brand_name variants,
matches fanciful_name to wine cuvée, and creates:
- COLA external_ids
- Vintages with ABV and label_image_url

Usage:
    python -m pipeline.identity.ttb_link_batch --dry-run --limit 5
    python -m pipeline.identity.ttb_link_batch --execute
    python -m pipeline.identity.ttb_link_batch --execute --skip-linked
"""
import argparse
import json
import re
import sys
import uuid
from collections import defaultdict

from pipeline.lib.db import get_conn
from pipeline.lib.normalize import normalize


def get_ttb_brand_variants(producer_name: str) -> list[str]:
    """Generate brand_name variants for TTB search."""
    variants = [producer_name]
    name_lower = producer_name.lower()

    # Strip common prefixes
    for prefix in ["Château ", "Chateau ", "Domaine ", "Bodegas ", "Maison ",
                   "Tenuta ", "Cantina ", "Casa ", "Caves ", "Weingut "]:
        if name_lower.startswith(prefix.lower()):
            variants.append(producer_name[len(prefix):])

    # Strip common suffixes
    for suffix in [" Vineyards", " Vineyard", " Winery", " Estate", " Cellars",
                   " Wines", " Wine", " Estates"]:
        if name_lower.endswith(suffix.lower()):
            variants.append(producer_name[:-len(suffix)])

    return variants


def match_ttb_to_wine(wines: list, fanciful: str | None, appellation: str | None) -> str | None:
    """Match a TTB record to a canonical wine by normalized name."""
    if not wines:
        return None

    fanciful_norm = normalize(fanciful) if fanciful else ""

    for wine_id, wine_name, display_name, slug in wines:
        wine_name_norm = normalize(wine_name) if wine_name else ""

        # Exact cuvée match
        if wine_name_norm and fanciful_norm and wine_name_norm == fanciful_norm:
            return str(wine_id)

        # If wine has no cuvée (NULL name), match by appellation
        if not wine_name and not fanciful:
            return str(wine_id)

    # If only one wine for this producer and TTB record has no fanciful, assume match
    if len(wines) == 1 and not fanciful:
        return str(wines[0][0])

    return None


def parse_label_url(label_urls) -> str | None:
    """Extract first label image URL from TTB data."""
    if not label_urls:
        return None
    try:
        urls = json.loads(label_urls) if isinstance(label_urls, str) else label_urls
        if isinstance(urls, list) and urls:
            return urls[0]
        elif isinstance(urls, str) and urls.startswith("http"):
            return urls
    except (json.JSONDecodeError, TypeError):
        if isinstance(label_urls, str) and label_urls.startswith("http"):
            return label_urls
    return None


def run(dry_run: bool = True, limit: int | None = None, skip_linked: bool = False):
    # Fix Windows stdout encoding
    import io, os
    if sys.stdout.encoding != 'utf-8':
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

    conn = get_conn()
    cur = conn.cursor()

    # Get all active producers
    cur.execute("""
        SELECT p.id, p.name, p.country_id,
               count(w.id) as wine_count
        FROM producers p
        JOIN wines w ON w.producer_id = p.id AND w.deleted_at IS NULL
        WHERE p.deleted_at IS NULL
        GROUP BY p.id, p.name, p.country_id
        ORDER BY count(w.id) DESC
    """)
    all_producers = cur.fetchall()

    if skip_linked:
        # Get producers that already have COLA links
        cur.execute("""
            SELECT DISTINCT p.id
            FROM producers p
            JOIN wines w ON w.producer_id = p.id AND w.deleted_at IS NULL
            JOIN external_ids e ON e.entity_id = w.id AND e.entity_type = 'wine' AND e.system = 'cola'
            WHERE p.deleted_at IS NULL
        """)
        linked_ids = {str(r[0]) for r in cur.fetchall()}
        producers = [(pid, pn, cc, wc) for pid, pn, cc, wc in all_producers
                     if str(pid) not in linked_ids]
        print(f"Skipping {len(linked_ids)} already-linked producers")
    else:
        producers = all_producers

    if limit:
        producers = producers[:limit]

    mode = "DRY RUN" if dry_run else "EXECUTE"
    print(f"\n{'='*60}")
    print(f"  TTB LINK BATCH — {mode}")
    print(f"  {len(producers)} producers ({sum(wc for _, _, _, wc in producers)} wines)")
    print(f"{'='*60}\n")

    # Stats
    stats = defaultdict(int)
    stats["producers_total"] = len(producers)

    for i, (producer_id, producer_name, country_id, wine_count) in enumerate(producers):
        producer_id = str(producer_id)

        # Get all wines for this producer
        cur.execute("""
            SELECT id, name, display_name, slug
            FROM wines WHERE producer_id = %s AND deleted_at IS NULL
        """, (producer_id,))
        wines = cur.fetchall()
        if not wines:
            continue

        # Build TTB brand name variants
        variants = get_ttb_brand_variants(producer_name)

        # Search TTB — use both UPPER and original case to hit btree index
        search_names = set()
        for v in variants:
            search_names.add(v)
            search_names.add(v.upper())
        search_names = list(search_names)

        placeholders = ", ".join(["%s"] * len(search_names))
        cur.execute(f"""
            SELECT ttb_id, fanciful_name, wine_appellation, grape_varietals,
                   class_type_desc, wine_vintage, abv, label_image_urls
            FROM source_ttb_colas
            WHERE brand_name IN ({placeholders})
            LIMIT 10000
        """, search_names)
        ttb_rows = cur.fetchall()

        if not ttb_rows:
            if (i + 1) % 200 == 0:
                print(f"  [{i+1}/{len(producers)}] {producer_name}: 0 TTB records")
            continue

        producer_linked = 0
        producer_vintages = 0
        producer_labels = 0

        for ttb_row in ttb_rows:
            ttb_id, fanciful, appellation, grapes, class_desc, vintage, abv, label_urls = ttb_row

            matched_wine_id = match_ttb_to_wine(wines, fanciful, appellation)
            if not matched_wine_id:
                continue

            if dry_run:
                producer_linked += 1
                continue

            # Create COLA external_id
            if ttb_id:
                try:
                    cur.execute("""
                        INSERT INTO external_ids (entity_type, entity_id, system, external_id)
                        VALUES ('wine', %s, 'cola', %s)
                        ON CONFLICT DO NOTHING
                    """, (matched_wine_id, str(ttb_id)))
                    if cur.rowcount > 0:
                        stats["cola_ids_created"] += 1
                except Exception:
                    conn.rollback()
                    continue

            # Create/update vintage if we have one
            if vintage and re.match(r'^(19|20)\d{2}$', str(vintage)):
                year = int(vintage)
                abv_val = None
                if abv:
                    try:
                        abv_val = float(abv)
                        if not (3 <= abv_val <= 25):
                            abv_val = None
                    except (ValueError, TypeError):
                        pass

                label_url = parse_label_url(label_urls)

                cur.execute("""
                    SELECT id, abv, label_image_url FROM wine_vintages
                    WHERE wine_id = %s AND vintage_year = %s
                """, (matched_wine_id, year))
                existing = cur.fetchone()

                if existing:
                    vintage_id, existing_abv, existing_label = existing
                    updates = []
                    params = []
                    if abv_val and not existing_abv:
                        updates.append("abv = %s")
                        params.append(abv_val)
                    if label_url and not existing_label:
                        updates.append("label_image_url = %s")
                        params.append(label_url)
                        producer_labels += 1
                    if updates:
                        params.append(str(vintage_id))
                        cur.execute(f"UPDATE wine_vintages SET {', '.join(updates)} WHERE id = %s", params)
                        if abv_val and not existing_abv:
                            stats["abv_filled"] += 1
                        if label_url and not existing_label:
                            stats["label_urls_filled"] += 1
                else:
                    label_url = parse_label_url(label_urls)
                    vintage_id = str(uuid.uuid4())
                    cur.execute("""
                        INSERT INTO wine_vintages (id, wine_id, vintage_year, abv, label_image_url)
                        VALUES (%s, %s, %s, %s, %s)
                    """, (vintage_id, matched_wine_id, year, abv_val, label_url))
                    stats["vintages_created"] += 1
                    producer_vintages += 1
                    if label_url:
                        stats["label_urls_new"] += 1

            producer_linked += 1

        # Commit per-producer
        if not dry_run:
            conn.commit()

        stats["ttb_matched"] += producer_linked
        if producer_linked > 0:
            stats["producers_matched"] += 1

        if (i + 1) % 50 == 0 or producer_linked > 50:
            print(f"  [{i+1}/{len(producers)}] {producer_name}: "
                  f"{len(ttb_rows)} TTB → {producer_linked} matched "
                  f"(+{producer_vintages} vint, +{producer_labels} labels)")

    # Final stats
    print(f"\n{'='*60}")
    print(f"  RESULTS")
    print(f"{'='*60}")
    print(f"  Producers processed:  {stats['producers_total']}")
    print(f"  Producers with TTB:   {stats['producers_matched']}")
    print(f"  TTB records matched:  {stats['ttb_matched']}")
    print(f"  COLA IDs created:     {stats['cola_ids_created']}")
    print(f"  Vintages created:     {stats['vintages_created']}")
    print(f"  ABV filled:           {stats['abv_filled']}")
    print(f"  Label URLs (new vint):{stats['label_urls_new']}")
    print(f"  Label URLs (filled):  {stats['label_urls_filled']}")
    print(f"{'='*60}\n")

    cur.close()
    conn.close()
    return dict(stats)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="TTB Link Batch")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--dry-run", action="store_true")
    group.add_argument("--execute", action="store_true")
    parser.add_argument("--limit", type=int)
    parser.add_argument("--skip-linked", action="store_true",
                        help="Skip producers that already have COLA links")
    args = parser.parse_args()

    run(dry_run=args.dry_run, limit=args.limit, skip_linked=args.skip_linked)
