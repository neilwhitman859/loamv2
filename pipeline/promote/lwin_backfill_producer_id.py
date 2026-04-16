#!/usr/bin/env python3
"""B6.2 backfill: set canonical_producer_id on source_lwin rows with NULL wine_name.

After `lwin_long_tail.py --resume-unlinked` finishes, any source_lwin rows whose
`wine_name` is NULL/empty remain unlinked — the main script skips them because it
can't create a canonical wine without a name. But the producer was matched/created
via that same `(producer_name, country)` pair when OTHER rows under it had wines,
OR the pair is composed entirely of wine_name-null rows (fully skipped).

This helper fills in `canonical_producer_id + processed_at` for those rows by
looking up the producer via normalized-name + country match. Same method as the
main import, just applied to rows we skipped.

Wine-level fields (`canonical_wine_id`, `canonical_wine_vintage_id`) stay NULL —
there are no wines to link to.

Usage:
    python -m pipeline.promote.lwin_backfill_producer_id --analyze
    python -m pipeline.promote.lwin_backfill_producer_id --dry-run
    python -m pipeline.promote.lwin_backfill_producer_id --execute
"""

import argparse
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from pipeline.lib.db import get_conn
from pipeline.lib.normalize import normalize
from pipeline.lib.resolve import ReferenceResolver


def fetch_candidate_rows(conn) -> list[tuple[str, str, str | None]]:
    """(lwin, producer_name, country) for every source_lwin row still unlinked
    after the main import."""
    sql = """
        SELECT lwin, producer_name, country
        FROM source_lwin
        WHERE canonical_producer_id IS NULL
          AND producer_name IS NOT NULL
          AND TRIM(producer_name) != ''
        ORDER BY country, producer_name, lwin
    """
    with conn.cursor() as cur:
        cur.execute(sql)
        return [(r[0], r[1], r[2]) for r in cur.fetchall()]


def build_producer_index(conn) -> dict[tuple[str, str | None], str]:
    """Return {(name_normalized, country_id): producer_id} for active canonical producers."""
    sql = """
        SELECT id, name_normalized, country_id
        FROM producers
        WHERE deleted_at IS NULL
          AND name_normalized IS NOT NULL
          AND TRIM(name_normalized) != ''
    """
    with conn.cursor() as cur:
        cur.execute(sql)
        index: dict[tuple[str, str | None], str] = {}
        collisions = 0
        for pid, norm, cid in cur.fetchall():
            key = (norm, str(cid) if cid else None)
            if key in index:
                collisions += 1
                continue
            index[key] = str(pid)
        if collisions:
            print(f"  [warn] {collisions} producer (name_normalized, country_id) collisions — first-seen wins")
        return index


def bulk_update(conn, updates: list[tuple[str, str]]) -> int:
    """Set canonical_producer_id + processed_at for (lwin, producer_id) pairs."""
    if not updates:
        return 0
    placeholders = ",".join(["(%s, %s::uuid)"] * len(updates))
    params: list = []
    for lwin, producer_id in updates:
        params.extend([lwin, producer_id])
    sql = f"""
        UPDATE source_lwin sl
        SET canonical_producer_id = v.producer_id,
            processed_at = COALESCE(sl.processed_at, NOW())
        FROM (VALUES {placeholders}) AS v(lwin, producer_id)
        WHERE sl.lwin = v.lwin
    """
    with conn.cursor() as cur:
        cur.execute(sql, params)
        return cur.rowcount or 0


def run(dry_run: bool):
    print("=" * 70)
    print("  LWIN producer-id backfill (B6.2 closeout)")
    print(f"  Mode: {'DRY-RUN' if dry_run else 'EXECUTE'}")
    print("=" * 70)

    conn = get_conn()
    resolver = ReferenceResolver(verbose=True)
    resolver.init_sync()

    print("\nBuilding producer index...")
    producer_index = build_producer_index(conn)
    print(f"  Indexed {len(producer_index):,} (name_normalized, country_id) keys")

    print("\nFetching unlinked source_lwin rows...")
    rows = fetch_candidate_rows(conn)
    print(f"  {len(rows):,} rows to consider")

    # Resolve producer_id per row; batch updates every 500
    pending: list[tuple[str, str]] = []
    stats = {"matched": 0, "no_producer": 0, "no_country": 0, "updated": 0}

    # Cache country resolution across same-country rows
    country_cache: dict[str, str | None] = {}
    for i, (lwin, producer_name, country_str) in enumerate(rows, 1):
        if country_str in country_cache:
            country_id = country_cache[country_str]
        else:
            country_id = resolver.resolve_country(country_str) if country_str else None
            country_cache[country_str] = country_id

        norm = normalize(producer_name)
        if not norm:
            stats["no_producer"] += 1
            continue

        # Try country-scoped match first, then fall back to country-agnostic
        producer_id = producer_index.get((norm, country_id))
        if not producer_id and country_id is None:
            # NULL country — try any unique match across all countries
            candidates = [pid for (n, _), pid in producer_index.items() if n == norm]
            if len(candidates) == 1:
                producer_id = candidates[0]
        if not producer_id:
            # Country-less fallback: if only one producer anywhere has this name, link it
            candidates = [pid for (n, _), pid in producer_index.items() if n == norm]
            if len(candidates) == 1:
                producer_id = candidates[0]

        if not producer_id:
            stats["no_producer"] += 1
            continue

        stats["matched"] += 1
        pending.append((lwin, producer_id))

        if len(pending) >= 500 and not dry_run:
            stats["updated"] += bulk_update(conn, pending)
            conn.commit()
            pending.clear()

        if i % 5000 == 0:
            print(f"  [{i:6,}/{len(rows):,}]  matched={stats['matched']:,}  no_producer={stats['no_producer']:,}")

    if pending and not dry_run:
        stats["updated"] += bulk_update(conn, pending)
        conn.commit()

    print("\n" + "=" * 70)
    print("  Backfill results")
    print("=" * 70)
    print(f"  Rows considered:   {len(rows):>6,}")
    print(f"  Producer-matched:  {stats['matched']:>6,}")
    print(f"  No producer match: {stats['no_producer']:>6,}")
    print(f"  source_lwin updated: {stats['updated']:>6,}")
    if dry_run:
        print("\n  (dry-run — no writes)")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--analyze", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--execute", action="store_true")
    args = parser.parse_args()

    if not any([args.analyze, args.dry_run, args.execute]):
        parser.error("Specify --analyze, --dry-run, or --execute")

    if args.analyze:
        conn = get_conn()
        sql = """
            SELECT
              COUNT(*) AS total,
              COUNT(*) FILTER (WHERE wine_name IS NULL OR TRIM(wine_name) = '') AS null_wine_name,
              COUNT(*) FILTER (WHERE wine_name IS NOT NULL AND TRIM(wine_name) != '') AS has_wine_name
            FROM source_lwin
            WHERE canonical_producer_id IS NULL
              AND producer_name IS NOT NULL
              AND TRIM(producer_name) != ''
        """
        with conn.cursor() as cur:
            cur.execute(sql)
            total, nulls, has = cur.fetchone()
        print(f"  Unlinked rows with producer_name: {total:,}")
        print(f"    wine_name NULL/empty: {nulls:,}")
        print(f"    wine_name present:    {has:,}")
        conn.close()
        return

    run(dry_run=args.dry_run)


if __name__ == "__main__":
    main()
