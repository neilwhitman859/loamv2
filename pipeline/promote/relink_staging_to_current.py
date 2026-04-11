#!/usr/bin/env python3
"""Re-link ALL source_* staging tables from archive.producers -> current producers.

One-off migration tool. Last ran in Session 13 (2026-04-10). Session 14 Phase B W5
moved the archive tables from public.archive_X to archive.X, so the SQL references
below were updated to archive.producers / archive.wines. If you're thinking about
re-running this script, check whether the staging FKs still need re-pointing first
(most of the S13 work is already landed).

Context:
    The 30K Plan Phase 0 rebuild created new canonical `producers` and `wines`
    tables with fresh UUIDs. Every `source_*` staging table's canonical_*
    foreign keys still point at `archive.producers`/`archive.wines`. This
    script walks the archive->current mapping via `name_normalized` match and
    bulk-updates the staging tables to point at the new canonical IDs.

    After running this, `ttb_wine_link_v2.py`, `retail_promote.py`, and any
    other downstream script that depends on `canonical_producer_id` becomes
    usable again.

Strategy:
    Phase 0: Build an `_archive_to_current_producer` mapping in a temp table.
             Archive producer -> current producer, matched by name_normalized
             (country_id as tiebreaker when multiple hits).
    Phase 1: For each source_* table, drop the broken archive FK (if any),
             UPDATE canonical_producer_id using the mapping, re-add a fresh FK
             pointing at the current producers table.
    Phase 2: For source_ttb_colas specifically, also NULL out canonical_wine_id
             since those pointed at archive.wines (which are all gone from the
             current wines table — zero UUID overlap confirmed). TTB-to-wine
             relinking happens in a separate step via ttb_wine_link_v2.

Usage:
    python -m pipeline.promote.relink_staging_to_current --dry-run
    python -m pipeline.promote.relink_staging_to_current --execute
    python -m pipeline.promote.relink_staging_to_current --execute --tables source_ttb_colas,source_pro_platform
"""

import argparse
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from pipeline.lib.db import get_conn


# All source_* tables with canonical_producer_id that need re-linking
# (confirmed via pg_constraint query 2026-04-10)
STAGING_TABLES_PRODUCER = [
    "source_bc_liquor", "source_berliner", "source_best_wine_store",
    "source_domestique", "source_empson", "source_enofile",
    "source_european_cellars", "source_firstleaf", "source_flatiron",
    "source_horizon", "source_kansas_brands", "source_kermit_lynch",
    "source_kermit_lynch_growers", "source_last_bottle", "source_lcbo",
    "source_openfoodfacts", "source_pa", "source_polaner",
    "source_pro_platform", "source_skurnik", "source_specs",
    "source_systembolaget", "source_tabc", "source_texsom",
    "source_ttb_colas", "source_utah_dabs", "source_wallys",
    "source_winebow", "source_winedeals", "source_wv_abca",
]

# source_lwin already handled in prior migration — skip it.
# match_decisions handled separately.

# Tables that ALSO have canonical_wine_id (must NULL it out since archive.wines are gone)
STAGING_TABLES_WINE = [
    "source_ttb_colas",
    # Add others if they have canonical_wine_id columns
]


def build_mapping_table(conn, dry_run: bool):
    """Build _archive_to_current_producer mapping. Returns (mapped_count, ambiguous_count)."""
    with conn.cursor() as cur:
        if not dry_run:
            cur.execute("DROP TABLE IF EXISTS _archive_to_current_producer")
            cur.execute("""
                CREATE TABLE _archive_to_current_producer (
                    archive_id uuid NOT NULL,
                    current_id uuid NOT NULL,
                    PRIMARY KEY (archive_id)
                )
            """)
            # Exact name_normalized + country_id match (strongest)
            cur.execute("""
                INSERT INTO _archive_to_current_producer (archive_id, current_id)
                SELECT DISTINCT ON (ap.id) ap.id, p.id
                FROM archive.producers ap
                JOIN producers p
                  ON p.name_normalized = ap.name_normalized
                 AND (
                    (ap.country_id IS NULL AND p.country_id IS NULL)
                    OR ap.country_id = p.country_id
                 )
                WHERE p.deleted_at IS NULL
                ORDER BY ap.id, p.country_id NULLS LAST
                ON CONFLICT (archive_id) DO NOTHING
            """)
            primary_matches = cur.rowcount

            # Second pass: name-only match for archive producers without a country-qualified hit
            cur.execute("""
                INSERT INTO _archive_to_current_producer (archive_id, current_id)
                SELECT DISTINCT ON (ap.id) ap.id, p.id
                FROM archive.producers ap
                JOIN producers p ON p.name_normalized = ap.name_normalized
                WHERE p.deleted_at IS NULL
                  AND ap.id NOT IN (SELECT archive_id FROM _archive_to_current_producer)
                ORDER BY ap.id, p.created_at
                ON CONFLICT (archive_id) DO NOTHING
            """)
            fallback_matches = cur.rowcount
            cur.execute("CREATE INDEX ON _archive_to_current_producer (current_id)")

            cur.execute("SELECT COUNT(*) FROM _archive_to_current_producer")
            total = cur.fetchone()[0]

            print(f"  Mapping: {primary_matches:,} primary + {fallback_matches:,} fallback = {total:,} archive producers mapped")
            return total
        else:
            cur.execute("""
                SELECT COUNT(DISTINCT ap.id)
                FROM archive.producers ap
                JOIN producers p ON p.name_normalized = ap.name_normalized
                WHERE p.deleted_at IS NULL
            """)
            total = cur.fetchone()[0]
            print(f"  [DRY] Would map ~{total:,} archive producers to current")
            return total


def drop_archive_fks(conn, tables: list[str], dry_run: bool) -> int:
    """Drop FKs on source_* tables that reference archive_*. Returns dropped count."""
    dropped = 0
    with conn.cursor() as cur:
        for t in tables:
            # Look for any FK on this table that references archive_*
            cur.execute("""
                SELECT c.conname
                FROM pg_constraint c
                JOIN pg_class t ON t.oid = c.conrelid
                JOIN pg_class f ON f.oid = c.confrelid
                WHERE c.contype = 'f'
                  AND t.relname = %s
                  AND f.relname LIKE 'archive\\_%%'
            """, (t,))
            fk_names = [r[0] for r in cur.fetchall()]
            for fk in fk_names:
                if dry_run:
                    print(f"  [DRY] Would DROP {t}.{fk}")
                else:
                    cur.execute(f'ALTER TABLE "{t}" DROP CONSTRAINT IF EXISTS "{fk}"')
                    print(f"  DROP  {t}.{fk}")
                dropped += 1
        if not dry_run:
            conn.commit()
    return dropped


def bulk_relink_table(
    conn, table: str, dry_run: bool, null_wine_id: bool = False
) -> dict:
    """Update one source_* table: remap canonical_producer_id via mapping, optionally null canonical_wine_id."""
    stats = {"relinked": 0, "cleared_no_match": 0, "cleared_wine_id": 0}
    with conn.cursor() as cur:
        if dry_run:
            # In dry-run, skip per-table counts (mapping table not built) — just report total rows
            cur.execute(f'SELECT COUNT(*) FROM "{table}" WHERE canonical_producer_id IS NOT NULL')
            stats["relinked"] = cur.fetchone()[0]
            if null_wine_id:
                cur.execute(f'SELECT COUNT(*) FROM "{table}" WHERE canonical_wine_id IS NOT NULL')
                stats["cleared_wine_id"] = cur.fetchone()[0]
            print(f"  [DRY] {table}: {stats['relinked']:,} rows with producer_id, "
                  f"{stats['cleared_wine_id']:,} rows with wine_id will be nulled")
            return stats

        # Phase 1: re-link producers through the mapping table (single bulk update)
        cur.execute(f"""
            UPDATE "{table}" t
            SET canonical_producer_id = m.current_id
            FROM _archive_to_current_producer m
            WHERE t.canonical_producer_id = m.archive_id
        """)
        stats["relinked"] = cur.rowcount or 0

        # Phase 2: null out any unmappable canonical_producer_id (points at archive producer not in current)
        cur.execute(f"""
            UPDATE "{table}" t
            SET canonical_producer_id = NULL
            WHERE t.canonical_producer_id IS NOT NULL
              AND NOT EXISTS (SELECT 1 FROM producers p WHERE p.id = t.canonical_producer_id)
        """)
        stats["cleared_no_match"] = cur.rowcount or 0

        # Phase 3: null canonical_wine_id (points at archive.wines, all gone)
        if null_wine_id:
            cur.execute(f'UPDATE "{table}" SET canonical_wine_id = NULL WHERE canonical_wine_id IS NOT NULL')
            stats["cleared_wine_id"] = cur.rowcount or 0

        conn.commit()
        print(
            f"  {table}: +{stats['relinked']:,} relinked, "
            f"-{stats['cleared_no_match']:,} nulled (no match), "
            f"-{stats['cleared_wine_id']:,} wine_id nulled"
        )
    return stats


def add_fresh_producer_fks(conn, tables: list[str], dry_run: bool):
    """Add fresh FKs pointing at current producers table."""
    if dry_run:
        return
    with conn.cursor() as cur:
        for t in tables:
            fk_name = f"{t}_canonical_producer_id_fkey"
            try:
                cur.execute(f"""
                    ALTER TABLE "{t}"
                    ADD CONSTRAINT "{fk_name}"
                    FOREIGN KEY (canonical_producer_id)
                    REFERENCES producers(id)
                    ON DELETE SET NULL
                """)
                conn.commit()
                print(f"  ADD   {t}.{fk_name}")
            except Exception as e:
                conn.rollback()
                print(f"  [skip] {t}: {e}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--execute", action="store_true", help="Apply changes (default dry-run)")
    parser.add_argument("--dry-run", action="store_true", help="Explicit dry-run")
    parser.add_argument("--tables", type=str, help="Comma-separated table list (default: all)")
    args = parser.parse_args()

    dry_run = not args.execute

    tables = STAGING_TABLES_PRODUCER
    if args.tables:
        requested = set(t.strip() for t in args.tables.split(","))
        tables = [t for t in STAGING_TABLES_PRODUCER if t in requested]
        missing = requested - set(tables)
        if missing:
            print(f"  [warn] Unknown tables (skipped): {missing}")

    conn = get_conn()
    try:
        conn.autocommit = False
        # Bump statement timeout to 30 minutes — source_ttb_colas is 3.28M rows
        with conn.cursor() as cur:
            cur.execute("SET statement_timeout = '1800000'")
        conn.commit()

        print("=" * 70)
        print(f"  relink_staging_to_current — {'DRY-RUN' if dry_run else 'EXECUTE'}")
        print(f"  Tables: {len(tables)}")
        print("=" * 70)

        # Step 1: mapping
        print("\n[1/4] Building archive->current producer mapping...")
        t0 = time.time()
        mapped_count = build_mapping_table(conn, dry_run)
        print(f"  {time.time()-t0:.1f}s")

        # Step 2: drop broken FKs
        print("\n[2/4] Dropping broken archive FKs...")
        drop_archive_fks(conn, tables, dry_run)

        # Step 3: bulk relink per table
        print("\n[3/4] Re-linking staging tables...")
        t0 = time.time()
        totals = {"relinked": 0, "cleared_no_match": 0, "cleared_wine_id": 0}
        for table in tables:
            null_wine = table in STAGING_TABLES_WINE
            stats = bulk_relink_table(conn, table, dry_run, null_wine_id=null_wine)
            for k, v in stats.items():
                totals[k] = totals.get(k, 0) + v
        print(f"  Total {time.time()-t0:.1f}s")

        # Step 4: re-add fresh FKs
        print("\n[4/4] Adding fresh FKs to current producers...")
        add_fresh_producer_fks(conn, tables, dry_run)

        print("\n" + "=" * 70)
        print("  Results")
        print("=" * 70)
        print(f"  Archive producers mapped:  {mapped_count:,}")
        print(f"  Rows re-linked:            {totals['relinked']:,}")
        print(f"  Rows nulled (no match):    {totals['cleared_no_match']:,}")
        print(f"  Wine IDs nulled:           {totals['cleared_wine_id']:,}")
        if dry_run:
            print("\n  (dry-run — no changes made)")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
