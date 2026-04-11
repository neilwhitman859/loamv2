#!/usr/bin/env python3
"""Fast SQL-based COLA depth promotion.

Replaces cola_depth.py (REST, slow) with direct bulk SQL. Takes every
source_ttb_colas row that has canonical_wine_id set and:

    1. Inserts a COLA external_ids row for the wine (first COLA per wine
       wins per the unique constraint on (entity_type, entity_id, system)).
    2. Inserts a wine_vintages row for the COLA's wine_vintage year, skipping
       conflicts with pre-existing vintages.

Prereqs:
    - source_ttb_colas.canonical_wine_id has been linked (run ttb_wine_link_sql)

Usage:
    python -m pipeline.promote.cola_depth_sql --dry-run
    python -m pipeline.promote.cola_depth_sql --execute
"""

import argparse
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from pipeline.lib.db import get_conn


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--execute", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    dry_run = not args.execute
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            # Diagnostics
            cur.execute("SELECT COUNT(*) FROM source_ttb_colas WHERE canonical_wine_id IS NOT NULL")
            linked_ttb = cur.fetchone()[0]
            print(f"source_ttb_colas rows with canonical_wine_id: {linked_ttb:,}")

            # 1. How many COLAs are insertable?
            cur.execute("""
                SELECT COUNT(DISTINCT (sl.canonical_wine_id, sl.ttb_id))
                FROM source_ttb_colas sl
                WHERE sl.canonical_wine_id IS NOT NULL
                  AND sl.ttb_id IS NOT NULL
            """)
            potential_colas = cur.fetchone()[0]

            cur.execute("""
                SELECT COUNT(DISTINCT sl.canonical_wine_id)
                FROM source_ttb_colas sl
                WHERE sl.canonical_wine_id IS NOT NULL
                  AND NOT EXISTS (
                      SELECT 1 FROM external_ids e
                      WHERE e.entity_type = 'wine'
                        AND e.entity_id = sl.canonical_wine_id
                        AND e.system = 'cola'
                  )
            """)
            insertable_cola_wines = cur.fetchone()[0]
            print(f"  {potential_colas:,} TTB-wine pairs, {insertable_cola_wines:,} wines need first COLA external_id")

            # 2. How many vintages are insertable?
            cur.execute("""
                SELECT COUNT(DISTINCT (sl.canonical_wine_id, sl.wine_vintage))
                FROM source_ttb_colas sl
                WHERE sl.canonical_wine_id IS NOT NULL
                  AND sl.wine_vintage ~ '^[0-9]{4}$'
                  AND CAST(sl.wine_vintage AS INT) BETWEEN 1900 AND 2030
                  AND NOT EXISTS (
                      SELECT 1 FROM wine_vintages wv
                      WHERE wv.wine_id = sl.canonical_wine_id
                        AND wv.vintage_year = CAST(sl.wine_vintage AS INT)
                  )
            """)
            insertable_vintages = cur.fetchone()[0]
            print(f"  {insertable_vintages:,} new (wine_id, vintage_year) pairs to insert")

            if dry_run:
                print("\n  [DRY-RUN] No changes made")
                return

            # INSERT COLA external IDs (one per wine, using the first ttb_id seen via ORDER BY)
            print("\n[1/2] Inserting COLA external_ids...")
            t0 = time.time()
            cur.execute("""
                INSERT INTO external_ids (entity_type, entity_id, system, external_id)
                SELECT DISTINCT ON (sl.canonical_wine_id)
                    'wine', sl.canonical_wine_id, 'cola', sl.ttb_id
                FROM source_ttb_colas sl
                WHERE sl.canonical_wine_id IS NOT NULL
                  AND sl.ttb_id IS NOT NULL
                  AND NOT EXISTS (
                      SELECT 1 FROM external_ids e
                      WHERE e.entity_type = 'wine'
                        AND e.entity_id = sl.canonical_wine_id
                        AND e.system = 'cola'
                  )
                ORDER BY sl.canonical_wine_id, sl.ttb_id
                ON CONFLICT (entity_id, entity_type, system, external_id) DO NOTHING
            """)
            cola_inserted = cur.rowcount or 0
            conn.commit()
            print(f"  {cola_inserted:,} COLA external_ids inserted in {time.time()-t0:.1f}s")

            # INSERT wine_vintages from COLA wine_vintage column
            print("\n[2/2] Inserting wine_vintages from TTB vintages...")
            t0 = time.time()
            cur.execute("""
                INSERT INTO wine_vintages (wine_id, vintage_year, abv)
                SELECT DISTINCT ON (sl.canonical_wine_id, CAST(sl.wine_vintage AS INT))
                    sl.canonical_wine_id,
                    CAST(sl.wine_vintage AS INT),
                    CASE
                        WHEN sl.abv ~ '^[0-9]+(\\.[0-9]+)?$' THEN CAST(sl.abv AS NUMERIC)
                        ELSE NULL
                    END
                FROM source_ttb_colas sl
                WHERE sl.canonical_wine_id IS NOT NULL
                  AND sl.wine_vintage ~ '^[0-9]{4}$'
                  AND CAST(sl.wine_vintage AS INT) BETWEEN 1900 AND 2030
                  AND NOT EXISTS (
                      SELECT 1 FROM wine_vintages wv
                      WHERE wv.wine_id = sl.canonical_wine_id
                        AND wv.vintage_year = CAST(sl.wine_vintage AS INT)
                  )
                ORDER BY sl.canonical_wine_id, CAST(sl.wine_vintage AS INT), sl.ttb_id
                ON CONFLICT DO NOTHING
            """)
            vintage_inserted = cur.rowcount or 0
            conn.commit()
            print(f"  {vintage_inserted:,} wine_vintages inserted in {time.time()-t0:.1f}s")

            # Summary
            cur.execute("""
                SELECT
                    (SELECT COUNT(*) FROM external_ids WHERE system='cola') AS total_cola,
                    (SELECT COUNT(*) FROM wine_vintages) AS total_vintages
            """)
            total_cola, total_vintages = cur.fetchone()
            print(f"\nTotals after: {total_cola:,} COLA external_ids, {total_vintages:,} wine_vintages")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
