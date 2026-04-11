#!/usr/bin/env python3
"""Fast SQL-based TTB wine linker.

Replaces the REST-per-row ttb_wine_link_v2 with a single bulk JOIN. Links
source_ttb_colas.canonical_wine_id to canonical wines via:

    source_ttb_colas.canonical_producer_id
      + UPPER(source_ttb_colas.fanciful_name) == _tmp_wine_match.name_upper
      = _tmp_wine_match.wine_id

Prereqs:
    1. source_ttb_colas.canonical_producer_id has been re-linked to current
       producers (run relink_staging_to_current.py first).
    2. _tmp_wine_match has been refreshed from current canonical wines
       (run refresh_tmp_wine_match.py first).

Usage:
    python -m pipeline.promote.ttb_wine_link_sql --dry-run
    python -m pipeline.promote.ttb_wine_link_sql --execute
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
            # Guard: make sure prereqs are in place
            cur.execute("SELECT COUNT(*) FROM _tmp_wine_match")
            tmp_count = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM _tmp_wine_match WHERE wine_id IN (SELECT id FROM wines WHERE deleted_at IS NULL)")
            tmp_current = cur.fetchone()[0]
            if tmp_current == 0:
                print("ERROR: _tmp_wine_match has no rows pointing at current wines.")
                print("       Run: python -m pipeline.promote.refresh_tmp_wine_match --execute")
                return
            print(f"_tmp_wine_match: {tmp_count:,} rows ({tmp_current:,} in current wines)")

            cur.execute("""
                SELECT COUNT(*) FROM source_ttb_colas
                WHERE canonical_producer_id IS NOT NULL
                  AND canonical_wine_id IS NULL
                  AND fanciful_name IS NOT NULL
            """)
            candidate_count = cur.fetchone()[0]
            print(f"TTB candidate rows (have producer, need wine): {candidate_count:,}")

            # Count how many will actually match
            cur.execute("""
                SELECT COUNT(*)
                FROM source_ttb_colas sl
                JOIN _tmp_wine_match twm
                  ON twm.producer_id = sl.canonical_producer_id
                 AND twm.name_upper = UPPER(sl.fanciful_name)
                WHERE sl.canonical_wine_id IS NULL
                  AND sl.fanciful_name IS NOT NULL
            """)
            matchable_count = cur.fetchone()[0]
            print(f"Matchable rows: {matchable_count:,}")

            if dry_run:
                print("\n[DRY-RUN] Would UPDATE these rows with wine_id from _tmp_wine_match.")
                return

            print("\n[1/1] Running bulk UPDATE...")
            t0 = time.time()
            cur.execute("""
                UPDATE source_ttb_colas sl
                SET canonical_wine_id = twm.wine_id
                FROM _tmp_wine_match twm
                WHERE twm.producer_id = sl.canonical_producer_id
                  AND twm.name_upper = UPPER(sl.fanciful_name)
                  AND sl.canonical_wine_id IS NULL
                  AND sl.fanciful_name IS NOT NULL
            """)
            updated = cur.rowcount or 0
            conn.commit()
            print(f"  {updated:,} rows updated in {time.time()-t0:.1f}s")

            # Verify
            cur.execute("SELECT COUNT(*) FROM source_ttb_colas WHERE canonical_wine_id IS NOT NULL")
            total_linked = cur.fetchone()[0]
            print(f"\nTotal source_ttb_colas rows now linked to a wine: {total_linked:,}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
