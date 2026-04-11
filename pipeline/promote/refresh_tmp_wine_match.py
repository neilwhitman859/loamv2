#!/usr/bin/env python3
"""Rebuild _tmp_wine_match from the current canonical wines + producers.

_tmp_wine_match is a helper table used by ttb_wine_link_v2.py to do fast
(producer_id, UPPER(name)) -> wine_id lookups. It was originally built against
the pre-rebuild canonical tables and still holds 490K archive.wines UUIDs.
(Prior to Session 14 Phase B W5, that table was named public.archive_wines.)

This script drops and rebuilds it from the current `wines` table with the
layout ttb_wine_link_v2.py expects:
    - producer_id  uuid
    - name_upper   text
    - wine_id      uuid

Usage:
    python -m pipeline.promote.refresh_tmp_wine_match --dry-run
    python -m pipeline.promote.refresh_tmp_wine_match --execute
"""

import argparse
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from pipeline.lib.db import get_conn


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--execute", action="store_true", help="Apply changes (default dry-run)")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    dry_run = not args.execute
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            # Count of what will be loaded
            cur.execute("""
                SELECT COUNT(*)
                FROM wines w
                WHERE w.deleted_at IS NULL
                  AND w.duplicate_of IS NULL
                  AND w.producer_id IS NOT NULL
                  AND w.name IS NOT NULL
            """)
            target_count = cur.fetchone()[0]
            print(f"Current canonical wines eligible for _tmp_wine_match: {target_count:,}")

            cur.execute("SELECT COUNT(*) FROM _tmp_wine_match")
            current_count = cur.fetchone()[0]
            print(f"Existing _tmp_wine_match rows: {current_count:,}")

            if dry_run:
                print("\n[DRY-RUN] Would:")
                print(f"  TRUNCATE _tmp_wine_match  ({current_count:,} rows)")
                print(f"  INSERT {target_count:,} rows from current wines")
                return

            print("\n[1/3] Truncating old _tmp_wine_match...")
            cur.execute("TRUNCATE TABLE _tmp_wine_match")
            conn.commit()

            print("[2/3] Inserting from current wines (bulk)...")
            t0 = time.time()
            cur.execute("""
                INSERT INTO _tmp_wine_match (producer_id, name_upper, wine_id)
                SELECT
                    w.producer_id,
                    UPPER(w.name),
                    w.id
                FROM wines w
                WHERE w.deleted_at IS NULL
                  AND w.duplicate_of IS NULL
                  AND w.producer_id IS NOT NULL
                  AND w.name IS NOT NULL
            """)
            inserted = cur.rowcount or 0
            conn.commit()
            print(f"  Inserted {inserted:,} rows in {time.time()-t0:.1f}s")

            print("[3/3] Verifying...")
            cur.execute("SELECT COUNT(*) FROM _tmp_wine_match")
            final = cur.fetchone()[0]
            print(f"  Final row count: {final:,}")

            # Validate that wine_ids now point at current table
            cur.execute("""
                SELECT
                    COUNT(*) FILTER (WHERE EXISTS (SELECT 1 FROM public.wines w WHERE w.id = t.wine_id)) AS in_current,
                    COUNT(*) FILTER (WHERE EXISTS (SELECT 1 FROM archive.wines aw WHERE aw.id = t.wine_id)) AS in_archive
                FROM _tmp_wine_match t
            """)
            in_current, in_archive = cur.fetchone()
            print(f"  Wine IDs in current public.wines: {in_current:,}")
            print(f"  Wine IDs in archive.wines: {in_archive:,}")
            if in_archive > 0:
                print("  WARNING: Some _tmp_wine_match rows still reference archive.wines.")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
