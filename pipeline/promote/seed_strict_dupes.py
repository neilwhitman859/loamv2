#!/usr/bin/env python3
"""Seed match_decisions with strict-exact wine duplicate groups.

Populates match_decisions.status = 'ai_accepted' for every group of wines that
share (name_normalized, producer_id, color, wine_type, appellation_id). These
are known-identical by deterministic key — no AI needed. Once seeded,
wine_merge.py consumes them and does the actual merge (including vintage
conflict handling).

Why not just use dedup_wines.py?
    dedup_wines.py only handles groups where NO wine has vintages. It soft-
    deletes dupes without reassigning vintage rows, which would orphan them.
    For LWIN long-tail cleanup, ~half the strict-exact groups mix LWIN wines
    (vintage-free) with pre-rebuild wines that picked up vintages from the
    earlier 30K plan. wine_merge.py handles that case correctly.

Picking the survivor:
    The wine with the MOST vintage children wins. Ties are broken by oldest
    created_at (the pre-rebuild wines tend to have more data than the fresh
    LWIN-only creates). Falls back to lowest id.

Usage:
    python -m pipeline.promote.seed_strict_dupes --dry-run
    python -m pipeline.promote.seed_strict_dupes --execute
    python -m pipeline.promote.seed_strict_dupes --execute --limit 100
"""

import argparse
import json
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from pipeline.lib.db import get_conn


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--execute", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--limit", type=int)
    args = parser.parse_args()

    dry_run = not args.execute
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            # Find strict exact dupe groups. Pick survivor = wine with most vintages,
            # then oldest created_at, then lowest id.
            print("[1/3] Identifying strict-exact duplicate groups...")
            t0 = time.time()
            limit_sql = f"LIMIT {int(args.limit)}" if args.limit else ""
            cur.execute(f"""
                WITH ranked AS (
                    SELECT
                        w.id,
                        w.name_normalized,
                        w.producer_id,
                        w.color,
                        w.wine_type,
                        w.appellation_id,
                        w.created_at,
                        (SELECT COUNT(*) FROM wine_vintages wv WHERE wv.wine_id = w.id) AS vintage_count,
                        ROW_NUMBER() OVER (
                            PARTITION BY w.name_normalized, w.producer_id, w.color, w.wine_type, w.appellation_id
                            ORDER BY
                                (SELECT COUNT(*) FROM wine_vintages wv WHERE wv.wine_id = w.id) DESC,
                                w.created_at ASC,
                                w.id ASC
                        ) AS rn,
                        COUNT(*) OVER (
                            PARTITION BY w.name_normalized, w.producer_id, w.color, w.wine_type, w.appellation_id
                        ) AS group_size
                    FROM wines w
                    WHERE w.deleted_at IS NULL
                      AND w.duplicate_of IS NULL
                      AND w.name_normalized IS NOT NULL
                      AND w.producer_id IS NOT NULL
                )
                SELECT name_normalized, producer_id, color, wine_type, appellation_id,
                       array_agg(id::text ORDER BY rn) AS ordered_ids
                FROM ranked
                WHERE group_size > 1
                GROUP BY name_normalized, producer_id, color, wine_type, appellation_id
                {limit_sql}
            """)
            groups = cur.fetchall()
            print(f"  Found {len(groups):,} strict-exact groups ({time.time()-t0:.1f}s)")

            if not groups:
                print("  Nothing to do.")
                return

            total_dupes = sum(len(row[5]) - 1 for row in groups)
            print(f"  Total dupe wines to merge: {total_dupes:,}")
            print(f"  Survivors (kept): {len(groups):,}")

            if dry_run:
                print("\n  Sample groups (first 5):")
                for name_norm, prod_id, color, wtype, appel, ids in groups[:5]:
                    survivor = ids[0]
                    dupes = ids[1:]
                    cur.execute("SELECT name, producer_id FROM wines WHERE id = %s", (survivor,))
                    row = cur.fetchone()
                    name = row[0] if row else "?"
                    cur.execute("SELECT name FROM producers WHERE id = %s", (prod_id,))
                    prow = cur.fetchone()
                    pname = prow[0] if prow else "?"
                    print(f"    {name!r} @ {pname!r}: keep {survivor[:8]}, merge {len(dupes)} dupes")
                print("\n  (DRY-RUN — no writes to match_decisions)")
                return

            # Insert match_decisions rows
            print("\n[2/3] Writing match_decisions...")
            t0 = time.time()
            inserted = 0
            errors = 0
            BATCH = 500
            for i in range(0, len(groups), BATCH):
                batch = groups[i:i+BATCH]
                for name_norm, prod_id, color, wtype, appel, ids in batch:
                    survivor = ids[0]
                    dupes = ids[1:]
                    if not dupes:
                        continue
                    try:
                        cur.execute("""
                            INSERT INTO match_decisions (
                                id, source_a, source_a_id, source_b, source_b_id,
                                entity_type, match_method, confidence,
                                status, ai_model, notes,
                                created_at, updated_at
                            ) VALUES (
                                gen_random_uuid(),
                                'wines', %s,
                                'wines', %s,
                                'wine', 'strict_exact_key', 1.00,
                                'ai_accepted', 'deterministic',
                                %s,
                                now(), now()
                            )
                        """, (
                            survivor,
                            ",".join(dupes),
                            f"Strict-exact match: name_normalized+producer+color+wine_type+appellation_id | {len(dupes)} dupes",
                        ))
                        inserted += 1
                    except Exception as e:
                        errors += 1
                        if errors <= 5:
                            print(f"  ERROR: {e}")
                conn.commit()
                print(f"  Batch {i//BATCH + 1}/{(len(groups) + BATCH - 1)//BATCH}: {inserted:,} inserted, {errors} errors")

            print(f"  Done in {time.time()-t0:.1f}s")

            # Verify
            print("\n[3/3] Verifying...")
            cur.execute("""
                SELECT COUNT(*) FROM match_decisions
                WHERE entity_type='wine' AND status='ai_accepted'
                  AND (notes IS NULL OR notes NOT LIKE '%[merged]%')
            """)
            pending = cur.fetchone()[0]
            print(f"  match_decisions pending merge: {pending:,}")
            print(f"\n  Now run: python -m pipeline.promote.wine_merge --execute")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
