"""
dedup_wines.py — Soft-delete exact duplicate wine rows created by Phase B wine creation.

Duplicates are wines with identical (name, producer_id, color, wine_type, appellation_id)
that have 0 linked wine_vintages. These were created when TTB COLA records for the same wine
came through multiple promotion passes.

Strategy:
  - Per duplicate group: keep MIN(id) as canonical
  - Reassign external_ids from delete_ids → keep_id
  - Reassign wine_grapes from delete_ids → keep_id (skip conflicts)
  - Transfer label_image_url to keep_id if keep lacks one
  - Soft-delete all delete_ids (set deleted_at = NOW())

Run with: python -m pipeline.promote.dedup_wines [--dry-run]
"""

import sys
import argparse
from datetime import datetime, timezone
from pipeline.lib.db import get_conn

DRY_RUN = False


def build_dedup_map(cur):
    """Return list of (keep_id, [delete_ids]) for all exact duplicate wine groups."""
    cur.execute("""
        SELECT
            MIN(w.id::text) AS keep_id,
            ARRAY_AGG(w.id::text ORDER BY w.id::text) AS all_ids
        FROM wines w
        WHERE w.deleted_at IS NULL
          AND w.producer_id IS NOT NULL
          AND NOT EXISTS (
              SELECT 1 FROM wine_vintages wv WHERE wv.wine_id = w.id
          )
        GROUP BY w.name, w.producer_id, w.color, w.wine_type, w.appellation_id
        HAVING COUNT(*) > 1
    """)
    rows = cur.fetchall()
    result = []
    for keep_id, all_ids in rows:
        delete_ids = [x for x in all_ids if x != keep_id]
        result.append((keep_id, delete_ids))
    return result


def reassign_external_ids(cur, keep_id: str, delete_ids: list[str], total_reassigned: list):
    """Reassign external_ids from delete_ids to keep_id.
    Unique constraint is (entity_type, entity_id, system) — skip systems where keep already has an entry.
    Delete leftover rows that couldn't be reassigned (keep already has that system).
    """
    for del_id in delete_ids:
        # Reassign rows where keep doesn't already have the same system
        cur.execute("""
            UPDATE external_ids
            SET entity_id = %s::uuid
            WHERE entity_type = 'wine'
              AND entity_id = %s::uuid
              AND system NOT IN (
                  SELECT system FROM external_ids
                  WHERE entity_type = 'wine' AND entity_id = %s::uuid
              )
        """, (keep_id, del_id, keep_id))
        total_reassigned[0] += cur.rowcount

        # Delete any remaining rows on del_id that couldn't be reassigned (keep already has that system)
        cur.execute("""
            DELETE FROM external_ids
            WHERE entity_type = 'wine' AND entity_id = %s::uuid
        """, (del_id,))


def reassign_wine_grapes(cur, keep_id: str, delete_ids: list[str], total_reassigned: list):
    """Reassign wine_grapes from delete_ids to keep_id. Skip if keep already has that grape."""
    for del_id in delete_ids:
        cur.execute("""
            UPDATE wine_grapes
            SET wine_id = %s::uuid
            WHERE wine_id = %s::uuid
              AND NOT EXISTS (
                  SELECT 1 FROM wine_grapes wg2
                  WHERE wg2.wine_id = %s::uuid
                    AND wg2.grape_id = wine_grapes.grape_id
              )
        """, (keep_id, del_id, keep_id))
        n = cur.rowcount
        if n > 0:
            total_reassigned[0] += n


def transfer_label_image(cur, keep_id: str, delete_ids: list[str], total_transferred: list):
    """If keep_id has no label_image_url but a delete_id does, transfer it."""
    cur.execute("SELECT label_image_url FROM wines WHERE id = %s::uuid", (keep_id,))
    row = cur.fetchone()
    if row and row[0]:
        return  # Keep already has image

    for del_id in delete_ids:
        cur.execute("SELECT label_image_url FROM wines WHERE id = %s::uuid", (del_id,))
        row = cur.fetchone()
        if row and row[0]:
            cur.execute(
                "UPDATE wines SET label_image_url = %s, updated_at = NOW() WHERE id = %s::uuid",
                (row[0], keep_id)
            )
            total_transferred[0] += 1
            break  # Only need one image


def soft_delete_duplicates(cur, delete_ids: list[str], total_deleted: list):
    """Soft-delete the duplicate wines."""
    if not delete_ids:
        return
    placeholders = ','.join(['%s::uuid'] * len(delete_ids))
    cur.execute(
        f"UPDATE wines SET deleted_at = NOW(), updated_at = NOW() WHERE id IN ({placeholders})",
        delete_ids
    )
    total_deleted[0] += cur.rowcount


def run(dry_run: bool = False):
    global DRY_RUN
    DRY_RUN = dry_run

    conn = get_conn()
    cur = conn.cursor()

    print(f"[dedup_wines] {'DRY RUN — ' if dry_run else ''}Building duplicate map...")
    dedup_groups = build_dedup_map(cur)
    total_groups = len(dedup_groups)
    total_to_delete = sum(len(d) for _, d in dedup_groups)
    print(f"  Found {total_groups} duplicate groups, {total_to_delete} wines to soft-delete")

    if dry_run:
        # Sample a few
        for keep_id, del_ids in dedup_groups[:5]:
            print(f"  KEEP {keep_id}, DELETE {del_ids}")
        conn.close()
        return

    ext_reassigned = [0]
    grape_reassigned = [0]
    images_transferred = [0]
    deleted = [0]

    batch_size = 500
    processed = 0

    for i in range(0, len(dedup_groups), batch_size):
        batch = dedup_groups[i:i + batch_size]
        for keep_id, delete_ids in batch:
            reassign_external_ids(cur, keep_id, delete_ids, ext_reassigned)
            reassign_wine_grapes(cur, keep_id, delete_ids, grape_reassigned)
            transfer_label_image(cur, keep_id, delete_ids, images_transferred)
            soft_delete_duplicates(cur, delete_ids, deleted)
        conn.commit()
        processed += len(batch)
        print(f"  Processed {processed}/{total_groups} groups | "
              f"deleted={deleted[0]}, ext_reassigned={ext_reassigned[0]}, "
              f"grapes_reassigned={grape_reassigned[0]}, images={images_transferred[0]}")

    # Log to accuracy_audit
    cur.execute("""
        INSERT INTO accuracy_audit (entity_type, entity_id, field_name, old_value, new_value,
                                    action_type, evidence_type, evidence_detail, confidence)
        VALUES ('wine', (SELECT id FROM wines WHERE deleted_at IS NOT NULL ORDER BY deleted_at DESC LIMIT 1),
                'deleted_at', NULL, 'NOW()', 'fix', 'cross_table_consistency',
                %s, 0.99)
    """, (
        f"Dedup: soft-deleted {deleted[0]} exact duplicate wine rows across {total_groups} groups. "
        f"Reassigned {ext_reassigned[0]} external_ids, {grape_reassigned[0]} wine_grapes, "
        f"{images_transferred[0]} label images transferred to keep IDs.",
    ))
    conn.commit()

    print(f"\n[dedup_wines] COMPLETE")
    print(f"  Groups processed:     {total_groups}")
    print(f"  Wines soft-deleted:   {deleted[0]}")
    print(f"  External IDs moved:   {ext_reassigned[0]}")
    print(f"  Grape links moved:    {grape_reassigned[0]}")
    print(f"  Images transferred:   {images_transferred[0]}")

    conn.close()
    return {
        "groups": total_groups,
        "deleted": deleted[0],
        "ext_reassigned": ext_reassigned[0],
        "grape_reassigned": grape_reassigned[0],
        "images_transferred": images_transferred[0],
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    run(dry_run=args.dry_run)
