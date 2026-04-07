#!/usr/bin/env python3
"""
wine_merge.py — Merge Haiku-classified duplicate wine groups into single canonical records.

Reads match_decisions (status='ai_accepted'), merges all duplicate IDs into the
survivor (source_a_id), re-points every child table, NULL-fills missing survivor
columns from duplicates, then soft-deletes the duplicates.

Only processes status='ai_accepted' groups. Flagged and rejected are untouched.

Usage:
    python -m pipeline.promote.wine_merge            # dry-run (default, safe)
    python -m pipeline.promote.wine_merge --execute  # apply changes
    python -m pipeline.promote.wine_merge --execute --limit 500

Options:
    --execute    Apply changes (default is dry-run)
    --limit N    Process at most N groups
"""

import sys
import argparse
from pipeline.lib.db import get_conn

# ---------------------------------------------------------------------------
# Child tables to re-point: wine_id FK → survivor
# Each entry: (table, wine_id_column, conflict_columns_or_None)
# conflict_columns: list of columns that form a unique key with wine_id.
#   When a dupe row would conflict, the dupe row is deleted (survivor already has it).
# None = simple UPDATE, no conflict handling needed.
# ---------------------------------------------------------------------------

WINE_ID_TABLES = [
    # Tables with unique (wine_id, X) constraints — delete conflicts first
    ("wine_grapes",                      "wine_id", ["grape_id"]),
    ("wine_label_designations",          "wine_id", ["label_designation_id"]),
    ("wine_appellations",                "wine_id", ["appellation_id"]),
    ("wine_regions",                     "wine_id", ["region_id"]),
    ("wine_soils",                       "wine_id", ["soil_type_id"]),
    ("wine_vineyards",                   "wine_id", ["vineyard_id"]),
    ("wine_biodiversity_certifications", "wine_id", ["biodiversity_certification_id"]),
    ("wine_farming_certifications",      "wine_id", ["farming_certification_id"]),
    ("wine_food_pairings",               "wine_id", ["food_category_id"]),
    ("wine_relationships",               "wine_id", ["related_wine_id"]),
    # One-per-wine tables — if survivor has one, drop dupe's
    ("wine_insights",                    "wine_id", ["wine_id"]),  # 1:1
    # Vintage tables using (wine_id, vintage_year) — no wine_vintage_id
    ("wine_vintage_grapes",              "wine_id", ["grape_id"]),      # wine_vintage_id already re-pointed above
    ("wine_vintage_formats",             "wine_id", ["bottle_format_id"]),
    ("wine_vintage_tasting_insights",    "wine_id", None),
    ("wine_vintage_insights",            "wine_id", None),
    ("wine_vintage_vineyards",           "wine_id", None),
    ("wine_vintage_descriptors",         "wine_id", None),
    ("wine_vintage_documents",           "wine_id", None),
    ("wine_vintage_nv_components",       "wine_id", None),
    # Simple re-point — no meaningful unique constraint with wine_id alone
    ("wine_aliases",                     "wine_id", None),
    ("wine_descriptors",                 "wine_id", None),
    ("wine_lookups",                     "wine_id", None),
    ("wine_water_bodies",                "wine_id", None),
]

# wine_relationships also has a related_wine_id FK — re-point that separately
WINE_RELATED_TABLES = [
    ("wine_relationships",   "related_wine_id"),
    ("wine_vintage_nv_components", "component_wine_id"),
    ("wines",                "parent_wine_id"),
]

# Staging tables: canonical_wine_id column, simple re-point
STAGING_TABLES = [
    "source_bc_liquor", "source_berliner", "source_best_wine_store",
    "source_domestique", "source_empson", "source_enofile",
    "source_european_cellars", "source_firstleaf", "source_flatiron",
    "source_horizon", "source_kansas_brands", "source_kermit_lynch",
    "source_last_bottle", "source_lcbo", "source_lwin",
    "source_openfoodfacts", "source_pa", "source_polaner",
    "source_pro_platform", "source_skurnik", "source_specs",
    "source_systembolaget", "source_tabc", "source_texsom",
    "source_ttb_colas", "source_utah_dabs", "source_wallys",
    "source_winebow", "source_winedeals", "source_wv_abca",
    # _tmp_wine_match uses wine_id not canonical_wine_id — handled separately
]

# Tables using wine_id (not canonical_wine_id) that are not canonical child tables
WINE_ID_LOOKUP_TABLES = ["_tmp_wine_match"]

# Columns to NULL-fill on survivor from dupes (first non-NULL wins)
NULL_FILL_COLUMNS = [
    "appellation_id", "region_id", "country_id", "color",
    "varietal_category_id", "style", "label_image_url",
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def load_groups(cur):
    """Load all ai_accepted groups not yet merged from match_decisions.

    Returns list of (decision_id, survivor_id, [dupe_ids]).
    """
    cur.execute("""
        SELECT id, source_a_id, source_b_id
        FROM match_decisions
        WHERE status = 'ai_accepted'
        AND entity_type = 'wine'
        AND (notes IS NULL OR notes NOT LIKE '%%[merged]%%')
        ORDER BY created_at
    """)
    rows = cur.fetchall()
    groups = []
    for decision_id, survivor_id, source_b_id in rows:
        if not source_b_id:
            continue
        dupe_ids = [d.strip() for d in source_b_id.split(",") if d.strip()]
        if dupe_ids:
            groups.append((str(decision_id), str(survivor_id), dupe_ids))
    return groups


def null_fill_survivor(cur, survivor_id, dupe_ids, dry_run, stats):
    """Copy first non-NULL value from dupes into survivor for missing columns."""
    # Get survivor's current values
    cols = ", ".join(NULL_FILL_COLUMNS)
    cur.execute(f"SELECT {cols} FROM wines WHERE id = %s::uuid", (survivor_id,))
    row = cur.fetchone()
    if not row:
        return
    survivor_vals = dict(zip(NULL_FILL_COLUMNS, row))

    missing = [c for c in NULL_FILL_COLUMNS if survivor_vals[c] is None]
    if not missing:
        return

    # Collect first non-NULL values from dupes
    fill = {}
    placeholders = ",".join(["%s::uuid"] * len(dupe_ids))
    missing_cols = ", ".join(missing)
    cur.execute(
        f"SELECT {missing_cols} FROM wines WHERE id IN ({placeholders}) AND deleted_at IS NULL",
        dupe_ids,
    )
    for dupe_row in cur.fetchall():
        for col, val in zip(missing, dupe_row):
            if col not in fill and val is not None:
                fill[col] = val
        if len(fill) == len(missing):
            break

    if not fill:
        return

    set_clause = ", ".join(f"{c} = %s" for c in fill)
    values = list(fill.values()) + [survivor_id]
    if not dry_run:
        cur.execute(
            f"UPDATE wines SET {set_clause}, updated_at = NOW() WHERE id = %s::uuid",
            values,
        )
    stats["null_fills"] += len(fill)


# Tables that actually have a wine_vintage_id FK column
VINTAGE_CHILD_TABLES = [
    ("wine_vintage_prices",  "wine_vintage_id"),
    ("wine_vintage_scores",  "wine_vintage_id"),
    ("wine_vintage_grapes",  "wine_vintage_id"),  # also needs wine_id re-point below
]


def handle_vintages(cur, survivor_id, dupe_ids, dry_run, stats):
    """Process all vintages for every dupe in order.

    For each vintage:
    - If year conflicts with an existing survivor vintage (including ones moved
      from earlier dupes in this same group): re-point its children to the
      survivor vintage, then delete it.
    - Otherwise: re-point wine_id to survivor.

    Processing per-vintage rather than per-dupe ensures intra-group conflicts
    (two dupes both having year X) are handled correctly.
    """
    for dupe_id in dupe_ids:
        cur.execute(
            "SELECT id, vintage_year FROM wine_vintages WHERE wine_id = %s::uuid",
            (dupe_id,),
        )
        dupe_vintages = cur.fetchall()

        for dupe_vid, year in dupe_vintages:
            dupe_vid = str(dupe_vid)

            # Check for conflict against current survivor vintages
            cur.execute(
                "SELECT id FROM wine_vintages WHERE wine_id = %s::uuid AND vintage_year = %s",
                (survivor_id, year),
            )
            conflict = cur.fetchone()

            if conflict:
                surv_vid = str(conflict[0])
                # Re-point all children of the dupe vintage → survivor vintage
                for table, col in VINTAGE_CHILD_TABLES:
                    if not dry_run:
                        cur.execute(
                            f"UPDATE {table} SET {col} = %s::uuid WHERE {col} = %s::uuid",
                            (surv_vid, dupe_vid),
                        )
                        stats["vintage_child_repoints"] += cur.rowcount
                # Delete the now-empty dupe vintage
                if not dry_run:
                    cur.execute("DELETE FROM wine_vintages WHERE id = %s::uuid", (dupe_vid,))
                stats["vintages_merged"] += 1
            else:
                # No conflict — re-point wine_id to survivor
                if not dry_run:
                    cur.execute(
                        "UPDATE wine_vintages SET wine_id = %s::uuid WHERE id = %s::uuid",
                        (survivor_id, dupe_vid),
                    )
                    stats["child_repoints"] += cur.rowcount


def repoint_wine_id_tables(cur, survivor_id, dupe_ids, dry_run, stats):
    """Re-point wine_id FK columns from all dupe IDs → survivor.

    For tables with conflict_columns, delete conflicting dupe rows first.
    """
    for table, col, conflict_cols in WINE_ID_TABLES:
        for dupe_id in dupe_ids:
            if conflict_cols:
                # Special case: 1:1 table (conflict_cols == ["wine_id"])
                if conflict_cols == ["wine_id"]:
                    # If survivor already has a row, just drop dupe's row
                    cur.execute(
                        f"SELECT 1 FROM {table} WHERE {col} = %s::uuid", (survivor_id,)
                    )
                    if cur.fetchone():
                        if not dry_run:
                            cur.execute(
                                f"DELETE FROM {table} WHERE {col} = %s::uuid", (dupe_id,)
                            )
                        continue

                else:
                    # Delete dupe rows that would conflict with survivor rows
                    conflict_join = " AND ".join(
                        f"existing.{c} = dupe.{c}" for c in conflict_cols
                    )
                    if not dry_run:
                        cur.execute(f"""
                            DELETE FROM {table} dupe
                            USING {table} existing
                            WHERE dupe.{col} = %s::uuid
                            AND existing.{col} = %s::uuid
                            AND {conflict_join}
                        """, (dupe_id, survivor_id))
                        stats["conflict_drops"] += cur.rowcount

            # Re-point remaining rows
            if not dry_run:
                cur.execute(
                    f"UPDATE {table} SET {col} = %s::uuid WHERE {col} = %s::uuid",
                    (survivor_id, dupe_id),
                )
                stats["child_repoints"] += cur.rowcount


def repoint_related_wine_columns(cur, survivor_id, dupe_ids, dry_run, stats):
    """Re-point non-primary wine_id columns (related_wine_id, parent_wine_id, etc.)."""
    ph = ",".join(["%s::uuid"] * len(dupe_ids))
    for table, col in WINE_RELATED_TABLES:
        if not dry_run:
            cur.execute(
                f"UPDATE {table} SET {col} = %s::uuid WHERE {col} IN ({ph})",
                [survivor_id] + dupe_ids,
            )
            stats["child_repoints"] += cur.rowcount


def repoint_external_ids(cur, survivor_id, dupe_ids, dry_run, stats):
    """Re-point external_ids. Unique on (entity_type, entity_id, system, external_id).

    For each dupe, reassign rows where survivor doesn't already have the same (system, external_id).
    Delete any remaining dupe rows (survivor already has that identifier).
    """
    for dupe_id in dupe_ids:
        if not dry_run:
            # Reassign non-conflicting rows
            cur.execute("""
                UPDATE external_ids
                SET entity_id = %s::uuid
                WHERE entity_type = 'wine'
                AND entity_id = %s::uuid
                AND (system, external_id) NOT IN (
                    SELECT system, external_id FROM external_ids
                    WHERE entity_type = 'wine' AND entity_id = %s::uuid
                )
            """, (survivor_id, dupe_id, survivor_id))
            stats["external_id_repoints"] += cur.rowcount

            # Delete remaining (duplicates the survivor already has)
            cur.execute("""
                DELETE FROM external_ids
                WHERE entity_type = 'wine' AND entity_id = %s::uuid
            """, (dupe_id,))


def repoint_vintage_prices_scores(cur, survivor_id, dupe_ids, dry_run, stats):
    """Update wine_id on prices/scores to match survivor.

    wine_vintage_id is already correct (set during vintage conflict merge).
    wine_vintage_formats handled in WINE_ID_TABLES (uses wine_id+vintage_year key).
    """
    ph = ",".join(["%s::uuid"] * len(dupe_ids))
    for table in ("wine_vintage_prices", "wine_vintage_scores"):
        if not dry_run:
            cur.execute(
                f"UPDATE {table} SET wine_id = %s::uuid WHERE wine_id IN ({ph})",
                [survivor_id] + dupe_ids,
            )
            stats["child_repoints"] += cur.rowcount


def repoint_staging_tables(cur, survivor_id, dupe_ids, dry_run, stats):
    """Update canonical_wine_id on all staging tables, and wine_id on lookup tables.

    Uses IN() to batch all dupe_ids in a single query per table.
    """
    ph = ",".join(["%s::uuid"] * len(dupe_ids))
    for table in STAGING_TABLES:
        if not dry_run:
            cur.execute(
                f"UPDATE {table} SET canonical_wine_id = %s::uuid "
                f"WHERE canonical_wine_id IN ({ph})",
                [survivor_id] + dupe_ids,
            )
            stats["staging_repoints"] += cur.rowcount
    for table in WINE_ID_LOOKUP_TABLES:
        if not dry_run:
            cur.execute(
                f"UPDATE {table} SET wine_id = %s::uuid WHERE wine_id IN ({ph})",
                [survivor_id] + dupe_ids,
            )
            stats["staging_repoints"] += cur.rowcount


def soft_delete_dupes(cur, survivor_id, dupe_ids, dry_run, stats):
    """Soft-delete all dupe wines: set deleted_at + duplicate_of."""
    placeholders = ",".join(["%s::uuid"] * len(dupe_ids))
    if not dry_run:
        cur.execute(
            f"""UPDATE wines
                SET deleted_at = NOW(),
                    duplicate_of = %s::uuid,
                    updated_at = NOW()
                WHERE id IN ({placeholders})
                AND deleted_at IS NULL""",
            [survivor_id] + dupe_ids,
        )
    stats["wines_deleted"] += len(dupe_ids)


def mark_merged(cur, decision_id, dry_run):
    """Append [merged] to match_decisions.notes so this group is skipped on re-run."""
    if not dry_run:
        cur.execute("""
            UPDATE match_decisions
            SET notes = COALESCE(notes, '') || ' [merged]',
                updated_at = NOW()
            WHERE id = %s::uuid
        """, (decision_id,))


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def run(execute=False, limit=None):
    dry_run = not execute
    mode = "DRY RUN" if dry_run else "EXECUTE"

    conn = get_conn()
    cur = conn.cursor()

    print(f"[wine_merge] {mode} — loading match_decisions...")
    groups = load_groups(cur)
    if limit:
        groups = groups[:limit]

    total = len(groups)
    total_dupes = sum(len(d) for _, _, d in groups)
    print(f"  {total} groups to merge ({total_dupes} dupe wines total)")

    if total == 0:
        print("  Nothing to do.")
        conn.close()
        return

    stats = {
        "groups": 0,
        "wines_deleted": 0,
        "vintages_merged": 0,
        "vintage_child_repoints": 0,
        "child_repoints": 0,
        "external_id_repoints": 0,
        "staging_repoints": 0,
        "conflict_drops": 0,
        "null_fills": 0,
        "errors": 0,
    }

    # Dry-run: just summarise and sample — don't do real work
    if dry_run:
        dupe_counts = [len(d) for _, _, d in groups]
        print(f"\n  Dry-run summary:")
        print(f"    Groups:          {total}")
        print(f"    Wines to delete: {sum(dupe_counts)}")
        print(f"    Avg dupes/group: {sum(dupe_counts)/total:.1f}")
        print(f"    Max dupes/group: {max(dupe_counts)}")
        print(f"\n  Sample groups (first 5):")
        for decision_id, survivor_id, dupe_ids in groups[:5]:
            cur.execute("SELECT name FROM wines WHERE id = %s::uuid", (survivor_id,))
            row = cur.fetchone()
            name = row[0] if row else "?"
            print(f"    KEEP {survivor_id[:8]}.. ({name!r}), DELETE {len(dupe_ids)} dupes")
        conn.close()
        print("\n  Run with --execute to apply.")
        return stats

    BATCH = 200
    for i in range(0, total, BATCH):
        batch = groups[i:i + BATCH]
        for decision_id, survivor_id, dupe_ids in batch:
            try:
                # 1. NULL-fill survivor columns from dupes
                null_fill_survivor(cur, survivor_id, dupe_ids, dry_run, stats)

                # 2. Handle all vintages: merge conflicts, re-point non-conflicts
                handle_vintages(cur, survivor_id, dupe_ids, dry_run, stats)

                # 3. Re-point wine_id FK tables (with conflict handling)
                repoint_wine_id_tables(cur, survivor_id, dupe_ids, dry_run, stats)

                # 5. Re-point related wine columns (parent_wine_id, etc.)
                repoint_related_wine_columns(cur, survivor_id, dupe_ids, dry_run, stats)

                # 6. Re-point external_ids
                repoint_external_ids(cur, survivor_id, dupe_ids, dry_run, stats)

                # 7. Re-point wine_id on prices/scores/formats
                repoint_vintage_prices_scores(cur, survivor_id, dupe_ids, dry_run, stats)

                # 8. Re-point staging tables
                repoint_staging_tables(cur, survivor_id, dupe_ids, dry_run, stats)

                # 9. Soft-delete dupes
                soft_delete_dupes(cur, survivor_id, dupe_ids, dry_run, stats)

                # 10. Mark decision as merged
                mark_merged(cur, decision_id, dry_run)

                stats["groups"] += 1

            except Exception as e:
                conn.rollback()
                stats["errors"] += 1
                print(f"  ERROR group {decision_id} (survivor={survivor_id}): {e}")
                continue

        conn.commit()

        pct = (i + len(batch)) / total * 100
        print(
            f"  [{pct:5.1f}%] {stats['groups']} merged | "
            f"deleted={stats['wines_deleted']} vintages_merged={stats['vintages_merged']} "
            f"ext_ids={stats['external_id_repoints']} errors={stats['errors']}"
        )

    print(f"\n[wine_merge] {'DRY RUN COMPLETE' if dry_run else 'COMPLETE'}")
    print(f"  Groups merged:           {stats['groups']}")
    print(f"  Wines soft-deleted:      {stats['wines_deleted']}")
    print(f"  Vintage conflicts merged:{stats['vintages_merged']}")
    print(f"  Child rows re-pointed:   {stats['child_repoints']}")
    print(f"  External IDs re-pointed: {stats['external_id_repoints']}")
    print(f"  Staging rows re-pointed: {stats['staging_repoints']}")
    print(f"  Conflict drops:          {stats['conflict_drops']}")
    print(f"  NULL fills on survivor:  {stats['null_fills']}")
    print(f"  Errors:                  {stats['errors']}")
    if dry_run:
        print("\n  Run with --execute to apply.")

    conn.close()
    return stats


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--execute", action="store_true",
                        help="Apply changes (default is dry-run)")
    parser.add_argument("--limit", type=int, default=None,
                        help="Process at most N groups")
    args = parser.parse_args()
    run(execute=args.execute, limit=args.limit)
