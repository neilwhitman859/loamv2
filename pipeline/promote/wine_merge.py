#!/usr/bin/env python3
"""
wine_merge.py — Merge Haiku-classified duplicate wine groups into single canonical records.

Strategy (redesigned for speed):
  Phase 1 — BULK: Build a dupe→survivor mapping table in Postgres, then do
             ONE UPDATE per staging table (30 tables, one JOIN each). Fast.
  Phase 2 — PER-GROUP: Handle canonical child tables with conflict logic
             (vintages, wine_grapes, external_ids, etc.). Small tables, fast.
  Phase 3 — BULK: Soft-delete all dupes + mark decisions merged in two queries.

The old approach did 30 staging UPDATEs per batch of 200 groups (~250K queries
total against source_ttb_colas). This approach does 30 total. Runtime: minutes.

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
# Config
# ---------------------------------------------------------------------------

# Staging tables with canonical_wine_id — updated in bulk via mapping table
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
]

# _tmp_wine_match uses wine_id, not canonical_wine_id
STAGING_WINE_ID_TABLES = ["_tmp_wine_match"]

# Canonical child tables: (table, wine_id_col, conflict_cols_or_None)
# conflict_cols: unique key partner columns. Dupe rows conflicting with
# survivor rows are deleted; non-conflicting rows are re-pointed.
# None = simple re-point, no conflict possible.
WINE_ID_TABLES = [
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
    ("wine_insights",                    "wine_id", ["wine_id"]),   # 1:1
    ("wine_vintage_grapes",              "wine_id", ["grape_id"]),
    ("wine_vintage_formats",             "wine_id", ["bottle_format_id"]),
    ("wine_vintage_tasting_insights",    "wine_id", None),
    ("wine_vintage_insights",            "wine_id", None),
    ("wine_vintage_vineyards",           "wine_id", None),
    ("wine_vintage_descriptors",         "wine_id", None),
    ("wine_vintage_documents",           "wine_id", None),
    ("wine_vintage_nv_components",       "wine_id", None),
    ("wine_aliases",                     "wine_id", None),
    ("wine_descriptors",                 "wine_id", None),
    ("wine_lookups",                     "wine_id", None),
    ("wine_water_bodies",                "wine_id", None),
]

WINE_RELATED_TABLES = [
    ("wine_relationships",         "related_wine_id"),
    ("wine_vintage_nv_components", "component_wine_id"),
    ("wines",                      "parent_wine_id"),
]

# Tables with wine_vintage_id FK (re-pointed when vintages are conflict-merged)
VINTAGE_CHILD_TABLES = [
    ("wine_vintage_prices",  "wine_vintage_id"),
    ("wine_vintage_scores",  "wine_vintage_id"),
    ("wine_vintage_grapes",  "wine_vintage_id"),
]

# Columns to NULL-fill on survivor from dupes
NULL_FILL_COLUMNS = [
    "appellation_id", "region_id", "country_id", "color",
    "varietal_category_id", "style", "label_image_url",
]


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------

def load_groups(cur, limit=None):
    """Load all unmerged ai_accepted groups from match_decisions."""
    q = """
        SELECT id, source_a_id, source_b_id
        FROM match_decisions
        WHERE status = 'ai_accepted'
        AND entity_type = 'wine'
        AND (notes IS NULL OR notes NOT LIKE '%%[merged]%%')
        ORDER BY created_at
    """
    if limit:
        q += f" LIMIT {int(limit)}"
    cur.execute(q)
    rows = cur.fetchall()
    groups = []
    for decision_id, survivor_id, source_b_id in rows:
        if not source_b_id:
            continue
        dupe_ids = [d.strip() for d in source_b_id.split(",") if d.strip()]
        if dupe_ids:
            groups.append((str(decision_id), str(survivor_id), dupe_ids))
    return groups


# ---------------------------------------------------------------------------
# Phase 1: Bulk staging table updates
# ---------------------------------------------------------------------------

def bulk_update_staging(cur, groups, dry_run, stats):
    """Build a mapping table and do one UPDATE per staging table.

    Creates a temp table of (dupe_id, survivor_id), then joins against each
    staging table. One query per table regardless of how many groups there are.
    """
    print("  [Phase 1] Building dupe→survivor mapping...", file=sys.stderr)

    # Collect all (dupe_id, survivor_id) pairs
    pairs = []
    for _, survivor_id, dupe_ids in groups:
        for dupe_id in dupe_ids:
            pairs.append((dupe_id, survivor_id))

    if not pairs:
        return

    print(f"  [Phase 1] {len(pairs):,} dupe→survivor pairs", file=sys.stderr)

    if not dry_run:
        # Create temp mapping table
        cur.execute("""
            CREATE TEMP TABLE _merge_map (
                dupe_id uuid NOT NULL,
                survivor_id uuid NOT NULL
            ) ON COMMIT DROP
        """)
        cur.executemany(
            "INSERT INTO _merge_map VALUES (%s::uuid, %s::uuid)",
            pairs,
        )
        cur.execute("CREATE INDEX ON _merge_map(dupe_id)")

        # Bulk update each staging table
        for table in STAGING_TABLES:
            cur.execute(f"""
                UPDATE {table} t
                SET canonical_wine_id = m.survivor_id
                FROM _merge_map m
                WHERE t.canonical_wine_id = m.dupe_id
            """)
            n = cur.rowcount
            stats["staging_repoints"] += n
            if n > 0:
                print(f"  [Phase 1] {table}: {n:,} rows updated", file=sys.stderr)

        for table in STAGING_WINE_ID_TABLES:
            cur.execute(f"""
                UPDATE {table} t
                SET wine_id = m.survivor_id
                FROM _merge_map m
                WHERE t.wine_id = m.dupe_id
            """)
            stats["staging_repoints"] += cur.rowcount

        # Also update match_decisions.canonical_wine_id
        cur.execute("""
            UPDATE match_decisions t
            SET canonical_wine_id = m.survivor_id
            FROM _merge_map m
            WHERE t.canonical_wine_id = m.dupe_id
        """)

    print(f"  [Phase 1] Staging updates complete. {stats['staging_repoints']:,} rows total.",
          file=sys.stderr)


# ---------------------------------------------------------------------------
# Phase 2: Per-group canonical child table work
# ---------------------------------------------------------------------------

def null_fill_survivor(cur, survivor_id, dupe_ids, stats):
    cols = ", ".join(NULL_FILL_COLUMNS)
    cur.execute(f"SELECT {cols} FROM wines WHERE id = %s::uuid", (survivor_id,))
    row = cur.fetchone()
    if not row:
        return
    survivor_vals = dict(zip(NULL_FILL_COLUMNS, row))
    missing = [c for c in NULL_FILL_COLUMNS if survivor_vals[c] is None]
    if not missing:
        return

    ph = ",".join(["%s::uuid"] * len(dupe_ids))
    missing_cols = ", ".join(missing)
    cur.execute(
        f"SELECT {missing_cols} FROM wines WHERE id IN ({ph}) AND deleted_at IS NULL",
        dupe_ids,
    )
    fill = {}
    for dupe_row in cur.fetchall():
        for col, val in zip(missing, dupe_row):
            if col not in fill and val is not None:
                fill[col] = val
        if len(fill) == len(missing):
            break

    if fill:
        set_clause = ", ".join(f"{c} = %s" for c in fill)
        cur.execute(
            f"UPDATE wines SET {set_clause}, updated_at = NOW() WHERE id = %s::uuid",
            list(fill.values()) + [survivor_id],
        )
        stats["null_fills"] += len(fill)


def handle_vintages(cur, survivor_id, dupe_ids, stats):
    """Merge vintages per-dupe, handling year conflicts."""
    for dupe_id in dupe_ids:
        cur.execute(
            "SELECT id, vintage_year FROM wine_vintages WHERE wine_id = %s::uuid",
            (dupe_id,),
        )
        for dupe_vid, year in cur.fetchall():
            dupe_vid = str(dupe_vid)
            cur.execute(
                "SELECT id FROM wine_vintages WHERE wine_id = %s::uuid AND vintage_year = %s",
                (survivor_id, year),
            )
            conflict = cur.fetchone()
            if conflict:
                surv_vid = str(conflict[0])
                for table, col in VINTAGE_CHILD_TABLES:
                    cur.execute(
                        f"UPDATE {table} SET {col} = %s::uuid WHERE {col} = %s::uuid",
                        (surv_vid, dupe_vid),
                    )
                    stats["vintage_child_repoints"] += cur.rowcount
                cur.execute("DELETE FROM wine_vintages WHERE id = %s::uuid", (dupe_vid,))
                stats["vintages_merged"] += 1
            else:
                cur.execute(
                    "UPDATE wine_vintages SET wine_id = %s::uuid WHERE id = %s::uuid",
                    (survivor_id, dupe_vid),
                )
                stats["child_repoints"] += cur.rowcount


def repoint_wine_id_tables(cur, survivor_id, dupe_ids, stats):
    ph = ",".join(["%s::uuid"] * len(dupe_ids))
    all_ids = [survivor_id] + dupe_ids
    ph_all = ",".join(["%s::uuid"] * len(all_ids))
    for table, col, conflict_cols in WINE_ID_TABLES:
        if conflict_cols:
            if conflict_cols == ["wine_id"]:
                cur.execute(f"SELECT 1 FROM {table} WHERE {col} = %s::uuid", (survivor_id,))
                if cur.fetchone():
                    cur.execute(f"DELETE FROM {table} WHERE {col} IN ({ph})", dupe_ids)
                    continue
            else:
                # Delete ANY dupe row whose conflict key appears more than once across
                # survivor+all dupes combined. This handles:
                #   - dupe conflicts with survivor (survivor count ≥ 1, so total > 1)
                #   - cross-dupe conflicts (two dupes share the same key)
                cc = ", ".join(conflict_cols)
                cur.execute(f"""
                    DELETE FROM {table} t
                    WHERE t.{col} IN ({ph})
                    AND ({cc}) IN (
                        SELECT {cc} FROM {table}
                        WHERE {col} IN ({ph_all})
                        GROUP BY {cc}
                        HAVING COUNT(*) > 1
                    )
                """, dupe_ids + all_ids)
                stats["conflict_drops"] += cur.rowcount

        cur.execute(
            f"UPDATE {table} SET {col} = %s::uuid WHERE {col} IN ({ph})",
            [survivor_id] + dupe_ids,
        )
        stats["child_repoints"] += cur.rowcount


def repoint_related_wine_columns(cur, survivor_id, dupe_ids, stats):
    ph = ",".join(["%s::uuid"] * len(dupe_ids))
    for table, col in WINE_RELATED_TABLES:
        cur.execute(
            f"UPDATE {table} SET {col} = %s::uuid WHERE {col} IN ({ph})",
            [survivor_id] + dupe_ids,
        )
        stats["child_repoints"] += cur.rowcount


def repoint_external_ids(cur, survivor_id, dupe_ids, stats):
    ph = ",".join(["%s::uuid"] * len(dupe_ids))
    all_ids = [survivor_id] + dupe_ids
    ph_all = ",".join(["%s::uuid"] * len(all_ids))
    # Delete dupe rows whose (system, external_id) appears more than once across
    # survivor+all dupes. Catches conflicts with survivor AND cross-dupe conflicts.
    cur.execute(f"""
        DELETE FROM external_ids
        WHERE entity_type = 'wine'
        AND entity_id IN ({ph})
        AND (system, external_id) IN (
            SELECT system, external_id FROM external_ids
            WHERE entity_type = 'wine' AND entity_id IN ({ph_all})
            GROUP BY system, external_id
            HAVING COUNT(*) > 1
        )
    """, dupe_ids + all_ids)
    stats["conflict_drops"] += cur.rowcount
    # Update remaining dupe rows (no conflicts remain)
    cur.execute(f"""
        UPDATE external_ids SET entity_id = %s::uuid
        WHERE entity_type = 'wine' AND entity_id IN ({ph})
    """, [survivor_id] + dupe_ids)
    stats["external_id_repoints"] += cur.rowcount


def repoint_vintage_prices_scores(cur, survivor_id, dupe_ids, stats):
    ph = ",".join(["%s::uuid"] * len(dupe_ids))
    for table in ("wine_vintage_prices", "wine_vintage_scores"):
        cur.execute(
            f"UPDATE {table} SET wine_id = %s::uuid WHERE wine_id IN ({ph})",
            [survivor_id] + dupe_ids,
        )
        stats["child_repoints"] += cur.rowcount


# ---------------------------------------------------------------------------
# Phase 3: Bulk soft-delete + mark merged
# ---------------------------------------------------------------------------

def bulk_finalize(cur, groups, dry_run, stats):
    """Soft-delete all dupes and mark all decisions merged in two queries."""
    print("  [Phase 3] Soft-deleting dupes and marking decisions merged...",
          file=sys.stderr)

    all_dupes = []       # (dupe_id, survivor_id)
    decision_ids = []
    failed = set(stats.get("error_groups", []))

    for decision_id, survivor_id, dupe_ids in groups:
        if decision_id in failed:
            continue  # skip groups whose child migration failed
        for dupe_id in dupe_ids:
            all_dupes.append((dupe_id, survivor_id))
        decision_ids.append(decision_id)

    if not dry_run:
        # Bulk soft-delete with duplicate_of set
        cur.execute("""
            CREATE TEMP TABLE _delete_map (
                dupe_id uuid NOT NULL,
                survivor_id uuid NOT NULL
            ) ON COMMIT DROP
        """)
        cur.executemany(
            "INSERT INTO _delete_map VALUES (%s::uuid, %s::uuid)",
            all_dupes,
        )
        cur.execute("""
            UPDATE wines w
            SET deleted_at = NOW(),
                duplicate_of = m.survivor_id,
                updated_at = NOW()
            FROM _delete_map m
            WHERE w.id = m.dupe_id
            AND w.deleted_at IS NULL
        """)
        stats["wines_deleted"] = cur.rowcount

        # Bulk mark decisions as merged
        ph = ",".join(["%s::uuid"] * len(decision_ids))
        cur.execute(
            f"UPDATE match_decisions SET notes = COALESCE(notes,'') || ' [merged]', "
            f"updated_at = NOW() WHERE id IN ({ph})",
            decision_ids,
        )

    print(f"  [Phase 3] {stats['wines_deleted']:,} wines soft-deleted.", file=sys.stderr)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def run(execute=False, limit=None):
    dry_run = not execute
    mode = "DRY RUN" if dry_run else "EXECUTE"

    conn = get_conn()
    cur = conn.cursor()

    print(f"[wine_merge] {mode} — loading unmerged groups...", file=sys.stderr)
    groups = load_groups(cur, limit=limit)
    total = len(groups)
    total_dupes = sum(len(d) for _, _, d in groups)
    print(f"  {total:,} groups to merge ({total_dupes:,} dupe wines total)", file=sys.stderr)

    if total == 0:
        print("  Nothing to do.")
        conn.close()
        return

    if dry_run:
        dupe_counts = [len(d) for _, _, d in groups]
        print(f"\n  Dry-run summary:")
        print(f"    Groups:          {total:,}")
        print(f"    Wines to delete: {sum(dupe_counts):,}")
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
        return

    stats = {
        "wines_deleted": 0,
        "vintages_merged": 0,
        "vintage_child_repoints": 0,
        "child_repoints": 0,
        "external_id_repoints": 0,
        "staging_repoints": 0,
        "conflict_drops": 0,
        "null_fills": 0,
        "errors": 0,
        "error_groups": [],
    }

    # -------------------------------------------------------------------------
    # Phase 1: Bulk staging updates (one query per table)
    # -------------------------------------------------------------------------
    bulk_update_staging(cur, groups, dry_run, stats)
    conn.commit()
    print("  [Phase 1] Committed.", file=sys.stderr)

    # -------------------------------------------------------------------------
    # Phase 2: Per-group canonical child table work
    # -------------------------------------------------------------------------
    print(f"  [Phase 2] Processing {total:,} groups for canonical child tables...",
          file=sys.stderr)

    BATCH = 500
    for i in range(0, total, BATCH):
        batch = groups[i:i + BATCH]
        for decision_id, survivor_id, dupe_ids in batch:
            try:
                cur.execute("SAVEPOINT sp_group")
                null_fill_survivor(cur, survivor_id, dupe_ids, stats)
                handle_vintages(cur, survivor_id, dupe_ids, stats)
                repoint_wine_id_tables(cur, survivor_id, dupe_ids, stats)
                repoint_related_wine_columns(cur, survivor_id, dupe_ids, stats)
                repoint_external_ids(cur, survivor_id, dupe_ids, stats)
                repoint_vintage_prices_scores(cur, survivor_id, dupe_ids, stats)
                cur.execute("RELEASE SAVEPOINT sp_group")
            except Exception as e:
                cur.execute("ROLLBACK TO SAVEPOINT sp_group")
                stats["errors"] += 1
                stats["error_groups"].append(decision_id)
                print(f"  ERROR group {decision_id}: {e}", file=sys.stderr)
                continue

        conn.commit()
        pct = (i + len(batch)) / total * 100
        print(
            f"  [Phase 2] [{pct:5.1f}%] {i+len(batch):,}/{total:,} groups | "
            f"vintages_merged={stats['vintages_merged']} "
            f"ext_ids={stats['external_id_repoints']} errors={stats['errors']}",
            file=sys.stderr,
        )

    # -------------------------------------------------------------------------
    # Phase 3: Bulk soft-delete + mark merged
    # -------------------------------------------------------------------------
    bulk_finalize(cur, groups, dry_run, stats)
    conn.commit()

    print(f"\n[wine_merge] COMPLETE")
    print(f"  Groups merged:             {total:,}")
    print(f"  Wines soft-deleted:        {stats['wines_deleted']:,}")
    print(f"  Vintage conflicts merged:  {stats['vintages_merged']:,}")
    print(f"  Child rows re-pointed:     {stats['child_repoints']:,}")
    print(f"  External IDs re-pointed:   {stats['external_id_repoints']:,}")
    print(f"  Staging rows re-pointed:   {stats['staging_repoints']:,}")
    print(f"  Conflict drops:            {stats['conflict_drops']:,}")
    print(f"  NULL fills on survivor:    {stats['null_fills']:,}")
    print(f"  Errors:                    {stats['errors']:,}")
    if stats["error_groups"]:
        print(f"  Failed group IDs:          {stats['error_groups'][:20]}")
        if len(stats["error_groups"]) > 20:
            print(f"    ... and {len(stats['error_groups']) - 20} more")

    conn.close()
    return stats


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--execute", action="store_true")
    parser.add_argument("--limit", type=int, default=None)
    args = parser.parse_args()
    run(execute=args.execute, limit=args.limit)
