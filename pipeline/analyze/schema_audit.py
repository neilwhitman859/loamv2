#!/usr/bin/env python3
"""Schema audit: comprehensive data quality and structural health checks.

Runs read-only checks against the database and produces a report.
Designed to run nightly (or at session start) to catch regressions.

Usage:
    python -m pipeline.analyze.schema_audit [--json] [--save]

Options:
    --json    Output raw JSON instead of formatted report
    --save    Save JSON results to data/stats/schema_audit_YYYY-MM-DD.json
"""

import json
import sys
from datetime import datetime, date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from pipeline.lib.db import get_conn


# ---------------------------------------------------------------------------
# Config: which tables/columns to audit
# ---------------------------------------------------------------------------

# Core canonical tables for deep NULL-rate analysis
CORE_TABLES = {
    "wines": [
        "name", "producer_id", "color", "wine_type", "country_id",
        "region_id", "appellation_id", "varietal_category_id",
        "identity_confidence", "data_grade", "search_vector",
    ],
    "producers": [
        "name", "country_id", "region_id", "appellation_id",
        "description", "year_established", "search_vector",
    ],
    "wine_vintages": [
        "wine_id", "vintage_year", "abv", "label_image_url",
    ],
    "wine_grapes": [
        "wine_id", "grape_id", "percentage",
    ],
    "wine_vintage_scores": [
        "wine_id", "wine_vintage_id", "publication_id", "score",
        "vintage_year", "review_date",
    ],
    "wine_vintage_prices": [
        "wine_id", "wine_vintage_id", "price_usd", "retailer_id",
        "vintage_year",
    ],
}

# Value constraint checks: (table, column, valid_values_or_range)
VALUE_CHECKS = [
    ("wines", "wine_type", {"values": ["table", "sparkling", "fortified"]}),
    ("wines", "color", {"values": ["red", "white", "rose", "orange"]}),
    ("wine_grapes", "percentage", {"range": [0, 100]}),
    ("wine_vintage_scores", "score", {"range": [0, 100]}),
    ("wine_vintages", "vintage_year", {"range": [0, 2026]}),  # 0 = NV
]

# Duplicate checks: (table, columns, description)
DUPE_CHECKS = [
    ("wines", ["name", "producer_id"], "same name + producer"),
    ("wine_grapes", ["wine_id", "grape_id"], "same wine + grape"),
    ("external_ids", ["entity_id", "system", "external_id"],
     "same entity + system + value"),
    ("wine_label_designations", ["wine_id", "label_designation_id"],
     "same wine + designation"),
]

# Polymorphic FK tables: (table, entity_type_col, entity_id_col, type->parent)
POLYMORPHIC_TABLES = [
    ("external_ids", "entity_type", "entity_id", {
        "wine": "wines",
        "producer": "producers",
        "wine_vintage": "wine_vintages",
    }),
    ("entity_classifications", "entity_type", "entity_id", {
        "wine": "wines",
        "producer": "producers",
        "vineyard": "vineyards",
    }),
]

# Whitespace checks: (table, column)
WHITESPACE_CHECKS = [
    ("wines", "name"),
    ("producers", "name"),
    ("appellations", "name"),
    ("regions", "name"),
    ("grapes", "name"),
]


# ---------------------------------------------------------------------------
# Audit functions
# ---------------------------------------------------------------------------

def audit_null_rates(cur):
    """Check NULL fill rates for key columns on core tables."""
    results = {}
    for table, columns in CORE_TABLES.items():
        cur.execute(f"SELECT count(*) FROM {table}")
        total = cur.fetchone()[0]
        if total == 0:
            results[table] = {"total": 0, "columns": {}}
            continue

        col_exprs = ", ".join(
            f"count(*) FILTER (WHERE {c} IS NULL) AS null_{c}"
            for c in columns
        )
        cur.execute(f"SELECT {col_exprs} FROM {table}")
        row = cur.fetchone()

        col_results = {}
        for i, col in enumerate(columns):
            null_count = row[i]
            filled = total - null_count
            col_results[col] = {
                "filled": filled,
                "null": null_count,
                "fill_pct": round(filled / total * 100, 1),
            }
        results[table] = {"total": total, "columns": col_results}
    return results


def audit_value_violations(cur):
    """Check for values outside allowed sets/ranges."""
    results = []
    for table, column, check in VALUE_CHECKS:
        if "values" in check:
            vals = ", ".join(f"'{v}'" for v in check["values"])
            cur.execute(f"""
                SELECT count(*) FROM {table}
                WHERE {column} IS NOT NULL
                AND {column} NOT IN ({vals})
            """)
        else:
            lo, hi = check["range"]
            cur.execute(f"""
                SELECT count(*) FROM {table}
                WHERE {column} IS NOT NULL
                AND ({column} < {lo} OR {column} > {hi})
            """)
        count = cur.fetchone()[0]
        results.append({
            "table": table,
            "column": column,
            "check": check,
            "violations": count,
        })
    return results


def audit_orphan_fks(cur):
    """Check for orphan FK references (child points to nonexistent parent).

    Only checks canonical tables (skips source_*, xwines_*).
    Skips nullable FK columns where NULL is valid (only checks non-NULL rows).
    """
    # Get all FK constraints dynamically
    cur.execute("""
        SELECT
            tc.table_name AS child_table,
            kcu.column_name AS fk_column,
            ccu.table_name AS parent_table,
            ccu.column_name AS parent_column
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu
            ON tc.constraint_name = kcu.constraint_name
            AND tc.table_schema = kcu.table_schema
        JOIN information_schema.constraint_column_usage ccu
            ON tc.constraint_name = ccu.constraint_name
            AND tc.table_schema = ccu.table_schema
        WHERE tc.constraint_type = 'FOREIGN KEY'
        AND tc.table_schema = 'public'
        AND tc.table_name NOT LIKE 'source\\_%'
        AND tc.table_name NOT LIKE 'xwines\\_%'
        ORDER BY tc.table_name, kcu.column_name
    """)
    fks = cur.fetchall()

    results = []
    for child_table, fk_column, parent_table, parent_column in fks:
        cur.execute(f"""
            SELECT count(*)
            FROM {child_table} c
            LEFT JOIN {parent_table} p ON c.{fk_column} = p.{parent_column}
            WHERE c.{fk_column} IS NOT NULL
            AND p.{parent_column} IS NULL
        """)
        orphans = cur.fetchone()[0]
        if orphans > 0:
            results.append({
                "child": f"{child_table}.{fk_column}",
                "parent": f"{parent_table}.{parent_column}",
                "orphans": orphans,
            })
    return results, len(fks)


def audit_fk_indexes(cur):
    """Find FK columns that lack an index (causes slow JOINs)."""
    cur.execute("""
        WITH fk_cols AS (
            SELECT
                tc.table_name,
                kcu.column_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
                ON tc.constraint_name = kcu.constraint_name
                AND tc.table_schema = kcu.table_schema
            WHERE tc.constraint_type = 'FOREIGN KEY'
            AND tc.table_schema = 'public'
            AND tc.table_name NOT LIKE 'source\\_%'
            AND tc.table_name NOT LIKE 'xwines\\_%'
        ),
        indexed_cols AS (
            SELECT
                t.relname AS table_name,
                a.attname AS column_name
            FROM pg_index i
            JOIN pg_class t ON t.oid = i.indrelid
            JOIN pg_attribute a ON a.attrelid = t.oid
                AND a.attnum = ANY(i.indkey)
            JOIN pg_namespace n ON n.oid = t.relnamespace
            WHERE n.nspname = 'public'
        )
        SELECT fk.table_name, fk.column_name
        FROM fk_cols fk
        LEFT JOIN indexed_cols ic
            ON fk.table_name = ic.table_name
            AND fk.column_name = ic.column_name
        WHERE ic.column_name IS NULL
        ORDER BY fk.table_name, fk.column_name
    """)
    return [{"table": r[0], "column": r[1]} for r in cur.fetchall()]


def audit_duplicates(cur):
    """Check for duplicate rows on key column combinations."""
    results = []
    for table, columns, desc in DUPE_CHECKS:
        col_list = ", ".join(columns)
        # Only count non-NULL groups
        where_clauses = " AND ".join(f"{c} IS NOT NULL" for c in columns)
        cur.execute(f"""
            SELECT count(*) FROM (
                SELECT {col_list}, count(*) AS cnt
                FROM {table}
                WHERE {where_clauses}
                GROUP BY {col_list}
                HAVING count(*) > 1
            ) sub
        """)
        count = cur.fetchone()[0]
        results.append({
            "table": table,
            "columns": columns,
            "description": desc,
            "duplicate_groups": count,
        })
    return results


def audit_polymorphic_fks(cur):
    """Check polymorphic FK integrity (entity_type + entity_id -> parent)."""
    results = []
    for table, type_col, id_col, type_map in POLYMORPHIC_TABLES:
        for entity_type, parent_table in type_map.items():
            cur.execute(f"""
                SELECT count(*)
                FROM {table} t
                LEFT JOIN {parent_table} p ON t.{id_col} = p.id
                WHERE t.{type_col} = %s
                AND p.id IS NULL
            """, (entity_type,))
            orphans = cur.fetchone()[0]
            if orphans > 0:
                results.append({
                    "table": table,
                    "entity_type": entity_type,
                    "parent_table": parent_table,
                    "orphans": orphans,
                })
    return results


def audit_whitespace(cur):
    """Check for whitespace issues in name columns."""
    results = []
    for table, column in WHITESPACE_CHECKS:
        cur.execute(f"""
            SELECT
                count(*) FILTER (WHERE {column} != trim({column}))
                    AS leading_trailing,
                count(*) FILTER (WHERE {column} LIKE '%%  %%')
                    AS double_spaces
            FROM {table}
        """)
        row = cur.fetchone()
        lt, ds = row[0], row[1]
        if lt > 0 or ds > 0:
            results.append({
                "table": table,
                "column": column,
                "leading_trailing": lt,
                "double_spaces": ds,
            })
    return results


def audit_table_sizes(cur):
    """Get table sizes (top 15 by total size)."""
    cur.execute("""
        SELECT
            c.relname AS table_name,
            pg_total_relation_size(c.oid) AS total_bytes,
            c.reltuples::bigint AS est_rows,
            pg_size_pretty(pg_total_relation_size(c.oid)) AS total_pretty
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = 'public'
        AND c.relkind = 'r'
        ORDER BY pg_total_relation_size(c.oid) DESC
        LIMIT 15
    """)
    return [{
        "table": r[0],
        "total_bytes": r[1],
        "est_rows": r[2],
        "pretty": r[3],
    } for r in cur.fetchall()]


def audit_dead_tuples(cur):
    """Check for tables with high dead tuple ratios (need VACUUM)."""
    cur.execute("""
        SELECT relname, n_live_tup, n_dead_tup,
            CASE WHEN n_live_tup > 0
                THEN round(n_dead_tup::numeric / n_live_tup * 100, 1)
                ELSE 0 END AS dead_pct
        FROM pg_stat_user_tables
        WHERE schemaname = 'public'
        AND n_dead_tup > 1000
        ORDER BY n_dead_tup DESC
        LIMIT 10
    """)
    return [{
        "table": r[0],
        "live": r[1],
        "dead": r[2],
        "dead_pct": float(r[3]),
    } for r in cur.fetchall()]


def audit_db_size(cur):
    """Total database size."""
    cur.execute("SELECT pg_size_pretty(pg_database_size(current_database()))")
    return cur.fetchone()[0]


# ---------------------------------------------------------------------------
# Report formatting
# ---------------------------------------------------------------------------

def format_report(results):
    """Format results as a human-readable report."""
    lines = []
    ts = results["timestamp"]
    lines.append(f"=== SCHEMA AUDIT REPORT -- {ts} ===\n")

    # DB size
    lines.append(f"DATABASE SIZE: {results['db_size']}\n")

    # NULL fill rates
    lines.append("--- NULL FILL RATES (key columns) ---")
    for table, data in results["null_rates"].items():
        total = data["total"]
        lines.append(f"\n  {table} ({total:,} rows)")
        for col, stats in data["columns"].items():
            fill = stats["fill_pct"]
            marker = "  " if fill >= 95 else "! " if fill < 50 else "  "
            bar = "#" * int(fill / 5) + "." * (20 - int(fill / 5))
            lines.append(
                f"  {marker}{col:<30} [{bar}] "
                f"{fill:5.1f}%  ({stats['filled']:>7,} / {total:,})"
            )
    lines.append("")

    # Value violations
    lines.append("--- VALUE VIOLATIONS ---")
    any_violations = False
    for v in results["value_violations"]:
        status = "X" if v["violations"] > 0 else "ok"
        icon = "!" if v["violations"] > 0 else " "
        lines.append(
            f" {icon} {v['table']}.{v['column']}: "
            f"{v['violations']} violations  [{status}]"
        )
        if v["violations"] > 0:
            any_violations = True
    if not any_violations:
        lines.append("  All clean.")
    lines.append("")

    # Orphan FKs
    orphans, total_fks = results["orphan_fks"], results["total_fk_count"]
    lines.append(f"--- ORPHAN FK CHECK ({total_fks} relationships) ---")
    if orphans:
        for o in orphans:
            lines.append(
                f"  ! {o['child']} -> {o['parent']}: "
                f"{o['orphans']} orphans"
            )
    else:
        lines.append("  All clean.")
    lines.append("")

    # FK indexes
    lines.append("--- FK COLUMNS WITHOUT INDEXES ---")
    missing_idx = results["fk_index_gaps"]
    if missing_idx:
        for m in missing_idx:
            lines.append(f"  ! {m['table']}.{m['column']} -- no index")
    else:
        lines.append("  All FK columns indexed.")
    lines.append("")

    # Duplicates
    lines.append("--- DUPLICATE CHECKS ---")
    for d in results["duplicates"]:
        status = "!" if d["duplicate_groups"] > 0 else " "
        lines.append(
            f"  {status} {d['table']} ({d['description']}): "
            f"{d['duplicate_groups']} duplicate groups"
        )
    lines.append("")

    # Polymorphic FK integrity
    lines.append("--- POLYMORPHIC FK INTEGRITY ---")
    poly = results["polymorphic_orphans"]
    if poly:
        for p in poly:
            lines.append(
                f"  ! {p['table']}[{p['entity_type']}] -> "
                f"{p['parent_table']}: {p['orphans']} orphans"
            )
    else:
        lines.append("  All clean.")
    lines.append("")

    # Whitespace
    lines.append("--- WHITESPACE ISSUES ---")
    ws = results["whitespace"]
    if ws:
        for w in ws:
            parts = []
            if w["leading_trailing"] > 0:
                parts.append(f"{w['leading_trailing']} leading/trailing")
            if w["double_spaces"] > 0:
                parts.append(f"{w['double_spaces']} double-space")
            lines.append(
                f"  ! {w['table']}.{w['column']}: {', '.join(parts)}"
            )
    else:
        lines.append("  All clean.")
    lines.append("")

    # Table sizes
    lines.append("--- TABLE SIZES (top 15) ---")
    for t in results["table_sizes"]:
        lines.append(
            f"  {t['table']:<40} {t['pretty']:>10}  "
            f"(~{t['est_rows']:,} rows)"
        )
    lines.append("")

    # Dead tuples
    lines.append("--- DEAD TUPLES (tables with >1K dead) ---")
    dead = results["dead_tuples"]
    if dead:
        for d in dead:
            lines.append(
                f"  {d['table']:<40} {d['dead']:>8,} dead  "
                f"({d['dead_pct']}% of live)"
            )
    else:
        lines.append("  All clean.")
    lines.append("")

    # Summary
    issues = 0
    issues += sum(1 for v in results["value_violations"] if v["violations"])
    issues += len(results["orphan_fks"])
    issues += len(results["fk_index_gaps"])
    issues += sum(1 for d in results["duplicates"] if d["duplicate_groups"])
    issues += len(results["polymorphic_orphans"])
    issues += len(results["whitespace"])
    issues += len(results["dead_tuples"])

    lines.append(f"=== SUMMARY: {issues} issue categories flagged ===")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    args = sys.argv[1:]
    output_json = "--json" in args
    save = "--save" in args

    conn = get_conn()
    try:
        with conn.cursor() as cur:
            print("Running schema audit...", file=sys.stderr)

            null_rates = audit_null_rates(cur)
            value_violations = audit_value_violations(cur)
            orphan_fks, total_fk_count = audit_orphan_fks(cur)
            fk_index_gaps = audit_fk_indexes(cur)
            duplicates = audit_duplicates(cur)
            polymorphic_orphans = audit_polymorphic_fks(cur)
            whitespace = audit_whitespace(cur)
            table_sizes = audit_table_sizes(cur)
            dead_tuples = audit_dead_tuples(cur)
            db_size = audit_db_size(cur)

        results = {
            "timestamp": datetime.now(tz=__import__('datetime').timezone.utc).isoformat(),
            "db_size": db_size,
            "null_rates": null_rates,
            "value_violations": value_violations,
            "orphan_fks": orphan_fks,
            "total_fk_count": total_fk_count,
            "fk_index_gaps": fk_index_gaps,
            "duplicates": duplicates,
            "polymorphic_orphans": polymorphic_orphans,
            "whitespace": whitespace,
            "table_sizes": table_sizes,
            "dead_tuples": dead_tuples,
        }

        if output_json:
            print(json.dumps(results, indent=2))
        else:
            print(format_report(results))

        if save:
            stats_dir = Path(__file__).resolve().parents[2] / "data" / "stats"
            stats_dir.mkdir(parents=True, exist_ok=True)
            out_path = stats_dir / f"schema_audit_{date.today()}.json"
            with open(out_path, "w") as f:
                json.dump(results, f, indent=2)
            # Also save as latest for easy diffing
            latest_path = stats_dir / "schema_audit_latest.json"
            with open(latest_path, "w") as f:
                json.dump(results, f, indent=2)
            print(f"\nSaved to {out_path}", file=sys.stderr)
            print(f"Saved to {latest_path}", file=sys.stderr)

    finally:
        conn.close()


if __name__ == "__main__":
    main()
