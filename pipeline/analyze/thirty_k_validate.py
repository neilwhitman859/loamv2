"""
30K Plan Validation Script — run after every session.

Usage:
    python -m pipeline.analyze.thirty_k_validate --session phase_0
    python -m pipeline.analyze.thirty_k_validate --session batch_0
"""

import sys
import os
import argparse
from datetime import datetime

if os.name == 'nt':
    try:
        os.system('chcp 65001 >nul 2>&1')
        if hasattr(sys.stdout, 'reconfigure'):
            sys.stdout.reconfigure(encoding='utf-8', errors='replace')
    except Exception:
        pass

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from pipeline.lib.db import get_conn

G = '\033[92m'
R = '\033[91m'
Y = '\033[93m'
B = '\033[1m'
D = '\033[2m'
X = '\033[0m'

PASS = f"{G}PASS{X}"
FAIL = f"{R}FAIL{X}"
SKIP = f"{D}SKIP{X}"
WARN = f"{Y}WARN{X}"


def q(conn, query, params=None):
    try:
        with conn.cursor() as cur:
            cur.execute(query, params)
            return cur.fetchall() if cur.description else []
    except Exception as e:
        conn.rollback()
        return None  # None = query error


def table_exists(conn, name):
    rows = q(conn, """SELECT EXISTS (SELECT 1 FROM information_schema.tables
                      WHERE table_schema='public' AND table_name=%s)""", (name,))
    return rows[0][0] if rows else False


def col_exists(conn, table, col):
    rows = q(conn, """SELECT EXISTS (SELECT 1 FROM information_schema.columns
                      WHERE table_schema='public' AND table_name=%s AND column_name=%s)""",
             (table, col))
    return rows[0][0] if rows else False


def count(conn, table, where=None):
    if not table_exists(conn, table):
        return None
    sql = f"SELECT count(*) FROM {table}"
    if where:
        sql += f" WHERE {where}"
    rows = q(conn, sql)
    return rows[0][0] if rows else None


def view_exists(conn, name):
    rows = q(conn, """SELECT EXISTS (SELECT 1 FROM information_schema.views
                      WHERE table_schema='public' AND table_name=%s)""", (name,))
    return rows[0][0] if rows else False


def rpc_exists(conn, name):
    rows = q(conn, """SELECT EXISTS (SELECT 1 FROM information_schema.routines
                      WHERE routine_schema='public' AND routine_name=%s)""", (name,))
    return rows[0][0] if rows else False


def run_check(checks, label, ok, detail=None, warn=False):
    """Record a check result."""
    status = PASS if ok else (WARN if warn else FAIL)
    checks.append((label, ok, warn, detail))
    icon = G + 'v' + X if ok else (Y + '!' + X if warn else R + 'X' + X)
    d = f"  {D}{detail}{X}" if detail else ""
    print(f"  [{icon}] {label}{d}")
    return ok


# ─────────────────────────────────────────────────────────────
# Phase 0 checks: Archive & Schema
# ─────────────────────────────────────────────────────────────

PHASE_0_NEW_COLS = [
    'display_name', 'confirmation', 'completeness', 'enrichment',
    'identity_complete', 'blend_complete', 'appellation_confirmed',
    'grapes_confirmed', 'color_confirmed',
]

PHASE_0_NEW_TABLES = [
    'wines', 'producers', 'wine_vintages', 'wine_grapes', 'external_ids',
    'wine_vintage_prices', 'wine_vintage_scores', 'wine_vintage_formats',
    'wine_label_designations', 'wine_farming_certifications',
    'entity_classifications', 'wine_insights', 'wine_vintage_tasting_insights',
    'wine_lookups', 'wine_relationships', 'wine_food_pairings', 'wine_aliases',
    'wine_soils', 'wine_regions', 'wine_water_bodies',
    'wine_biodiversity_certifications', 'wine_descriptors', 'wine_appellations',
    'wine_vineyards', 'wine_vintage_descriptors', 'wine_vintage_documents',
    'wine_vintage_nv_components', 'wine_vintage_vineyards', 'wine_vintage_grapes',
    'wine_vintage_insights',
    'producer_farming_certifications', 'producer_winemakers', 'producer_aliases',
    'producer_biodiversity_certifications', 'producer_documents',
    'producer_importers', 'producer_insights', 'producer_regions',
    'producer_timeline', 'vineyard_producers', 'vineyard_soils',
    'winemakers', 'vineyards', 'enrichment_log', 'match_decisions',
    'data_provenance', 'ai_suggestions',
]

REFERENCE_TABLES = [
    ('appellations', 3000),
    ('grapes', 9000),
    ('regions', 300),
    ('countries', 60),
    ('farming_certifications', 15),
    ('biodiversity_certifications', 5),
    ('label_designations', 50),
    ('appellation_rules', 500),
]

STAGING_TABLES = [
    'source_ttb_colas', 'source_lwin', 'source_berliner', 'source_texsom',
    'source_wallys', 'source_skurnik', 'source_kermit_lynch', 'source_empson',
    'source_flatiron', 'source_bc_liquor', 'source_specs',
]


def validate_phase_0(conn):
    checks = []
    passed = 0
    total = 0

    print(f"\n{B}Phase 0: Archive & Schema{X}")
    print("─" * 55)

    # S1.1 archive_wines exists with expected row count
    cnt = count(conn, 'archive_wines')
    ok = cnt is not None and cnt > 500000
    run_check(checks, f"S1.1  archive_wines {cnt:,} rows" if cnt else "S1.1  archive_wines",
              ok, detail=None if ok else "expected >500K")
    total += 1; passed += (1 if ok else 0)

    # S1.2 archive_producers
    cnt = count(conn, 'archive_producers')
    ok = cnt is not None and cnt > 40000
    run_check(checks, f"S1.2  archive_producers {cnt:,} rows" if cnt else "S1.2  archive_producers",
              ok, detail=None if ok else "expected >40K")
    total += 1; passed += (1 if ok else 0)

    # S1.3 wines table exists, 0 rows
    cnt = count(conn, 'wines')
    ok = cnt is not None and cnt == 0
    run_check(checks, f"S1.3  wines table 0 rows (actual: {cnt})" if cnt is not None else "S1.3  wines table",
              ok)
    total += 1; passed += (1 if ok else 0)

    # S1.4 producers table exists, 0 rows
    cnt = count(conn, 'producers')
    ok = cnt is not None and cnt == 0
    run_check(checks, f"S1.4  producers table 0 rows (actual: {cnt})" if cnt is not None else "S1.4  producers table",
              ok)
    total += 1; passed += (1 if ok else 0)

    # S1.5 data_provenance
    cnt = count(conn, 'data_provenance')
    ok = cnt is not None and cnt == 0
    run_check(checks, f"S1.5  data_provenance 0 rows", ok)
    total += 1; passed += (1 if ok else 0)

    # S1.6 ai_suggestions
    cnt = count(conn, 'ai_suggestions')
    ok = cnt is not None and cnt == 0
    run_check(checks, f"S1.6  ai_suggestions 0 rows", ok)
    total += 1; passed += (1 if ok else 0)

    # S1.7 All new 30K columns on wines
    missing = [c for c in PHASE_0_NEW_COLS if not col_exists(conn, 'wines', c)]
    ok = len(missing) == 0
    run_check(checks, f"S1.7  New 30K columns on wines",
              ok, detail=f"missing: {missing}" if missing else None)
    total += 1; passed += (1 if ok else 0)

    # S1.8 All new tables exist
    missing_tables = [t for t in PHASE_0_NEW_TABLES if not table_exists(conn, t)]
    ok = len(missing_tables) == 0
    run_check(checks, f"S1.8  All fresh canonical tables exist",
              ok, detail=f"missing: {missing_tables[:5]}" if missing_tables else None)
    total += 1; passed += (1 if ok else 0)

    # S1.9 search_catalog RPC works (returns empty, not error)
    rows = q(conn, "SELECT * FROM search_catalog('Opus One', 3, ARRAY['wine','producer'])")
    ok = rows is not None  # None = error; [] = empty result = OK
    run_check(checks, f"S1.9  search_catalog RPC works ({len(rows) if rows else 'ERROR'} results)",
              ok)
    total += 1; passed += (1 if ok else 0)

    # S1.10 wine_detail_view works
    rows = q(conn, "SELECT count(*) FROM wine_detail_view")
    ok = rows is not None
    run_check(checks, f"S1.10 wine_detail_view accessible", ok)
    total += 1; passed += (1 if ok else 0)

    # S1.11 producer_detail_view works
    ok = view_exists(conn, 'producer_detail_view')
    run_check(checks, f"S1.11 producer_detail_view exists", ok)
    total += 1; passed += (1 if ok else 0)

    # S1.12 wine_search_view works
    ok = view_exists(conn, 'wine_search_view')
    run_check(checks, f"S1.12 wine_search_view exists", ok)
    total += 1; passed += (1 if ok else 0)

    # S1.13 Reference tables have expected minimums
    print(f"\n  {D}Reference table sanity:{X}")
    ref_ok = True
    for tbl, minimum in REFERENCE_TABLES:
        cnt = count(conn, tbl)
        ok = cnt is not None and cnt >= minimum
        if not ok:
            ref_ok = False
        icon = G + 'v' + X if ok else R + 'X' + X
        print(f"    [{icon}] {tbl:<32} {cnt:,}" if cnt else f"    [X] {tbl} MISSING")
    run_check(checks, f"S1.13 Reference tables intact", ref_ok)
    total += 1; passed += (1 if ref_ok else 0)

    # S1.14 Staging tables still exist
    missing_staging = [t for t in STAGING_TABLES if not table_exists(conn, t)]
    ok = len(missing_staging) == 0
    run_check(checks, f"S1.14 Staging tables untouched",
              ok, detail=f"missing: {missing_staging}" if missing_staging else None)
    total += 1; passed += (1 if ok else 0)

    # S1.15 RLS enabled on fresh canonical tables
    rows = q(conn, """SELECT count(*) FROM pg_tables
                      WHERE schemaname='public' AND rowsecurity=true
                      AND tablename IN ('wines','producers','wine_vintages',
                                        'external_ids','wine_grapes','data_provenance')""")
    rls_count = rows[0][0] if rows else 0
    ok = rls_count == 6
    run_check(checks, f"S1.15 RLS enabled on canonical tables ({rls_count}/6)",
              ok)
    total += 1; passed += (1 if ok else 0)

    return passed, total


# ─────────────────────────────────────────────────────────────
# Generic ongoing validation (run every session)
# ─────────────────────────────────────────────────────────────

def validate_ongoing(conn):
    """Checks that should pass at every session."""
    checks = []
    passed = 0
    total = 0

    print(f"\n{B}Ongoing integrity checks{X}")
    print("─" * 55)

    # No orphaned wines (producer_id FK valid)
    rows = q(conn, """SELECT count(*) FROM wines w
                      WHERE w.deleted_at IS NULL
                      AND NOT EXISTS (SELECT 1 FROM producers p WHERE p.id = w.producer_id)""")
    cnt = rows[0][0] if rows else None
    ok = cnt is not None and cnt == 0
    run_check(checks, f"Orphan wines (no producer): {cnt}", ok)
    total += 1; passed += (1 if ok else 0)

    # No wines with invalid confirmation
    rows = q(conn, """SELECT count(*) FROM wines
                      WHERE confirmation IS NOT NULL
                      AND confirmation NOT IN ('D','C','B','A')""")
    cnt = rows[0][0] if rows else None
    ok = cnt is not None and cnt == 0
    run_check(checks, f"Invalid confirmation values: {cnt}", ok)
    total += 1; passed += (1 if ok else 0)

    # Completeness in valid range
    rows = q(conn, """SELECT count(*) FROM wines
                      WHERE completeness NOT BETWEEN 0 AND 11""")
    cnt = rows[0][0] if rows else None
    ok = cnt is not None and cnt == 0
    run_check(checks, f"Completeness out of range [0-11]: {cnt}", ok)
    total += 1; passed += (1 if ok else 0)

    # No duplicate slugs in wines
    rows = q(conn, """SELECT count(*) FROM (
        SELECT slug FROM wines WHERE deleted_at IS NULL GROUP BY slug HAVING count(*) > 1
    ) x""")
    cnt = rows[0][0] if rows else None
    ok = cnt is not None and cnt == 0
    run_check(checks, f"Duplicate wine slugs: {cnt}", ok)
    total += 1; passed += (1 if ok else 0)

    # No duplicate slugs in producers
    rows = q(conn, """SELECT count(*) FROM (
        SELECT slug FROM producers WHERE deleted_at IS NULL GROUP BY slug HAVING count(*) > 1
    ) x""")
    cnt = rows[0][0] if rows else None
    ok = cnt is not None and cnt == 0
    run_check(checks, f"Duplicate producer slugs: {cnt}", ok)
    total += 1; passed += (1 if ok else 0)

    # wine_grapes percentages valid
    rows = q(conn, """SELECT count(*) FROM wine_grapes
                      WHERE percentage IS NOT NULL AND percentage NOT BETWEEN 0 AND 100""")
    cnt = rows[0][0] if rows else None
    ok = cnt is not None and cnt == 0
    run_check(checks, f"Invalid grape percentages: {cnt}", ok)
    total += 1; passed += (1 if ok else 0)

    return passed, total


# ─────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────

SESSION_VALIDATORS = {
    'phase_0': validate_phase_0,
}


def main():
    parser = argparse.ArgumentParser(description='30K Plan Validation')
    parser.add_argument('--session', required=True,
                        help='Session to validate (e.g. phase_0, batch_0)')
    args = parser.parse_args()

    session = args.session.lower().replace('-', '_')
    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    print(f"\n{B}{'=' * 55}{X}")
    print(f"{B}  30K VALIDATION  —  session: {session}{X}")
    print(f"{B}  {now}{X}")
    print(f"{B}{'=' * 55}{X}")

    conn = get_conn()
    conn.autocommit = True

    total_pass = 0
    total_checks = 0

    # Session-specific checks
    validator = SESSION_VALIDATORS.get(session)
    if validator:
        p, t = validator(conn)
        total_pass += p
        total_checks += t
    else:
        print(f"\n{Y}No session-specific checks for '{session}' — running ongoing only{X}")

    # Ongoing checks
    p, t = validate_ongoing(conn)
    total_pass += p
    total_checks += t

    conn.close()

    # Summary
    print(f"\n{'─' * 55}")
    pct = total_pass / total_checks * 100 if total_checks else 0
    color = G if pct == 100 else Y if pct >= 80 else R
    print(f"  {B}Result: {color}{total_pass}/{total_checks} checks passed ({pct:.0f}%){X}")

    if total_pass == total_checks:
        print(f"  {G}All checks passed. Session {session} validated.{X}")
        sys.exit(0)
    else:
        failures = total_checks - total_pass
        print(f"  {R}{failures} check(s) failed. Review above.{X}")
        sys.exit(1)


if __name__ == '__main__':
    main()
