"""
30K Plan Dashboard — persistent progress tracker.

Queries the DB and displays current state of the 30K plan.
Run with --watch to auto-refresh in a PowerShell window.

Usage:
    python -m pipeline.analyze.thirty_k_dashboard           # one-shot
    python -m pipeline.analyze.thirty_k_dashboard --watch    # auto-refresh every 60s
    python -m pipeline.analyze.thirty_k_dashboard --watch 30 # refresh every 30s
"""

import sys
import os
import io
import time
import argparse
import re
from datetime import datetime

# Fix Windows console encoding
if os.name == 'nt':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

# Add project root to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from pipeline.lib.db import get_conn

# ANSI
G = '\033[92m'   # green
Y = '\033[93m'   # yellow
R = '\033[91m'   # red
C = '\033[96m'   # cyan
B = '\033[1m'    # bold
D = '\033[2m'    # dim
X = '\033[0m'    # reset


def q(conn, query):
    """Run query, return rows. Rollback on error."""
    try:
        with conn.cursor() as cur:
            cur.execute(query)
            return cur.fetchall() if cur.description else []
    except Exception:
        conn.rollback()
        return []


def n(val):
    """Format number."""
    return f"{val:,}" if val is not None else "—"


def bar(cur, target, w=20):
    """Progress bar."""
    if target == 0:
        return f"{D}[{'.' * w}]{X}"
    pct = min(cur / target, 1.0)
    filled = int(pct * w)
    color = G if pct >= 0.8 else Y if pct >= 0.4 else R
    return f"{color}[{'#' * filled}{'.' * (w - filled)}]{X} {pct:.0%}"


def status_color(s):
    if 'DONE' in s or 'PASS' in s:
        return f"{G}{s}{X}"
    elif 'PROGRESS' in s or 'ACTIVE' in s:
        return f"{Y}{s}{X}"
    elif 'BLOCK' in s or 'FAIL' in s:
        return f"{R}{s}{X}"
    return f"{D}{s}{X}"


def table_exists(conn, name):
    rows = q(conn, f"""
        SELECT EXISTS (SELECT 1 FROM information_schema.tables
        WHERE table_schema='public' AND table_name='{name}')
    """)
    return rows[0][0] if rows else False


def col_exists(conn, table, col):
    rows = q(conn, f"""
        SELECT EXISTS (SELECT 1 FROM information_schema.columns
        WHERE table_schema='public' AND table_name='{table}' AND column_name='{col}')
    """)
    return rows[0][0] if rows else False


def generate(conn):
    lines = []
    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    # === Header ===
    lines.append(f"{B}{C}{'=' * 55}{X}")
    lines.append(f"{B}{C}  LOAM 30K PLAN{X}                    {D}{now}{X}")
    lines.append(f"{B}{C}{'=' * 55}{X}")
    lines.append("")

    # === Session Tracker ===
    sessions_path = os.path.join(os.path.dirname(__file__), '..', '..', 'data', 'stats', '30k_sessions.json')
    sessions = []
    if os.path.exists(sessions_path):
        try:
            import json
            with open(sessions_path) as f:
                sessions = json.load(f).get('sessions', [])
        except Exception:
            pass

    if sessions:
        lines.append(f"  {B}SESSIONS{X}")
        lines.append(f"  {'_' * 51}")
        for s in sessions:
            sid = s['id']
            name = s['name']
            st = s.get('status', 'not_started')
            date = s.get('date', '')
            spend = s.get('ai_spend')

            if st == 'done':
                icon = f"{G}[x]{X}"
                detail = f"{D}{date}{X}"
                if spend is not None:
                    detail += f" {D}(${spend:.2f}){X}"
            elif st == 'in_progress':
                icon = f"{Y}[>]{X}"
                detail = f"{Y}ACTIVE{X}"
            elif st == 'skipped':
                icon = f"{D}[-]{X}"
                detail = f"{D}skipped{X}"
            else:
                icon = f"{D}[ ]{X}"
                detail = ""

            # Truncate name to fit
            short_name = name[:36] + '..' if len(name) > 38 else name
            lines.append(f"  {icon} {sid:>2}. {short_name:<38} {detail}")
        lines.append("")

    # === Detect state ===
    archived = table_exists(conn, 'archive_wines')
    has_confirmation = col_exists(conn, 'wines', 'confirmation')
    has_provenance = table_exists(conn, 'data_provenance')
    has_suggestions = table_exists(conn, 'ai_suggestions')

    rows = q(conn, "SELECT count(*) FROM producers WHERE deleted_at IS NULL")
    prod_count = rows[0][0] if rows else 0

    rows = q(conn, "SELECT count(*) FROM wines WHERE deleted_at IS NULL")
    wine_count = rows[0][0] if rows else 0

    # === Phase Status ===
    lines.append(f"  {B}PHASE STATUS{X}")
    lines.append(f"  {'_' * 51}")

    phase0 = 'DONE' if (archived and has_provenance and has_confirmation) else ('PARTIAL' if archived else 'NOT STARTED')
    phase1 = f'{n(prod_count)} producers' if prod_count > 0 and archived else ('BLOCKED' if not archived else 'READY')
    phase2 = f'{n(wine_count)} wines' if wine_count > 0 and prod_count > 0 else ('BLOCKED' if prod_count == 0 else 'READY')

    for num, name, st in [
        ("0", "Archive & Schema", phase0),
        ("1", "Producer Canon", phase1),
        ("2", "Wine Identity", phase2),
        ("3", "Vintage & Depth", "PENDING"),
        ("4", "Enrichment", "PENDING"),
        ("5", "Josh Test", "PENDING"),
    ]:
        lines.append(f"  {B}{num}{X}  {name:<22} {status_color(st)}")
    lines.append("")

    # === Progress Bars ===
    lines.append(f"  {B}PROGRESS{X}")
    lines.append(f"  {'_' * 51}")
    lines.append(f"  Producers    {n(prod_count):>8}  {bar(prod_count, 5000)}")
    lines.append(f"  Wines        {n(wine_count):>8}  {bar(wine_count, 25000)}")

    rows = q(conn, "SELECT count(DISTINCT wine_id) FROM wine_vintages") if wine_count > 0 else []
    vint_wines = rows[0][0] if rows else 0
    lines.append(f"  w/ vintage   {n(vint_wines):>8}  {bar(vint_wines, wine_count) if wine_count > 0 else ''}")

    rows = q(conn, "SELECT count(DISTINCT wine_id) FROM wine_grapes") if wine_count > 0 else []
    grape_wines = rows[0][0] if rows else 0
    lines.append(f"  w/ grapes    {n(grape_wines):>8}  {bar(grape_wines, wine_count) if wine_count > 0 else ''}")

    rows = q(conn, """SELECT count(DISTINCT entity_id) FROM external_ids
                      WHERE entity_type='wine' AND source='upc'""") if wine_count > 0 else []
    upc_wines = rows[0][0] if rows else 0
    lines.append(f"  w/ UPC       {n(upc_wines):>8}  {bar(upc_wines, wine_count) if wine_count > 0 else ''}")
    lines.append("")

    # === Three Metrics ===
    if wine_count > 0 and has_confirmation:
        # Confirmation
        lines.append(f"  {B}CONFIRMATION{X}")
        lines.append(f"  {'_' * 51}")
        rows = q(conn, """SELECT confirmation, count(*) FROM wines
                         WHERE deleted_at IS NULL GROUP BY confirmation ORDER BY confirmation""")
        if rows:
            for grade, cnt in rows:
                g = grade or 'NULL'
                pct = cnt / wine_count * 100
                color = G if g in ('A', 'B') else Y if g == 'C' else R
                lines.append(f"  {color}{g:<6}{X} {n(cnt):>8}  ({pct:.1f}%)")
        else:
            lines.append(f"  {D}No grades yet{X}")
        lines.append("")

        # Completeness
        lines.append(f"  {B}COMPLETENESS{X}")
        lines.append(f"  {'_' * 51}")
        rows = q(conn, """SELECT
            avg(completeness)::numeric(4,1),
            count(*) FILTER (WHERE identity_complete),
            count(*) FILTER (WHERE completeness >= 6),
            count(*) FILTER (WHERE completeness >= 8)
            FROM wines WHERE deleted_at IS NULL""")
        if rows and rows[0][0] is not None:
            avg_c, id_complete, gte6, gte8 = rows[0]
            lines.append(f"  Average      {avg_c}/11")
            lines.append(f"  ID complete  {n(id_complete):>8}  ({id_complete/wine_count*100:.0f}%)")
            lines.append(f"  >= 6/11      {n(gte6):>8}  ({gte6/wine_count*100:.0f}%)")
            lines.append(f"  >= 8/11      {n(gte8):>8}  ({gte8/wine_count*100:.0f}%)")
        else:
            lines.append(f"  {D}No data yet{X}")
        lines.append("")

        # Enrichment
        lines.append(f"  {B}ENRICHMENT{X}")
        lines.append(f"  {'_' * 51}")
        rows = q(conn, """SELECT enrichment, count(*) FROM wines
                         WHERE deleted_at IS NULL GROUP BY enrichment ORDER BY enrichment""")
        if rows:
            labels = {0: 'None', 1: 'Basic (AI gap-fill)', 2: 'Full (Sonnet)'}
            for grade, cnt in rows:
                pct = cnt / wine_count * 100
                lines.append(f"  {grade}: {labels.get(grade, '?'):<22} {n(cnt):>8}  ({pct:.1f}%)")
        lines.append("")

    elif wine_count > 0:
        lines.append(f"  {D}New grading columns not yet added (Phase 0 pending){X}")
        lines.append("")
    else:
        lines.append(f"  {D}No wines yet{X}")
        lines.append("")

    # === Josh Test ===
    lines.append(f"  {B}JOSH TEST{X}")
    lines.append(f"  {'_' * 51}")
    # Check if josh test results exist
    josh_path = os.path.join(os.path.dirname(__file__), '..', '..', 'data', 'stats', 'josh_test_latest.json')
    if os.path.exists(josh_path):
        try:
            import json
            with open(josh_path) as f:
                jt = json.load(f)
            find_rate = jt.get('find_rate', 0)
            color = G if find_rate >= 0.85 else Y if find_rate >= 0.6 else R
            lines.append(f"  Find rate     {color}{find_rate:.0%}{X}  (target: 85%)")
            lines.append(f"  Avg confirm   {jt.get('avg_confirmation', '—')}")
            lines.append(f"  Avg complete  {jt.get('avg_completeness', '—')}/11")
            lines.append(f"  Avg enrich    {jt.get('avg_enrichment', '—')}")
        except Exception:
            lines.append(f"  {D}Results file exists but couldn't parse{X}")
    else:
        lines.append(f"  {D}Not yet run{X}")
    lines.append("")

    # === Budget ===
    lines.append(f"  {B}BUDGET{X}")
    lines.append(f"  {'_' * 51}")
    budget_path = os.path.join(os.path.dirname(__file__), '..', '..', 'data', 'stats', '30k_budget.json')
    spent = 0
    if os.path.exists(budget_path):
        try:
            import json
            with open(budget_path) as f:
                budget = json.load(f)
            spent = budget.get('total_spent', 0)
        except Exception:
            pass
    lines.append(f"  Spent: ${spent:.2f} / $175.00")
    lines.append(f"  {bar(spent, 175)}")
    lines.append("")

    # === Reference Tables (sanity) ===
    lines.append(f"  {B}REFERENCE TABLES{X} {D}(should be stable){X}")
    lines.append(f"  {'_' * 51}")
    for label, query in [
        ("Appellations", "SELECT count(*) FROM appellations"),
        ("App. rules", "SELECT count(*) FROM appellation_rules"),
        ("Grapes", "SELECT count(*) FROM grapes"),
        ("Regions", "SELECT count(*) FROM regions"),
        ("Countries", "SELECT count(*) FROM countries"),
    ]:
        rows = q(conn, query)
        cnt = rows[0][0] if rows else 0
        lines.append(f"  {label:<16} {n(cnt):>8}")
    lines.append("")

    # === Provenance ===
    if has_provenance:
        rows = q(conn, "SELECT count(*) FROM data_provenance")
        prov_count = rows[0][0] if rows else 0
        lines.append(f"  {B}PROVENANCE{X}")
        lines.append(f"  {'_' * 51}")
        lines.append(f"  Total entries  {n(prov_count):>8}")
        if prov_count > 0:
            rows = q(conn, """SELECT source_type, count(*) FROM data_provenance
                             GROUP BY source_type ORDER BY count(*) DESC LIMIT 5""")
            for src, cnt in (rows or []):
                lines.append(f"    {src:<16} {n(cnt):>8}")
        lines.append("")

    # === Next Action ===
    lines.append(f"  {B}{C}NEXT ACTION{X}")
    lines.append(f"  {'_' * 51}")
    if not archived:
        lines.append(f"  {Y}-> Session 1: Archive & Schema{X}")
        lines.append(f"  {D}   data/session_prompts/30k_phase_0.md{X}")
    elif prod_count == 0:
        if not has_confirmation:
            lines.append(f"  {Y}-> Session 1: Phase 0 incomplete (missing new columns){X}")
        else:
            lines.append(f"  {Y}-> Session 2: Identity Design + Josh Test Sample{X}")
    elif wine_count == 0:
        lines.append(f"  {Y}-> Session 3: Batch 0 Prototype (50 producers){X}")
    else:
        lines.append(f"  {D}  Check docs/30K_PLAN.md for current session{X}")
    lines.append("")

    lines.append(f"{B}{C}{'=' * 55}{X}")
    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(description='30K Plan Dashboard')
    parser.add_argument('--watch', nargs='?', const=60, type=int, metavar='SEC',
                        help='Auto-refresh (default 60s)')
    args = parser.parse_args()

    if args.watch:
        print(f"Watching every {args.watch}s. Ctrl+C to stop.\n")
        try:
            while True:
                conn = get_conn()
                conn.autocommit = True
                out = generate(conn)
                conn.close()
                os.system('cls' if os.name == 'nt' else 'clear')
                print(out)
                time.sleep(args.watch)
        except KeyboardInterrupt:
            print("\nStopped.")
    else:
        conn = get_conn()
        conn.autocommit = True
        out = generate(conn)
        conn.close()
        print(out)

        # Save clean markdown
        clean = re.sub(r'\033\[[0-9;]*m', '', out)
        path = os.path.join(os.path.dirname(__file__), '..', '..', 'data', 'stats', '30k_dashboard.md')
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, 'w', encoding='utf-8') as f:
            f.write(clean)


if __name__ == '__main__':
    main()
