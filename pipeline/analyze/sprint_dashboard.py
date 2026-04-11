"""
Sprint Dashboard — multi-sprint aware progress tracker.

Reads `data/sprints/current.json` to find the active sprint, then loads that sprint's
meta.json, sessions.json, budget.json, status.md, journal.md. Renders an above-the-fold
split-screen view: sprint name + current session + blocker, live KPIs + budget, then
below-fold session list, top BACKLOG P0/P1, next action.

Usage:
    python -m pipeline.analyze.sprint_dashboard                 # active sprint, one-shot
    python -m pipeline.analyze.sprint_dashboard --watch         # auto-refresh 60s
    python -m pipeline.analyze.sprint_dashboard --watch 30      # auto-refresh 30s
    python -m pipeline.analyze.sprint_dashboard --sprint 30k    # archived sprint view
    python -m pipeline.analyze.sprint_dashboard --save          # also write to sprint_dashboard.md
"""

import sys
import os
import re
import json
import time
import argparse
from datetime import datetime
from pathlib import Path

# Fix Windows console encoding
if os.name == "nt":
    try:
        os.system("chcp 65001 >nul 2>&1")
        if hasattr(sys.stdout, "reconfigure"):
            sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

# Add project root to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

# ANSI
G = "\033[92m"
Y = "\033[93m"
R = "\033[91m"
C = "\033[96m"
M = "\033[95m"
B = "\033[1m"
D = "\033[2m"
X = "\033[0m"

SPRINTS_DIR = Path("data/sprints")
BACKLOG_PATH = Path("docs/BACKLOG.md")


def q(conn, sql):
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
            return cur.fetchall() if cur.description else []
    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass
        return []


def n(val):
    return f"{val:,}" if val is not None else "—"


def bar(cur, target, w=20):
    if not target:
        return f"{D}[{'.' * w}]{X}"
    pct = min(cur / target, 1.0)
    filled = int(pct * w)
    color = G if pct >= 0.8 else Y if pct >= 0.4 else R
    return f"{color}[{'#' * filled}{'.' * (w - filled)}]{X} {pct:.0%}"


def strip_ansi(s: str) -> str:
    return re.sub(r"\033\[[0-9;]*m", "", s)


def resolve_sprint_dir(name_arg: str | None) -> tuple[Path | None, dict, bool]:
    """
    Return (sprint_dir_or_None, current_ptr, is_archive).

    - If name_arg is None, reads current.json and returns the active sprint.
    - If name_arg is supplied, looks first in data/sprints/<name>/ then data/sprints/_archive/<name>/.
    - If no valid sprint is found, returns None for sprint_dir.
    """
    current_path = SPRINTS_DIR / "current.json"
    current = {}
    if current_path.exists():
        try:
            current = json.loads(current_path.read_text(encoding="utf-8"))
        except Exception:
            current = {}

    if name_arg:
        live = SPRINTS_DIR / name_arg
        archived = SPRINTS_DIR / "_archive" / name_arg
        if live.exists() and live.is_dir():
            return live, current, False
        if archived.exists() and archived.is_dir():
            return archived, current, True
        return None, current, False

    name = current.get("name")
    if not name:
        return None, current, False
    live = SPRINTS_DIR / name
    if live.exists():
        return live, current, False
    archived = SPRINTS_DIR / "_archive" / name
    if archived.exists():
        return archived, current, True
    return None, current, False


def load_sprint(sprint_dir: Path | None) -> dict:
    """Load all sprint files into a dict. Missing files are skipped silently."""
    bundle = {
        "meta": {},
        "sessions": [],
        "budget": {},
        "status_md": "",
        "journal_md": "",
    }
    if sprint_dir is None or not sprint_dir.exists():
        return bundle

    meta_path = sprint_dir / "meta.json"
    if meta_path.exists():
        try:
            bundle["meta"] = json.loads(meta_path.read_text(encoding="utf-8"))
        except Exception:
            pass

    sessions_path = sprint_dir / "sessions.json"
    if sessions_path.exists():
        try:
            data = json.loads(sessions_path.read_text(encoding="utf-8"))
            bundle["sessions"] = data.get("sessions", [])
        except Exception:
            pass

    budget_path = sprint_dir / "budget.json"
    if budget_path.exists():
        try:
            bundle["budget"] = json.loads(budget_path.read_text(encoding="utf-8"))
        except Exception:
            pass

    status_path = sprint_dir / "status.md"
    if status_path.exists():
        bundle["status_md"] = status_path.read_text(encoding="utf-8")

    journal_path = sprint_dir / "journal.md"
    if journal_path.exists():
        bundle["journal_md"] = journal_path.read_text(encoding="utf-8")

    return bundle


def fetch_kpis(conn) -> dict:
    """Pull live DB KPIs. Works for any sprint — caller decides what to display."""
    kpis = {}
    if conn is None:
        return kpis

    rows = q(conn, "SELECT count(*) FROM wines WHERE deleted_at IS NULL")
    kpis["wines_active"] = rows[0][0] if rows else 0
    rows = q(conn, "SELECT count(*) FROM producers WHERE deleted_at IS NULL")
    kpis["producers_active"] = rows[0][0] if rows else 0
    rows = q(conn, "SELECT count(*) FROM wine_vintages")
    kpis["wine_vintages"] = rows[0][0] if rows else 0
    rows = q(conn, "SELECT count(*) FROM wine_grapes")
    kpis["wine_grapes"] = rows[0][0] if rows else 0
    rows = q(conn, "SELECT count(*) FROM wine_insights")
    kpis["wine_insights"] = rows[0][0] if rows else 0
    rows = q(conn, "SELECT count(*) FROM data_provenance")
    kpis["data_provenance"] = rows[0][0] if rows else 0

    rows = q(conn, """
        SELECT data_grade, count(*) FROM wines
        WHERE deleted_at IS NULL GROUP BY data_grade ORDER BY data_grade
    """)
    kpis["grade_dist"] = {r[0] or "?": r[1] for r in rows}

    rows = q(conn, """
        SELECT system, count(*) FROM external_ids
        GROUP BY system ORDER BY count(*) DESC LIMIT 10
    """)
    kpis["external_ids"] = {r[0]: r[1] for r in rows}

    return kpis


def fetch_reference_layer(conn) -> dict:
    """Optional reference-layer enrichment panel (hidden until a Reference-First sprint enables it)."""
    ref = {}
    if conn is None:
        return ref
    rows = q(conn, "SELECT count(*) FROM grapes")
    ref["grapes_total"] = rows[0][0] if rows else 0
    rows = q(conn, "SELECT count(*) FROM regions")
    ref["regions_total"] = rows[0][0] if rows else 0
    rows = q(conn, "SELECT count(*) FROM appellations")
    ref["appellations_total"] = rows[0][0] if rows else 0
    rows = q(conn, "SELECT count(*) FROM appellation_rules")
    ref["appellation_rules"] = rows[0][0] if rows else 0
    rows = q(conn, "SELECT count(*) FROM appellation_grapes")
    ref["appellation_grapes"] = rows[0][0] if rows else 0
    return ref


def parse_backlog_top(path: Path, limit: int = 3) -> list[str]:
    """Return up to `limit` top P0/P1 entries from BACKLOG.md as short lines."""
    if not path.exists():
        return []
    try:
        text = path.read_text(encoding="utf-8")
    except Exception:
        return []
    lines = text.splitlines()
    entries = []
    current_title = None
    current_priority = None
    for line in lines:
        m = re.match(r"^### \[(\d{4}-\d{2}-\d{2})\] (.+)$", line)
        if m:
            if current_title and current_priority in ("P0", "P1"):
                entries.append((current_priority, current_title))
            current_title = m.group(2).strip()
            current_priority = None
            continue
        m = re.match(r"^\*\*Priority:\*\*\s*(P[0-3])", line)
        if m:
            current_priority = m.group(1)
    if current_title and current_priority in ("P0", "P1"):
        entries.append((current_priority, current_title))
    entries.sort(key=lambda e: (e[0], e[1]))
    out = []
    for pr, title in entries[:limit]:
        out.append(f"[{pr}] {title[:70]}")
    return out


def parse_next_action(status_md: str) -> str:
    """Extract 'Next Session's Goal' section from status.md, first paragraph."""
    if not status_md:
        return ""
    m = re.search(r"## Next Session['\u2019]s Goal\s*\n(.+?)(?:\n##|\Z)", status_md, re.DOTALL)
    if not m:
        return ""
    block = m.group(1).strip()
    lines = [ln for ln in block.splitlines() if ln.strip()]
    return "\n".join(lines[:3])


def session_status_icon(st: str) -> str:
    return {
        "done": f"{G}[x]{X}",
        "in_progress": f"{Y}[>]{X}",
        "skipped": f"{D}[-]{X}",
        "not_started": f"{D}[ ]{X}",
    }.get(st, f"{D}[ ]{X}")


def generate(conn, sprint_name_arg: str | None) -> str:
    lines = []
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    sprint_dir, current_ptr, is_archive = resolve_sprint_dir(sprint_name_arg)
    sprint_missing = sprint_dir is None or not sprint_dir.exists()

    # === Header (above-the-fold) ===
    lines.append(f"{B}{C}{'=' * 65}{X}")
    lines.append(f"{B}{C}  SPRINT DASHBOARD{X}              {D}{now}{X}")
    lines.append(f"{B}{C}{'=' * 65}{X}")

    if sprint_missing:
        lines.append("")
        if sprint_name_arg:
            lines.append(f"  {R}Sprint '{sprint_name_arg}' not found.{X}")
            lines.append(f"  {D}Looked in data/sprints/{sprint_name_arg}/ and data/sprints/_archive/{sprint_name_arg}/.{X}")
        else:
            lines.append(f"  {R}No active sprint.{X}")
            lines.append(f"  {D}`data/sprints/current.json` is missing, empty, or points at a sprint that doesn't exist.{X}")
            lines.append(f"  {D}Create a sprint folder under `data/sprints/<name>/` and point current.json at it.{X}")
        lines.append("")
        lines.append(f"{B}{C}{'=' * 65}{X}")
        return "\n".join(lines)

    bundle = load_sprint(sprint_dir)
    meta = bundle["meta"]
    sessions = bundle["sessions"]
    budget = bundle["budget"]
    status_md = bundle["status_md"]

    display_name = meta.get("display_name") or meta.get("name") or sprint_dir.name
    sprint_status = meta.get("status", "unknown")
    if is_archive:
        sprint_status = f"archived ({sprint_status})"
    ptr_name = current_ptr.get("name")
    ptr_status = current_ptr.get("status", "")

    # Current session — latest in_progress, else latest done
    active_sess = next((s for s in reversed(sessions) if s.get("status") == "in_progress"), None)
    if not active_sess:
        active_sess = next((s for s in reversed(sessions) if s.get("status") == "done"), None)

    # === Above-the-fold: sprint + session + blocker + KPIs + budget ===
    lines.append("")
    line_1 = f"  {B}{display_name}{X}  {D}({sprint_status}){X}"
    if ptr_name and not sprint_name_arg:
        if ptr_status:
            line_1 += f"  {D}· current.json: {ptr_name} ({ptr_status}){X}"
        else:
            line_1 += f"  {D}· current.json: {ptr_name}{X}"
    lines.append(line_1)

    if active_sess:
        sess_id = active_sess.get("id", "—")
        sess_name = active_sess.get("name", "")
        sess_st = active_sess.get("status", "")
        icon = session_status_icon(sess_st)
        lines.append(f"  {icon} Session {sess_id}: {sess_name}  {D}[{sess_st}]{X}")

    # Blocker line from status.md
    blocker_m = re.search(r"\*\*BLOCKER:\*\*\s*(.+?)(?:\n|$)", status_md)
    if blocker_m:
        blocker = blocker_m.group(1).strip()
        lines.append(f"  {R}BLOCKER:{X} {blocker[:100]}")
    lines.append("")

    # KPIs
    kpis = fetch_kpis(conn)
    if kpis:
        lines.append(f"  {B}LIVE KPIs{X}   {D}(query DB — never stale){X}")
        lines.append(f"  {'_' * 61}")
        wines = kpis.get("wines_active", 0)
        prods = kpis.get("producers_active", 0)
        vint = kpis.get("wine_vintages", 0)
        gr = kpis.get("wine_grapes", 0)
        lines.append(f"  Wines        {n(wines):>10}       Producers    {n(prods):>10}")
        lines.append(f"  Vintages     {n(vint):>10}       Wine_grapes  {n(gr):>10}")
        ins = kpis.get("wine_insights", 0)
        prov = kpis.get("data_provenance", 0)
        lines.append(f"  Insights     {n(ins):>10}       Provenance   {n(prov):>10}")
        lines.append("")

        gdist = kpis.get("grade_dist", {})
        if gdist and wines > 0:
            parts = []
            for grade in ("B", "C", "D", "F"):
                cnt = gdist.get(grade, 0)
                color = G if grade == "B" else Y if grade == "C" else D
                parts.append(f"{color}{grade}={n(cnt)}{X}")
            lines.append(f"  Grades: {'  '.join(parts)}")

        eids = kpis.get("external_ids", {})
        if eids:
            lwin = eids.get("lwin", 0) + eids.get("lwin_7", 0)
            cola = eids.get("cola", 0)
            upc = eids.get("upc", 0)
            lines.append(f"  {D}IDs: LWIN={n(lwin)}  COLA={n(cola)}  UPC={n(upc)}{X}")
        lines.append("")

    # Budget
    if budget:
        spent = budget.get("total_spent", 0) or 0
        total = budget.get("total_budget", 0) or meta.get("budget_total", 0) or 175
        pct = (spent / total * 100) if total else 0
        lines.append(f"  {B}BUDGET{X}")
        lines.append(f"  {'_' * 61}")
        lines.append(f"  ${spent:.2f} / ${total:.2f}  ({pct:.1f}%)   {bar(spent, total)}")
        lines.append("")

    # Reference-layer panel — scaffolded, hidden unless RF sprint enables it
    show_ref = bool(meta.get("show_reference_panel", False))
    if show_ref:
        ref = fetch_reference_layer(conn)
        if ref:
            lines.append(f"  {B}REFERENCE LAYER{X}  {D}(RF sprint){X}")
            lines.append(f"  {'_' * 61}")
            lines.append(
                f"  grapes: {n(ref.get('grapes_total'))}   "
                f"regions: {n(ref.get('regions_total'))}   "
                f"appellations: {n(ref.get('appellations_total'))}"
            )
            lines.append(
                f"  appellation_rules: {n(ref.get('appellation_rules'))}   "
                f"appellation_grapes: {n(ref.get('appellation_grapes'))}"
            )
            lines.append("")

    # === Below-the-fold: session list + backlog + next action ===
    if sessions:
        lines.append(f"  {B}SESSIONS{X}")
        lines.append(f"  {'_' * 61}")
        for s in sessions:
            sid = s.get("id", "?")
            name = s.get("name", "")
            st = s.get("status", "not_started")
            date = s.get("date", "")
            spend = s.get("ai_spend")
            icon = session_status_icon(st)
            short_name = name[:44] + ".." if len(name) > 46 else name
            detail = ""
            if date:
                detail = f"{D}{date}{X}"
            if spend is not None:
                detail += f" {D}(${spend:.2f}){X}" if detail else f"{D}(${spend:.2f}){X}"
            lines.append(f"  {icon} {str(sid):>2}. {short_name:<46} {detail}")
        lines.append("")

    backlog_top = parse_backlog_top(BACKLOG_PATH, limit=3)
    if backlog_top:
        lines.append(f"  {B}BACKLOG TOP 3 (P0/P1){X}")
        lines.append(f"  {'_' * 61}")
        for entry in backlog_top:
            lines.append(f"  {Y}!{X} {entry}")
        lines.append("")

    next_action = parse_next_action(status_md)
    if next_action:
        lines.append(f"  {B}{C}NEXT ACTION{X}  {D}(from status.md){X}")
        lines.append(f"  {'_' * 61}")
        for nl in next_action.splitlines():
            lines.append(f"  {nl}")
        lines.append("")

    lines.append(f"{B}{C}{'=' * 65}{X}")
    return "\n".join(lines)


def save_to_file(content: str):
    out_path = Path("data/stats/sprint_dashboard.md")
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(strip_ansi(content), encoding="utf-8")


def main():
    parser = argparse.ArgumentParser(description="Sprint dashboard")
    parser.add_argument("--sprint", help="Sprint name (default: active from current.json). Archived sprints are searched automatically.")
    parser.add_argument("--watch", nargs="?", const=60, type=int, metavar="SEC", help="Auto-refresh")
    parser.add_argument("--save", action="store_true", help="Write plain-text to data/stats/sprint_dashboard.md")
    args = parser.parse_args()

    conn = None
    try:
        from pipeline.lib.db import get_conn
        conn = get_conn()
        conn.autocommit = True
    except Exception as e:
        print(f"{Y}[warn] Could not connect to DB: {e}{X}")

    if args.watch:
        try:
            while True:
                os.system("cls" if os.name == "nt" else "clear")
                content = generate(conn, args.sprint)
                print(content)
                print(f"\n  {D}Refreshing every {args.watch}s... (Ctrl+C to stop){X}")
                if args.save:
                    save_to_file(content)
                time.sleep(args.watch)
        except KeyboardInterrupt:
            print("\n  Stopped.")
    else:
        content = generate(conn, args.sprint)
        print(content)
        if args.save:
            save_to_file(content)
            print(f"\n  Saved to data/stats/sprint_dashboard.md")

    if conn:
        conn.close()


if __name__ == "__main__":
    main()
