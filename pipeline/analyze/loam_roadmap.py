"""
Loam Roadmap Dashboard — high-level phases and flow.

Reads data/stats/loam_roadmap.json and renders the full product roadmap
with phase status, sub-tasks, and live DB metrics where applicable.

Usage:
    python -m pipeline.analyze.loam_roadmap           # one-shot
    python -m pipeline.analyze.loam_roadmap --watch   # auto-refresh every 60s
    python -m pipeline.analyze.loam_roadmap --save    # write to loam_roadmap.md
"""

import sys
import os
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

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

ROADMAP_PATH = Path("data/stats/loam_roadmap.json")
OUTPUT_PATH = Path("data/stats/loam_roadmap.md")

# ANSI colors (stripped when writing to file)
B = "\033[1m"
D = "\033[2m"
X = "\033[0m"
G = "\033[92m"  # green
Y = "\033[93m"  # yellow
R = "\033[91m"  # red
C = "\033[96m"  # cyan
M = "\033[95m"  # magenta


def strip_ansi(s: str) -> str:
    import re
    return re.sub(r"\033\[[0-9;]*m", "", s)


def status_icon(status: str) -> str:
    return {
        "done": f"{G}[x]{X}",
        "in_progress": f"{Y}[>]{X}",
        "next": f"{M}[!]{X}",
        "planned": f"{D}[ ]{X}",
        "paused": f"{D}[-]{X}",
        "pending": f"{D}[ ]{X}",
    }.get(status, f"{D}[?]{X}")


def status_label(status: str) -> str:
    return {
        "done": f"{G}DONE{X}",
        "in_progress": f"{Y}IN PROGRESS{X}",
        "next": f"{M}NEXT{X}",
        "planned": f"{D}planned{X}",
        "paused": f"{D}paused{X}",
        "pending": f"{D}pending{X}",
    }.get(status, f"{D}{status}{X}")


def q(conn, sql):
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
            return cur.fetchall()
    except Exception:
        return []


def fetch_live_metrics(conn) -> dict:
    """Pull live DB numbers for the phases that care."""
    metrics = {}

    # Phase 2: data population
    rows = q(conn, "SELECT count(*) FROM wines WHERE deleted_at IS NULL")
    metrics["wines"] = rows[0][0] if rows else 0

    rows = q(conn, "SELECT count(*) FROM producers WHERE deleted_at IS NULL")
    metrics["producers"] = rows[0][0] if rows else 0

    # Phase 3: enrichment
    rows = q(conn, """
        SELECT data_grade, count(*) FROM wines
        WHERE deleted_at IS NULL GROUP BY data_grade
    """)
    grade_counts = {g: c for g, c in rows}
    metrics["grade_b"] = grade_counts.get("B", 0)
    metrics["grade_c"] = grade_counts.get("C", 0)
    metrics["grade_d"] = grade_counts.get("D", 0)
    metrics["grade_f"] = grade_counts.get("F", 0)

    rows = q(conn, "SELECT count(*) FROM wine_insights")
    metrics["insights"] = rows[0][0] if rows else 0

    # Foundation counters (Phase 1)
    rows = q(conn, "SELECT count(*) FROM appellations")
    metrics["appellations"] = rows[0][0] if rows else 0
    rows = q(conn, "SELECT count(*) FROM grapes")
    metrics["grapes"] = rows[0][0] if rows else 0
    rows = q(conn, "SELECT count(*) FROM regions")
    metrics["regions"] = rows[0][0] if rows else 0
    rows = q(conn, "SELECT count(*) FROM countries")
    metrics["countries"] = rows[0][0] if rows else 0
    rows = q(conn, "SELECT count(*) FROM appellation_rules")
    metrics["appellation_rules"] = rows[0][0] if rows else 0

    # Josh Test (from file)
    josh_path = Path("data/stats/josh_test_latest.json")
    if josh_path.exists():
        try:
            jt = json.loads(josh_path.read_text())
            metrics["josh_rate"] = jt.get("find_rate", 0)
        except Exception:
            metrics["josh_rate"] = None
    else:
        metrics["josh_rate"] = None

    # Budget: read from the active sprint via data/sprints/current.json → data/sprints/<name>/budget.json
    metrics["total_spent"] = 0
    metrics["total_budget"] = 175
    current_path = Path("data/sprints/current.json")
    if current_path.exists():
        try:
            cur = json.loads(current_path.read_text())
            sprint_name = cur.get("name")
            if sprint_name:
                for candidate in (
                    Path(f"data/sprints/{sprint_name}/budget.json"),
                    Path(f"data/sprints/_archive/{sprint_name}/budget.json"),
                ):
                    if candidate.exists():
                        b = json.loads(candidate.read_text())
                        metrics["total_spent"] = b.get("total_spent", 0)
                        metrics["total_budget"] = b.get("total_budget", 175)
                        metrics["sprint_name"] = sprint_name
                        metrics["sprint_status"] = cur.get("status", "")
                        break
        except Exception:
            pass

    return metrics


# === Phase metric dispatch ===
# Each key is a metrics_query name from loam_roadmap.json. Each function takes the
# fetch_live_metrics() dict and returns a list of short strings to render under a phase.
#
# This replaces the old hardcoded `key_metrics` arrays, which would go stale and mislead.

def _metrics_foundation(m: dict) -> list[str]:
    return [
        f"Countries: {m.get('countries', 0):,}  ·  Regions: {m.get('regions', 0):,}  ·  Appellations: {m.get('appellations', 0):,}",
        f"Grapes: {m.get('grapes', 0):,}  ·  Appellation rules: {m.get('appellation_rules', 0):,}",
    ]


def _metrics_data_population(m: dict) -> list[str]:
    return [
        f"Producers: {m.get('producers', 0):,}  ·  Wines: {m.get('wines', 0):,}",
    ]


def _metrics_enrichment(m: dict) -> list[str]:
    wines = m.get("wines", 0) or 0
    ins = m.get("insights", 0) or 0
    pct = (ins / wines * 100) if wines else 0
    out = [
        f"Grades: B={m.get('grade_b', 0):,}  C={m.get('grade_c', 0):,}  D={m.get('grade_d', 0):,}  F={m.get('grade_f', 0):,}",
        f"Insights: {ins:,} wines ({pct:.1f}% of corpus)",
    ]
    if m.get("josh_rate") is not None:
        out.append(f"Josh Test: {m['josh_rate']:.0%} findability")
    return out


METRIC_DISPATCH = {
    "foundation": _metrics_foundation,
    "data_population": _metrics_data_population,
    "enrichment": _metrics_enrichment,
}


def generate(conn=None) -> str:
    lines = []
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # Header
    lines.append(f"{B}{C}{'=' * 65}{X}")
    lines.append(f"{B}{C}  LOAM PRODUCT ROADMAP{X}            {D}{now}{X}")
    lines.append(f"{B}{C}{'=' * 65}{X}")
    lines.append("")

    # Load roadmap
    if not ROADMAP_PATH.exists():
        lines.append(f"  {R}ERROR: {ROADMAP_PATH} not found{X}")
        return "\n".join(lines)

    roadmap = json.loads(ROADMAP_PATH.read_text())
    phases = roadmap["phases"]

    # Live metrics
    metrics = fetch_live_metrics(conn) if conn else {}

    # Summary counters
    done_count = sum(1 for p in phases if p["status"] == "done")
    active_count = sum(1 for p in phases if p["status"] == "in_progress")
    pending_count = sum(1 for p in phases if p["status"] in ("pending", "planned"))

    # Current sprint banner (reads data/sprints/current.json via fetch_live_metrics)
    if metrics.get("sprint_name"):
        sname = metrics["sprint_name"]
        sstat = metrics.get("sprint_status", "")
        banner = f"  {B}Current Sprint:{X} {B}{sname.upper()}{X}"
        if sstat:
            banner += f"  {D}({sstat}){X}"
        lines.append(banner)
        lines.append("")

    lines.append(f"  {B}OVERVIEW{X}")
    lines.append(f"  {'_' * 61}")
    lines.append(f"  Phases: {G}{done_count} done{X}  "
                 f"{Y}{active_count} active{X}  "
                 f"{D}{pending_count} pending{X}")
    if metrics.get("wines"):
        lines.append(f"  Catalog: {B}{metrics['wines']:,}{X} wines, "
                     f"{B}{metrics['producers']:,}{X} producers")
    if metrics.get("insights"):
        pct = metrics["insights"] / metrics["wines"] * 100 if metrics.get("wines") else 0
        lines.append(f"  Enriched: {B}{metrics['insights']:,}{X} wines ({pct:.1f}%)")
    if metrics.get("josh_rate") is not None:
        lines.append(f"  Josh Test: {B}{metrics['josh_rate']:.0%}{X} findability")
    if metrics.get("total_spent") is not None:
        pct = metrics["total_spent"] / metrics["total_budget"] * 100 if metrics.get("total_budget") else 0
        lines.append(f"  Budget: ${metrics['total_spent']:.2f} / ${metrics['total_budget']:.0f} ({pct:.1f}%)")
    lines.append("")

    # Current phase banner
    current = next((p for p in phases if p["status"] == "in_progress"), None)
    if current:
        lines.append(f"  {B}{Y}>>> CURRENTLY IN PHASE {current['id']}: {current['name']}{X}")
        lines.append("")

    # Phase details
    for p in phases:
        pid = p["id"]
        name = p["name"]
        st = p["status"]
        summary = p.get("summary", "")
        date = p.get("date", "")

        icon = status_icon(st)
        label = status_label(st)

        # Phase header
        header = f"  {icon} {B}PHASE {pid}:{X} {B}{name}{X}  [{label}]"
        if date:
            header += f"  {D}{date}{X}"
        lines.append(header)
        if summary:
            lines.append(f"      {D}{summary}{X}")

        # Phase-level live metrics via dispatch (preferred — always current)
        mq = p.get("metrics_query")
        if mq and mq in METRIC_DISPATCH:
            try:
                for line in METRIC_DISPATCH[mq](metrics):
                    lines.append(f"      • {line}")
            except Exception:
                pass
        elif p.get("key_metrics"):
            # Legacy: fall back to hardcoded key_metrics for phases we haven't wired a query for yet
            for km in p["key_metrics"]:
                lines.append(f"      • {km}")

        # Sub-tasks
        if p.get("sub_tasks"):
            for sub in p["sub_tasks"]:
                sub_icon = status_icon(sub["status"])
                sub_name = sub["name"]
                sub_metric = sub.get("metric", "")
                line = f"      {sub_icon} {sub_name}"
                if sub_metric:
                    line += f"  {D}— {sub_metric}{X}"
                lines.append(line)

        # Notes
        if p.get("notes"):
            lines.append(f"      {D}{p['notes']}{X}")

        lines.append("")

    # Footer
    lines.append(f"  {B}{C}{'=' * 61}{X}")
    lines.append(f"  {D}Data: data/stats/loam_roadmap.json{X}")
    lines.append(f"  {D}Script: pipeline/analyze/loam_roadmap.py{X}")
    lines.append(f"  {B}{C}{'=' * 61}{X}")

    return "\n".join(lines)


def save_to_file(content: str):
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_PATH.write_text(strip_ansi(content), encoding="utf-8")


def main():
    parser = argparse.ArgumentParser(description="Loam high-level roadmap dashboard")
    parser.add_argument("--watch", nargs="?", const=60, type=int, help="Auto-refresh interval (seconds)")
    parser.add_argument("--save", action="store_true", help="Write plain-text version to loam_roadmap.md")
    args = parser.parse_args()

    # Get DB connection (optional — works without DB for a pure-file view)
    conn = None
    try:
        from pipeline.lib.db import get_conn
        conn = get_conn()
    except Exception as e:
        print(f"[warn] Could not connect to DB: {e}")

    if args.watch:
        try:
            while True:
                os.system("cls" if os.name == "nt" else "clear")
                content = generate(conn)
                print(content)
                print(f"\n  {D}Refreshing every {args.watch}s... (Ctrl+C to stop){X}")
                if args.save:
                    save_to_file(content)
                time.sleep(args.watch)
        except KeyboardInterrupt:
            print("\n  Stopped.")
    else:
        content = generate(conn)
        print(content)
        if args.save:
            save_to_file(content)
            print(f"\n  Saved to {OUTPUT_PATH}")

    if conn:
        conn.close()


if __name__ == "__main__":
    main()
