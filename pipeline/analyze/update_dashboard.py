"""Update dashboard.html with fresh session tokens + progress block.

Replaces content between these HTML comment markers:
  <!--updated-->...<!--/updated-->
  <!--session-strip-->...<!--/session-strip-->
  <!--progress-block-->...<!--/progress-block-->

Usage:
  python -m pipeline.analyze.update_dashboard \
      --dashboard data/dashboard.html \
      --transcript <path> \
      --log gemini=data/stats/b6_5a_l1_gemini_basic.log \
      [--step "Step 1 of 11 — L1.5 Gemini basic on all 151K pairs"] \
      [--sprint-spent 107.76] [--sprint-ceiling 250]
"""
from __future__ import annotations

import argparse
import datetime as dt
import re
import sys
from pathlib import Path

from pipeline.analyze.session_tokens import parse_log, parse_transcript


MARK = lambda name: re.compile(
    rf"(<!--{name}-->)(.*?)(<!--/{name}-->)", re.DOTALL
)

# Sprint 6 Steps (flattened, replaces old B6.X/Phase-X nomenclature)
B6_5A_STEPS = [
    "Step 1 — Plan lock-in",
    "Step 2 — LWIN import (+22,598 producers)",
    "Step 3 — Schema + L1 classification (151K pairs)",
    "Step 4 — Calibration + committed thresholds",
    "Step 5 — Full AI classification (L1.5, L2, L2.5)",
    "Step 6 — Phase 3a web-validation on 3,398 high-risk pairs",
    "Step 7 — Expand web coverage (Core + Tail escalations + audit)",
    "Step 8 — Build routing_stage3 decision table",
    "Step 9 — Opus pattern audit + §11 amendments + 50-pair user signoff",
    "Step 10 — Execute merges + Safety Net B + quality audit",
    "Step 11 — Sprint close + Sprint 7 handoff",
]


def build_steps_block(current_step_nums: list[int], done_up_to: int) -> str:
    """Render the step list with done/current/pending markers.

    done_up_to: all steps <= this number are marked done.
    current_step_nums: 1-indexed step numbers currently active (set of in-progress).
    """
    lines = ['<!--block-steps-->', '  <ul class="steps">']
    for i, label in enumerate(B6_5A_STEPS, start=1):
        if i <= done_up_to:
            cls = "done-s"; mark = "&#10003;"
        elif i in current_step_nums:
            cls = "current-s"; mark = "&#9654;"
        else:
            cls = "pending-s"; mark = "&#9675;"
        lines.append(
            f'    <li><span class="marker {cls}">{mark}</span>'
            f'<span class="{cls}">{label}</span></li>'
        )
    lines.append('  </ul>')
    lines.append('  <!--/block-steps-->')
    return "\n".join(lines)


def fmt_k(n: int) -> str:
    if n >= 1_000_000:
        return f"{n/1_000_000:.1f}M"
    if n >= 1_000:
        return f"{n/1_000:.0f}K"
    return str(n)


def build_session_strip(claude: dict, logs: dict, sprint_spent: float,
                        sprint_ceiling: float) -> str:
    # Token totals (input side = raw input + cache reads + cache writes)
    c_in_side = (claude.get("input", 0)
                 + claude.get("cache_read", 0)
                 + claude.get("cache_write_1h", 0)
                 + claude.get("cache_write_5m", 0))
    c_out = claude.get("output", 0)
    c_total = c_in_side + c_out

    p_in = sum(v.get("input_tokens", 0) + v.get("cache_read", 0)
               + v.get("cache_write", 0) for v in logs.values())
    p_out = sum(v.get("output_tokens", 0) for v in logs.values())
    p_total = p_in + p_out

    total = c_total + p_total

    claude_detail = (
        f"{claude.get('entries', 0)} msgs &middot; "
        f"{fmt_k(c_in_side)} in &middot; {fmt_k(c_out)} out"
    )
    pipe_bits = []
    for label, p in logs.items():
        if p.get("pairs") or p.get("input_tokens"):
            pipe_bits.append(
                f"{label}: {p.get('pairs',0):,} pairs &middot; "
                f"{fmt_k(p.get('input_tokens',0) + p.get('cache_read',0) + p.get('cache_write',0))} in &middot; "
                f"{fmt_k(p.get('output_tokens',0))} out"
            )
    pipe_detail = " &nbsp;|&nbsp; ".join(pipe_bits) if pipe_bits else "(none active)"
    return f"""<!--session-strip-->
<div class="session-strip">
  <div class="session-cell">
    <div class="s-label">Opus 4.7 chat tokens</div>
    <div class="s-cost">{fmt_k(c_total)}</div>
    <div class="s-detail">{claude_detail}</div>
  </div>
  <div class="session-cell">
    <div class="s-label">Pipeline tokens</div>
    <div class="s-cost">{fmt_k(p_total)}</div>
    <div class="s-detail">{pipe_detail}</div>
  </div>
  <div class="session-cell total">
    <div class="s-label">Session total tokens</div>
    <div class="s-cost">{fmt_k(total)}</div>
    <div class="s-detail">{fmt_k(c_in_side + p_in)} in &middot; {fmt_k(c_out + p_out)} out</div>
  </div>
</div>
<!--/session-strip-->"""


SPRINT_HISTORY = [
    ("Sprint 1 Build",       0.00),
    ("Sprint 2 Audit",       0.00),
    ("Sprint 3 Fix",         0.00),
    ("Sprint 4 Demo",       37.44),
    ("Sprint 5 AI Bakeoff", 55.00),
]

B6_BLOCKS_HISTORICAL = [
    ("Step 1-2 Plan + LWIN import",   0.00),
    ("Step 3 Schema + L1 classify", 78.44),
    ("Step 4 Calibration",          24.51),
    ("Step 5 L1.5 + L2 + L2.5",    115.85),
    ("Step 6 Phase 3a web-validate", 12.87),
]


def build_spend_breakdown(logs: dict, sprint_ceiling: float) -> str:
    b6_5a_spend = sum(v.get("cost_usd", 0) for v in logs.values())
    sprint6_spend = sum(cost for _, cost in B6_BLOCKS_HISTORICAL) + b6_5a_spend
    total_project = sum(cost for _, cost in SPRINT_HISTORY) + sprint6_spend
    remaining = sprint_ceiling - sprint6_spend

    def row(label, val, muted=True, highlight=False, bold=False, border_top=False, color=None):
        td_left_style = "color:var(--muted);"
        td_right_style = "text-align:right; font-variant-numeric: tabular-nums;"
        if highlight:
            td_left_style = "color:var(--accent); font-weight:700;"
            td_right_style = "text-align:right; font-weight:700; color:var(--accent); font-variant-numeric: tabular-nums;"
        elif bold:
            td_left_style = "font-weight:600;"
            td_right_style = "text-align:right; font-weight:700; font-variant-numeric: tabular-nums;"
        if border_top:
            td_left_style += " border-top:1px solid var(--border); padding-top:4px;"
            td_right_style += " border-top:1px solid var(--border); padding-top:4px;"
        if color:
            td_left_style += f" color:{color};"
            td_right_style += f" color:{color};"
        return f'<tr><td style="{td_left_style}">{label}</td><td style="{td_right_style}">${val:,.2f}</td></tr>'

    sprint_rows = "\n        ".join(row(lbl, v) for lbl, v in SPRINT_HISTORY)
    sprint_rows += "\n        " + row("Sprint 6 Producer Dedup", sprint6_spend, highlight=True)
    sprint_rows += "\n        " + row("Total project spend", total_project, bold=True, border_top=True)

    block_rows = "\n        ".join(row(lbl, v) for lbl, v in B6_BLOCKS_HISTORICAL)
    block_rows += "\n        " + row("Step 7 Coverage expansion", b6_5a_spend, highlight=True)
    block_rows += "\n        " + row("Sprint 6 spent", sprint6_spend, bold=True, border_top=True)
    color = "var(--green)" if remaining > 30 else ("var(--amber)" if remaining > 0 else "var(--red)")
    block_rows += "\n        " + row(f"Remaining of ${sprint_ceiling:.0f} ceiling", remaining, color=color)

    return f"""<!--spend-breakdown-->
<div class="sprint-card">
  <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 18px;">
    <div>
      <div style="font-size: 10px; text-transform: uppercase; letter-spacing: 0.08em; color: var(--muted); font-weight: 700; margin-bottom: 6px;">By sprint</div>
      <table style="width:100%; font-size:12.5px; border-collapse:collapse;">
        {sprint_rows}
      </table>
    </div>
    <div>
      <div style="font-size: 10px; text-transform: uppercase; letter-spacing: 0.08em; color: var(--muted); font-weight: 700; margin-bottom: 6px;">Sprint 6 by block</div>
      <table style="width:100%; font-size:12.5px; border-collapse:collapse;">
        {block_rows}
      </table>
    </div>
  </div>
</div>
<!--/spend-breakdown-->"""


STEP7_LINE_RE = re.compile(r"(\d+)/(\d+)\s*\|\s*(\w+)\s+[\d.]+\s*\|\s*llm=([\d.]+)c\s+web=([\d.]+)c\s*\|\s*\$([\d.]+)\s*total\s*\|\s*eta=(\d+)m")
STEP7_CREDITS_RE = re.compile(r"Serper credits used:\s*(\d+)")


def parse_step7_log(path) -> dict:
    """Parse Step 7 log format:
      N/M | VERDICT 0.95 | llm=0.14c web=0.20c | $X.XX total | eta=Nm
    Plus periodic 'Serper credits used: N' lines.
    """
    from pathlib import Path as _P
    p = _P(path)
    out = {"processed": 0, "total": 0, "cost": 0.0, "eta_min": None,
           "serper_credits": 0, "errors": 0}
    if not p.exists():
        return out
    last_progress = None
    last_credits = 0
    err = 0
    for line in p.read_text(encoding="utf-8", errors="replace").splitlines():
        m = STEP7_LINE_RE.search(line)
        if m:
            last_progress = m
        if STEP7_CREDITS_RE.search(line):
            last_credits = int(STEP7_CREDITS_RE.search(line).group(1))
        if "ERR " in line or "error:" in line.lower():
            err += 1
    if last_progress:
        out["processed"] = int(last_progress.group(1))
        out["total"] = int(last_progress.group(2))
        out["cost"] = float(last_progress.group(6))
        out["eta_min"] = int(last_progress.group(7))
    out["serper_credits"] = last_credits
    out["errors"] = err
    return out


def build_progress_block(step_label: str, logs: dict) -> str:
    """Render progress block. If multiple logs, stack per-run rows."""
    if not logs:
        inner = (
            f'<div class="pstep">{step_label}</div>'
            f'<div class="pstats">(no active job log)</div>'
        )
        return f'<!--progress-block-->\n  <div class="block-progress">\n    {inner}\n  </div>\n  <!--/progress-block-->'

    # Try Step 7 format first (3 parallel runs); fall back to single-run format
    step7_data = {}
    for label, path_or_parsed in logs.items():
        # If the path was passed as a Path-like (log filename), try parsing as Step 7
        pth = getattr(path_or_parsed, '_path', None) if hasattr(path_or_parsed, '_path') else None
    # Simpler: re-parse each log path directly if we can find it
    # We don't have raw paths here; fall back to using the existing parse_log data
    is_step7 = all(l.startswith("step7") or l in ("core", "tail", "audit") for l in logs.keys())

    if is_step7 and len(logs) >= 2:
        rows = []
        total_processed = 0
        total_cost = 0.0
        total_credits = 0
        for label, p in logs.items():
            processed = p.get("processed") or 0
            total = p.get("total") or 0
            pct = (processed / total * 100) if total else 0
            cost = p.get("cost_usd") or p.get("cost") or 0
            eta = p.get("eta_min")
            eta_str = f"ETA {eta}m" if eta is not None else "ETA --"
            credits = p.get("serper_credits", 0)
            total_processed += processed
            total_cost += cost
            total_credits += credits
            rows.append(
                f'<div class="pstep" style="font-size:12px; margin-top:6px;">{label}</div>'
                f'<div class="progress-bar"><div class="progress-fill" style="width: {pct:.1f}%"></div></div>'
                f'<div class="pstats" style="font-size:11.5px;">{processed:,} / {total:,} ({pct:.1f}%) &middot; '
                f'${cost:.2f} &middot; {eta_str} &middot; {credits:,} Serper credits</div>'
            )
        header = (
            f'<div class="pstep">{step_label}</div>'
            f'<div class="pstats">combined: {total_processed:,} pairs processed &middot; '
            f'${total_cost:.2f} spent &middot; {total_credits:,} Serper credits</div>'
        )
        inner = header + "\n    " + "\n    ".join(rows)
    else:
        label, p = next(iter(logs.items()))
        processed = p.get("processed") or p.get("pairs") or 0
        total = p.get("total") or 0
        pct = (processed / total * 100) if total else 0
        eta = p.get("eta_min")
        eta_str = f"ETA {eta} min" if eta is not None else "ETA --"
        errs = p.get("errors", 0)
        err_pct = (errs / max(1, processed) * 100)
        rate = "0.00¢" if not processed else f"{p.get('cost_usd',0)*100/max(1,processed):.3f}¢"
        inner = (
            f'<div class="pstep">{step_label}</div>'
            f'<div class="progress-bar"><div class="progress-fill" style="width: {pct:.1f}%"></div></div>'
            f'<div class="pstats">{processed:,} / {total:,} pairs ({pct:.1f}%) &middot; '
            f'${p.get("cost_usd",0):.2f} &middot; {eta_str} &middot; '
            f'{errs} parse errors ({err_pct:.2f}%) &middot; {rate}/pair</div>'
        )
    return f'<!--progress-block-->\n  <div class="block-progress">\n    {inner}\n  </div>\n  <!--/progress-block-->'


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dashboard", required=True)
    ap.add_argument("--transcript", default=None)
    ap.add_argument("--log", action="append", default=[])
    ap.add_argument("--step", default="Step 7 of 11 — Web-validation coverage expansion (Core + Tail + Audit in parallel)")
    ap.add_argument("--sprint-spent", type=float, default=231.67)
    ap.add_argument("--sprint-ceiling", type=float, default=450)
    ap.add_argument("--done-up-to", type=int, default=0,
                    help="all steps <= this number are marked done (1-indexed)")
    ap.add_argument("--current-steps", default="",
                    help="comma-sep 1-indexed step numbers currently in-progress")
    args = ap.parse_args()

    claude = parse_transcript(Path(args.transcript)) if args.transcript else {}
    logs = {}
    for entry in args.log:
        label, pth = entry.split("=", 1) if "=" in entry else (Path(entry).stem, entry)
        path_obj = Path(pth)
        # If this looks like a Step 7 log (filename contains step7), use the dedicated parser.
        if "step7" in path_obj.name or label in ("core", "tail", "audit"):
            parsed = parse_step7_log(path_obj)
            # session_tokens.parse_log also parses cost/tokens for spend breakdown;
            # run it additionally so spend_breakdown has token-level data when available
            sess = parse_log(path_obj)
            parsed["cost_usd"] = parsed.get("cost") or sess.get("cost_usd", 0)
            parsed["input_tokens"] = sess.get("input_tokens", 0)
            parsed["output_tokens"] = sess.get("output_tokens", 0)
            parsed["cache_read"] = sess.get("cache_read", 0)
            parsed["cache_write"] = sess.get("cache_write", 0)
            parsed["pairs"] = parsed.get("processed", 0)
            logs[label] = parsed
        else:
            logs[label] = parse_log(path_obj)

    dash_path = Path(args.dashboard)
    text = dash_path.read_text(encoding="utf-8")

    now = dt.datetime.now().strftime("%Y-%m-%d %H:%M")
    text = MARK("updated").sub(f"<!--updated-->updated {now}<!--/updated-->", text)
    text = MARK("session-strip").sub(lambda _: build_session_strip(
        claude, logs, args.sprint_spent, args.sprint_ceiling), text)
    text = MARK("progress-block").sub(lambda _: build_progress_block(args.step, logs), text)
    current = [int(x) for x in args.current_steps.split(",") if x.strip().isdigit()]
    text = MARK("block-steps").sub(lambda _: build_steps_block(current, args.done_up_to), text)
    text = MARK("spend-breakdown").sub(lambda _: build_spend_breakdown(logs, args.sprint_ceiling), text)

    dash_path.write_text(text, encoding="utf-8")
    pipe_cost = sum(v.get("cost_usd", 0) for v in logs.values())
    print(f"[{now}] updated {dash_path.name} | chat ${claude.get('cost_usd',0):.2f} | "
          f"pipeline ${pipe_cost:.2f} | total ${claude.get('cost_usd',0)+pipe_cost:.2f}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
