#!/usr/bin/env python3
"""Stage 1 of the three-stage enrichment rollout — revalidate the 34 audit fails.

Reads data/stats/enrichment_audit.json, filters to wines with overall.verdict
== 'fail' (7 Grade B + 27 Grade C), re-enriches each one through the full
L1+L3 pipeline IN MEMORY, then re-audits with the same auditor used in
Session 11.

This is a TEST. Nothing writes to wine_insights.

Models:
    Grade B → Sonnet 4.6 (mirrors production)
    Grade C → Haiku 4.5 (mirrors production — different from Session 11)

Outputs:
    data/stats/stage1_results.json — full results + per-wine details
    data/stats/stage1_results.md   — markdown summary

Usage:
    python -m pipeline.enrich.stage1_revalidate
    python -m pipeline.enrich.stage1_revalidate --limit 5    # smoke test
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

os.environ["PYTHONUNBUFFERED"] = "1"
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from dotenv import load_dotenv
load_dotenv(override=True)

import anthropic

from pipeline.lib.db import get_conn
from pipeline.enrich.build_facts_packet import build_facts_packet
from pipeline.enrich.enrich_prompts import HAIKU_MODEL, SONNET_MODEL
from pipeline.enrich.fact_check_pass import run_l3_loop
from pipeline.analyze.enrichment_audit import (
    audit_grade_b,
    audit_grade_c,
    summarize,
    PRICING as AUDIT_PRICING,
    SONNET_MODEL as AUDIT_MODEL,
)

ORIGINAL_AUDIT_JSON = Path("data/stats/enrichment_audit.json")
OUT_JSON = Path("data/stats/stage1_results.json")
OUT_MD = Path("data/stats/stage1_results.md")

BUDGET_LIMIT = 5.0  # hard stop


# ── Helpers ──────────────────────────────────────────────────────────────────


def load_fail_set() -> tuple[list[dict], dict]:
    """Read the original audit, return (list of fail rows, dict by wine_id of original scores)."""
    audit = json.loads(ORIGINAL_AUDIT_JSON.read_text(encoding="utf-8"))
    fails = []
    orig_scores = {}
    for r in audit["results"]:
        a = r.get("audit")
        if not a:
            continue
        overall = a.get("overall")
        if not isinstance(overall, dict):
            continue
        orig_scores[r["wine_id"]] = {
            "score": overall.get("score"),
            "verdict": overall.get("verdict"),
            "summary": overall.get("summary", ""),
        }
        if overall.get("verdict") == "fail":
            fails.append(r)
    return fails, orig_scores


def reshape_for_audit(wine_id: str, grade: str, enrichment: dict, db_facts: dict) -> dict:
    """Build the dict shape audit_grade_b / audit_grade_c expect."""
    enr = enrichment or {}
    grapes_str = ", ".join(g["name"] for g in db_facts.get("grapes") or []) or "—"
    return {
        "wine_id": wine_id,
        "wine_name": db_facts.get("display_name") or "—",
        "producer": db_facts.get("producer_name") or "—",
        "appellation": db_facts.get("appellation") or "—",
        "region": db_facts.get("region_name") or "—",
        "country": db_facts.get("country") or "—",
        "color": db_facts.get("color") or "—",
        "wine_type": db_facts.get("wine_type") or "—",
        "grapes": grapes_str,
        "ai_hook": enr.get("hook"),
        "ai_wine_summary": enr.get("wine_summary"),
        "ai_style_profile": enr.get("style_profile"),
        "ai_terroir_expression": enr.get("terroir_expression"),
        "ai_food_pairing": enr.get("food_pairing"),
        "ai_cellar_recommendation": enr.get("cellar_recommendation"),
        "ai_comparable_wines": enr.get("comparable_wines"),
        "ai_vinification_summary": enr.get("vinification_summary"),
    }


# ── Main ─────────────────────────────────────────────────────────────────────


def main() -> int:
    parser = argparse.ArgumentParser(description="Stage 1 enrichment revalidation")
    parser.add_argument("--limit", type=int, default=None, help="Cap wines processed (smoke test)")
    parser.add_argument("--dry-run", action="store_true", help="Skip API calls, list what would run")
    args = parser.parse_args()

    if os.name == "nt":
        import io
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

    print(f"\n{'='*60}")
    print(f"  STAGE 1 ENRICHMENT REVALIDATION")
    print(f"  {datetime.now(timezone.utc).isoformat()}")
    print(f"{'='*60}\n")

    fails, orig_scores = load_fail_set()
    if args.limit:
        fails = fails[:args.limit]
    print(f"  Loaded {len(fails)} fail wines from {ORIGINAL_AUDIT_JSON}")
    n_b = len([r for r in fails if r["grade"] == "B"])
    n_c = len([r for r in fails if r["grade"] == "C"])
    print(f"  Grade B: {n_b}    Grade C: {n_c}")
    print(f"  Models:  B → {SONNET_MODEL}    C → {HAIKU_MODEL}")
    print(f"  Budget:  ${BUDGET_LIMIT:.2f}")
    print()

    if args.dry_run:
        for r in fails:
            print(f"  [{r['grade']}] {r['wine_id'][:8]} {r['producer'][:25] if r['producer'] else '?'} — {(r.get('wine_name') or '?')[:40]}")
        return 0

    conn = get_conn()
    cur = conn.cursor()
    client = anthropic.Anthropic()

    results: list[dict] = []
    total_gen_cost = 0.0
    total_input_tokens = 0
    total_output_tokens = 0

    for i, fail_row in enumerate(fails, 1):
        wine_id = fail_row["wine_id"]
        grade = fail_row["grade"]
        prefix = f"[{i}/{len(fails)}] {grade} {wine_id[:8]}"
        print(f"  {prefix} fetch facts...", end=" ", flush=True)

        facts = build_facts_packet(cur, wine_id)
        if not facts:
            print("MISSING")
            results.append({
                "wine_id": wine_id,
                "grade": grade,
                "error": "wine not found",
            })
            continue

        print(f"L1+L3...", end=" ", flush=True)
        loop = run_l3_loop(client, facts, grade)
        total_gen_cost += loop.get("cost_usd", 0.0)
        total_input_tokens += loop.get("usage", {}).get("input_tokens", 0)
        total_output_tokens += loop.get("usage", {}).get("output_tokens", 0)

        status = loop.get("fact_check_status")
        flags = loop.get("fact_check_flags") or []
        retries = loop.get("retries", 0)
        err = loop.get("error")

        if err:
            print(f"ERROR ({err[:40]})")
        else:
            print(f"{status} (flags={len(flags)}, retries={retries})")

        results.append({
            "wine_id": wine_id,
            "grade": grade,
            "display_name": facts.get("display_name"),
            "producer": facts.get("producer_name"),
            "appellation": facts.get("appellation"),
            "country": facts.get("country"),
            "original_score": (orig_scores.get(wine_id) or {}).get("score"),
            "original_verdict": (orig_scores.get(wine_id) or {}).get("verdict"),
            "facts_summary": {
                "n_grapes": len(facts.get("grapes") or []),
                "n_vintages": len(facts.get("vintages") or []),
                "has_appellation_rules": facts.get("appellation_rules") is not None,
                "n_scores": len(facts.get("scores") or []),
                "has_price": facts.get("price_range") is not None,
                "n_unknowns": len(facts.get("unknowns") or []),
                "n_comparables": len(facts.get("comparables") or []),
            },
            "enrichment": loop.get("enrichment"),
            "fact_check_status": status,
            "fact_check_flags": flags,
            "retries": retries,
            "model_used": loop.get("model_used"),
            "gen_cost_usd": loop.get("cost_usd", 0.0),
            "gen_usage": loop.get("usage", {}),
            "error": err,
            "_db_facts": {  # used for re-audit reshape, dropped from final output
                "display_name": facts.get("display_name"),
                "producer_name": facts.get("producer_name"),
                "appellation": facts.get("appellation"),
                "region_name": facts.get("region_name"),
                "country": facts.get("country"),
                "color": facts.get("color"),
                "wine_type": facts.get("wine_type"),
                "grapes": facts.get("grapes") or [],
            },
        })

        if total_gen_cost > BUDGET_LIMIT:
            print(f"\n  BUDGET EXCEEDED ({total_gen_cost:.2f} > {BUDGET_LIMIT}). STOP.")
            break

    # ── Re-audit ────────────────────────────────────────────────────────
    print(f"\n  Re-auditing with {AUDIT_MODEL}...")
    sys.stdout.flush()

    grade_b_samples = []
    grade_c_samples = []
    sample_to_result_idx = {}  # wine_id -> result index for joining audits back
    for idx, r in enumerate(results):
        if r.get("error") or not r.get("enrichment"):
            continue
        sample = reshape_for_audit(r["wine_id"], r["grade"], r["enrichment"], r["_db_facts"])
        sample_to_result_idx[r["wine_id"]] = idx
        if r["grade"] == "B":
            grade_b_samples.append(sample)
        else:
            grade_c_samples.append(sample)

    audit_b = audit_grade_b(client, grade_b_samples) if grade_b_samples else []
    audit_c = audit_grade_c(client, grade_c_samples) if grade_c_samples else []
    all_audits = audit_b + audit_c

    audit_input = sum(a.get("usage", {}).get("input_tokens", 0) for a in all_audits)
    audit_output = sum(a.get("usage", {}).get("output_tokens", 0) for a in all_audits)
    audit_cost = (audit_input * AUDIT_PRICING["input"] + audit_output * AUDIT_PRICING["output"]) / 1_000_000

    # Join audit results back into the results list
    audit_by_wine = {a["wine_id"]: a for a in all_audits}
    for r in results:
        wid = r["wine_id"]
        a = audit_by_wine.get(wid)
        if not a:
            continue
        audit = a.get("audit") or {}
        overall = audit.get("overall") if isinstance(audit.get("overall"), dict) else {}
        r["stage1_score"] = overall.get("score")
        r["stage1_verdict"] = overall.get("verdict")
        r["stage1_summary"] = overall.get("summary", "")
        r["stage1_audit_full"] = audit

    # ── Aggregates ──────────────────────────────────────────────────────
    by_grade = {}
    fact_check_impact = {
        "passed_first_try": 0,
        "retried_once_passed": 0,
        "partial": 0,
        "failed": 0,
        "total_flags": 0,
    }

    for grade in ("B", "C"):
        rows = [r for r in results if r["grade"] == grade and not r.get("error")]
        scored = [r for r in rows if r.get("stage1_score") is not None]
        orig_scored = [r for r in rows if r.get("original_score") is not None]
        avg_orig = round(sum(r["original_score"] for r in orig_scored) / len(orig_scored), 2) if orig_scored else None
        avg_new = round(sum(r["stage1_score"] for r in scored) / len(scored), 2) if scored else None
        fails_after = len([r for r in rows if r.get("stage1_verdict") == "fail"])
        warns = len([r for r in rows if r.get("stage1_verdict") == "warn"])
        passes = len([r for r in rows if r.get("stage1_verdict") == "pass"])

        fc_pass = len([r for r in rows if r.get("fact_check_status") == "passed"])
        fc_retry = len([r for r in rows if r.get("fact_check_status") == "retried_passed"])
        fc_partial = len([r for r in rows if r.get("fact_check_status") == "partial"])
        fc_failed = len([r for r in rows if r.get("fact_check_status") == "failed"])

        by_grade[grade] = {
            "n": len(rows),
            "orig_avg": avg_orig,
            "new_avg": avg_new,
            "delta": round(avg_new - avg_orig, 2) if (avg_new and avg_orig) else None,
            "verdicts_after": {"pass": passes, "warn": warns, "fail": fails_after},
            "fact_check": {
                "passed": fc_pass,
                "retried_passed": fc_retry,
                "partial": fc_partial,
                "failed": fc_failed,
            },
        }

    for r in results:
        if r.get("error"):
            continue
        s = r.get("fact_check_status")
        if s == "passed":
            fact_check_impact["passed_first_try"] += 1
        elif s == "retried_passed":
            fact_check_impact["retried_once_passed"] += 1
        elif s == "partial":
            fact_check_impact["partial"] += 1
        elif s == "failed":
            fact_check_impact["failed"] += 1
        fact_check_impact["total_flags"] += len(r.get("fact_check_flags") or [])

    n_results_no_err = len([r for r in results if not r.get("error")])
    fact_check_impact["avg_flags_per_wine"] = round(
        fact_check_impact["total_flags"] / n_results_no_err, 2
    ) if n_results_no_err else 0

    overall_orig = [r["original_score"] for r in results if r.get("original_score")]
    overall_new = [r["stage1_score"] for r in results if r.get("stage1_score")]
    overall_orig_avg = round(sum(overall_orig) / len(overall_orig), 2) if overall_orig else None
    overall_new_avg = round(sum(overall_new) / len(overall_new), 2) if overall_new else None
    overall_delta = round(overall_new_avg - overall_orig_avg, 2) if (overall_new_avg and overall_orig_avg) else None

    total_cost = total_gen_cost + audit_cost

    payload = {
        "run_date": datetime.now(timezone.utc).isoformat(),
        "models": {"grade_b": SONNET_MODEL, "grade_c": HAIKU_MODEL, "audit": AUDIT_MODEL},
        "n_wines": len(results),
        "original_avg": overall_orig_avg,
        "stage1_avg": overall_new_avg,
        "delta": overall_delta,
        "by_grade": by_grade,
        "fact_check_impact": fact_check_impact,
        "cost_usd": {
            "generation_l1_l3": round(total_gen_cost, 4),
            "audit": round(audit_cost, 4),
            "total": round(total_cost, 4),
        },
        "tokens": {
            "generation_input": total_input_tokens,
            "generation_output": total_output_tokens,
            "audit_input": audit_input,
            "audit_output": audit_output,
        },
        "results": [{k: v for k, v in r.items() if k != "_db_facts"} for r in results],
    }

    OUT_JSON.parent.mkdir(parents=True, exist_ok=True)
    OUT_JSON.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")

    write_markdown(payload)

    print(f"\n{'='*60}")
    print(f"  STAGE 1 COMPLETE")
    print(f"  Original avg: {overall_orig_avg}/5")
    print(f"  Stage 1 avg:  {overall_new_avg}/5  ({'+' if overall_delta and overall_delta > 0 else ''}{overall_delta})")
    if "B" in by_grade:
        b = by_grade["B"]
        print(f"  Grade B:      {b['orig_avg']} → {b['new_avg']} ({'+' if b['delta'] and b['delta'] > 0 else ''}{b['delta']})  pass/warn/fail = {b['verdicts_after']['pass']}/{b['verdicts_after']['warn']}/{b['verdicts_after']['fail']}")
    if "C" in by_grade:
        c = by_grade["C"]
        print(f"  Grade C:      {c['orig_avg']} → {c['new_avg']} ({'+' if c['delta'] and c['delta'] > 0 else ''}{c['delta']})  pass/warn/fail = {c['verdicts_after']['pass']}/{c['verdicts_after']['warn']}/{c['verdicts_after']['fail']}")
    print(f"  Cost:         ${total_cost:.4f} (gen ${total_gen_cost:.4f} + audit ${audit_cost:.4f})")
    print(f"  Output:       {OUT_JSON} / {OUT_MD}")
    print(f"{'='*60}\n")

    cur.close()
    conn.close()
    return 0


def write_markdown(payload: dict) -> None:
    lines = ["# Stage 1 Enrichment Revalidation Results\n"]
    lines.append(f"**Run:** {payload['run_date']}")
    lines.append(f"**Models:** Grade B → `{payload['models']['grade_b']}`, Grade C → `{payload['models']['grade_c']}`")
    lines.append(f"**Auditor:** `{payload['models']['audit']}`")
    lines.append(f"**Wines tested:** {payload['n_wines']}")
    lines.append(f"**Cost:** ${payload['cost_usd']['total']:.4f} (gen ${payload['cost_usd']['generation_l1_l3']:.4f} + audit ${payload['cost_usd']['audit']:.4f})")
    lines.append("")
    lines.append("## Overall\n")
    lines.append(f"- Original avg: {payload['original_avg']}/5")
    lines.append(f"- Stage 1 avg:  **{payload['stage1_avg']}/5**")
    delta = payload['delta']
    lines.append(f"- Delta: **{'+' if delta and delta > 0 else ''}{delta}**")
    lines.append("")

    for grade in ("B", "C"):
        bg = payload["by_grade"].get(grade)
        if not bg:
            continue
        lines.append(f"## Grade {grade}\n")
        lines.append(f"- N: {bg['n']}")
        lines.append(f"- Avg: {bg['orig_avg']} → **{bg['new_avg']}** ({'+' if bg['delta'] and bg['delta'] > 0 else ''}{bg['delta']})")
        v = bg['verdicts_after']
        lines.append(f"- Verdicts after: pass {v['pass']} / warn {v['warn']} / fail {v['fail']}")
        fc = bg['fact_check']
        lines.append(f"- Fact-check: passed {fc['passed']} / retried_passed {fc['retried_passed']} / partial {fc['partial']} / failed {fc['failed']}")
        lines.append("")

    fci = payload["fact_check_impact"]
    lines.append("## Fact-check impact\n")
    lines.append(f"- Passed first try: {fci['passed_first_try']}")
    lines.append(f"- Retried once and passed: {fci['retried_once_passed']}")
    lines.append(f"- Partial (low/medium flags only): {fci['partial']}")
    lines.append(f"- Failed (high-severity persisted): {fci['failed']}")
    lines.append(f"- Avg flags per wine: {fci['avg_flags_per_wine']}")
    lines.append("")

    lines.append("## Per-wine deltas\n")
    lines.append("| Grade | Wine | Original | Stage 1 | Δ | Status | Flags |")
    lines.append("|-------|------|---------:|--------:|---:|--------|------:|")
    rows = sorted(
        payload["results"],
        key=lambda r: (r.get("stage1_score") or 0) - (r.get("original_score") or 0),
        reverse=True,
    )
    for r in rows:
        if r.get("error"):
            lines.append(f"| {r['grade']} | {r['wine_id'][:8]} | — | — | — | ERROR | — |")
            continue
        prod = (r.get("producer") or "")[:20]
        wine = (r.get("display_name") or "")[:30]
        orig = r.get("original_score")
        new = r.get("stage1_score")
        delta = (new - orig) if (new and orig) else None
        status = r.get("fact_check_status") or "?"
        n_flags = len(r.get("fact_check_flags") or [])
        lines.append(
            f"| {r['grade']} | {prod} — {wine} | {orig} | {new} | "
            f"{'+' if delta and delta > 0 else ''}{delta} | {status} | {n_flags} |"
        )

    OUT_MD.parent.mkdir(parents=True, exist_ok=True)
    OUT_MD.write_text("\n".join(lines), encoding="utf-8")


if __name__ == "__main__":
    sys.exit(main())
