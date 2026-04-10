#!/usr/bin/env python3
"""Audit the L1 test results using the same auditor as enrichment_audit.py.

Reads data/stats/l1_test.json, re-shapes the samples into the format
enrichment_audit expects, runs them through audit_grade_b / audit_grade_c,
and compares to the original audit scores from enrichment_audit.json.

Usage:
    python -m pipeline.enrich.l1_audit
"""

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
from pipeline.analyze.enrichment_audit import (
    audit_grade_b, audit_grade_c, summarize, PRICING, SONNET_MODEL
)

L1_JSON = Path("data/stats/l1_test.json")
ORIGINAL_AUDIT_JSON = Path("data/stats/enrichment_audit.json")
OUT_JSON = Path("data/stats/l1_audit.json")
OUT_MD = Path("data/stats/l1_audit.md")


def build_sample_from_l1(l1_row: dict, db_facts: dict) -> dict:
    """Reshape an L1 result into the format audit_grade_b/c expect.
    The audit functions read ai_hook, ai_wine_summary, etc. from the sample dict."""
    enrichment = l1_row.get("l1_enrichment") or {}
    grapes_str = ", ".join(g["name"] for g in db_facts.get("grapes", []))

    return {
        "wine_id": l1_row["wine_id"],
        "wine_name": db_facts.get("display_name") or l1_row["display_name"],
        "producer": l1_row["producer"] or "—",
        "appellation": l1_row["appellation"] or "—",
        "region": db_facts.get("region_name") or "—",
        "country": l1_row["country"] or "—",
        "color": db_facts.get("color") or "—",
        "wine_type": db_facts.get("wine_type") or "—",
        "grapes": grapes_str or "—",
        "ai_hook": enrichment.get("hook"),
        "ai_wine_summary": enrichment.get("wine_summary"),
        "ai_style_profile": enrichment.get("style_profile"),
        "ai_terroir_expression": enrichment.get("terroir_expression"),
        "ai_food_pairing": enrichment.get("food_pairing"),
        "ai_cellar_recommendation": enrichment.get("cellar_recommendation"),
        "ai_comparable_wines": enrichment.get("comparable_wines"),
        "ai_vinification_summary": enrichment.get("vinification_summary"),
    }


def fetch_db_context(cur, wine_id: str) -> dict:
    """Minimal DB context for sample shaping."""
    cur.execute("""
        SELECT w.display_name, w.color, w.wine_type, r.name AS region_name
        FROM wines w
        LEFT JOIN regions r ON w.region_id = r.id
        WHERE w.id = %s
    """, (wine_id,))
    row = cur.fetchone()
    if not row:
        return {}
    facts = {"display_name": row[0], "color": row[1], "wine_type": row[2], "region_name": row[3]}

    cur.execute("""
        SELECT g.display_name FROM wine_grapes wg
        JOIN grapes g ON wg.grape_id = g.id
        WHERE wg.wine_id = %s
        ORDER BY wg.percentage DESC NULLS LAST
    """, (wine_id,))
    facts["grapes"] = [{"name": r[0]} for r in cur.fetchall()]
    return facts


def main():
    if os.name == "nt":
        import io
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

    print(f"\n{'='*60}")
    print(f"  L1 AUDIT — scoring the L1-regenerated enrichments")
    print(f"  {datetime.now(timezone.utc).isoformat()}")
    print(f"{'='*60}\n")

    l1_data = json.loads(L1_JSON.read_text(encoding="utf-8"))
    original = json.loads(ORIGINAL_AUDIT_JSON.read_text(encoding="utf-8"))

    # Build a lookup of original scores by wine_id
    orig_scores = {}
    for r in original["results"]:
        a = r.get("audit") or {}
        overall = a.get("overall") or {}
        if overall.get("score"):
            orig_scores[r["wine_id"]] = {
                "score": overall["score"],
                "verdict": overall.get("verdict"),
                "summary": overall.get("summary", ""),
            }

    conn = get_conn()
    cur = conn.cursor()
    client = anthropic.Anthropic()

    # Re-shape into audit-compatible samples, split by grade
    grade_b_samples = []
    grade_c_samples = []
    for row in l1_data["results"]:
        if row.get("error") or not row.get("l1_enrichment"):
            print(f"Skip {row['wine_id'][:8]} (error or no enrichment)")
            continue
        db_facts = fetch_db_context(cur, row["wine_id"])
        sample = build_sample_from_l1(row, db_facts)
        if row["grade"] == "B":
            grade_b_samples.append(sample)
        else:
            grade_c_samples.append(sample)

    print(f"\nGrade B samples: {len(grade_b_samples)}")
    print(f"Grade C samples: {len(grade_c_samples)}")

    print("\nAuditing L1 Grade B results...")
    grade_b_audits = audit_grade_b(client, grade_b_samples) if grade_b_samples else []

    print("\nAuditing L1 Grade C results...")
    grade_c_audits = audit_grade_c(client, grade_c_samples) if grade_c_samples else []

    all_audits = grade_b_audits + grade_c_audits

    # Cost calculation
    total_input = sum(a.get("usage", {}).get("input_tokens", 0) for a in all_audits)
    total_output = sum(a.get("usage", {}).get("output_tokens", 0) for a in all_audits)
    cost = (total_input * PRICING["input"] + total_output * PRICING["output"]) / 1_000_000

    summary = {}
    if grade_b_audits:
        summary["B"] = summarize(grade_b_audits)
    if grade_c_audits:
        summary["C"] = summarize(grade_c_audits)

    # Build comparison table
    comparison = []
    for a in all_audits:
        wine_id = a["wine_id"]
        orig = orig_scores.get(wine_id, {})
        new_score = None
        new_verdict = None
        new_summary = ""
        if a.get("audit") and isinstance(a["audit"].get("overall"), dict):
            overall = a["audit"]["overall"]
            new_score = overall.get("score")
            new_verdict = overall.get("verdict")
            new_summary = overall.get("summary", "")
        comparison.append({
            "wine_id": wine_id,
            "grade": a["grade"],
            "producer": a["producer"],
            "wine_name": a["wine_name"],
            "original_score": orig.get("score"),
            "original_verdict": orig.get("verdict"),
            "l1_score": new_score,
            "l1_verdict": new_verdict,
            "delta": (new_score - orig["score"]) if (new_score and orig.get("score")) else None,
            "l1_summary": new_summary,
        })

    # Average delta
    deltas = [c["delta"] for c in comparison if c["delta"] is not None]
    avg_delta = round(sum(deltas) / len(deltas), 2) if deltas else None

    orig_avg = round(sum(c["original_score"] for c in comparison if c["original_score"]) / len(comparison), 2)
    l1_avg = round(sum(c["l1_score"] for c in comparison if c["l1_score"]) / len(comparison), 2)

    payload = {
        "run_date": datetime.now(timezone.utc).isoformat(),
        "model": SONNET_MODEL,
        "n_wines": len(comparison),
        "cost_usd": round(cost, 4),
        "original_avg": orig_avg,
        "l1_avg": l1_avg,
        "avg_delta": avg_delta,
        "summary": summary,
        "comparison": comparison,
        "raw_audits": all_audits,
    }

    OUT_JSON.parent.mkdir(parents=True, exist_ok=True)
    OUT_JSON.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")

    # Markdown summary
    lines = ["# L1 Enrichment Validation — Audit Results\n"]
    lines.append(f"**Run:** {payload['run_date']}")
    lines.append(f"**Wines tested:** {payload['n_wines']}")
    lines.append(f"**Cost:** ${payload['cost_usd']:.4f}")
    lines.append("")
    lines.append(f"## Before vs After\n")
    lines.append(f"- **Original avg:** {orig_avg}/5")
    lines.append(f"- **L1 avg:** {l1_avg}/5")
    lines.append(f"- **Avg delta:** {'+' if avg_delta and avg_delta > 0 else ''}{avg_delta}")
    lines.append("")
    lines.append("## Per-wine comparison\n")
    lines.append("| Grade | Producer — Wine | Original | L1 | Δ |")
    lines.append("|-------|-----------------|---------:|---:|---:|")
    for c in sorted(comparison, key=lambda x: x["delta"] or 0, reverse=True):
        prod = c["producer"][:25]
        wine = (c["wine_name"] or "")[:30]
        lines.append(
            f"| {c['grade']} | {prod} — {wine} | {c['original_score']} ({c['original_verdict']}) | "
            f"{c['l1_score']} ({c['l1_verdict']}) | "
            f"{'+' if c['delta'] and c['delta'] > 0 else ''}{c['delta']} |"
        )
    lines.append("")

    if summary.get("B"):
        s = summary["B"]
        lines.append(f"## Grade B Summary\n")
        lines.append(f"- Sample: {s['total']}")
        if s.get("overall_avg") is not None:
            lines.append(f"- Overall avg: **{s['overall_avg']}/5**")
        lines.append(f"- Verdicts: {s.get('verdicts', {})}")
        if s.get("top_issues"):
            lines.append("- Top issues: " + ", ".join(f"`{t}` ({c})" for t, c in s["top_issues"]))
        lines.append("")

    if summary.get("C"):
        s = summary["C"]
        lines.append(f"## Grade C Summary\n")
        lines.append(f"- Sample: {s['total']}")
        if s.get("overall_avg") is not None:
            lines.append(f"- Overall avg: **{s['overall_avg']}/5**")
        lines.append(f"- Verdicts: {s.get('verdicts', {})}")
        if s.get("top_issues"):
            lines.append("- Top issues: " + ", ".join(f"`{t}` ({c})" for t, c in s["top_issues"]))
        lines.append("")

    OUT_MD.write_text("\n".join(lines), encoding="utf-8")

    print(f"\n{'='*60}")
    print(f"  L1 AUDIT COMPLETE")
    print(f"  Original avg: {orig_avg}/5")
    print(f"  L1 avg:       {l1_avg}/5")
    print(f"  Delta:        {'+' if avg_delta and avg_delta > 0 else ''}{avg_delta}")
    print(f"  Cost:         ${cost:.4f}")
    print(f"  Output:       {OUT_JSON} / {OUT_MD}")
    print(f"{'='*60}\n")

    cur.close()
    conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
