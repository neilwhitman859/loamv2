#!/usr/bin/env python3
"""Stage 2 of the three-stage enrichment rollout — population validation.

Where Stage 1 tested the NEW pipeline on the 34 Session 11 audit *fails*
(selected-for-worst), Stage 2 tests the new pipeline on a RANDOM sample
from the population. The fails were data-limited — a representative sample
tells us whether the pipeline improves the typical wine.

Samples:
    30 Grade B wines chosen at random from data_grade='B'
    30 Grade C wines chosen at random from data_grade='C'
    Excludes the 80 Session 11 audit wines (already measured)

Each wine goes through the full L1+L3 loop IN MEMORY. Nothing writes to
wine_insights.

Outputs:
    data/stats/stage2_results.json — full payload
    data/stats/stage2_results.md   — markdown summary

Comparisons:
    - Against S11 population baselines (Grade B 2.65, Grade C 2.48)
    - Against Stage 1 pass-2 averages (Grade B 3.29, Grade C 1.70 — selected fails)
"""

from __future__ import annotations

import argparse
import io
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
    PRICING as AUDIT_PRICING,
    SONNET_MODEL as AUDIT_MODEL,
)

S11_AUDIT_JSON = Path("data/stats/enrichment_audit.json")
OUT_JSON = Path("data/stats/stage2_results.json")
OUT_MD = Path("data/stats/stage2_results.md")

BUDGET_LIMIT = 4.0  # hard stop (session has ~$3 left after stage 1)

# Baselines to compare against
S11_BASELINE_B = 2.65
S11_BASELINE_C = 2.48


def load_s11_wine_ids() -> set[str]:
    """Return the wine_ids audited in Session 11 — to exclude from Stage 2 sampling."""
    if not S11_AUDIT_JSON.exists():
        return set()
    audit = json.loads(S11_AUDIT_JSON.read_text(encoding="utf-8"))
    return {r["wine_id"] for r in audit.get("results", [])}


def sample_random_wines(cur, grade: str, n: int, exclude: set[str]) -> list[dict]:
    """Pull N random wines of a given grade that have wine_insights + aren't excluded."""
    cur.execute("""
        SELECT w.id, w.name, w.data_grade,
               COALESCE(p.name, '—') AS producer,
               COALESCE(a.name, '—') AS appellation,
               COALESCE(r.name, '—') AS region,
               COALESCE(c.name, '—') AS country,
               COALESCE(w.color::text, '—') AS color,
               COALESCE(w.wine_type::text, '—') AS wine_type
        FROM wines w
        JOIN wine_insights wi ON wi.wine_id = w.id
        LEFT JOIN producers p ON w.producer_id = p.id
        LEFT JOIN appellations a ON w.appellation_id = a.id
        LEFT JOIN regions r ON w.region_id = r.id
        LEFT JOIN countries c ON w.country_id = c.id
        WHERE w.duplicate_of IS NULL
          AND w.deleted_at IS NULL
          AND w.data_grade = %s
          AND wi.ai_hook IS NOT NULL
        ORDER BY random()
        LIMIT %s
    """, (grade, n + len(exclude)))
    rows = cur.fetchall()
    cols = [d[0] for d in cur.description]
    out = []
    for r in rows:
        row = dict(zip(cols, r))
        if str(row["id"]) in exclude:
            continue
        out.append(row)
        if len(out) >= n:
            break
    return out


def reshape_for_audit(wine_id: str, enrichment: dict, db_facts: dict) -> dict:
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


def main() -> int:
    parser = argparse.ArgumentParser(description="Stage 2 enrichment population validation")
    parser.add_argument("--n-b", type=int, default=30, help="Grade B sample size")
    parser.add_argument("--n-c", type=int, default=30, help="Grade C sample size")
    parser.add_argument("--dry-run", action="store_true", help="List wines without generating")
    args = parser.parse_args()

    if os.name == "nt":
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

    print(f"\n{'='*60}")
    print(f"  STAGE 2 POPULATION VALIDATION")
    print(f"  {datetime.now(timezone.utc).isoformat()}")
    print(f"{'='*60}\n")

    s11_ids = load_s11_wine_ids()
    print(f"  Excluding {len(s11_ids)} Session 11 audit wines from sample\n")

    conn = get_conn()
    cur = conn.cursor()

    print(f"  Sampling {args.n_b} Grade B + {args.n_c} Grade C random wines...")
    samples_b = sample_random_wines(cur, "B", args.n_b, s11_ids)
    samples_c = sample_random_wines(cur, "C", args.n_c, s11_ids)
    samples = [(s, "B") for s in samples_b] + [(s, "C") for s in samples_c]
    print(f"  Got {len(samples_b)} B + {len(samples_c)} C (target {args.n_b}+{args.n_c})")
    print(f"  Models:  B → {SONNET_MODEL}    C → {HAIKU_MODEL}")
    print(f"  Budget:  ${BUDGET_LIMIT:.2f}")
    print()

    if args.dry_run:
        for sample, grade in samples:
            name = (sample.get("name") or "?")[:40]
            prod = (sample.get("producer") or "?")[:25]
            print(f"  [{grade}] {str(sample['id'])[:8]} {prod} — {name}")
        cur.close()
        conn.close()
        return 0

    client = anthropic.Anthropic()

    results: list[dict] = []
    total_gen_cost = 0.0
    total_input_tokens = 0
    total_output_tokens = 0

    for i, (sample, grade) in enumerate(samples, 1):
        wine_id = str(sample["id"])
        prefix = f"[{i}/{len(samples)}] {grade} {wine_id[:8]}"
        print(f"  {prefix} fetch facts...", end=" ", flush=True)

        facts = build_facts_packet(cur, wine_id)
        if not facts:
            print("MISSING")
            results.append({"wine_id": wine_id, "grade": grade, "error": "wine not found"})
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
            "error": err,
            "_db_facts": {
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
            print(f"\n  BUDGET EXCEEDED (${total_gen_cost:.2f} > ${BUDGET_LIMIT}). STOP.")
            break

    # ── Re-audit ────────────────────────────────────────────────────────
    print(f"\n  Re-auditing with {AUDIT_MODEL}...")
    sys.stdout.flush()

    grade_b_samples = []
    grade_c_samples = []
    for r in results:
        if r.get("error") or not r.get("enrichment"):
            continue
        sample = reshape_for_audit(r["wine_id"], r["enrichment"], r["_db_facts"])
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

    audit_by_wine = {a["wine_id"]: a for a in all_audits}
    for r in results:
        wid = r["wine_id"]
        a = audit_by_wine.get(wid)
        if not a:
            continue
        audit = a.get("audit") or {}
        overall = audit.get("overall") if isinstance(audit.get("overall"), dict) else {}
        r["stage2_score"] = overall.get("score")
        r["stage2_verdict"] = overall.get("verdict")
        r["stage2_summary"] = overall.get("summary", "")
        r["stage2_audit_full"] = audit

    # ── Aggregates ──────────────────────────────────────────────────────
    by_grade = {}
    for grade, baseline in (("B", S11_BASELINE_B), ("C", S11_BASELINE_C)):
        rows = [r for r in results if r["grade"] == grade and not r.get("error")]
        scored = [r for r in rows if r.get("stage2_score") is not None]
        avg_new = round(sum(r["stage2_score"] for r in scored) / len(scored), 2) if scored else None
        fails = len([r for r in rows if r.get("stage2_verdict") == "fail"])
        warns = len([r for r in rows if r.get("stage2_verdict") == "warn"])
        passes = len([r for r in rows if r.get("stage2_verdict") == "pass"])

        fc_pass = len([r for r in rows if r.get("fact_check_status") == "passed"])
        fc_retry = len([r for r in rows if r.get("fact_check_status") == "retried_passed"])
        fc_partial = len([r for r in rows if r.get("fact_check_status") == "partial"])
        fc_failed = len([r for r in rows if r.get("fact_check_status") == "failed"])

        by_grade[grade] = {
            "n": len(rows),
            "s11_baseline": baseline,
            "stage2_avg": avg_new,
            "delta_vs_s11": round(avg_new - baseline, 2) if avg_new else None,
            "verdicts": {"pass": passes, "warn": warns, "fail": fails},
            "fact_check": {
                "passed": fc_pass,
                "retried_passed": fc_retry,
                "partial": fc_partial,
                "failed": fc_failed,
            },
        }

    total_cost = total_gen_cost + audit_cost

    payload = {
        "run_date": datetime.now(timezone.utc).isoformat(),
        "models": {"grade_b": SONNET_MODEL, "grade_c": HAIKU_MODEL, "audit": AUDIT_MODEL},
        "n_wines": len(results),
        "sample_sizes": {"B": args.n_b, "C": args.n_c},
        "by_grade": by_grade,
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
    print(f"  STAGE 2 COMPLETE")
    for g in ("B", "C"):
        b = by_grade.get(g)
        if not b:
            continue
        d = b["delta_vs_s11"]
        print(f"  Grade {g}: {b['stage2_avg']}/5 (S11 baseline {b['s11_baseline']}, delta {'+' if d and d > 0 else ''}{d})")
        print(f"            pass/warn/fail = {b['verdicts']['pass']}/{b['verdicts']['warn']}/{b['verdicts']['fail']}")
    print(f"  Cost:   ${total_cost:.4f} (gen ${total_gen_cost:.4f} + audit ${audit_cost:.4f})")
    print(f"  Output: {OUT_JSON} / {OUT_MD}")
    print(f"{'='*60}\n")

    cur.close()
    conn.close()
    return 0


def write_markdown(payload: dict) -> None:
    lines = ["# Stage 2 Enrichment Population Validation\n"]
    lines.append(f"**Run:** {payload['run_date']}")
    lines.append(f"**Models:** Grade B → `{payload['models']['grade_b']}`, Grade C → `{payload['models']['grade_c']}`")
    lines.append(f"**Auditor:** `{payload['models']['audit']}`")
    lines.append(f"**Wines tested:** {payload['n_wines']} (target {payload['sample_sizes']['B']}B + {payload['sample_sizes']['C']}C)")
    lines.append(f"**Cost:** ${payload['cost_usd']['total']:.4f} (gen ${payload['cost_usd']['generation_l1_l3']:.4f} + audit ${payload['cost_usd']['audit']:.4f})")
    lines.append("")
    lines.append("## Results vs S11 baselines\n")
    lines.append("| Grade | N | S11 baseline | Stage 2 avg | Δ | pass/warn/fail |")
    lines.append("|---|---:|---:|---:|---:|---:|")
    for g in ("B", "C"):
        b = payload["by_grade"].get(g)
        if not b:
            continue
        d = b["delta_vs_s11"]
        v = b["verdicts"]
        lines.append(
            f"| {g} | {b['n']} | {b['s11_baseline']} | **{b['stage2_avg']}** | "
            f"{'+' if d and d > 0 else ''}{d} | {v['pass']}/{v['warn']}/{v['fail']} |"
        )
    lines.append("")
    lines.append("## Fact-check status\n")
    lines.append("| Grade | passed | retried_passed | partial | failed |")
    lines.append("|---|---:|---:|---:|---:|")
    for g in ("B", "C"):
        b = payload["by_grade"].get(g)
        if not b:
            continue
        fc = b["fact_check"]
        lines.append(
            f"| {g} | {fc['passed']} | {fc['retried_passed']} | "
            f"{fc['partial']} | {fc['failed']} |"
        )
    lines.append("")

    lines.append("## Per-wine results\n")
    lines.append("| Grade | Producer — Wine | Score | Verdict | Status | Flags |")
    lines.append("|---|---|---:|---|---|---:|")
    for r in sorted(payload["results"], key=lambda r: (r.get("grade", ""), -(r.get("stage2_score") or 0))):
        if r.get("error"):
            lines.append(f"| {r['grade']} | ERROR | — | — | — | — |")
            continue
        prod = (r.get("producer") or "")[:22]
        wine = (r.get("display_name") or "")[:36]
        score = r.get("stage2_score") or "—"
        verdict = r.get("stage2_verdict") or "—"
        status = r.get("fact_check_status") or "—"
        n_flags = len(r.get("fact_check_flags") or [])
        lines.append(f"| {r['grade']} | {prod} — {wine} | {score} | {verdict} | {status} | {n_flags} |")

    OUT_MD.parent.mkdir(parents=True, exist_ok=True)
    OUT_MD.write_text("\n".join(lines), encoding="utf-8")


if __name__ == "__main__":
    sys.exit(main())
