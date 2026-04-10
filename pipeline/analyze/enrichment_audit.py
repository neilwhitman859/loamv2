#!/usr/bin/env python3
"""Enrichment quality audit — sample-based voice/quality check.

Samples random Grade C and Grade B enriched wines and asks Sonnet to grade
each one against the voice rules in docs/VOICE.md. Flags systematic issues
like generic filler, sommelier theater, factual errors, and voice drift.

Usage:
    # Default: 50 Grade C + 20 Grade B sample
    python -m pipeline.analyze.enrichment_audit

    # Custom sample sizes
    python -m pipeline.analyze.enrichment_audit --grade-c 30 --grade-b 10

    # Reproducible
    python -m pipeline.analyze.enrichment_audit --seed 42

    # Dry run — pull samples but don't call Sonnet
    python -m pipeline.analyze.enrichment_audit --dry-run

Outputs:
    data/stats/enrichment_audit.json — full audit results
    data/stats/enrichment_audit.md   — human-readable summary
"""

import argparse
import json
import os
import random
import sys
from datetime import datetime, timezone
from pathlib import Path

os.environ["PYTHONUNBUFFERED"] = "1"
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from dotenv import load_dotenv
load_dotenv(override=True)

import anthropic

from pipeline.lib.db import get_conn

SONNET_MODEL = "claude-sonnet-4-6"
PRICING = {"input": 3.0, "output": 15.0}  # per 1M tokens

OUT_JSON = Path("data/stats/enrichment_audit.json")
OUT_MD = Path("data/stats/enrichment_audit.md")
VOICE_GUIDE = Path("docs/VOICE.md")


# ── Voice rules excerpt (kept short to control prompt size) ──

VOICE_RULES = """\
LOAM VOICE RULES (excerpt — see docs/VOICE.md):

DO:
- Be specific. Name soils, climates, grapes, producers.
- Connect place to taste. Draw the line from geology to flavor.
- State what you know directly. Have a point of view.
- Be honest about what you don't know — say so plainly.
- Give real information, not atmosphere. Every sentence teaches or helps a decision.
- Be comfortable saying negative things, matter-of-fact.
- Include market and value perspective.

DON'T:
- Generic filler ("creating something unique", "remarkable quality", "reflects the terroir")
- Sommelier theater ("exquisite accompaniment", "pairs beautifully", "elevates the dining experience")
- Vague hedging ("seems to", "appears to", "may benefit")
- Overly poetic language ("dance between traditional and modern", "amphitheater bathed at its feet")
- Performative enthusiasm (exclamation marks, "Where to begin?!")

FOOD PAIRINGS (when present):
- Name real food. Specific dishes ("prosciutto and melon" not "delicate stone fruit and charcuterie")
- Lead with classics (Sancerre + goat cheese, Chianti + tomato pasta)
- Cover the full table — Tuesday night meal AND Saturday dinner
- Explain the why (acid cuts fat, RS tames chili heat)
"""


GRADE_C_AUDIT_PROMPT = """\
You are auditing a Grade C (Haiku-generated catalog enrichment) wine entry against Loam's voice guide.

WINE: {producer} — {wine_name}
APPELLATION: {appellation} ({region}, {country})
GRAPES: {grapes}
COLOR/TYPE: {color} / {wine_type}

ENRICHMENT FIELDS TO AUDIT:

ai_hook (1-2 sentences, the headline):
"{hook}"

ai_style_profile (short structured tag):
"{style}"

ai_comparable_wines (similar wines a buyer might also like):
"{comparable}"

{voice_rules}

Rate each of the three fields on these dimensions (1-5, 5=best):
- specificity: Does it name specific things (soils, places, producers, grapes) vs generic filler?
- voice: Does it follow Loam's voice (direct, knowledgeable, no theater, no hedging)?
- accuracy: Is what it says factually defensible? (Use "uncertain" if you can't tell)

Then flag any issues using these tags:
- generic_filler — vague phrases that say nothing
- sommelier_theater — flowery food pairing language
- vague_hedging — "seems to", "may benefit"
- poetic — overly artistic language
- factual_error — something demonstrably wrong
- voice_drift — wrong persona, sounds AI-generic
- ok — no issues found

Return ONLY a JSON object:
{{
  "hook":      {{"specificity": N, "voice": N, "accuracy": N, "issues": ["tag", ...], "note": "one sentence"}},
  "style":     {{"specificity": N, "voice": N, "accuracy": N, "issues": ["tag", ...], "note": "one sentence"}},
  "comparable":{{"specificity": N, "voice": N, "accuracy": N, "issues": ["tag", ...], "note": "one sentence"}},
  "overall":   {{"score": N, "verdict": "pass|warn|fail", "summary": "one sentence"}}
}}
"""


GRADE_B_AUDIT_PROMPT = """\
You are auditing a Grade B (Sonnet-generated narrative enrichment) wine entry against Loam's voice guide.

WINE: {producer} — {wine_name}
APPELLATION: {appellation} ({region}, {country})
GRAPES: {grapes}
COLOR/TYPE: {color} / {wine_type}

ENRICHMENT FIELDS TO AUDIT:

ai_hook:
"{hook}"

ai_wine_summary:
"{summary}"

ai_style_profile:
"{style}"

ai_terroir_expression:
"{terroir}"

ai_food_pairing:
"{food}"

ai_cellar_recommendation:
"{cellar}"

ai_comparable_wines:
"{comparable}"

ai_vinification_summary:
"{vinification}"

{voice_rules}

Rate each of the eight fields on these dimensions (1-5, 5=best):
- specificity: Does it name specific things vs generic filler?
- voice: Does it follow Loam's voice?
- accuracy: Is what it says factually defensible?

Flag any issues using these tags:
- generic_filler / sommelier_theater / vague_hedging / poetic / factual_error / voice_drift / ok

Return ONLY a JSON object:
{{
  "hook":         {{"specificity": N, "voice": N, "accuracy": N, "issues": [...], "note": "..."}},
  "summary":      {{"specificity": N, "voice": N, "accuracy": N, "issues": [...], "note": "..."}},
  "style":        {{"specificity": N, "voice": N, "accuracy": N, "issues": [...], "note": "..."}},
  "terroir":      {{"specificity": N, "voice": N, "accuracy": N, "issues": [...], "note": "..."}},
  "food":         {{"specificity": N, "voice": N, "accuracy": N, "issues": [...], "note": "..."}},
  "cellar":       {{"specificity": N, "voice": N, "accuracy": N, "issues": [...], "note": "..."}},
  "comparable":   {{"specificity": N, "voice": N, "accuracy": N, "issues": [...], "note": "..."}},
  "vinification": {{"specificity": N, "voice": N, "accuracy": N, "issues": [...], "note": "..."}},
  "overall":      {{"score": N, "verdict": "pass|warn|fail", "summary": "one sentence"}}
}}
"""


def fetch_sample(cur, grade: str, n: int, seed: int | None) -> list[dict]:
    """Random sample of enriched wines with all context needed for audit."""
    # Use TABLESAMPLE alternative — we want a real random sample, not deterministic.
    where_seed = ""
    if seed is not None:
        cur.execute("SELECT setseed(%s)", (seed * 0.001,))

    cur.execute(f"""
        SELECT wi.wine_id,
               w.name AS wine_name,
               COALESCE(p.name, '—') AS producer,
               COALESCE(a.name, '—') AS appellation,
               COALESCE(r.name, '—') AS region,
               COALESCE(c.name, '—') AS country,
               COALESCE(w.color::text, '—') AS color,
               COALESCE(w.wine_type::text, '—') AS wine_type,
               wi.ai_hook,
               wi.ai_wine_summary,
               wi.ai_style_profile,
               wi.ai_terroir_expression,
               wi.ai_food_pairing,
               wi.ai_cellar_recommendation,
               wi.ai_comparable_wines,
               wi.ai_vinification_summary
        FROM wine_insights wi
        JOIN wines w ON wi.wine_id = w.id
        LEFT JOIN producers p ON w.producer_id = p.id
        LEFT JOIN appellations a ON w.appellation_id = a.id
        LEFT JOIN regions r ON w.region_id = r.id
        LEFT JOIN countries c ON w.country_id = c.id
        WHERE wi.enrichment_tier = %s
          AND wi.ai_hook IS NOT NULL
        ORDER BY random()
        LIMIT %s
    """, (grade, n))

    rows = cur.fetchall()
    cols = [d[0] for d in cur.description]
    samples = []
    for r in rows:
        row = dict(zip(cols, r))
        # Pull grapes
        cur.execute("""
            SELECT g.name FROM wine_grapes wg
            JOIN grapes g ON wg.grape_id = g.id
            WHERE wg.wine_id = %s
            ORDER BY wg.percentage DESC NULLS LAST
        """, (row["wine_id"],))
        row["grapes"] = ", ".join(g[0] for g in cur.fetchall()) or "—"
        samples.append(row)
    return samples


def call_sonnet(client, prompt: str) -> tuple[dict | None, dict, str | None]:
    """Returns (parsed_json, usage, error)."""
    try:
        msg = client.messages.create(
            model=SONNET_MODEL,
            max_tokens=2000,
            messages=[{"role": "user", "content": prompt}],
        )
        raw = msg.content[0].text.strip()
        if raw.startswith("```"):
            raw = raw.split("\n", 1)[1] if "\n" in raw else raw[3:]
            if raw.endswith("```"):
                raw = raw[:-3].strip()
        parsed = json.loads(raw)
        return parsed, {
            "input_tokens": msg.usage.input_tokens,
            "output_tokens": msg.usage.output_tokens,
        }, None
    except Exception as e:
        return None, {}, str(e)[:300]


def audit_grade_c(client, samples: list[dict]) -> list[dict]:
    results = []
    for i, w in enumerate(samples, 1):
        prompt = GRADE_C_AUDIT_PROMPT.format(
            producer=w["producer"], wine_name=w["wine_name"],
            appellation=w["appellation"], region=w["region"], country=w["country"],
            grapes=w["grapes"], color=w["color"], wine_type=w["wine_type"],
            hook=(w["ai_hook"] or "")[:1000],
            style=(w["ai_style_profile"] or "")[:300],
            comparable=(w["ai_comparable_wines"] or "")[:1500],
            voice_rules=VOICE_RULES,
        )
        parsed, usage, err = call_sonnet(client, prompt)
        results.append({
            "wine_id": str(w["wine_id"]),
            "wine_name": w["wine_name"],
            "producer": w["producer"],
            "appellation": w["appellation"],
            "country": w["country"],
            "grade": "C",
            "audit": parsed,
            "usage": usage,
            "error": err,
        })
        if i % 5 == 0 or i == len(samples):
            print(f"  Grade C: {i}/{len(samples)}")
            sys.stdout.flush()
    return results


def audit_grade_b(client, samples: list[dict]) -> list[dict]:
    results = []
    for i, w in enumerate(samples, 1):
        prompt = GRADE_B_AUDIT_PROMPT.format(
            producer=w["producer"], wine_name=w["wine_name"],
            appellation=w["appellation"], region=w["region"], country=w["country"],
            grapes=w["grapes"], color=w["color"], wine_type=w["wine_type"],
            hook=(w["ai_hook"] or "—")[:1000],
            summary=(w["ai_wine_summary"] or "—")[:1500],
            style=(w["ai_style_profile"] or "—")[:300],
            terroir=(w["ai_terroir_expression"] or "—")[:1500],
            food=(w["ai_food_pairing"] or "—")[:1000],
            cellar=(w["ai_cellar_recommendation"] or "—")[:500],
            comparable=(w["ai_comparable_wines"] or "—")[:1000],
            vinification=(w["ai_vinification_summary"] or "—")[:1000],
            voice_rules=VOICE_RULES,
        )
        parsed, usage, err = call_sonnet(client, prompt)
        results.append({
            "wine_id": str(w["wine_id"]),
            "wine_name": w["wine_name"],
            "producer": w["producer"],
            "appellation": w["appellation"],
            "country": w["country"],
            "grade": "B",
            "audit": parsed,
            "usage": usage,
            "error": err,
        })
        if i % 5 == 0 or i == len(samples):
            print(f"  Grade B: {i}/{len(samples)}")
            sys.stdout.flush()
    return results


def summarize(results: list[dict]) -> dict:
    """Aggregate scores and issue tags across all audited wines."""
    total = len(results)
    parsed = [r for r in results if r.get("audit")]
    failed = [r for r in results if not r.get("audit")]

    if not parsed:
        return {"total": total, "parsed": 0, "failed": len(failed)}

    # Field-level scores
    field_scores = {}  # field -> {specificity: [...], voice: [...], accuracy: [...]}
    issue_counts = {}  # tag -> count
    overall_scores = []
    verdicts = {"pass": 0, "warn": 0, "fail": 0}

    for r in parsed:
        a = r["audit"]
        for field, vals in a.items():
            if field == "overall":
                if isinstance(vals, dict):
                    if "score" in vals:
                        try:
                            overall_scores.append(int(vals["score"]))
                        except (ValueError, TypeError):
                            pass
                    v = str(vals.get("verdict", "")).lower()
                    if v in verdicts:
                        verdicts[v] += 1
                continue
            if not isinstance(vals, dict):
                continue
            fs = field_scores.setdefault(field, {"specificity": [], "voice": [], "accuracy": []})
            for dim in ("specificity", "voice", "accuracy"):
                v = vals.get(dim)
                if isinstance(v, (int, float)):
                    fs[dim].append(v)
            for tag in vals.get("issues") or []:
                tag = str(tag).lower().strip()
                if tag and tag != "ok":
                    issue_counts[tag] = issue_counts.get(tag, 0) + 1

    field_avgs = {}
    for f, dims in field_scores.items():
        field_avgs[f] = {
            dim: round(sum(vs) / len(vs), 2) if vs else None
            for dim, vs in dims.items()
        }

    return {
        "total": total,
        "parsed": len(parsed),
        "failed": len(failed),
        "overall_avg": round(sum(overall_scores) / len(overall_scores), 2) if overall_scores else None,
        "verdicts": verdicts,
        "field_avgs": field_avgs,
        "top_issues": sorted(issue_counts.items(), key=lambda x: -x[1])[:10],
    }


def write_json(payload: dict) -> None:
    OUT_JSON.parent.mkdir(parents=True, exist_ok=True)
    OUT_JSON.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")


def write_markdown(payload: dict) -> None:
    OUT_MD.parent.mkdir(parents=True, exist_ok=True)
    lines = ["# Enrichment Quality Audit\n"]
    lines.append(f"**Run:** {payload['run_date']}  ")
    lines.append(f"**Model:** {payload['model']}  ")
    lines.append(f"**Cost:** ${payload['cost']:.4f}\n")

    for grade in ("C", "B"):
        s = payload["summary"].get(grade)
        if not s:
            continue
        lines.append(f"## Grade {grade}\n")
        lines.append(f"- Sample: {s['total']} ({s['parsed']} parsed, {s['failed']} failed)")
        if s.get("overall_avg") is not None:
            lines.append(f"- Overall avg: **{s['overall_avg']}/5**")
        v = s.get("verdicts", {})
        lines.append(f"- Verdicts: pass {v.get('pass', 0)} / warn {v.get('warn', 0)} / fail {v.get('fail', 0)}")
        lines.append("")

        if s.get("field_avgs"):
            lines.append("### Field scores (1-5)\n")
            lines.append("| Field | Specificity | Voice | Accuracy |")
            lines.append("|-------|-------------|-------|----------|")
            for field, dims in s["field_avgs"].items():
                lines.append(f"| {field} | {dims.get('specificity', '—')} | {dims.get('voice', '—')} | {dims.get('accuracy', '—')} |")
            lines.append("")

        if s.get("top_issues"):
            lines.append("### Top issues flagged\n")
            for tag, count in s["top_issues"]:
                lines.append(f"- `{tag}`: {count}")
            lines.append("")

    # Show worst-rated wines for spot-check
    lines.append("## Worst-rated samples (for spot-check)\n")
    all_results = payload.get("results", [])
    rated = [
        r for r in all_results
        if r.get("audit") and isinstance(r["audit"].get("overall"), dict) and r["audit"]["overall"].get("score")
    ]
    rated.sort(key=lambda r: r["audit"]["overall"].get("score", 5))
    for r in rated[:5]:
        a = r["audit"]["overall"]
        lines.append(f"- **Grade {r['grade']}** {r['producer']} — {r['wine_name']} ({r['country']})")
        lines.append(f"  - Score: {a.get('score')}/5 ({a.get('verdict')})")
        lines.append(f"  - {a.get('summary', '')}")
        lines.append("")

    OUT_MD.write_text("\n".join(lines), encoding="utf-8")


def main():
    parser = argparse.ArgumentParser(description="Enrichment quality audit")
    parser.add_argument("--grade-c", type=int, default=50)
    parser.add_argument("--grade-b", type=int, default=20)
    parser.add_argument("--seed", type=int, default=None)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    if os.name == "nt":
        import io
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

    print(f"\n{'='*60}")
    print(f"  ENRICHMENT QUALITY AUDIT")
    print(f"  {datetime.now(timezone.utc).isoformat()}")
    print(f"{'='*60}")
    print(f"  Grade C sample: {args.grade_c}")
    print(f"  Grade B sample: {args.grade_b}")
    if args.seed:
        print(f"  Seed: {args.seed}")
    print()

    conn = get_conn()
    cur = conn.cursor()

    print("  Pulling Grade C sample...")
    sys.stdout.flush()
    grade_c_samples = fetch_sample(cur, "C", args.grade_c, args.seed)
    print(f"    got {len(grade_c_samples)} wines")

    print("  Pulling Grade B sample...")
    sys.stdout.flush()
    grade_b_samples = fetch_sample(cur, "B", args.grade_b, args.seed)
    print(f"    got {len(grade_b_samples)} wines")

    if args.dry_run:
        print("\n  DRY-RUN — would audit these wines:")
        for w in grade_c_samples[:5]:
            print(f"    [C] {w['producer']} — {w['wine_name']}")
        if len(grade_c_samples) > 5:
            print(f"    ... and {len(grade_c_samples) - 5} more Grade C")
        for w in grade_b_samples[:5]:
            print(f"    [B] {w['producer']} — {w['wine_name']}")
        if len(grade_b_samples) > 5:
            print(f"    ... and {len(grade_b_samples) - 5} more Grade B")
        return 0

    print(f"\n  Auditing with Sonnet ({SONNET_MODEL})...")
    sys.stdout.flush()
    client = anthropic.Anthropic()

    grade_c_results = audit_grade_c(client, grade_c_samples)
    grade_b_results = audit_grade_b(client, grade_b_samples)

    all_results = grade_c_results + grade_b_results

    # Cost
    total_input = sum(r.get("usage", {}).get("input_tokens", 0) for r in all_results)
    total_output = sum(r.get("usage", {}).get("output_tokens", 0) for r in all_results)
    cost = (total_input * PRICING["input"] + total_output * PRICING["output"]) / 1_000_000

    summary = {
        "C": summarize(grade_c_results),
        "B": summarize(grade_b_results),
    }

    payload = {
        "run_date": datetime.now(timezone.utc).isoformat(),
        "model": SONNET_MODEL,
        "samples": {"grade_c": args.grade_c, "grade_b": args.grade_b},
        "tokens": {"input": total_input, "output": total_output},
        "cost": round(cost, 4),
        "summary": summary,
        "results": all_results,
    }

    write_json(payload)
    write_markdown(payload)

    print(f"\n  {'='*50}")
    print(f"  AUDIT COMPLETE")
    print(f"  Cost: ${cost:.4f}")
    print(f"  Grade C overall: {summary['C'].get('overall_avg', '—')}/5")
    print(f"  Grade B overall: {summary['B'].get('overall_avg', '—')}/5")
    print(f"  Results: {OUT_JSON}")
    print(f"  Summary: {OUT_MD}")
    print(f"  {'='*50}\n")

    cur.close()
    conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
