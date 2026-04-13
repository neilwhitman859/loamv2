#!/usr/bin/env python3
"""L1 enrichment validation — 10-sample test.

Goal: validate the three-layer enrichment design by building a minimal Layer 1
(retrieval-grounded generation) prototype and re-enriching the 10 worst audit
samples. If the new average jumps from ~2/5 to 4+/5, L1 alone is sufficient.
If it only reaches 3/5, we need L3 (fact-check pass).

This is a one-off validation, not production code. Does NOT touch wine_insights.
Writes results to data/stats/l1_test_{run_date}.json.

Key differences from the current enrich-wine Edge Function:
1. Structured "facts packet" compiled from the DB (not freeform context)
2. Explicit "do not invent" instructions
3. Comparable wines pre-fetched from our own DB (no model fabrication)
4. Known unknowns listed explicitly (soil, vine age, etc.)
5. appellation_rules legal text included when available

Usage:
    python -m pipeline.enrich.l1_test
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
from pipeline.lib.models import SONNET_MODEL
PRICING = {"input": 3.0, "output": 15.0}  # per 1M

OUT_JSON = Path("data/stats/l1_test.json")
OUT_MD = Path("data/stats/l1_test.md")

# The 10 worst wines from enrichment_audit.json
TEST_WINES = [
    ("29d0e262-bbcb-432d-9276-f6a310e1e628", "B"),  # Frei Brothers
    ("5de74b56-2ab1-4e7b-9ab3-cbacf97cfab5", "B"),  # Henschke Julius
    ("777d8193-4c19-4271-9d77-b5393663a8a5", "B"),  # Fess Parker American Tradition
    ("8e03d24a-ffd2-4c48-9893-7ad0ae58b338", "B"),  # Cuvaison Durrell
    ("c2fb7430-4d41-485f-94c2-6128fd49daeb", "B"),  # Landmark Overlook
    ("d4a3e184-0567-4b91-a905-f186f71e5902", "B"),  # Taylor's Fine
    ("de8f4f9e-02f3-45ce-860e-1a0d6f8a02fa", "B"),  # Frank Family Chiles Valley
    ("069a7c0f-bee1-421a-95ae-59475fc40372", "C"),  # des Bosquets La Font
    ("08120386-225b-4d6b-a0d4-51383a074b55", "C"),  # Louis Latour Les Quatre Journaux
    ("12b6479f-0c67-486d-b8bd-15258a9b4d82", "C"),  # Felton Road Block 2
]


def fetch_hard_facts(cur, wine_id: str) -> dict:
    """Pull all verified facts for a single wine from the DB."""
    cur.execute("""
        SELECT
            w.name, w.display_name, w.color, w.wine_type, w.effervescence,
            w.sweetness_level, w.lwin, w.data_grade,
            p.name AS producer_name, p.year_established, p.website_url,
            a.name AS appellation, a.id AS appellation_id,
            r.name AS region, r.name AS region_name,
            c.name AS country, c.iso_code
        FROM wines w
        LEFT JOIN producers p ON w.producer_id = p.id
        LEFT JOIN appellations a ON w.appellation_id = a.id
        LEFT JOIN regions r ON w.region_id = r.id
        LEFT JOIN countries c ON w.country_id = c.id
        WHERE w.id = %s
    """, (wine_id,))
    row = cur.fetchone()
    cols = [d[0] for d in cur.description]
    facts = dict(zip(cols, row))

    # Grapes
    cur.execute("""
        SELECT g.display_name, wg.percentage
        FROM wine_grapes wg JOIN grapes g ON wg.grape_id = g.id
        WHERE wg.wine_id = %s
        ORDER BY wg.percentage DESC NULLS LAST
    """, (wine_id,))
    facts["grapes"] = [
        {"name": r[0], "percentage": float(r[1]) if r[1] else None}
        for r in cur.fetchall()
    ]

    # Latest 3 vintages + chemistry
    cur.execute("""
        SELECT vintage_year, abv, ph, ta_g_l, rs_g_l,
               duration_in_oak_months, new_oak_pct, oak_origin,
               fermentation_vessel, yeast_type, mlf, closure,
               cases_produced, release_price_usd, winemaker_notes
        FROM wine_vintages
        WHERE wine_id = %s AND deleted_at IS NULL
        ORDER BY vintage_year DESC NULLS LAST
        LIMIT 3
    """, (wine_id,))
    vintage_cols = [d[0] for d in cur.description]
    facts["vintages"] = [dict(zip(vintage_cols, r)) for r in cur.fetchall()]

    # Appellation rules text (if available)
    facts["appellation_rules"] = None
    if facts.get("appellation_id"):
        cur.execute("""
            SELECT source_text_excerpt, source_url, source_document_title,
                   source_organization, rules
            FROM appellation_rules
            WHERE appellation_id = %s
            ORDER BY last_verified_at DESC NULLS LAST
            LIMIT 1
        """, (facts["appellation_id"],))
        ar = cur.fetchone()
        if ar:
            facts["appellation_rules"] = {
                "excerpt": ar[0],
                "url": ar[1],
                "title": ar[2],
                "organization": ar[3],
                "rules_json": ar[4],
            }

    # Farming certifications
    cur.execute("""
        SELECT fc.name FROM wine_farming_certifications wfc
        JOIN farming_certifications fc ON wfc.farming_certification_id = fc.id
        WHERE wfc.wine_id = %s
    """, (wine_id,))
    facts["certifications"] = [r[0] for r in cur.fetchall()]

    # Scores
    cur.execute("""
        SELECT pub.name, wvs.score, wvs.score_scale, wvs.vintage_year, wvs.medal
        FROM wine_vintage_scores wvs
        LEFT JOIN publications pub ON wvs.publication_id = pub.id
        WHERE wvs.wine_id = %s
        ORDER BY wvs.score DESC NULLS LAST
        LIMIT 5
    """, (wine_id,))
    facts["scores"] = [
        {"publication": r[0] or "Unknown", "score": r[1], "scale": r[2], "vintage": r[3], "medal": r[4]}
        for r in cur.fetchall()
    ]

    # Prices (range)
    cur.execute("""
        SELECT min(price_usd), avg(price_usd), max(price_usd), count(*)
        FROM wine_vintage_prices
        WHERE wine_id = %s AND price_usd IS NOT NULL
    """, (wine_id,))
    r = cur.fetchone()
    if r and r[3]:
        facts["price_range"] = {
            "min_usd": float(r[0]) if r[0] else None,
            "avg_usd": round(float(r[1]), 2) if r[1] else None,
            "max_usd": float(r[2]) if r[2] else None,
            "count": r[3],
        }
    else:
        facts["price_range"] = None

    return facts


def fetch_comparable_wines(cur, wine_id: str, limit: int = 3) -> list:
    """Pre-fetch 3 comparable wines from OUR OWN DB for the comparable_wines field.
    Similarity criteria: shares primary grape + similar appellation/region + similar price."""
    cur.execute("""
        WITH target AS (
            SELECT w.id, w.appellation_id, w.country_id,
                   (SELECT grape_id FROM wine_grapes WHERE wine_id = w.id ORDER BY percentage DESC NULLS LAST LIMIT 1) AS primary_grape_id,
                   (SELECT avg(price_usd) FROM wine_vintage_prices WHERE wine_id = w.id AND price_usd IS NOT NULL) AS target_price
            FROM wines w WHERE w.id = %s
        )
        SELECT DISTINCT ON (w.id)
            w.id, w.display_name, p.name AS producer, a.name AS appellation,
            (SELECT g.display_name FROM wine_grapes wg2 JOIN grapes g ON wg2.grape_id = g.id
             WHERE wg2.wine_id = w.id ORDER BY wg2.percentage DESC NULLS LAST LIMIT 1) AS primary_grape,
            (SELECT avg(price_usd) FROM wine_vintage_prices WHERE wine_id = w.id AND price_usd IS NOT NULL) AS avg_price
        FROM wines w
        JOIN wine_grapes wg ON wg.wine_id = w.id
        LEFT JOIN producers p ON w.producer_id = p.id
        LEFT JOIN appellations a ON w.appellation_id = a.id,
             target t
        WHERE w.id != t.id
          AND w.deleted_at IS NULL
          AND wg.grape_id = t.primary_grape_id
        ORDER BY w.id,
                 (w.appellation_id = t.appellation_id)::int DESC,
                 (w.country_id = t.country_id)::int DESC
        LIMIT %s
    """, (wine_id, limit))
    return [
        {"wine_id": str(r[0]), "display_name": r[1], "producer": r[2],
         "appellation": r[3], "grape": r[4], "avg_price": float(r[5]) if r[5] else None}
        for r in cur.fetchall()
    ]


def build_prompt(facts: dict, comparables: list, grade: str) -> str:
    """L1 prompt — facts packet + explicit do-not-invent instructions."""
    g_list = ", ".join(
        f"{g['name']} ({g['percentage']}%)" if g["percentage"] else g["name"]
        for g in facts["grapes"]
    ) or "Not documented"

    vintage_lines = []
    for v in facts["vintages"]:
        if not v:
            continue
        parts = [f"Vintage {v.get('vintage_year') or 'NV'}"]
        if v.get("abv"): parts.append(f"ABV {v['abv']}%")
        if v.get("ph"): parts.append(f"pH {v['ph']}")
        if v.get("ta_g_l"): parts.append(f"TA {v['ta_g_l']} g/L")
        if v.get("rs_g_l") is not None: parts.append(f"RS {v['rs_g_l']} g/L")
        if v.get("fermentation_vessel"): parts.append(f"ferment: {v['fermentation_vessel']}")
        if v.get("duration_in_oak_months"): parts.append(f"oak: {v['duration_in_oak_months']} mo")
        if v.get("new_oak_pct"): parts.append(f"{v['new_oak_pct']}% new oak")
        if v.get("oak_origin"): parts.append(f"{v['oak_origin']} oak")
        if v.get("mlf") is not None: parts.append("MLF" if v["mlf"] else "no MLF")
        if v.get("closure"): parts.append(f"closure: {v['closure']}")
        if v.get("cases_produced"): parts.append(f"{v['cases_produced']} cases")
        if v.get("winemaker_notes"): parts.append(f"notes: {v['winemaker_notes'][:200]}")
        vintage_lines.append(", ".join(parts))
    vintage_block = "\n".join(vintage_lines) if vintage_lines else "Not documented — no vintage-level chemistry or winemaking data in our DB for this wine."

    # Appellation rules block
    ar = facts.get("appellation_rules")
    if ar:
        ar_block = f"**Legal source:** {ar.get('title') or 'appellation rules'} ({ar.get('organization') or 'unknown organization'})\n"
        if ar.get("url"):
            ar_block += f"**URL:** {ar['url']}\n"
        if ar.get("excerpt"):
            ar_block += f"**Text:** {ar['excerpt'][:1000]}\n"
        if ar.get("rules_json"):
            rj = ar["rules_json"]
            if isinstance(rj, dict):
                if rj.get("colors"): ar_block += f"**Legal colors:** {rj['colors']}\n"
                if rj.get("grape_rules"): ar_block += f"**Grape rules:** {json.dumps(rj['grape_rules'])[:500]}\n"
                if rj.get("aging_requirements"): ar_block += f"**Aging:** {json.dumps(rj['aging_requirements'])[:300]}\n"
    else:
        ar_block = "Not documented — no legal appellation rules in our DB for this wine's appellation. Describe only the general stylistic tendency of this appellation based on widely-known facts; do NOT invent specific legal requirements."

    # Scores block
    if facts["scores"]:
        score_block = "\n".join(
            f"- {s['publication']}: {s['score']}/{s['scale']} ({s['vintage'] or 'NV'})"
            + (f" ({s['medal']} medal)" if s['medal'] else "")
            for s in facts["scores"]
        )
    else:
        score_block = "Not documented — no critic scores in our DB for this wine."

    # Price
    if facts.get("price_range"):
        pr = facts["price_range"]
        price_block = f"${pr['min_usd']:.0f}–${pr['max_usd']:.0f} (avg ${pr['avg_usd']:.0f}, {pr['count']} merchant listings)" if pr['min_usd'] else "Not documented"
    else:
        price_block = "Not documented"

    # Comparables block
    if comparables:
        comp_lines = [
            f"- {c['display_name']} — {c['grape'] or '?'}" + (f", ~${c['avg_price']:.0f}" if c['avg_price'] else "")
            for c in comparables
        ]
        comp_block = "\n".join(comp_lines)
        comp_instruction = (
            "IMPORTANT: for ai_comparable_wines, pick 2-3 wines ONLY from this list. "
            "Do NOT invent or reference wines that are not in this list. For each chosen wine, "
            "write one sentence on why it's comparable to the target wine."
        )
    else:
        comp_block = "No comparable wines found in our catalog with the same primary grape."
        comp_instruction = "For ai_comparable_wines, write 'No close comparables in the Loam catalog yet.' Do NOT invent wines."

    # Unknown list — things we don't know and the model must not invent
    unknowns = []
    if not any(v.get("abv") for v in facts["vintages"]):
        unknowns.append("ABV")
    if not any(v.get("ph") for v in facts["vintages"]):
        unknowns.append("pH")
    if not any(v.get("ta_g_l") for v in facts["vintages"]):
        unknowns.append("TA")
    if not any(v.get("rs_g_l") is not None for v in facts["vintages"]):
        unknowns.append("residual sugar")
    if not any(v.get("fermentation_vessel") for v in facts["vintages"]):
        unknowns.append("fermentation vessel")
    if not any(v.get("duration_in_oak_months") for v in facts["vintages"]):
        unknowns.append("oak aging duration")
    if not any(v.get("oak_origin") for v in facts["vintages"]):
        unknowns.append("oak origin (French/American/etc.)")
    if not any(v.get("new_oak_pct") for v in facts["vintages"]):
        unknowns.append("new oak percentage")
    if not any(v.get("mlf") is not None for v in facts["vintages"]):
        unknowns.append("malolactic fermentation")
    if not any(v.get("closure") for v in facts["vintages"]):
        unknowns.append("closure (cork/screwcap)")
    if not any(v.get("cases_produced") for v in facts["vintages"]):
        unknowns.append("annual production volume")
    if not any(v.get("winemaker_notes") for v in facts["vintages"]):
        unknowns.append("winemaker notes / vinification details")
    unknowns_block = "\n".join(f"- {u}" for u in unknowns) if unknowns else "(all core chemistry fields are documented)"

    # Grade-specific field list
    if grade == "B":
        fields_spec = """{
  "hook": "2-3 sentences. The 30-second story, specific and factual.",
  "wine_summary": "1-2 paragraphs. What makes the producer and wine matter. Use only verified facts and widely-known appellation-level truths.",
  "style_profile": "One phrase. E.g., 'Full-bodied dry red with firm tannins and earthy depth.'",
  "terroir_expression": "1 paragraph. Draw ONLY from the appellation legal text and verified facts. If soil type is unknown, say so.",
  "food_pairing": "3-5 specific dishes with brief reasoning.",
  "cellar_recommendation": "1-2 sentences. If drinking window is unknown, say 'Producer does not publish a specific window.'",
  "comparable_wines": "2-3 wines from the list below, one sentence each.",
  "vinification_summary": "1 paragraph. If winemaker notes are unknown, describe only what is typical for the appellation (citing the legal rule) and label it as category-level."
}"""
    else:  # Grade C — catalog
        fields_spec = """{
  "hook": "2-3 sentences.",
  "style_profile": "One phrase.",
  "comparable_wines": "2-3 wines from the list below, one sentence each."
}"""

    return f"""You are writing a catalog entry for a wine. Your writing is direct, specific, and knowledgeable — no generic filler, no sommelier theater, no poetic language, no vague hedging.

**CRITICAL RULE:** Use ONLY the facts provided below. For any field marked "Not documented" or listed under UNKNOWNS, do NOT invent. Either omit the claim, or explicitly say "Not documented."

**SAFE INFERENCES ALLOWED:**
- Grape → typical flavor profile (e.g., Nebbiolo brings tar and rose)
- Appellation → broad stylistic tendency from widely-known facts
- Sugar level + acidity → food-pairing logic
- Documented oak → mouthfeel implications

**NEVER INVENT:**
- Specific soil types (unless in appellation rules)
- Specific ABV, pH, or other numeric values
- Specific vineyard aspects, elevations, plantings
- Specific vinification details not in the facts
- Wines that are not in the comparable list below

---

## WINE IDENTITY
- Display name: {facts['display_name']}
- Producer: {facts['producer_name']}
- Appellation: {facts['appellation'] or 'None — no appellation assigned'}
- Region: {facts['region_name'] or 'None'}
- Country: {facts['country']}
- Color: {facts['color'] or 'Not documented'}
- Wine type: {facts['wine_type'] or 'table'}
- Sweetness level: {facts['sweetness_level'] or 'Not documented'}
- LWIN: {facts.get('lwin') or 'None'}
- Grapes: {g_list}
- Farming certifications: {', '.join(facts['certifications']) if facts['certifications'] else 'None documented'}

## VINTAGE DATA (latest 3)
{vintage_block}

## APPELLATION LAW (ground truth for terroir claims)
{ar_block}

## CRITIC SCORES
{score_block}

## PRICE RANGE (from our merchant data)
{price_block}

## UNKNOWN FIELDS (DO NOT INVENT)
{unknowns_block}

## COMPARABLE WINES (only pick from this list)
{comp_block}

{comp_instruction}

---

## OUTPUT FORMAT

Return ONLY a JSON object matching this schema. No markdown fences, no preamble.

{fields_spec}
"""


def call_sonnet(client, prompt: str) -> tuple[dict | None, dict, str | None]:
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


def main():
    if os.name == "nt":
        import io
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

    print(f"\n{'='*60}")
    print(f"  L1 ENRICHMENT VALIDATION (10 worst audit samples)")
    print(f"  {datetime.now(timezone.utc).isoformat()}")
    print(f"{'='*60}\n")

    conn = get_conn()
    cur = conn.cursor()
    client = anthropic.Anthropic()

    results = []
    total_input = 0
    total_output = 0

    for i, (wine_id, grade) in enumerate(TEST_WINES, 1):
        print(f"[{i}/{len(TEST_WINES)}] Grade {grade} {wine_id[:8]}...", end=" ")
        sys.stdout.flush()

        facts = fetch_hard_facts(cur, wine_id)
        comparables = fetch_comparable_wines(cur, wine_id, limit=3)
        prompt = build_prompt(facts, comparables, grade)

        parsed, usage, err = call_sonnet(client, prompt)

        if err:
            print(f"ERROR: {err}")
        else:
            hook_preview = (parsed.get("hook") or "")[:80]
            print(f"OK ({usage.get('input_tokens',0)}/{usage.get('output_tokens',0)} tok)")
            print(f"    hook: {hook_preview}")

        total_input += usage.get("input_tokens", 0)
        total_output += usage.get("output_tokens", 0)

        results.append({
            "wine_id": wine_id,
            "grade": grade,
            "display_name": facts["display_name"],
            "producer": facts["producer_name"],
            "appellation": facts["appellation"],
            "country": facts["country"],
            "facts": {
                "n_grapes": len(facts["grapes"]),
                "n_vintages": len(facts["vintages"]),
                "has_appellation_rules": facts["appellation_rules"] is not None,
                "n_scores": len(facts["scores"]),
                "has_price": facts["price_range"] is not None,
                "n_comparables": len(comparables),
            },
            "l1_enrichment": parsed,
            "usage": usage,
            "error": err,
            "prompt_length": len(prompt),
        })

    cost = (total_input * PRICING["input"] + total_output * PRICING["output"]) / 1_000_000

    payload = {
        "run_date": datetime.now(timezone.utc).isoformat(),
        "model": SONNET_MODEL,
        "wines_tested": len(TEST_WINES),
        "total_input_tokens": total_input,
        "total_output_tokens": total_output,
        "cost_usd": round(cost, 4),
        "results": results,
    }

    OUT_JSON.parent.mkdir(parents=True, exist_ok=True)
    OUT_JSON.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")

    print(f"\n{'='*60}")
    print(f"  L1 TEST COMPLETE")
    print(f"  Cost: ${cost:.4f}")
    print(f"  Output: {OUT_JSON}")
    print(f"{'='*60}\n")

    cur.close()
    conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
