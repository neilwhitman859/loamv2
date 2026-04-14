#!/usr/bin/env python3
"""Batch wine enrichment: Grade C (Haiku), Grade B (Sonnet), Grade A (Sonnet + cascade).

Grade C: hook, style, sensory profile, comparable wines. ~$0.003-0.005/wine.
Grade B: full narrative (wine summary, terroir, vinification, food, cellar). ~$0.02-0.04/wine.
Grade A: full narrative + cascade context + value/market/insider. ~$0.03-0.04/wine.

Usage:
    # Dry-run (prints context, no API calls)
    python -m pipeline.enrich.batch_enrich --grade C --limit 5

    # Calibrate: enrich specific wines, print results to review
    python -m pipeline.enrich.batch_enrich --grade B --ids UUID1,UUID2 --execute --print

    # Batch: enrich top wines by score count
    python -m pipeline.enrich.batch_enrich --grade C --limit 1000 --execute --budget 5.0

    # Parallel: 8 concurrent API calls (same cost, ~5x faster)
    python -m pipeline.enrich.batch_enrich --grade A --producer UUID --limit 500 --execute --workers 8

    # Resume-safe: skips wines already at target grade or higher
    python -m pipeline.enrich.batch_enrich --grade C --execute --budget 15.0
"""

import argparse
import json
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from threading import Lock
from uuid import uuid4

# Force unbuffered output on Windows
os.environ["PYTHONUNBUFFERED"] = "1"

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import anthropic

from pipeline.lib.db import get_conn
from pipeline.lib.models import HAIKU_MODEL, SONNET_MODEL

# ── Models & pricing ──────────────────────────────────────────

# Per million tokens
PRICING = {
    HAIKU_MODEL: {"input": 0.80, "output": 4.00},
    SONNET_MODEL: {"input": 3.00, "output": 15.00},
}

GRADE_ORDER = {"F": 0, "D": 1, "C": 2, "B": 3, "A": 4}


# ── Context assembly ──────────────────────────────────────────

def assemble_context(cur, wine_id: str) -> dict:
    """Gather all available data for a wine. Single-wine mode (fallback)."""
    return _assemble_single(cur, wine_id)


def bulk_preload_context(cur, wine_ids: list[str]) -> dict[str, dict]:
    """Pre-load all context for a batch of wines in bulk queries. Much faster than per-wine."""
    if not wine_ids:
        return {}

    placeholders = ",".join(["%s"] * len(wine_ids))

    # 1. Wine + producer + geography (one query)
    cur.execute(f"""
        SELECT w.id, w.display_name, w.name, w.color, w.wine_type,
               w.sweetness_level, w.description, w.data_grade,
               w.appellation_id,
               p.name as producer_name, p.description as producer_desc,
               p.year_established, p.website_url,
               c.name as country_name,
               r.name as region_name,
               a.name as appellation_name
        FROM wines w
        JOIN producers p ON w.producer_id = p.id
        LEFT JOIN countries c ON w.country_id = c.id
        LEFT JOIN regions r ON w.region_id = r.id
        LEFT JOIN appellations a ON w.appellation_id = a.id
        WHERE w.id IN ({placeholders}) AND w.deleted_at IS NULL
    """, wine_ids)
    cols = [d[0] for d in cur.description]
    wines = {}
    for row in cur.fetchall():
        d = dict(zip(cols, row))
        wid = str(d["id"])
        wines[wid] = d
        wines[wid]["grapes"] = []
        wines[wid]["scores"] = []
        wines[wid]["vintages"] = []
        wines[wid]["prices"] = []
        wines[wid]["certs"] = []
        wines[wid]["designations"] = []
        wines[wid]["appellation_rule"] = None

    if not wines:
        return {}

    # 2. Grapes (one query)
    cur.execute(f"""
        SELECT wg.wine_id, g.name, wg.percentage
        FROM wine_grapes wg
        JOIN grapes g ON wg.grape_id = g.id
        WHERE wg.wine_id IN ({placeholders})
        ORDER BY wg.wine_id, wg.percentage DESC NULLS LAST
    """, wine_ids)
    for r in cur.fetchall():
        wid = str(r[0])
        if wid in wines:
            wines[wid]["grapes"].append({"name": r[1], "percentage": r[2]})

    # 3. Scores (one query, limited per wine via window function)
    cur.execute(f"""
        SELECT * FROM (
            SELECT s.wine_id, s.score, s.score_scale, s.medal, s.vintage_year, s.tasting_note,
                   pub.name as publication,
                   ROW_NUMBER() OVER (PARTITION BY s.wine_id ORDER BY s.score DESC NULLS LAST) as rn
            FROM wine_vintage_scores s
            LEFT JOIN publications pub ON s.publication_id = pub.id
            WHERE s.wine_id IN ({placeholders})
        ) sub WHERE rn <= 10
    """, wine_ids)
    for r in cur.fetchall():
        wid = str(r[0])
        if wid in wines:
            wines[wid]["scores"].append({
                "score": r[1], "score_scale": r[2], "medal": r[3],
                "vintage_year": r[4], "tasting_note": r[5], "publication": r[6]
            })

    # 4. Vintages (one query, limited per wine)
    cur.execute(f"""
        SELECT * FROM (
            SELECT wine_id, vintage_year, abv, ph, ta_g_l, rs_g_l,
                   fermentation_vessel, yeast_type, mlf,
                   aging_vessel, duration_in_oak_months, oak_origin, new_oak_pct,
                   closure, cases_produced, winemaker_notes,
                   ROW_NUMBER() OVER (PARTITION BY wine_id ORDER BY vintage_year DESC) as rn
            FROM wine_vintages
            WHERE wine_id IN ({placeholders})
        ) sub WHERE rn <= 5
    """, wine_ids)
    vint_cols = ["wine_id", "vintage_year", "abv", "ph", "ta_g_l", "rs_g_l",
                 "fermentation_vessel", "yeast_type", "mlf",
                 "aging_vessel", "duration_in_oak_months", "oak_origin", "new_oak_pct",
                 "closure", "cases_produced", "winemaker_notes", "rn"]
    for r in cur.fetchall():
        d = dict(zip(vint_cols, r))
        wid = str(d.pop("wine_id"))
        d.pop("rn", None)
        if wid in wines:
            wines[wid]["vintages"].append(d)

    # 5. Prices (one query, limited per wine)
    cur.execute(f"""
        SELECT * FROM (
            SELECT wine_id, price_usd, currency, merchant_name, vintage_year,
                   ROW_NUMBER() OVER (PARTITION BY wine_id ORDER BY price_usd DESC NULLS LAST) as rn
            FROM wine_vintage_prices
            WHERE wine_id IN ({placeholders})
        ) sub WHERE rn <= 5
    """, wine_ids)
    for r in cur.fetchall():
        wid = str(r[0])
        if wid in wines:
            wines[wid]["prices"].append({
                "price_usd": r[1], "currency": r[2], "merchant_name": r[3], "vintage_year": r[4]
            })

    # 6. Appellation rules (one query for all unique appellations)
    app_ids = [str(w["appellation_id"]) for w in wines.values() if w.get("appellation_id")]
    if app_ids:
        app_ph = ",".join(["%s"] * len(app_ids))
        cur.execute(f"""
            SELECT appellation_id, rules FROM appellation_rules
            WHERE appellation_id IN ({app_ph})
        """, app_ids)
        app_rules = {str(r[0]): r[1] for r in cur.fetchall()}
        for w in wines.values():
            aid = str(w.get("appellation_id", ""))
            if aid in app_rules:
                w["appellation_rule"] = app_rules[aid]

    # 7. Farming certifications (one query)
    cur.execute(f"""
        SELECT wfc.wine_id, fc.name
        FROM wine_farming_certifications wfc
        JOIN farming_certifications fc ON wfc.farming_certification_id = fc.id
        WHERE wfc.wine_id IN ({placeholders})
    """, wine_ids)
    for r in cur.fetchall():
        wid = str(r[0])
        if wid in wines:
            wines[wid]["certs"].append(r[1])

    # 8. Label designations (one query)
    cur.execute(f"""
        SELECT wld.wine_id, ld.canonical_name
        FROM wine_label_designations wld
        JOIN label_designations ld ON wld.label_designation_id = ld.id
        WHERE wld.wine_id IN ({placeholders})
    """, wine_ids)
    for r in cur.fetchall():
        wid = str(r[0])
        if wid in wines:
            wines[wid]["designations"].append(r[1])

    # 9. Cascade context: appellation insights
    if app_ids:
        cur.execute(f"""
            SELECT appellation_id, ai_overview, ai_soil_profile
            FROM appellation_insights
            WHERE appellation_id IN ({app_ph})
              AND ai_overview IS NOT NULL
        """, app_ids)
        app_insights = {str(r[0]): {"overview": r[1], "soil": r[2]} for r in cur.fetchall()}
        for w in wines.values():
            aid = str(w.get("appellation_id", ""))
            w["appellation_insight"] = app_insights.get(aid)

    # 10. Cascade context: producer insights (one query for all unique producers)
    producer_ids = list(set(str(w.get("producer_id", w.get("_producer_id", "")))
                           for w in wines.values()))
    # We need producer_id — it's in the wine row but not named. Re-fetch.
    cur.execute(f"""
        SELECT DISTINCT w.producer_id FROM wines w WHERE w.id IN ({placeholders})
    """, wine_ids)
    producer_ids = [str(r[0]) for r in cur.fetchall() if r[0]]
    if producer_ids:
        prod_ph = ",".join(["%s"] * len(producer_ids))
        cur.execute(f"""
            SELECT producer_id, ai_overview, ai_winemaking_style
            FROM producer_insights
            WHERE producer_id IN ({prod_ph})
              AND ai_overview IS NOT NULL
        """, producer_ids)
        prod_insights = {str(r[0]): {"overview": r[1], "style": r[2]} for r in cur.fetchall()}
        # Map producer_id back to wines
        cur.execute(f"""
            SELECT w.id, w.producer_id FROM wines w WHERE w.id IN ({placeholders})
        """, wine_ids)
        wine_producer_map = {str(r[0]): str(r[1]) for r in cur.fetchall()}
        for wid, w in wines.items():
            pid = wine_producer_map.get(wid, "")
            w["producer_insight"] = prod_insights.get(pid)

    # 11. Cascade context: grape insights (primary grape per wine)
    cur.execute(f"""
        SELECT DISTINCT ON (wg.wine_id) wg.wine_id, gi.ai_flavor_profile
        FROM wine_grapes wg
        JOIN grape_insights gi ON gi.grape_id = wg.grape_id
        WHERE wg.wine_id IN ({placeholders})
          AND gi.ai_flavor_profile IS NOT NULL
        ORDER BY wg.wine_id, wg.percentage DESC NULLS LAST
    """, wine_ids)
    for r in cur.fetchall():
        wid = str(r[0])
        if wid in wines:
            wines[wid]["grape_flavor_profile"] = r[1]

    return wines


def _assemble_single(cur, wine_id: str) -> dict:
    """Fallback: assemble context for a single wine (8 queries)."""
    result = bulk_preload_context(cur, [wine_id])
    return result.get(wine_id)


# ── Prompt builders ───────────────────────────────────────────

VOICE_PREAMBLE = """You are a wine expert writing for Loam, a wine intelligence platform. Your voice is knowledgeable but approachable — like a sommelier friend who explains the "why" behind everything. Be specific (name soils, climate patterns, geological formations). Connect place to taste. Have a point of view. No generic filler, no sommelier theater, no vague hedging.

Rules:
- Be specific: name the soil type, climate pattern, geological formation
- Connect place to taste: every fact should point toward why the wine tastes the way it does
- State what you know directly — "Etna Rosso is having a moment" not "seems to be gaining popularity"
- Be honest about what you don't know — "Limited plantings make this hard to assess" is fine
- Give real information, not atmosphere — every sentence teaches something
- Include market and value perspective where relevant
- Explain technical concepts briefly inline when they matter
- Food pairings: name real dishes (Nashville hot chicken, not "grilled meats"), cover casual and fine dining, explain why the pairing works
- Never manufacture specificity — if you don't know the soil type, don't guess"""


def _wine_identity_block(ctx: dict) -> str:
    """Build the wine identity section shared by both grades."""
    lines = [
        f"- Wine: {ctx['display_name']}",
        f"- Producer: {ctx['producer_name']}",
        f"- Country: {ctx.get('country_name', 'Unknown')}",
        f"- Region: {ctx.get('region_name', 'Unknown')}",
        f"- Appellation: {ctx.get('appellation_name', 'Unknown')}",
        f"- Type: {ctx.get('wine_type', 'table')} {ctx.get('color', '')}",
        f"- Sweetness: {ctx.get('sweetness_level', 'Unknown')}",
    ]

    grapes = ctx.get("grapes", [])
    if grapes:
        parts = []
        for g in grapes:
            s = g["name"]
            if g.get("percentage"):
                s += f" ({g['percentage']}%)"
            parts.append(s)
        lines.append(f"- Grapes: {', '.join(parts)}")
    else:
        lines.append("- Grapes: Unknown")

    if ctx.get("description"):
        lines.append(f"- Tasting: {ctx['description'][:500]}")
    if ctx.get("certs"):
        lines.append(f"- Certifications: {', '.join(ctx['certs'])}")
    if ctx.get("designations"):
        lines.append(f"- Designations: {', '.join(ctx['designations'])}")
    if ctx.get("producer_desc"):
        lines.append(f"- Producer notes: {ctx['producer_desc'][:300]}")
    if ctx.get("year_established"):
        lines.append(f"- Established: {ctx['year_established']}")

    return "\n".join(lines)


def _data_blocks(ctx: dict) -> str:
    """Build vintage/score/price/rule sections."""
    sections = []

    vintages = ctx.get("vintages", [])
    if vintages:
        vlines = []
        for v in vintages:
            parts = [f"Vintage {v.get('vintage_year') or 'NV'}"]
            if v.get("abv"): parts.append(f"ABV {v['abv']}%")
            if v.get("ph"): parts.append(f"pH {v['ph']}")
            if v.get("ta_g_l"): parts.append(f"TA {v['ta_g_l']} g/L")
            if v.get("rs_g_l"): parts.append(f"RS {v['rs_g_l']} g/L")
            if v.get("fermentation_vessel"): parts.append(f"Fermented in {v['fermentation_vessel']}")
            if v.get("yeast_type"): parts.append(f"{v['yeast_type']} yeast")
            if v.get("mlf") is not None: parts.append("Full MLF" if v["mlf"] else "No MLF")
            if v.get("aging_vessel"): parts.append(f"Aged in {v['aging_vessel']}")
            if v.get("duration_in_oak_months"): parts.append(f"{v['duration_in_oak_months']}mo oak")
            if v.get("oak_origin"): parts.append(f"{v['oak_origin']} oak")
            if v.get("new_oak_pct"): parts.append(f"{v['new_oak_pct']}% new oak")
            if v.get("cases_produced"): parts.append(f"{v['cases_produced']} cases")
            vlines.append(", ".join(parts))
        sections.append("## Vintage Data\n" + "\n".join(vlines))

    scores = ctx.get("scores", [])
    if scores:
        slines = []
        for s in scores:
            pub = s.get("publication") or "Unknown"
            vy = s.get("vintage_year") or "NV"
            if s.get("medal"):
                slines.append(f"{pub}: {s['medal']} medal ({vy})")
            elif s.get("score"):
                scale = s.get("score_scale") or 100
                line = f"{pub}: {s['score']}/{scale} ({vy})"
                if s.get("tasting_note"):
                    line += f" — {s['tasting_note'][:150]}"
                slines.append(line)
        sections.append("## Critic Scores\n" + "\n".join(slines))

    prices = ctx.get("prices", [])
    if prices:
        plines = []
        for p in prices:
            amt = f"${p['price_usd']}" if p.get("price_usd") else f"{p.get('price_original', '?')} {p.get('currency', '')}"
            plines.append(f"{amt} ({p.get('merchant_name', '?')}, {p.get('vintage_year') or 'NV'})")
        sections.append("## Prices\n" + ", ".join(plines))

    if ctx.get("appellation_rule"):
        rule_str = ctx["appellation_rule"] if isinstance(ctx["appellation_rule"], str) else json.dumps(ctx["appellation_rule"])
        sections.append(f"## Appellation Rules\n{rule_str[:600]}")

    return "\n\n".join(sections)


def build_grade_c_prompt(ctx: dict) -> str:
    """Grade C: hook + style + sensory + comparables. Lighter, cheaper."""
    identity = _wine_identity_block(ctx)
    data = _data_blocks(ctx)

    prompt = f"""{VOICE_PREAMBLE}

Enrich this wine with a catalog-level profile. Return ONLY valid JSON (no markdown, no backticks).

## Wine Identity
{identity}

{data}

## Output Format
Return a JSON object with these fields:
{{
  "hook": "2-3 sentences. The 30-second story — what makes this wine worth knowing about. Be specific to THIS wine, not generic about the region.",
  "style_profile": "One phrase, e.g., 'Full-bodied dry red with firm tannins and earthy depth'",
  "comparable_wines": "2-3 similar wines with one-line reasoning each. Format: 'Wine Name — why it\\'s comparable.'",
  "sensory": {{
    "acidity": 1-5,
    "tannin": 1-5,
    "body": 1-5,
    "sweetness": 1-5,
    "alcohol": 1-5,
    "color_intensity": "pale|medium|deep",
    "aroma_intensity": "light|medium|pronounced",
    "finish_length": "short|medium|long",
    "complexity": "simple|moderate|complex",
    "quality_level": "acceptable|good|very_good|outstanding|exceptional"
  }}
}}"""
    return prompt


def build_grade_b_prompt(ctx: dict) -> str:
    """Grade B: full narrative enrichment. Richer, more expensive."""
    identity = _wine_identity_block(ctx)
    data = _data_blocks(ctx)

    prompt = f"""{VOICE_PREAMBLE}

Enrich this wine with a complete profile. Return ONLY valid JSON (no markdown, no backticks).

## Wine Identity
{identity}

{data}

## Output Format
Return a JSON object with these fields:
{{
  "hook": "2-3 sentences. The 30-second story — what makes this wine worth knowing about.",
  "wine_summary": "1-2 paragraphs. The rich story: why this wine matters, what makes the producer distinctive, how terroir shapes the wine. Be specific.",
  "terroir_expression": "1 paragraph. How the specific soil, climate, and site express themselves in this wine. Connect geology to glass.",
  "vinification_summary": "1 paragraph. How the wine is made and why those choices matter. Explain technical concepts briefly.",
  "food_pairing": "3-5 specific pairings with brief reasoning. Name real dishes, not categories. Include casual and fine dining.",
  "comparable_wines": "2-3 similar wines with reasoning. Format: 'Wine Name — why it\\'s comparable.'",
  "style_profile": "One phrase, e.g., 'Full-bodied dry red with firm tannins and earthy depth'",
  "cellar_recommendation": "1-2 sentences on when to drink and storage advice.",
  "aging_potential_years": null or integer,
  "drinking_window_min_years": null or integer,
  "drinking_window_max_years": null or integer,
  "sensory": {{
    "acidity": 1-5,
    "tannin": 1-5,
    "body": 1-5,
    "sweetness": 1-5,
    "alcohol": 1-5,
    "color_intensity": "pale|medium|deep",
    "aroma_intensity": "light|medium|pronounced",
    "finish_length": "short|medium|long",
    "complexity": "simple|moderate|complex",
    "quality_level": "acceptable|good|very_good|outstanding|exceptional"
  }}
}}"""
    return prompt


def _cascade_context_block(ctx: dict) -> str:
    """Build cascade context section from reference entity AI insights."""
    sections = []

    ai = ctx.get("appellation_insight")
    if ai:
        if ai.get("overview"):
            sections.append(f"## Appellation Context (enriched)\n{ai['overview'][:800]}")
        if ai.get("soil"):
            sections.append(f"## Appellation Soil Profile (enriched)\n{ai['soil'][:500]}")

    pi = ctx.get("producer_insight")
    if pi:
        if pi.get("style"):
            sections.append(f"## Producer Winemaking Style (enriched)\n{pi['style'][:500]}")
        if pi.get("overview"):
            sections.append(f"## Producer Overview (enriched)\n{pi['overview'][:600]}")

    gfp = ctx.get("grape_flavor_profile")
    if gfp:
        sections.append(f"## Primary Grape Flavor Profile (enriched)\n{gfp[:500]}")

    if not sections:
        return ""
    return "\n\n".join(sections)


def build_grade_a_prompt(ctx: dict) -> str:
    """Grade A: full narrative + cascade context + value/market/insider. Uses Sonnet."""
    identity = _wine_identity_block(ctx)
    data = _data_blocks(ctx)
    cascade = _cascade_context_block(ctx)

    cascade_section = ""
    if cascade:
        cascade_section = f"""
## Reference Entity Context (from enriched appellation, producer, and grape data)
Use this to ground your writing in specific, validated context. Reference these facts when relevant.

{cascade}
"""

    prompt = f"""{VOICE_PREAMBLE}

Enrich this wine with a complete Grade A profile. Return ONLY valid JSON (no markdown, no backticks).

## Wine Identity
{identity}

{data}
{cascade_section}
## Output Format
Return a JSON object with these fields:
{{
  "hook": "2-3 sentences. The 30-second story — what makes this wine worth knowing about.",
  "wine_summary": "1-2 paragraphs. The rich story: why this wine matters, what makes the producer distinctive, how terroir shapes the wine. Be specific.",
  "terroir_expression": "1 paragraph. How the specific soil, climate, and site express themselves in this wine. Connect geology to glass. Draw from the appellation context above when available.",
  "vinification_summary": "1 paragraph. How the wine is made and why those choices matter. Draw from producer winemaking style context when available.",
  "food_pairing": "3-5 specific pairings with brief reasoning. Name real dishes, not categories. Include casual and fine dining.",
  "comparable_wines": "2-3 similar wines with reasoning. Format: 'Wine Name — why it\\'s comparable.'",
  "style_profile": "One phrase, e.g., 'Full-bodied dry red with firm tannins and earthy depth'",
  "cellar_recommendation": "1-2 sentences on when to drink and storage advice.",
  "value_assessment": "2-3 sentences. Is this wine worth the price? What does it compete with at its price point? Be honest and specific.",
  "market_position": "2-3 sentences. Where this wine sits in the market — restaurant wine, collector wine, everyday drinker? Who buys it and why?",
  "insider_take": "2-3 sentences. The honest take a knowledgeable wine shop owner would give a trusted customer. What's the real story? Have a point of view.",
  "aging_potential_years": null or integer,
  "drinking_window_min_years": null or integer,
  "drinking_window_max_years": null or integer,
  "sensory": {{
    "acidity": 1-5,
    "tannin": 1-5,
    "body": 1-5,
    "sweetness": 1-5,
    "alcohol": 1-5,
    "color_intensity": "pale|medium|deep",
    "aroma_intensity": "light|medium|pronounced",
    "finish_length": "short|medium|long",
    "complexity": "simple|moderate|complex",
    "quality_level": "acceptable|good|very_good|outstanding|exceptional"
  }}
}}"""
    return prompt


# ── API call ──────────────────────────────────────────────────

def call_claude(client: anthropic.Anthropic, prompt: str, model: str,
                max_tokens: int = 2000) -> tuple[dict, dict]:
    """Call Claude API and return (parsed_result, usage_info)."""
    resp = client.messages.create(
        model=model,
        max_tokens=max_tokens,
        messages=[{"role": "user", "content": prompt}],
    )
    raw = resp.content[0].text.strip()
    # Strip markdown fences if present
    if raw.startswith("```"):
        raw = raw.split("\n", 1)[1] if "\n" in raw else raw[3:]
        if raw.endswith("```"):
            raw = raw[:-3].strip()

    result = json.loads(raw)

    # Normalize comparable_wines — sometimes comes back as list/dict instead of string
    cw = result.get("comparable_wines")
    if cw and not isinstance(cw, str):
        if isinstance(cw, list):
            result["comparable_wines"] = "\n".join(str(x) for x in cw)
        elif isinstance(cw, dict):
            result["comparable_wines"] = "\n".join(f"{k} — {v}" for k, v in cw.items())
        else:
            result["comparable_wines"] = str(cw)

    # Normalize food_pairing similarly
    fp = result.get("food_pairing")
    if fp and not isinstance(fp, str):
        if isinstance(fp, list):
            result["food_pairing"] = "\n".join(str(x) for x in fp)
        else:
            result["food_pairing"] = str(fp)

    usage = {
        "input_tokens": resp.usage.input_tokens,
        "output_tokens": resp.usage.output_tokens,
    }
    return result, usage


# ── DB writes ─────────────────────────────────────────────────

def write_grade_c(cur, wine_id: str, result: dict):
    """Write Grade C results to DB."""
    now = datetime.now(timezone.utc).isoformat()
    refresh = datetime(2027, 4, 9, tzinfo=timezone.utc).isoformat()

    cur.execute("SAVEPOINT grade_c_write")

    cur.execute("""
        INSERT INTO wine_insights (wine_id, ai_hook, ai_style_profile, ai_comparable_wines,
                                   enrichment_tier, confidence, enriched_at, refresh_after)
        VALUES (%s, %s, %s, %s, 'C', 0.80, %s, %s)
        ON CONFLICT (wine_id) DO UPDATE SET
            ai_hook = EXCLUDED.ai_hook,
            ai_style_profile = EXCLUDED.ai_style_profile,
            ai_comparable_wines = EXCLUDED.ai_comparable_wines,
            enrichment_tier = EXCLUDED.enrichment_tier,
            confidence = EXCLUDED.confidence,
            enriched_at = EXCLUDED.enriched_at,
            refresh_after = EXCLUDED.refresh_after
    """, (wine_id, result.get("hook"), result.get("style_profile"),
          result.get("comparable_wines"), now, refresh))

    cur.execute("""
        UPDATE wines SET data_grade = 'C'
        WHERE id = %s AND (data_grade IS NULL OR data_grade IN ('F', 'D'))
    """, (wine_id,))

    sensory = result.get("sensory", {})
    if sensory:
        cur.execute("""
            INSERT INTO wine_vintage_tasting_insights
                (id, wine_id, vintage_year, sensory_acidity, sensory_tannin,
                 sensory_body, sensory_sweetness, sensory_alcohol,
                 color_intensity, aroma_intensity, finish_length,
                 complexity, quality_level, confidence, enriched_at, refresh_after)
            VALUES (%s, %s, 0, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 0.75, %s, %s)
            ON CONFLICT (wine_id, vintage_year) DO UPDATE SET
                sensory_acidity = EXCLUDED.sensory_acidity,
                sensory_tannin = EXCLUDED.sensory_tannin,
                sensory_body = EXCLUDED.sensory_body,
                sensory_sweetness = EXCLUDED.sensory_sweetness,
                sensory_alcohol = EXCLUDED.sensory_alcohol,
                color_intensity = EXCLUDED.color_intensity,
                aroma_intensity = EXCLUDED.aroma_intensity,
                finish_length = EXCLUDED.finish_length,
                complexity = EXCLUDED.complexity,
                quality_level = EXCLUDED.quality_level,
                confidence = EXCLUDED.confidence,
                enriched_at = EXCLUDED.enriched_at,
                refresh_after = EXCLUDED.refresh_after
        """, (str(uuid4()), wine_id,
              sensory.get("acidity"), sensory.get("tannin"),
              sensory.get("body"), sensory.get("sweetness"), sensory.get("alcohol"),
              sensory.get("color_intensity"), sensory.get("aroma_intensity"),
              sensory.get("finish_length"), sensory.get("complexity"),
              sensory.get("quality_level"), now, refresh))


def write_grade_b(cur, wine_id: str, result: dict):
    """Write Grade B results to DB."""
    now = datetime.now(timezone.utc).isoformat()
    refresh = datetime(2027, 4, 9, tzinfo=timezone.utc).isoformat()

    cur.execute("""
        INSERT INTO wine_insights (wine_id, ai_hook, ai_wine_summary, ai_terroir_expression,
                                   ai_vinification_summary, ai_food_pairing, ai_comparable_wines,
                                   ai_style_profile, ai_cellar_recommendation,
                                   typical_aging_potential_years,
                                   typical_drinking_window_min_years, typical_drinking_window_max_years,
                                   enrichment_tier, confidence, enriched_at, refresh_after)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'B', 0.85, %s, %s)
        ON CONFLICT (wine_id) DO UPDATE SET
            ai_hook = EXCLUDED.ai_hook,
            ai_wine_summary = EXCLUDED.ai_wine_summary,
            ai_terroir_expression = EXCLUDED.ai_terroir_expression,
            ai_vinification_summary = EXCLUDED.ai_vinification_summary,
            ai_food_pairing = EXCLUDED.ai_food_pairing,
            ai_comparable_wines = EXCLUDED.ai_comparable_wines,
            ai_style_profile = EXCLUDED.ai_style_profile,
            ai_cellar_recommendation = EXCLUDED.ai_cellar_recommendation,
            typical_aging_potential_years = EXCLUDED.typical_aging_potential_years,
            typical_drinking_window_min_years = EXCLUDED.typical_drinking_window_min_years,
            typical_drinking_window_max_years = EXCLUDED.typical_drinking_window_max_years,
            enrichment_tier = EXCLUDED.enrichment_tier,
            confidence = EXCLUDED.confidence,
            enriched_at = EXCLUDED.enriched_at,
            refresh_after = EXCLUDED.refresh_after
    """, (wine_id,
          result.get("hook"), result.get("wine_summary"),
          result.get("terroir_expression"), result.get("vinification_summary"),
          result.get("food_pairing"), result.get("comparable_wines"),
          result.get("style_profile"), result.get("cellar_recommendation"),
          result.get("aging_potential_years"),
          result.get("drinking_window_min_years"), result.get("drinking_window_max_years"),
          now, refresh))

    sensory = result.get("sensory", {})
    if sensory:
        # Use most recent vintage year, or 0 for NV/general
        cur.execute("""
            SELECT vintage_year FROM wine_vintages
            WHERE wine_id = %s ORDER BY vintage_year DESC LIMIT 1
        """, (wine_id,))
        row = cur.fetchone()
        vy = row[0] if row and row[0] else 0

        cur.execute("""
            INSERT INTO wine_vintage_tasting_insights
                (id, wine_id, vintage_year, sensory_acidity, sensory_tannin,
                 sensory_body, sensory_sweetness, sensory_alcohol,
                 color_intensity, aroma_intensity, finish_length,
                 complexity, quality_level, confidence, enriched_at, refresh_after)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 0.80, %s, %s)
            ON CONFLICT (wine_id, vintage_year) DO UPDATE SET
                sensory_acidity = EXCLUDED.sensory_acidity,
                sensory_tannin = EXCLUDED.sensory_tannin,
                sensory_body = EXCLUDED.sensory_body,
                sensory_sweetness = EXCLUDED.sensory_sweetness,
                sensory_alcohol = EXCLUDED.sensory_alcohol,
                color_intensity = EXCLUDED.color_intensity,
                aroma_intensity = EXCLUDED.aroma_intensity,
                finish_length = EXCLUDED.finish_length,
                complexity = EXCLUDED.complexity,
                quality_level = EXCLUDED.quality_level,
                confidence = EXCLUDED.confidence,
                enriched_at = EXCLUDED.enriched_at,
                refresh_after = EXCLUDED.refresh_after
        """, (str(uuid4()), wine_id, vy,
              sensory.get("acidity"), sensory.get("tannin"),
              sensory.get("body"), sensory.get("sweetness"), sensory.get("alcohol"),
              sensory.get("color_intensity"), sensory.get("aroma_intensity"),
              sensory.get("finish_length"), sensory.get("complexity"),
              sensory.get("quality_level"), now, refresh))

    cur.execute("""
        UPDATE wines SET data_grade = 'B'
        WHERE id = %s AND (data_grade IS NULL OR data_grade IN ('F', 'D', 'C'))
    """, (wine_id,))


def write_grade_a(cur, wine_id: str, result: dict):
    """Write Grade A results to DB (all Grade B fields + value/market/insider)."""
    now = datetime.now(timezone.utc).isoformat()
    refresh = datetime(2027, 4, 9, tzinfo=timezone.utc).isoformat()

    cur.execute("""
        INSERT INTO wine_insights (wine_id, ai_hook, ai_wine_summary, ai_terroir_expression,
                                   ai_vinification_summary, ai_food_pairing, ai_comparable_wines,
                                   ai_style_profile, ai_cellar_recommendation,
                                   ai_value_assessment, ai_market_position, ai_insider_take,
                                   typical_aging_potential_years,
                                   typical_drinking_window_min_years, typical_drinking_window_max_years,
                                   enrichment_tier, confidence, enriched_at, refresh_after)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'A', 0.90, %s, %s)
        ON CONFLICT (wine_id) DO UPDATE SET
            ai_hook = EXCLUDED.ai_hook,
            ai_wine_summary = EXCLUDED.ai_wine_summary,
            ai_terroir_expression = EXCLUDED.ai_terroir_expression,
            ai_vinification_summary = EXCLUDED.ai_vinification_summary,
            ai_food_pairing = EXCLUDED.ai_food_pairing,
            ai_comparable_wines = EXCLUDED.ai_comparable_wines,
            ai_style_profile = EXCLUDED.ai_style_profile,
            ai_cellar_recommendation = EXCLUDED.ai_cellar_recommendation,
            ai_value_assessment = EXCLUDED.ai_value_assessment,
            ai_market_position = EXCLUDED.ai_market_position,
            ai_insider_take = EXCLUDED.ai_insider_take,
            typical_aging_potential_years = EXCLUDED.typical_aging_potential_years,
            typical_drinking_window_min_years = EXCLUDED.typical_drinking_window_min_years,
            typical_drinking_window_max_years = EXCLUDED.typical_drinking_window_max_years,
            enrichment_tier = EXCLUDED.enrichment_tier,
            confidence = EXCLUDED.confidence,
            enriched_at = EXCLUDED.enriched_at,
            refresh_after = EXCLUDED.refresh_after
    """, (wine_id,
          result.get("hook"), result.get("wine_summary"),
          result.get("terroir_expression"), result.get("vinification_summary"),
          result.get("food_pairing"), result.get("comparable_wines"),
          result.get("style_profile"), result.get("cellar_recommendation"),
          result.get("value_assessment"), result.get("market_position"),
          result.get("insider_take"),
          result.get("aging_potential_years"),
          result.get("drinking_window_min_years"), result.get("drinking_window_max_years"),
          now, refresh))

    sensory = result.get("sensory", {})
    if sensory:
        cur.execute("""
            SELECT vintage_year FROM wine_vintages
            WHERE wine_id = %s ORDER BY vintage_year DESC LIMIT 1
        """, (wine_id,))
        row = cur.fetchone()
        vy = row[0] if row and row[0] else 0

        cur.execute("""
            INSERT INTO wine_vintage_tasting_insights
                (id, wine_id, vintage_year, sensory_acidity, sensory_tannin,
                 sensory_body, sensory_sweetness, sensory_alcohol,
                 color_intensity, aroma_intensity, finish_length,
                 complexity, quality_level, confidence, enriched_at, refresh_after)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 0.85, %s, %s)
            ON CONFLICT (wine_id, vintage_year) DO UPDATE SET
                sensory_acidity = EXCLUDED.sensory_acidity,
                sensory_tannin = EXCLUDED.sensory_tannin,
                sensory_body = EXCLUDED.sensory_body,
                sensory_sweetness = EXCLUDED.sensory_sweetness,
                sensory_alcohol = EXCLUDED.sensory_alcohol,
                color_intensity = EXCLUDED.color_intensity,
                aroma_intensity = EXCLUDED.aroma_intensity,
                finish_length = EXCLUDED.finish_length,
                complexity = EXCLUDED.complexity,
                quality_level = EXCLUDED.quality_level,
                confidence = EXCLUDED.confidence,
                enriched_at = EXCLUDED.enriched_at,
                refresh_after = EXCLUDED.refresh_after
        """, (str(uuid4()), wine_id, vy,
              sensory.get("acidity"), sensory.get("tannin"),
              sensory.get("body"), sensory.get("sweetness"), sensory.get("alcohol"),
              sensory.get("color_intensity"), sensory.get("aroma_intensity"),
              sensory.get("finish_length"), sensory.get("complexity"),
              sensory.get("quality_level"), now, refresh))

    cur.execute("""
        UPDATE wines SET data_grade = 'A'
        WHERE id = %s AND (data_grade IS NULL OR data_grade IN ('F', 'D', 'C', 'B'))
    """, (wine_id,))


def log_enrichment(cur, wine_id: str, grade: str, model: str, usage: dict, cost: float, status: str, error: str = None):
    """Write enrichment_log entry."""
    cur.execute("""
        INSERT INTO enrichment_log (entity_type, entity_id, enrichment_type, model,
                                    prompt_template, input_tokens, output_tokens,
                                    cost_usd, fields_updated, status, error_message, attempts)
        VALUES ('wine', %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 1)
    """, (wine_id,
          f"grade_{grade.lower()}", model, f"batch-enrich-v1-{grade.lower()}",
          usage.get("input_tokens", 0), usage.get("output_tokens", 0),
          cost,
          ["wine_insights", "wine_vintage_tasting_insights", "data_grade"],
          status, error))


# ── Wine selection ────────────────────────────────────────────

def select_wines(cur, grade: str, limit: int, wine_ids: list[str] = None,
                  producer_ids: list[str] = None) -> list[dict]:
    """Select wines to enrich, ordered by priority."""
    if wine_ids:
        placeholders = ",".join(["%s"] * len(wine_ids))
        cur.execute(f"""
            SELECT w.id, w.display_name, w.data_grade
            FROM wines w
            WHERE w.id IN ({placeholders}) AND w.deleted_at IS NULL
        """, wine_ids)
    elif producer_ids:
        # Select all wines from given producers that don't already have target-grade insights
        prod_ph = ",".join(["%s"] * len(producer_ids))
        cur.execute(f"""
            SELECT w.id, w.display_name, w.data_grade
            FROM wines w
            WHERE w.deleted_at IS NULL
              AND w.producer_id IN ({prod_ph})
              AND NOT EXISTS (
                  SELECT 1 FROM wine_insights wi
                  WHERE wi.wine_id = w.id
                  AND wi.enrichment_tier = %s
              )
            ORDER BY
                (SELECT count(*) FROM wine_vintage_scores s WHERE s.wine_id = w.id) DESC,
                (SELECT count(*) FROM wine_vintage_prices p WHERE p.wine_id = w.id) DESC,
                (SELECT count(*) FROM wine_grapes wg WHERE wg.wine_id = w.id) DESC,
                w.display_name
            LIMIT %s
        """, (*producer_ids, grade, limit))
    else:
        target_grades = ("F", "D") if grade == "C" else ("F", "D", "C")
        placeholders = ",".join(["%s"] * len(target_grades))
        cur.execute(f"""
            SELECT w.id, w.display_name, w.data_grade
            FROM wines w
            WHERE w.deleted_at IS NULL
              AND (w.data_grade IN ({placeholders}))
              AND NOT EXISTS (
                  SELECT 1 FROM wine_insights wi
                  WHERE wi.wine_id = w.id
                  AND wi.enrichment_tier >= %s
              )
            ORDER BY
                -- Prioritize: wines with scores > wines with prices > wines with grapes > rest
                (SELECT count(*) FROM wine_vintage_scores s WHERE s.wine_id = w.id) DESC,
                (SELECT count(*) FROM wine_vintage_prices p WHERE p.wine_id = w.id) DESC,
                (SELECT count(*) FROM wine_grapes wg WHERE wg.wine_id = w.id) DESC,
                (SELECT count(*) FROM wine_vintages wv WHERE wv.wine_id = w.id) DESC
            LIMIT %s
        """, (*target_grades, grade, limit))

    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, r)) for r in cur.fetchall()]


# ── Main ──────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Batch wine enrichment")
    parser.add_argument("--grade", choices=["C", "B", "A"], required=True, help="Enrichment grade")
    parser.add_argument("--limit", type=int, default=10, help="Max wines to process")
    parser.add_argument("--ids", help="Comma-separated wine UUIDs (overrides selection)")
    parser.add_argument("--producer", help="Comma-separated producer UUIDs (select all their wines)")
    parser.add_argument("--execute", action="store_true", help="Actually call API and write DB")
    parser.add_argument("--print", dest="print_results", action="store_true", help="Print enrichment results")
    parser.add_argument("--budget", type=float, default=999.0, help="Max USD to spend")
    parser.add_argument("--delay", type=float, default=0.0, help="Seconds between API calls")
    parser.add_argument("--workers", type=int, default=1, help="Parallel API workers (1=serial, 8=fast)")
    args = parser.parse_args()

    model = HAIKU_MODEL if args.grade == "C" else SONNET_MODEL
    pricing = PRICING[model]

    conn = get_conn()
    cur = conn.cursor()

    # Select wines
    wine_ids = args.ids.split(",") if args.ids else None
    producer_ids = args.producer.split(",") if args.producer else None
    wines = select_wines(cur, args.grade, args.limit, wine_ids, producer_ids)
    print(f"\n{'='*60}")
    print(f"  BATCH ENRICHMENT — Grade {args.grade} ({model.split('-')[1].title()})")
    print(f"{'='*60}")
    print(f"  Wines selected: {len(wines)}")
    print(f"  Mode: {'EXECUTE' if args.execute else 'DRY-RUN'}")
    print(f"  Budget: ${args.budget:.2f}")
    print()

    if not wines:
        print("  No wines to enrich.")
        return

    # Show selection
    for i, w in enumerate(wines[:20]):
        print(f"  {i+1:3}. {w['display_name']} [{w['data_grade']}]")
    if len(wines) > 20:
        print(f"  ... and {len(wines) - 20} more")
    print()

    if not args.execute:
        # Dry-run: show context for first wine
        ctx = assemble_context(cur, str(wines[0]["id"]))
        if ctx:
            print(f"  Sample context for: {ctx['display_name']}")
            print(f"    Grapes: {len(ctx['grapes'])}, Scores: {len(ctx['scores'])}, "
                  f"Vintages: {len(ctx['vintages'])}, Prices: {len(ctx['prices'])}")
            if ctx["grapes"]:
                print(f"    Grape list: {', '.join(g['name'] for g in ctx['grapes'])}")
            if ctx["scores"]:
                print(f"    Top score: {ctx['scores'][0].get('publication')} {ctx['scores'][0].get('score')}")
            if ctx.get("appellation_rule"):
                rule_str = str(ctx['appellation_rule'])
                print(f"    Appellation rule: {rule_str[:100]}...")
        print(f"\n  Run with --execute to call API and write results.")
        cur.close()
        conn.close()
        return

    # Execute mode — bulk preload context
    client = anthropic.Anthropic()
    total_cost = 0.0
    success = 0
    errors = 0

    # Pre-load all context in bulk (8 queries total, not 8*N)
    all_wine_ids = [str(w["id"]) for w in wines]
    BATCH_SIZE = 500  # preload in chunks to avoid query size limits
    all_contexts = {}
    for batch_start in range(0, len(all_wine_ids), BATCH_SIZE):
        batch_ids = all_wine_ids[batch_start:batch_start + BATCH_SIZE]
        batch_ctx = bulk_preload_context(cur, batch_ids)
        all_contexts.update(batch_ctx)
    print(f"  Pre-loaded context for {len(all_contexts)} wines")
    print(f"  Workers: {args.workers}")
    sys.stdout.flush()

    max_tok = 3500 if args.grade == "A" else 2000
    print_lock = Lock()

    def _enrich_one(wine_id: str, display: str) -> tuple[str, str, dict | None, dict, float, str | None]:
        """Call Claude API for one wine (thread-safe). Returns (wine_id, display, result, usage, cost, error)."""
        ctx = all_contexts.get(wine_id)
        if not ctx:
            return wine_id, display, None, {}, 0.0, "no context"

        if args.grade == "C":
            prompt = build_grade_c_prompt(ctx)
        elif args.grade == "A":
            prompt = build_grade_a_prompt(ctx)
        else:
            prompt = build_grade_b_prompt(ctx)

        # Call API with retry on rate limit
        for attempt in range(3):
            try:
                result, usage = call_claude(client, prompt, model, max_tokens=max_tok)
                cost = (usage["input_tokens"] * pricing["input"] / 1_000_000 +
                        usage["output_tokens"] * pricing["output"] / 1_000_000)
                return wine_id, display, result, usage, cost, None
            except json.JSONDecodeError as e:
                return wine_id, display, None, {}, 0.0, f"JSON parse: {e}"
            except Exception as e:
                err_str = str(e)
                if "rate" in err_str.lower() or "429" in err_str:
                    wait = 2 ** attempt * 5
                    with print_lock:
                        print(f"    Rate limited, waiting {wait}s...")
                        sys.stdout.flush()
                    time.sleep(wait)
                    continue
                return wine_id, display, None, {}, 0.0, err_str[:500]
        return wine_id, display, None, {}, 0.0, "max retries exceeded (rate limit)"

    if args.workers <= 1:
        # ── Serial path (original behavior) ──
        for i, w in enumerate(wines):
            wine_id = str(w["id"])
            display = w["display_name"]

            wine_id, display, result, usage, cost, error = _enrich_one(wine_id, display)
            total_cost += cost

            if error:
                errors += 1
                log_enrichment(cur, wine_id, args.grade, model, usage, cost, "error", error)
                conn.commit()
                print(f"  [{i+1}/{len(wines)}] ERROR {display} — {error}")
            elif result:
                if args.grade == "C":
                    write_grade_c(cur, wine_id, result)
                elif args.grade == "A":
                    write_grade_a(cur, wine_id, result)
                else:
                    write_grade_b(cur, wine_id, result)
                log_enrichment(cur, wine_id, args.grade, model, usage, cost, "completed")
                conn.commit()
                success += 1

                hook_preview = (result.get("hook") or "")[:120]
                style = result.get("style_profile", "")
                quality = result.get("sensory", {}).get("quality_level", "?")
                print(f"  [{i+1}/{len(wines)}] {display}")
                print(f"    -> {style} | {quality} | ${cost:.4f}")
                print(f"    -> {hook_preview}...")
                sys.stdout.flush()

                if args.print_results:
                    print(f"\n{'─'*60}")
                    print(json.dumps(result, indent=2, ensure_ascii=True))
                    print(f"{'─'*60}\n")

            if total_cost >= args.budget:
                print(f"\n  Budget limit reached (${total_cost:.4f} >= ${args.budget:.2f})")
                break
            if args.delay and i < len(wines) - 1:
                time.sleep(args.delay)
    else:
        # ── Parallel path: API calls in thread pool, DB writes serial ──
        done_count = 0
        budget_hit = False

        with ThreadPoolExecutor(max_workers=args.workers) as pool:
            # Submit all futures
            future_to_wine = {}
            for w in wines:
                wine_id = str(w["id"])
                display = w["display_name"]
                fut = pool.submit(_enrich_one, wine_id, display)
                future_to_wine[fut] = (wine_id, display)

            # Process results as they complete (DB writes are serial)
            for fut in as_completed(future_to_wine):
                wine_id, display = future_to_wine[fut]
                done_count += 1
                try:
                    wine_id, display, result, usage, cost, error = fut.result()
                except Exception as e:
                    error = str(e)[:500]
                    result, usage, cost = None, {}, 0.0

                total_cost += cost

                if error:
                    errors += 1
                    log_enrichment(cur, wine_id, args.grade, model, usage, cost, "error", error)
                    conn.commit()
                    print(f"  [{done_count}/{len(wines)}] ERROR {display} — {error}")
                elif result:
                    if args.grade == "C":
                        write_grade_c(cur, wine_id, result)
                    elif args.grade == "A":
                        write_grade_a(cur, wine_id, result)
                    else:
                        write_grade_b(cur, wine_id, result)
                    log_enrichment(cur, wine_id, args.grade, model, usage, cost, "completed")
                    conn.commit()
                    success += 1

                    hook_preview = (result.get("hook") or "")[:120]
                    style = result.get("style_profile", "")
                    quality = result.get("sensory", {}).get("quality_level", "?")
                    print(f"  [{done_count}/{len(wines)}] {display}")
                    print(f"    -> {style} | {quality} | ${cost:.4f}")
                    print(f"    -> {hook_preview}...")

                sys.stdout.flush()

                if total_cost >= args.budget and not budget_hit:
                    budget_hit = True
                    print(f"\n  Budget limit reached (${total_cost:.4f} >= ${args.budget:.2f})")
                    print(f"  Waiting for {args.workers - 1} in-flight requests to finish...")

    print(f"\n{'='*60}")
    print(f"  RESULTS: {success} enriched, {errors} errors, ${total_cost:.4f} spent")
    print(f"{'='*60}\n")

    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
