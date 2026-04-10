#!/usr/bin/env python3
"""Facts packet builder for retrieval-grounded enrichment.

Pulls every verifiable fact about a wine from the canonical DB and renders
it into the markdown block that L1 prompts (and L3 fact-check prompts) consume.

The packet is the ground truth for one wine. Anything NOT in the packet must
not appear in the model's output as a specific claim.

Ported from the inline logic in pipeline/enrich/l1_test.py so it can be
shared by the L1 generator, the L3 fact-checker, and the eventual full
re-enrichment script.

Standalone usage (for spot-check):
    python -m pipeline.enrich.build_facts_packet <wine_id>
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))


# ── Data fetchers ────────────────────────────────────────────────────────────


def _fetch_hard_facts(cur, wine_id: str) -> dict:
    """Pull all verified facts for a single wine from the DB."""
    cur.execute(
        """
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
        """,
        (wine_id,),
    )
    row = cur.fetchone()
    if not row:
        return {}
    cols = [d[0] for d in cur.description]
    facts: dict[str, Any] = dict(zip(cols, row))

    # Grapes
    cur.execute(
        """
        SELECT g.display_name, wg.percentage
        FROM wine_grapes wg JOIN grapes g ON wg.grape_id = g.id
        WHERE wg.wine_id = %s
        ORDER BY wg.percentage DESC NULLS LAST
        """,
        (wine_id,),
    )
    facts["grapes"] = [
        {"name": r[0], "percentage": float(r[1]) if r[1] is not None else None}
        for r in cur.fetchall()
    ]

    # Latest 3 vintages + chemistry
    cur.execute(
        """
        SELECT vintage_year, abv, ph, ta_g_l, rs_g_l,
               duration_in_oak_months, new_oak_pct, oak_origin,
               fermentation_vessel, yeast_type, mlf, closure,
               cases_produced, release_price_usd, winemaker_notes
        FROM wine_vintages
        WHERE wine_id = %s AND deleted_at IS NULL
        ORDER BY vintage_year DESC NULLS LAST
        LIMIT 3
        """,
        (wine_id,),
    )
    vintage_cols = [d[0] for d in cur.description]
    facts["vintages"] = [dict(zip(vintage_cols, r)) for r in cur.fetchall()]

    # Appellation rules text (if available)
    facts["appellation_rules"] = None
    if facts.get("appellation_id"):
        cur.execute(
            """
            SELECT source_text_excerpt, source_url, source_document_title,
                   source_organization, rules
            FROM appellation_rules
            WHERE appellation_id = %s
            ORDER BY last_verified_at DESC NULLS LAST
            LIMIT 1
            """,
            (facts["appellation_id"],),
        )
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
    cur.execute(
        """
        SELECT fc.name FROM wine_farming_certifications wfc
        JOIN farming_certifications fc ON wfc.farming_certification_id = fc.id
        WHERE wfc.wine_id = %s
        """,
        (wine_id,),
    )
    facts["certifications"] = [r[0] for r in cur.fetchall()]

    # Scores
    cur.execute(
        """
        SELECT pub.name, wvs.score, wvs.score_scale, wvs.vintage_year, wvs.medal
        FROM wine_vintage_scores wvs
        LEFT JOIN publications pub ON wvs.publication_id = pub.id
        WHERE wvs.wine_id = %s
        ORDER BY wvs.score DESC NULLS LAST
        LIMIT 5
        """,
        (wine_id,),
    )
    facts["scores"] = [
        {
            "publication": r[0] or "Unknown",
            "score": r[1],
            "scale": r[2],
            "vintage": r[3],
            "medal": r[4],
        }
        for r in cur.fetchall()
    ]

    # Prices (range)
    cur.execute(
        """
        SELECT min(price_usd), avg(price_usd), max(price_usd), count(*)
        FROM wine_vintage_prices
        WHERE wine_id = %s AND price_usd IS NOT NULL
        """,
        (wine_id,),
    )
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


def _fetch_comparable_wines(cur, wine_id: str, limit: int = 3) -> list[dict]:
    """Pre-fetch comparable wines from OUR OWN DB.

    Similarity = shares primary grape, ranked by appellation match then country
    match. Returns at most `limit` rows. Caller must use ONLY these wines for
    the ai_comparable_wines field — never invent.
    """
    cur.execute(
        """
        WITH target AS (
            SELECT w.id, w.appellation_id, w.country_id,
                   (SELECT grape_id FROM wine_grapes
                    WHERE wine_id = w.id
                    ORDER BY percentage DESC NULLS LAST LIMIT 1) AS primary_grape_id,
                   (SELECT avg(price_usd) FROM wine_vintage_prices
                    WHERE wine_id = w.id AND price_usd IS NOT NULL) AS target_price
            FROM wines w WHERE w.id = %s
        )
        SELECT DISTINCT ON (w.id)
            w.id, w.display_name, p.name AS producer, a.name AS appellation,
            (SELECT g.display_name FROM wine_grapes wg2
             JOIN grapes g ON wg2.grape_id = g.id
             WHERE wg2.wine_id = w.id
             ORDER BY wg2.percentage DESC NULLS LAST LIMIT 1) AS primary_grape,
            (SELECT avg(price_usd) FROM wine_vintage_prices
             WHERE wine_id = w.id AND price_usd IS NOT NULL) AS avg_price
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
        """,
        (wine_id, limit),
    )
    return [
        {
            "wine_id": str(r[0]),
            "display_name": r[1],
            "producer": r[2],
            "appellation": r[3],
            "grape": r[4],
            "avg_price": float(r[5]) if r[5] else None,
        }
        for r in cur.fetchall()
    ]


def _identify_unknowns(facts: dict) -> list[str]:
    """Build the explicit list of fields the model must NOT invent."""
    vintages = facts.get("vintages") or []
    unknowns: list[str] = []

    if not any(v.get("abv") for v in vintages):
        unknowns.append("ABV")
    if not any(v.get("ph") for v in vintages):
        unknowns.append("pH")
    if not any(v.get("ta_g_l") for v in vintages):
        unknowns.append("TA")
    if not any(v.get("rs_g_l") is not None for v in vintages):
        unknowns.append("residual sugar")
    if not any(v.get("fermentation_vessel") for v in vintages):
        unknowns.append("fermentation vessel")
    if not any(v.get("duration_in_oak_months") for v in vintages):
        unknowns.append("oak aging duration")
    if not any(v.get("oak_origin") for v in vintages):
        unknowns.append("oak origin (French/American/etc.)")
    if not any(v.get("new_oak_pct") for v in vintages):
        unknowns.append("new oak percentage")
    if not any(v.get("mlf") is not None for v in vintages):
        unknowns.append("malolactic fermentation")
    if not any(v.get("closure") for v in vintages):
        unknowns.append("closure (cork/screwcap)")
    if not any(v.get("cases_produced") for v in vintages):
        unknowns.append("annual production volume")
    if not any(v.get("winemaker_notes") for v in vintages):
        unknowns.append("winemaker notes / vinification details")

    if not facts.get("color"):
        unknowns.append("wine color")
    if not facts.get("sweetness_level"):
        unknowns.append("sweetness level")
    if not facts.get("appellation_rules"):
        unknowns.append("appellation legal rules (no DB record)")

    return unknowns


# ── Public API ───────────────────────────────────────────────────────────────


def build_facts_packet(cur, wine_id: str) -> dict:
    """Returns a structured facts dict for a wine, ready to be serialized
    into a prompt. Contains hard facts, vintage chemistry, appellation law,
    explicit unknowns, and pre-fetched comparable wines.
    """
    facts = _fetch_hard_facts(cur, wine_id)
    if not facts:
        return {}
    facts["comparables"] = _fetch_comparable_wines(cur, wine_id, limit=3)
    facts["unknowns"] = _identify_unknowns(facts)
    return facts


def render_facts_packet(facts: dict) -> str:
    """Renders a facts dict into the markdown-style block embedded in prompts.

    Output sections:
        ## WINE IDENTITY
        ## VINTAGE DATA (latest 3)
        ## APPELLATION LAW
        ## CRITIC SCORES
        ## PRICE RANGE
        ## UNKNOWN FIELDS (DO NOT INVENT)
        ## COMPARABLE WINES (only pick from this list)
    """
    if not facts:
        return "(no facts)"

    g_list = ", ".join(
        f"{g['name']} ({g['percentage']}%)" if g.get("percentage") is not None else g["name"]
        for g in facts.get("grapes") or []
    ) or "Not documented"

    vintage_lines: list[str] = []
    for v in facts.get("vintages") or []:
        if not v:
            continue
        parts = [f"Vintage {v.get('vintage_year') or 'NV'}"]
        if v.get("abv"):
            parts.append(f"ABV {v['abv']}%")
        if v.get("ph"):
            parts.append(f"pH {v['ph']}")
        if v.get("ta_g_l"):
            parts.append(f"TA {v['ta_g_l']} g/L")
        if v.get("rs_g_l") is not None:
            parts.append(f"RS {v['rs_g_l']} g/L")
        if v.get("fermentation_vessel"):
            parts.append(f"ferment: {v['fermentation_vessel']}")
        if v.get("duration_in_oak_months"):
            parts.append(f"oak: {v['duration_in_oak_months']} mo")
        if v.get("new_oak_pct"):
            parts.append(f"{v['new_oak_pct']}% new oak")
        if v.get("oak_origin"):
            parts.append(f"{v['oak_origin']} oak")
        if v.get("mlf") is not None:
            parts.append("MLF" if v["mlf"] else "no MLF")
        if v.get("closure"):
            parts.append(f"closure: {v['closure']}")
        if v.get("cases_produced"):
            parts.append(f"{v['cases_produced']} cases")
        if v.get("winemaker_notes"):
            parts.append(f"notes: {v['winemaker_notes'][:200]}")
        vintage_lines.append(", ".join(parts))
    vintage_block = (
        "\n".join(vintage_lines)
        if vintage_lines
        else "Not documented — no vintage-level chemistry or winemaking data in our DB for this wine."
    )

    # Appellation rules block
    ar = facts.get("appellation_rules")
    if ar:
        ar_block = (
            f"**Legal source:** {ar.get('title') or 'appellation rules'} "
            f"({ar.get('organization') or 'unknown organization'})\n"
        )
        if ar.get("url"):
            ar_block += f"**URL:** {ar['url']}\n"
        if ar.get("excerpt"):
            ar_block += f"**Text:** {ar['excerpt'][:1000]}\n"
        if ar.get("rules_json"):
            rj = ar["rules_json"]
            if isinstance(rj, dict):
                if rj.get("colors"):
                    ar_block += f"**Legal colors:** {rj['colors']}\n"
                if rj.get("grape_rules"):
                    ar_block += f"**Grape rules:** {json.dumps(rj['grape_rules'])[:500]}\n"
                if rj.get("aging_requirements"):
                    ar_block += f"**Aging:** {json.dumps(rj['aging_requirements'])[:300]}\n"
    else:
        ar_block = (
            "Not documented — no legal appellation rules in our DB for this wine's appellation. "
            "Describe only the general stylistic tendency of this appellation based on widely-known "
            "facts; do NOT invent specific legal requirements."
        )

    # Scores block
    if facts.get("scores"):
        score_block = "\n".join(
            f"- {s['publication']}: {s['score']}/{s['scale']} ({s['vintage'] or 'NV'})"
            + (f" ({s['medal']} medal)" if s.get("medal") else "")
            for s in facts["scores"]
        )
    else:
        score_block = "Not documented — no critic scores in our DB for this wine."

    # Price
    pr = facts.get("price_range")
    if pr and pr.get("min_usd"):
        price_block = (
            f"${pr['min_usd']:.0f}–${pr['max_usd']:.0f} "
            f"(avg ${pr['avg_usd']:.0f}, {pr['count']} merchant listings)"
        )
    else:
        price_block = "Not documented"

    # Comparables block
    comparables = facts.get("comparables") or []
    if comparables:
        comp_lines = [
            f"- {c['display_name']} — {c.get('grape') or '?'}"
            + (f", ~${c['avg_price']:.0f}" if c.get("avg_price") else "")
            for c in comparables
        ]
        comp_block = "\n".join(comp_lines)
    else:
        comp_block = "No comparable wines found in our catalog with the same primary grape."

    unknowns = facts.get("unknowns") or []
    unknowns_block = (
        "\n".join(f"- {u}" for u in unknowns)
        if unknowns
        else "(all core chemistry fields are documented)"
    )

    return f"""## WINE IDENTITY
- Display name: {facts.get('display_name')}
- Producer: {facts.get('producer_name')}
- Appellation: {facts.get('appellation') or 'None — no appellation assigned'}
- Region: {facts.get('region_name') or 'None'}
- Country: {facts.get('country')}
- Color: {facts.get('color') or 'Not documented'}
- Wine type: {facts.get('wine_type') or 'table'}
- Sweetness level: {facts.get('sweetness_level') or 'Not documented'}
- LWIN: {facts.get('lwin') or 'None'}
- Grapes: {g_list}
- Farming certifications: {', '.join(facts.get('certifications') or []) if facts.get('certifications') else 'None documented'}

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
{comp_block}"""


# ── Standalone test ──────────────────────────────────────────────────────────


def _main() -> int:
    if len(sys.argv) != 2:
        print("Usage: python -m pipeline.enrich.build_facts_packet <wine_id>")
        return 1
    wine_id = sys.argv[1]

    if __name__ == "__main__":
        from dotenv import load_dotenv
        load_dotenv(override=True)

    from pipeline.lib.db import get_conn

    conn = get_conn()
    cur = conn.cursor()
    facts = build_facts_packet(cur, wine_id)
    if not facts:
        print(f"No wine found for id {wine_id}")
        return 1
    print(render_facts_packet(facts))
    print()
    print(f"--- {len(facts.get('unknowns') or [])} unknowns / "
          f"{len(facts.get('comparables') or [])} comparables ---")
    cur.close()
    conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(_main())
