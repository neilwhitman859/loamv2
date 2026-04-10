#!/usr/bin/env python3
"""L3 fact-check pass — Haiku validates enrichment claims against ground truth.

The L3 layer of the three-layer enrichment design (Session 11):
  L1: retrieval-grounded prompt (build_facts_packet + enrich_prompts)
  L2: per-field constraints (DB-only comparables, appellation_rules terroir)
  L3: post-generation Haiku fact-check, retry once if any high-severity flag

Workflow used by stage1_revalidate.py and the future full re-enrichment script:
    1. Generate enrichment via L1 (Sonnet for Grade B, Haiku for Grade C)
    2. Run fact_check() on the enrichment + facts packet
    3. If verified == True → done, status = "passed"
    4. If only low/medium flags → done, status = "partial" (kept as-is)
    5. If any high-severity flag → call retry_with_flags() ONCE
       - if retry passes → status = "retried_passed"
       - if retry still fails → status = "failed", drop the problem fields

Module structure:
    fact_check(client, enrichment, facts_packet, grade) -> dict
    retry_with_flags(client, prompt_builder, facts_packet, flagged, grade) -> dict
    run_l3_loop(client, generation_fn, facts_packet, grade) -> dict
"""

from __future__ import annotations

import json
from typing import Any, Callable

from pipeline.enrich.build_facts_packet import render_facts_packet
from pipeline.enrich.enrich_prompts import (
    HAIKU_MODEL,
    SONNET_MODEL,
    build_grade_b_prompt,
    build_grade_c_prompt,
    build_retry_prompt,
    call_model,
    cost_for,
)


# ── Fact-check prompt ────────────────────────────────────────────────────────


FACT_CHECK_PROMPT = """You are a fact-checker for wine catalog entries. Your job: identify every specific factual claim in the enrichment below, then check whether each claim is supported by the ground-truth facts packet.

## ENRICHMENT TO CHECK
{enrichment_json}

## GROUND TRUTH (only source of verifiable facts for this wine)
{facts_packet_markdown}

## TASK

For each factual claim in the enrichment, determine:
1. Is it a SPECIFIC claim? Specific claims include:
   - Numbers (ABV, pH, vine age, elevation, hectares, year founded)
   - Soil types or geological features
   - Grape percentages
   - Vinification methods (yeast strain, fermentation vessel, oak details, MLF)
   - Named comparable wines (must come from the COMPARABLE WINES list)
   - Producer history dates or named family members
   - Vintage-specific weather, harvest dates, scores
2. Is the specific claim supported by the ground truth?
   - "supported": the exact fact is in the ground truth (or is a safe inference)
   - "not_in_facts": the fact is not in the ground truth AND is not a safe inference
   - "contradicted": the ground truth says something different
3. Safe inferences are OK and should NOT be flagged:
   - Grape character (e.g., "Nebbiolo brings tar and rose")
   - Broad appellation style (e.g., "Russian River Chardonnay tends toward riper, oak-influenced expressions")
   - Acid-cuts-fat food pairing logic
   - Oak-means-round-mouthfeel implications
   - Color → broad sensory profile
4. Voice issues (hedging words, sommelier theater, generic filler) are NOT your job — only flag factual claims.

## TRUST RULES (do not flag these)

- **COMPARABLE WINES producers:** If the enrichment references a producer by name that appears in the COMPARABLE WINES section of the ground truth, treat the producer reference as SUPPORTED even if the enrichment omits, abbreviates, or rephrases the appellation/region. Flag ONLY if the producer name itself is not in the list, OR the enrichment invents a specific wine/cuvée name that is not in the list.
- **CRITIC SCORES counts and publications:** If the CRITIC SCORES section lists medals from a publication, the enrichment may describe medal counts, spans of years, or the publication name without citing each individual vintage. Flag ONLY if the enrichment invents a specific vintage year, numeric score, or publication that is not in the section.
- **APPELLATION LAW inferences:** Anything derived from the APPELLATION LAW text in the packet is SUPPORTED, including minimum ABV, yield caps, authorized grapes, and aging requirements. Do not re-verify legal text.
- **Claims marked "Not documented":** If the enrichment explicitly states something is "Not documented" or "unknown", that is SUPPORTED (the enrichment is correctly acknowledging a gap).

## OUTPUT FORMAT

Return ONLY a JSON object. No markdown fences, no preamble.

{{
  "verified": true_or_false,
  "flagged": [
    {{
      "field": "ai_hook|ai_wine_summary|ai_style_profile|ai_terroir_expression|ai_food_pairing|ai_cellar_recommendation|ai_comparable_wines|ai_vinification_summary",
      "claim": "the specific sentence or phrase from the enrichment",
      "issue": "not_in_facts|contradicted",
      "severity": "high|medium|low",
      "suggested_fix": "remove|mark_unknown|replace_with_facts"
    }}
  ],
  "notes": "one sentence on overall quality"
}}

Severity guide:
- "high": the claim is verifiable and wrong, OR a specific number / soil / vinification detail not in the facts
- "medium": a vague but unsupported claim (named family member, dated history)
- "low": a borderline inference that leans on something not strictly documented

Set "verified": true ONLY if the flagged list is empty.
"""


# ── Public API ───────────────────────────────────────────────────────────────


def fact_check(
    client,
    enrichment: dict,
    facts_packet: dict,
    grade: str,
    enrichment_field_prefix: str = "ai_",
) -> dict:
    """Run Haiku to extract and verify factual claims.

    Returns:
        {
          "verified": bool,        # true if no flagged claims
          "flagged": [...],
          "notes": str,
          "usage": {...},
          "cost_usd": float,
          "error": str | None,
        }
    """
    if not enrichment:
        return {
            "verified": False,
            "flagged": [],
            "notes": "no enrichment to check",
            "usage": {},
            "cost_usd": 0.0,
            "error": "empty enrichment",
        }

    # Re-key the enrichment to use ai_* field names so the L3 prompt's field
    # enum lines up with how the auditor expects things.
    keyed = {}
    field_map = {
        "hook": "ai_hook",
        "wine_summary": "ai_wine_summary",
        "style_profile": "ai_style_profile",
        "terroir_expression": "ai_terroir_expression",
        "food_pairing": "ai_food_pairing",
        "cellar_recommendation": "ai_cellar_recommendation",
        "comparable_wines": "ai_comparable_wines",
        "vinification_summary": "ai_vinification_summary",
    }
    for k, v in enrichment.items():
        keyed[field_map.get(k, f"{enrichment_field_prefix}{k}")] = v

    facts_md = render_facts_packet(facts_packet)
    prompt = FACT_CHECK_PROMPT.format(
        enrichment_json=json.dumps(keyed, indent=2, ensure_ascii=False),
        facts_packet_markdown=facts_md,
    )

    parsed, usage, err = call_model(client, prompt, HAIKU_MODEL, max_tokens=2000)

    if err or not parsed:
        return {
            "verified": False,
            "flagged": [],
            "notes": "fact-check call failed",
            "usage": usage,
            "cost_usd": cost_for(usage, HAIKU_MODEL),
            "error": err or "no parsed response",
        }

    flagged = parsed.get("flagged") or []
    # Normalize flag entries to a known shape
    normalized_flags = []
    for f in flagged:
        if not isinstance(f, dict):
            continue
        normalized_flags.append({
            "field": f.get("field", "?"),
            "claim": f.get("claim", ""),
            "issue": f.get("issue", "?"),
            "severity": (f.get("severity") or "medium").lower(),
            "suggested_fix": f.get("suggested_fix", "remove"),
        })

    verified_flag = parsed.get("verified")
    # Trust the model's verified bool only if it agrees with the flag list
    verified = bool(verified_flag) and len(normalized_flags) == 0

    return {
        "verified": verified,
        "flagged": normalized_flags,
        "notes": parsed.get("notes", ""),
        "usage": usage,
        "cost_usd": cost_for(usage, HAIKU_MODEL),
        "error": None,
    }


def retry_with_flags(
    client,
    facts_packet: dict,
    flagged_claims: list[dict],
    grade: str,
) -> tuple[dict | None, dict, str | None, float]:
    """Re-call the generation model with the flagged claims as corrections.

    Returns (parsed_enrichment, usage, error, cost_usd). The model used matches
    the original generation model for the grade (Sonnet for B, Haiku for C).
    """
    facts_md = render_facts_packet(facts_packet)
    retry_prompt = build_retry_prompt(facts_md, grade, flagged_claims)
    model = SONNET_MODEL if grade == "B" else HAIKU_MODEL
    parsed, usage, err = call_model(client, retry_prompt, model, max_tokens=2000)
    return parsed, usage, err, cost_for(usage, model)


# ── End-to-end loop helper ───────────────────────────────────────────────────


def run_l3_loop(
    client,
    facts_packet: dict,
    grade: str,
) -> dict:
    """Full L1 + L3 generate-and-verify loop for one wine.

    Steps:
        1. Generate enrichment via L1 (Sonnet B / Haiku C)
        2. Run fact_check on the result
        3. If high-severity flags → retry_with_flags ONCE
        4. Re-run fact_check on retry
        5. If retry still has high-severity flags → drop those fields, mark "failed"

    Returns:
        {
          "enrichment": dict,            # final enrichment (possibly with fields dropped)
          "fact_check_status": str,      # passed | retried_passed | partial | failed
          "fact_check_flags": list,      # final flags (or empty)
          "retries": int,                # 0 or 1
          "model_used": str,
          "cost_usd": float,
          "usage": {...},
          "error": str | None,
        }
    """
    facts_md = render_facts_packet(facts_packet)
    gen_model = SONNET_MODEL if grade == "B" else HAIKU_MODEL

    # ── L1 generation ────────────────────────────────────────────────────
    if grade == "B":
        prompt = build_grade_b_prompt(facts_md)
    else:
        prompt = build_grade_c_prompt(facts_md)

    parsed, gen_usage, gen_err = call_model(client, prompt, gen_model, max_tokens=2000)
    total_cost = cost_for(gen_usage, gen_model)
    total_input = gen_usage.get("input_tokens", 0) if gen_usage else 0
    total_output = gen_usage.get("output_tokens", 0) if gen_usage else 0

    if gen_err or not parsed:
        return {
            "enrichment": None,
            "fact_check_status": "failed",
            "fact_check_flags": [],
            "retries": 0,
            "model_used": gen_model,
            "cost_usd": total_cost,
            "usage": {"input_tokens": total_input, "output_tokens": total_output},
            "error": gen_err or "no parsed enrichment",
        }

    # ── L3 fact-check ────────────────────────────────────────────────────
    fc = fact_check(client, parsed, facts_packet, grade)
    total_cost += fc.get("cost_usd", 0.0)
    total_input += fc.get("usage", {}).get("input_tokens", 0)
    total_output += fc.get("usage", {}).get("output_tokens", 0)

    if fc.get("verified"):
        return {
            "enrichment": parsed,
            "fact_check_status": "passed",
            "fact_check_flags": [],
            "retries": 0,
            "model_used": gen_model,
            "cost_usd": total_cost,
            "usage": {"input_tokens": total_input, "output_tokens": total_output},
            "error": None,
        }

    flags = fc.get("flagged") or []
    high_severity = [f for f in flags if f.get("severity") == "high"]

    # Only flagged with low/medium → keep as-is, mark partial
    if not high_severity:
        return {
            "enrichment": parsed,
            "fact_check_status": "partial",
            "fact_check_flags": flags,
            "retries": 0,
            "model_used": gen_model,
            "cost_usd": total_cost,
            "usage": {"input_tokens": total_input, "output_tokens": total_output},
            "error": None,
        }

    # ── L1 retry with flagged claims as corrections ──────────────────────
    retry_parsed, retry_usage, retry_err, retry_cost = retry_with_flags(
        client, facts_packet, flags, grade
    )
    total_cost += retry_cost
    total_input += retry_usage.get("input_tokens", 0) if retry_usage else 0
    total_output += retry_usage.get("output_tokens", 0) if retry_usage else 0

    if retry_err or not retry_parsed:
        # Retry failed entirely — return the original with the flags noted
        return {
            "enrichment": parsed,
            "fact_check_status": "failed",
            "fact_check_flags": flags,
            "retries": 1,
            "model_used": gen_model,
            "cost_usd": total_cost,
            "usage": {"input_tokens": total_input, "output_tokens": total_output},
            "error": retry_err or "retry returned no enrichment",
        }

    # Re-check the retry
    fc2 = fact_check(client, retry_parsed, facts_packet, grade)
    total_cost += fc2.get("cost_usd", 0.0)
    total_input += fc2.get("usage", {}).get("input_tokens", 0)
    total_output += fc2.get("usage", {}).get("output_tokens", 0)

    if fc2.get("verified"):
        return {
            "enrichment": retry_parsed,
            "fact_check_status": "retried_passed",
            "fact_check_flags": [],
            "retries": 1,
            "model_used": gen_model,
            "cost_usd": total_cost,
            "usage": {"input_tokens": total_input, "output_tokens": total_output},
            "error": None,
        }

    flags2 = fc2.get("flagged") or []
    high2 = [f for f in flags2 if f.get("severity") == "high"]

    # Keep the retry content regardless of remaining flags. Dropping fields on
    # "failed" was tanking Grade C scores in Stage 1 — the auditor scored the
    # None-filled rows worse than the flagged content. The flags are preserved
    # in fact_check_flags so downstream can still warn, filter, or re-run.
    status = "partial" if not high2 else "failed"
    return {
        "enrichment": retry_parsed,
        "fact_check_status": status,
        "fact_check_flags": flags2,
        "retries": 1,
        "model_used": gen_model,
        "cost_usd": total_cost,
        "usage": {"input_tokens": total_input, "output_tokens": total_output},
        "error": None,
    }
