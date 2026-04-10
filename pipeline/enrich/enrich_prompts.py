#!/usr/bin/env python3
"""Prompt builders + shared model call helper for L1 enrichment.

Holds the Grade B / Grade C prompt templates plus a shared `call_model`
helper that handles markdown-fence stripping and JSON parsing.

Voice rules are TIGHTENED relative to the Session 11 L1 prototype based on
the Session 11 audit findings:
  - Top remaining issues on Grade B were vague_hedging (20 tags) and
    generic_filler (16 tags). The new rules name the offending words
    explicitly.
  - Sommelier theater (6 tags) and voice_drift (5 tags) are also targeted.

Both prompts share the same retrieval-grounded structure: facts packet first,
then explicit do-not-invent rules, then the field schema. The model is told
to write declarative statements only and to use "Not documented" or omit
the claim if a fact is missing.

Models (these match the latest Sonnet 4.6 / Haiku 4.5 release IDs):
    SONNET_MODEL = "claude-sonnet-4-6"
    HAIKU_MODEL  = "claude-haiku-4-5-20251001"
"""

from __future__ import annotations

import json
from typing import Any

SONNET_MODEL = "claude-sonnet-4-6"
HAIKU_MODEL = "claude-haiku-4-5-20251001"

PRICING = {
    SONNET_MODEL: {"input": 3.0, "output": 15.0},  # per 1M tokens
    HAIKU_MODEL: {"input": 1.0, "output": 5.0},    # per 1M tokens
}


# ── Voice rules ──────────────────────────────────────────────────────────────


VOICE_RULES_BLOCK = """**VOICE RULES (strict — auditor flags every violation):**

DO NOT use hedging words: may, tends to, appears to, seems to, might, could be, likely, possibly, often, typically, generally, usually, somewhat, fairly, rather.

DO NOT use sommelier theater: exquisite, elegant, harmonious, pairs beautifully, sophisticated, refined, dance of flavors, symphony of, complex interplay, marriage of, expression of terroir, sense of place, beautiful balance, perfectly balanced, masterfully crafted, lovingly, painstakingly.

DO NOT use generic filler: remarkable quality, creating something unique, true expression, reflects the terroir, showcases, embodies, captures the essence, world-class, premium, exceptional.

DO NOT use performative enthusiasm: exclamation marks, "Where to begin?", "What a wine!", "Truly", "Indeed".

DO write in declarative statements. State the fact, then move on. If you do not know something, write "Not documented" or omit the claim entirely. Do not pad sentences with adverbs.

DO be specific. Name soils, places, grape percentages, ABV, vinification methods, real winemakers — but ONLY if they appear in the facts packet.

**COMPARABLES — critical rule:**
Some entries in COMPARABLE WINES have a producer but no specific wine name. When that happens, refer to the producer by name and explain the shared characteristic ("Copain, another small-production Sonoma Zinfandel specialist"). DO NOT invent wine names or cuvées. DO NOT attach an appellation to a producer unless the packet says so.

**SCORES — critical rule:**
CRITIC SCORES lists medals and publication names. When scores are numeric, cite them exactly. When only medals are listed (no numeric score), describe them as counts and publication names ("three TEXSOM medals spanning 2007–2014"). DO NOT invent specific vintage years, specific scores, or years not in the packet."""


# ── Field schemas ────────────────────────────────────────────────────────────


GRADE_B_FIELDS = """{
  "hook": "2-3 sentences. The 30-second story, specific and factual. State what this wine is and why it matters.",
  "wine_summary": "1-2 paragraphs. What makes the producer and wine matter. Use only verified facts and widely-known appellation-level truths.",
  "style_profile": "One phrase. E.g., 'Full-bodied dry red with firm tannins and earthy depth.'",
  "terroir_expression": "1 paragraph. Draw ONLY from the appellation legal text and verified facts. If soil type is unknown, say so.",
  "food_pairing": "3-5 specific dishes with brief reasoning. Real food, not 'delicate stone fruit and charcuterie'.",
  "cellar_recommendation": "1-2 sentences. If drinking window is unknown, write 'Producer does not publish a specific window.'",
  "comparable_wines": "2-3 wines from the COMPARABLE WINES list, one sentence each on why each is comparable.",
  "vinification_summary": "1 paragraph. If winemaker notes are unknown, describe only what is typical for the appellation (citing the legal rule) and label it as category-level."
}"""


GRADE_C_FIELDS = """{
  "hook": "2-3 sentences. REQUIRED content: (a) name the primary grape (or blend composition) from WINE IDENTITY, (b) name the specific appellation or region from WINE IDENTITY, (c) add one specific piece of context — a medal count from CRITIC SCORES, a vintage span from VINTAGE DATA, a price tier from PRICE RANGE, or a grape/appellation characteristic inferable from APPELLATION LAW. DO NOT repeat the producer name if it's already in the wine name. DO NOT use generic opening phrases like 'This wine is...' — lead with what makes it specific.",
  "style_profile": "One phrase naming grape + color + body + one distinguishing trait. E.g., 'Medium-bodied Pinot Noir with cool-climate acidity and firm tannin' — NOT 'Dry red from Oregon.' The phrase must include the grape AND at least one structural descriptor (body, acid, tannin, sweetness, or oak).",
  "comparable_wines": "2-3 producers from the COMPARABLE WINES list. For each, give the producer name + one sentence on the shared characteristic (grape, region, style). Format: 'Producer — reason'. If the list has producers without wine names, just use the producer name. Do NOT invent wine or cuvée names. If the packet says 'No comparable wines found', write 'No close comparables in the Loam catalog yet.'"
}"""


# ── Common preamble ──────────────────────────────────────────────────────────


def _preamble(facts_packet_markdown: str, fields_schema: str, comp_instruction: str) -> str:
    return f"""You are writing a catalog entry for a wine. Your writing is direct, specific, and knowledgeable.

**CRITICAL RULE:** Use ONLY the facts provided below. For any field marked "Not documented" or listed under UNKNOWNS, do NOT invent. Either omit the claim, or explicitly say "Not documented."

**SAFE INFERENCES ALLOWED:**
- Grape → typical flavor profile (e.g., Nebbiolo brings tar and rose, Cabernet Sauvignon brings cassis and structure)
- Appellation → broad stylistic tendency from widely-known facts
- Sugar level + acidity → food-pairing logic (acid cuts fat, RS tames chili heat)
- Documented oak → mouthfeel implications (oak-aged whites tend rounder, more vanilla; new oak adds toast)

**NEVER INVENT:**
- Specific soil types (unless in appellation rules)
- Specific ABV, pH, or other numeric values
- Specific vineyard aspects, elevations, plantings, vine ages
- Specific vinification details not in the facts (clones, yeast strains, élevage, MLF)
- Wines that are not in the COMPARABLE WINES list
- Producer history dates, founding stories, family lineage not in the facts
- Specific awards, scores, or critic quotes not in the facts

{VOICE_RULES_BLOCK}

---

{facts_packet_markdown}

{comp_instruction}

---

## OUTPUT FORMAT

Return ONLY a JSON object matching this schema. No markdown fences, no preamble, no trailing commentary.

{fields_schema}
"""


# ── Public prompt builders ───────────────────────────────────────────────────


def _comparable_instruction(facts_packet_markdown: str) -> str:
    """The instruction to attach near the comparables list — depends on whether
    the packet has any comparables. We detect by reading a line in the rendered
    packet."""
    if "No comparable wines found" in facts_packet_markdown:
        return (
            "**Comparables instruction:** For comparable_wines, write "
            "'No close comparables in the Loam catalog yet.' Do NOT invent wines."
        )
    return (
        "**Comparables instruction:** For comparable_wines, pick 2-3 wines ONLY from "
        "the COMPARABLE WINES list above. Do NOT invent or reference wines that are "
        "not in this list. For each chosen wine, write one sentence on why it's "
        "comparable to the target wine."
    )


def build_grade_b_prompt(facts_packet_markdown: str) -> str:
    """L1 prompt for Grade B (8 narrative fields). Uses Sonnet."""
    return _preamble(
        facts_packet_markdown,
        GRADE_B_FIELDS,
        _comparable_instruction(facts_packet_markdown),
    )


def build_grade_c_prompt(facts_packet_markdown: str) -> str:
    """L1 prompt for Grade C (3 fields: hook, style, comparable). Uses Haiku."""
    return _preamble(
        facts_packet_markdown,
        GRADE_C_FIELDS,
        _comparable_instruction(facts_packet_markdown),
    )


def build_retry_prompt(
    facts_packet_markdown: str,
    grade: str,
    flagged_claims: list[dict],
) -> str:
    """L1 retry prompt with L3-flagged claims as corrections.

    Used when the L3 fact-check pass flags high-severity issues. The original
    enrichment is implicitly thrown away — we just regenerate from scratch with
    the additional 'do not write these specific claims' constraint.
    """
    base = build_grade_b_prompt(facts_packet_markdown) if grade == "B" \
        else build_grade_c_prompt(facts_packet_markdown)

    if not flagged_claims:
        return base

    correction_lines = ["", "---", "", "**FACT-CHECK FAILURES FROM PREVIOUS ATTEMPT — DO NOT REPEAT:**", ""]
    for fc in flagged_claims:
        field = fc.get("field", "?")
        claim = (fc.get("claim") or "").strip()
        issue = fc.get("issue", "?")
        fix = fc.get("suggested_fix", "remove")
        correction_lines.append(
            f"- In `{field}`, you previously wrote: \"{claim[:200]}\". "
            f"Issue: {issue}. Fix: {fix}. Either omit this claim or replace with a "
            f"verifiable fact from the facts packet."
        )

    return base + "\n".join(correction_lines)


# ── Shared model call helper ─────────────────────────────────────────────────


def call_model(
    client,
    prompt: str,
    model: str,
    max_tokens: int = 2000,
) -> tuple[dict | None, dict, str | None]:
    """Shared Claude call + JSON parsing + markdown fence stripping.

    Returns (parsed_json, usage_dict, error_or_none). On success, error is None.
    On failure, parsed is None and error contains the message.
    """
    try:
        msg = client.messages.create(
            model=model,
            max_tokens=max_tokens,
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
    except json.JSONDecodeError as e:
        return None, {
            "input_tokens": getattr(getattr(msg, "usage", None), "input_tokens", 0) if "msg" in locals() else 0,
            "output_tokens": getattr(getattr(msg, "usage", None), "output_tokens", 0) if "msg" in locals() else 0,
        }, f"JSONDecodeError: {str(e)[:200]}"
    except Exception as e:
        return None, {}, str(e)[:300]


def cost_for(usage: dict, model: str) -> float:
    """Compute USD cost for a single call's usage."""
    p = PRICING.get(model)
    if not p or not usage:
        return 0.0
    return (
        usage.get("input_tokens", 0) * p["input"]
        + usage.get("output_tokens", 0) * p["output"]
    ) / 1_000_000
