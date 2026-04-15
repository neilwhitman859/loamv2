#!/usr/bin/env python3
"""B5.6: Score Task 3 prose outputs via Opus judge.

Sends each model output + wine context + rubric to Opus via OpenRouter.
Opus fact-checks claims against its own knowledge and scores on 4 per-wine
dimensions. Structural diversity is computed separately as a batch metric.

Usage:
    python -m bakeoff.run_task3_judge                     # score all
    python -m bakeoff.run_task3_judge --model gpt-5.4     # single model (partial slug match)
    python -m bakeoff.run_task3_judge --wine <id>         # single wine
    python -m bakeoff.run_task3_judge --dry-run            # show prompts, no API calls
    python -m bakeoff.run_task3_judge --summary-only       # re-generate summaries from saved scores
"""

import argparse
import csv
import json
import os
import re
import sys
import time
from pathlib import Path

os.environ["PYTHONUNBUFFERED"] = "1"

import requests as http_requests
from dotenv import load_dotenv

load_dotenv()

OPENROUTER_API_KEY = os.getenv("OPENROUTER_API_KEY")
OPENROUTER_URL = "https://openrouter.ai/api/v1/chat/completions"
JUDGE_MODEL = "anthropic/claude-opus-4.6"

BASE_DIR = Path(__file__).resolve().parent
CONTEXTS_FILE = BASE_DIR / "data" / "task3" / "contexts.json"
GROUND_TRUTH_FILE = BASE_DIR / "data" / "task3" / "ground_truth.json"
RESULTS_DIR = BASE_DIR / "results" / "task3"
SCORES_DIR = BASE_DIR / "scores"
JUDGE_DIR = SCORES_DIR / "task3_judge"

# All 21 models from run_task3.py
TASK3_MODELS = [
    "anthropic/claude-sonnet-4.6",
    "anthropic/claude-haiku-4.5",
    "anthropic/claude-opus-4.6",
    "deepseek/deepseek-v3.2",
    "google/gemini-2.5-flash",
    "google/gemini-2.5-pro",
    "meta-llama/llama-4-maverick",
    "mistralai/mistral-large",
    "mistralai/mistral-large-2512",
    "mistralai/mistral-nemo",
    "openai/gpt-5.4",
    "openai/gpt-5.4-mini",
    "openai/gpt-5-mini",
    "qwen/qwen3.5-plus-02-15",
    "qwen/qwen3.6-plus",
    "google/gemini-3.1-pro-preview",
    "google/gemini-3-flash-preview",
    "xiaomi/mimo-v2-pro",
    "xiaomi/mimo-v2-flash",
    "minimax/minimax-m2.7",
    "x-ai/grok-4.1-fast",
]

MODEL_PRICING = {
    "anthropic/claude-sonnet-4.6": (3.0, 15.0),
    "anthropic/claude-haiku-4.5": (0.80, 4.0),
    "anthropic/claude-opus-4.6": (15.0, 75.0),
    "deepseek/deepseek-v3.2": (0.14, 0.28),
    "google/gemini-2.5-flash": (0.15, 0.60),
    "google/gemini-2.5-pro": (1.25, 10.0),
    "meta-llama/llama-4-maverick": (0.20, 0.60),
    "mistralai/mistral-large": (2.0, 6.0),
    "mistralai/mistral-large-2512": (2.0, 6.0),
    "mistralai/mistral-nemo": (0.035, 0.08),
    "openai/gpt-5.4": (2.50, 10.0),
    "openai/gpt-5.4-mini": (0.40, 1.60),
    "openai/gpt-5-mini": (0.30, 1.20),
    "qwen/qwen3.5-plus-02-15": (0.40, 1.20),
    "qwen/qwen3.6-plus": (0.40, 1.20),
    "google/gemini-3.1-pro-preview": (1.25, 10.0),
    "google/gemini-3-flash-preview": (0.15, 0.60),
    "xiaomi/mimo-v2-pro": (0.50, 2.0),
    "xiaomi/mimo-v2-flash": (0.07, 0.28),
    "minimax/minimax-m2.7": (0.50, 2.0),
    "x-ai/grok-4.1-fast": (0.60, 4.0),
}

BANNED_WORDS = [
    "genuinely", "genuine", "elegant", "exquisite", "harmonious", "remarkable",
    "world-class", "showcases", "embodies", "captures the essence", "refined",
    "sophisticated", "masterfully", "lovingly", "painstakingly", "defensible",
    "intellectually honest", "intellectually serious",
]

# ── Voice reference (condensed for judge prompt) ──────────────

VOICE_REFERENCE = """Loam's voice is knowledgeable but approachable — like a sommelier friend who explains the "why" behind everything. Be specific (name soils, climate patterns, geological formations). Connect place to taste. Have a point of view. No generic filler, no sommelier theater, no vague hedging.

Key voice rules:
- Be specific: name the soil type, climate pattern, geological formation
- Connect place to taste: every fact should point toward why the wine tastes the way it does
- State what you know directly — "Etna Rosso is having a moment" not "seems to be gaining popularity"
- Be honest about what you don't know
- Give real information, not atmosphere — every sentence teaches something
- Be comfortable saying negative things when warranted
- Food pairings: name real dishes (Nashville hot chicken, not "grilled meats"), explain why the pairing works

BANNED WORDS (zero tolerance): genuinely, genuine, elegant, exquisite, harmonious, remarkable, world-class, showcases, embodies, captures the essence, refined, sophisticated, masterfully, lovingly, painstakingly

DON'T DO: generic filler ("creating something unique"), sommelier theater ("an exquisite accompaniment"), vague hedging ("seems to be"), overly poetic language, performative enthusiasm."""


# ── Judge prompt ──────────────────────────────────────────────

JUDGE_PROMPT = """You are scoring a wine enrichment output for the Loam wine platform. You are an expert wine judge with deep knowledge of wine regions, producers, and winemaking.

## Your Task

Score this output on 4 dimensions. The output was generated by an AI model given the wine context data below. You do NOT know which model produced it — score purely on quality.

## CRITICAL: Correctness Is Everything

Loam's product is structured, trustworthy data. Wrong facts make it AI slop. Your primary job is FACT-CHECKING.

For every specific claim in the output:
1. Is it present in the provided context data? If yes, verify the output matches.
2. If NOT in the context data, is it correct based on your wine knowledge? If yes, it ADDS value.
3. If NOT in the context data AND wrong or unverifiable, it is a FABRICATION.

Wrong facts — especially in the hook (the first thing users read) — are devastating. The hook is the first sentence a user reads. A wrong fact there destroys trust in everything that follows.

## Scoring Rubric (1.0-5.0, half-points allowed)

### Dimension 1: CORRECTNESS (weight: 35%)
Does the output state true things? Does it avoid fabricating?

- 5.0: Every claim is either verifiable from context data or correct from wine knowledge. No fabrications. Adds value through correct training knowledge.
- 4.0: One minor unsourced claim that's plausibly true. All numbers match. No wrong facts.
- 3.0: Mixes correct claims with some unverifiable additions. Nothing provably wrong.
- 2.0: States specific claims that are wrong (wrong geography, wrong naming origin, wrong competitor comparisons). Or fabricates numbers not in the data (pH, oak months, cases).
- 1.0: Multiple wrong facts. Contradicts provided data. Fundamentally unreliable.

CRITICAL — apply these hard caps STRICTLY:
HARD PENALTY: Any output that fabricates a specific number (pH, ABV, blend %, cases, oak months) NOT present in the context data → MAX 1.5 on correctness. No exceptions.
HARD PENALTY: Any output with a provably wrong factual claim in the HOOK field → MAX 1.5 on correctness. The hook is the first thing users see. A wrong naming origin, wrong grape, wrong geography in the hook poisons the entire entry. Examples of wrong hook claims: fabricated naming origins for the wine, wrong vineyard designations, calling a blended wine "single-vineyard" when it isn't.
NOTE: If a claim in the hook is correct but not in the context data (e.g., the model knows the true naming origin from training data), that is GOOD — reward it with a higher correctness score.

### Dimension 2: VOICE (weight: 25%)
Does it sound like Loam? Direct, opinionated, explains the "why," names names. Comfortable being negative.

- 5.0: Indistinguishable from great wine writing. Direct, opinionated, specific. Takes a side.
- 4.0: Right voice with minor lapses. One hedge too many, or one generic phrase.
- 3.0: Recognizable attempt but with template patterns, hedging, or generic praise.
- 2.0: Generic wine writing. Could be any wine website.
- 1.0: Sommelier theater or chatbot wine copy.

HARD PENALTY: Any output containing a BANNED WORD gets MAX 2.0 on this dimension.
PENALTY: Excessive length (>2500 tokens) that dilutes directness drops this by at least 1.0.

Banned words: genuinely, genuine, elegant, exquisite, harmonious, remarkable, world-class, showcases, embodies, captures the essence, refined, sophisticated, masterfully, lovingly, painstakingly

### Dimension 3: SPECIFICITY (weight: 20%)
Does it name actual things, or describe in generalities?

- 5.0: Names specific soils, climate patterns, geological formations, real competitors with reasoning.
- 4.0: Mostly specific with one generic phrase that could be cut.
- 3.0: Mix of specific and generic. Some good details surrounded by filler.
- 2.0: Mostly generic. "Well-drained soils," "favorable climate," "careful winemaking."
- 1.0: Entirely generic. Pure atmosphere.

### Dimension 4: USEFULNESS (weight: 20%)
Would a wine enthusiast learn something? Does the insider take help someone decide?

- 5.0: Reader comes away understanding something new. Insider take gives a clear, actionable opinion.
- 4.0: Good information density. One or two weak spots but overall teaches well.
- 3.0: Correct but not enlightening. Reorganizes prompt data into prose without adding insight.
- 2.0: Mostly atmosphere, little substance. Insider take is generic.
- 1.0: Adds nothing. Pure filler.

## Voice Reference
{voice_ref}

## Wine Context Data (what the model was given)
{context_data}

## VERIFIED GROUND TRUTH (from web-grounded research)
These are facts researched from official producer tech sheets and authoritative sources.
Use this to fact-check the output's specific claims. The model did NOT see this — it only
saw the context data above. If a model states something that contradicts ground truth,
it's WRONG regardless of how confident or plausible it sounds. If a model states something
that MATCHES ground truth (even if not in the context data), that's GOOD — reward it.

{ground_truth}

## Output to Score
{model_output}

## Response Format (JSON only, no markdown fencing)

Return ONLY valid JSON:
{{"correctness": <1.0-5.0>, "voice": <1.0-5.0>, "specificity": <1.0-5.0>, "usefulness": <1.0-5.0>, "wrong_facts": ["list of specific wrong claims found, or empty"], "banned_words_found": ["list of banned words found, or empty"], "fabricated_numbers": ["list of numbers not in context data, or empty"], "reasoning": "2-3 sentences: what drove the scores. Be specific about what's good and what's wrong."}}"""


def model_slug(model_id: str) -> str:
    return model_id.replace("/", "__")


def format_context_for_judge(wine: dict) -> str:
    """Format the wine context data as the judge sees it."""
    ctx = wine["context"]
    lines = [
        f"Wine: {ctx['display_name']}",
        f"Producer: {ctx['producer_name']}",
        f"Country: {ctx.get('country_name', 'Unknown')}",
        f"Region: {ctx.get('region_name', 'Unknown')}",
        f"Appellation: {ctx.get('appellation_name', 'Unknown')}",
        f"Type: {ctx.get('wine_type', 'table')} {ctx.get('color', '')}",
    ]

    grapes = ctx.get("grapes", [])
    if grapes:
        parts = [f"{g['name']} ({g.get('percentage', '?')}%)" for g in grapes]
        lines.append(f"Grapes: {', '.join(parts)}")
    else:
        lines.append("Grapes: NOT PROVIDED")

    vintages = ctx.get("vintages", [])
    if vintages:
        vlines = []
        for v in vintages:
            parts = [f"Year {v.get('vintage_year', 'NV')}"]
            for key, label in [("abv", "ABV"), ("ph", "pH"), ("ta_g_l", "TA"),
                               ("rs_g_l", "RS"), ("cases_produced", "Cases"),
                               ("duration_in_oak_months", "Oak months"),
                               ("new_oak_pct", "New oak %")]:
                if v.get(key):
                    parts.append(f"{label}: {v[key]}")
            vlines.append(", ".join(parts))
        lines.append(f"\nVintage data:\n" + "\n".join(vlines))
    else:
        lines.append("Vintage data: NONE PROVIDED")

    scores = ctx.get("scores", [])
    if scores:
        slines = []
        for s in scores[:10]:
            pub = s.get("publication") or "Unknown"
            vy = s.get("vintage_year", "NV")
            if s.get("medal"):
                slines.append(f"  {pub}: {s['medal']} ({vy})")
            elif s.get("score"):
                slines.append(f"  {pub}: {s['score']}/{s.get('score_scale', '?')} ({vy})")
        lines.append(f"\nScores:\n" + "\n".join(slines))

    prices = ctx.get("prices", [])
    if prices:
        plines = [f"  ${p.get('price_usd', '?')} from {p.get('merchant_name', '?')} ({p.get('vintage_year', 'NV')})"
                  for p in prices[:5]]
        lines.append(f"\nPrices:\n" + "\n".join(plines))
    else:
        lines.append("Prices: NONE PROVIDED")

    # Cascade context
    ai = ctx.get("appellation_insight", {})
    if ai and ai.get("overview"):
        lines.append(f"\nAppellation context (enriched): {str(ai['overview'])[:500]}")
    pi = ctx.get("producer_insight", {})
    if pi and pi.get("style"):
        lines.append(f"\nProducer style (enriched): {str(pi['style'])[:300]}")
    gfp = ctx.get("grape_flavor_profile", "")
    if gfp:
        lines.append(f"\nGrape flavor profile (enriched): {str(gfp)[:300]}")

    return "\n".join(lines)


def format_output_for_judge(result: dict) -> str:
    """Format the model output for the judge (blinded — no model name)."""
    if not result.get("parse_ok"):
        raw = result.get("raw_content", "")
        return f"[OUTPUT FAILED TO PARSE AS JSON]\n\nRaw text:\n{raw[:2000]}"

    p = result.get("parsed", {})
    sections = []
    for field in ["hook", "wine_summary", "terroir_expression", "vinification_summary",
                   "food_pairing", "comparable_wines", "style_profile",
                   "cellar_recommendation", "value_assessment", "market_position",
                   "insider_take"]:
        val = p.get(field)
        if val is None:
            continue
        if isinstance(val, list):
            val = "\n".join(str(x) for x in val)
        elif isinstance(val, dict):
            val = json.dumps(val, indent=2)
        sections.append(f"[{field}]\n{val}")

    # Include sensory and numeric fields
    sensory = p.get("sensory", {})
    if sensory:
        sections.append(f"[sensory]\n{json.dumps(sensory, indent=2)}")

    for nf in ["aging_potential_years", "drinking_window_min_years", "drinking_window_max_years"]:
        if p.get(nf) is not None:
            sections.append(f"[{nf}]: {p[nf]}")

    return "\n\n".join(sections)


def call_judge(prompt: str, retries: int = 2) -> dict:
    """Call Opus judge via OpenRouter."""
    headers = {
        "Authorization": f"Bearer {OPENROUTER_API_KEY}",
        "Content-Type": "application/json",
        "HTTP-Referer": "https://loam.onrender.com",
        "X-Title": "Loam Bake-off Judge",
    }

    body = {
        "model": JUDGE_MODEL,
        "messages": [{"role": "user", "content": prompt}],
        "temperature": 0.0,
        "max_tokens": 1000,
    }

    for attempt in range(retries + 1):
        try:
            t0 = time.time()
            resp = http_requests.post(OPENROUTER_URL, headers=headers, json=body, timeout=120)
            elapsed = time.time() - t0

            if resp.status_code == 429:
                wait = min(2 ** (attempt + 2), 60)
                print(f"    Rate limited, waiting {wait}s...")
                time.sleep(wait)
                continue

            if resp.status_code != 200:
                if attempt < retries:
                    time.sleep(3)
                    continue
                return {"error": f"HTTP {resp.status_code}: {resp.text[:300]}", "elapsed": elapsed}

            data = resp.json()
            content = data.get("choices", [{}])[0].get("message", {}).get("content", "")
            usage = data.get("usage", {})

            return {
                "raw_content": content,
                "tokens_in": usage.get("prompt_tokens", 0),
                "tokens_out": usage.get("completion_tokens", 0),
                "elapsed": elapsed,
                "error": None,
            }

        except http_requests.exceptions.Timeout:
            if attempt < retries:
                time.sleep(3)
                continue
            return {"error": "Timeout", "elapsed": 120}
        except Exception as e:
            if attempt < retries:
                time.sleep(3)
                continue
            return {"error": str(e), "elapsed": 0}

    return {"error": "Max retries exceeded", "elapsed": 0}


def parse_judge_response(raw: str) -> dict:
    """Parse the judge's JSON response."""
    if not raw:
        return None

    cleaned = re.sub(r"^```(?:json)?\s*", "", raw.strip())
    cleaned = re.sub(r"\s*```$", "", cleaned.strip())

    # Find outermost JSON object
    depth = 0
    start = None
    for i, ch in enumerate(cleaned):
        if ch == '{':
            if depth == 0:
                start = i
            depth += 1
        elif ch == '}':
            depth -= 1
            if depth == 0 and start is not None:
                try:
                    return json.loads(cleaned[start:i + 1])
                except json.JSONDecodeError:
                    pass

    try:
        return json.loads(cleaned)
    except json.JSONDecodeError:
        return None


def compute_composite(scores: dict) -> float:
    """Compute weighted composite score."""
    weights = {
        "correctness": 0.35,
        "voice": 0.25,
        "specificity": 0.20,
        "usefulness": 0.20,
    }
    total = 0
    for dim, weight in weights.items():
        total += scores.get(dim, 1.0) * weight
    return round(total, 3)


def format_ground_truth(gt_entry: dict | None) -> str:
    """Format ground truth entry as human-readable verified facts."""
    if not gt_entry:
        return "(No verified ground truth available for this wine. Judge on general plausibility.)"

    lines = [f"**Wine:** {gt_entry.get('display_name', '')}"]
    facts = gt_entry.get("verified_facts", {})
    for key, val in facts.items():
        if isinstance(val, list):
            val = " / ".join(str(x) for x in val)
        lines.append(f"- **{key}:** {val}")
    lines.append(f"\n*Source: {gt_entry.get('source', 'unknown')}*")
    return "\n".join(lines)


def score_one(wine: dict, model: str, ground_truth_map: dict, dry_run: bool = False) -> dict | None:
    """Score one model's output for one wine."""
    slug = model_slug(model)
    result_file = RESULTS_DIR / slug / f"{wine['wine_id']}.json"

    if not result_file.exists():
        return None

    result = json.load(open(result_file, encoding="utf-8"))

    # Pre-check: parse failure → minimum scores
    if not result.get("parse_ok"):
        return {
            "wine_id": wine["wine_id"],
            "wine_name": wine["display_name"],
            "tier": wine["tier"],
            "model": model,
            "correctness": 1.0,
            "voice": 1.0,
            "specificity": 1.0,
            "usefulness": 1.0,
            "composite": 1.0,
            "wrong_facts": ["PARSE_FAILED"],
            "banned_words_found": [],
            "fabricated_numbers": [],
            "reasoning": "Output failed to parse as valid JSON. Unusable for production.",
            "judge_tokens_in": 0,
            "judge_tokens_out": 0,
        }

    # Build judge prompt
    context_str = format_context_for_judge(wine)
    output_str = format_output_for_judge(result)
    gt_entry = ground_truth_map.get(wine["wine_id"])
    ground_truth_str = format_ground_truth(gt_entry)

    prompt = JUDGE_PROMPT.format(
        voice_ref=VOICE_REFERENCE,
        context_data=context_str,
        ground_truth=ground_truth_str,
        model_output=output_str,
    )

    if dry_run:
        print(f"  [DRY RUN] {wine['display_name'][:40]} — prompt {len(prompt)} chars")
        return None

    # Call judge
    resp = call_judge(prompt)

    if resp.get("error"):
        print(f"    ERROR: {resp['error']}")
        return None

    parsed = parse_judge_response(resp["raw_content"])
    if not parsed:
        print(f"    JUDGE PARSE FAILED: {resp['raw_content'][:200]}")
        return None

    # Compute composite
    composite = compute_composite(parsed)

    return {
        "wine_id": wine["wine_id"],
        "wine_name": wine["display_name"],
        "tier": wine["tier"],
        "model": model,
        "correctness": parsed.get("correctness", 1.0),
        "voice": parsed.get("voice", 1.0),
        "specificity": parsed.get("specificity", 1.0),
        "usefulness": parsed.get("usefulness", 1.0),
        "composite": composite,
        "wrong_facts": parsed.get("wrong_facts", []),
        "banned_words_found": parsed.get("banned_words_found", []),
        "fabricated_numbers": parsed.get("fabricated_numbers", []),
        "reasoning": parsed.get("reasoning", ""),
        "judge_tokens_in": resp.get("tokens_in", 0),
        "judge_tokens_out": resp.get("tokens_out", 0),
    }


def write_scores_csv(all_scores: list):
    """Write per-wine per-model scores."""
    SCORES_DIR.mkdir(parents=True, exist_ok=True)
    out_file = SCORES_DIR / "task3_scores.csv"

    fields = [
        "wine_id", "wine_name", "tier", "model",
        "correctness", "voice", "specificity", "usefulness", "composite",
        "wrong_facts", "banned_words_found", "fabricated_numbers", "reasoning",
    ]

    with open(out_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        for s in all_scores:
            row = dict(s)
            # Serialize lists as strings for CSV
            for lf in ["wrong_facts", "banned_words_found", "fabricated_numbers"]:
                row[lf] = "; ".join(str(x) for x in row.get(lf, []))
            writer.writerow(row)

    print(f"\nScores written to {out_file} ({len(all_scores)} rows)")
    return out_file


def write_summary_csv(all_scores: list):
    """Write per-model summary."""
    SCORES_DIR.mkdir(parents=True, exist_ok=True)
    out_file = SCORES_DIR / "task3_summary.csv"

    # Group by model
    by_model = {}
    for s in all_scores:
        m = s["model"]
        by_model.setdefault(m, []).append(s)

    summaries = []
    for model in TASK3_MODELS:
        scores = by_model.get(model, [])
        if not scores:
            continue

        n = len(scores)
        avg = lambda field: sum(s[field] for s in scores) / n

        # Tier breakdowns
        tier_comp = {}
        for tier in ["A", "B", "C"]:
            tier_scores = [s for s in scores if s["tier"] == tier]
            if tier_scores:
                tier_comp[tier] = sum(s["composite"] for s in tier_scores) / len(tier_scores)
            else:
                tier_comp[tier] = 0

        # Token/cost stats from the original results
        slug = model_slug(model)
        tok_in_total = 0
        tok_out_total = 0
        for s in scores:
            rf = RESULTS_DIR / slug / f"{s['wine_id']}.json"
            if rf.exists():
                r = json.load(open(rf, encoding="utf-8"))
                tok_in_total += r.get("tokens_in", 0)
                tok_out_total += r.get("tokens_out", 0)

        avg_tok_in = tok_in_total / n
        avg_tok_out = tok_out_total / n
        pricing = MODEL_PRICING.get(model, (1.0, 3.0))
        cost_per_call = (avg_tok_in * pricing[0] + avg_tok_out * pricing[1]) / 1_000_000
        cost_at_170k = cost_per_call * 170_000

        wrong_fact_count = sum(len(s.get("wrong_facts", [])) for s in scores)
        banned_count = sum(len(s.get("banned_words_found", [])) for s in scores)

        summaries.append({
            "model": model,
            "avg_correctness": round(avg("correctness"), 2),
            "avg_voice": round(avg("voice"), 2),
            "avg_specificity": round(avg("specificity"), 2),
            "avg_usefulness": round(avg("usefulness"), 2),
            "composite": round(avg("composite"), 3),
            "tier_a_composite": round(tier_comp["A"], 3),
            "tier_b_composite": round(tier_comp["B"], 3),
            "tier_c_composite": round(tier_comp["C"], 3),
            "wrong_facts_total": wrong_fact_count,
            "banned_words_total": banned_count,
            "avg_tokens_out": round(avg_tok_out),
            "cost_per_call": round(cost_per_call, 6),
            "cost_at_170k": round(cost_at_170k, 2),
            "n_scored": n,
        })

    # Sort by composite descending
    summaries.sort(key=lambda s: s["composite"], reverse=True)

    fields = [
        "model", "composite", "avg_correctness", "avg_voice", "avg_specificity", "avg_usefulness",
        "tier_a_composite", "tier_b_composite", "tier_c_composite",
        "wrong_facts_total", "banned_words_total", "avg_tokens_out",
        "cost_per_call", "cost_at_170k", "n_scored",
    ]

    with open(out_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(summaries)

    print(f"Summary written to {out_file} ({len(summaries)} models)")
    return summaries


def print_leaderboard(summaries: list):
    """Print a nice leaderboard."""
    print("\n" + "=" * 120)
    print("TASK 3 LEADERBOARD — Customer-Facing Prose (Opus-Judged)")
    print("=" * 120)
    print(f"{'#':>2}  {'Model':<30} {'Comp':>5} {'Corr':>5} {'Voice':>5} {'Spec':>5} "
          f"{'Useful':>6} {'TierA':>5} {'TierC':>5} {'Wrong':>5} {'Ban':>4} {'$/170K':>8}")
    print("-" * 120)

    for i, s in enumerate(summaries):
        short = s["model"].split("/")[-1][:28]
        print(f"{i+1:>2}  {short:<30} {s['composite']:>5.2f} {s['avg_correctness']:>5.2f} "
              f"{s['avg_voice']:>5.2f} {s['avg_specificity']:>5.2f} {s['avg_usefulness']:>5.2f} "
              f"{s['tier_a_composite']:>5.2f} {s['tier_c_composite']:>5.2f} "
              f"{s['wrong_facts_total']:>5} {s['banned_words_total']:>4} "
              f"${s['cost_at_170k']:>7.0f}")

    print("=" * 120)
    print("Comp = weighted composite (Corr 35%, Voice 25%, Spec 20%, Useful 20%)")
    print("Wrong = total wrong-fact flags across 30 wines | Ban = banned word occurrences")
    print("$/170K = projected cost for full 170K corpus enrichment")


def main():
    parser = argparse.ArgumentParser(description="B5.6: Score Task 3 outputs via Opus judge")
    parser.add_argument("--model", help="Score a single model (partial slug match)")
    parser.add_argument("--exact-models", help="Comma-separated EXACT slugs (no partial match). Overrides --model.")
    parser.add_argument("--wine", help="Score a single wine ID")
    parser.add_argument("--dry-run", action="store_true", help="Show prompts without API calls")
    parser.add_argument("--summary-only", action="store_true", help="Regenerate summaries from saved judge results")
    args = parser.parse_args()

    if not OPENROUTER_API_KEY and not args.summary_only:
        print("ERROR: OPENROUTER_API_KEY not set")
        sys.exit(1)

    # Load wine contexts
    with open(CONTEXTS_FILE, "r", encoding="utf-8") as f:
        wines = json.load(f)
    print(f"Loaded {len(wines)} wines")

    # Load ground truth
    if GROUND_TRUTH_FILE.exists():
        with open(GROUND_TRUTH_FILE, "r", encoding="utf-8") as f:
            ground_truth_list = json.load(f)
        ground_truth_map = {g["wine_id"]: g for g in ground_truth_list}
        print(f"Loaded ground truth for {len(ground_truth_map)} wines")
    else:
        ground_truth_map = {}
        print("WARNING: No ground truth file — scoring without fact-check baseline")

    # Filter wines if --wine specified
    if args.wine:
        wines = [w for w in wines if w["wine_id"] == args.wine]
        if not wines:
            print(f"Wine {args.wine} not found")
            sys.exit(1)

    # Filter models
    if args.exact_models:
        wanted = [s.strip() for s in args.exact_models.split(",") if s.strip()]
        models = [m for m in TASK3_MODELS if m in wanted]
        missing = [s for s in wanted if s not in TASK3_MODELS]
        if missing:
            print(f"WARNING: Unknown slugs ignored: {missing}")
        if not models:
            print(f"No models matched --exact-models: {wanted}")
            sys.exit(1)
    elif args.model:
        models = [m for m in TASK3_MODELS if args.model.lower() in m.lower()]
        if not models:
            print(f"No model matching '{args.model}'")
            sys.exit(1)
    else:
        models = TASK3_MODELS

    # Resume: load existing scores
    JUDGE_DIR.mkdir(parents=True, exist_ok=True)
    all_scores = []

    if args.summary_only:
        # Load all saved judge results
        for jf in JUDGE_DIR.glob("*.json"):
            all_scores.append(json.load(open(jf, encoding="utf-8")))
        print(f"Loaded {len(all_scores)} saved judge results")
    else:
        total_calls = len(wines) * len(models)
        print(f"\nJudging {len(models)} models x {len(wines)} wines = {total_calls} calls")
        if args.dry_run:
            print("[DRY RUN MODE]")

        done = 0
        errors = 0
        skipped = 0

        for wi, wine in enumerate(wines):
            for mi, model in enumerate(models):
                slug = model_slug(model)
                judge_file = JUDGE_DIR / f"{slug}__{wine['wine_id']}.json"

                # Resume: skip if already scored
                if judge_file.exists():
                    saved = json.load(open(judge_file, encoding="utf-8"))
                    all_scores.append(saved)
                    skipped += 1
                    continue

                score = score_one(wine, model, ground_truth_map, dry_run=args.dry_run)

                if score is None:
                    if not args.dry_run:
                        errors += 1
                    continue

                # Save immediately
                with open(judge_file, "w", encoding="utf-8") as f:
                    json.dump(score, f, indent=2, ensure_ascii=False)
                all_scores.append(score)
                done += 1

                # Progress
                total_done = done + skipped
                if total_done % 21 == 0 or total_done == total_calls:
                    print(f"  {total_done}/{total_calls} (done={done}, skipped={skipped}, errors={errors})")

                # Brief pause
                time.sleep(0.15)

        if skipped:
            print(f"  Resumed: {skipped} cached, {done} new, {errors} errors")

    if not all_scores:
        print("No scores to summarize.")
        return

    # Write CSVs
    write_scores_csv(all_scores)
    summaries = write_summary_csv(all_scores)
    print_leaderboard(summaries)

    # Judge cost
    judge_tok_in = sum(s.get("judge_tokens_in", 0) for s in all_scores)
    judge_tok_out = sum(s.get("judge_tokens_out", 0) for s in all_scores)
    judge_cost = (judge_tok_in * 15.0 + judge_tok_out * 75.0) / 1_000_000
    print(f"\nJudge cost: ~${judge_cost:.2f} ({judge_tok_in:,} in, {judge_tok_out:,} out)")

    print("\nDone.")


if __name__ == "__main__":
    main()
