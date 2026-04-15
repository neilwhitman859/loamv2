#!/usr/bin/env python3
"""Round 5: Per-field judge. Scores each output field separately.

For each (wine, model) pair, Opus judges the 11 prose fields one by one on:
- correctness (1-5)
- voice (1-5)

The aggregation is across wines × models, producing a matrix of
"which model is best at which field." Used to decide whether a
field-split production pipeline is viable.
"""

import argparse
import csv
import json
import os
import re
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
FIELD_DIR = SCORES_DIR / "field_judge"

# 11 prose fields scored individually
FIELDS = [
    "hook", "wine_summary", "terroir_expression", "vinification_summary",
    "food_pairing", "comparable_wines", "style_profile",
    "cellar_recommendation", "value_assessment", "market_position", "insider_take",
]

BANNED_WORDS = {
    "genuinely", "genuine", "elegant", "exquisite", "harmonious", "remarkable",
    "world-class", "showcases", "embodies", "captures the essence", "refined",
    "sophisticated", "masterfully", "lovingly", "painstakingly",
}


JUDGE_PROMPT = """You are scoring individual fields of a wine enrichment output. Score each field separately on correctness and voice.

## Wine context (what the model was given)
{context}

## Verified ground truth
{ground_truth}

## Voice rules
Loam voice: direct, specific, names geology/soils/climate, takes a position, no sommelier theater, no hedging.
BANNED WORDS (zero tolerance — voice MAX 2.0 if used): genuinely, genuine, elegant, exquisite, harmonious, remarkable, world-class, showcases, embodies, captures the essence, refined, sophisticated, masterfully, lovingly, painstakingly.

## Fields to score

{fields_block}

## Task

For each field, give two scores 1.0-5.0:
- **correctness**: does the content match context + ground truth? Fabricated numbers (pH, cases, oak months NOT in context) → MAX 1.5.
- **voice**: directness, specificity, opinion. Banned words → MAX 2.0.

Return ONLY JSON:
{{"scores": {{"<field>": {{"correctness": <float>, "voice": <float>, "note": "<1-sentence issue>"}}}}, "overall_note": "<1-sentence summary>"}}"""


def model_slug(m: str) -> str:
    return m.replace("/", "__")


def format_context(wine: dict) -> str:
    ctx = wine["context"]
    lines = [
        f"Wine: {ctx['display_name']}",
        f"Producer: {ctx['producer_name']}",
        f"Appellation: {ctx.get('appellation_name', '?')} / {ctx.get('region_name', '?')} / {ctx.get('country_name', '?')}",
        f"Type: {ctx.get('wine_type', 'table')} {ctx.get('color', '')}",
    ]
    grapes = ctx.get("grapes", [])
    if grapes:
        lines.append(f"Grapes: " + ", ".join(f"{g['name']} ({g.get('percentage', '?')}%)" for g in grapes))
    vintages = ctx.get("vintages", [])
    if vintages:
        vlines = []
        for v in vintages[:5]:
            parts = [f"Y{v.get('vintage_year', 'NV')}"]
            for k, lab in [("abv", "ABV"), ("ph", "pH"), ("ta_g_l", "TA"),
                           ("rs_g_l", "RS"), ("cases_produced", "Cases"),
                           ("duration_in_oak_months", "OakMo"), ("new_oak_pct", "NewOak%")]:
                if v.get(k):
                    parts.append(f"{lab}={v[k]}")
            vlines.append(", ".join(parts))
        lines.append("Vintages: " + "; ".join(vlines))
    else:
        lines.append("Vintages: NONE in context")
    prices = ctx.get("prices", [])
    if prices:
        lines.append("Prices: " + ", ".join(f"${p.get('price_usd','?')}" for p in prices[:5]))
    scores = ctx.get("scores", [])
    if scores:
        lines.append(f"Scores: {len(scores)} entries (medals/points)")
    return "\n".join(lines)


def format_ground_truth(gt: dict | None) -> str:
    if not gt:
        return "(No ground truth available — judge on plausibility only.)"
    lines = []
    facts = gt.get("verified_facts", {})
    for k, v in facts.items():
        if isinstance(v, list):
            v = " / ".join(str(x) for x in v)
        lines.append(f"- **{k}:** {v}")
    return "\n".join(lines) + f"\n\n(Source: {gt.get('source', '?')})"


def format_fields_block(parsed: dict) -> str:
    sections = []
    for f in FIELDS:
        val = parsed.get(f)
        if val is None:
            sections.append(f"### {f}\n(MISSING)")
            continue
        if isinstance(val, list):
            val = "\n".join(str(x) for x in val)
        elif isinstance(val, dict):
            val = json.dumps(val, indent=2)
        sections.append(f"### {f}\n{val}")
    return "\n\n".join(sections)


def call_judge(prompt: str, retries: int = 2) -> dict:
    headers = {
        "Authorization": f"Bearer {OPENROUTER_API_KEY}",
        "Content-Type": "application/json",
        "HTTP-Referer": "https://loam.onrender.com",
        "X-Title": "Loam Field Judge",
    }
    body = {
        "model": JUDGE_MODEL,
        "messages": [{"role": "user", "content": prompt}],
        "temperature": 0.0,
        "max_tokens": 2000,
    }
    for attempt in range(retries + 1):
        try:
            t0 = time.time()
            resp = http_requests.post(OPENROUTER_URL, headers=headers, json=body, timeout=180)
            elapsed = time.time() - t0
            if resp.status_code == 429:
                time.sleep(min(2 ** (attempt + 2), 60))
                continue
            if resp.status_code != 200:
                if attempt < retries:
                    time.sleep(3); continue
                return {"error": f"HTTP {resp.status_code}: {resp.text[:300]}"}
            data = resp.json()
            content = data.get("choices", [{}])[0].get("message", {}).get("content", "")
            usage = data.get("usage", {})
            return {
                "raw_content": content,
                "tokens_in": usage.get("prompt_tokens", 0),
                "tokens_out": usage.get("completion_tokens", 0),
                "elapsed": elapsed,
            }
        except Exception as e:
            if attempt < retries:
                time.sleep(3); continue
            return {"error": str(e)}
    return {"error": "max retries"}


def parse_judge_response(raw: str) -> dict:
    if not raw: return None
    cleaned = re.sub(r"^```(?:json)?\s*", "", raw.strip())
    cleaned = re.sub(r"\s*```$", "", cleaned.strip())
    depth = 0; start = None
    for i, ch in enumerate(cleaned):
        if ch == '{':
            if depth == 0: start = i
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


def score_one(wine: dict, model: str, ground_truth_map: dict) -> dict | None:
    slug = model_slug(model)
    rf = RESULTS_DIR / slug / f"{wine['wine_id']}.json"
    if not rf.exists():
        return None
    result = json.load(open(rf, encoding="utf-8"))
    if not result.get("parse_ok"):
        return None
    parsed = result["parsed"]

    prompt = JUDGE_PROMPT.format(
        context=format_context(wine),
        ground_truth=format_ground_truth(ground_truth_map.get(wine["wine_id"])),
        fields_block=format_fields_block(parsed),
    )

    resp = call_judge(prompt)
    if resp.get("error"):
        print(f"    ERROR: {resp['error']}")
        return None
    parsed_judge = parse_judge_response(resp["raw_content"])
    if not parsed_judge:
        print(f"    Parse failed: {resp['raw_content'][:200]}")
        return None

    return {
        "wine_id": wine["wine_id"],
        "wine_name": wine["display_name"],
        "tier": wine["tier"],
        "model": model,
        "scores": parsed_judge.get("scores", {}),
        "overall_note": parsed_judge.get("overall_note", ""),
        "judge_tokens_in": resp.get("tokens_in", 0),
        "judge_tokens_out": resp.get("tokens_out", 0),
    }


def print_matrix(all_scores: list, dim: str = "correctness"):
    """Print field × model matrix averaged across wines."""
    from collections import defaultdict
    matrix = defaultdict(lambda: defaultdict(list))  # field -> model -> [scores]
    for s in all_scores:
        for field in FIELDS:
            f_score = s["scores"].get(field, {})
            if f_score and f_score.get(dim) is not None:
                matrix[field][s["model"]].append(f_score[dim])

    models = sorted({s["model"] for s in all_scores})
    print(f"\n=== FIELD × MODEL MATRIX ({dim}) ===")
    print(f"{'Field':<26} " + "".join(f'{m.split(chr(47))[-1][:14]:>15}' for m in models) + '  best')
    print('-' * (26 + 15 * len(models) + 6))
    for field in FIELDS:
        row = [field]
        avgs = {}
        for m in models:
            lst = matrix[field].get(m, [])
            if lst:
                avgs[m] = sum(lst) / len(lst)
        if not avgs: continue
        best_model = max(avgs, key=avgs.get)
        line = f"{field:<26} "
        for m in models:
            v = avgs.get(m, 0)
            marker = " *" if m == best_model else "  "
            line += f'{v:>13.2f}{marker}'
        line += f"  {best_model.split('/')[-1][:14]}"
        print(line)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--wines", help="Comma-separated wine IDs to score")
    parser.add_argument("--models", help="Comma-separated model slugs")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    if not args.wines:
        # Default: 1 wine per tier, famous names
        wine_ids = [
            "1231e371-af5f-4bde-9d2d-be094605cc21",  # Shafer (A)
            "e998b872-3ce8-419e-ba0c-9904a4638629",  # Terroir Al Limit (B)
            "9934442d-7786-489b-aca3-e329b1c12da5",  # Plantagenet (C)
        ]
    else:
        wine_ids = args.wines.split(",")

    if not args.models:
        models = [
            "openai/gpt-5.4-mini",
            "google/gemini-3-flash-preview",
            "deepseek/deepseek-v3.2",
        ]
    else:
        models = args.models.split(",")

    # Load contexts
    with open(CONTEXTS_FILE, encoding="utf-8") as f:
        all_wines = json.load(f)
    wines = [w for w in all_wines if w["wine_id"] in wine_ids]

    with open(GROUND_TRUTH_FILE, encoding="utf-8") as f:
        gt_list = json.load(f)
    gt_map = {g["wine_id"]: g for g in gt_list}

    FIELD_DIR.mkdir(parents=True, exist_ok=True)
    all_scores = []

    total = len(wines) * len(models)
    print(f"Field-judging {len(wines)} wines × {len(models)} models = {total} judge calls")

    for wine in wines:
        for model in models:
            slug = model_slug(model)
            judge_file = FIELD_DIR / f"{slug}__{wine['wine_id']}.json"
            if judge_file.exists():
                saved = json.load(open(judge_file, encoding="utf-8"))
                all_scores.append(saved)
                print(f"  [cached] {wine['display_name'][:40]} × {model}")
                continue

            if args.dry_run:
                print(f"  [DRY] {wine['display_name'][:40]} × {model}")
                continue

            print(f"  {wine['display_name'][:40]} × {model}...")
            score = score_one(wine, model, gt_map)
            if score is None:
                print(f"    skipped (no prose or error)")
                continue
            with open(judge_file, "w", encoding="utf-8") as f:
                json.dump(score, f, indent=2, ensure_ascii=False)
            all_scores.append(score)
            time.sleep(0.3)

    if not all_scores:
        print("No scores.")
        return

    # Matrices
    print_matrix(all_scores, "correctness")
    print_matrix(all_scores, "voice")

    # Field-level best model tally
    from collections import Counter, defaultdict
    wins = defaultdict(lambda: Counter())  # field -> Counter(model -> wins)
    for field in FIELDS:
        by_model = defaultdict(list)
        for s in all_scores:
            fs = s["scores"].get(field, {})
            if fs.get("correctness") is not None and fs.get("voice") is not None:
                combined = fs["correctness"] * 0.5 + fs["voice"] * 0.5
                by_model[s["model"]].append(combined)
        if not by_model: continue
        # Per wine, find winner
        for wine_idx in range(max(len(v) for v in by_model.values())):
            scores_at_idx = {}
            for m, lst in by_model.items():
                if wine_idx < len(lst):
                    scores_at_idx[m] = lst[wine_idx]
            if scores_at_idx:
                winner = max(scores_at_idx, key=scores_at_idx.get)
                wins[field][winner] += 1

    print("\n=== FIELD WIN TALLY (per-wine winner on correctness×voice) ===")
    for field in FIELDS:
        if field in wins:
            tally = ", ".join(f"{m.split('/')[-1]}={c}" for m, c in wins[field].most_common())
            print(f"  {field:<26} {tally}")

    # Cost
    tok_in = sum(s.get("judge_tokens_in", 0) for s in all_scores)
    tok_out = sum(s.get("judge_tokens_out", 0) for s in all_scores)
    cost = (tok_in * 15 + tok_out * 75) / 1_000_000
    print(f"\nJudge cost: ${cost:.2f} ({tok_in:,} in, {tok_out:,} out)")


if __name__ == "__main__":
    main()
