#!/usr/bin/env python3
"""B5.3: Run + score Task 1 (dedup classification) across 17 models.

Sends 200 labeled wine pairs to each model via OpenRouter, collects verdicts,
scores against ground truth, and writes bakeoff/scores/task1_scores.csv.

Usage:
    python -m bakeoff.run_task1                    # run all models
    python -m bakeoff.run_task1 --model google/gemini-2.5-flash  # single model
    python -m bakeoff.run_task1 --score-only       # just re-score from saved results
    python -m bakeoff.run_task1 --dry-run          # show what would be sent, no API calls
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

import requests
from dotenv import load_dotenv

load_dotenv()

OPENROUTER_API_KEY = os.getenv("OPENROUTER_API_KEY")
OPENROUTER_URL = "https://openrouter.ai/api/v1/chat/completions"

BASE_DIR = Path(__file__).resolve().parent
PAIRS_FILE = BASE_DIR / "data" / "task1" / "pairs.json"
RESULTS_DIR = BASE_DIR / "results" / "task1"
SCORES_DIR = BASE_DIR / "scores"

# 17 models from MODELS.md
TASK1_MODELS = [
    "anthropic/claude-sonnet-4.6",
    "anthropic/claude-haiku-4.5",
    "deepseek/deepseek-v3.2",
    "google/gemini-2.5-flash",
    "google/gemini-2.5-pro",
    "openai/o4-mini",
    "cohere/command-r7b-12-2024",
    "meta-llama/llama-4-maverick",
    "meta-llama/llama-4-scout",
    "mistralai/mistral-nemo",
    "openai/gpt-5.4",
    "openai/gpt-5.4-mini",
    "x-ai/grok-4.1-fast",
    "qwen/qwen3.6-plus",
    "google/gemini-3-flash-preview",
    "google/gemini-3.1-flash-lite-preview",
    "xiaomi/mimo-v2-flash",
]

# OpenRouter pricing ($/M tokens) — input/output
# Used for cost projection; actual costs come from usage headers when available
MODEL_PRICING = {
    "anthropic/claude-sonnet-4.6": (3.0, 15.0),
    "anthropic/claude-haiku-4.5": (0.80, 4.0),
    "deepseek/deepseek-v3.2": (0.14, 0.28),
    "google/gemini-2.5-flash": (0.15, 0.60),
    "google/gemini-2.5-pro": (1.25, 10.0),
    "openai/o4-mini": (1.10, 4.40),
    "cohere/command-r7b-12-2024": (0.04, 0.04),
    "meta-llama/llama-4-maverick": (0.20, 0.60),
    "meta-llama/llama-4-scout": (0.15, 0.42),
    "mistralai/mistral-nemo": (0.035, 0.08),
    "openai/gpt-5.4": (2.50, 10.0),
    "openai/gpt-5.4-mini": (0.40, 1.60),
    "x-ai/grok-4.1-fast": (0.60, 4.0),
    "qwen/qwen3.6-plus": (0.40, 1.20),
    "google/gemini-3-flash-preview": (0.15, 0.60),
    "google/gemini-3.1-flash-lite-preview": (0.02, 0.10),
    "xiaomi/mimo-v2-flash": (0.07, 0.28),
}

PROMPT_TEMPLATE = """You are classifying whether two wine records represent the same wine or different wines.
Two records are the SAME wine if they would share a single page in a wine encyclopedia \
— same producer, same vineyard/cuvée, same grape variety, same style. They may differ \
only in name formatting (abbreviation, varietal suffix appended/omitted).

Two records are DIFFERENT wines if they represent distinct bottlings that a consumer \
would encounter separately: different grapes, different classification tiers (Reserva \
vs Gran Reserva), different vineyard designations, different colors, different wine \
types (still vs sparkling), or different sweetness levels (Sec vs Moelleux).

Key rules:
- Varietal suffix difference ALONE (e.g., "Armillary" vs "Armillary Cabernet Sauvignon") \
usually means SAME — the shorter name is just an abbreviation
- UNLESS the producer makes multiple wines from that vineyard with different grapes
- Different grapes = DIFFERENT wine, always, even if vineyard name matches
- Different Rioja classification (Reserva/Gran Reserva/Crianza) = DIFFERENT
- Different German Prädikat level (Kabinett/Spätlese/Auslese/BA/TBA) = DIFFERENT
- Different lot/cask numbers at the same Prädikat level = DIFFERENT
- Rosé vs Red from the same vineyard = DIFFERENT
- NV (non-vintage) vs named cuvée = requires analysis of whether they're the same bottling

## Wine Pair

Wine A: {name_a}
Wine B: {name_b}
Producer: {producer_name}

{metadata_block}

## Response Format (JSON only)

Return ONLY valid JSON, no markdown fencing, no extra text.

{{"verdict": "SAME or DIFFERENT", "confidence": 0.0-1.0, "reasoning": "2-3 sentences explaining the key evidence"}}"""


def model_slug(model_id: str) -> str:
    """Convert model ID to filesystem-safe directory name."""
    return model_id.replace("/", "__")


def call_openrouter(model: str, prompt: str, retries: int = 2) -> dict:
    """Call OpenRouter API and return parsed response with metadata."""
    headers = {
        "Authorization": f"Bearer {OPENROUTER_API_KEY}",
        "Content-Type": "application/json",
        "HTTP-Referer": "https://loam.onrender.com",
        "X-Title": "Loam Bake-off",
    }

    # Build request — disable thinking/reasoning for fair comparison
    # Reasoning models need more output budget (thinking tokens count against max_tokens)
    max_tok = 300
    if "2.5-pro" in model or "o4-mini" in model:
        max_tok = 4000

    body = {
        "model": model,
        "messages": [{"role": "user", "content": prompt}],
        "temperature": 0.0,
        "max_tokens": max_tok,
    }

    # Provider-specific: disable thinking tokens for Gemini/DeepSeek/o4
    # Gemini 2.5 Pro requires reasoning — don't disable it
    if "gemini" in model and "2.5-pro" not in model:
        body["provider"] = {"require_parameters": True}
        body["reasoning"] = {"effort": "none"}
    if "o4-mini" in model:
        body["reasoning"] = {"effort": "low"}

    for attempt in range(retries + 1):
        try:
            t0 = time.time()
            resp = requests.post(OPENROUTER_URL, headers=headers, json=body, timeout=60)
            elapsed = time.time() - t0

            if resp.status_code == 429:
                wait = min(2 ** (attempt + 1), 30)
                print(f"    Rate limited, waiting {wait}s...")
                time.sleep(wait)
                continue

            if resp.status_code != 200:
                if attempt < retries:
                    time.sleep(2)
                    continue
                return {
                    "error": f"HTTP {resp.status_code}: {resp.text[:200]}",
                    "elapsed": elapsed,
                    "tokens_in": 0,
                    "tokens_out": 0,
                }

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

        except requests.exceptions.Timeout:
            if attempt < retries:
                time.sleep(2)
                continue
            return {"error": "Timeout", "elapsed": 60, "tokens_in": 0, "tokens_out": 0}
        except Exception as e:
            if attempt < retries:
                time.sleep(2)
                continue
            return {"error": str(e), "elapsed": 0, "tokens_in": 0, "tokens_out": 0}

    return {"error": "Max retries exceeded", "elapsed": 0, "tokens_in": 0, "tokens_out": 0}


def parse_verdict(raw: str) -> dict:
    """Parse model response into verdict dict. Handles JSON in markdown fences."""
    if not raw:
        return {"verdict": None, "confidence": None, "reasoning": None, "parse_ok": False}

    # Strip markdown code fences
    cleaned = re.sub(r"^```(?:json)?\s*", "", raw.strip())
    cleaned = re.sub(r"\s*```$", "", cleaned.strip())

    # Try to find JSON object
    match = re.search(r"\{[^{}]*\}", cleaned, re.DOTALL)
    if match:
        try:
            parsed = json.loads(match.group())
            verdict = parsed.get("verdict", "").upper().strip()
            if verdict not in ("SAME", "DIFFERENT"):
                # Try to extract from string
                if "SAME" in verdict:
                    verdict = "SAME"
                elif "DIFFERENT" in verdict or "DIFF" in verdict:
                    verdict = "DIFFERENT"
                else:
                    return {"verdict": None, "confidence": None, "reasoning": None, "parse_ok": False}

            conf = parsed.get("confidence")
            if isinstance(conf, str):
                try:
                    conf = float(conf)
                except ValueError:
                    conf = None
            if isinstance(conf, (int, float)):
                conf = max(0.0, min(1.0, float(conf)))

            return {
                "verdict": verdict,
                "confidence": conf,
                "reasoning": parsed.get("reasoning", ""),
                "parse_ok": True,
            }
        except json.JSONDecodeError:
            pass

    # Fallback: look for SAME/DIFFERENT anywhere in text
    upper = raw.upper()
    if "DIFFERENT" in upper and "SAME" not in upper:
        return {"verdict": "DIFFERENT", "confidence": None, "reasoning": raw[:200], "parse_ok": False}
    elif "SAME" in upper and "DIFFERENT" not in upper:
        return {"verdict": "SAME", "confidence": None, "reasoning": raw[:200], "parse_ok": False}

    return {"verdict": None, "confidence": None, "reasoning": raw[:200], "parse_ok": False}


def run_model(model: str, pairs: list, dry_run: bool = False) -> list:
    """Run one model against all pairs. Supports resume via saved results."""
    slug = model_slug(model)
    out_dir = RESULTS_DIR / slug
    out_dir.mkdir(parents=True, exist_ok=True)

    results = []
    skipped = 0
    errors = 0

    for i, pair in enumerate(pairs):
        pair_id = pair["pair_id"]
        result_file = out_dir / f"{pair_id}.json"

        # Resume: skip if already done
        if result_file.exists():
            with open(result_file, "r") as f:
                saved = json.load(f)
            results.append(saved)
            skipped += 1
            continue

        if dry_run:
            print(f"  [DRY RUN] Would send {pair_id}: {pair['wine_a']['name']} vs {pair['wine_b']['name']}")
            continue

        # Build prompt
        prompt = PROMPT_TEMPLATE.format(
            name_a=pair["wine_a"]["name"],
            name_b=pair["wine_b"]["name"],
            producer_name=pair["producer"]["name"],
            metadata_block=pair.get("metadata_block", ""),
        )

        # Call API
        resp = call_openrouter(model, prompt)

        if resp.get("error"):
            errors += 1
            result = {
                "pair_id": pair_id,
                "model": model,
                "error": resp["error"],
                "verdict": None,
                "confidence": None,
                "reasoning": None,
                "parse_ok": False,
                "tokens_in": resp.get("tokens_in", 0),
                "tokens_out": resp.get("tokens_out", 0),
                "elapsed": resp.get("elapsed", 0),
            }
        else:
            parsed = parse_verdict(resp["raw_content"])
            result = {
                "pair_id": pair_id,
                "model": model,
                "raw_content": resp["raw_content"],
                "verdict": parsed["verdict"],
                "confidence": parsed["confidence"],
                "reasoning": parsed["reasoning"],
                "parse_ok": parsed["parse_ok"],
                "tokens_in": resp["tokens_in"],
                "tokens_out": resp["tokens_out"],
                "elapsed": resp["elapsed"],
                "error": None,
            }

        # Save immediately for resume
        with open(result_file, "w") as f:
            json.dump(result, f, indent=2)
        results.append(result)

        # Progress
        done = i + 1 - skipped + skipped
        if (i + 1) % 20 == 0 or (i + 1) == len(pairs):
            print(f"  {i+1}/{len(pairs)} (errors: {errors})")

        # Brief pause to avoid rate limits
        time.sleep(0.05)

    if skipped:
        print(f"  Resumed: {skipped} cached, {len(pairs) - skipped} new")

    return results


def score_model(model: str, pairs: list) -> dict:
    """Score a single model's results against ground truth."""
    slug = model_slug(model)
    out_dir = RESULTS_DIR / slug

    # Build ground truth lookup
    gt_map = {p["pair_id"]: p for p in pairs}

    # Load all results
    results = []
    for pair in pairs:
        result_file = out_dir / f"{pair['pair_id']}.json"
        if result_file.exists():
            with open(result_file, "r") as f:
                results.append(json.load(f))

    if not results:
        return None

    # Metrics
    total = len(results)
    correct = 0
    false_pos = 0  # predicted SAME, truth DIFFERENT
    false_neg = 0  # predicted DIFFERENT, truth SAME
    parse_ok = 0
    total_tokens_in = 0
    total_tokens_out = 0
    total_elapsed = 0.0
    stratum_correct = {"A": 0, "B": 0, "C": 0, "D": 0}
    stratum_total = {"A": 0, "B": 0, "C": 0, "D": 0}
    conf_correct = []  # (confidence, is_correct) for calibration
    errors = 0

    truth_same_total = 0
    truth_diff_total = 0

    for r in results:
        pid = r["pair_id"]
        gt_pair = gt_map.get(pid)
        if not gt_pair:
            continue

        stratum = gt_pair["stratum"]
        truth = gt_pair["ground_truth"]
        verdict = r.get("verdict")

        stratum_total[stratum] = stratum_total.get(stratum, 0) + 1

        if truth == "SAME":
            truth_same_total += 1
        else:
            truth_diff_total += 1

        if r.get("parse_ok"):
            parse_ok += 1

        total_tokens_in += r.get("tokens_in", 0)
        total_tokens_out += r.get("tokens_out", 0)
        total_elapsed += r.get("elapsed", 0)

        if r.get("error") or verdict is None:
            errors += 1
            continue

        is_correct = verdict == truth
        if is_correct:
            correct += 1
            stratum_correct[stratum] = stratum_correct.get(stratum, 0) + 1

        # False positive: said SAME when truth is DIFFERENT
        if verdict == "SAME" and truth == "DIFFERENT":
            false_pos += 1
        # False negative: said DIFFERENT when truth is SAME
        if verdict == "DIFFERENT" and truth == "SAME":
            false_neg += 1

        # Confidence calibration
        conf = r.get("confidence")
        if conf is not None:
            conf_correct.append((conf, 1.0 if is_correct else 0.0))

    # Calculate rates
    answered = total - errors
    accuracy = correct / answered if answered > 0 else 0
    fpr = false_pos / truth_diff_total if truth_diff_total > 0 else 0
    fnr = false_neg / truth_same_total if truth_same_total > 0 else 0
    parse_rate = parse_ok / total if total > 0 else 0

    # Per-stratum accuracy
    stratum_acc = {}
    for s in ["A", "B", "C", "D"]:
        st = stratum_total.get(s, 0)
        sc = stratum_correct.get(s, 0)
        stratum_acc[s] = sc / st if st > 0 else 0

    # Confidence calibration (Pearson correlation)
    conf_corr = None
    if len(conf_correct) > 5:
        confs = [c[0] for c in conf_correct]
        accs = [c[1] for c in conf_correct]
        mean_c = sum(confs) / len(confs)
        mean_a = sum(accs) / len(accs)
        cov = sum((c - mean_c) * (a - mean_a) for c, a in zip(confs, accs))
        var_c = sum((c - mean_c) ** 2 for c in confs)
        var_a = sum((a - mean_a) ** 2 for a in accs)
        denom = (var_c * var_a) ** 0.5
        conf_corr = cov / denom if denom > 0 else 0

    # Token averages
    avg_tokens_in = total_tokens_in / total if total > 0 else 0
    avg_tokens_out = total_tokens_out / total if total > 0 else 0

    # Cost projection
    pricing = MODEL_PRICING.get(model, (1.0, 3.0))
    cost_per_call = (avg_tokens_in * pricing[0] + avg_tokens_out * pricing[1]) / 1_000_000
    cost_at_4k = cost_per_call * 4000

    return {
        "model": model,
        "accuracy": round(accuracy, 4),
        "fpr": round(fpr, 4),
        "fnr": round(fnr, 4),
        "stratum_a": round(stratum_acc["A"], 4),
        "stratum_b": round(stratum_acc["B"], 4),
        "stratum_c": round(stratum_acc["C"], 4),
        "stratum_d": round(stratum_acc["D"], 4),
        "confidence_corr": round(conf_corr, 4) if conf_corr is not None else "",
        "parse_rate": round(parse_rate, 4),
        "avg_tokens_in": round(avg_tokens_in),
        "avg_tokens_out": round(avg_tokens_out),
        "cost_per_call": round(cost_per_call, 6),
        "cost_at_4k": round(cost_at_4k, 2),
        "errors": errors,
        "total": total,
    }


def write_scores(scores: list):
    """Write task1_scores.csv."""
    SCORES_DIR.mkdir(parents=True, exist_ok=True)
    out_file = SCORES_DIR / "task1_scores.csv"

    fields = [
        "model", "accuracy", "fpr", "fnr",
        "stratum_a", "stratum_b", "stratum_c", "stratum_d",
        "confidence_corr", "parse_rate",
        "avg_tokens_in", "avg_tokens_out",
        "cost_per_call", "cost_at_4k", "errors", "total",
    ]

    # Sort by accuracy descending
    scores.sort(key=lambda s: s["accuracy"], reverse=True)

    with open(out_file, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(scores)

    print(f"\nScores written to {out_file}")
    return out_file


def print_leaderboard(scores: list):
    """Print a nice leaderboard to stdout."""
    scores.sort(key=lambda s: s["accuracy"], reverse=True)
    print("\n" + "=" * 100)
    print("TASK 1 LEADERBOARD — Dedup Classification (200 pairs)")
    print("=" * 100)
    print(f"{'#':>2}  {'Model':<40} {'Acc':>6} {'FPR':>6} {'FNR':>6} "
          f"{'A':>5} {'B':>5} {'C':>5} {'D':>5} {'Parse':>6} {'$/4K':>7}")
    print("-" * 100)
    for i, s in enumerate(scores):
        short = s["model"].split("/")[-1][:38]
        print(f"{i+1:>2}  {short:<40} {s['accuracy']:>5.1%} {s['fpr']:>5.1%} {s['fnr']:>5.1%} "
              f"{s['stratum_a']:>4.0%} {s['stratum_b']:>4.0%} {s['stratum_c']:>4.0%} {s['stratum_d']:>4.0%} "
              f"{s['parse_rate']:>5.0%} ${s['cost_at_4k']:>6.2f}")
    print("=" * 100)
    print(f"FPR = false merge rate (predicted SAME when DIFFERENT) — lower is better, safety-critical")
    print(f"FNR = missed merge rate (predicted DIFFERENT when SAME) — lower is better")
    print(f"$/4K = projected cost for 4,000 production pairs")


def main():
    parser = argparse.ArgumentParser(description="B5.3: Run + score Task 1 (dedup)")
    parser.add_argument("--model", help="Run a single model (OpenRouter slug)")
    parser.add_argument("--score-only", action="store_true", help="Score from saved results only")
    parser.add_argument("--dry-run", action="store_true", help="Show prompts without API calls")
    args = parser.parse_args()

    if not OPENROUTER_API_KEY and not args.score_only:
        print("ERROR: OPENROUTER_API_KEY not set")
        sys.exit(1)

    # Load pairs
    with open(PAIRS_FILE, "r", encoding="utf-8") as f:
        pairs = json.load(f)
    print(f"Loaded {len(pairs)} pairs from {PAIRS_FILE}")

    # Determine which models to run
    if args.model:
        models = [args.model]
    else:
        models = TASK1_MODELS

    if not args.score_only:
        print(f"\nRunning {len(models)} models × {len(pairs)} pairs = {len(models) * len(pairs)} calls")
        if args.dry_run:
            print("[DRY RUN MODE]")

        for i, model in enumerate(models):
            print(f"\n[{i+1}/{len(models)}] {model}")
            results = run_model(model, pairs, dry_run=args.dry_run)
            if not args.dry_run:
                # Quick accuracy check
                gt_map = {p["pair_id"]: p["ground_truth"] for p in pairs}
                correct = sum(1 for r in results if r.get("verdict") == gt_map.get(r["pair_id"]))
                answered = sum(1 for r in results if r.get("verdict") is not None)
                if answered:
                    print(f"  Quick score: {correct}/{answered} = {correct/answered:.1%}")

    # Score all models
    print("\n\nScoring all models...")
    all_scores = []
    for model in models:
        slug_dir = RESULTS_DIR / model_slug(model)
        if not slug_dir.exists():
            print(f"  {model}: no results found, skipping")
            continue
        score = score_model(model, pairs)
        if score:
            all_scores.append(score)
            print(f"  {model}: {score['accuracy']:.1%} accuracy, "
                  f"FPR={score['fpr']:.1%}, FNR={score['fnr']:.1%}")

    if all_scores:
        write_scores(all_scores)
        print_leaderboard(all_scores)

    print("\nDone.")


if __name__ == "__main__":
    main()
