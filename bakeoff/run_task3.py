#!/usr/bin/env python3
"""B5.5: Run Task 3 (customer-facing prose) across 21 models.

Sends 30 wine context packages to each model via OpenRouter, collects raw
prose outputs. No scoring — that's B5.6 (Opus inline judging).

Usage:
    python -m bakeoff.run_task3                    # run all models
    python -m bakeoff.run_task3 --model google/gemini-2.5-flash  # single model
    python -m bakeoff.run_task3 --dry-run          # show what would be sent
"""

import argparse
import json
import os
import re
import sys
import time
from pathlib import Path

os.environ["PYTHONUNBUFFERED"] = "1"

import requests as http_requests
from dotenv import load_dotenv

# Import Grade A prompt builder from production pipeline
from pipeline.enrich.batch_enrich import build_grade_a_prompt

load_dotenv()

OPENROUTER_API_KEY = os.getenv("OPENROUTER_API_KEY")
OPENROUTER_URL = "https://openrouter.ai/api/v1/chat/completions"

BASE_DIR = Path(__file__).resolve().parent
CONTEXTS_FILE = BASE_DIR / "data" / "task3" / "contexts.json"
RESULTS_DIR = BASE_DIR / "results" / "task3"

# 21 models — Task 3 list from MODELS.md
# mistral-large-3 replaced with mistral-large-2512 (verified slug)
# qwen3.5-plus uses -02-15 suffix (verified slug)
# R6 additions (2026-04-15): 9 models — search-capable + cheap Chinese
TASK3_MODELS = [
    # Original 21
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
    # R6 additions: search-capable + cheap Chinese (tested on 12-wine subset only)
    "perplexity/sonar",
    "perplexity/sonar-reasoning",
    "perplexity/llama-3.1-sonar-small-128k-online",
    "deepseek/deepseek-v3.2:online",
    "google/gemini-3-flash-preview:online",
    "openai/gpt-5.4-mini:online",
    "moonshotai/kimi-k2",
    "moonshotai/kimi-k2:online",
    "z-ai/glm-4.6",
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
    # R6 (token pricing only — search fees billed separately by OpenRouter)
    "perplexity/sonar": (1.0, 1.0),
    "perplexity/sonar-reasoning": (1.0, 5.0),
    "perplexity/sonar-reasoning-pro": (2.0, 8.0),
    "perplexity/llama-3.1-sonar-small-128k-online": (0.20, 0.20),
    "deepseek/deepseek-v3.2:online": (0.14, 0.28),
    "google/gemini-3-flash-preview:online": (0.15, 0.60),
    "openai/gpt-5.4-mini:online": (0.40, 1.60),
    "moonshotai/kimi-k2": (0.60, 2.50),
    "moonshotai/kimi-k2:online": (0.60, 2.50),
    "z-ai/glm-4.6": (0.60, 2.20),
}


def model_slug(model_id: str) -> str:
    """Convert model ID to filesystem-safe directory name.

    Windows does not allow `:` in paths, so `:online` → `_online`.
    This is distinct from the non-online variant's slug.
    """
    return model_id.replace("/", "__").replace(":", "_")


def call_openrouter(model: str, prompt: str, retries: int = 2) -> dict:
    """Call OpenRouter API and return raw response with metadata."""
    headers = {
        "Authorization": f"Bearer {OPENROUTER_API_KEY}",
        "Content-Type": "application/json",
        "HTTP-Referer": "https://loam.onrender.com",
        "X-Title": "Loam Bake-off",
    }

    # Prose output is ~800-1500 tokens. Reasoning models need more budget.
    max_tok = 3000
    if "2.5-pro" in model or "3.1-pro" in model or "opus" in model or "gpt-5-mini" in model:
        max_tok = 8000
    # Reasoning / thinking / search models often produce more tokens
    if "sonar-reasoning" in model or "glm-4.6" in model or "kimi-k2" in model:
        max_tok = 8000

    body = {
        "model": model,
        "messages": [{"role": "user", "content": prompt}],
        "temperature": 0.7,
        "max_tokens": max_tok,
    }

    # Disable thinking for Gemini models EXCEPT 2.5-pro and 3.1-pro-preview
    if "gemini" in model and "2.5-pro" not in model and "3.1-pro" not in model:
        body["provider"] = {"require_parameters": True}
        body["reasoning"] = {"effort": "none"}

    for attempt in range(retries + 1):
        try:
            t0 = time.time()
            resp = http_requests.post(OPENROUTER_URL, headers=headers, json=body, timeout=180)
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
                    "error": f"HTTP {resp.status_code}: {resp.text[:300]}",
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

        except http_requests.exceptions.Timeout:
            if attempt < retries:
                time.sleep(2)
                continue
            return {"error": "Timeout", "elapsed": 180, "tokens_in": 0, "tokens_out": 0}
        except Exception as e:
            if attempt < retries:
                time.sleep(2)
                continue
            return {"error": str(e), "elapsed": 0, "tokens_in": 0, "tokens_out": 0}

    return {"error": "Max retries exceeded", "elapsed": 0, "tokens_in": 0, "tokens_out": 0}


def try_parse_json(raw: str) -> tuple[dict | None, bool]:
    """Attempt to parse model output as JSON. Returns (parsed, parse_ok)."""
    if not raw:
        return None, False

    # Strip markdown code fences
    cleaned = re.sub(r"^```(?:json)?\s*", "", raw.strip())
    cleaned = re.sub(r"\s*```$", "", cleaned.strip())

    # Try to find the outermost JSON object
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
                    parsed = json.loads(cleaned[start:i + 1])
                    return parsed, True
                except json.JSONDecodeError:
                    pass

    # Try the whole string
    try:
        parsed = json.loads(cleaned)
        return parsed, True
    except json.JSONDecodeError:
        pass

    return None, False


def run_model(model: str, wines: list, dry_run: bool = False) -> list:
    """Run one model against all wines. Supports resume via saved results."""
    slug = model_slug(model)
    out_dir = RESULTS_DIR / slug
    out_dir.mkdir(parents=True, exist_ok=True)

    results = []
    skipped = 0
    errors = 0

    for i, wine in enumerate(wines):
        wine_id = wine["wine_id"]
        result_file = out_dir / f"{wine_id}.json"

        # Resume: skip if already done
        if result_file.exists():
            with open(result_file, "r", encoding="utf-8") as f:
                saved = json.load(f)
            results.append(saved)
            skipped += 1
            continue

        if dry_run:
            prompt = build_grade_a_prompt(wine["context"])
            print(f"  [DRY RUN] {wine['display_name']} ({wine['tier']}) — prompt {len(prompt)} chars")
            continue

        # Build prompt using the production Grade A prompt builder
        prompt = build_grade_a_prompt(wine["context"])

        # Call API
        resp = call_openrouter(model, prompt)

        if resp.get("error"):
            errors += 1
            result = {
                "wine_id": wine_id,
                "model": model,
                "tier": wine["tier"],
                "display_name": wine["display_name"],
                "error": resp["error"],
                "raw_content": None,
                "parsed": None,
                "parse_ok": False,
                "tokens_in": resp.get("tokens_in", 0),
                "tokens_out": resp.get("tokens_out", 0),
                "elapsed": resp.get("elapsed", 0),
            }
        else:
            parsed, parse_ok = try_parse_json(resp["raw_content"])
            result = {
                "wine_id": wine_id,
                "model": model,
                "tier": wine["tier"],
                "display_name": wine["display_name"],
                "raw_content": resp["raw_content"],
                "parsed": parsed,
                "parse_ok": parse_ok,
                "tokens_in": resp["tokens_in"],
                "tokens_out": resp["tokens_out"],
                "elapsed": resp["elapsed"],
                "error": None,
            }

        # Save immediately for resume
        with open(result_file, "w", encoding="utf-8") as f:
            json.dump(result, f, indent=2, ensure_ascii=False)
        results.append(result)

        # Progress
        if (i + 1) % 10 == 0 or (i + 1) == len(wines):
            print(f"  {i + 1}/{len(wines)} (errors: {errors})")

        # Brief pause
        time.sleep(0.1)

    if skipped:
        print(f"  Resumed: {skipped} cached, {len(wines) - skipped} new")

    return results


def print_summary(wines: list):
    """Print per-model summary from saved results."""
    print("\n" + "=" * 110)
    print("TASK 3 SUMMARY — Customer-Facing Prose (30 wines × 21 models)")
    print("=" * 110)
    print(f"{'#':>2}  {'Model':<35} {'Done':>4} {'Err':>4} {'Parse':>6} "
          f"{'AvgIn':>7} {'AvgOut':>7} {'AvgSec':>7} {'Est$':>7}")
    print("-" * 110)

    summaries = []
    for model in TASK3_MODELS:
        slug = model_slug(model)
        out_dir = RESULTS_DIR / slug
        if not out_dir.exists():
            continue

        total = 0
        errors = 0
        parse_ok = 0
        tokens_in = 0
        tokens_out = 0
        elapsed = 0.0

        for wine in wines:
            result_file = out_dir / f"{wine['wine_id']}.json"
            if not result_file.exists():
                continue
            r = json.load(open(result_file, encoding="utf-8"))
            total += 1
            if r.get("error"):
                errors += 1
            if r.get("parse_ok"):
                parse_ok += 1
            tokens_in += r.get("tokens_in", 0)
            tokens_out += r.get("tokens_out", 0)
            elapsed += r.get("elapsed", 0)

        if total == 0:
            continue

        avg_in = tokens_in / total
        avg_out = tokens_out / total
        avg_sec = elapsed / total
        parse_rate = parse_ok / total

        pricing = MODEL_PRICING.get(model, (1.0, 3.0))
        est_cost = (tokens_in * pricing[0] + tokens_out * pricing[1]) / 1_000_000

        summaries.append({
            "model": model,
            "total": total,
            "errors": errors,
            "parse_rate": parse_rate,
            "avg_in": avg_in,
            "avg_out": avg_out,
            "avg_sec": avg_sec,
            "est_cost": est_cost,
        })

    for i, s in enumerate(summaries):
        short = s["model"].split("/")[-1][:33]
        print(f"{i + 1:>2}  {short:<35} {s['total']:>4} {s['errors']:>4} {s['parse_rate']:>5.0%} "
              f"{s['avg_in']:>7.0f} {s['avg_out']:>7.0f} {s['avg_sec']:>6.1f}s ${s['est_cost']:>6.3f}")

    print("=" * 110)
    total_cost = sum(s["est_cost"] for s in summaries)
    total_calls = sum(s["total"] for s in summaries)
    total_errors = sum(s["errors"] for s in summaries)
    print(f"Total: {total_calls} calls, {total_errors} errors, ~${total_cost:.2f} estimated cost")


def main():
    parser = argparse.ArgumentParser(description="B5.5: Run Task 3 (prose generation)")
    parser.add_argument("--model", help="Run a single model (OpenRouter slug)")
    parser.add_argument("--models", help="Comma-separated list of models to run")
    parser.add_argument("--wines", help="Comma-separated wine IDs to limit to")
    parser.add_argument("--dry-run", action="store_true", help="Show prompts without API calls")
    parser.add_argument("--summary-only", action="store_true", help="Just print summary from saved results")
    args = parser.parse_args()

    if not OPENROUTER_API_KEY and not args.summary_only:
        print("ERROR: OPENROUTER_API_KEY not set")
        sys.exit(1)

    # Load wine contexts
    with open(CONTEXTS_FILE, "r", encoding="utf-8") as f:
        wines = json.load(f)

    # Filter wines if --wines specified
    if args.wines:
        wanted_ids = {w.strip() for w in args.wines.split(",") if w.strip()}
        wines = [w for w in wines if w["wine_id"] in wanted_ids]
        missing = wanted_ids - {w["wine_id"] for w in wines}
        if missing:
            print(f"WARNING: wines not found: {missing}")

    print(f"Loaded {len(wines)} wines from {CONTEXTS_FILE}")
    for tier in ["A", "B", "C"]:
        count = sum(1 for w in wines if w["tier"] == tier)
        print(f"  Tier {tier}: {count}")

    if args.summary_only:
        print_summary(wines)
        return

    # Determine which models to run
    if args.models:
        models = [m.strip() for m in args.models.split(",") if m.strip()]
    elif args.model:
        models = [args.model]
    else:
        models = TASK3_MODELS

    print(f"\nRunning {len(models)} models x {len(wines)} wines = {len(models) * len(wines)} calls")
    if args.dry_run:
        print("[DRY RUN MODE]")

    for i, model in enumerate(models):
        print(f"\n[{i + 1}/{len(models)}] {model}")
        results = run_model(model, wines, dry_run=args.dry_run)
        if not args.dry_run and results:
            ok = sum(1 for r in results if r.get("parse_ok"))
            err = sum(1 for r in results if r.get("error"))
            print(f"  Parse: {ok}/{len(results)}, Errors: {err}")

    # Print summary
    if not args.dry_run:
        print_summary(wines)

    print("\nDone.")


if __name__ == "__main__":
    main()
