#!/usr/bin/env python3
"""B5.4: Run + score Task 2 (structured data extraction) across 17 models.

Sends 50 HTML pages to each model via OpenRouter, collects extracted data,
scores against ground truth, and writes bakeoff/scores/task2_scores.csv.

Usage:
    python -m bakeoff.run_task2                    # run all models
    python -m bakeoff.run_task2 --model google/gemini-2.5-flash  # single model
    python -m bakeoff.run_task2 --score-only       # just re-score from saved results
    python -m bakeoff.run_task2 --dry-run          # show what would be sent, no API calls
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

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data" / "task2"
RESULTS_DIR = BASE_DIR / "results" / "task2"
SCORES_DIR = BASE_DIR / "scores"

# 17 models from MODELS.md (Task 2 list)
TASK2_MODELS = [
    "anthropic/claude-sonnet-4.6",
    "anthropic/claude-haiku-4.5",
    "deepseek/deepseek-v3.2",
    "google/gemini-2.5-flash",
    "google/gemini-2.5-pro",
    "qwen/qwen3.5-plus-02-15",
    "openai/gpt-5.4-mini",
    "meta-llama/llama-4-maverick",
    "mistralai/mistral-large",
    "openai/gpt-5.4",
    "qwen/qwen3.6-plus",
    "x-ai/grok-4.1-fast",
    "google/gemini-3.1-pro-preview",
    "google/gemini-3-flash-preview",
    "google/gemini-3.1-flash-lite-preview",
    "xiaomi/mimo-v2-flash",
    "minimax/minimax-m2.7",
]

MODEL_PRICING = {
    "anthropic/claude-sonnet-4.6": (3.0, 15.0),
    "anthropic/claude-haiku-4.5": (0.80, 4.0),
    "deepseek/deepseek-v3.2": (0.14, 0.28),
    "google/gemini-2.5-flash": (0.15, 0.60),
    "google/gemini-2.5-pro": (1.25, 10.0),
    "qwen/qwen3.5-plus-02-15": (0.40, 1.20),
    "openai/gpt-5.4-mini": (0.40, 1.60),
    "meta-llama/llama-4-maverick": (0.20, 0.60),
    "mistralai/mistral-large": (2.0, 6.0),
    "openai/gpt-5.4": (2.50, 10.0),
    "qwen/qwen3.6-plus": (0.40, 1.20),
    "x-ai/grok-4.1-fast": (0.60, 4.0),
    "google/gemini-3.1-pro-preview": (1.25, 10.0),
    "google/gemini-3-flash-preview": (0.15, 0.60),
    "google/gemini-3.1-flash-lite-preview": (0.02, 0.10),
    "xiaomi/mimo-v2-flash": (0.07, 0.28),
    "minimax/minimax-m2.7": (0.50, 2.0),
}

PROMPT_TEMPLATE = """Extract structured wine data from this webpage HTML. Return ONLY valid JSON.

For every field: if the information is not explicitly stated on the page, return null.
Do NOT guess, estimate, or infer values that are not clearly present in the text.
A null is always preferable to a wrong number.

## HTML Content
{raw_html}

## Output Format (JSON only, no markdown fencing)

{{"wines_found": [{{"name": "wine name as it appears on the page", "vintage_year": null, "abv_pct": null, "blend": null, "oak": null, "cases_produced": null, "chemistry": null, "winemaker": null, "farming": null}}]}}

Field details:
- vintage_year: integer or null
- abv_pct: number or null (e.g. 14.5)
- blend: [{{"grape": "name", "pct": number}}] or null
- oak: {{"vessel": "barrel type", "months": integer, "new_pct": number, "origin": "French/American/etc"}} with nulls for unknown subfields
- cases_produced: integer or null
- chemistry: {{"ph": number, "ta_g_l": number, "rs_g_l": number, "so2_total_mg_l": number}} with nulls for unknown subfields
- winemaker: "name" or null
- farming: "organic"/"biodynamic"/"sustainable"/"conventional" or null"""


def model_slug(model_id: str) -> str:
    return model_id.replace("/", "__")


def call_openrouter(model: str, prompt: str, retries: int = 2) -> dict:
    headers = {
        "Authorization": f"Bearer {OPENROUTER_API_KEY}",
        "Content-Type": "application/json",
        "HTTP-Referer": "https://loam.onrender.com",
        "X-Title": "Loam Bake-off",
    }

    max_tok = 2000
    if "2.5-pro" in model or "3.1-pro" in model:
        max_tok = 8000  # reasoning tokens

    body = {
        "model": model,
        "messages": [{"role": "user", "content": prompt}],
        "temperature": 0.0,
        "max_tokens": max_tok,
    }

    if "gemini" in model and "2.5-pro" not in model and "3.1-pro" not in model:
        body["provider"] = {"require_parameters": True}
        body["reasoning"] = {"effort": "none"}
    if "o4-mini" in model:
        body["reasoning"] = {"effort": "low"}

    for attempt in range(retries + 1):
        try:
            t0 = time.time()
            resp = http_requests.post(OPENROUTER_URL, headers=headers, json=body, timeout=120)
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
                return {"error": f"HTTP {resp.status_code}: {resp.text[:200]}", "elapsed": elapsed,
                        "tokens_in": 0, "tokens_out": 0}

            data = resp.json()
            content = data.get("choices", [{}])[0].get("message", {}).get("content", "")
            usage = data.get("usage", {})

            return {"raw_content": content, "tokens_in": usage.get("prompt_tokens", 0),
                    "tokens_out": usage.get("completion_tokens", 0), "elapsed": elapsed, "error": None}

        except http_requests.exceptions.Timeout:
            if attempt < retries:
                time.sleep(2)
                continue
            return {"error": "Timeout", "elapsed": 120, "tokens_in": 0, "tokens_out": 0}
        except Exception as e:
            if attempt < retries:
                time.sleep(2)
                continue
            return {"error": str(e), "elapsed": 0, "tokens_in": 0, "tokens_out": 0}

    return {"error": "Max retries exceeded", "elapsed": 0, "tokens_in": 0, "tokens_out": 0}


def parse_extraction(raw: str) -> dict:
    if not raw:
        return {"wines_found": [], "parse_ok": False}

    cleaned = re.sub(r"^```(?:json)?\s*", "", raw.strip())
    cleaned = re.sub(r"\s*```$", "", cleaned.strip())

    # Try to find JSON object
    # Look for the outermost { }
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
                    wines = parsed.get("wines_found", [])
                    if isinstance(wines, list):
                        return {"wines_found": wines, "parse_ok": True}
                except json.JSONDecodeError:
                    pass

    # Try the whole string
    try:
        parsed = json.loads(cleaned)
        wines = parsed.get("wines_found", [])
        if isinstance(wines, list):
            return {"wines_found": wines, "parse_ok": True}
    except json.JSONDecodeError:
        pass

    return {"wines_found": [], "parse_ok": False}


def truncate_html(html: str, max_chars: int = 80000) -> str:
    """Truncate HTML to fit within token limits. Strip scripts/styles first."""
    # Remove script and style tags
    html = re.sub(r'<script[^>]*>.*?</script>', '', html, flags=re.DOTALL | re.IGNORECASE)
    html = re.sub(r'<style[^>]*>.*?</style>', '', html, flags=re.DOTALL | re.IGNORECASE)
    html = re.sub(r'<noscript[^>]*>.*?</noscript>', '', html, flags=re.DOTALL | re.IGNORECASE)
    # Remove HTML comments
    html = re.sub(r'<!--.*?-->', '', html, flags=re.DOTALL)
    # Collapse whitespace
    html = re.sub(r'\s+', ' ', html)

    if len(html) > max_chars:
        html = html[:max_chars] + "\n[TRUNCATED]"
    return html


def load_pages() -> list:
    """Load all test pages with HTML and ground truth."""
    pages = []
    for i in range(1, 100):  # scan up to 99 pages
        html_file = DATA_DIR / f"final_page_{i:03d}.html"
        truth_file = DATA_DIR / f"truth_page_{i:03d}.json"
        meta_file = DATA_DIR / f"final_page_{i:03d}_meta.json"

        if not html_file.exists() or not truth_file.exists():
            continue

        html = html_file.read_text(encoding="utf-8", errors="ignore")
        truth = json.loads(truth_file.read_text(encoding="utf-8"))
        meta = json.loads(meta_file.read_text(encoding="utf-8")) if meta_file.exists() else {}

        pages.append({
            "page_id": f"page_{i:03d}",
            "html_file": str(html_file),
            "html": html,
            "truth": truth,
            "meta": meta,
            "producer": truth.get("producer", meta.get("producer", "")),
            "wine": truth.get("wine_name", meta.get("wine", "")),
        })

    return pages


def run_model(model: str, pages: list, dry_run: bool = False) -> list:
    slug = model_slug(model)
    out_dir = RESULTS_DIR / slug
    out_dir.mkdir(parents=True, exist_ok=True)

    results = []
    skipped = 0
    errors = 0

    for i, page in enumerate(pages):
        page_id = page["page_id"]
        result_file = out_dir / f"{page_id}.json"

        if result_file.exists():
            with open(result_file, "r", encoding="utf-8") as f:
                saved = json.load(f)
            results.append(saved)
            skipped += 1
            continue

        if dry_run:
            html_len = len(page["html"])
            print(f"  [DRY RUN] {page_id}: {page['producer']} — {page['wine']} ({html_len:,} bytes)")
            continue

        # Truncate and clean HTML
        clean_html = truncate_html(page["html"])
        prompt = PROMPT_TEMPLATE.format(raw_html=clean_html)

        resp = call_openrouter(model, prompt)

        if resp.get("error"):
            errors += 1
            result = {
                "page_id": page_id, "model": model, "error": resp["error"],
                "wines_found": [], "parse_ok": False,
                "tokens_in": resp.get("tokens_in", 0), "tokens_out": resp.get("tokens_out", 0),
                "elapsed": resp.get("elapsed", 0),
            }
        else:
            parsed = parse_extraction(resp["raw_content"])
            result = {
                "page_id": page_id, "model": model,
                "raw_content": resp["raw_content"],
                "wines_found": parsed["wines_found"],
                "parse_ok": parsed["parse_ok"],
                "tokens_in": resp["tokens_in"], "tokens_out": resp["tokens_out"],
                "elapsed": resp["elapsed"], "error": None,
            }

        with open(result_file, "w", encoding="utf-8") as f:
            json.dump(result, f, indent=2, ensure_ascii=False)
        results.append(result)

        if (i + 1) % 10 == 0 or (i + 1) == len(pages):
            print(f"  {i + 1}/{len(pages)} (errors: {errors})")

        time.sleep(0.1)

    if skipped:
        print(f"  Resumed: {skipped} cached, {len(pages) - skipped} new")

    return results


def compare_numeric(extracted, truth, tolerance=0.1):
    """Compare numeric values with tolerance. Returns (match, hallucinated, missed)."""
    if truth is None and extracted is None:
        return "correct_null", None
    if truth is None and extracted is not None:
        return "hallucinated", extracted
    if truth is not None and extracted is None:
        return "missed", truth
    # Both non-null
    try:
        e = float(extracted)
        t = float(truth)
        if abs(e - t) <= tolerance:
            return "correct", None
        else:
            return "wrong", (extracted, truth)
    except (ValueError, TypeError):
        return "wrong", (extracted, truth)


def score_model(model: str, pages: list) -> dict:
    slug = model_slug(model)
    out_dir = RESULTS_DIR / slug

    # Numeric fields to check
    NUM_FIELDS = ["vintage_year", "abv_pct", "cases_produced"]
    CHEM_FIELDS = ["ph", "ta_g_l", "rs_g_l", "so2_total_mg_l"]
    OAK_NUM_FIELDS = ["months", "new_pct"]
    TEXT_FIELDS = ["winemaker", "farming"]

    stats = {
        "total_pages": 0, "parse_ok": 0, "errors": 0,
        "num_correct": 0, "num_wrong": 0, "num_hallucinated": 0, "num_missed": 0, "num_correct_null": 0,
        "text_correct": 0, "text_wrong": 0, "text_hallucinated": 0, "text_missed": 0, "text_correct_null": 0,
        "blend_correct": 0, "blend_wrong": 0, "blend_hallucinated": 0, "blend_missed": 0, "blend_correct_null": 0,
        "total_tokens_in": 0, "total_tokens_out": 0, "total_elapsed": 0.0,
        "field_stats": {},
    }

    for page in pages:
        page_id = page["page_id"]
        result_file = out_dir / f"{page_id}.json"
        if not result_file.exists():
            continue

        result = json.load(open(result_file, encoding="utf-8"))
        truth = page["truth"]["extraction"]
        stats["total_pages"] += 1
        stats["total_tokens_in"] += result.get("tokens_in", 0)
        stats["total_tokens_out"] += result.get("tokens_out", 0)
        stats["total_elapsed"] += result.get("elapsed", 0)

        if result.get("error"):
            stats["errors"] += 1
            continue
        if result.get("parse_ok"):
            stats["parse_ok"] += 1

        # Get the first extracted wine (or empty dict)
        wines = result.get("wines_found", [])
        ext = wines[0] if wines else {}

        # Score numeric fields
        for field in NUM_FIELDS:
            t_val = truth.get(field)
            e_val = ext.get(field)
            tol = 0.1 if field == "abv_pct" else 0
            outcome, _ = compare_numeric(e_val, t_val, tolerance=tol)
            cat = "num_" + outcome.split("_")[0] if "correct" in outcome else "num_" + outcome
            if outcome == "correct_null":
                cat = "num_correct_null"
            elif outcome == "correct":
                cat = "num_correct"
            stats[cat] = stats.get(cat, 0) + 1
            stats["field_stats"].setdefault(field, {"correct": 0, "wrong": 0, "hallucinated": 0, "missed": 0, "correct_null": 0})
            stats["field_stats"][field][outcome.replace("correct_null", "correct_null")] += 1

        # Score chemistry subfields
        t_chem = truth.get("chemistry") or {}
        e_chem = ext.get("chemistry") or {}
        for field in CHEM_FIELDS:
            t_val = t_chem.get(field) if t_chem else None
            e_val = e_chem.get(field) if e_chem else None
            outcome, _ = compare_numeric(e_val, t_val, tolerance=0.1)
            cat = "num_" + outcome.split("_")[0] if "correct" in outcome else "num_" + outcome
            if outcome == "correct_null":
                cat = "num_correct_null"
            elif outcome == "correct":
                cat = "num_correct"
            stats[cat] = stats.get(cat, 0) + 1
            stats["field_stats"].setdefault(f"chem_{field}", {"correct": 0, "wrong": 0, "hallucinated": 0, "missed": 0, "correct_null": 0})
            stats["field_stats"][f"chem_{field}"][outcome.replace("correct_null", "correct_null")] += 1

        # Score oak subfields
        t_oak = truth.get("oak") if isinstance(truth.get("oak"), dict) else {}
        e_oak = ext.get("oak") if isinstance(ext.get("oak"), dict) else {}
        for field in OAK_NUM_FIELDS:
            t_val = t_oak.get(field) if t_oak else None
            e_val = e_oak.get(field) if e_oak else None
            outcome, _ = compare_numeric(e_val, t_val, tolerance=0.5)
            cat = "num_" + outcome.split("_")[0] if "correct" in outcome else "num_" + outcome
            if outcome == "correct_null":
                cat = "num_correct_null"
            elif outcome == "correct":
                cat = "num_correct"
            stats[cat] = stats.get(cat, 0) + 1

        # Score text fields
        for field in TEXT_FIELDS:
            t_val = truth.get(field)
            e_val = ext.get(field)
            if t_val is None and e_val is None:
                stats["text_correct_null"] += 1
            elif t_val is None and e_val is not None:
                stats["text_hallucinated"] += 1
            elif t_val is not None and e_val is None:
                stats["text_missed"] += 1
            elif t_val and e_val:
                if str(t_val).lower().strip() == str(e_val).lower().strip():
                    stats["text_correct"] += 1
                else:
                    stats["text_wrong"] += 1

        # Score blend
        t_blend = truth.get("blend")
        e_blend = ext.get("blend")
        if t_blend is None and e_blend is None:
            stats["blend_correct_null"] += 1
        elif t_blend is None and e_blend is not None:
            stats["blend_hallucinated"] += 1
        elif t_blend is not None and e_blend is None:
            stats["blend_missed"] += 1
        elif t_blend and e_blend:
            # Check grape names match
            t_grapes = {g.get("grape", "").lower() for g in t_blend if isinstance(g, dict)}
            e_grapes = {g.get("grape", "").lower() for g in e_blend if isinstance(g, dict)}
            if t_grapes == e_grapes:
                stats["blend_correct"] += 1
            else:
                stats["blend_wrong"] += 1

    # Calculate rates
    n = stats["total_pages"]
    num_total = stats["num_correct"] + stats["num_wrong"] + stats["num_hallucinated"] + stats["num_missed"] + stats["num_correct_null"]
    num_answered = stats["num_correct"] + stats["num_wrong"]
    num_null_truth = stats["num_correct_null"] + stats["num_hallucinated"]
    num_present_truth = stats["num_correct"] + stats["num_wrong"] + stats["num_missed"]

    num_accuracy = stats["num_correct"] / num_present_truth if num_present_truth > 0 else 0
    halluc_rate = stats["num_hallucinated"] / num_null_truth if num_null_truth > 0 else 0
    null_recall = stats["num_correct_null"] / num_null_truth if num_null_truth > 0 else 0
    miss_rate = stats["num_missed"] / num_present_truth if num_present_truth > 0 else 0

    parse_rate = stats["parse_ok"] / n if n > 0 else 0
    avg_tokens_in = stats["total_tokens_in"] / n if n > 0 else 0
    avg_tokens_out = stats["total_tokens_out"] / n if n > 0 else 0

    pricing = MODEL_PRICING.get(model, (1.0, 3.0))
    cost_per_call = (avg_tokens_in * pricing[0] + avg_tokens_out * pricing[1]) / 1_000_000
    cost_at_50k = cost_per_call * 50000

    return {
        "model": model,
        "num_accuracy": round(num_accuracy, 4),
        "halluc_rate": round(halluc_rate, 4),
        "null_recall": round(null_recall, 4),
        "miss_rate": round(miss_rate, 4),
        "text_correct": stats["text_correct"],
        "text_hallucinated": stats["text_hallucinated"],
        "blend_correct": stats["blend_correct"],
        "blend_hallucinated": stats["blend_hallucinated"],
        "parse_rate": round(parse_rate, 4),
        "avg_tokens_in": round(avg_tokens_in),
        "avg_tokens_out": round(avg_tokens_out),
        "cost_per_call": round(cost_per_call, 6),
        "cost_at_50k": round(cost_at_50k, 2),
        "errors": stats["errors"],
        "total": n,
    }


def write_scores(scores: list):
    SCORES_DIR.mkdir(parents=True, exist_ok=True)
    out_file = SCORES_DIR / "task2_scores.csv"

    fields = [
        "model", "num_accuracy", "halluc_rate", "null_recall", "miss_rate",
        "text_correct", "text_hallucinated", "blend_correct", "blend_hallucinated",
        "parse_rate", "avg_tokens_in", "avg_tokens_out",
        "cost_per_call", "cost_at_50k", "errors", "total",
    ]

    scores.sort(key=lambda s: s["num_accuracy"], reverse=True)

    with open(out_file, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(scores)

    print(f"\nScores written to {out_file}")
    return out_file


def print_leaderboard(scores: list):
    scores.sort(key=lambda s: s["num_accuracy"], reverse=True)
    print("\n" + "=" * 110)
    print("TASK 2 LEADERBOARD — Structured Data Extraction (50 pages)")
    print("=" * 110)
    print(f"{'#':>2}  {'Model':<35} {'NumAcc':>7} {'Halluc':>7} {'NullRec':>7} {'Miss':>6} "
          f"{'Parse':>6} {'$/50K':>8}")
    print("-" * 110)
    for i, s in enumerate(scores):
        short = s["model"].split("/")[-1][:33]
        print(f"{i + 1:>2}  {short:<35} {s['num_accuracy']:>6.1%} {s['halluc_rate']:>6.1%} "
              f"{s['null_recall']:>6.1%} {s['miss_rate']:>5.1%} "
              f"{s['parse_rate']:>5.0%} ${s['cost_at_50k']:>7.2f}")
    print("=" * 110)
    print("NumAcc = % of present numeric fields extracted correctly")
    print("Halluc = % of absent fields where model invented a value (lower is better)")
    print("NullRec = % of absent fields correctly returned as null (higher is better)")
    print("Miss = % of present fields the model returned null for (lower is better)")


def main():
    parser = argparse.ArgumentParser(description="B5.4: Run + score Task 2 (extraction)")
    parser.add_argument("--model", help="Run a single model (OpenRouter slug)")
    parser.add_argument("--score-only", action="store_true", help="Score from saved results only")
    parser.add_argument("--dry-run", action="store_true", help="Show prompts without API calls")
    args = parser.parse_args()

    if not OPENROUTER_API_KEY and not args.score_only:
        print("ERROR: OPENROUTER_API_KEY not set")
        sys.exit(1)

    pages = load_pages()
    print(f"Loaded {len(pages)} pages from {DATA_DIR}")

    if args.model:
        models = [args.model]
    else:
        models = TASK2_MODELS

    if not args.score_only:
        print(f"\nRunning {len(models)} models x {len(pages)} pages = {len(models) * len(pages)} calls")
        if args.dry_run:
            print("[DRY RUN MODE]")

        for i, model in enumerate(models):
            print(f"\n[{i + 1}/{len(models)}] {model}")
            run_model(model, pages, dry_run=args.dry_run)

    print("\n\nScoring all models...")
    all_scores = []
    for model in models:
        slug_dir = RESULTS_DIR / model_slug(model)
        if not slug_dir.exists():
            print(f"  {model}: no results found, skipping")
            continue
        score = score_model(model, pages)
        if score:
            all_scores.append(score)
            print(f"  {model}: NumAcc={score['num_accuracy']:.1%}, "
                  f"Halluc={score['halluc_rate']:.1%}, Parse={score['parse_rate']:.0%}")

    if all_scores:
        write_scores(all_scores)
        print_leaderboard(all_scores)

    print("\nDone.")


if __name__ == "__main__":
    main()
