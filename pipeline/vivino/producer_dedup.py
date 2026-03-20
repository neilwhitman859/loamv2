"""
Producer dedup pipeline for Loam v2.
Sends fuzzy-matched producer name pairs to Claude Haiku for merge/keep_separate verdicts.
Reads from and writes to the producer_dedup_pairs table in Supabase.

Usage:
    python -m pipeline.vivino.producer_dedup --dry-run
    python -m pipeline.vivino.producer_dedup --limit=100
    python -m pipeline.vivino.producer_dedup
"""

import sys
import json
import time
import argparse
from pathlib import Path

import httpx

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from pipeline.lib.db import get_supabase, get_env

BATCH_SIZE = 50
MODEL = "claude-haiku-4-5-20251001"

SYSTEM_PROMPT = """You are a wine industry data expert helping deduplicate wine producer names.

For each numbered pair of producer names (from the same country), decide if they are the SAME producer or DIFFERENT producers.

Key rules:
- "Chateau X" and "Chateau X" = SAME (just accent difference)
- "Chateau X" and "Domaine X" = DIFFERENT (different business types in France)
- "Tenuta X" and "Fattoria X" = DIFFERENT (different estate types in Italy)
- "Cantina X" and "Cantine X" = SAME (singular vs plural)
- "X Winery" and "X Vineyards" = probably SAME (just suffix)
- "X" and "X Wines" = probably SAME
- "Domaine de X" and "Domaine X" = probably SAME
- "Cascina Ca' Rossa" and "Cascina Rossa" = DIFFERENT (Ca' Rossa is a specific name)
- "Castello di Gabiano" and "Castello di Gabbiano" = DIFFERENT (different places)
- Short names that are common words (e.g., "Aurora", "Carmen") matching longer names = usually DIFFERENT

Respond with ONLY a JSON array. Each element: {"pair": <number>, "verdict": "merge" or "separate", "reason": "<brief reason>"}
No other text, no markdown fences, just the JSON array."""


def get_pending_pairs(sb, limit=None):
    all_data = []
    page_size = 1000
    offset = 0
    max_rows = limit or 100000

    while len(all_data) < max_rows:
        fetch_size = min(page_size, max_rows - len(all_data))
        result = (
            sb.table("producer_dedup_pairs")
            .select("id, name_a, name_b, country, similarity")
            .eq("verdict", "pending")
            .order("similarity", desc=True)
            .range(offset, offset + fetch_size - 1)
            .execute()
        )
        if not result.data:
            break
        all_data.extend(result.data)
        offset += len(result.data)
        if len(result.data) < fetch_size:
            break
    return all_data


def build_batch_prompt(pairs):
    return "\n".join(
        f'{i + 1}. "{p["name_a"]}" vs "{p["name_b"]}" (country: {p["country"]}, similarity: {p.get("similarity", "?")})'
        for i, p in enumerate(pairs)
    )


def call_haiku(prompt, api_key):
    resp = httpx.post(
        "https://api.anthropic.com/v1/messages",
        headers={"x-api-key": api_key, "anthropic-version": "2023-06-01", "content-type": "application/json"},
        json={
            "model": MODEL, "max_tokens": 4096,
            "system": SYSTEM_PROMPT,
            "messages": [{"role": "user", "content": prompt}],
        },
        timeout=60,
    )
    resp.raise_for_status()
    data = resp.json()
    text = (data.get("content", [{}])[0].get("text", "") or "").strip()

    if text.startswith("```"):
        text = "\n".join(text.split("\n")[1:]) if "\n" in text else text[3:]
    if text.endswith("```"):
        text = text[:-3]
    text = text.strip()

    try:
        verdicts = json.loads(text)
        return {"verdicts": verdicts, "usage": data.get("usage", {})}
    except json.JSONDecodeError as e:
        print(f"  WARNING: Failed to parse Haiku response: {e}")
        print(f"  Response was: {text[:200]}...")
        return {"verdicts": None, "usage": data.get("usage", {})}


def update_verdicts(sb, pairs, verdicts):
    if not verdicts:
        return 0
    updated = 0
    for v in verdicts:
        pair_idx = v.get("pair", 0) - 1
        if pair_idx < 0 or pair_idx >= len(pairs):
            continue
        pair = pairs[pair_idx]
        verdict = "merge" if v.get("verdict") == "merge" else "keep_separate"
        try:
            sb.table("producer_dedup_pairs").update({"verdict": verdict, "verdict_source": "haiku"}).eq("id", pair["id"]).execute()
            updated += 1
        except Exception as e:
            print(f"  WARNING: Failed to update pair {pair['id']}: {e}")
    return updated


def main():
    parser = argparse.ArgumentParser(description="Producer dedup pipeline")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--limit", type=int, default=None)
    args = parser.parse_args()

    api_key = get_env("ANTHROPIC_API_KEY")
    sb = get_supabase()

    if args.dry_run:
        print("DRY RUN MODE - no API calls will be made")

    pairs = get_pending_pairs(sb, args.limit)
    total = len(pairs)
    print(f"Fetched {total} pending pairs")

    if total == 0:
        print("Nothing to process!")
        return

    total_input_tokens = 0
    total_output_tokens = 0
    total_merges = 0
    total_separates = 0
    total_updated = 0
    batch_num = 0
    total_batches = (total + BATCH_SIZE - 1) // BATCH_SIZE
    failed_batches = []

    for i in range(0, total, BATCH_SIZE):
        batch = pairs[i:i + BATCH_SIZE]
        batch_num += 1

        prompt = build_batch_prompt(batch)

        if args.dry_run:
            print(f"  Batch {batch_num}: {len(batch)} pairs (dry run, skipping API call)")
            continue

        print(f"  Batch {batch_num}/{total_batches}: {len(batch)} pairs...", end="", flush=True)

        attempts = 0
        max_attempts = 2
        while attempts < max_attempts:
            attempts += 1
            try:
                result = call_haiku(prompt, api_key)
                usage = result["usage"]
                total_input_tokens += usage.get("input_tokens", 0)
                total_output_tokens += usage.get("output_tokens", 0)

                if result["verdicts"]:
                    merges = sum(1 for v in result["verdicts"] if v.get("verdict") == "merge")
                    separates = sum(1 for v in result["verdicts"] if v.get("verdict") != "merge")
                    total_merges += merges
                    total_separates += separates

                    updated = update_verdicts(sb, batch, result["verdicts"])
                    total_updated += updated

                    print(f" merge:{merges} separate:{separates} (updated:{updated})")
                    break
                elif attempts < max_attempts:
                    print(" retry...", end="", flush=True)
                    time.sleep(1)
                else:
                    print(" parse error (giving up)")
                    failed_batches.append({"batchNum": batch_num, "startIdx": i, "count": len(batch)})
            except Exception as e:
                if attempts < max_attempts:
                    print(f" retry({e})...", end="", flush=True)
                    time.sleep(2)
                else:
                    print(f" error: {e}")
                    failed_batches.append({"batchNum": batch_num, "startIdx": i, "count": len(batch)})

        time.sleep(0.5)

    # Summary
    print(f"\n{'=' * 50}")
    print("PIPELINE COMPLETE")
    print("=" * 50)
    print(f"Total pairs processed: {total_updated}/{total}")
    print(f"Merges: {total_merges}")
    print(f"Separates: {total_separates}")
    print(f"Input tokens: {total_input_tokens:,}")
    print(f"Output tokens: {total_output_tokens:,}")

    input_cost = (total_input_tokens / 1_000_000) * 0.25
    output_cost = (total_output_tokens / 1_000_000) * 1.25
    print(f"Cost: ${input_cost + output_cost:.2f}")

    if failed_batches:
        batch_nums = ", ".join(f"#{b['batchNum']}" for b in failed_batches)
        print(f"\nFailed batches ({len(failed_batches)}): {batch_nums}")
        print("Re-run the pipeline to retry these (they remain 'pending').")


if __name__ == "__main__":
    main()
