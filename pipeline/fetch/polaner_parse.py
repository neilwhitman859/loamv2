#!/usr/bin/env python3
"""
Polaner title parser — uses Claude Haiku to extract producer/wine_name from title strings.

Usage:
    python -m pipeline.fetch.polaner_parse              # dry-run
    python -m pipeline.fetch.polaner_parse --apply       # parse and update DB
    python -m pipeline.fetch.polaner_parse --stats       # show parse coverage
    python -m pipeline.fetch.polaner_parse --limit 50    # process only 50 rows
"""

import argparse
import json
import sys
import time
from pathlib import Path

import httpx

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from pipeline.lib.db import get_supabase, get_env

BATCH_SIZE = 20  # Titles per Haiku call


def show_stats(sb):
    total = sb.table("source_polaner").select("*", count="exact", head=True).execute().count or 0
    parsed = sb.table("source_polaner").select("*", count="exact", head=True).not_.is_("producer", "null").execute().count or 0
    print(f"source_polaner: {total} total, {parsed} parsed, {total - parsed} remaining")
    return total, parsed


def call_haiku(api_key: str, prompt: str) -> str:
    """Call Claude Haiku API and return text response."""
    resp = httpx.post(
        "https://api.anthropic.com/v1/messages",
        headers={
            "Content-Type": "application/json",
            "x-api-key": api_key,
            "anthropic-version": "2023-06-01",
        },
        json={
            "model": "claude-haiku-4-5-20251001",
            "max_tokens": 4096,
            "messages": [{"role": "user", "content": prompt}],
        },
        timeout=60.0,
    )
    resp.raise_for_status()
    return resp.json()["content"][0]["text"]


def parse_batch(api_key: str, batch: list[dict]) -> list[dict] | None:
    """Send a batch of titles to Haiku for parsing."""
    titles_block = "\n".join(
        f'{i+1}. "{r["title"]}" [country: {r.get("country") or "?"}, '
        f'region: {r.get("region") or "?"}, appellation: {r.get("appellation") or "?"}]'
        for i, r in enumerate(batch)
    )

    prompt = f"""You are a wine data expert. Parse each wine title into producer name and wine name.

The title format is: "{{Producer Name}} {{Wine/Cuvee Name}} {{Region/Appellation}}".
The appellation/region context is provided in brackets to help you identify where the producer name ends.

Rules:
- The producer is typically the first part of the title (a person's name, estate name, or domaine name)
- The wine name includes the cuvee/vineyard/designation and often the appellation
- If the title IS just "Producer Appellation" with no cuvee, the wine_name should be the appellation
- Remove "[base YYYY.x]", "GIFT BOX", "[lieu dit]" suffixes
- Preserve accents and special characters exactly as they appear
- For reversed names like "Sigaut Anne & Herve", normalize to "Anne & Herve Sigaut"

Return ONLY a JSON array (no markdown, no explanation) with objects: {{"i": <1-based index>, "producer": "...", "wine_name": "..."}}

Titles:
{titles_block}"""

    response = call_haiku(api_key, prompt)
    text = response.strip()
    if text.startswith("```"):
        text = text.removeprefix("```json").removeprefix("```").removesuffix("```").strip()

    try:
        return json.loads(text)
    except json.JSONDecodeError as e:
        print(f"  JSON parse error: {e}")
        return None


def main():
    parser = argparse.ArgumentParser(description="Parse Polaner wine titles with Claude Haiku")
    parser.add_argument("--apply", action="store_true", help="Update database")
    parser.add_argument("--stats", action="store_true", help="Show stats only")
    parser.add_argument("--limit", type=int, help="Max rows to process")
    args = parser.parse_args()

    sb = get_supabase()
    api_key = get_env("ANTHROPIC_API_KEY")

    if args.stats:
        show_stats(sb)
        return

    print("=== Polaner Title Parser (Claude Haiku) ===\n")
    show_stats(sb)

    # Fetch unparsed rows (paginate past 1000 limit)
    all_rows = []
    offset = 0
    max_rows = args.limit or float("inf")
    while len(all_rows) < max_rows:
        fetch_limit = min(1000, int(max_rows - len(all_rows)))
        result = (sb.table("source_polaner")
                  .select("id, title, country, region, appellation")
                  .is_("producer", "null")
                  .order("title")
                  .range(offset, offset + fetch_limit - 1)
                  .execute())
        if not result.data:
            break
        all_rows.extend(result.data)
        offset += 1000
        if len(result.data) < fetch_limit:
            break

    if not all_rows:
        print("\nAll titles already parsed!")
        return

    total_batches = (len(all_rows) + BATCH_SIZE - 1) // BATCH_SIZE
    print(f"\nProcessing {len(all_rows)} titles in {total_batches} batches...\n")

    total_parsed = 0
    total_errors = 0

    for i in range(0, len(all_rows), BATCH_SIZE):
        batch = all_rows[i:i + BATCH_SIZE]
        batch_num = i // BATCH_SIZE + 1

        print(f"  Batch {batch_num}/{total_batches} ({len(batch)} titles)...", end="", flush=True)

        try:
            results = parse_batch(api_key, batch)
            if not results:
                print(" PARSE ERROR")
                total_errors += len(batch)
                continue

            batch_parsed = 0
            for result in results:
                idx = result.get("i", 0) - 1
                if idx < 0 or idx >= len(batch):
                    continue
                row = batch[idx]
                producer = (result.get("producer") or "").strip()
                wine_name = (result.get("wine_name") or "").strip()

                if not producer or not wine_name:
                    total_errors += 1
                    continue

                if args.apply:
                    sb.table("source_polaner").update({
                        "producer": producer,
                        "wine_name": wine_name,
                    }).eq("id", row["id"]).execute()
                else:
                    if batch_parsed == 0:
                        print()
                    print(f'    "{row["title"]}" -> producer: "{producer}" | wine: "{wine_name}"')

                batch_parsed += 1

            total_parsed += batch_parsed
            if args.apply:
                print(f" {batch_parsed} parsed")

            if i + BATCH_SIZE < len(all_rows):
                time.sleep(0.2)

        except Exception as e:
            print(f" ERROR: {e}")
            total_errors += len(batch)
            if "429" in str(e):
                print("  Rate limited, waiting 30s...")
                time.sleep(30)

    print(f"\n=== Done ===")
    print(f"Parsed: {total_parsed}, Errors: {total_errors}")
    if not args.apply:
        print("(dry-run — use --apply to update database)")
    show_stats(sb)


if __name__ == "__main__":
    main()
