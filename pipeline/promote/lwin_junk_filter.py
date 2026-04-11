#!/usr/bin/env python3
"""Haiku-powered junk filter for US 1-wine LWIN producers.

Context:
    LWIN's 1-wine US producer bucket contains ~1,300 producer names. Most are
    legitimate small wineries (Ahern, Canyon Road, Monte Volpe) but some are
    noise — orphan country names, trailing-whitespace artifacts, cuvée text
    mistakenly stored as producer, single-cuvée merchant labels.

    The long-tail sweep doesn't want to create canonical producers for the noise,
    so this pre-pass uses Haiku to classify each name as "real" or "junk" and
    writes the result to data/stats/lwin_us_junk_classifications.json. The sweep
    script reads the file and skips any producer classified "junk".

Usage:
    python -m pipeline.promote.lwin_junk_filter             # dry-run (no writes)
    python -m pipeline.promote.lwin_junk_filter --execute    # write classifications file
    python -m pipeline.promote.lwin_junk_filter --execute --budget 1.0
    python -m pipeline.promote.lwin_junk_filter --execute --bucket 1,2  # also classify 2-wine bucket

Output:
    data/stats/lwin_us_junk_classifications.json
      {
        "produced_at": "2026-04-10T...",
        "model": "claude-haiku-4-5-...",
        "bucket": "1",
        "total_input_tokens": ...,
        "total_output_tokens": ...,
        "cost_usd": 0.04,
        "classifications": {
          "Canyon Road": {"real": true, "reason": "California winery"},
          "Peru": {"real": false, "reason": "Country name, not a producer"},
          ...
        }
      }
"""

import argparse
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import anthropic

from pipeline.lib.db import get_conn, get_env

# ── Constants ────────────────────────────────────────────────
HAIKU_MODEL = "claude-haiku-4-5-20251001"
BATCH_SIZE = 30  # producer names per API call
MAX_TOKENS = 2048

# Haiku pricing (per million tokens)
INPUT_COST_PER_M = 0.80
OUTPUT_COST_PER_M = 4.00

OUTPUT_FILE = Path(__file__).resolve().parents[2] / "data" / "stats" / "lwin_us_junk_classifications.json"

SYSTEM_PROMPT = """You classify whether each entry is a legitimate US wine producer/winery name.

Return JSON array with one element per entry: [{"idx": 1, "real": true|false, "reason": "<short>"}]

Rules for "real" (true):
- Legitimate winery, vineyard, estate, or wine brand
- Unusual or obscure names are OK — many small wineries have creative names
- Named for people (e.g. "Jones Cellars") is fine

Rules for NOT real (false):
- Country/region name WITHOUT winery/vineyard/cellars qualifier (e.g. "Peru" alone)
- Trailing whitespace or punctuation artifact with no real content
- Obvious cuvée/vintage text (e.g. "2015 Reserve Red")
- Empty parenthesis or orphan parenthetical
- Pure generic word with no brand character (e.g. "Premium", "Red Wine")

When in doubt, mark as TRUE. We want to be inclusive — a real-but-obscure small winery is higher-value than filtering noise.

"reason" must be 6 words or fewer."""


def load_us_producers_by_bucket(conn, bucket: str):
    """Return the US producer names in the specified wine-count bucket.

    bucket: '1' | '2' | '3-4' | '5-7' | '8-9' | '10-19' | '20+'
    """
    bucket_filter = {
        "1":    "wines = 1",
        "2":    "wines = 2",
        "3-4":  "wines BETWEEN 3 AND 4",
        "5-7":  "wines BETWEEN 5 AND 7",
        "8-9":  "wines BETWEEN 8 AND 9",
        "10-19": "wines BETWEEN 10 AND 19",
        "20+":  "wines >= 20",
    }
    if bucket not in bucket_filter:
        raise ValueError(f"Unknown bucket: {bucket}")

    sql = f"""
        WITH us_buckets AS (
            SELECT producer_name, COUNT(*) AS wines
            FROM source_lwin
            WHERE country = 'United States'
              AND producer_name IS NOT NULL
              AND TRIM(producer_name) != ''
            GROUP BY producer_name
        )
        SELECT producer_name
        FROM us_buckets
        WHERE {bucket_filter[bucket]}
        ORDER BY producer_name
    """
    with conn.cursor() as cur:
        cur.execute(sql)
        return [row[0] for row in cur.fetchall()]


def build_prompt(batch):
    """Build the user prompt for a batch of producer names."""
    lines = ["Classify each of these US producer names:\n"]
    for i, name in enumerate(batch, 1):
        # Show the name with explicit quotes so trailing whitespace is visible
        lines.append(f'{i}. "{name}"')
    return "\n".join(lines)


def parse_response(text, batch):
    """Parse Haiku JSON response into {name: {"real": bool, "reason": str}}."""
    text = text.strip()
    if text.startswith("```"):
        text = text.split("\n", 1)[1]
        if text.endswith("```"):
            text = text[:-3]
        text = text.strip()

    start = text.find("[")
    end = text.rfind("]") + 1
    if start < 0 or end <= start:
        print(f"  WARNING: No JSON array found. Response: {text[:200]}")
        return {}

    try:
        items = json.loads(text[start:end])
    except json.JSONDecodeError as e:
        print(f"  WARNING: JSON parse error: {e}")
        print(f"  Snippet: {text[start:start+200]}")
        return {}

    results = {}
    for item in items:
        if not isinstance(item, dict):
            continue
        idx = item.get("idx")
        real = item.get("real")
        reason = item.get("reason", "")

        if not isinstance(idx, int) or idx < 1 or idx > len(batch):
            continue
        if not isinstance(real, bool):
            continue

        name = batch[idx - 1]
        results[name] = {"real": real, "reason": str(reason)[:80]}

    return results


def compute_cost(input_tokens, output_tokens):
    return (input_tokens * INPUT_COST_PER_M + output_tokens * OUTPUT_COST_PER_M) / 1_000_000


def main():
    parser = argparse.ArgumentParser(description="Haiku junk filter for US 1-wine LWIN producers")
    parser.add_argument("--execute", action="store_true", help="Write classifications file (default dry-run)")
    parser.add_argument("--budget", type=float, default=2.0, help="Max spend in USD (default $2.00)")
    parser.add_argument("--bucket", default="1", help="Comma-separated bucket(s): 1,2,3-4,5-7,8-9,10-19,20+")
    parser.add_argument("--limit", type=int, help="Max producers to process (debugging)")
    args = parser.parse_args()

    buckets = [b.strip() for b in args.bucket.split(",")]
    api_key = get_env("ANTHROPIC_API_KEY")
    client = anthropic.Anthropic(api_key=api_key)
    conn = get_conn()

    try:
        # Fetch producers for each requested bucket
        all_producers = []
        bucket_names_per_producer = {}
        for b in buckets:
            names = load_us_producers_by_bucket(conn, b)
            print(f"  bucket {b}: {len(names)} US producers")
            for n in names:
                if n not in bucket_names_per_producer:
                    all_producers.append(n)
                    bucket_names_per_producer[n] = b

        if args.limit:
            all_producers = all_producers[:args.limit]

        print(f"\nTotal distinct producers to classify: {len(all_producers)}")
        print(f"Batch size: {BATCH_SIZE}")
        print(f"Expected calls: {(len(all_producers) + BATCH_SIZE - 1) // BATCH_SIZE}")
        print(f"Budget: ${args.budget:.2f}")
        print(f"Mode: {'EXECUTE' if args.execute else 'DRY-RUN (no file written)'}\n")

        if not all_producers:
            print("Nothing to classify.")
            return

        classifications = {}
        total_input_tokens = 0
        total_output_tokens = 0
        cumulative_cost = 0.0
        errors = 0

        batches = [
            all_producers[i:i + BATCH_SIZE]
            for i in range(0, len(all_producers), BATCH_SIZE)
        ]

        for bi, batch in enumerate(batches):
            if cumulative_cost >= args.budget:
                print(f"\n  BUDGET EXCEEDED (${cumulative_cost:.4f}). Stopping.")
                break

            try:
                response = client.messages.create(
                    model=HAIKU_MODEL,
                    max_tokens=MAX_TOKENS,
                    system=SYSTEM_PROMPT,
                    messages=[{"role": "user", "content": build_prompt(batch)}],
                )
            except Exception as e:
                print(f"  Batch {bi+1}/{len(batches)}: API ERROR: {e}")
                errors += 1
                time.sleep(2)
                continue

            inp_tok = response.usage.input_tokens
            out_tok = response.usage.output_tokens
            total_input_tokens += inp_tok
            total_output_tokens += out_tok
            batch_cost = compute_cost(inp_tok, out_tok)
            cumulative_cost += batch_cost

            parsed = parse_response(response.content[0].text, batch)

            # Count real vs junk for this batch
            real_count = sum(1 for v in parsed.values() if v["real"])
            junk_count = sum(1 for v in parsed.values() if not v["real"])
            missing = len(batch) - len(parsed)

            classifications.update(parsed)

            print(
                f"  Batch {bi+1:3d}/{len(batches)} | "
                f"real={real_count:2d} junk={junk_count:2d} miss={missing} | "
                f"${batch_cost:.4f} cum=${cumulative_cost:.4f}"
            )

        # Assume missing entries are real (inclusive stance)
        for name in all_producers:
            if name not in classifications:
                classifications[name] = {"real": True, "reason": "unparsed — defaulted real"}

        real_total = sum(1 for v in classifications.values() if v["real"])
        junk_total = sum(1 for v in classifications.values() if not v["real"])

        print("\n=== Results ===")
        print(f"  Classified: {len(classifications)}")
        print(f"  Real:       {real_total}")
        print(f"  Junk:       {junk_total}")
        print(f"  Errors:     {errors}")
        print(f"  Input tok:  {total_input_tokens:,}")
        print(f"  Output tok: {total_output_tokens:,}")
        print(f"  Cost:       ${cumulative_cost:.4f}")

        # Show a sample of junk classifications
        print("\n  Sample junk classifications (first 20):")
        junk_sample = [(n, v) for n, v in classifications.items() if not v["real"]][:20]
        for name, v in junk_sample:
            print(f"    {name!r}: {v['reason']}")

        if args.execute:
            OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
            payload = {
                "produced_at": datetime.now(timezone.utc).isoformat(),
                "model": HAIKU_MODEL,
                "buckets": buckets,
                "total_input_tokens": total_input_tokens,
                "total_output_tokens": total_output_tokens,
                "cost_usd": round(cumulative_cost, 4),
                "real_count": real_total,
                "junk_count": junk_total,
                "classifications": classifications,
            }
            OUTPUT_FILE.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
            print(f"\n  Wrote: {OUTPUT_FILE}")
        else:
            print("\n  DRY-RUN — no file written. Use --execute.")

    finally:
        conn.close()


if __name__ == "__main__":
    main()
