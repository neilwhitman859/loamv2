#!/usr/bin/env python3
"""Fill grapes.color for grapes with NULL color using Haiku.

Uses established ampelographic knowledge — only writes where Haiku is
confident (>= 0.85). Skips obscure varieties it doesn't know.

Usage:
    python -m pipeline.analyze.grape_color_haiku [--dry-run] [--limit N]
"""

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from pipeline.lib.db import get_conn, get_env
from pipeline.lib.models import HAIKU_MODEL
CONFIDENCE_THRESHOLD = 0.85
BATCH_SIZE = 40


SYSTEM_PROMPT = """You are an expert ampelographer with comprehensive knowledge of grape varieties.
For each grape variety listed, determine its berry skin color based on established viticultural knowledge.

Respond with one line per grape in exactly this format:
GRAPE_NAME|color|confidence

Where:
- color is exactly "red" or "white" (red = dark-skinned/black/blue/pink berries that produce red/rosé wine; white = green/yellow/golden berries)
- confidence is a decimal 0.0-1.0 (use >= 0.90 only for varieties you are certain about; use 0.5 for unknown varieties)

Rules:
- Pink/rosé/grey mutations of white grapes (e.g. Pinot Gris, Gewurztraminer) = "white"
- Pink/rosé mutations of red grapes = "red"
- If the name contains a well-known parent variety (e.g. "AUTOFECONDATION SYRAH") → inherit that parent's color
- If you genuinely don't know → use confidence 0.5
- Do not guess. Accuracy > coverage.
"""


def fetch_colorless_grapes(cur):
    cur.execute("""
        SELECT id, name, vivc_number, species, grape_type
        FROM grapes
        WHERE color IS NULL
        ORDER BY name
    """)
    return cur.fetchall()


def build_prompt(batch):
    lines = ["Classify the berry skin color for these grape varieties:"]
    for _, name, vivc, species, grape_type in batch:
        ctx = []
        if species:
            ctx.append(f"species={species}")
        if vivc:
            ctx.append(f"VIVC={vivc}")
        suffix = f" ({', '.join(ctx)})" if ctx else ""
        lines.append(f"{name}{suffix}")
    return "\n".join(lines)


def parse_response(text, batch):
    """Parse Haiku response into {name: (color, confidence)} dict."""
    results = {}
    name_map = {row[1]: row[0] for row in batch}  # name → id

    for line in text.strip().splitlines():
        line = line.strip()
        if not line or "|" not in line:
            continue
        parts = line.split("|")
        if len(parts) != 3:
            continue
        raw_name = parts[0].strip()
        color = parts[1].strip().lower()
        try:
            confidence = float(parts[2].strip())
        except ValueError:
            continue

        if color not in ("red", "white"):
            continue

        # Match back to a grape ID — try exact name first, then strip context
        grape_id = name_map.get(raw_name)
        if grape_id is None:
            # Strip " (species=..., VIVC=...)" suffix Haiku might echo back
            base = raw_name.split(" (")[0].strip()
            grape_id = name_map.get(base)

        if grape_id:
            results[grape_id] = (color, confidence)

    return results


def main():
    args = sys.argv[1:]
    dry_run = "--dry-run" in args
    limit = None
    if "--limit" in args:
        idx = args.index("--limit")
        limit = int(args[idx + 1])

    try:
        import anthropic
    except ImportError:
        print("ERROR: anthropic package not installed. Run: pip install anthropic")
        sys.exit(1)

    client = anthropic.Anthropic(api_key=get_env("ANTHROPIC_API_KEY"))
    conn = get_conn()

    try:
        with conn.cursor() as cur:
            grapes = fetch_colorless_grapes(cur)

        if limit:
            grapes = grapes[:limit]

        print(f"Found {len(grapes)} grapes with NULL color")
        if dry_run:
            print("DRY RUN — no writes")

        total_filled = 0
        total_skipped = 0
        total_tokens = 0

        batches = [
            grapes[i:i + BATCH_SIZE]
            for i in range(0, len(grapes), BATCH_SIZE)
        ]

        for i, batch in enumerate(batches):
            print(f"\nBatch {i + 1}/{len(batches)} ({len(batch)} grapes)...")
            prompt = build_prompt(batch)

            response = client.messages.create(
                model=HAIKU_MODEL,
                max_tokens=512,
                system=SYSTEM_PROMPT,
                messages=[{"role": "user", "content": prompt}],
            )
            total_tokens += response.usage.input_tokens + response.usage.output_tokens
            text = response.content[0].text
            parsed = parse_response(text, batch)

            writes = [
                (grape_id, color)
                for grape_id, (color, conf) in parsed.items()
                if conf >= CONFIDENCE_THRESHOLD
            ]
            skips = len(batch) - len(writes)

            print(f"  ->{len(writes)} confident fills, {skips} skipped (low confidence or unknown)")

            if writes and not dry_run:
                with conn.cursor() as cur:
                    for grape_id, color in writes:
                        cur.execute(
                            "UPDATE grapes SET color = %s WHERE id = %s AND color IS NULL",
                            (color, grape_id)
                        )
                conn.commit()
                total_filled += len(writes)
            elif writes and dry_run:
                for grape_id, color in writes:
                    name = next(r[1] for r in batch if r[0] == grape_id)
                    conf = parsed[grape_id][1]
                    print(f"    WOULD SET: {name} -> {color} (conf={conf:.2f})")
                total_filled += len(writes)

            total_skipped += skips

        cost = (total_tokens / 1_000_000) * 0.80  # Haiku ~$0.80/1M blended
        print(f"\n{'DRY RUN ' if dry_run else ''}DONE")
        print(f"  Filled:  {total_filled}")
        print(f"  Skipped: {total_skipped} (confidence < {CONFIDENCE_THRESHOLD})")
        print(f"  Tokens:  {total_tokens:,}  (~${cost:.4f})")

    finally:
        conn.close()


if __name__ == "__main__":
    main()
