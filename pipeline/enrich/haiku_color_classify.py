#!/usr/bin/env python3
"""Classify wine color (red/white/rose) using Haiku.

Two priority tiers:
  1. TTB prefix 80 wines — known "not white", need red vs rose classification
  2. All other colorless wines — full red/white/rose classification

Uses wine name, grape varieties, appellation, and wine_type as context.
Only writes at confidence >= 0.85. Resume-safe (skips wines with color set).

Usage:
    python -m pipeline.enrich.haiku_color_classify                     # dry-run
    python -m pipeline.enrich.haiku_color_classify --execute            # apply
    python -m pipeline.enrich.haiku_color_classify --execute --limit 500
    python -m pipeline.enrich.haiku_color_classify --execute --budget 5.0
    python -m pipeline.enrich.haiku_color_classify --execute --priority 1  # TTB 80 only
    python -m pipeline.enrich.haiku_color_classify --execute --priority 2  # non-TTB only
"""

import argparse
import json
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import anthropic

from pipeline.lib.db import get_conn, get_env

# ── Constants ───────────────────────────────────────────────
HAIKU_MODEL = "claude-haiku-4-5-20251001"
CONFIDENCE_THRESHOLD = 0.85
BATCH_SIZE = 35  # wines per API call — balances cost vs latency
MAX_TOKENS = 4096

# Haiku pricing (per million tokens)
INPUT_COST_PER_M = 0.80
OUTPUT_COST_PER_M = 4.00

SYSTEM_PROMPT = """You are a wine color classifier. For each wine, determine if it is red, white, or rosé based on the wine name, grape varieties, appellation, and wine type.

Rules:
- Use grape variety as strongest signal (e.g. Cabernet Sauvignon = red, Chardonnay = white, unless name says otherwise)
- Rosé/Rose/Rosato/Rosado in the name → rose
- Blanc/Bianco/Blanco/White in the name → white
- Appellation context matters (e.g. Sancerre is usually white, Barolo is always red)
- "Blanc de Noirs" = white (despite dark grapes)
- "Blanc de Blancs" = white
- Port/Tawny Port/Ruby Port = red (unless White Port)
- Champagne/Prosecco/Cava without color indicator → white
- If truly ambiguous, set confidence below 0.85 and we will skip it

Respond with ONLY a JSON array. Each element must have exactly these keys:
- "idx": the index number from the input list (integer)
- "color": exactly "red", "white", or "rose" (no accent on rose)
- "confidence": a decimal 0.0-1.0

Example response:
[{"idx": 1, "color": "red", "confidence": 0.95}, {"idx": 2, "color": "white", "confidence": 0.90}]
"""


def fetch_colorless_wines(conn, limit=None):
    """Fetch all colorless wines with context.

    Two-step: get IDs (fast), then fetch context in small batches.
    """
    # Step 1: fast ID scan — skip junk, prioritize wines with context
    print("  Fetching colorless wine IDs...")
    with conn.cursor() as cur:
        cur.execute("SET statement_timeout = '600000'")  # 10 min for data load
        limit_clause = f"LIMIT {int(limit)}" if limit else ""
        cur.execute(f"""
            SELECT id, name, wine_type, appellation_id
            FROM wines
            WHERE color IS NULL AND name IS NOT NULL
              AND length(name) > 3
              AND wine_type IN ('table', 'sparkling', 'fortified')
            ORDER BY
              (appellation_id IS NOT NULL) DESC,
              name
            {limit_clause}
        """)
        base_rows = cur.fetchall()
    print(f"  Found {len(base_rows):,} colorless wines")

    if not base_rows:
        return []

    # Step 2: batch-fetch grape names (the only expensive join)
    print("  Fetching grape context...")
    wine_grapes = {}
    ids = [str(r[0]) for r in base_rows]
    chunk_size = 3000
    for i in range(0, len(ids), chunk_size):
        chunk = ids[i:i + chunk_size]
        placeholders = ",".join(["%s"] * len(chunk))
        with conn.cursor() as cur:
            cur.execute(f"""
                SELECT wg.wine_id, string_agg(DISTINCT g.name, ', ')
                FROM wine_grapes wg
                JOIN grapes g ON g.id = wg.grape_id
                WHERE wg.wine_id IN ({placeholders})
                GROUP BY wg.wine_id
            """, chunk)
            for wid, grapes in cur.fetchall():
                wine_grapes[str(wid)] = grapes

    # Step 3: batch-fetch appellation names
    app_ids = list({str(r[3]) for r in base_rows if r[3]})
    app_names = {}
    if app_ids:
        for i in range(0, len(app_ids), 3000):
            chunk = app_ids[i:i + 3000]
            placeholders = ",".join(["%s"] * len(chunk))
            with conn.cursor() as cur:
                cur.execute(f"SELECT id, name FROM appellations WHERE id IN ({placeholders})", chunk)
                for aid, aname in cur.fetchall():
                    app_names[str(aid)] = aname

    # Combine
    results = []
    for wid, name, wine_type, app_id in base_rows:
        grapes = wine_grapes.get(str(wid))
        appellation = app_names.get(str(app_id)) if app_id else None
        results.append((wid, name, wine_type, grapes, appellation))

    print(f"  Ready: {len(results):,} wines with context")
    return results


def fetch_priority_2(conn, limit=None):
    """All other colorless wines — no TTB prefix 80 link."""
    sql = """
        SELECT w.id, w.name, w.wine_type,
               string_agg(DISTINCT g.name, ', ') as grapes,
               a.name as appellation
        FROM wines w
        LEFT JOIN wine_grapes wg ON wg.wine_id = w.id
        LEFT JOIN grapes g ON g.id = wg.grape_id
        LEFT JOIN appellations a ON a.id = w.appellation_id
        WHERE w.color IS NULL
          AND NOT EXISTS (
              SELECT 1 FROM source_ttb_colas s
              WHERE s.canonical_wine_id = w.id
                AND s.class_type IS NOT NULL
                AND LEFT(s.class_type, 2) = '80'
          )
        GROUP BY w.id, w.name, w.wine_type, a.name
        ORDER BY w.name
    """
    if limit:
        sql += f" LIMIT {int(limit)}"
    with conn.cursor() as cur:
        cur.execute(sql)
        return cur.fetchall()


def build_prompt(batch, is_priority_1=False):
    """Build the user prompt for a batch of wines."""
    context = ""

    lines = [f"Classify the color of these wines:{context}"]
    for i, (wine_id, name, wine_type, grapes, appellation) in enumerate(batch, 1):
        parts = [f"{i}. {name}"]
        ctx = []
        if grapes:
            ctx.append(f"grapes: {grapes}")
        if appellation:
            ctx.append(f"appellation: {appellation}")
        if wine_type and wine_type != "table":
            ctx.append(f"type: {wine_type}")
        if ctx:
            parts.append(f"   [{'; '.join(ctx)}]")
        lines.append("\n".join(parts))

    return "\n".join(lines)


def parse_response(text, batch):
    """Parse Haiku JSON response into {wine_id: (color, confidence)} dict."""
    # Strip markdown code blocks if present
    text = text.strip()
    if text.startswith("```"):
        text = text.split("\n", 1)[1]
        if text.endswith("```"):
            text = text[:-3]
        text = text.strip()

    # Find JSON array
    start = text.find("[")
    end = text.rfind("]") + 1
    if start < 0 or end <= start:
        print(f"  WARNING: No JSON array found in response: {text[:200]}")
        return {}

    try:
        items = json.loads(text[start:end])
    except json.JSONDecodeError as e:
        print(f"  WARNING: JSON parse error: {e}")
        print(f"  Response snippet: {text[start:start+200]}")
        return {}

    results = {}
    for item in items:
        if not isinstance(item, dict):
            continue
        idx = item.get("idx")
        color = item.get("color", "").lower().strip()
        confidence = item.get("confidence", 0)

        # Validate
        if not isinstance(idx, int) or idx < 1 or idx > len(batch):
            continue
        if color not in ("red", "white", "rose"):
            continue
        if not isinstance(confidence, (int, float)):
            continue

        wine_id = batch[idx - 1][0]  # idx is 1-based
        results[wine_id] = (color, float(confidence))

    return results


def compute_cost(input_tokens, output_tokens):
    """Compute cost in USD from token counts."""
    input_cost = input_tokens * INPUT_COST_PER_M / 1_000_000
    output_cost = output_tokens * OUTPUT_COST_PER_M / 1_000_000
    return input_cost + output_cost


def main():
    parser = argparse.ArgumentParser(
        description="Haiku-powered wine color classification"
    )
    parser.add_argument(
        "--execute", action="store_true",
        help="Apply changes (default is dry-run)"
    )
    parser.add_argument(
        "--limit", type=int, default=None,
        help="Max wines to process (across both priorities)"
    )
    parser.add_argument(
        "--budget", type=float, default=10.0,
        help="Max spend in USD (default $10.00)"
    )
    parser.add_argument(
        "--priority", type=int, choices=[1, 2], default=None,
        help="Run only priority 1 (TTB-80) or priority 2 (other). Default: both."
    )
    args = parser.parse_args()

    api_key = get_env("ANTHROPIC_API_KEY")
    client = anthropic.Anthropic(api_key=api_key)
    conn = get_conn()

    try:
        # ── Fetch wines ──────────────────────────────────────
        wines_p1 = []
        wines_p2 = []
        remaining_limit = args.limit

        if args.priority in (None, 1):
            wines_p1 = fetch_colorless_wines(conn, limit=remaining_limit)
            print(f"Priority 1 (all colorless wines): {len(wines_p1)} wines")
            if remaining_limit:
                remaining_limit -= len(wines_p1)
                if remaining_limit <= 0:
                    remaining_limit = 0

        if args.priority in (None, 2) and (remaining_limit is None or remaining_limit > 0):
            wines_p2 = fetch_priority_2(conn, limit=remaining_limit)
            print(f"Priority 2 (other colorless):          {len(wines_p2)} wines")

        total_wines = len(wines_p1) + len(wines_p2)
        if total_wines == 0:
            print("No wines to classify. All wines have color set.")
            return

        if not args.execute:
            print("DRY RUN -- no writes (use --execute to apply)")
        print(f"Budget: ${args.budget:.2f}")
        print(f"Confidence threshold: {CONFIDENCE_THRESHOLD}")
        print()

        # ── Process ──────────────────────────────────────────
        cumulative_cost = 0.0
        total_input_tokens = 0
        total_output_tokens = 0
        total_classified = 0
        total_skipped_low_conf = 0
        total_errors = 0
        budget_exceeded = False

        for priority_label, wines, is_p1 in [
            ("Priority 1 (TTB-80)", wines_p1, True),
            ("Priority 2 (other)", wines_p2, False),
        ]:
            if not wines:
                continue
            print(f"{'=' * 60}")
            print(f"  {priority_label}: {len(wines)} wines")
            print(f"{'=' * 60}")

            batches = [
                wines[i:i + BATCH_SIZE]
                for i in range(0, len(wines), BATCH_SIZE)
            ]

            for batch_idx, batch in enumerate(batches):
                batch_num = batch_idx + 1

                # Budget check at start of each batch
                if cumulative_cost >= args.budget:
                    print(f"\n  BUDGET EXCEEDED (${cumulative_cost:.4f} >= ${args.budget:.2f}). Stopping.")
                    budget_exceeded = True
                    break

                prompt = build_prompt(batch, is_priority_1=is_p1)

                try:
                    response = client.messages.create(
                        model=HAIKU_MODEL,
                        max_tokens=MAX_TOKENS,
                        system=SYSTEM_PROMPT,
                        messages=[{"role": "user", "content": prompt}],
                    )
                except Exception as e:
                    print(f"  Batch {batch_num}/{len(batches)}: API ERROR: {e}")
                    total_errors += 1
                    time.sleep(2)
                    continue

                # Track tokens and cost
                inp_tok = response.usage.input_tokens
                out_tok = response.usage.output_tokens
                total_input_tokens += inp_tok
                total_output_tokens += out_tok
                batch_cost = compute_cost(inp_tok, out_tok)
                cumulative_cost += batch_cost

                # Parse response
                text = response.content[0].text
                parsed = parse_response(text, batch)

                # Filter by confidence
                writes = []
                low_conf = 0
                for wine_id, (color, conf) in parsed.items():
                    if conf >= CONFIDENCE_THRESHOLD:
                        writes.append((wine_id, color, conf))
                    else:
                        low_conf += 1

                unparsed = len(batch) - len(parsed)
                total_skipped_low_conf += low_conf

                # Apply writes
                if writes and args.execute:
                    with conn.cursor() as cur:
                        for wine_id, color, conf in writes:
                            cur.execute(
                                "UPDATE wines SET color = %s WHERE id = %s AND color IS NULL",
                                (color, str(wine_id))
                            )
                    conn.commit()

                total_classified += len(writes)

                # Progress output
                color_dist = {}
                for _, color, _ in writes:
                    color_dist[color] = color_dist.get(color, 0) + 1
                dist_str = ", ".join(f"{c}={n}" for c, n in sorted(color_dist.items()))
                print(
                    f"  Batch {batch_num}/{len(batches)}: "
                    f"{len(writes)}/{len(batch)} classified ({dist_str}), "
                    f"{low_conf} low-conf, {unparsed} unparsed | "
                    f"cost so far ${cumulative_cost:.4f}"
                )

                # Show sample in dry-run mode
                if not args.execute and writes and batch_num <= 3:
                    for wine_id, color, conf in writes[:5]:
                        name = next(
                            r[1] for r in batch if r[0] == wine_id
                        )
                        print(f"    WOULD SET: {name} -> {color} (conf={conf:.2f})")

                # Brief pause to avoid rate limits
                if batch_idx + 1 < len(batches):
                    time.sleep(0.3)

            if budget_exceeded:
                break

        # ── Summary ──────────────────────────────────────────
        print(f"\n{'=' * 60}")
        print(f"  SUMMARY {'(DRY RUN)' if not args.execute else ''}")
        print(f"{'=' * 60}")
        print(f"  Total wines processed:     {total_wines}")
        print(f"  Classified (>={CONFIDENCE_THRESHOLD}): {total_classified}")
        print(f"  Skipped (low confidence):  {total_skipped_low_conf}")
        print(f"  API errors:                {total_errors}")
        print(f"  Input tokens:              {total_input_tokens:,}")
        print(f"  Output tokens:             {total_output_tokens:,}")
        print(f"  Total cost:                ${cumulative_cost:.4f}")
        if budget_exceeded:
            print(f"  ** Stopped early due to budget limit **")
        if not args.execute and total_classified > 0:
            print(f"\n  Re-run with --execute to apply {total_classified} color updates.")

    finally:
        conn.close()


if __name__ == "__main__":
    main()
