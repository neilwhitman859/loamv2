#!/usr/bin/env python3
"""Extract grape varieties from wine names using Haiku.

For the ~292K wines with 0 wine_grapes links, uses Haiku to parse grape names
from wine names — especially multi-grape blends, percentages, synonyms, and
ambiguous names that the deterministic grape_from_name.py couldn't handle.

Haiku's returned grape names are matched against our grapes + grape_synonyms
reference tables. Unmatched grapes are logged for potential synonym additions.

Resume-safe: only processes wines with 0 wine_grapes links.

Usage:
    python -m pipeline.enrich.haiku_grape_extract                        # dry-run
    python -m pipeline.enrich.haiku_grape_extract --execute              # apply
    python -m pipeline.enrich.haiku_grape_extract --execute --limit 5000
    python -m pipeline.enrich.haiku_grape_extract --execute --budget 15
"""

import argparse
import json
import sys
import time
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import anthropic

from pipeline.lib.db import get_conn, get_env
from pipeline.lib.models import HAIKU_MODEL
from pipeline.lib.normalize import normalize

# ── Constants ───────────────────────────────────────────────
BATCH_SIZE = 40  # wines per API call — names are short
MAX_TOKENS = 8192  # multi-grape blends produce more output

# Haiku pricing (per million tokens)
INPUT_COST_PER_M = 0.80
OUTPUT_COST_PER_M = 4.00

SYSTEM_PROMPT = """You are a wine grape variety extractor. Given a wine name, extract the grape varieties mentioned or strongly implied. Return ONLY grapes you are confident about — do not guess.

Rules:
- If the name contains explicit grape names (e.g., 'Cabernet Sauvignon'), extract them
- If percentages are given (e.g., '61% Roussanne'), include the percentage
- Appellation names are NOT grapes (e.g., 'Napa Valley', 'Barossa', 'Burgundy', 'Rioja', 'Chianti', 'Champagne' are places, not grapes)
- Producer names are NOT grapes
- Vintage years are NOT grapes
- Label designations are NOT grapes (e.g., 'Reserve', 'Grand Cru', 'Riserva', 'Brut')
- Wine style words are NOT grapes (e.g., 'Blend', 'Cuvee', 'Selection')
- If you cannot identify any grapes, return an empty array
- Use the full grape name (e.g., 'Cabernet Sauvignon' not just 'Cabernet')
- For blends with percentages, include the percentage as an integer (no % sign)
- For blends without percentages, omit the percentage field entirely
- Common synonyms: Shiraz = Syrah, Pinot Grigio = Pinot Gris, Garnacha = Grenache

Respond with ONLY a JSON array. Each element has:
- "idx": the index number from the input (integer)
- "grapes": array of objects with "name" (string) and optionally "percentage" (integer 1-100)

Example:
[
  {"idx": 1, "grapes": [{"name": "Roussanne", "percentage": 61}, {"name": "Grenache Blanc", "percentage": 36}, {"name": "Viognier", "percentage": 3}]},
  {"idx": 2, "grapes": []},
  {"idx": 3, "grapes": [{"name": "Cabernet Sauvignon"}, {"name": "Merlot"}]}
]"""


def build_grape_lookup(conn):
    """Load all grape names + synonyms into a normalized lookup dict.

    Mirrors ReferenceResolver.resolve_grape() tier ordering:
    display_name > VIVC name > synonyms. Primary names always win over synonyms.

    Returns:
        grape_lookup: dict[normalized_name] -> grape_id (UUID string)
        display_names: dict[grape_id] -> display_name
    """
    grape_lookup = {}       # norm_name -> grape_id (primary names, always win)
    grape_by_vivc = {}      # norm_vivc_name -> grape_id
    grape_synonyms = {}     # norm_synonym -> grape_id (only if no primary collision)
    display_names = {}      # grape_id -> display_name

    with conn.cursor() as cur:
        # Primary grape names (display_name + VIVC name)
        cur.execute("""
            SELECT id, name, display_name FROM grapes WHERE deleted_at IS NULL
        """)
        for grape_id, vivc_name, display_name in cur.fetchall():
            gid = str(grape_id)
            if display_name:
                display_names[gid] = display_name
                norm = normalize(display_name)
                if norm:
                    grape_lookup[norm] = gid
            if vivc_name:
                norm_vivc = normalize(vivc_name)
                if norm_vivc:
                    grape_by_vivc[norm_vivc] = gid

        # Synonyms — never overwrite primary names
        cur.execute("""
            SELECT grape_id, synonym FROM grape_synonyms
        """)
        for grape_id, synonym in cur.fetchall():
            gid = str(grape_id)
            norm = normalize(synonym)
            if norm and norm not in grape_lookup and norm not in grape_by_vivc:
                grape_synonyms[norm] = gid

    # Merge: primary > vivc > synonyms
    merged = {}
    merged.update(grape_synonyms)  # lowest priority
    merged.update(grape_by_vivc)   # medium priority
    merged.update(grape_lookup)    # highest priority (display_name)

    return merged, display_names


def resolve_grape(name, grape_lookup):
    """Try to match a Haiku-returned grape name to our reference table.

    Mirrors ReferenceResolver.resolve_grape() logic:
      1. Exact normalized match
      2. Common variations (strip "grape", "the")
      3. Common color suffixes (noir, blanc, tinto, etc.)

    Returns grape_id or None.
    """
    norm = normalize(name)
    if not norm:
        return None

    # 1. Exact match
    if norm in grape_lookup:
        return grape_lookup[norm]

    # 2. Try common variations
    stripped = norm
    if stripped.endswith(" grape"):
        stripped = stripped[:-6].strip()
    if stripped.startswith("the "):
        stripped = stripped[4:].strip()
    if stripped and stripped in grape_lookup:
        return grape_lookup[stripped]

    # 3. Try common color suffixes (matches ReferenceResolver step 5)
    for suffix in [" noir", " blanc", " tinto", " tinta", " blanco"]:
        with_suffix = grape_lookup.get(norm + suffix)
        if with_suffix:
            return with_suffix

    return None


def fetch_wines(conn, limit=None):
    """Fetch wines with 0 wine_grapes links. Simple query to avoid timeouts."""
    limit_clause = f"LIMIT {int(limit)}" if limit else ""

    print("  Fetching wines with no grape links...")
    with conn.cursor() as cur:
        cur.execute("SET statement_timeout = '600000'")  # 10 min for data load
        cur.execute(f"""
            SELECT w.id, w.name, w.wine_type, a.name as appellation, c.name as country
            FROM wines w
            LEFT JOIN appellations a ON a.id = w.appellation_id
            LEFT JOIN countries c ON c.id = w.country_id
            WHERE NOT EXISTS (SELECT 1 FROM wine_grapes wg WHERE wg.wine_id = w.id)
              AND w.name IS NOT NULL AND length(w.name) > 3
              AND w.deleted_at IS NULL
            ORDER BY w.name
            {limit_clause}
        """)
        return cur.fetchall()


def build_prompt(batch):
    """Build the user prompt for a batch of wines."""
    lines = ["Extract grape varieties from these wine names:\n"]
    for i, (wine_id, name, wine_type, appellation, country) in enumerate(batch, 1):
        parts = [f"{i}. {name}"]
        ctx = []
        if appellation:
            ctx.append(f"appellation: {appellation}")
        if country:
            ctx.append(f"country: {country}")
        if wine_type and wine_type != "table":
            ctx.append(f"type: {wine_type}")
        if ctx:
            parts.append(f"   [{'; '.join(ctx)}]")
        lines.append("\n".join(parts))

    return "\n".join(lines)


def parse_response(text, batch):
    """Parse Haiku JSON response into list of (wine_id, grape_name, percentage) tuples.

    Returns:
        results: list of (wine_id, grape_name_raw, percentage_or_None)
    """
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
        print(f"  WARNING: No JSON array in response: {text[:200]}")
        return []

    try:
        items = json.loads(text[start:end])
    except json.JSONDecodeError as e:
        print(f"  WARNING: JSON parse error: {e}")
        print(f"  Response snippet: {text[start:start + 300]}")
        return []

    results = []
    for item in items:
        if not isinstance(item, dict):
            continue
        idx = item.get("idx")
        grapes = item.get("grapes", [])

        # Validate idx
        if not isinstance(idx, int) or idx < 1 or idx > len(batch):
            continue

        wine_id = batch[idx - 1][0]  # idx is 1-based

        if not isinstance(grapes, list):
            continue

        for grape_obj in grapes:
            if not isinstance(grape_obj, dict):
                continue
            gname = grape_obj.get("name", "").strip()
            pct = grape_obj.get("percentage")

            if not gname:
                continue

            # Validate percentage
            if pct is not None:
                if isinstance(pct, (int, float)) and 1 <= pct <= 100:
                    pct = int(pct)
                else:
                    pct = None

            results.append((wine_id, gname, pct))

    return results


def compute_cost(input_tokens, output_tokens):
    """Compute cost in USD from token counts."""
    return (input_tokens * INPUT_COST_PER_M + output_tokens * OUTPUT_COST_PER_M) / 1_000_000


def main():
    parser = argparse.ArgumentParser(
        description="Haiku-powered grape variety extraction from wine names"
    )
    parser.add_argument(
        "--execute", action="store_true",
        help="Apply changes (default is dry-run)"
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Preview without writing (default behavior)"
    )
    parser.add_argument(
        "--limit", type=int, default=None,
        help="Max wines to process"
    )
    parser.add_argument(
        "--budget", type=float, default=15.0,
        help="Max spend in USD (default $15.00)"
    )
    args = parser.parse_args()

    if not args.execute and not args.dry_run:
        args.dry_run = True

    api_key = get_env("ANTHROPIC_API_KEY")
    client = anthropic.Anthropic(api_key=api_key)
    conn = get_conn()

    try:
        # ── Load grape reference ────────────────────────────
        print("Loading grape reference table...")
        grape_lookup, display_names = build_grape_lookup(conn)
        print(f"  {len(grape_lookup):,} normalized grape names/synonyms loaded")
        print(f"  {len(display_names):,} distinct grapes in DB")

        # ── Fetch wines ─────────────────────────────────────
        print("\nFetching wines with 0 grape links...")
        wines = fetch_wines(conn, limit=args.limit)
        total_wines = len(wines)
        print(f"  {total_wines:,} wines to process")

        if total_wines == 0:
            print("No wines need grape extraction.")
            return

        mode_label = "DRY RUN" if not args.execute else "EXECUTE"
        print(f"\nMode: {mode_label}")
        print(f"Budget: ${args.budget:.2f}")
        print(f"Batch size: {BATCH_SIZE}")
        estimated_batches = (total_wines + BATCH_SIZE - 1) // BATCH_SIZE
        print(f"Estimated batches: {estimated_batches:,}")
        print()

        # ── Process in batches ──────────────────────────────
        cumulative_cost = 0.0
        total_input_tokens = 0
        total_output_tokens = 0
        total_links_created = 0
        total_links_skipped_unmatched = 0
        total_wines_with_grapes = 0
        total_wines_empty = 0
        total_api_errors = 0
        unmatched_grapes = defaultdict(int)  # raw_name -> count

        batches = [
            wines[i:i + BATCH_SIZE]
            for i in range(0, total_wines, BATCH_SIZE)
        ]

        for batch_idx, batch in enumerate(batches):
            batch_num = batch_idx + 1

            # Budget check
            if cumulative_cost >= args.budget:
                print(f"\n  BUDGET EXCEEDED (${cumulative_cost:.4f} >= ${args.budget:.2f}). Stopping.")
                break

            prompt = build_prompt(batch)

            try:
                response = client.messages.create(
                    model=HAIKU_MODEL,
                    max_tokens=MAX_TOKENS,
                    system=SYSTEM_PROMPT,
                    messages=[{"role": "user", "content": prompt}],
                )
            except anthropic.RateLimitError:
                print(f"  Batch {batch_num}/{len(batches)}: RATE LIMITED. Sleeping 30s...")
                time.sleep(30)
                try:
                    response = client.messages.create(
                        model=HAIKU_MODEL,
                        max_tokens=MAX_TOKENS,
                        system=SYSTEM_PROMPT,
                        messages=[{"role": "user", "content": prompt}],
                    )
                except Exception as e:
                    print(f"  Batch {batch_num}/{len(batches)}: RETRY FAILED: {e}")
                    total_api_errors += 1
                    continue
            except Exception as e:
                print(f"  Batch {batch_num}/{len(batches)}: API ERROR: {e}")
                total_api_errors += 1
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
            raw_results = parse_response(text, batch)

            # Resolve grape names against our reference table
            batch_links = []  # (wine_id, grape_id, percentage, display_name)
            batch_unmatched = 0
            wine_ids_with_grapes = set()

            for wine_id, grape_name_raw, pct in raw_results:
                grape_id = resolve_grape(grape_name_raw, grape_lookup)
                if grape_id:
                    batch_links.append((str(wine_id), grape_id, pct, display_names.get(grape_id, "?")))
                    wine_ids_with_grapes.add(wine_id)
                else:
                    unmatched_grapes[grape_name_raw] += 1
                    batch_unmatched += 1

            # Count wines with empty grape response
            responded_wine_ids = set()
            for wine_id, _, _ in raw_results:
                responded_wine_ids.add(wine_id)
            batch_empty = len(batch) - len(wine_ids_with_grapes)
            total_wines_empty += max(0, batch_empty - batch_unmatched)
            total_wines_with_grapes += len(wine_ids_with_grapes)

            # Deduplicate (wine_id, grape_id) within batch — keep first (highest percentage)
            seen = set()
            unique_links = []
            for wine_id, grape_id, pct, display in batch_links:
                key = (wine_id, grape_id)
                if key not in seen:
                    seen.add(key)
                    unique_links.append((wine_id, grape_id, pct, display))

            # Write to DB
            batch_inserted = 0
            if unique_links and args.execute:
                with conn.cursor() as cur:
                    for wine_id, grape_id, pct, display in unique_links:
                        try:
                            cur.execute(
                                """INSERT INTO wine_grapes (wine_id, grape_id, percentage)
                                   VALUES (%s, %s, %s)
                                   ON CONFLICT (wine_id, grape_id) DO NOTHING""",
                                (wine_id, grape_id, pct)
                            )
                            batch_inserted += cur.rowcount
                        except Exception as e:
                            conn.rollback()
                            print(f"    INSERT ERROR for wine {wine_id}: {e}")
                            break
                    else:
                        conn.commit()

            total_links_created += batch_inserted if args.execute else len(unique_links)
            total_links_skipped_unmatched += batch_unmatched

            # Progress
            pct_done = batch_num / len(batches) * 100
            print(
                f"  Batch {batch_num}/{len(batches)} ({pct_done:5.1f}%): "
                f"+{len(unique_links)} links ({len(wine_ids_with_grapes)} wines), "
                f"{batch_unmatched} unmatched | "
                f"cost ${cumulative_cost:.4f}"
            )

            # Show samples in first few batches
            if batch_num <= 3:
                shown = 0
                for wine_id, grape_id, pct, display in unique_links[:5]:
                    wine_name = next(
                        (r[1] for r in batch if str(r[0]) == wine_id), "?"
                    )
                    pct_str = f" ({pct}%)" if pct else ""
                    action = "WOULD LINK" if not args.execute else "LINKED"
                    print(f"    {action}: {wine_name[:55]} -> {display}{pct_str}")
                    shown += 1
                if shown < len(unique_links):
                    print(f"    ... and {len(unique_links) - shown} more")

            # Brief pause to avoid rate limits
            if batch_idx + 1 < len(batches):
                time.sleep(0.3)

        # ── Summary ─────────────────────────────────────────
        print(f"\n{'=' * 65}")
        print(f"  GRAPE EXTRACTION SUMMARY {'(DRY RUN)' if not args.execute else ''}")
        print(f"{'=' * 65}")
        wines_processed = min(total_wines, (batch_idx + 1) * BATCH_SIZE) if batches else 0
        print(f"  Wines processed:           {wines_processed:,} / {total_wines:,}")
        print(f"  Wines with grapes found:   {total_wines_with_grapes:,}")
        print(f"  Wine_grapes links created: {total_links_created:,}")
        print(f"  Unmatched grape names:     {total_links_skipped_unmatched:,}")
        print(f"  API errors:                {total_api_errors}")
        print(f"  Input tokens:              {total_input_tokens:,}")
        print(f"  Output tokens:             {total_output_tokens:,}")
        print(f"  Total cost:                ${cumulative_cost:.4f}")

        # Show unmatched grapes sorted by frequency
        if unmatched_grapes:
            print(f"\n  Unmatched grape names (top 40 by frequency):")
            print(f"  {'Grape Name':<40} {'Count':>6}")
            print(f"  {'-' * 40} {'-' * 6}")
            for gname, count in sorted(unmatched_grapes.items(), key=lambda x: -x[1])[:40]:
                print(f"  {gname:<40} {count:>6}")
            total_unmatched_unique = len(unmatched_grapes)
            if total_unmatched_unique > 40:
                remaining = total_unmatched_unique - 40
                print(f"  ... and {remaining} more unique names")
            print(f"\n  These could be candidates for grape_synonyms additions.")

        if not args.execute and total_links_created > 0:
            print(f"\n  Re-run with --execute to create {total_links_created:,} wine_grapes links.")

    finally:
        conn.close()


if __name__ == "__main__":
    main()
