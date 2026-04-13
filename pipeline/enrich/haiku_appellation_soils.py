#!/usr/bin/env python3
"""Extract soil types from appellation rules text using Haiku.

Reads appellation_rules (rules JSONB, notes, source_text_excerpt) and identifies
which of the 39 canonical soil_types are mentioned or clearly described. Populates
the appellation_soils junction table.

The text may be in English, French, Italian, Spanish, Portuguese, or German.
Pure extraction — no inference, only soils explicitly mentioned or clearly described.

Usage:
    python -m pipeline.enrich.haiku_appellation_soils                     # dry-run
    python -m pipeline.enrich.haiku_appellation_soils --execute            # apply
    python -m pipeline.enrich.haiku_appellation_soils --execute --limit 50
    python -m pipeline.enrich.haiku_appellation_soils --execute --budget 0.50
"""

import argparse
import json
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import anthropic

from pipeline.lib.db import get_conn, get_env
from pipeline.lib.models import HAIKU_MODEL

# -- Constants ---------------------------------------------------------------
BATCH_SIZE = 10  # appellations per API call (rules text can be long)
MAX_TOKENS = 4096
TEXT_TRUNCATE = 2000  # max chars of combined text per appellation

# Haiku pricing (per million tokens)
INPUT_COST_PER_M = 0.80
OUTPUT_COST_PER_M = 4.00

SYSTEM_PROMPT = """You are a wine terroir expert. Given appellation regulation text, identify which soil types are present in the appellation's terroir.

The text may be in English, French, Italian, Spanish, Portuguese, or German. Extract soil types that are explicitly mentioned or clearly described in the text.

You MUST match to ONLY the following 39 soil type names. Do not invent new types or use synonyms — map everything to the closest match from this list:

Alluvial, Basalt, Calcaire, Calcareous, Chalk, Clay, Flint, Flysch, Galestro, Galets Roulés, Glacial, Gneiss, Granite, Gravel, Iron-Rich, Ite, Kimmeridgian, Limestone, Loam, Loess, Marl, Mica Schist, Molasse, Obsidian, Porphyry, Pumice, Quartzite, Sand, Sandstone, Schist, Shale, Silex, Silt, Slate, Terra Rossa, Tufa, Tuff, Tuffeau, Volcanic

Mapping guidance for non-English terms:
- "argile" / "argilla" / "arcilla" / "Ton" = Clay
- "calcaire" / "calcare" / "caliza" / "Kalk" = Calcareous (or Calcaire if specifically French limestone)
- "craie" / "gesso" / "creta" / "Kreide" = Chalk
- "sable" / "sabbia" / "arena" / "Sand" = Sand
- "grès" / "arenaria" / "arenisca" / "Sandstein" = Sandstone
- "schiste" / "scisto" / "pizarra" / "Schiefer" = Schist (or Slate if thin-layered)
- "marne" / "marna" / "Mergel" = Marl
- "granit" / "granito" = Granite
- "limon" / "limo" / "Schluff" = Silt
- "galets roulés" = Galets Roulés (Rhône river stones)
- "silex" = Silex (flint nodules)
- "tuffeau" / "tufo" / "toba" = Tuffeau or Tuff or Tufa (context-dependent)
- "volcanico" / "volcanique" / "vulkanisch" = Volcanic
- "loess" / "Löss" = Loess
- "terra rossa" = Terra Rossa
- "kimméridgien" / "kimmeridgiano" = Kimmeridgian
- "alluvionnaire" / "alluvionale" / "aluvial" = Alluvial

If the text mentions no soil types at all, return an empty soils array.
Be conservative — only extract soils that are clearly referenced, not implied.

Respond with ONLY a JSON array. Each element must have:
- "appellation": the appellation name (for alignment)
- "soils": array of soil type names from the 39-name list (or empty array if none found)

Example response:
[{"appellation": "Chablis", "soils": ["Kimmeridgian", "Limestone", "Clay"]}, {"appellation": "Mosel", "soils": []}]
"""


def fetch_appellations(conn, limit=None):
    """Fetch appellation rules that don't yet have soil entries."""
    sql = """
        SELECT ar.appellation_id, a.name AS appellation_name,
               ar.rules::text AS rules_text, ar.notes, ar.source_text_excerpt
        FROM appellation_rules ar
        JOIN appellations a ON a.id = ar.appellation_id
        WHERE NOT EXISTS (
            SELECT 1 FROM appellation_soils asol
            WHERE asol.appellation_id = ar.appellation_id
        )
        ORDER BY a.name
    """
    if limit:
        sql += f" LIMIT {int(limit)}"
    with conn.cursor() as cur:
        cur.execute(sql)
        return cur.fetchall()


def fetch_soil_types(conn):
    """Load soil_types into a name -> id lookup dict."""
    with conn.cursor() as cur:
        cur.execute("SELECT id, name FROM soil_types")
        rows = cur.fetchall()
    # Build case-insensitive lookup
    lookup = {}
    for soil_id, name in rows:
        lookup[name.lower()] = (soil_id, name)
    return lookup


def truncate_text(rules_text, notes, source_text_excerpt, max_chars=TEXT_TRUNCATE):
    """Combine and truncate appellation text fields to fit in prompt."""
    parts = []
    if rules_text and rules_text.strip() and rules_text.strip() != "null":
        parts.append(rules_text.strip())
    if notes and notes.strip():
        parts.append(f"Notes: {notes.strip()}")
    if source_text_excerpt and source_text_excerpt.strip():
        parts.append(f"Source excerpt: {source_text_excerpt.strip()}")

    combined = "\n".join(parts)
    if len(combined) > max_chars:
        combined = combined[:max_chars] + "..."
    return combined


def build_prompt(batch):
    """Build the user prompt for a batch of appellations."""
    lines = ["Identify the soil types mentioned in these appellation regulation texts:\n"]
    for i, (app_id, name, rules_text, notes, excerpt) in enumerate(batch, 1):
        text = truncate_text(rules_text, notes, excerpt)
        if not text:
            text = "(no text available)"
        lines.append(f"---\n{i}. Appellation: {name}\nText: {text}\n")
    return "\n".join(lines)


def parse_response(text, batch, soil_lookup):
    """Parse Haiku JSON response into list of (appellation_id, soil_type_id) pairs."""
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
        return [], 0

    try:
        items = json.loads(text[start:end])
    except json.JSONDecodeError as e:
        print(f"  WARNING: JSON parse error: {e}")
        print(f"  Response snippet: {text[start:start + 200]}")
        return [], 0

    # Build a name -> batch entry lookup for alignment
    batch_by_name = {}
    for entry in batch:
        batch_by_name[entry[1].lower()] = entry  # name is index 1

    pairs = []
    unmatched_soils = 0
    for item in items:
        if not isinstance(item, dict):
            continue
        app_name = item.get("appellation", "").strip()
        soils = item.get("soils", [])

        if not isinstance(soils, list):
            continue

        # Find the matching batch entry
        batch_entry = batch_by_name.get(app_name.lower())
        if not batch_entry:
            # Try fuzzy: check if any batch name starts with or contains the response name
            for bname, bentry in batch_by_name.items():
                if app_name.lower() in bname or bname in app_name.lower():
                    batch_entry = bentry
                    break
        if not batch_entry:
            print(f"  WARNING: Could not align appellation '{app_name}' to batch")
            continue

        app_id = batch_entry[0]

        for soil_name in soils:
            if not isinstance(soil_name, str):
                continue
            soil_key = soil_name.strip().lower()
            if soil_key in soil_lookup:
                soil_id, canonical_name = soil_lookup[soil_key]
                pairs.append((app_id, soil_id, canonical_name))
            else:
                unmatched_soils += 1
                print(f"    WARNING: Unknown soil type '{soil_name}' for {app_name} — skipped")

    return pairs, unmatched_soils


def compute_cost(input_tokens, output_tokens):
    """Compute cost in USD from token counts."""
    input_cost = input_tokens * INPUT_COST_PER_M / 1_000_000
    output_cost = output_tokens * OUTPUT_COST_PER_M / 1_000_000
    return input_cost + output_cost


def main():
    parser = argparse.ArgumentParser(
        description="Haiku-powered soil type extraction from appellation rules"
    )
    parser.add_argument(
        "--execute", action="store_true",
        help="Apply changes (default is dry-run)"
    )
    parser.add_argument(
        "--limit", type=int, default=None,
        help="Max appellations to process"
    )
    parser.add_argument(
        "--budget", type=float, default=1.0,
        help="Max spend in USD (default $1.00)"
    )
    args = parser.parse_args()

    api_key = get_env("ANTHROPIC_API_KEY")
    client = anthropic.Anthropic(api_key=api_key)
    conn = get_conn()

    try:
        # -- Load soil types lookup ----------------------------------------
        soil_lookup = fetch_soil_types(conn)
        print(f"Loaded {len(soil_lookup)} soil types from reference table")

        # -- Fetch appellations needing soils ------------------------------
        appellations = fetch_appellations(conn, limit=args.limit)
        total_appellations = len(appellations)
        print(f"Appellations to process: {total_appellations}")

        if total_appellations == 0:
            print("No appellations need soil extraction. All already have entries.")
            return

        if not args.execute:
            print("DRY RUN -- no writes (use --execute to apply)")
        print(f"Budget: ${args.budget:.2f}")
        print(f"Batch size: {BATCH_SIZE} appellations per API call")
        print()

        # -- Process in batches --------------------------------------------
        batches = [
            appellations[i:i + BATCH_SIZE]
            for i in range(0, len(appellations), BATCH_SIZE)
        ]

        cumulative_cost = 0.0
        total_input_tokens = 0
        total_output_tokens = 0
        total_links_created = 0
        total_appellations_with_soils = 0
        total_appellations_no_soils = 0
        total_unmatched = 0
        total_errors = 0
        budget_exceeded = False

        for batch_idx, batch in enumerate(batches):
            batch_num = batch_idx + 1

            # Budget check
            if cumulative_cost >= args.budget:
                print(f"\nBUDGET EXCEEDED (${cumulative_cost:.4f} >= ${args.budget:.2f}). Stopping.")
                budget_exceeded = True
                break

            prompt = build_prompt(batch)

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
            pairs, unmatched = parse_response(text, batch, soil_lookup)
            total_unmatched += unmatched

            # Count unique appellations with soil links in this batch
            app_ids_with_soils = set(p[0] for p in pairs)
            batch_with_soils = len(app_ids_with_soils)
            batch_no_soils = len(batch) - batch_with_soils
            total_appellations_with_soils += batch_with_soils
            total_appellations_no_soils += batch_no_soils

            # Apply writes
            if pairs and args.execute:
                with conn.cursor() as cur:
                    for app_id, soil_id, _ in pairs:
                        cur.execute(
                            """INSERT INTO appellation_soils (appellation_id, soil_type_id)
                               VALUES (%s, %s)
                               ON CONFLICT DO NOTHING""",
                            (str(app_id), str(soil_id))
                        )
                conn.commit()

            total_links_created += len(pairs)

            # Soil distribution for this batch
            soil_counts = {}
            for _, _, soil_name in pairs:
                soil_counts[soil_name] = soil_counts.get(soil_name, 0) + 1
            top_soils = sorted(soil_counts.items(), key=lambda x: -x[1])[:5]
            top_str = ", ".join(f"{s}={n}" for s, n in top_soils) if top_soils else "none"

            print(
                f"  Batch {batch_num}/{len(batches)}: "
                f"{batch_with_soils}/{len(batch)} appellations with soils, "
                f"{len(pairs)} links (top: {top_str}) | "
                f"cost ${cumulative_cost:.4f}"
            )

            # Show samples in dry-run
            if not args.execute and pairs and batch_num <= 3:
                shown_apps = set()
                for app_id, soil_id, soil_name in pairs:
                    if app_id in shown_apps:
                        continue
                    shown_apps.add(app_id)
                    app_name = next(b[1] for b in batch if b[0] == app_id)
                    app_soils = [s for a, _, s in pairs if a == app_id]
                    print(f"    WOULD INSERT: {app_name} -> {', '.join(app_soils)}")
                    if len(shown_apps) >= 5:
                        break

            # Brief pause to avoid rate limits
            if batch_idx + 1 < len(batches):
                time.sleep(0.3)

        # -- Summary -------------------------------------------------------
        print(f"\n{'=' * 60}")
        print(f"  SUMMARY {'(DRY RUN)' if not args.execute else ''}")
        print(f"{'=' * 60}")
        print(f"  Appellations processed:     {total_appellations_with_soils + total_appellations_no_soils}")
        print(f"  With soil matches:          {total_appellations_with_soils}")
        print(f"  Without soil mentions:      {total_appellations_no_soils}")
        print(f"  Total soil links created:   {total_links_created}")
        print(f"  Unmatched soil names:       {total_unmatched}")
        print(f"  API errors:                 {total_errors}")
        print(f"  Input tokens:               {total_input_tokens:,}")
        print(f"  Output tokens:              {total_output_tokens:,}")
        print(f"  Total cost:                 ${cumulative_cost:.4f}")
        if budget_exceeded:
            print(f"  ** Stopped early due to budget limit **")
        if not args.execute and total_links_created > 0:
            print(f"\n  Re-run with --execute to apply {total_links_created} soil links.")

    finally:
        conn.close()


if __name__ == "__main__":
    main()
