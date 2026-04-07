#!/usr/bin/env python3
"""Re-classify 'flagged' (unclear) duplicate wine pairs in match_decisions using Haiku.

The original wine_dupe_classify.py run marked 2,682 pairs as 'unclear' with minimal
context. This script re-visits them with richer context — grapes, appellation, region,
external IDs, and vintages — to get a clearer classification.

Only updates rows at confidence >= 0.80. Resume-safe (skips rows where status != 'flagged').

Usage:
    python -m pipeline.enrich.haiku_dupe_reclassify                     # dry-run
    python -m pipeline.enrich.haiku_dupe_reclassify --execute            # apply
    python -m pipeline.enrich.haiku_dupe_reclassify --execute --limit 100
    python -m pipeline.enrich.haiku_dupe_reclassify --execute --budget 1.5
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
CONFIDENCE_THRESHOLD = 0.80
BATCH_SIZE = 15  # pairs per API call
MAX_TOKENS = 4096

# Haiku pricing (per million tokens)
INPUT_COST_PER_M = 0.80
OUTPUT_COST_PER_M = 4.00

SYSTEM_PROMPT = """You are a wine duplicate detection expert. For each pair of wine records, classify whether they are:

- true_duplicate: same wine, should merge (different vintages, spelling variations, same producer/appellation)
- distinct_wines: legitimately different wines (different grapes, different styles, different appellations from same producer)

Key signals:
- Shared LWIN or COLA IDs across both records → strong true_duplicate signal
- Same producer + same appellation + same grapes + only spelling/whitespace differences → true_duplicate
- Same producer but different appellation or different grapes → distinct_wines
- Same name but one is red and the other is white → distinct_wines
- "Reserve" vs non-Reserve from same producer → distinct_wines
- Different cuvee/vineyard designations → distinct_wines
- One has vintages the other doesn't, but otherwise identical → true_duplicate

Respond with ONLY a JSON array. Each element must have exactly these keys:
- "idx": the pair index number from the input list (integer)
- "decision": exactly "true_duplicate" or "distinct_wines" (never "unclear" — make a call)
- "confidence": a decimal 0.0-1.0 reflecting how sure you are
- "reason": a short phrase (5-15 words max) explaining why

Example response:
[{"idx": 1, "decision": "true_duplicate", "confidence": 0.95, "reason": "Same producer, grapes, appellation — only whitespace difference in name"}, {"idx": 2, "decision": "distinct_wines", "confidence": 0.88, "reason": "Same producer but different appellations and grape varieties"}]"""


WINE_CONTEXT_SQL = """
SELECT w.id, w.name, w.color, w.wine_type, p.name as producer,
       a.name as appellation, r.name as region,
       string_agg(DISTINCT g.name, ', ') as grapes,
       array_agg(DISTINCT ei.external_id) FILTER (WHERE ei.system = 'lwin') as lwin_ids,
       array_agg(DISTINCT ei.external_id) FILTER (WHERE ei.system = 'cola') as cola_ids,
       array_agg(DISTINCT wv.vintage_year ORDER BY wv.vintage_year) as vintages
FROM wines w
LEFT JOIN producers p ON p.id = w.producer_id
LEFT JOIN appellations a ON a.id = w.appellation_id
LEFT JOIN regions r ON r.id = w.region_id
LEFT JOIN wine_grapes wg ON wg.wine_id = w.id
LEFT JOIN grapes g ON g.id = wg.grape_id
LEFT JOIN external_ids ei ON ei.entity_id = w.id
LEFT JOIN wine_vintages wv ON wv.wine_id = w.id
WHERE w.id = %s
GROUP BY w.id, w.name, w.color, w.wine_type, p.name, a.name, r.name
"""


def compute_cost(input_tokens, output_tokens):
    """Compute cost in USD from token counts."""
    return (input_tokens * INPUT_COST_PER_M + output_tokens * OUTPUT_COST_PER_M) / 1_000_000


def fetch_flagged_groups(conn, limit=None):
    """Fetch all flagged match_decisions for wine entity type.

    Returns list of (md_id, primary_wine_id, [duplicate_wine_ids], notes).
    source_a_id is the primary wine, source_b_id is comma-separated duplicate IDs.
    """
    sql = """
        SELECT md.id, md.source_a_id, md.source_b_id, md.notes
        FROM match_decisions md
        WHERE md.status = 'flagged' AND md.entity_type = 'wine'
        ORDER BY md.created_at
    """
    if limit:
        sql += f" LIMIT {int(limit)}"
    with conn.cursor() as cur:
        cur.execute(sql)
        rows = cur.fetchall()
    result = []
    for md_id, source_a_id, source_b_id, notes in rows:
        dupe_ids = [s.strip() for s in source_b_id.split(",") if s.strip()]
        result.append((md_id, source_a_id, dupe_ids, notes))
    return result


def fetch_wine_context(conn, wine_id):
    """Fetch enriched context for a single wine ID."""
    with conn.cursor() as cur:
        cur.execute(WINE_CONTEXT_SQL, (str(wine_id),))
        row = cur.fetchone()
    if not row:
        return None
    return {
        "id": str(row[0]),
        "name": row[1],
        "color": row[2],
        "wine_type": row[3],
        "producer": row[4],
        "appellation": row[5],
        "region": row[6],
        "grapes": row[7],
        "lwin_ids": row[8] if row[8] and row[8] != [None] else None,
        "cola_ids": row[9] if row[9] and row[9] != [None] else None,
        "vintages": row[10] if row[10] and row[10] != [None] else None,
    }


def prefetch_wine_contexts(conn, wine_ids):
    """Batch-fetch wine context for a list of wine IDs. Returns {str(id): context_dict}."""
    cache = {}
    for wid in wine_ids:
        wid_str = str(wid)
        if wid_str not in cache:
            ctx = fetch_wine_context(conn, wid)
            cache[wid_str] = ctx
    return cache


def format_wine_for_prompt(ctx):
    """Format a wine context dict into a readable string."""
    if ctx is None:
        return "(wine not found in database)"
    parts = [f'"{ctx["name"]}"']
    details = []
    if ctx["producer"]:
        details.append(f'producer: {ctx["producer"]}')
    if ctx["color"]:
        details.append(f'color: {ctx["color"]}')
    if ctx["wine_type"] and ctx["wine_type"] != "table":
        details.append(f'type: {ctx["wine_type"]}')
    if ctx["grapes"]:
        details.append(f'grapes: {ctx["grapes"]}')
    if ctx["appellation"]:
        details.append(f'appellation: {ctx["appellation"]}')
    if ctx["region"]:
        details.append(f'region: {ctx["region"]}')
    if ctx["lwin_ids"]:
        details.append(f'LWIN: {", ".join(ctx["lwin_ids"][:3])}')
    if ctx["cola_ids"]:
        ids_display = ctx["cola_ids"][:5]
        details.append(f'COLA: {", ".join(ids_display)}{"..." if len(ctx["cola_ids"]) > 5 else ""}')
    if ctx["vintages"]:
        vints = [str(v) if v != 0 else "NV" for v in ctx["vintages"][:10]]
        details.append(f'vintages: [{", ".join(vints)}{"..." if len(ctx["vintages"]) > 10 else ""}]')
    if details:
        parts.append(f'  [{"; ".join(details)}]')
    return "\n".join(parts)


def build_prompt(batch, context_cache):
    """Build the user prompt for a batch of flagged groups.

    batch: list of (md_id, primary_wine_id, [dupe_wine_ids], notes)
    context_cache: {str(wine_id): context_dict}
    """
    lines = ["Classify each group of wine records as true_duplicate or distinct_wines:\n"]
    for i, (md_id, primary_id, dupe_ids, notes) in enumerate(batch, 1):
        ctx_primary = context_cache.get(str(primary_id))
        lines.append(f"--- GROUP {i} ({1 + len(dupe_ids)} records) ---")
        lines.append(f"Wine A (primary): {format_wine_for_prompt(ctx_primary)}")
        for j, did in enumerate(dupe_ids[:5]):  # Cap at 5 to control prompt size
            ctx_d = context_cache.get(str(did))
            lines.append(f"Wine {chr(66+j)}: {format_wine_for_prompt(ctx_d)}")
        if notes:
            lines.append(f"Previous note: {notes[:200]}")
        lines.append("")
    return "\n".join(lines)


def parse_response(text, batch_size):
    """Parse Haiku JSON response into {idx: (decision, confidence, reason)} dict."""
    text = text.strip()
    if text.startswith("```"):
        text = text.split("\n", 1)[1]
        if text.endswith("```"):
            text = text[:-3]
        text = text.strip()

    start = text.find("[")
    end = text.rfind("]") + 1
    if start < 0 or end <= start:
        print(f"  WARNING: No JSON array found in response: {text[:200]}")
        return {}

    try:
        items = json.loads(text[start:end])
    except json.JSONDecodeError as e:
        print(f"  WARNING: JSON parse error: {e}")
        print(f"  Response snippet: {text[start:start + 200]}")
        return {}

    results = {}
    for item in items:
        if not isinstance(item, dict):
            continue
        idx = item.get("idx")
        decision = item.get("decision", "").lower().strip()
        confidence = item.get("confidence", 0)
        reason = item.get("reason", "")

        if not isinstance(idx, int) or idx < 1 or idx > batch_size:
            continue
        # Normalize decision
        if "duplicate" in decision:
            decision = "true_duplicate"
        elif "distinct" in decision:
            decision = "distinct_wines"
        else:
            continue  # skip unparseable decisions
        if not isinstance(confidence, (int, float)):
            continue

        results[idx] = (decision, float(confidence), str(reason)[:200])

    return results


def main():
    parser = argparse.ArgumentParser(
        description="Re-classify flagged duplicate wine pairs with richer Haiku context"
    )
    parser.add_argument(
        "--execute", action="store_true",
        help="Apply changes (default is dry-run)"
    )
    parser.add_argument(
        "--limit", type=int, default=None,
        help="Max pairs to process"
    )
    parser.add_argument(
        "--budget", type=float, default=2.0,
        help="Max spend in USD (default $2.00)"
    )
    args = parser.parse_args()

    api_key = get_env("ANTHROPIC_API_KEY")
    client = anthropic.Anthropic(api_key=api_key)
    conn = get_conn()

    try:
        # ── Fetch flagged pairs ─────────────────────────────────
        groups = fetch_flagged_groups(conn, limit=args.limit)
        total_groups = len(groups)
        print(f"Flagged groups to reclassify: {total_groups}")

        if total_groups == 0:
            print("No flagged groups found. Nothing to do.")
            return

        if not args.execute:
            print("DRY RUN -- no writes (use --execute to apply)")
        print(f"Budget: ${args.budget:.2f}")
        print(f"Confidence threshold: {CONFIDENCE_THRESHOLD}")
        print(f"Batch size: {BATCH_SIZE} groups per API call")
        print()

        # ── Process in batches ──────────────────────────────────
        batches = [groups[i:i + BATCH_SIZE] for i in range(0, len(groups), BATCH_SIZE)]

        cumulative_cost = 0.0
        total_input_tokens = 0
        total_output_tokens = 0
        total_reclassified = 0
        total_low_conf = 0
        total_unparsed = 0
        total_missing_wine = 0
        total_errors = 0
        counts = {"true_duplicate": 0, "distinct_wines": 0}
        budget_exceeded = False

        for batch_idx, batch in enumerate(batches):
            batch_num = batch_idx + 1

            # Budget check
            if cumulative_cost >= args.budget:
                print(f"\n  BUDGET EXCEEDED (${cumulative_cost:.4f} >= ${args.budget:.2f}). Stopping.")
                budget_exceeded = True
                break

            # Prefetch wine contexts for this batch
            wine_ids = set()
            for _, primary_id, dupe_ids, _ in batch:
                wine_ids.add(primary_id)
                for did in dupe_ids:
                    wine_ids.add(did)
            context_cache = prefetch_wine_contexts(conn, wine_ids)

            # Check for missing wines
            missing_in_batch = 0
            for _, primary_id, dupe_ids, _ in batch:
                if context_cache.get(str(primary_id)) is None:
                    missing_in_batch += 1
                for did in dupe_ids:
                    if context_cache.get(str(did)) is None:
                        missing_in_batch += 1
            total_missing_wine += missing_in_batch

            prompt = build_prompt(batch, context_cache)

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
            parsed = parse_response(text, len(batch))

            # Apply decisions
            batch_reclassified = 0
            batch_low_conf = 0
            batch_counts = {"true_duplicate": 0, "distinct_wines": 0}

            for group_idx, (md_id, primary_id, dupe_ids, notes) in enumerate(batch, 1):
                if group_idx not in parsed:
                    total_unparsed += 1
                    continue

                decision, confidence, reason = parsed[group_idx]

                if confidence < CONFIDENCE_THRESHOLD:
                    batch_low_conf += 1
                    total_low_conf += 1
                    continue

                new_status = "ai_accepted" if decision == "true_duplicate" else "ai_rejected"
                note_append = f"Reclassified from flagged to {decision} (conf={confidence:.2f}): {reason}"

                batch_counts[decision] = batch_counts.get(decision, 0) + 1
                counts[decision] = counts.get(decision, 0) + 1

                if args.execute:
                    with conn.cursor() as cur:
                        cur.execute("""
                            UPDATE match_decisions
                            SET status = %s,
                                confidence = %s,
                                ai_model = %s,
                                ai_response = %s,
                                notes = COALESCE(notes, '') || E'\n' || %s,
                                updated_at = now()
                            WHERE id = %s AND status = 'flagged'
                        """, (
                            new_status,
                            confidence,
                            HAIKU_MODEL,
                            text[:4000],
                            note_append,
                            str(md_id),
                        ))
                    conn.commit()

                batch_reclassified += 1
                total_reclassified += 1

            unparsed_in_batch = len(batch) - len(parsed)

            # Progress output
            dist_str = ", ".join(f"{k}={v}" for k, v in sorted(batch_counts.items()) if v > 0)
            print(
                f"  Batch {batch_num}/{len(batches)}: "
                f"{batch_reclassified}/{len(batch)} reclassified ({dist_str}), "
                f"{batch_low_conf} low-conf, {unparsed_in_batch} unparsed | "
                f"cost ${cumulative_cost:.4f}"
            )

            # Show samples in dry-run mode
            if not args.execute and batch_reclassified > 0 and batch_num <= 3:
                shown = 0
                for group_idx, (md_id, primary_id, dupe_ids, notes) in enumerate(batch, 1):
                    if group_idx in parsed and shown < 3:
                        decision, confidence, reason = parsed[group_idx]
                        if confidence >= CONFIDENCE_THRESHOLD:
                            ctx_a = context_cache.get(str(primary_id))
                            name_a = ctx_a["name"] if ctx_a else "?"
                            n_dupes = len(dupe_ids)
                            print(f"    WOULD SET: {name_a} ({n_dupes} dupes) -> {decision} (conf={confidence:.2f}) — {reason}")
                            shown += 1

            # Brief pause to avoid rate limits
            if batch_idx + 1 < len(batches):
                time.sleep(0.3)

        # ── Summary ──────────────────────────────────────────
        print(f"\n{'=' * 60}")
        print(f"  SUMMARY {'(DRY RUN)' if not args.execute else ''}")
        print(f"{'=' * 60}")
        print(f"  Total flagged groups:       {total_groups}")
        print(f"  Batches processed:         {min(batch_idx + 1, len(batches)) if batches else 0}")
        print(f"  Reclassified (>={CONFIDENCE_THRESHOLD}): {total_reclassified}")
        print(f"    -> true_duplicate:       {counts['true_duplicate']}")
        print(f"    -> distinct_wines:       {counts['distinct_wines']}")
        print(f"  Skipped (low confidence):  {total_low_conf}")
        print(f"  Unparsed:                  {total_unparsed}")
        print(f"  Missing wines:             {total_missing_wine}")
        print(f"  API errors:                {total_errors}")
        print(f"  Input tokens:              {total_input_tokens:,}")
        print(f"  Output tokens:             {total_output_tokens:,}")
        print(f"  Total cost:                ${cumulative_cost:.4f}")
        if budget_exceeded:
            print(f"  ** Stopped early due to budget limit **")
        still_flagged = total_groups - total_reclassified
        print(f"  Still flagged:             {still_flagged}")
        if not args.execute and total_reclassified > 0:
            print(f"\n  Re-run with --execute to apply {total_reclassified} reclassifications.")

    finally:
        conn.close()


if __name__ == "__main__":
    main()
