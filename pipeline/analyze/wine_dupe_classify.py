#!/usr/bin/env python3
"""Classify duplicate wine groups using Haiku. Writes results to match_decisions.

Finds all wines sharing the same (name, producer_id), fetches their key
distinguishing attributes, and asks Haiku to classify each group as:
  - true_duplicate: same wine, should merge
  - distinct_wines: legitimately different wines sharing a name
  - unclear: not enough info to decide

All output goes to match_decisions — zero canonical table writes.

Usage:
    python -m pipeline.analyze.wine_dupe_classify [--dry-run] [--limit N] [--max-dupes N]
"""

import json
import sys
import uuid
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from pipeline.lib.db import get_conn, get_env

HAIKU_MODEL = "claude-haiku-4-5-20251001"
BATCH_SIZE = 12  # groups per Haiku call


SYSTEM_PROMPT = """You are a wine data quality expert. For each group of wine records that share
the same name and producer, classify whether they are:

- "true_duplicate": the same wine appearing multiple times (should merge into one record)
- "distinct_wines": legitimately different wines that happen to share a name (keep separate)
- "unclear": insufficient information to decide

Common patterns:
- Same wine name + different vintages in wine_vintages → true_duplicate (vintages belong on one record)
- Same wine name + different colors or wine_types → distinct_wines
- Same wine name + same LWIN or COLA ID on multiple records → true_duplicate
- Generic regional name (e.g., "Barossa Valley", "Napa Valley") + many records → likely distinct_wines (different varieties/cuvées)
- Producer makes only one wine of that style (e.g., "Le Pin" makes one Pomerol) → true_duplicate

Respond with one line per group in exactly this format:
GROUP_ID|classification|reason

Where classification is exactly one of: true_duplicate, distinct_wines, unclear
And reason is a short phrase (5-10 words max).
"""


def fetch_dupe_groups(cur, max_dupes=None):
    """Return list of (wine_name, producer_id, producer_name, [wine_ids])."""
    where = ""
    if max_dupes:
        where = f"HAVING count(*) <= {max_dupes}"
    else:
        where = "HAVING count(*) > 1"

    cur.execute(f"""
        SELECT w.name, w.producer_id, p.name as producer_name,
               array_agg(w.id::text ORDER BY w.created_at) as wine_ids
        FROM wines w
        JOIN producers p ON p.id = w.producer_id
        WHERE w.deleted_at IS NULL
          AND w.duplicate_of IS NULL
          AND w.name IS NOT NULL
        GROUP BY w.name, w.producer_id, p.name
        {where}
        ORDER BY count(*) DESC
    """)
    return cur.fetchall()


def fetch_wine_details(cur, wine_ids):
    """Fetch distinguishing attributes for a list of wine IDs."""
    ids_placeholder = ",".join(["%s"] * len(wine_ids))
    cur.execute(f"""
        SELECT
            w.id,
            w.color,
            w.wine_type,
            a.name AS appellation,
            array_agg(DISTINCT wv.vintage_year ORDER BY wv.vintage_year) AS vintages,
            array_agg(DISTINCT ei.system ORDER BY ei.system) FILTER (WHERE ei.system IS NOT NULL) AS id_systems,
            count(DISTINCT wvs.id) AS score_count
        FROM wines w
        LEFT JOIN appellations a ON a.id = w.appellation_id
        LEFT JOIN wine_vintages wv ON wv.wine_id = w.id
        LEFT JOIN external_ids ei ON ei.entity_id = w.id AND ei.entity_type = 'wine'
        LEFT JOIN wine_vintage_scores wvs ON wvs.wine_id = w.id
        WHERE w.id IN ({ids_placeholder})
        GROUP BY w.id, w.color, w.wine_type, a.name
    """, wine_ids)
    return {str(row[0]): row for row in cur.fetchall()}


def format_group_for_prompt(group_id, wine_name, producer_name, wine_ids, details):
    """Format a dupe group as a readable block for Haiku."""
    lines = [f"GROUP_ID: {group_id}",
             f"Wine: {wine_name} | Producer: {producer_name}",
             f"Records: {len(wine_ids)}"]
    for wid in wine_ids:
        d = details.get(wid)
        if not d:
            lines.append(f"  [{str(wid)[:8]}] (no details)")
            continue
        _, color, wine_type, appellation, vintages, id_systems, score_count = d
        vintage_str = str(vintages).replace("{", "").replace("}", "").replace("NULL", "NV") if vintages else "none"
        id_str = ", ".join(id_systems) if id_systems else "none"
        lines.append(
            f"  [{str(wid)[:8]}] color={color} type={wine_type} "
            f"appellation={appellation or 'none'} "
            f"vintages=[{vintage_str}] ids=[{id_str}] scores={score_count}"
        )
    return "\n".join(lines)


def parse_response(text, group_id_map):
    """Parse Haiku response into {group_id: (classification, reason)}."""
    results = {}
    for line in text.strip().splitlines():
        line = line.strip()
        if not line or "|" not in line:
            continue
        parts = line.split("|", 2)
        if len(parts) < 2:
            continue
        gid = parts[0].strip()
        classification = parts[1].strip().lower()
        reason = parts[2].strip() if len(parts) > 2 else ""

        if classification not in ("true_duplicate", "distinct_wines", "unclear"):
            # Try to normalize
            if "duplicate" in classification:
                classification = "true_duplicate"
            elif "distinct" in classification:
                classification = "distinct_wines"
            else:
                classification = "unclear"

        if gid in group_id_map:
            results[gid] = (classification, reason)
    return results


def insert_match_decisions(cur, group_id, wine_name, producer_name, wine_ids,
                            classification, reason, prompt_text, response_text):
    """Insert one match_decision row per duplicate group."""
    cur.execute("""
        INSERT INTO match_decisions (
            id, source_a, source_a_id, source_b, source_b_id,
            entity_type, match_method, confidence,
            status, ai_model, ai_prompt, ai_response, ai_extracted_data, notes,
            created_at, updated_at
        ) VALUES (
            gen_random_uuid(),
            'wines', %s,
            'wines', %s,
            'wine', 'haiku_dupe_classify', %s,
            %s, %s, %s, %s, %s, %s,
            now(), now()
        )
        ON CONFLICT DO NOTHING
    """, (
        str(wine_ids[0]),                          # source_a_id (canonical/first)
        ",".join(str(w) for w in wine_ids[1:]),    # source_b_id (rest)
        0.90 if classification == "true_duplicate" else 0.75,
        'ai_accepted' if classification == 'true_duplicate' else ('ai_rejected' if classification == 'distinct_wines' else 'flagged'),
        HAIKU_MODEL,
        prompt_text[:4000],
        response_text[:4000],
        json.dumps({
            "wine_name": wine_name,
            "producer_name": producer_name,
            "wine_ids": [str(w) for w in wine_ids],
            "classification": classification,
            "reason": reason,
        }),
        f"Dupe group: {wine_name} | {producer_name} | {len(wine_ids)} records | {reason}",
    ))


def main():
    args = sys.argv[1:]
    dry_run = "--dry-run" in args
    limit = None
    max_dupes = None

    if "--limit" in args:
        idx = args.index("--limit")
        limit = int(args[idx + 1])
    if "--max-dupes" in args:
        idx = args.index("--max-dupes")
        max_dupes = int(args[idx + 1])

    try:
        import anthropic
    except ImportError:
        print("ERROR: anthropic package not installed. Run: pip install anthropic")
        sys.exit(1)

    client = anthropic.Anthropic(api_key=get_env("ANTHROPIC_API_KEY"))
    conn = get_conn()

    try:
        with conn.cursor() as cur:
            print("Fetching duplicate groups...")
            groups = fetch_dupe_groups(cur, max_dupes=max_dupes)

        if limit:
            groups = groups[:limit]

        print(f"Found {len(groups)} duplicate groups")
        if dry_run:
            print("DRY RUN — no writes to match_decisions")

        total_tokens = 0
        counts = {"true_duplicate": 0, "distinct_wines": 0, "unclear": 0}

        # Build all group details
        batches = [
            groups[i:i + BATCH_SIZE]
            for i in range(0, len(groups), BATCH_SIZE)
        ]

        for batch_num, batch in enumerate(batches):
            print(f"\nBatch {batch_num + 1}/{len(batches)} ({len(batch)} groups)...")

            # Fetch wine details for all wines in this batch
            all_ids = [wid for _, _, _, wine_ids in batch for wid in wine_ids]
            with conn.cursor() as cur:
                details = fetch_wine_details(cur, all_ids)

            # Assign short group IDs for this batch
            group_id_map = {}
            prompt_blocks = []
            for idx, (wine_name, producer_id, producer_name, wine_ids) in enumerate(batch):
                gid = f"G{batch_num:03d}_{idx:02d}"
                group_id_map[gid] = (wine_name, producer_id, producer_name, wine_ids)
                block = format_group_for_prompt(
                    gid, wine_name, producer_name, wine_ids, details
                )
                prompt_blocks.append(block)

            prompt = "\n\n---\n\n".join(prompt_blocks)

            response = client.messages.create(
                model=HAIKU_MODEL,
                max_tokens=len(batch) * 30 + 200,
                system=SYSTEM_PROMPT,
                messages=[{"role": "user", "content": prompt}],
            )
            total_tokens += response.usage.input_tokens + response.usage.output_tokens
            response_text = response.content[0].text
            parsed = parse_response(response_text, group_id_map)

            # Write to match_decisions
            written = 0
            for gid, (classification, reason) in parsed.items():
                wine_name, producer_id, producer_name, wine_ids = group_id_map[gid]
                counts[classification] += 1

                if not dry_run:
                    with conn.cursor() as cur:
                        insert_match_decisions(
                            cur, gid, wine_name, producer_name, wine_ids,
                            classification, reason, prompt[:2000], response_text
                        )
                    conn.commit()
                    written += 1
                else:
                    print(f"  {classification:15s} | {wine_name} ({producer_name}) | {reason}")

            print(f"  ->{len(parsed)} classified, {written} written")

        cost = (total_tokens / 1_000_000) * 0.80
        print(f"\n{'DRY RUN ' if dry_run else ''}DONE")
        print(f"  true_duplicate:  {counts['true_duplicate']:,}")
        print(f"  distinct_wines:  {counts['distinct_wines']:,}")
        print(f"  unclear:         {counts['unclear']:,}")
        print(f"  Tokens: {total_tokens:,}  (~${cost:.3f})")

    finally:
        conn.close()


if __name__ == "__main__":
    main()
