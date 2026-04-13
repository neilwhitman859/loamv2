#!/usr/bin/env python3
"""Match knowledge_seed wines to LWIN using Sonnet for fuzzy evaluation.

Finds LWIN candidates by producer name, then asks Sonnet to verify matches.
Only assigns LWIN-7 codes at confidence >= 0.85 and when no conflict exists.

Usage:
    python -m pipeline.promote.lwin_sonnet_match [--budget 5.0] [--dry-run]
"""

import argparse
import json
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import anthropic

from pipeline.lib.db import get_conn, get_env
from pipeline.lib.models import SONNET_MODEL
from pipeline.lib.normalize import normalize

PREFIXES = ['chateau ', 'domaine ', 'bodega ', 'bodegas ', 'weingut ',
            'tenuta ', 'cantina ', 'maison ', 'cave ']


def strip_prefix(name):
    lower = name.lower()
    for p in PREFIXES:
        if lower.startswith(p):
            return name[len(p):].strip()
    return name


SYSTEM_PROMPT = (
    "You are a wine identity matcher. For each wine, I give you a target wine "
    "(producer + wine name) and LWIN database candidates. Determine which LWIN "
    "entry, if any, is the SAME wine.\n\n"
    "Rules:\n"
    "- Only match if you are CERTAIN it is the same wine (same producer, same cuvee)\n"
    "- Producer name variants OK (Chateau Margaux = Margaux, Penfolds = Penfolds)\n"
    "- Wine name variants OK (Grange = Grange Hermitage, Brut = Brut NV)\n"
    "- Generic varietal by same producer matches same varietal in LWIN\n"
    "- If NONE match, output lwin_7 as \"none\"\n"
    "- Be conservative: wrong matches are worse than missed matches\n\n"
    "Respond with ONLY a JSON array. Each element:\n"
    "{\"idx\": <1-based>, \"lwin_7\": \"<LWIN-7 or none>\", \"confidence\": <0.0-1.0>}"
)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--budget", type=float, default=5.0)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    api_key = get_env("ANTHROPIC_API_KEY")
    client = anthropic.Anthropic(api_key=api_key)
    conn = get_conn()
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute("SET statement_timeout = 0")

    # Get promoted wines without backbone IDs
    cur.execute("""
        SELECT w.id, w.name, p.name as prod_name
        FROM wines w
        JOIN producers p ON p.id = w.producer_id
        WHERE w.id IN (
            SELECT canonical_wine_id FROM source_claude_knowledge
            WHERE match_status = 'promoted'
        )
        AND w.id NOT IN (
            SELECT entity_id::uuid FROM external_ids
            WHERE entity_type = 'wine' AND system IN ('lwin_7', 'cola')
        )
    """)
    wines = cur.fetchall()
    print(f"{len(wines)} wines without backbone IDs")

    # Build candidates by searching LWIN by producer name
    all_candidates = []
    for wine_id, wine_name, prod_name in wines:
        prod_stripped = strip_prefix(prod_name)
        cur.execute("""
            SELECT DISTINCT producer_name, wine_name, lwin_7
            FROM source_lwin
            WHERE lwin_7 IS NOT NULL
              AND (
                LOWER(producer_name) = LOWER(%s)
                OR LOWER(producer_name) = LOWER(%s)
                OR LOWER(producer_name) LIKE %s
              )
            LIMIT 15
        """, (prod_name, prod_stripped, f'%{prod_stripped.lower()}%'))
        lwin_cands = cur.fetchall()
        if lwin_cands:
            all_candidates.append({
                'wine_id': str(wine_id),
                'wine_name': wine_name,
                'producer': prod_name,
                'lwin_options': [
                    {'lwin_prod': r[0], 'lwin_wine': r[1], 'lwin_7': r[2]}
                    for r in lwin_cands
                ]
            })

    print(f"{len(all_candidates)} wines have LWIN candidates to evaluate")
    if not all_candidates:
        print("Nothing to do.")
        conn.close()
        return

    # Batch to Sonnet
    BATCH_SIZE = 20
    total_matched = 0
    total_cost = 0.0
    all_matches = []

    for batch_start in range(0, len(all_candidates), BATCH_SIZE):
        batch = all_candidates[batch_start:batch_start + BATCH_SIZE]
        batch_num = batch_start // BATCH_SIZE + 1
        total_batches = (len(all_candidates) + BATCH_SIZE - 1) // BATCH_SIZE

        if total_cost >= args.budget:
            print(f"\nBUDGET EXCEEDED (${total_cost:.4f} >= ${args.budget:.2f}). Stopping.")
            break

        lines = []
        for i, cand in enumerate(batch, 1):
            lines.append(f"{i}. TARGET: {cand['producer']} / {cand['wine_name']}")
            lines.append("   LWIN candidates:")
            for opt in cand['lwin_options']:
                wn = opt['lwin_wine'] or '(no wine name)'
                lines.append(
                    f"     - {opt['lwin_prod']} / {wn} (LWIN-7: {opt['lwin_7']})"
                )

        prompt = "\n".join(lines)

        try:
            response = client.messages.create(
                model=SONNET_MODEL,
                max_tokens=4096,
                system=SYSTEM_PROMPT,
                messages=[{"role": "user", "content": prompt}],
            )
        except Exception as e:
            print(f"  Batch {batch_num}: API error: {e}")
            time.sleep(2)
            continue

        inp_tok = response.usage.input_tokens
        out_tok = response.usage.output_tokens
        batch_cost = inp_tok * 3.00 / 1_000_000 + out_tok * 15.00 / 1_000_000
        total_cost += batch_cost

        text = response.content[0].text.strip()
        start = text.find("[")
        end = text.rfind("]") + 1
        if start < 0 or end <= start:
            print(f"  Batch {batch_num}: no JSON found")
            continue

        try:
            items = json.loads(text[start:end])
        except json.JSONDecodeError as e:
            print(f"  Batch {batch_num}: JSON error: {e}")
            continue

        batch_matches = 0
        for item in items:
            idx = item.get('idx', 0)
            lwin7 = item.get('lwin_7', 'none')
            conf = item.get('confidence', 0)
            if lwin7 == 'none' or conf < 0.85:
                continue
            if idx < 1 or idx > len(batch):
                continue

            cand = batch[idx - 1]
            # Verify LWIN-7 is actually from the candidate list
            valid_lwins = {opt['lwin_7'] for opt in cand['lwin_options']}
            if lwin7 not in valid_lwins:
                continue

            all_matches.append((
                cand['wine_id'], cand['wine_name'], cand['producer'],
                lwin7, conf
            ))
            batch_matches += 1

        total_matched += batch_matches
        print(
            f"  Batch {batch_num}/{total_batches}: "
            f"{batch_matches} matches, cost ${batch_cost:.4f}"
        )
        time.sleep(0.3)

    print(f"\nSonnet evaluation: {total_matched} matches, ${total_cost:.4f}")

    if args.dry_run:
        print("\nDRY RUN - matches found but not applied:")
        for wine_id, wine_name, producer, lwin7, conf in all_matches:
            print(f"  {producer} / {wine_name} -> LWIN {lwin7} (conf={conf:.2f})")
        conn.close()
        return

    # Apply matches, checking for conflicts
    applied = 0
    skipped = 0
    for wine_id, wine_name, producer, lwin7, conf in all_matches:
        cur.execute(
            "SELECT entity_id FROM external_ids "
            "WHERE entity_type='wine' AND system='lwin_7' AND external_id=%s",
            (lwin7,)
        )
        existing = cur.fetchone()
        if existing and str(existing[0]) != wine_id:
            skipped += 1
            continue

        cur.execute(
            "INSERT INTO external_ids (entity_type, entity_id, system, external_id) "
            "VALUES ('wine', %s, 'lwin_7', %s) ON CONFLICT DO NOTHING",
            (wine_id, lwin7)
        )
        applied += 1
        print(f"  {producer} / {wine_name} -> LWIN {lwin7} (conf={conf:.2f})")

    print(f"\nApplied: {applied}, Skipped (conflict): {skipped}")
    conn.close()


if __name__ == '__main__':
    main()
