"""
AI-assisted wine matching: importer staging wines → LWIN canonical wines.

Uses Haiku to match wines within the same producer. The producer constraint
limits candidates to typically 3-30 wines, making this a trivial judgment call
for an LLM with wine knowledge.

Usage:
    python -m pipeline.promote.importer_wine_match [--dry-run] [--source skurnik|kl|empson|ec|winebow|all] [--limit N]
"""

import argparse
import json
import os
import time
from collections import defaultdict

import anthropic

from pipeline.lib.db import get_supabase, get_env
from pipeline.lib.models import SONNET_MODEL


# Source configurations
SOURCES = {
    "skurnik": {
        "table": "source_skurnik",
        "wine_col": "name",
        "producer_col": "producer",
    },
    "kl": {
        "table": "source_kermit_lynch",
        "wine_col": "wine_name",
        "producer_col": "grower_name",
    },
    "empson": {
        "table": "source_empson",
        "wine_col": "name",
        "producer_col": "producer",
    },
    "ec": {
        "table": "source_european_cellars",
        "wine_col": "name",
        "producer_col": "producer",
    },
    "winebow": {
        "table": "source_winebow",
        "wine_col": "name",
        "producer_col": "producer",
    },
}


def load_unmatched_wines(sb, source_key):
    """Load unmatched wines grouped by producer from a staging source."""
    cfg = SOURCES[source_key]
    table = cfg["table"]
    wine_col = cfg["wine_col"]
    producer_col = cfg["producer_col"]

    # Fetch unmatched, producer-linked wines
    all_rows = []
    offset = 0
    batch = 1000
    while True:
        resp = (
            sb.table(table)
            .select(f"id, {wine_col}, {producer_col}, canonical_producer_id")
            .not_.is_(f"canonical_producer_id", "null")
            .is_(f"canonical_wine_id", "null")
            .not_.is_(wine_col, "null")
            .range(offset, offset + batch - 1)
            .execute()
        )
        if not resp.data:
            break
        all_rows.extend(resp.data)
        if len(resp.data) < batch:
            break
        offset += batch

    # Group by producer
    by_producer = defaultdict(list)
    for row in all_rows:
        pid = row["canonical_producer_id"]
        wine_name = row[wine_col]
        by_producer[pid].append({"id": row["id"], "name": wine_name})

    # Deduplicate wine names within each producer (same wine, multiple vintages)
    for pid in by_producer:
        seen = {}
        deduped = []
        for w in by_producer[pid]:
            name_upper = w["name"].strip().upper()
            if name_upper not in seen:
                seen[name_upper] = w
                deduped.append(w)
            else:
                # Keep track of all row IDs for this wine name
                if "extra_ids" not in seen[name_upper]:
                    seen[name_upper]["extra_ids"] = []
                seen[name_upper]["extra_ids"].append(w["id"])
        by_producer[pid] = deduped

    return by_producer


def load_lwin_wines_for_producers(sb, producer_ids):
    """Load all canonical wines for a set of producers."""
    wines_by_producer = defaultdict(list)

    # Batch fetch in groups of 50 producer IDs
    pid_list = list(producer_ids)
    for i in range(0, len(pid_list), 50):
        batch_pids = pid_list[i : i + 50]
        offset = 0
        while True:
            resp = (
                sb.table("wines")
                .select("id, name, producer_id, color, wine_type")
                .in_("producer_id", batch_pids)
                .range(offset, offset + 999)
                .execute()
            )
            if not resp.data:
                break
            for w in resp.data:
                wines_by_producer[w["producer_id"]].append(w)
            if len(resp.data) < 1000:
                break
            offset += 1000

    return wines_by_producer


def build_match_prompt(producer_name, importer_wine_name, lwin_candidates):
    """Build a Haiku prompt for wine matching."""
    candidates_text = "\n".join(
        f"  {i+1}. {w['name']}" for i, w in enumerate(lwin_candidates)
    )

    return f"""You are matching wine names from an importer catalog to a canonical wine database (LWIN).
Both refer to the same producer. The naming conventions differ:
- Importers use: grape + vineyard abbreviations, quoted cuvee names, appellation shorthand
- LWIN uses: full vineyard names, appellation + region suffixes, no quotes

Producer: {producer_name}
Importer wine: "{importer_wine_name}"

LWIN candidates:
{candidates_text}

Which LWIN candidate (if any) is the SAME wine? Consider:
- Same vineyard/cuvee name (even if abbreviated differently)
- Same appellation + grape combination
- "Vyd" = "Vineyard", "VT" = "Vendanges Tardives", "GC" = "Grand Cru"
- Ignore vintage/year differences

Reply with ONLY:
- The candidate number (e.g., "3") if you find a match
- "NONE" if no candidate matches (this is a new wine not in LWIN)
- "UNCERTAIN" if you're not sure

One word answer only."""


def match_wines_with_ai(client, producer_name, importer_wines, lwin_wines, dry_run=False):
    """Match a batch of importer wines against LWIN candidates using Haiku."""
    results = []

    for imp_wine in importer_wines:
        if not lwin_wines:
            results.append({"importer_wine": imp_wine, "match": None, "reason": "no_candidates"})
            continue

        prompt = build_match_prompt(producer_name, imp_wine["name"], lwin_wines)

        if dry_run:
            results.append({"importer_wine": imp_wine, "match": None, "reason": "dry_run"})
            continue

        try:
            resp = client.messages.create(
                model=SONNET_MODEL,
                max_tokens=20,
                messages=[{"role": "user", "content": prompt}],
            )
            answer = resp.content[0].text.strip().upper()

            if answer == "NONE":
                results.append({
                    "importer_wine": imp_wine,
                    "match": None,
                    "reason": "no_match",
                    "tokens_in": resp.usage.input_tokens,
                    "tokens_out": resp.usage.output_tokens,
                })
            elif answer == "UNCERTAIN":
                results.append({
                    "importer_wine": imp_wine,
                    "match": None,
                    "reason": "uncertain",
                    "tokens_in": resp.usage.input_tokens,
                    "tokens_out": resp.usage.output_tokens,
                })
            elif answer.isdigit():
                idx = int(answer) - 1
                if 0 <= idx < len(lwin_wines):
                    results.append({
                        "importer_wine": imp_wine,
                        "match": lwin_wines[idx],
                        "reason": "ai_match",
                        "tokens_in": resp.usage.input_tokens,
                        "tokens_out": resp.usage.output_tokens,
                    })
                else:
                    results.append({
                        "importer_wine": imp_wine,
                        "match": None,
                        "reason": f"bad_index_{answer}",
                        "tokens_in": resp.usage.input_tokens,
                        "tokens_out": resp.usage.output_tokens,
                    })
            else:
                results.append({
                    "importer_wine": imp_wine,
                    "match": None,
                    "reason": f"unexpected_{answer[:20]}",
                    "tokens_in": resp.usage.input_tokens,
                    "tokens_out": resp.usage.output_tokens,
                })
        except Exception as e:
            print(f"    API ERROR: {e}")
            results.append({
                "importer_wine": imp_wine,
                "match": None,
                "reason": f"error_{str(e)[:80]}",
            })

    return results


def apply_matches(sb, source_key, all_results, dry_run=False):
    """Write AI matches back to staging tables."""
    cfg = SOURCES[source_key]
    table = cfg["table"]

    matched = 0
    no_match = 0
    uncertain = 0
    errors = 0

    for result in all_results:
        if result["match"] is not None and result["reason"] == "ai_match":
            wine_id = result["match"]["id"]
            row_ids = [result["importer_wine"]["id"]]
            row_ids.extend(result["importer_wine"].get("extra_ids", []))

            if not dry_run:
                for rid in row_ids:
                    try:
                        sb.table(table).update({
                            "canonical_wine_id": wine_id,
                            "match_confidence": 0.85,
                        }).eq("id", rid).execute()
                    except Exception as e:
                        print(f"  ERROR updating {rid}: {e}")
                        errors += 1
                        continue

            matched += len(row_ids)
        elif result["reason"] == "no_match":
            no_match += 1
        elif result["reason"] == "uncertain":
            uncertain += 1
        else:
            errors += 1

    return matched, no_match, uncertain, errors


def main():
    parser = argparse.ArgumentParser(description="AI wine matching for importer staging")
    parser.add_argument("--dry-run", action="store_true", help="Don't write to DB")
    parser.add_argument("--source", default="all", help="Source key or 'all'")
    parser.add_argument("--limit", type=int, default=0, help="Limit wines per source (0=all)")
    args = parser.parse_args()

    sb = get_supabase()
    client = anthropic.Anthropic(api_key=get_env("ANTHROPIC_API_KEY"))

    sources = list(SOURCES.keys()) if args.source == "all" else [args.source]
    total_input_tokens = 0
    total_output_tokens = 0
    grand_matched = 0
    grand_no_match = 0
    grand_uncertain = 0

    for source_key in sources:
        print(f"\n{'='*60}")
        print(f"SOURCE: {source_key}")
        print(f"{'='*60}")

        # Load unmatched wines grouped by producer
        by_producer = load_unmatched_wines(sb, source_key)
        total_wines = sum(len(wines) for wines in by_producer.values())
        print(f"Loaded {total_wines} unmatched wines across {len(by_producer)} producers")

        if args.limit:
            # Limit total wines processed
            limited = {}
            count = 0
            for pid, wines in by_producer.items():
                limited[pid] = wines
                count += len(wines)
                if count >= args.limit:
                    break
            by_producer = limited
            total_wines = sum(len(w) for w in by_producer.values())
            print(f"Limited to {total_wines} wines across {len(by_producer)} producers")

        # Load LWIN candidates for all relevant producers
        lwin_wines = load_lwin_wines_for_producers(sb, by_producer.keys())
        total_candidates = sum(len(w) for w in lwin_wines.values())
        print(f"Loaded {total_candidates} LWIN candidates")

        # Load producer names for prompts
        producer_names = {}
        pid_list = list(by_producer.keys())
        for i in range(0, len(pid_list), 50):
            batch = pid_list[i:i+50]
            resp = sb.table("producers").select("id, name").in_("id", batch).execute()
            for p in resp.data:
                producer_names[p["id"]] = p["name"]

        # Match each producer's wines — write to DB immediately per producer
        processed = 0
        matched = 0
        no_match = 0
        uncertain = 0
        errors = 0
        print(f"Matching wines via Sonnet...")
        for pid, imp_wines in by_producer.items():
            producer_name = producer_names.get(pid, "Unknown")
            candidates = lwin_wines.get(pid, [])

            results = match_wines_with_ai(
                client, producer_name, imp_wines, candidates, dry_run=args.dry_run
            )

            for r in results:
                total_input_tokens += r.get("tokens_in", 0)
                total_output_tokens += r.get("tokens_out", 0)

            # Apply matches immediately for this producer
            m, n, u, e = apply_matches(sb, source_key, results, dry_run=args.dry_run)
            matched += m
            no_match += n
            uncertain += u
            errors += e

            processed += len(imp_wines)
            if processed % 50 == 0 or processed == total_wines:
                print(f"  {processed}/{total_wines} wines | {matched} matched | {no_match} new | {uncertain} uncertain | {errors} errors")

        grand_matched += matched
        grand_no_match += no_match
        grand_uncertain += uncertain

        print(f"\n{source_key} results:")
        print(f"  Matched: {matched}")
        print(f"  No match (new wine): {no_match}")
        print(f"  Uncertain: {uncertain}")
        print(f"  Errors: {errors}")

    # Cost summary
    cost = (total_input_tokens * 0.80 + total_output_tokens * 4.0) / 1_000_000
    print(f"\n{'='*60}")
    print(f"COMPLETE")
    print(f"Total matched: {grand_matched}")
    print(f"Total no match: {grand_no_match}")
    print(f"Total uncertain: {grand_uncertain}")
    print(f"Tokens: {total_input_tokens:,} input, {total_output_tokens:,} output")
    print(f"Estimated cost: ${cost:.4f}")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
