#!/usr/bin/env python3
"""Anthropic Batch API enrichment — submit, poll, process.

50% cost discount vs sequential. No rate limiting. Submit and forget.

Usage:
    # Submit Grade C batch for all D-grade wines
    python -m pipeline.enrich.batch_api submit --grade C

    # Submit Grade B batch for top N wines
    python -m pipeline.enrich.batch_api submit --grade B --limit 100

    # Check batch status
    python -m pipeline.enrich.batch_api status --batch-id msgbatch_xxx

    # Process completed batch results into DB
    python -m pipeline.enrich.batch_api process --batch-id msgbatch_xxx --grade C

    # List recent batches
    python -m pipeline.enrich.batch_api list
"""

import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

os.environ["PYTHONUNBUFFERED"] = "1"
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from dotenv import load_dotenv
load_dotenv()

import anthropic
from anthropic.types.message_create_params import MessageCreateParamsNonStreaming
from anthropic.types.messages.batch_create_params import Request

from pipeline.lib.db import get_conn
from pipeline.enrich.batch_enrich import (
    bulk_preload_context,
    build_grade_c_prompt,
    build_grade_b_prompt,
    write_grade_c,
    write_grade_b,
    log_enrichment,
    select_wines,
    HAIKU_MODEL,
    SONNET_MODEL,
    PRICING,
)


BATCH_LOG_FILE = Path("data/stats/batch_enrichment_log.json")


def save_batch_info(batch_id: str, grade: str, model: str, wine_count: int, wine_ids: list[str]):
    """Save batch metadata for later processing."""
    log = []
    if BATCH_LOG_FILE.exists():
        log = json.loads(BATCH_LOG_FILE.read_text())

    log.append({
        "batch_id": batch_id,
        "grade": grade,
        "model": model,
        "wine_count": wine_count,
        "wine_ids": wine_ids,
        "submitted_at": datetime.now(timezone.utc).isoformat(),
        "status": "submitted",
    })
    BATCH_LOG_FILE.write_text(json.dumps(log, indent=2))


def cmd_submit(args):
    """Build prompts and submit batch to Anthropic."""
    model = HAIKU_MODEL if args.grade == "C" else SONNET_MODEL
    pricing = PRICING[model]

    conn = get_conn()
    cur = conn.cursor()

    # Select wines
    wines = select_wines(cur, args.grade, args.limit)
    print(f"\n  Selected {len(wines)} wines for Grade {args.grade} batch")

    if not wines:
        print("  No wines to enrich.")
        return

    # Bulk preload context
    wine_ids = [str(w["id"]) for w in wines]
    CHUNK = 500
    all_contexts = {}
    for i in range(0, len(wine_ids), CHUNK):
        chunk = wine_ids[i:i + CHUNK]
        all_contexts.update(bulk_preload_context(cur, chunk))
        print(f"  Pre-loaded {min(i + CHUNK, len(wine_ids))}/{len(wine_ids)} contexts...")
    sys.stdout.flush()

    cur.close()
    conn.close()

    # Build batch requests
    requests = []
    skipped = 0
    for wid in wine_ids:
        ctx = all_contexts.get(wid)
        if not ctx:
            skipped += 1
            continue

        if args.grade == "C":
            prompt = build_grade_c_prompt(ctx)
        else:
            prompt = build_grade_b_prompt(ctx)

        requests.append(Request(
            custom_id=wid,
            params=MessageCreateParamsNonStreaming(
                model=model,
                max_tokens=2000,
                messages=[{"role": "user", "content": prompt}],
            ),
        ))

    print(f"  Built {len(requests)} requests ({skipped} skipped)")

    if not requests:
        print("  Nothing to submit.")
        return

    # Estimate cost (50% batch discount)
    avg_input = 1500  # rough estimate
    avg_output = 500
    est_cost = len(requests) * (
        avg_input * pricing["input"] / 1_000_000 +
        avg_output * pricing["output"] / 1_000_000
    ) * 0.5  # 50% batch discount
    print(f"  Estimated cost: ~${est_cost:.2f} (50% batch discount)")
    print(f"  Submitting to Anthropic Batch API...")
    sys.stdout.flush()

    # Submit
    client = anthropic.Anthropic()
    batch = client.messages.batches.create(requests=requests)

    print(f"\n  Batch submitted!")
    print(f"  Batch ID: {batch.id}")
    print(f"  Status: {batch.processing_status}")
    print(f"  Requests: {len(requests)}")
    print(f"\n  To check status:")
    print(f"    python -m pipeline.enrich.batch_api status --batch-id {batch.id}")
    print(f"\n  To process results when done:")
    print(f"    python -m pipeline.enrich.batch_api process --batch-id {batch.id} --grade {args.grade}")

    # Save for later
    save_batch_info(batch.id, args.grade, model, len(requests), wine_ids)


def cmd_status(args):
    """Check batch processing status."""
    client = anthropic.Anthropic()
    batch = client.messages.batches.retrieve(args.batch_id)

    print(f"\n  Batch: {batch.id}")
    print(f"  Status: {batch.processing_status}")
    print(f"  Succeeded: {batch.request_counts.succeeded}")
    print(f"  Errored: {batch.request_counts.errored}")
    print(f"  Expired: {batch.request_counts.expired}")
    print(f"  Processing: {batch.request_counts.processing}")
    print(f"  Created: {batch.created_at}")

    if batch.processing_status == "ended":
        print(f"\n  Ready to process! Run:")
        # Look up grade from log
        grade = "C"
        if BATCH_LOG_FILE.exists():
            for entry in json.loads(BATCH_LOG_FILE.read_text()):
                if entry["batch_id"] == args.batch_id:
                    grade = entry["grade"]
                    break
        print(f"    python -m pipeline.enrich.batch_api process --batch-id {batch.id} --grade {grade}")


def cmd_process(args):
    """Download batch results and write to DB."""
    client = anthropic.Anthropic()

    # Verify batch is done
    batch = client.messages.batches.retrieve(args.batch_id)
    if batch.processing_status != "ended":
        print(f"  Batch still processing ({batch.processing_status}). Try again later.")
        return

    print(f"\n  Processing batch {args.batch_id}")
    print(f"  Grade: {args.grade}")
    print(f"  Succeeded: {batch.request_counts.succeeded}")
    print(f"  Errored: {batch.request_counts.errored}")

    model = HAIKU_MODEL if args.grade == "C" else SONNET_MODEL
    pricing = PRICING[model]

    conn = get_conn()
    cur = conn.cursor()

    success = 0
    errors = 0
    total_cost = 0.0

    for result in client.messages.batches.results(args.batch_id):
        wine_id = result.custom_id

        if result.result.type == "succeeded":
            try:
                msg = result.result.message
                raw = msg.content[0].text.strip()

                # Strip markdown fences
                if raw.startswith("```"):
                    raw = raw.split("\n", 1)[1] if "\n" in raw else raw[3:]
                    if raw.endswith("```"):
                        raw = raw[:-3].strip()

                parsed = json.loads(raw)

                # Normalize comparable_wines
                cw = parsed.get("comparable_wines")
                if cw and not isinstance(cw, str):
                    if isinstance(cw, list):
                        parsed["comparable_wines"] = "\n".join(str(x) for x in cw)
                    elif isinstance(cw, dict):
                        parsed["comparable_wines"] = "\n".join(f"{k} — {v}" for k, v in cw.items())

                fp = parsed.get("food_pairing")
                if fp and not isinstance(fp, str):
                    if isinstance(fp, list):
                        parsed["food_pairing"] = "\n".join(str(x) for x in fp)

                # Write to DB
                if args.grade == "C":
                    write_grade_c(cur, wine_id, parsed)
                else:
                    write_grade_b(cur, wine_id, parsed)

                # Calculate cost (with 50% discount)
                usage = msg.usage
                cost = (usage.input_tokens * pricing["input"] / 1_000_000 +
                        usage.output_tokens * pricing["output"] / 1_000_000) * 0.5
                total_cost += cost

                log_enrichment(cur, wine_id, args.grade, model,
                               {"input_tokens": usage.input_tokens, "output_tokens": usage.output_tokens},
                               cost, "completed")
                conn.commit()
                success += 1

                if success % 100 == 0:
                    print(f"  Processed {success}... (${total_cost:.4f})")
                    sys.stdout.flush()

            except Exception as e:
                errors += 1
                log_enrichment(cur, wine_id, args.grade, model, {}, 0, "error", str(e)[:500])
                conn.commit()
                if errors <= 5:
                    print(f"  ERROR {wine_id}: {e}")

        elif result.result.type == "errored":
            errors += 1
            err_msg = str(result.result.error) if result.result.error else "unknown"
            log_enrichment(cur, wine_id, args.grade, model, {}, 0, "error", err_msg[:500])
            conn.commit()

        elif result.result.type == "expired":
            errors += 1
            log_enrichment(cur, wine_id, args.grade, model, {}, 0, "error", "batch_expired")
            conn.commit()

    print(f"\n  {'='*50}")
    print(f"  RESULTS: {success} enriched, {errors} errors")
    print(f"  Cost: ${total_cost:.4f} (with 50% batch discount)")
    print(f"  {'='*50}")

    # Update batch log
    if BATCH_LOG_FILE.exists():
        log = json.loads(BATCH_LOG_FILE.read_text())
        for entry in log:
            if entry["batch_id"] == args.batch_id:
                entry["status"] = "processed"
                entry["success"] = success
                entry["errors"] = errors
                entry["cost"] = round(total_cost, 4)
                entry["processed_at"] = datetime.now(timezone.utc).isoformat()
        BATCH_LOG_FILE.write_text(json.dumps(log, indent=2))

    cur.close()
    conn.close()


def cmd_list(args):
    """List recent batches."""
    if BATCH_LOG_FILE.exists():
        log = json.loads(BATCH_LOG_FILE.read_text())
        for entry in log:
            print(f"  {entry['batch_id']} | Grade {entry['grade']} | "
                  f"{entry['wine_count']} wines | {entry['status']} | "
                  f"{entry.get('submitted_at', '?')[:16]}")
    else:
        print("  No batches submitted yet.")

    # Also check Anthropic for active batches
    print("\n  Checking Anthropic for active batches...")
    client = anthropic.Anthropic()
    batches = client.messages.batches.list(limit=5)
    for b in batches:
        print(f"  {b.id} | {b.processing_status} | "
              f"ok={b.request_counts.succeeded} err={b.request_counts.errored} "
              f"proc={b.request_counts.processing}")


def main():
    parser = argparse.ArgumentParser(description="Anthropic Batch API enrichment")
    sub = parser.add_subparsers(dest="command", required=True)

    p_submit = sub.add_parser("submit", help="Submit batch")
    p_submit.add_argument("--grade", choices=["C", "B"], required=True)
    p_submit.add_argument("--limit", type=int, default=5000)

    p_status = sub.add_parser("status", help="Check batch status")
    p_status.add_argument("--batch-id", required=True)

    p_process = sub.add_parser("process", help="Process completed batch")
    p_process.add_argument("--batch-id", required=True)
    p_process.add_argument("--grade", choices=["C", "B"], required=True)

    sub.add_parser("list", help="List batches")

    args = parser.parse_args()

    if args.command == "submit":
        cmd_submit(args)
    elif args.command == "status":
        cmd_status(args)
    elif args.command == "process":
        cmd_process(args)
    elif args.command == "list":
        cmd_list(args)


if __name__ == "__main__":
    main()
