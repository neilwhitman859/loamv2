#!/usr/bin/env python3
"""Cron-friendly batch enrichment runner.

End-to-end: select wines → submit batch → poll → process → log.
Designed to be run by a scheduler (cron, Windows Task Scheduler, etc.)
or manually for sweeps and refresh cycles.

Modes:
  --mode new        Enrich wines that have no wine_insights row yet
  --mode refresh    Re-enrich wines whose refresh_after has passed
  --mode retry      Retry wines with error logs that have no insights
  --mode promote    Promote Grade C wines that have been looked up recently to Grade B

Usage:
  # Nightly: enrich new wines without insights
  python -m pipeline.enrich.batch_runner --mode new --grade C --limit 5000

  # Weekly: retry failed enrichments
  python -m pipeline.enrich.batch_runner --mode retry --grade C

  # Annually: refresh stale enrichments
  python -m pipeline.enrich.batch_runner --mode refresh --grade C

  # On-demand: promote hot wines
  python -m pipeline.enrich.batch_runner --mode promote --limit 100

Logs all runs to data/stats/batch_runner_journal.md.
"""

import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

os.environ["PYTHONUNBUFFERED"] = "1"
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from dotenv import load_dotenv
load_dotenv(override=True)

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
    HAIKU_MODEL,
    SONNET_MODEL,
    PRICING,
)

JOURNAL = Path("data/stats/batch_runner_journal.md")
MAX_WAIT_MINUTES = 60  # how long to wait for batch to finish


# ── Wine selectors ────────────────────────────────────────────

def select_new_wines(cur, grade: str, limit: int) -> list[str]:
    """Wines that have no wine_insights row yet."""
    target_grade = "'C'" if grade == "C" else "'B'"
    cur.execute(f"""
        SELECT w.id FROM wines w
        WHERE w.deleted_at IS NULL
          AND NOT EXISTS (
              SELECT 1 FROM wine_insights wi
              WHERE wi.wine_id = w.id
              AND wi.enrichment_tier >= {target_grade}
          )
        ORDER BY
            (SELECT count(*) FROM wine_vintage_scores s WHERE s.wine_id = w.id) DESC,
            (SELECT count(*) FROM wine_vintage_prices p WHERE p.wine_id = w.id) DESC,
            (SELECT count(*) FROM wine_grapes wg WHERE wg.wine_id = w.id) DESC,
            (SELECT count(*) FROM wine_vintages wv WHERE wv.wine_id = w.id) DESC
        LIMIT %s
    """, (limit,))
    return [str(r[0]) for r in cur.fetchall()]


def select_refresh_wines(cur, grade: str, limit: int) -> list[str]:
    """Wines whose enrichment has gone stale."""
    cur.execute("""
        SELECT wi.wine_id FROM wine_insights wi
        JOIN wines w ON wi.wine_id = w.id
        WHERE w.deleted_at IS NULL
          AND wi.enrichment_tier = %s
          AND wi.refresh_after < NOW()
        ORDER BY wi.refresh_after
        LIMIT %s
    """, (grade, limit))
    return [str(r[0]) for r in cur.fetchall()]


def select_retry_wines(cur, grade: str, limit: int) -> list[str]:
    """Wines that errored during previous enrichment and still have no insights."""
    enrichment_type = f"grade_{grade.lower()}"
    cur.execute("""
        SELECT DISTINCT el.entity_id FROM enrichment_log el
        JOIN wines w ON el.entity_id = w.id
        WHERE el.status = 'error'
          AND el.enrichment_type = %s
          AND w.deleted_at IS NULL
          AND NOT EXISTS (
              SELECT 1 FROM wine_insights wi WHERE wi.wine_id = w.id
          )
        LIMIT %s
    """, (enrichment_type, limit))
    return [str(r[0]) for r in cur.fetchall()]


def select_promote_wines(cur, limit: int) -> list[str]:
    """Grade C wines that have been looked up recently — promote to Grade B.

    Uses wines.lookup_count if it exists; otherwise falls back to most-scored wines.
    """
    cur.execute("""
        SELECT column_name FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = 'wines' AND column_name = 'lookup_count'
    """)
    has_lookup = bool(cur.fetchone())

    if has_lookup:
        cur.execute("""
            SELECT wi.wine_id FROM wine_insights wi
            JOIN wines w ON wi.wine_id = w.id
            WHERE w.deleted_at IS NULL
              AND wi.enrichment_tier = 'C'
              AND w.lookup_count > 0
            ORDER BY w.lookup_count DESC
            LIMIT %s
        """, (limit,))
    else:
        # Fallback: promote the most data-rich C wines
        cur.execute("""
            SELECT wi.wine_id FROM wine_insights wi
            JOIN wines w ON wi.wine_id = w.id
            WHERE w.deleted_at IS NULL
              AND wi.enrichment_tier = 'C'
            ORDER BY
                (SELECT count(*) FROM wine_vintage_scores s WHERE s.wine_id = w.id) DESC,
                (SELECT count(*) FROM wine_vintage_prices p WHERE p.wine_id = w.id) DESC
            LIMIT %s
        """, (limit,))
    return [str(r[0]) for r in cur.fetchall()]


# ── Batch lifecycle ───────────────────────────────────────────

def submit_batch(client, wine_ids: list[str], grade: str, cur) -> str:
    """Preload context, build prompts, submit to Batch API."""
    print(f"  Preloading context for {len(wine_ids)} wines...")
    sys.stdout.flush()

    contexts = {}
    CHUNK = 500
    for i in range(0, len(wine_ids), CHUNK):
        contexts.update(bulk_preload_context(cur, wine_ids[i:i + CHUNK]))

    requests = []
    for wid in wine_ids:
        ctx = contexts.get(wid)
        if not ctx:
            continue

        if grade == "C":
            prompt = build_grade_c_prompt(ctx)
            model = HAIKU_MODEL
        else:
            prompt = build_grade_b_prompt(ctx)
            model = SONNET_MODEL

        requests.append(Request(
            custom_id=wid,
            params=MessageCreateParamsNonStreaming(
                model=model,
                max_tokens=2000,
                messages=[{"role": "user", "content": prompt}],
            ),
        ))

    print(f"  Built {len(requests)} requests, submitting...")
    sys.stdout.flush()

    batch = client.messages.batches.create(requests=requests)
    print(f"  Batch ID: {batch.id}")
    return batch.id


def poll_batch(client, batch_id: str, max_wait_min: int = MAX_WAIT_MINUTES) -> bool:
    """Poll until batch ends or timeout. Returns True if ended successfully."""
    start = time.time()
    last_status = None

    while (time.time() - start) < (max_wait_min * 60):
        batch = client.messages.batches.retrieve(batch_id)
        status = batch.processing_status
        counts = batch.request_counts

        if status != last_status:
            print(f"  [{int((time.time() - start) / 60)}m] {status}: "
                  f"ok={counts.succeeded} err={counts.errored} proc={counts.processing}")
            sys.stdout.flush()
            last_status = status

        if status == "ended":
            return True

        # Adaptive polling: quick at first, slower later
        elapsed = time.time() - start
        sleep_s = 10 if elapsed < 60 else 30 if elapsed < 300 else 60
        time.sleep(sleep_s)

    print(f"  Timeout after {max_wait_min} minutes")
    return False


def process_results(client, batch_id: str, grade: str, cur, conn) -> dict:
    """Download and write batch results. Returns stats dict."""
    model = HAIKU_MODEL if grade == "C" else SONNET_MODEL
    pricing = PRICING[model]

    success = 0
    errors = 0
    total_cost = 0.0

    for result in client.messages.batches.results(batch_id):
        wine_id = result.custom_id

        if result.result.type != "succeeded":
            errors += 1
            err_msg = "batch_expired" if result.result.type == "expired" else \
                      (str(result.result.error)[:500] if hasattr(result.result, 'error') else "unknown")
            log_enrichment(cur, wine_id, grade, model, {}, 0, "error", err_msg)
            conn.commit()
            continue

        try:
            msg = result.result.message
            raw = msg.content[0].text.strip()
            if raw.startswith("```"):
                raw = raw.split("\n", 1)[1] if "\n" in raw else raw[3:]
                if raw.endswith("```"):
                    raw = raw[:-3].strip()

            parsed = json.loads(raw)

            # Normalize non-string fields
            for field in ("comparable_wines", "food_pairing"):
                val = parsed.get(field)
                if val and not isinstance(val, str):
                    if isinstance(val, list):
                        parsed[field] = "\n".join(str(x) for x in val)
                    elif isinstance(val, dict):
                        parsed[field] = "\n".join(f"{k} — {v}" for k, v in val.items())

            if grade == "C":
                write_grade_c(cur, wine_id, parsed)
            else:
                write_grade_b(cur, wine_id, parsed)

            usage = msg.usage
            cost = (usage.input_tokens * pricing["input"] / 1_000_000 +
                    usage.output_tokens * pricing["output"] / 1_000_000) * 0.5  # batch discount
            total_cost += cost

            log_enrichment(
                cur, wine_id, grade, model,
                {"input_tokens": usage.input_tokens, "output_tokens": usage.output_tokens},
                cost, "completed"
            )
            conn.commit()
            success += 1

            if success % 250 == 0:
                print(f"  Processed {success}... (${total_cost:.2f})")
                sys.stdout.flush()

        except Exception as e:
            errors += 1
            log_enrichment(cur, wine_id, grade, model, {}, 0, "error", str(e)[:500])
            conn.commit()

    return {"success": success, "errors": errors, "cost": round(total_cost, 4)}


def append_journal(entry: dict):
    """Append run summary to the journal."""
    JOURNAL.parent.mkdir(parents=True, exist_ok=True)
    header = not JOURNAL.exists() or JOURNAL.stat().st_size == 0

    lines = []
    if header:
        lines.append("# Batch Runner Journal\n\n")
        lines.append("Append-only log of batch enrichment runs.\n\n")
        lines.append("| Date | Mode | Grade | Selected | Success | Errors | Cost | Batch ID |\n")
        lines.append("|------|------|-------|----------|---------|--------|------|----------|\n")

    lines.append(
        f"| {entry['date']} "
        f"| {entry['mode']} "
        f"| {entry['grade']} "
        f"| {entry['selected']} "
        f"| {entry['success']} "
        f"| {entry['errors']} "
        f"| ${entry['cost']:.4f} "
        f"| `{entry['batch_id']}` |\n"
    )

    with open(JOURNAL, "a", encoding="utf-8") as f:
        f.writelines(lines)


# ── Main ──────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Cron-friendly batch enrichment runner")
    parser.add_argument("--mode", choices=["new", "refresh", "retry", "promote"], required=True)
    parser.add_argument("--grade", choices=["C", "B"], default="C")
    parser.add_argument("--limit", type=int, default=1000)
    parser.add_argument("--max-wait-min", type=int, default=MAX_WAIT_MINUTES)
    parser.add_argument("--dry-run", action="store_true", help="Select wines but don't submit")
    args = parser.parse_args()

    # Promote mode is always Grade B
    if args.mode == "promote":
        args.grade = "B"

    print(f"\n{'='*60}")
    print(f"  BATCH RUNNER — {args.mode} / Grade {args.grade}")
    print(f"  {datetime.now(timezone.utc).isoformat()}")
    print(f"{'='*60}")

    conn = get_conn()
    cur = conn.cursor()

    # Select wines
    selectors = {
        "new": lambda: select_new_wines(cur, args.grade, args.limit),
        "refresh": lambda: select_refresh_wines(cur, args.grade, args.limit),
        "retry": lambda: select_retry_wines(cur, args.grade, args.limit),
        "promote": lambda: select_promote_wines(cur, args.limit),
    }
    wine_ids = selectors[args.mode]()
    print(f"  Selected {len(wine_ids)} wines")

    if not wine_ids:
        print("  Nothing to do.")
        return 0

    if args.dry_run:
        print(f"  DRY-RUN — would submit {len(wine_ids)} wines")
        return 0

    # Submit batch
    client = anthropic.Anthropic()
    try:
        batch_id = submit_batch(client, wine_ids, args.grade, cur)
    except Exception as e:
        print(f"  SUBMIT FAILED: {e}")
        return 1

    # Poll for completion
    if not poll_batch(client, batch_id, args.max_wait_min):
        print(f"  Batch {batch_id} did not end in {args.max_wait_min} minutes")
        print(f"  Run `batch_api process --batch-id {batch_id} --grade {args.grade}` later")
        append_journal({
            "date": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M"),
            "mode": args.mode,
            "grade": args.grade,
            "selected": len(wine_ids),
            "success": 0,
            "errors": 0,
            "cost": 0,
            "batch_id": batch_id + " (timeout)",
        })
        return 2

    # Process results
    print(f"  Processing results...")
    sys.stdout.flush()
    stats = process_results(client, batch_id, args.grade, cur, conn)

    print(f"\n  {'='*50}")
    print(f"  DONE: {stats['success']} enriched, {stats['errors']} errors, ${stats['cost']:.4f}")
    print(f"  {'='*50}")

    append_journal({
        "date": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M"),
        "mode": args.mode,
        "grade": args.grade,
        "selected": len(wine_ids),
        "success": stats["success"],
        "errors": stats["errors"],
        "cost": stats["cost"],
        "batch_id": batch_id,
    })

    cur.close()
    conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
