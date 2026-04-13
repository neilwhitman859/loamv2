"""
Haiku Country Inference

Uses Haiku to infer country_id for wines that have no country_id assigned.
Sends batches of 40 wines to Haiku with wine name + producer name.
Only writes high-confidence answers (Haiku must return an ISO code).

Usage:
    python -m pipeline.analyze.haiku_country_infer [--dry-run] [--limit 500]
"""

import argparse
import json
import sys
import time
from pathlib import Path

import anthropic
import psycopg2
import psycopg2.extras

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from pipeline.lib.db import get_conn
from pipeline.lib.models import HAIKU_MODEL

# Country ISO → UUID mapping (populated from DB at runtime)
COUNTRY_MAP: dict[str, str] = {}  # iso_code.upper() → id

BATCH_SIZE = 40

SYSTEM_PROMPT = """You are a wine data analyst. Given a list of wine entries (wine name + producer name),
identify the country of origin for each wine. Return ONLY a JSON object mapping entry index to ISO 3166-1 alpha-2
country code (e.g. "FR", "IT", "US", "ES", "DE", "PT", "AR", "AU", "NZ", "ZA", "CL", "AT", "HU", "GR", "RO", "HR", "SI").

Rules:
- Only return a mapping for wines you are CONFIDENT about (90%+ certainty)
- Omit entries where the name is too generic or ambiguous to be certain
- Do not guess — if uncertain, omit the index
- Return valid JSON only, no explanation

Example output: {"0": "FR", "2": "IT", "5": "US"}"""


def load_country_map(conn) -> dict[str, str]:
    """Load ISO code → UUID mapping from countries table."""
    with conn.cursor() as cur:
        cur.execute("SELECT id, iso_code FROM countries WHERE iso_code IS NOT NULL")
        return {row[1].upper(): row[0] for row in cur.fetchall()}


def fetch_wines_without_country(conn, limit: int) -> list[dict]:
    """Fetch wines without country_id, with producer name."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT w.id, w.name, p.name as producer_name
            FROM wines w
            JOIN producers p ON p.id = w.producer_id
            WHERE w.deleted_at IS NULL AND w.country_id IS NULL
            ORDER BY w.id
            LIMIT %s
        """, (limit,))
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]


def infer_countries_batch(client: anthropic.Anthropic, batch: list[dict]) -> dict[int, str]:
    """Send a batch to Haiku and return {index: iso_code} for confident answers."""
    entries = "\n".join(
        f'{i}: wine="{item["name"]}" producer="{item["producer_name"]}"'
        for i, item in enumerate(batch)
    )

    message = client.messages.create(
        model=HAIKU_MODEL,
        max_tokens=512,
        messages=[
            {"role": "user", "content": f"Identify country of origin for these wines:\n\n{entries}"}
        ],
        system=SYSTEM_PROMPT,
    )

    text = message.content[0].text.strip()

    # Extract JSON from response
    try:
        # Sometimes Haiku wraps in ```json ... ```
        if "```" in text:
            start = text.find("{")
            end = text.rfind("}") + 1
            text = text[start:end]
        result = json.loads(text)
        return {int(k): v.upper() for k, v in result.items() if isinstance(v, str)}
    except (json.JSONDecodeError, ValueError) as e:
        print(f"  JSON parse error: {e} | response: {text[:200]}")
        return {}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true", help="Don't write to DB")
    parser.add_argument("--limit", type=int, default=15000, help="Max wines to process")
    args = parser.parse_args()

    client = anthropic.Anthropic()
    conn = get_conn()

    global COUNTRY_MAP
    COUNTRY_MAP = load_country_map(conn)
    print(f"[haiku_country_infer] Loaded {len(COUNTRY_MAP)} countries")

    wines = fetch_wines_without_country(conn, args.limit)
    print(f"[haiku_country_infer] {len(wines)} wines without country_id")

    if not wines:
        print("[haiku_country_infer] Nothing to do.")
        conn.close()
        return

    total_assigned = 0
    total_skipped = 0
    total_in_tokens = 0
    total_out_tokens = 0
    country_counts: dict[str, int] = {}

    for batch_start in range(0, len(wines), BATCH_SIZE):
        batch = wines[batch_start:batch_start + BATCH_SIZE]
        batch_num = batch_start // BATCH_SIZE + 1
        total_batches = (len(wines) + BATCH_SIZE - 1) // BATCH_SIZE

        print(f"  Batch {batch_num}/{total_batches} ({batch_start}-{batch_start+len(batch)})...", end=" ", flush=True)

        try:
            # Use messages.create directly and track tokens
            entries = "\n".join(
                f'{i}: wine="{item["name"]}" producer="{item["producer_name"]}"'
                for i, item in enumerate(batch)
            )
            message = client.messages.create(
                model=HAIKU_MODEL,
                max_tokens=512,
                messages=[
                    {"role": "user", "content": f"Identify country of origin for these wines:\n\n{entries}"}
                ],
                system=SYSTEM_PROMPT,
            )
            total_in_tokens += message.usage.input_tokens
            total_out_tokens += message.usage.output_tokens

            text = message.content[0].text.strip()
            try:
                if "```" in text:
                    start = text.find("{")
                    end = text.rfind("}") + 1
                    text = text[start:end]
                inferred = json.loads(text)
                inferred = {int(k): v.upper() for k, v in inferred.items() if isinstance(v, str)}
            except (json.JSONDecodeError, ValueError) as e:
                print(f"JSON error: {e}")
                inferred = {}

        except Exception as e:
            print(f"API error: {e}")
            time.sleep(2)
            continue

        # Map inferred ISO codes to UUIDs and update
        updates = []
        for idx, iso_code in inferred.items():
            if idx >= len(batch):
                continue
            country_uuid = COUNTRY_MAP.get(iso_code)
            if not country_uuid:
                print(f"\n  Unknown ISO code: {iso_code} (skipping)")
                total_skipped += 1
                continue
            wine = batch[idx]
            updates.append((country_uuid, wine["id"]))
            country_counts[iso_code] = country_counts.get(iso_code, 0) + 1

        print(f"{len(updates)} assigned, {len(batch) - len(updates)} skipped")

        if updates and not args.dry_run:
            with conn.cursor() as cur:
                psycopg2.extras.execute_batch(
                    cur,
                    "UPDATE wines SET country_id = %s WHERE id = %s",
                    updates,
                    page_size=100
                )
            conn.commit()
            total_assigned += len(updates)
        elif updates and args.dry_run:
            total_assigned += len(updates)  # Count for dry-run reporting

    # Bulk validation stamp for newly assigned wines
    if not args.dry_run and total_assigned > 0:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE wines SET last_validated_at = NOW()
                WHERE deleted_at IS NULL
                  AND country_id IS NOT NULL
                  AND last_validated_at IS NULL
            """)
        conn.commit()
        print(f"\n[haiku_country_infer] Stamped newly validated wines")

    conn.close()

    cost_in = total_in_tokens / 1_000_000 * 0.25
    cost_out = total_out_tokens / 1_000_000 * 1.25
    total_cost = cost_in + cost_out

    print(f"\n[haiku_country_infer] COMPLETE")
    print(f"  Wines assigned:  {total_assigned}")
    print(f"  Skipped:         {total_skipped + (len(wines) - total_assigned - total_skipped)}")
    print(f"  Tokens:          {total_in_tokens} in / {total_out_tokens} out")
    print(f"  Cost:            ${total_cost:.4f}")
    print(f"  Top countries:   {sorted(country_counts.items(), key=lambda x: -x[1])[:10]}")


if __name__ == "__main__":
    main()
