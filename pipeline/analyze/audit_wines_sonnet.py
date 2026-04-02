"""
Audit 300 sampled canonical wines using Sonnet for accuracy validation.
Uses the $2 AI common sense budget to check if wine data looks correct.

Checks: producer→country, producer→region, wine→appellation, wine→color, wine_type accuracy.
"""

import os
import json
import random
from dotenv import load_dotenv

load_dotenv()

from pipeline.lib.db import get_supabase
import anthropic


def sample_wines(sb, n=300):
    """Sample n wines stratified by country for diversity."""
    # Get country distribution
    countries = sb.table("wines").select("country_id", count="exact").limit(0).execute()

    # Pull a random sample - Supabase doesn't support ORDER BY RANDOM()
    # So we pull more and sample locally
    all_wines = []
    offset = 0
    batch_size = 1000

    # Pull a broad sample across the table
    # Use multiple random offsets to get diversity
    total_resp = sb.table("wines").select("id", count="exact").limit(0).execute()
    total = total_resp.count
    print(f"Total wines in canonical: {total}")

    # Pick 300 random offsets
    offsets = sorted(random.sample(range(total), min(n * 2, total)))

    wines = []
    for i in range(0, len(offsets), 50):
        batch_offsets = offsets[i:i+50]
        for off in batch_offsets:
            resp = sb.table("wines").select(
                "id, name, color, wine_type, effervescence, "
                "producer_id, country_id, region_id, appellation_id"
            ).range(off, off).execute()
            if resp.data:
                wines.append(resp.data[0])
        if len(wines) >= n:
            break

    wines = wines[:n]
    print(f"Sampled {len(wines)} wines")
    return wines


def sample_wines_rpc(sb, n=300):
    """Sample wines using direct SQL via RPC for efficiency."""
    # Use a simpler approach: pull 300 at fixed intervals
    total_resp = sb.table("wines").select("id", count="exact").limit(0).execute()
    total = total_resp.count
    step = total // n

    wines = []
    for i in range(0, n):
        offset = i * step + random.randint(0, max(step - 1, 0))
        offset = min(offset, total - 1)
        resp = sb.table("wines").select(
            "id, name, color, wine_type, effervescence, "
            "producer:producers(name, country_id, region_id), "
            "country:countries(name), "
            "region:regions(name), "
            "appellation:appellations(name)"
        ).range(offset, offset).execute()
        if resp.data:
            wines.append(resp.data[0])

    print(f"Sampled {len(wines)} wines (step={step})")
    return wines


def sample_wines_bulk(sb, n=300):
    """Sample wines in bulk pulls then random select."""
    # Pull 3000 wines from different parts of the table and randomly pick 300
    total_resp = sb.table("wines").select("id", count="exact").limit(0).execute()
    total = total_resp.count
    print(f"Total canonical wines: {total}")

    pool = []
    # Pull from 6 different regions of the table
    for chunk in range(6):
        offset = chunk * (total // 6) + random.randint(0, 1000)
        resp = sb.table("wines").select(
            "id, name, color, wine_type, effervescence, "
            "producer:producers(name), "
            "country:countries(name), "
            "region:regions!wines_region_id_fkey(name), "
            "appellation:appellations!wines_appellation_id_fkey(name)"
        ).range(offset, offset + 499).execute()
        pool.extend(resp.data or [])

    print(f"Pool size: {len(pool)}")
    sample = random.sample(pool, min(n, len(pool)))
    print(f"Selected {len(sample)} wines for audit")
    return sample


def format_wines_for_audit(wines):
    """Format wine data into a compact audit table."""
    lines = []
    for i, w in enumerate(wines):
        producer = w.get("producer") or {}
        country = w.get("country") or {}
        region = w.get("region") or {}
        appellation = w.get("appellation") or {}

        line = (
            f"{i+1}. {producer.get('name', '?')} | "
            f"{w.get('name', '?')} | "
            f"{country.get('name', 'NULL')} | "
            f"{region.get('name', 'NULL')} | "
            f"{appellation.get('name', 'NULL')} | "
            f"{w.get('color', 'NULL')} | "
            f"{w.get('wine_type', 'NULL')} | "
            f"{w.get('effervescence', 'NULL')}"
        )
        lines.append(line)
    return "\n".join(lines)


def audit_batch(client, batch_text, batch_num, total_batches):
    """Send a batch of wines to Sonnet for accuracy audit."""
    prompt = f"""You are a wine industry expert auditing a wine database for accuracy. Below are wines from batch {batch_num}/{total_batches}.

Format: NUMBER. PRODUCER | WINE_NAME | COUNTRY | REGION | APPELLATION | COLOR | WINE_TYPE | EFFERVESCENCE

Review each wine and flag ONLY clear errors — things that are definitely wrong based on your wine knowledge. For each error, explain what's wrong and what it should be.

Categories of errors to check:
1. COUNTRY: Is the producer in the right country? (e.g., Chateau Margaux should be France, not Italy)
2. REGION: Is the region correct for this producer/appellation? (e.g., Barolo should be in Piedmont, not Tuscany)
3. APPELLATION: Is the appellation correct for this wine/producer?
4. COLOR: Is the color right? (e.g., a Sauternes labeled "red" is wrong)
5. WINE_TYPE: Should this be "table", "fortified", or "sparkling"? (e.g., Champagne should be sparkling, Port should be fortified)
6. EFFERVESCENCE: Is still/sparkling/etc correct?

Do NOT flag:
- NULL values (missing data is expected, not an error)
- Minor regional categorization judgment calls
- Wines you're not sure about

Output format:
- If no errors: "BATCH {batch_num}: ALL CLEAN (N wines reviewed)"
- If errors found:
  BATCH {batch_num}: N ERRORS FOUND (out of M wines)
  [line number]. PRODUCER | WINE: [what's wrong] → [what it should be]

Here are the wines:

{batch_text}"""

    response = client.messages.create(
        model="claude-sonnet-4-20250514",
        max_tokens=4000,
        messages=[{"role": "user", "content": prompt}]
    )

    return response.content[0].text, response.usage


def main():
    sb = get_supabase()
    client = anthropic.Anthropic()

    # Sample 300 wines
    print("Sampling 300 wines from canonical...")
    wines = sample_wines_bulk(sb, 300)

    # Format for audit
    audit_text = format_wines_for_audit(wines)
    lines = audit_text.split("\n")
    print(f"\nFormatted {len(lines)} wines for audit")
    print(f"Sample line: {lines[0]}")

    # Split into batches of 100 (manageable context for Sonnet)
    batch_size = 100
    batches = [lines[i:i+batch_size] for i in range(0, len(lines), batch_size)]
    print(f"\nSending {len(batches)} batches to Sonnet...")

    total_input_tokens = 0
    total_output_tokens = 0
    results = []

    for i, batch in enumerate(batches):
        batch_text = "\n".join(batch)
        print(f"\n--- Batch {i+1}/{len(batches)} ({len(batch)} wines) ---")
        result, usage = audit_batch(client, batch_text, i+1, len(batches))
        results.append(result)
        total_input_tokens += usage.input_tokens
        total_output_tokens += usage.output_tokens
        print(f"Tokens: {usage.input_tokens} in, {usage.output_tokens} out")
        print(result[:500].encode('ascii', errors='replace').decode('ascii'))

    # Cost calculation (Sonnet pricing: $3/M input, $15/M output)
    cost = (total_input_tokens * 3 + total_output_tokens * 15) / 1_000_000
    print(f"\n{'='*60}")
    print(f"AUDIT COMPLETE")
    print(f"Total tokens: {total_input_tokens} input, {total_output_tokens} output")
    print(f"Estimated cost: ${cost:.4f}")
    print(f"{'='*60}")

    # Save full results
    output = {
        "sample_size": len(wines),
        "batches": len(batches),
        "total_input_tokens": total_input_tokens,
        "total_output_tokens": total_output_tokens,
        "estimated_cost_usd": round(cost, 4),
        "results": results,
        "sample_wines": [
            {
                "producer": (w.get("producer") or {}).get("name"),
                "wine": w.get("name"),
                "country": (w.get("country") or {}).get("name"),
                "region": (w.get("region") or {}).get("name"),
                "appellation": (w.get("appellation") or {}).get("name"),
                "color": w.get("color"),
                "wine_type": w.get("wine_type"),
                "effervescence": w.get("effervescence")
            }
            for w in wines
        ]
    }

    out_path = "data/imports/audit_sonnet_300.json"
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    with open(out_path, "w") as f:
        json.dump(output, f, indent=2)
    print(f"\nFull results saved to {out_path}")


if __name__ == "__main__":
    main()
