"""
Enrich appellations with AI-generated insights using Claude Sonnet.
Writes results to the appellation_insights table via Supabase.

Usage:
    python -m pipeline.enrich.appellation_insights
    python -m pipeline.enrich.appellation_insights --force
    python -m pipeline.enrich.appellation_insights --dry-run --limit 10
"""

import sys
import json
import re
import time
import argparse
from pathlib import Path
from datetime import datetime, timedelta, timezone

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import anthropic
from pipeline.lib.db import get_supabase, get_env, fetch_all

# ── Constants ───────────────────────────────────────────────
CONCURRENCY = 3
MAX_GRAPES = 15
MAX_TOKENS = 1500
EXPECTED_KEYS = [
    "ai_overview", "ai_climate_profile", "ai_soil_profile",
    "ai_signature_style", "ai_key_grapes", "ai_aging_generalization",
    "confidence",
]
BANNED_WORDS = [
    "prestigious", "world-class", "exceptional", "unparalleled",
    "legendary", "iconic", "finest", "renowned",
]

SYSTEM_PROMPT = """You are a wine expert writing for Loam, a wine intelligence platform. You write with genuine love for wine and deep knowledge — the way a great winemaker talks about their craft. Be specific, grounded, and real. Never use marketing fluff or generic wine-magazine language.

When you write about an appellation, write like someone who has walked the vineyards and tasted the wines. Use specific details — soil types, elevations, microclimates, grape varieties.

HANDLING UNCERTAINTY: If you don't know specific details about a lesser-known appellation:
- Write shorter entries (1 sentence is fine).
- State the general climate zone and likely soil family rather than guessing specifics.
- Set confidence to 0.5 or lower.
- A honest one-sentence entry is always better than a padded three-sentence guess.

Return a JSON object with EXACTLY these keys (no others):

{
  "ai_overview": "...",
  "ai_climate_profile": "...",
  "ai_soil_profile": "...",
  "ai_signature_style": "...",
  "ai_key_grapes": "...",
  "ai_aging_generalization": "...",
  "confidence": 0.9
}

Field guidelines:
- ai_overview (2-4 sentences): What this place is and why it matters. Lead with what makes it distinctive.
- ai_climate_profile (2-3 sentences): The climate that shapes the wines. Be specific about the weather patterns that matter for grape growing.
- ai_soil_profile (2-3 sentences): What's in the ground and why it matters. Name actual soil types and parent rock.
- ai_signature_style (2-3 sentences): What wines from here taste and feel like. Sensory language rooted in the place.
- ai_key_grapes (1-2 sentences): The varieties that define this appellation and why they work here.
- ai_aging_generalization (1-2 sentences): How wines from here typically age.
- confidence: Your honest self-assessment. 0.9 = major appellation you know deeply. 0.7 = you know it moderately well. 0.5 = you know basics only. 0.3 = you're mostly guessing.

CRITICAL RULES:
- Do NOT reference database counts, wine counts, or producer counts.
- You may mention a producer by name ONLY if they genuinely defined or shaped the appellation (e.g., a pioneer who put the region on the map). Keep it to 1-2 names max, woven naturally into the narrative — never a list.
- No marketing language. No "prestigious", "world-class", "exceptional", "unparalleled", "legendary", "iconic", "finest", "renowned".
- No markdown code fences. Start your response with the opening brace.
- Every field must have a value — use shorter honest text for fields you're less sure about."""


def call_sonnet(client: anthropic.Anthropic, user_msg: str, max_tokens: int = MAX_TOKENS) -> dict:
    """Call Claude Sonnet with retry logic."""
    max_retries = 3
    for attempt in range(1, max_retries + 1):
        try:
            response = client.messages.create(
                model="claude-sonnet-4-20250514",
                max_tokens=max_tokens,
                messages=[
                    {"role": "user", "content": SYSTEM_PROMPT + "\n\n" + user_msg},
                    {"role": "assistant", "content": "{"},
                ],
            )
            return {
                "content": response.content[0].text,
                "stop_reason": response.stop_reason,
                "usage": {
                    "input_tokens": response.usage.input_tokens,
                    "output_tokens": response.usage.output_tokens,
                },
            }
        except anthropic.RateLimitError:
            wait = min(2 ** attempt * 2, 30)
            print(f"  Rate limited, waiting {wait}s...")
            time.sleep(wait)
        except Exception as e:
            if attempt == max_retries:
                raise
            wait = 2 ** attempt
            print(f"  Attempt {attempt} failed: {e}, retrying in {wait}s...")
            time.sleep(wait)
    return {}


def validate_response(parsed: dict) -> list[str]:
    """Validate the parsed JSON response."""
    warnings = []
    missing = [k for k in EXPECTED_KEYS if k not in parsed]
    if missing:
        warnings.append(f"Missing keys: {', '.join(missing)}")

    extra = [k for k in parsed if k not in EXPECTED_KEYS]
    if extra:
        warnings.append(f"Unexpected keys: {', '.join(extra)}")

    conf = parsed.get("confidence")
    if not isinstance(conf, (int, float)) or conf < 0 or conf > 1:
        warnings.append(f"Bad confidence: {conf}")

    for key in EXPECTED_KEYS:
        if key == "confidence":
            continue
        val = parsed.get(key)
        if isinstance(val, str) and val.strip() == "":
            warnings.append(f"Empty field: {key}")

    all_text = " ".join(str(parsed.get(k, "")) for k in EXPECTED_KEYS if k != "confidence").lower()
    found = [w for w in BANNED_WORDS if w in all_text]
    if found:
        warnings.append(f"Banned words: {', '.join(found)}")

    return warnings


def fetch_appellations(sb, force: bool) -> list[dict]:
    """Fetch appellations to process."""
    result = sb.table("appellations").select(
        "id, name, designation_type, region:regions(name, country:countries(name))"
    ).order("name").execute()
    appellations = result.data

    if len(appellations) >= 1000:
        appellations = fetch_all("appellations", "id, name, designation_type, region:regions(name, country:countries(name))")

    enriched_ids = set()
    if not force:
        existing = sb.table("appellation_insights").select("appellation_id").execute()
        enriched_ids = {e["appellation_id"] for e in existing.data}

    return [a for a in appellations if a["id"] not in enriched_ids]


def fetch_grape_map(sb, appellation_ids: list[str]) -> dict[str, list[str]]:
    """Fetch top grapes per appellation."""
    grapes_by_app: dict[str, list[str]] = {}
    batch_size = 50

    for i in range(0, len(appellation_ids), batch_size):
        batch_ids = appellation_ids[i:i + batch_size]

        try:
            result = (
                sb.table("wine_grapes")
                .select("wine:wines!inner(appellation_id), grape:grapes(name)")
                .in_("wine.appellation_id", batch_ids)
                .limit(10000)
                .execute()
            )
            data = result.data or []
        except Exception as e:
            print(f"  Batch grape fetch failed: {e}")
            continue

        counts: dict[str, dict[str, int]] = {}
        for row in data:
            app_id = (row.get("wine") or {}).get("appellation_id")
            grape_name = (row.get("grape") or {}).get("name")
            if not app_id or not grape_name:
                continue
            if app_id not in counts:
                counts[app_id] = {}
            counts[app_id][grape_name] = counts[app_id].get(grape_name, 0) + 1

        for app_id, gc in counts.items():
            top = sorted(gc.items(), key=lambda x: x[1], reverse=True)[:MAX_GRAPES]
            grapes_by_app[app_id] = [name for name, _ in top]

    return grapes_by_app


def process_appellation(client: anthropic.Anthropic, app: dict, grapes: list[str]) -> dict:
    """Process a single appellation through Claude."""
    country = (app.get("region") or {}).get("country", {}).get("name", "Unknown")
    region = (app.get("region") or {}).get("name", "Unknown")

    user_msg = f"""Write appellation insights for:

Name: {app['name']}
Designation: {app.get('designation_type') or 'Unknown'}
Country: {country}
Region: {region}"""
    if grapes:
        user_msg += f"\nKey grapes: {', '.join(grapes[:MAX_GRAPES])}"

    result = call_sonnet(client, user_msg, MAX_TOKENS)
    if not result:
        return {"error": "API call returned empty"}

    if result.get("stop_reason") == "max_tokens":
        return {"error": "TRUNCATED", "tokens": result["usage"]}

    text = "{" + result["content"].strip()
    text = re.sub(r"```(?:json)?\s*", "", text)
    text = re.sub(r"\s*```", "", text)

    try:
        parsed = json.loads(text)
        return {"parsed": parsed, "warnings": validate_response(parsed), "tokens": result["usage"]}
    except json.JSONDecodeError as e:
        return {"error": f"JSON parse failed: {e}", "raw": text[:200], "tokens": result["usage"]}


def write_insight(sb, appellation_id: str, parsed: dict):
    """Upsert an appellation insight row."""
    now = datetime.now(timezone.utc)
    row = {
        "appellation_id": appellation_id,
        "ai_overview": parsed["ai_overview"],
        "ai_climate_profile": parsed["ai_climate_profile"],
        "ai_soil_profile": parsed["ai_soil_profile"],
        "ai_signature_style": parsed["ai_signature_style"],
        "ai_key_grapes": parsed["ai_key_grapes"],
        "ai_aging_generalization": parsed["ai_aging_generalization"],
        "confidence": parsed["confidence"],
        "enriched_at": now.isoformat(),
        "refresh_after": (now + timedelta(days=90)).isoformat(),
    }
    sb.table("appellation_insights").upsert(row, on_conflict="appellation_id").execute()


def main():
    parser = argparse.ArgumentParser(description="Enrich appellations with AI insights")
    parser.add_argument("--force", action="store_true", help="Re-run all (overwrite existing)")
    parser.add_argument("--dry-run", action="store_true", help="Preview without writing")
    parser.add_argument("--limit", type=int, default=None, help="Process only N appellations")
    args = parser.parse_args()

    print("Appellation Insights Enrichment Pipeline")
    print(f"   Model: Claude Sonnet | Concurrency: {CONCURRENCY}")
    print(f"   Force: {args.force} | Dry run: {args.dry_run}" +
          (f" | Limit: {args.limit}" if args.limit else ""))
    print()

    sb = get_supabase()
    client = anthropic.Anthropic(api_key=get_env("ANTHROPIC_API_KEY"))

    to_process = fetch_appellations(sb, args.force)
    total = min(len(to_process), args.limit) if args.limit else len(to_process)
    appellations = to_process[:total]

    print(f"{total} appellations to process ({len(to_process)} unenriched total)\n")

    if total == 0:
        print("Nothing to do!")
        return

    print("Fetching grape data...")
    grapes_by_app = fetch_grape_map(sb, [a["id"] for a in appellations])
    print(f"   {len(grapes_by_app)} appellations have grape data\n")

    processed = 0
    succeeded = 0
    warning_count = 0
    failed = 0
    total_input_tokens = 0
    total_output_tokens = 0
    errors = []
    start_time = time.time()

    for app in appellations:
        processed += 1
        country = (app.get("region") or {}).get("country", {}).get("name", "?")
        label = f"{app['name']} ({country})"
        grapes = grapes_by_app.get(app["id"], [])

        r = process_appellation(client, app, grapes)

        if r.get("tokens"):
            total_input_tokens += r["tokens"]["input_tokens"]
            total_output_tokens += r["tokens"]["output_tokens"]

        if r.get("error"):
            failed += 1
            errors.append({"name": label, "error": r["error"]})
            print(f"  FAIL {processed}/{total} {label} -- {r['error']}")
            continue

        if r.get("warnings"):
            warning_count += 1
            print(f"  WARN {processed}/{total} {label} (conf: {r['parsed']['confidence']}) -- {'; '.join(r['warnings'])}")
        else:
            print(f"  OK   {processed}/{total} {label} (conf: {r['parsed']['confidence']})")

        if not args.dry_run:
            try:
                write_insight(sb, app["id"], r["parsed"])
                succeeded += 1
            except Exception as e:
                failed += 1
                errors.append({"name": label, "error": str(e)})
                print(f"     DB write failed: {e}")
        else:
            succeeded += 1

        if processed % 30 == 0 and processed < total:
            elapsed = time.time() - start_time
            rate = processed / elapsed if elapsed > 0 else 1
            remaining = int((total - processed) / rate)
            print(f"\n  -- {processed}/{total} done | {succeeded} ok, {failed} failed | ~{remaining}s remaining --\n")

    elapsed = time.time() - start_time
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f"  Total processed: {processed}")
    print(f"  Succeeded:       {succeeded}")
    print(f"  Warnings:        {warning_count}")
    print(f"  Failed:          {failed}")
    print(f"  Tokens:          {total_input_tokens:,} in / {total_output_tokens:,} out")
    cost = (total_input_tokens * 3 + total_output_tokens * 15) / 1_000_000
    print(f"  Est. cost:       ${cost:.2f}")
    print(f"  Time:            {elapsed:.1f}s")
    if args.dry_run:
        print("  DRY RUN -- nothing written to database")

    if errors:
        print(f"\nERRORS ({len(errors)}):")
        for e in errors:
            print(f"  - {e['name']}: {e['error']}")


if __name__ == "__main__":
    main()
