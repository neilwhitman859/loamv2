"""
Enrich grapes with AI-generated insights using Claude Sonnet.
Writes results to the grape_insights table via Supabase.

Usage:
    python -m pipeline.enrich.grape_insights
    python -m pipeline.enrich.grape_insights --force
    python -m pipeline.enrich.grape_insights --dry-run --limit 10
"""

import sys
import json
import re
import time
import argparse
import asyncio
from pathlib import Path
from datetime import datetime, timedelta, timezone

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import anthropic
from pipeline.lib.db import get_supabase, get_env, fetch_all

# ── Constants ───────────────────────────────────────────────
CONCURRENCY = 3
MAX_TOKENS = 1500
TOP_APPELLATIONS = 10
TOP_COUNTRIES = 8
EXPECTED_KEYS = [
    "ai_overview", "ai_flavor_profile", "ai_growing_conditions",
    "ai_food_pairing", "ai_regions_of_note", "ai_aging_characteristics",
    "confidence",
]
BANNED_WORDS = [
    "prestigious", "world-class", "exceptional", "unparalleled",
    "legendary", "iconic", "finest", "renowned",
]

SYSTEM_PROMPT = """You are a wine expert writing for Loam, a wine intelligence platform. You write with genuine love for wine and deep knowledge — the way a great winemaker talks about their craft. Be specific, grounded, and real. Never use marketing fluff or generic wine-magazine language.

When you write about a grape variety, write like someone who has grown it, vinified it, and tasted it across many regions. Use specific sensory details, real place names, and honest assessments of how terroir shapes expression.

HANDLING UNCERTAINTY: If you don't know specific details about an obscure grape:
- Write shorter entries (1-2 sentences is fine).
- State the general flavor family and likely growing profile rather than guessing specifics.
- Set confidence to 0.5 or lower.
- An honest short entry is always better than a padded guess.

Return a JSON object with EXACTLY these keys (no others):

{
  "ai_overview": "...",
  "ai_flavor_profile": "...",
  "ai_growing_conditions": "...",
  "ai_food_pairing": "...",
  "ai_regions_of_note": "...",
  "ai_aging_characteristics": "...",
  "confidence": 0.9
}

Field guidelines:
- ai_overview (2-4 sentences): What this grape is — its origins, identity, and why it matters. What should someone know first?
- ai_flavor_profile (2-4 sentences): Aromas, flavors, texture, structure. How does this grape express itself in the glass? Be specific about tannin, acid, body, and aromatic character. Note how expression shifts across climates (cool-climate vs warm-climate styles) where relevant.
- ai_growing_conditions (2-3 sentences): What this grape needs in the vineyard. Climate preferences, vigor, ripening behavior, disease susceptibility. What makes a site good or bad for this variety?
- ai_food_pairing (3-5 sentences): What to eat with wines from this grape. Follow these rules strictly:
  * Start with classic/traditional pairings — they exist for a reason.
  * Name specific dishes and cuisines (Thai, Mexican, Korean, Southern US, Japanese, etc.).
  * Cover the full range — a Tuesday night meal AND a Saturday dinner where it fits.
  * Explain the flavor logic briefly (why the pairing works: acid cuts fat, tannin matches protein, etc.).
  * No sommelier theater — no "pairs beautifully with a delicate..." Just name the food.
  * No generic cop-outs like "pairs well with grilled meats and seafood."
- ai_regions_of_note (2-4 sentences): Where this grape shines and why. How does terroir shape its expression? Contrast different regional styles where relevant (e.g., Burgundy Pinot vs Oregon Pinot vs Central Otago Pinot). These should connect growing conditions to the flavors they produce.
- ai_aging_characteristics (1-3 sentences): How wines from this grape evolve over time. What develops, what fades? What's the typical drinking window range across quality levels?
- confidence: Your honest self-assessment. 0.9+ = major international grape you know deeply. 0.7-0.8 = well-known regional grape. 0.5-0.6 = you know basics. 0.3-0.4 = mostly guessing.

CRITICAL RULES:
- Do NOT reference database counts, wine counts, or producer counts from the context data.
- The context data (appellations, countries) helps ground your response — use it to inform which regions you discuss, but don't quote the numbers.
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
        except anthropic.RateLimitError as e:
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


def fetch_grapes(sb, force: bool) -> tuple[list[dict], int]:
    """Fetch grapes to process."""
    grapes = (
        sb.table("grapes")
        .select("id, name, color, origin_country:countries(name)")
        .is_("deleted_at", "null")
        .order("name")
        .execute()
    )
    all_grapes = grapes.data

    # Paginate if needed
    if len(all_grapes) == 1000:
        all_grapes = fetch_all("grapes", "id, name, color, origin_country:countries(name)")

    enriched_ids = set()
    if not force:
        existing = sb.table("grape_insights").select("grape_id").execute()
        enriched_ids = {e["grape_id"] for e in existing.data}

    to_process = [g for g in all_grapes if g["id"] not in enriched_ids]
    return to_process, len(all_grapes)


def fetch_grape_context(sb, grape_ids: list[str]) -> dict:
    """Fetch top appellations and countries per grape from wine_grapes."""
    context_map = {}
    batch_size = 50

    for i in range(0, len(grape_ids), batch_size):
        batch_ids = grape_ids[i:i + batch_size]

        try:
            result = (
                sb.table("wine_grapes")
                .select("grape_id, wine:wines!inner(appellation:appellations(name), country:countries(name))")
                .in_("grape_id", batch_ids)
                .limit(10000)
                .execute()
            )
            data = result.data or []
        except Exception as e:
            print(f"  Batch context fetch failed: {e}")
            for gid in batch_ids:
                if gid not in context_map:
                    context_map[gid] = {"appellations": [], "countries": []}
            continue

        # Tally appellations and countries per grape
        app_counts: dict[str, dict[str, dict]] = {}
        country_counts: dict[str, dict[str, int]] = {}

        for row in data:
            grape_id = row.get("grape_id")
            app_name = (row.get("wine") or {}).get("appellation", {})
            if app_name:
                app_name = app_name.get("name")
            country_name = (row.get("wine") or {}).get("country", {})
            if country_name:
                country_name = country_name.get("name")

            if grape_id and app_name:
                if grape_id not in app_counts:
                    app_counts[grape_id] = {}
                ac = app_counts[grape_id]
                if app_name not in ac:
                    ac[app_name] = {"count": 0, "country": country_name or "Unknown"}
                ac[app_name]["count"] += 1

            if grape_id and country_name:
                if grape_id not in country_counts:
                    country_counts[grape_id] = {}
                cc = country_counts[grape_id]
                cc[country_name] = cc.get(country_name, 0) + 1

        for gid in batch_ids:
            ac = app_counts.get(gid, {})
            cc = country_counts.get(gid, {})

            top_apps = sorted(ac.items(), key=lambda x: x[1]["count"], reverse=True)[:TOP_APPELLATIONS]
            top_apps = [f"{name} ({info['country']})" for name, info in top_apps]

            top_countries = sorted(cc.items(), key=lambda x: x[1], reverse=True)[:TOP_COUNTRIES]
            top_countries = [name for name, _ in top_countries]

            context_map[gid] = {"appellations": top_apps, "countries": top_countries}

    return context_map


def process_grape(client: anthropic.Anthropic, grape: dict, context: dict) -> dict:
    """Process a single grape through Claude."""
    origin_country = None
    if grape.get("origin_country"):
        origin_country = grape["origin_country"].get("name")

    user_msg = f"Write grape insights for:\n\nGrape: {grape['name']}\nColor: {grape.get('color') or 'Unknown'}"
    if origin_country:
        user_msg += f"\nOrigin country: {origin_country}"
    if context["appellations"]:
        user_msg += f"\nTop appellations in our database: {', '.join(context['appellations'])}"
    if context["countries"]:
        user_msg += f"\nTop countries: {', '.join(context['countries'])}"

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
        warnings = validate_response(parsed)
        return {"parsed": parsed, "warnings": warnings, "tokens": result["usage"]}
    except json.JSONDecodeError as e:
        return {"error": f"JSON parse failed: {e}", "raw": text[:200], "tokens": result["usage"]}


def write_insight(sb, grape_id: str, parsed: dict):
    """Upsert a grape insight row."""
    now = datetime.now(timezone.utc)
    row = {
        "grape_id": grape_id,
        "ai_overview": parsed["ai_overview"],
        "ai_flavor_profile": parsed["ai_flavor_profile"],
        "ai_growing_conditions": parsed["ai_growing_conditions"],
        "ai_food_pairing": parsed["ai_food_pairing"],
        "ai_regions_of_note": parsed["ai_regions_of_note"],
        "ai_aging_characteristics": parsed["ai_aging_characteristics"],
        "confidence": parsed["confidence"],
        "enriched_at": now.isoformat(),
        "refresh_after": (now + timedelta(days=90)).isoformat(),
    }
    sb.table("grape_insights").upsert(row, on_conflict="grape_id").execute()


def main():
    parser = argparse.ArgumentParser(description="Enrich grapes with AI insights")
    parser.add_argument("--force", action="store_true", help="Re-run all (overwrite existing)")
    parser.add_argument("--dry-run", action="store_true", help="Preview without writing")
    parser.add_argument("--limit", type=int, default=None, help="Process only N grapes")
    args = parser.parse_args()

    print("Grape Insights Enrichment Pipeline")
    print(f"   Model: Claude Sonnet | Concurrency: {CONCURRENCY}")
    print(f"   Force: {args.force} | Dry run: {args.dry_run}" +
          (f" | Limit: {args.limit}" if args.limit else ""))
    print()

    sb = get_supabase()
    client = anthropic.Anthropic(api_key=get_env("ANTHROPIC_API_KEY"))

    to_process, total_grapes = fetch_grapes(sb, args.force)
    total = min(len(to_process), args.limit) if args.limit else len(to_process)
    grapes = to_process[:total]

    print(f"{total} grapes to process ({total_grapes} total, {len(to_process)} unenriched)\n")

    if total == 0:
        print("Nothing to do!")
        return

    # Fetch context
    print("Fetching grape context data (appellations, countries)...")
    context_map = fetch_grape_context(sb, [g["id"] for g in grapes])
    with_context = sum(1 for c in context_map.values() if c["appellations"])
    print(f"   {with_context}/{len(grapes)} grapes have appellation context\n")

    # Process
    processed = 0
    succeeded = 0
    warning_count = 0
    failed = 0
    total_input_tokens = 0
    total_output_tokens = 0
    errors = []
    start_time = time.time()

    for grape in grapes:
        processed += 1
        label = f"{grape['name']} ({grape.get('color') or '?'})"
        context = context_map.get(grape["id"], {"appellations": [], "countries": []})

        r = process_grape(client, grape, context)

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
                write_insight(sb, grape["id"], r["parsed"])
                succeeded += 1
            except Exception as e:
                failed += 1
                errors.append({"name": label, "error": str(e)})
                print(f"     DB write failed: {e}")
        else:
            succeeded += 1

        # Progress every 30
        if processed % 30 == 0 and processed < total:
            elapsed = time.time() - start_time
            rate = processed / elapsed if elapsed > 0 else 1
            remaining = int((total - processed) / rate)
            print(f"\n  -- {processed}/{total} done | {succeeded} ok, {failed} failed | ~{remaining}s remaining --\n")

    # Summary
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
