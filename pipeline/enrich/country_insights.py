"""
Enrich countries with AI-generated insights using Claude Sonnet.
Writes results to the country_insights table via Supabase.

Usage:
    python -m pipeline.enrich.country_insights
    python -m pipeline.enrich.country_insights --force
    python -m pipeline.enrich.country_insights --dry-run --limit 10
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
MAX_TOKENS = 1500
EXPECTED_KEYS = [
    "ai_overview", "ai_wine_history", "ai_key_regions",
    "ai_signature_styles", "ai_regulatory_overview", "confidence",
]
BANNED_WORDS = [
    "prestigious", "world-class", "exceptional", "unparalleled",
    "legendary", "iconic", "finest", "renowned",
]

SYSTEM_PROMPT = """You are a wine expert writing for Loam, a wine intelligence platform. You write with genuine love for wine and deep knowledge — the way a great winemaker talks about their craft. Be specific, grounded, and real. Never use marketing fluff or generic wine-magazine language.

When you write about a wine country, write like someone who has traveled extensively through its regions, understands its traditions and modern evolution, and can speak to both the classics and the emerging stories.

HANDLING UNCERTAINTY: If you don't know specific details about a lesser-known wine country:
- Write shorter entries (1-2 sentences is fine).
- State what's generally known rather than guessing specifics.
- Set confidence to 0.5 or lower.
- An honest short entry is always better than a padded guess.

Return a JSON object with EXACTLY these keys (no others):

{
  "ai_overview": "...",
  "ai_wine_history": "...",
  "ai_key_regions": "...",
  "ai_signature_styles": "...",
  "ai_regulatory_overview": "...",
  "confidence": 0.9
}

Field guidelines:
- ai_overview (2-4 sentences): This country's wine identity — what defines it, what role it plays in the global wine landscape.
- ai_wine_history (2-4 sentences): The arc of wine in this country. Ancient roots, key turning points, modern evolution. What shaped it?
- ai_key_regions (3-5 sentences): The major wine regions and what distinguishes each. Focus on the regions that matter most and how they differ from each other.
- ai_signature_styles (2-4 sentences): The wines this country is known for. What are the flagship styles? What should someone expect when they pick up a bottle?
- ai_regulatory_overview (2-3 sentences): How wine is classified and labeled in this country. What system(s) govern quality tiers, geographic designations, and labeling rules?
- confidence: Your honest self-assessment. 0.9+ = major wine country you know deeply. 0.7-0.8 = well-known wine country. 0.5-0.6 = you know basics. 0.3-0.4 = mostly guessing.

CRITICAL RULES:
- Do NOT reference database counts, wine counts, or producer counts from the context data.
- The context data helps ground your response — use it to inform which regions and grapes you discuss, but don't quote numbers.
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


def fetch_countries(sb, force: bool) -> list[dict]:
    """Fetch countries to process."""
    result = (
        sb.table("countries")
        .select("id, name")
        .is_("deleted_at", "null")
        .order("name")
        .execute()
    )
    countries = result.data

    enriched_ids = set()
    if not force:
        existing = sb.table("country_insights").select("country_id").execute()
        enriched_ids = {e["country_id"] for e in existing.data}

    return [c for c in countries if c["id"] not in enriched_ids]


def fetch_country_context(sb, country_ids: list[str]) -> dict:
    """Fetch regions, appellations, and top grapes per country."""
    context_map = {}

    for cid in country_ids:
        # Regions (non-catch-all)
        try:
            region_result = (
                sb.table("regions")
                .select("name, slug")
                .eq("country_id", cid)
                .is_("deleted_at", "null")
                .order("name")
                .execute()
            )
            regions = [r["name"] for r in (region_result.data or [])
                       if not (r.get("slug") or "").endswith("-country")]
        except Exception:
            regions = []

        # Top appellations
        try:
            app_result = (
                sb.table("appellations")
                .select("name")
                .eq("country_id", cid)
                .limit(10)
                .execute()
            )
            appellations = [a["name"] for a in (app_result.data or [])]
        except Exception:
            appellations = []

        # Top grapes
        try:
            grape_result = (
                sb.table("wine_grapes")
                .select("grape:grapes(name), wine:wines!inner(country_id)")
                .eq("wine.country_id", cid)
                .limit(5000)
                .execute()
            )
            grape_counts: dict[str, int] = {}
            for row in (grape_result.data or []):
                name = (row.get("grape") or {}).get("name")
                if name:
                    grape_counts[name] = grape_counts.get(name, 0) + 1
            top_grapes = sorted(grape_counts.items(), key=lambda x: x[1], reverse=True)[:8]
            top_grapes = [name for name, _ in top_grapes]
        except Exception:
            top_grapes = []

        context_map[cid] = {"regions": regions, "appellations": appellations, "grapes": top_grapes}

    return context_map


def process_country(client: anthropic.Anthropic, country: dict, context: dict) -> dict:
    """Process a single country through Claude."""
    user_msg = f"Write country insights for:\n\nCountry: {country['name']}"
    if context["regions"]:
        user_msg += f"\nMajor regions: {', '.join(context['regions'])}"
    if context["appellations"]:
        user_msg += f"\nTop appellations: {', '.join(context['appellations'])}"
    if context["grapes"]:
        user_msg += f"\nTop grapes: {', '.join(context['grapes'])}"

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


def write_insight(sb, country_id: str, parsed: dict):
    """Upsert a country insight row."""
    now = datetime.now(timezone.utc)
    row = {
        "country_id": country_id,
        "ai_overview": parsed["ai_overview"],
        "ai_wine_history": parsed["ai_wine_history"],
        "ai_key_regions": parsed["ai_key_regions"],
        "ai_signature_styles": parsed["ai_signature_styles"],
        "ai_regulatory_overview": parsed["ai_regulatory_overview"],
        "confidence": parsed["confidence"],
        "enriched_at": now.isoformat(),
        "refresh_after": (now + timedelta(days=90)).isoformat(),
    }
    sb.table("country_insights").upsert(row, on_conflict="country_id").execute()


def main():
    parser = argparse.ArgumentParser(description="Enrich countries with AI insights")
    parser.add_argument("--force", action="store_true", help="Re-run all (overwrite existing)")
    parser.add_argument("--dry-run", action="store_true", help="Preview without writing")
    parser.add_argument("--limit", type=int, default=None, help="Process only N countries")
    args = parser.parse_args()

    print("Country Insights Enrichment Pipeline")
    print(f"   Model: Claude Sonnet | Concurrency: {CONCURRENCY}")
    print(f"   Force: {args.force} | Dry run: {args.dry_run}" +
          (f" | Limit: {args.limit}" if args.limit else ""))
    print()

    sb = get_supabase()
    client = anthropic.Anthropic(api_key=get_env("ANTHROPIC_API_KEY"))

    to_process = fetch_countries(sb, args.force)
    total = min(len(to_process), args.limit) if args.limit else len(to_process)
    countries = to_process[:total]

    print(f"{total} countries to process ({len(to_process)} unenriched)\n")
    if total == 0:
        print("Nothing to do!")
        return

    print("Fetching country context data...")
    context_map = fetch_country_context(sb, [c["id"] for c in countries])
    with_context = sum(1 for c in context_map.values() if c["regions"])
    print(f"   {with_context}/{len(countries)} countries have region context\n")

    processed = 0
    succeeded = 0
    warning_count = 0
    failed = 0
    total_in = 0
    total_out = 0
    errors = []
    start_time = time.time()

    for country in countries:
        processed += 1
        label = country["name"]
        ctx = context_map.get(country["id"], {"regions": [], "appellations": [], "grapes": []})

        r = process_country(client, country, ctx)

        if r.get("tokens"):
            total_in += r["tokens"]["input_tokens"]
            total_out += r["tokens"]["output_tokens"]

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
                write_insight(sb, country["id"], r["parsed"])
                succeeded += 1
            except Exception as e:
                failed += 1
                errors.append({"name": label, "error": str(e)})
                print(f"     DB write failed: {e}")
        else:
            succeeded += 1

    elapsed = time.time() - start_time
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f"  Total processed: {processed}")
    print(f"  Succeeded:       {succeeded}")
    print(f"  Warnings:        {warning_count}")
    print(f"  Failed:          {failed}")
    print(f"  Tokens:          {total_in:,} in / {total_out:,} out")
    cost = (total_in * 3 + total_out * 15) / 1_000_000
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
