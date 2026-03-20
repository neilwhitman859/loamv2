"""
Enrich regions with AI-generated insights using Claude Sonnet.
Includes country catch-all regions -- treated as broadly representative
everyday wines from that country.

Usage:
    python -m pipeline.enrich.region_insights
    python -m pipeline.enrich.region_insights --force
    python -m pipeline.enrich.region_insights --dry-run --limit 10
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
    "ai_overview", "ai_climate_profile", "ai_sub_region_comparison",
    "ai_signature_style", "ai_history", "confidence",
]
BANNED_WORDS = [
    "prestigious", "world-class", "exceptional", "unparalleled",
    "legendary", "iconic", "finest", "renowned",
]

SYSTEM_PROMPT = """You are a wine expert writing for Loam, a wine intelligence platform. You write with genuine love for wine and deep knowledge — the way a great winemaker talks about their craft. Be specific, grounded, and real. Never use marketing fluff or generic wine-magazine language.

When you write about a wine region, write like someone who has traveled there, tasted across the range, and understands how the landscape shapes what ends up in the glass.

HANDLING UNCERTAINTY: If you don't know specific details about a lesser-known region:
- Write shorter entries (1-2 sentences is fine).
- State the general climate zone and likely character rather than guessing specifics.
- Set confidence to 0.5 or lower.
- An honest short entry is always better than a padded guess.

CATCH-ALL REGIONS: Some entries are country-level catch-alls (flagged in the context). These represent wines labeled broadly under the country name rather than a specific region — often entry-level, everyday wines. Write about the general character and range of these wines honestly. Don't pretend they're from a specific place.

Return a JSON object with EXACTLY these keys (no others):

{
  "ai_overview": "...",
  "ai_climate_profile": "...",
  "ai_sub_region_comparison": "...",
  "ai_signature_style": "...",
  "ai_history": "...",
  "confidence": 0.9
}

Field guidelines:
- ai_overview (2-4 sentences): What this region is and why it matters in the wine world. Lead with what makes it distinctive.
- ai_climate_profile (2-3 sentences): The climate patterns that shape the wines. Be specific about temperature, rainfall, maritime/continental influence, altitude — whatever matters most here.
- ai_sub_region_comparison (1-4 sentences): How the sub-regions or zones within this area differ. If the region has no meaningful sub-regions, write 1 short sentence acknowledging this. Don't force a comparison that doesn't exist.
- ai_signature_style (2-3 sentences): What wines from here taste and feel like. Sensory language rooted in the place.
- ai_history (2-3 sentences): The wine history of this region. Key turning points, traditions, how it got to where it is today.
- confidence: Your honest self-assessment. 0.9+ = major region you know deeply. 0.7-0.8 = well-known regional area. 0.5-0.6 = you know basics. 0.3-0.4 = mostly guessing.

CRITICAL RULES:
- Do NOT reference database counts, wine counts, or producer counts from the context data.
- The context data helps ground your response — use it to inform which appellations and grapes you discuss, but don't quote numbers.
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


def fetch_regions(sb, force: bool) -> list[dict]:
    """Fetch regions to process, with parent name lookup."""
    result = (
        sb.table("regions")
        .select("id, name, slug, parent_id, country:countries(name)")
        .is_("deleted_at", "null")
        .order("name")
        .execute()
    )
    regions = result.data
    if len(regions) >= 1000:
        regions = fetch_all("regions", "id, name, slug, parent_id, country:countries(name)")

    # Build parent name lookup
    name_by_id = {r["id"]: r["name"] for r in regions}
    for r in regions:
        r["parent_name"] = name_by_id.get(r.get("parent_id")) if r.get("parent_id") else None

    enriched_ids = set()
    if not force:
        existing = sb.table("region_insights").select("region_id").execute()
        enriched_ids = {e["region_id"] for e in existing.data}

    return [r for r in regions if r["id"] not in enriched_ids]


def fetch_region_context(sb, region_ids: list[str]) -> dict:
    """Fetch child regions, appellations, and top grapes per region."""
    context_map = {}
    batch_size = 50

    for i in range(0, len(region_ids), batch_size):
        batch_ids = region_ids[i:i + batch_size]

        # Child regions
        try:
            children_result = (
                sb.table("regions")
                .select("parent_id, name")
                .in_("parent_id", batch_ids)
                .is_("deleted_at", "null")
                .execute()
            )
            children = children_result.data or []
        except Exception:
            children = []

        child_map: dict[str, list[str]] = {}
        for c in children:
            pid = c.get("parent_id")
            if pid:
                child_map.setdefault(pid, []).append(c["name"])

        # Appellations per region
        try:
            app_result = (
                sb.table("appellations")
                .select("region_id, name")
                .in_("region_id", batch_ids)
                .execute()
            )
            app_data = app_result.data or []
        except Exception:
            app_data = []

        app_map: dict[str, list[str]] = {}
        for a in app_data:
            rid = a.get("region_id")
            if rid:
                app_map.setdefault(rid, []).append(a["name"])

        # Top grapes per region
        try:
            grape_result = (
                sb.table("wine_grapes")
                .select("grape:grapes(name), wine:wines!inner(region_id)")
                .in_("wine.region_id", batch_ids)
                .limit(10000)
                .execute()
            )
            grape_data = grape_result.data or []
        except Exception:
            grape_data = []

        grape_counts: dict[str, dict[str, int]] = {}
        for row in grape_data:
            rid = (row.get("wine") or {}).get("region_id")
            gname = (row.get("grape") or {}).get("name")
            if not rid or not gname:
                continue
            if rid not in grape_counts:
                grape_counts[rid] = {}
            grape_counts[rid][gname] = grape_counts[rid].get(gname, 0) + 1

        for rid in batch_ids:
            top_grapes = []
            if rid in grape_counts:
                top_grapes = sorted(grape_counts[rid].items(), key=lambda x: x[1], reverse=True)[:8]
                top_grapes = [name for name, _ in top_grapes]
            context_map[rid] = {
                "children": child_map.get(rid, []),
                "appellations": (app_map.get(rid, []))[:10],
                "grapes": top_grapes,
            }

    return context_map


def process_region(client: anthropic.Anthropic, region: dict, context: dict) -> dict:
    """Process a single region through Claude."""
    country = (region.get("country") or {}).get("name", "Unknown")
    parent_region = region.get("parent_name")
    is_catch_all = (region.get("slug") or "").endswith("-country")

    user_msg = f"Write region insights for:\n\nRegion: {region['name']}\nCountry: {country}"
    if parent_region:
        user_msg += f"\nParent region: {parent_region}"
    if context["children"]:
        user_msg += f"\nSub-regions: {', '.join(context['children'])}"
    if is_catch_all:
        user_msg += "\nThis is a CATCH-ALL region -- wines here are labeled under the country name, not a specific region. They are broadly representative of the country's everyday output."
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


def write_insight(sb, region_id: str, parsed: dict):
    """Upsert a region insight row."""
    now = datetime.now(timezone.utc)
    row = {
        "region_id": region_id,
        "ai_overview": parsed["ai_overview"],
        "ai_climate_profile": parsed["ai_climate_profile"],
        "ai_sub_region_comparison": parsed["ai_sub_region_comparison"],
        "ai_signature_style": parsed["ai_signature_style"],
        "ai_history": parsed["ai_history"],
        "confidence": parsed["confidence"],
        "enriched_at": now.isoformat(),
        "refresh_after": (now + timedelta(days=90)).isoformat(),
    }
    sb.table("region_insights").upsert(row, on_conflict="region_id").execute()


def main():
    parser = argparse.ArgumentParser(description="Enrich regions with AI insights")
    parser.add_argument("--force", action="store_true", help="Re-run all (overwrite existing)")
    parser.add_argument("--dry-run", action="store_true", help="Preview without writing")
    parser.add_argument("--limit", type=int, default=None, help="Process only N regions")
    args = parser.parse_args()

    print("Region Insights Enrichment Pipeline")
    print(f"   Model: Claude Sonnet | Concurrency: {CONCURRENCY}")
    print(f"   Force: {args.force} | Dry run: {args.dry_run}" +
          (f" | Limit: {args.limit}" if args.limit else ""))
    print()

    sb = get_supabase()
    client = anthropic.Anthropic(api_key=get_env("ANTHROPIC_API_KEY"))

    to_process = fetch_regions(sb, args.force)
    total = min(len(to_process), args.limit) if args.limit else len(to_process)
    regions = to_process[:total]

    print(f"{total} regions to process ({len(to_process)} unenriched)\n")
    if total == 0:
        print("Nothing to do!")
        return

    print("Fetching region context data...")
    context_map = fetch_region_context(sb, [r["id"] for r in regions])
    with_context = sum(1 for c in context_map.values() if c["appellations"] or c["grapes"])
    print(f"   {with_context}/{len(regions)} regions have context data\n")

    processed = 0
    succeeded = 0
    warning_count = 0
    failed = 0
    total_in = 0
    total_out = 0
    errors = []
    start_time = time.time()

    for region in regions:
        processed += 1
        country = (region.get("country") or {}).get("name", "?")
        label = f"{region['name']} ({country})"
        ctx = context_map.get(region["id"], {"children": [], "appellations": [], "grapes": []})

        r = process_region(client, region, ctx)

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
                write_insight(sb, region["id"], r["parsed"])
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
