"""
Test the region_insights prompt against 4 regions spanning
major/moderate/small/catch-all before running the full pipeline.

Usage:
    python -m pipeline.enrich.test_region_prompt
"""

import sys
import json
import re
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import anthropic
from pipeline.lib.db import get_env

# ── Test data ───────────────────────────────────────────────
TEST_REGIONS = [
    {
        "name": "Bordeaux",
        "country": "France",
        "parent_region": None,
        "child_regions": ["Left Bank", "Right Bank"],
        "is_catch_all": False,
        "top_appellations": ["Bordeaux", "Bordeaux Superieur", "Cotes de Bourg",
                             "Puisseguin-Saint-Emilion", "Blaye-Cotes de Bordeaux",
                             "Montagne-Saint-Emilion"],
        "top_grapes": ["Merlot", "Cabernet Sauvignon", "Cabernet Franc", "Malbec",
                        "Sauvignon Blanc", "Petit Verdot", "Semillon"],
    },
    {
        "name": "Willamette Valley",
        "country": "United States",
        "parent_region": "Oregon",
        "child_regions": [],
        "is_catch_all": False,
        "top_appellations": ["Willamette Valley", "Dundee Hills", "Eola-Amity Hills",
                             "Chehalem Mountains", "Ribbon Ridge", "McMinnville"],
        "top_grapes": ["Pinot Noir", "Chardonnay", "Pinot Gris", "Riesling", "Pinot Blanc"],
    },
    {
        "name": "Kamptal",
        "country": "Austria",
        "parent_region": None,
        "child_regions": [],
        "is_catch_all": False,
        "top_appellations": ["Kamptal"],
        "top_grapes": ["Gruner Veltliner", "Riesling", "Zweigelt", "Chardonnay"],
    },
    {
        "name": "France",
        "country": "France",
        "parent_region": None,
        "child_regions": [],
        "is_catch_all": True,
        "top_appellations": ["Vin de France", "Vin de Pays"],
        "top_grapes": ["Chardonnay", "Syrah", "Grenache", "Pinot Noir",
                        "Cabernet Sauvignon", "Merlot", "Cabernet Franc", "Sauvignon Blanc"],
    },
]

# ── Constants ───────────────────────────────────────────────
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


def build_user_message(region: dict) -> str:
    """Build the user message for a test region."""
    msg = f"Write region insights for:\n\nRegion: {region['name']}\nCountry: {region['country']}"
    if region["parent_region"]:
        msg += f"\nParent region: {region['parent_region']}"
    if region["child_regions"]:
        msg += f"\nSub-regions: {', '.join(region['child_regions'])}"
    if region["is_catch_all"]:
        msg += "\nThis is a CATCH-ALL region -- wines here are labeled under the country name, not a specific region. They are broadly representative of the country's everyday output."
    if region["top_appellations"]:
        msg += f"\nTop appellations: {', '.join(region['top_appellations'])}"
    if region["top_grapes"]:
        msg += f"\nTop grapes: {', '.join(region['top_grapes'])}"
    return msg


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


def main():
    client = anthropic.Anthropic(api_key=get_env("ANTHROPIC_API_KEY"))
    total_in = 0
    total_out = 0

    for region in TEST_REGIONS:
        catch_all_tag = " [CATCH-ALL]" if region["is_catch_all"] else ""
        print(f"\n{'=' * 60}")
        print(f"Testing: {region['name']} ({region['country']}){catch_all_tag}")
        print(f"  Appellations: {', '.join(region['top_appellations'])}")
        print(f"  Grapes: {', '.join(region['top_grapes'])}")
        print("=" * 60)

        user_msg = build_user_message(region)
        response = client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=1500,
            messages=[
                {"role": "user", "content": SYSTEM_PROMPT + "\n\n" + user_msg},
                {"role": "assistant", "content": "{"},
            ],
        )

        total_in += response.usage.input_tokens
        total_out += response.usage.output_tokens

        if response.stop_reason == "max_tokens":
            print("\nTRUNCATED")

        text = "{" + response.content[0].text.strip()
        text = re.sub(r"```(?:json)?\s*", "", text)
        text = re.sub(r"\s*```", "", text)

        try:
            parsed = json.loads(text)
            warnings = validate_response(parsed)
            if warnings:
                print("\nVALIDATION WARNINGS:")
                for w in warnings:
                    print(f"  - {w}")
            else:
                print("\nValidation passed")

            print("\n--- PARSED FIELDS ---")
            for key, val in parsed.items():
                print(f"\n{key}:\n  {val}")
        except json.JSONDecodeError as e:
            print(f"\n--- RAW ---\n{text[:500]}")
            print(f"\nJSON parse failed: {e}")

        print(f"\nTokens: {response.usage.input_tokens} in, {response.usage.output_tokens} out")

    print(f"\n{'=' * 60}")
    print(f"TOTAL: {total_in} in, {total_out} out")
    cost = (total_in * 3 + total_out * 15) / 1_000_000
    print(f"Est. cost: ${cost:.4f}")


if __name__ == "__main__":
    main()
