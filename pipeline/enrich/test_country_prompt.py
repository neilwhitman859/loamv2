"""
Test the country_insights prompt against 3 countries spanning
major/moderate/small before running the full pipeline.

Usage:
    python -m pipeline.enrich.test_country_prompt
"""

import sys
import json
import re
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import anthropic
from pipeline.lib.db import get_env

# ── Test data ───────────────────────────────────────────────
TEST_COUNTRIES = [
    {
        "name": "France",
        "top_regions": ["Burgundy", "Champagne", "Bordeaux", "Loire Valley",
                         "Southern Rhone", "Alsace", "Languedoc-Roussillon"],
        "top_appellations": ["Bourgogne", "Champagne", "Alsace", "Bordeaux",
                              "Chablis", "Cote de Beaune", "Languedoc", "Meursault"],
        "top_grapes": ["Chardonnay", "Pinot Noir", "Merlot", "Cabernet Sauvignon",
                        "Syrah", "Cabernet Franc", "Grenache", "Mourvedre"],
    },
    {
        "name": "Uruguay",
        "top_regions": ["Canelones", "Maldonado"],
        "top_appellations": ["Canelones", "Maldonado", "Cerro Chapeu", "Juanico", "San Jose"],
        "top_grapes": ["Tannat", "Cabernet Sauvignon", "Merlot", "Cabernet Franc",
                        "Chardonnay", "Syrah", "Sauvignon Blanc", "Pinot Noir"],
    },
    {
        "name": "Hungary",
        "top_regions": ["Tokaj", "Villany", "Eger"],
        "top_appellations": ["Tokaj", "Villany", "Szekszard", "Eger", "Egri Bikaver"],
        "top_grapes": ["Furmint", "Cabernet Sauvignon", "Cabernet Franc", "Merlot",
                        "Harslevelu", "Blaufrankisch", "Pinot Noir", "Welschriesling"],
    },
]

# ── Constants ───────────────────────────────────────────────
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


def build_user_message(country: dict) -> str:
    """Build the user message for a test country."""
    msg = f"Write country insights for:\n\nCountry: {country['name']}"
    msg += f"\nMajor regions: {', '.join(country['top_regions'])}"
    msg += f"\nTop appellations: {', '.join(country['top_appellations'])}"
    msg += f"\nTop grapes: {', '.join(country['top_grapes'])}"
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

    for country in TEST_COUNTRIES:
        print(f"\n{'=' * 60}")
        print(f"Testing: {country['name']}")
        print(f"  Regions: {', '.join(country['top_regions'])}")
        print(f"  Grapes: {', '.join(country['top_grapes'])}")
        print("=" * 60)

        user_msg = build_user_message(country)
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
