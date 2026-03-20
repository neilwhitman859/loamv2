"""
Test the appellation_insights prompt against a few appellations
to nail the voice before running the full pipeline.

Modes:
    python -m pipeline.enrich.test_appellation_prompt producers   # Producer knowledge test
    python -m pipeline.enrich.test_appellation_prompt insights    # Full insight prompt test
"""

import sys
import json
import re
import argparse
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import anthropic
from pipeline.lib.db import get_env

# ── Test data ───────────────────────────────────────────────
TEST_APPELLATIONS = [
    {
        "name": "Napa Valley",
        "designation_type": "AVA",
        "country": "United States",
        "region": "Napa Valley",
        "grapes": ["Cabernet Sauvignon", "Merlot", "Chardonnay", "Pinot Noir", "Sauvignon Blanc",
                    "Zinfandel", "Syrah", "Petite Sirah", "Cabernet Franc", "Petit Verdot", "Viognier"],
        "producers": ["Abreu", "Alpha Omega", "Beaulieu Vineyard (BV)", "Beringer", "Bond",
                       "Bryant Family Vineyard", "Cade", "Cain", "Cakebread", "Caymus", "Chappellet",
                       "Charles Krug", "Chateau Montelena", "Cliff Lede", "Clos du Val", "Colgin",
                       "Corison", "Dalla Valle", "Dana", "Darioush", "David Arthur", "Diamond Creek",
                       "Dominus", "Duckhorn", "Dunn", "Far Niente", "Favia", "Flora Springs",
                       "Frank Family", "Freemark Abbey", "Frog's Leap", "Grace Family Vineyards",
                       "Grgich Hills", "Groth", "Hall", "Harlan Estate", "Heitz Cellar", "Honig",
                       "Hundred Acre", "Inglenook", "Joseph Phelps", "Kapcsandy", "Kongsgaard",
                       "La Jota", "Lail Vineyards", "Larkmead", "Lewis Cellars", "Lokoya",
                       "Long Meadow Ranch", "Louis M. Martini", "Matthiasson", "Memento Mori",
                       "Merryvale", "Miner", "Newton", "Nickel & Nickel", "Opus One", "Orin Swift",
                       "Ovid", "Pahlmeyer", "Paul Hobbs", "Philip Togni", "Pine Ridge", "PlumpJack",
                       "Pride Mountain Vineyards", "Promontory", "Realm", "Robert Mondavi",
                       "Rombauer Vineyards", "Round Pond Estate", "Rutherford Hill", "Schrader",
                       "Schramsberg", "Shafer", "Silver Oak", "Silverado Vineyards", "Spottswoode",
                       "Spring Mountain Vineyard", "Stag's Leap Wine Cellars", "Staglin",
                       "Sterling Vineyards", "Trefethen", "Turley", "Turnbull", "Vineyard 29",
                       "William Hill", "ZD Wines"],
    },
    {
        "name": "Rudesheim",
        "designation_type": "Weinbaugebiet",
        "country": "Germany",
        "region": "Rheingau",
        "grapes": ["Riesling", "Pinot Noir"],
        "producers": ["Weingut Carl Ehrhard", "Georg Breuer", "Johannishof", "Hammond"],
    },
    {
        "name": "Cremant de Limoux",
        "designation_type": "AOC",
        "country": "France",
        "region": "Languedoc",
        "grapes": ["Chardonnay", "Chenin Blanc", "Mauzac", "Pinot Noir"],
        "producers": ["Domaine Rosier", "Gerard Bertrand", "Antech", "Philippe Collin",
                       "Domaine Delmas", "Michel Olivier", "Domaine de Tholomies",
                       "Domaine de la Baume", "La Louviere", "Chateau Beausoleil"],
    },
]

# ── Producer knowledge test prompt ──────────────────────────
PRODUCER_TEST_PROMPT = """You are a wine expert. For each producer listed below, categorize your knowledge level as one of:
- "strong": You know specific details about this producer — their flagship wines, style, history, or reputation.
- "moderate": You recognize the name and can say something general about them.
- "weak": You've maybe heard the name but can't say anything specific.
- "unknown": You don't recognize this producer at all.

Be honest. Do not inflate your knowledge. Return ONLY a raw JSON object (no markdown fences) with producer names as keys and knowledge levels as values."""

# ── Insight prompt ──────────────────────────────────────────
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
        warnings.append(f"Bad confidence value: {conf}")
    for key in EXPECTED_KEYS:
        if key == "confidence":
            continue
        val = parsed.get(key)
        if isinstance(val, str) and val.strip() == "":
            warnings.append(f"Empty field: {key}")
    all_text = " ".join(str(parsed.get(k, "")) for k in EXPECTED_KEYS if k != "confidence").lower()
    found = [w for w in BANNED_WORDS if w in all_text]
    if found:
        warnings.append(f"Banned words found: {', '.join(found)}")
    return warnings


def test_producer_knowledge(client: anthropic.Anthropic):
    """Test Claude's knowledge of producers per appellation."""
    for app in TEST_APPELLATIONS:
        print(f"\n{'=' * 60}")
        print(f"Producer knowledge test: {app['name']} ({len(app['producers'])} producers)")
        print("=" * 60)

        user_msg = (
            f"Categorize your knowledge of these producers from {app['name']}, {app['country']}:\n\n"
            + "\n".join(app["producers"])
            + "\n\nReturn only the raw JSON object."
        )

        response = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=4096,
            messages=[{"role": "user", "content": PRODUCER_TEST_PROMPT + "\n\n" + user_msg}],
        )

        text = response.content[0].text.strip()
        text = re.sub(r"^```(?:json)?\s*", "", text)
        text = re.sub(r"\s*```$", "", text)

        try:
            parsed = json.loads(text)
            buckets: dict[str, list[str]] = {"strong": [], "moderate": [], "weak": [], "unknown": []}
            for producer, level in parsed.items():
                if level in buckets:
                    buckets[level].append(producer)
                else:
                    print(f"  Unexpected level \"{level}\" for {producer}")

            for level in ("strong", "moderate", "weak", "unknown"):
                names = ", ".join(buckets[level]) or "none"
                print(f"\n  {level.capitalize()} ({len(buckets[level])}): {names}")
        except json.JSONDecodeError as e:
            print("\n--- RAW RESPONSE ---")
            print(text)
            print(f"\nFailed to parse JSON: {e}")

        print(f"\nTokens: {response.usage.input_tokens} in, {response.usage.output_tokens} out")


def test_insights(client: anthropic.Anthropic):
    """Test the full appellation insights prompt."""
    for app in TEST_APPELLATIONS:
        print(f"\n{'=' * 60}")
        print(f"Testing: {app['name']} ({app['designation_type']}, {app['country']})")
        print(f"  Grapes: {', '.join(app['grapes'])}")
        print("=" * 60)

        user_msg = f"""Write appellation insights for:

Name: {app['name']}
Designation: {app['designation_type']}
Country: {app['country']}
Region: {app['region']}
Key grapes: {', '.join(app['grapes'])}"""

        response = client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=1500,
            messages=[
                {"role": "user", "content": SYSTEM_PROMPT + "\n\n" + user_msg},
                {"role": "assistant", "content": "{"},
            ],
        )

        if response.stop_reason == "max_tokens":
            print("\nTRUNCATED -- response hit max_tokens limit")

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
                print(f"\n{key}:")
                print(f"  {val}")
        except json.JSONDecodeError as e:
            print("\n--- RAW RESPONSE ---")
            print(text)
            print(f"\nFailed to parse JSON: {e}")

        print(f"\nTokens: {response.usage.input_tokens} in, {response.usage.output_tokens} out")


def main():
    parser = argparse.ArgumentParser(description="Test appellation prompts")
    parser.add_argument("mode", nargs="?", default="producers",
                        choices=["producers", "insights"],
                        help="Test mode: 'producers' or 'insights'")
    args = parser.parse_args()

    client = anthropic.Anthropic(api_key=get_env("ANTHROPIC_API_KEY"))

    if args.mode == "producers":
        test_producer_knowledge(client)
    else:
        test_insights(client)


if __name__ == "__main__":
    main()
