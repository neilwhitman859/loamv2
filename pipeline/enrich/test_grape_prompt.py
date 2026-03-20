"""
Test the grape_insights prompt against 4 grapes spanning
major/moderate/niche profiles before running the full pipeline.

Usage:
    python -m pipeline.enrich.test_grape_prompt
"""

import sys
import json
import re
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import anthropic
from pipeline.lib.db import get_env

# ── Test data ───────────────────────────────────────────────
TEST_GRAPES = [
    {
        "name": "Pinot Noir",
        "color": "red",
        "top_appellations": [
            {"appellation": "Champagne", "country": "France"},
            {"appellation": "Bourgogne", "country": "France"},
            {"appellation": "Baden", "country": "Germany"},
            {"appellation": "Côte de Beaune", "country": "France"},
            {"appellation": "Gevrey-Chambertin", "country": "France"},
            {"appellation": "Willamette Valley", "country": "United States"},
            {"appellation": "Russian River Valley", "country": "United States"},
            {"appellation": "Pfalz", "country": "Germany"},
        ],
        "top_countries": ["France", "United States", "Germany", "Australia", "Italy"],
    },
    {
        "name": "Gruner Veltliner",
        "color": "white",
        "top_appellations": [
            {"appellation": "Niederosterreich", "country": "Austria"},
            {"appellation": "Wachau", "country": "Austria"},
            {"appellation": "Kamptal", "country": "Austria"},
            {"appellation": "Kremstal", "country": "Austria"},
            {"appellation": "Burgenland", "country": "Austria"},
            {"appellation": "Weinviertel", "country": "Austria"},
            {"appellation": "Wagram", "country": "Austria"},
        ],
        "top_countries": ["Austria", "Hungary", "United States"],
    },
    {
        "name": "Tannat",
        "color": "red",
        "top_appellations": [
            {"appellation": "Canelones", "country": "Uruguay"},
            {"appellation": "Serra Gaucha", "country": "Brazil"},
            {"appellation": "Vale dos Vinhedos", "country": "Brazil"},
            {"appellation": "Campanha Gaucha", "country": "Brazil"},
            {"appellation": "Maldonado", "country": "Uruguay"},
            {"appellation": "Madiran", "country": "France"},
            {"appellation": "San Jose", "country": "Uruguay"},
            {"appellation": "Mendoza", "country": "Argentina"},
        ],
        "top_countries": ["Uruguay", "Brazil", "France", "Argentina", "United States"],
    },
    {
        "name": "Assyrtiko",
        "color": "white",
        "top_appellations": [
            {"appellation": "Santorini", "country": "Greece"},
            {"appellation": "Chalkidiki", "country": "Greece"},
            {"appellation": "Attiki", "country": "Greece"},
            {"appellation": "Crete", "country": "Greece"},
            {"appellation": "Drama", "country": "Greece"},
        ],
        "top_countries": ["Greece", "Cyprus"],
    },
]

# ── Constants ───────────────────────────────────────────────
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


def main():
    client = anthropic.Anthropic(api_key=get_env("ANTHROPIC_API_KEY"))
    total_in = 0
    total_out = 0

    for grape in TEST_GRAPES:
        print(f"\n{'=' * 60}")
        print(f"Testing: {grape['name']} ({grape['color']})")
        print(f"  Top appellations: {', '.join(a['appellation'] for a in grape['top_appellations'])}")
        print(f"  Top countries: {', '.join(grape['top_countries'])}")
        print("=" * 60)

        appellation_list = ", ".join(
            f"{a['appellation']} ({a['country']})" for a in grape["top_appellations"]
        )
        country_list = ", ".join(grape["top_countries"])

        user_msg = f"""Write grape insights for:

Grape: {grape['name']}
Color: {grape['color']}
Top appellations in our database: {appellation_list}
Top countries: {country_list}"""

        response = client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=2000,
            messages=[
                {"role": "user", "content": SYSTEM_PROMPT + "\n\n" + user_msg},
                {"role": "assistant", "content": "{"},
            ],
        )

        total_in += response.usage.input_tokens
        total_out += response.usage.output_tokens

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

    print(f"\n{'=' * 60}")
    print(f"TOTAL: {total_in} in, {total_out} out")
    cost = (total_in * 3 + total_out * 15) / 1_000_000
    print(f"Est. cost: ${cost:.4f}")


if __name__ == "__main__":
    main()
