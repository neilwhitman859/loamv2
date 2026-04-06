"""
WineTest benchmark generation.

Generates a benchmark set of ~200 wines from external sources that represent
what an American wine consumer actually encounters in stores, restaurants,
and friends' houses. The benchmark is fresh each run — no cherry-picking
from our own database.

Primary source: Haiku-generated wine lists based on market knowledge.
Stable core (~60 wines) + rotating category samples (~140 wines).
"""

import json
import random
import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from pipeline.lib.db import get_env  # loads .env

import anthropic

# Categories for rotation — each run samples from this pool
CATEGORY_POOL = [
    # US regions
    "Napa Valley Cabernet Sauvignon",
    "Sonoma County Pinot Noir",
    "Oregon Pinot Noir",
    "Washington State red wines",
    "California Chardonnay",
    "Paso Robles red wines",
    "Santa Barbara County wines",
    "Finger Lakes Riesling",
    "Willamette Valley wines",
    # France
    "Burgundy red wines",
    "Burgundy white wines",
    "Bordeaux red wines",
    "Champagne and French sparkling",
    "Rhône Valley red wines",
    "Loire Valley white wines",
    "Alsace white wines",
    "Southern French red wines under $25",
    "Beaujolais",
    "Provence rosé",
    # Italy
    "Barolo and Barbaresco",
    "Chianti and Tuscan reds",
    "Pinot Grigio and Italian whites",
    "Prosecco and Italian sparkling",
    "Southern Italian reds",
    "Super Tuscans",
    # Spain / Portugal
    "Rioja and Spanish reds",
    "Spanish white wines and rosé",
    "Portuguese red wines",
    "Sherry and fortified wines",
    # New World
    "New Zealand Sauvignon Blanc",
    "Argentine Malbec",
    "Chilean Cabernet Sauvignon",
    "Australian Shiraz",
    "South African wines",
    # Styles / price points
    "Natural and orange wines",
    "Red wines under $15",
    "White wines under $15",
    "Rosé wines",
    "Dessert and sweet wines",
    "Sparkling wines under $30",
    "Premium wines $50-$100",
    "Luxury and collectible wines over $100",
    "Wines commonly found at Costco or Trader Joe's",
    "Restaurant by-the-glass staples",
    "Popular red blends",
]

CORE_PROMPT = """\
You are generating a benchmark list of wines for testing a wine database's \
coverage of the American market.

List exactly {count} wines that are universally stocked at American wine stores \
and well-known to educated American wine consumers. These should be wines you'd \
find at any well-stocked wine shop, upscale grocery store, or restaurant in \
the United States.

Requirements:
- Every wine must be REAL and currently available in the US market
- Mix across price points: ~30% everyday ($10-20), ~35% mid-range ($20-50), \
~25% premium ($50-100), ~10% luxury ($100+)
- Mix across regions: California, France, Italy, Spain, New Zealand, Argentina, \
Oregon, Washington, Australia, Germany, Portugal, and others
- Mix across types: ~55% red, ~30% white, ~10% sparkling, ~5% rosé
- Include both mass-market brands (Josh, Meiomi, Yellow Tail) and prestigious \
producers (Opus One, DRC, Penfolds)
- Include wines people actually BUY, not just talk about

Return ONLY a JSON array. No other text. Each element:
{{"producer": "...", "name": "...", "country": "...", "color": "red|white|rosé|sparkling", "price_tier": "everyday|midrange|premium|luxury"}}

Example entry:
{{"producer": "Caymus", "name": "Cabernet Sauvignon Napa Valley", "country": "US", "color": "red", "price_tier": "premium"}}
"""

CATEGORY_PROMPT = """\
You are generating a benchmark list of wines for testing a wine database.

List exactly {count} wines in the category "{category}" that are commonly \
found at American wine stores and restaurants.

Requirements:
- Every wine must be REAL and currently available in the US market
- Mix across price points within this category
- Include both well-known producers and quality mid-tier producers
- Focus on wines an American consumer would actually encounter and buy

Do NOT include any of these wines (already in the benchmark):
{exclusions}

Return ONLY a JSON array. No other text. Each element:
{{"producer": "...", "name": "...", "country": "...", "color": "red|white|rosé|sparkling", "price_tier": "everyday|midrange|premium|luxury"}}
"""


def generate_benchmark(
    size: int = 200,
    num_categories: int = 4,
    seed: int | None = None,
) -> dict:
    """Generate a benchmark wine list from external sources.

    Returns dict with 'wines' list and 'metadata' about the generation.
    """
    if seed is None:
        seed = int(datetime.now().timestamp()) % 100000
    rng = random.Random(seed)

    api_key = get_env("ANTHROPIC_API_KEY")
    client = anthropic.Anthropic(api_key=api_key)
    core_size = min(60, size)
    rotate_size = size - core_size
    per_category = rotate_size // num_categories

    # Pick random categories for this run
    categories = rng.sample(CATEGORY_POOL, min(num_categories, len(CATEGORY_POOL)))

    wines = []
    sources_used = []

    # 1. Generate stable core
    print(f"  Generating core benchmark ({core_size} wines)...")
    core_wines = _generate_via_haiku(
        client,
        CORE_PROMPT.format(count=core_size),
        source="core",
    )
    for w in core_wines:
        w["category"] = "core"
    wines.extend(core_wines)
    sources_used.append(f"haiku_core ({len(core_wines)} wines)")

    # 2. Generate category rotation
    for cat in categories:
        print(f"  Generating category: {cat} ({per_category} wines)...")
        exclusion_list = "\n".join(
            f"- {w['producer']} {w['name']}" for w in wines
        )
        cat_wines = _generate_via_haiku(
            client,
            CATEGORY_PROMPT.format(
                count=per_category,
                category=cat,
                exclusions=exclusion_list if exclusion_list else "(none yet)",
            ),
            source=f"category:{cat}",
        )
        for w in cat_wines:
            w["category"] = cat
        wines.extend(cat_wines)
        sources_used.append(f"haiku_category:{cat} ({len(cat_wines)} wines)")

    # 3. Deduplicate by (producer_lower, name_lower)
    seen = set()
    deduped = []
    for w in wines:
        key = (w["producer"].lower().strip(), w["name"].lower().strip())
        if key not in seen:
            seen.add(key)
            deduped.append(w)
    wines = deduped

    return {
        "wines": wines,
        "metadata": {
            "seed": seed,
            "target_size": size,
            "actual_size": len(wines),
            "categories_sampled": categories,
            "sources": sources_used,
            "generated_at": datetime.now().isoformat(),
        },
    }


def _generate_via_haiku(
    client: anthropic.Anthropic,
    prompt: str,
    source: str,
) -> list[dict]:
    """Call Haiku to generate a wine list. Returns parsed list of dicts."""
    try:
        response = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=4096,
            messages=[{"role": "user", "content": prompt}],
        )
        text = response.content[0].text.strip()

        # Handle markdown code blocks
        if text.startswith("```"):
            text = text.split("\n", 1)[1]
            if text.endswith("```"):
                text = text[: text.rfind("```")]
            text = text.strip()

        wines = json.loads(text)

        # Validate and normalize
        valid = []
        for w in wines:
            if not isinstance(w, dict):
                continue
            if "producer" not in w or "name" not in w:
                continue
            valid.append(
                {
                    "producer": str(w.get("producer", "")).strip(),
                    "name": str(w.get("name", "")).strip(),
                    "country": str(w.get("country", "")).strip(),
                    "color": str(w.get("color", "")).strip().lower(),
                    "price_tier": str(w.get("price_tier", "")).strip().lower(),
                    "source": source,
                }
            )
        return valid

    except (json.JSONDecodeError, anthropic.APIError, IndexError, KeyError) as e:
        print(f"  WARNING: Failed to generate from {source}: {e}")
        return []
