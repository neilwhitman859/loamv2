# Loam — Schema Rework Decisions

Running log of schema decisions made during the rework discussion.
This document becomes the spec for implementation.

---

## Migration Strategy

**Decision: Full DB rebuild.**

Current state: ~75 wines, 75 vintages, 149 grapes, 1,517 appellations, 317 regions, 59 countries. Small enough to re-seed from scratch. No need to migrate in place — design the schema we actually want, build it clean, re-run the enrichment pipeline to populate it.

The frontend and enrichment pipeline logic stay; it's the data model underneath that gets reworked.

---

## Decisions

### 1. Weather lives at appellation level, not wine level

**What changed:** Weather data (GDD, rainfall, frost days, heat spikes, harvest conditions) moves from individual wine records to a shared appellation-vintage record.

**Current state:** Each wine has its own lat/lng in `wines.js`. The pipeline calls Open-Meteo once per wine per vintage using that coordinate. Weather metrics are stored on the `vintages` table (one row per wine-vintage combo). 10 Rioja wines = 10 separate weather fetches with slightly different results.

**New state:** Weather is fetched once per appellation per vintage using a representative coordinate (appellation centroid or defined reference point). All wines in that appellation reference the same weather record.

**Reasoning:**
- Weather is a property of a place and a year, not of a specific bottle.
- Wine-level weather creates meaningless variation between wines in the same appellation (noise from ERA5 grid resolution and minor coordinate differences).
- Appellation-level is actually *less* data and fewer API calls.
- Enables answering "what was Rioja like in 2010?" as a standalone question.

**Implies:**
- Appellation needs to be a real structured table (not just a `_meta` content table).
- A new vintage/weather table keyed on appellation + year.
- Wine-level lat/lng still useful for map display but no longer used for weather.
- Growing season dates also belong at appellation level (same reasoning — growing season is a property of a place, not a bottle).

---

### 2. Geographic hierarchy: Country → Region → Appellation

**Decision: Keep existing geographic tables but restructure them as core entities, not just `_meta` content tables.**

**Structure:**
- **Countries** — flat, no nesting.
- **Regions** — self-referencing `parent_id` for nesting (California → Napa Valley → etc.). One table handles all levels.
- **Appellations** — strictly legal designations (AVA, AOC, DOCa, DOCG, etc.). FK to region. This is where structured terroir data, weather, and growing season attach.

**How wines connect:**
- `wines.country_id` — FK to countries, **NOT NULL**. Every wine comes from somewhere.
- `wines.appellation_id` — FK to appellation, **nullable** (not all wines have a legal appellation).
- `wines.region_id` — FK to region, **nullable**. Null when a wine sources from multiple regions (join table is source of truth in that case).
- `wine_regions` **join table** — a wine can be linked to multiple regions (e.g., a blend sourcing from Napa and Sonoma).

**Multi-appellation:** Deferred. `wines.appellation_id` is a single FK for now. Less than 1% of wines span multiple appellations. Can add a `wine_appellations` join table later if needed — straightforward migration.

**Region nesting and filtering:**
- Regions use `parent_id` for hierarchy (e.g., Napa Valley's parent is California).
- Each wine is tagged to its most specific region.
- Filtering by a parent region (e.g., California) returns all wines in California and all descendant regions (Napa Valley, Sonoma Coast, etc.) by walking the tree downward.
- A wine that sources grapes from multiple regions appears in filter results for any of those regions.

**Reasoning:**
- Appellations are legal facts — they should be their own entity, not a text field.
- Regions exist at variable depth (California vs. Napa Valley vs. Sonoma Coast) — self-referencing `parent_id` handles this naturally without multiple tables.
- Join table for regions handles the real-world case where a wine draws from multiple regions.

---

### 3. Producer as entity

**Decision: `producers` is its own table. Wines FK to it.**

**`producers` table:**
- `id` (PK, slug)
- `name`
- `country_id` (FK → countries)
- `region_id` (FK → regions, nullable — null if producer works across multiple regions)
- `overview` (text, nullable)
- `website` (text, nullable)
- `established_year` (integer, nullable)

**`producer_regions` join table** — for producers that work across multiple regions. Same pattern as `wine_regions`.

**`wines` table change:**
- Drop `producer` text field
- Add `producer_id` (FK → producers)

**Pipeline implication:** When Haiku parses a new wine, it identifies the producer and either matches to an existing `producers` row or creates a new one. Fully automated, no manual matching.

**Reasoning:**
- Enables producer pages, "all wines by this producer" queries, producer-level data and filtering.
- More fields can be added later (accolades, production volume, certifications, etc.) without reworking the structure.

---

### 4. Grape modeling: varietal categories + blend composition

**Decision: Three layers of grape information, each serving a different purpose.**

Each layer answers a different question:
- **Varietal category:** "What kind of wine is this?" → Bordeaux Blend. *(For filtering, browsing, grouping.)*
- **Label designation:** "What does the producer call it?" → Red Wine. *(Raw label data, searchable, producer's own words.)*
- **Wine grapes:** "What's actually in it?" → 79% Cab Sauv, 7% Merlot, 7% Cab Franc, 4% Petit Verdot, 3% Malbec. *(Blend truth, where available.)*

---

**`grapes` table** *(existing, 149 rows)*
- `id` (PK)
- `name`
- *The botanical entity. What the plant actually is. Stores viticultural data — growing characteristics, flavor profiles, origin. Cabernet Sauvignon the grape variety. A grape exists whether or not any wine in our database uses it. Fields on this table left as-is for now — joins and keys are correct.*

**`varietal_categories` table** *(new, pre-seeded with ~130-150 entries)*
- `id` (PK, slug)
- `name` ("Cabernet Sauvignon", "Bordeaux Blend", "Red Blend", "GSM", "Super Tuscan", etc.)
- `color` (red, white, rosé, orange)
- `effervescence` (still, sparkling, semi_sparkling) — default for this category
- `type` (single_varietal, named_blend, generic_blend, regional_designation, proprietary)
- `grape_id` (FK → grapes, nullable) — populated for single varietals (Cabernet Sauvignon the category → Cabernet Sauvignon the grape), null for blends/regional/proprietary
- `description` (text, nullable)
- *The normalized label vocabulary. A controlled, pre-seeded list that the pipeline matches wines against. This is how users filter and browse — "show me all Bordeaux Blends," "show me all Cabernet Sauvignons." It captures the winemaking tradition and style, not just the grape. A "Bordeaux Blend" means a specific family of grapes (Cab Sauv, Merlot, Cab Franc, Petit Verdot, Malbec) regardless of where the wine is made. A "Super Tuscan" means an Italian wine that breaks DOC rules by using international varieties. Pre-seeded for consistency — the pipeline picks from existing categories rather than inventing new ones.*

**`wines` table** *(relevant fields)*
- `varietal_category_id` (FK → varietal_categories) — the normalized, filterable classification. What this wine *is* in terms of style and tradition. **AI-enriched field** (convention prefix TBD during AI layer separation discussion) — set by the AI pipeline based on the wine's actual composition and context. This is a judgment call: Opus One doesn't call itself a Bordeaux Blend, but that's what it is. Revisited on re-enrichment.
- `label_designation` (text, nullable, **searchable**) — exactly what the producer prints on the front label as the varietal/type descriptor. Raw data, captured as-is. "Cabernet Sauvignon", "Red Wine", "Toscana IGT", "Brut", or null if the label doesn't include one (common in France). This is the producer's marketing choice, not ours. Adds a search path that other fields don't cover — a user searching "Toscana IGT" should find Tignanello even though its varietal category is "Super Tuscan."
- `effervescence` (still, sparkling, semi_sparkling — nullable) — overrides the varietal category default when the wine breaks the norm. A sparkling Pinot Noir would have varietal_category → Pinot Noir (default: still) but `wines.effervescence` = sparkling. Null means use the category default.

**`wine_grapes` join table** *(new)*
- `wine_id` (FK → wines)
- `grape_id` (FK → grapes)
- `percentage` (decimal, nullable — not all producers publish blend breakdowns)
- PK: composite (`wine_id`, `grape_id`)
- *The truth of what's in the bottle. Every grape in the blend gets a row. Percentage included where available — many producers publish this on tech sheets, but not all. When percentage isn't available, we still capture which grapes are present. Independent of the varietal category — a wine categorized as "Cabernet Sauvignon" might have three grapes in the blend, and we capture all of them here.*

**Joins:**
- `wines` → `varietal_categories` via `wines.varietal_category_id` — *how the wine is classified for filtering*
- `varietal_categories` → `grapes` via `varietal_categories.grape_id` — *links the label term back to the botanical grape for single varietals*
- `wines` ↔ `grapes` via `wine_grapes` — *the actual blend composition*

**Search paths:** A user can find wines through multiple grape-related paths:
- Search "Bordeaux Blend" → matches via `varietal_categories`
- Search "Toscana IGT" → matches via `label_designation`
- Search "Merlot" → matches via `wine_grapes` (finds every wine containing Merlot, even if not categorized as Merlot)

**AI enrichment note:** `varietal_category_id` is AI-classified. The pipeline needs wine knowledge to make correct classification calls (e.g., knowing a Cab/Merlot/Cab Franc blend is a Bordeaux Blend, not just a Red Blend). Handled via classification rules and few-shot examples in the prompt. Flagged as AI-enriched so re-enrichment revisits it. Exact convention (prefix, metadata, etc.) to be determined during AI layer separation discussion.

**Examples:**

*2022 Stag's Leap Artemis:*
- varietal_category → Cabernet Sauvignon
- label_designation → "Cabernet Sauvignon"
- effervescence → null (category default: still)
- wine_grapes: Cab Sauv 98%, Cab Franc 1.5%, Petit Verdot 0.5%

*Opus One:*
- varietal_category → Bordeaux Blend
- label_designation → "Red Wine"
- effervescence → null (category default: still)
- wine_grapes: Cab Sauv 79%, Merlot 7%, Cab Franc 7%, Petit Verdot 4%, Malbec 3%

*Château Margaux:*
- varietal_category → Bordeaux Blend
- label_designation → null (French labels typically don't include a varietal descriptor)
- effervescence → null (category default: still)
- wine_grapes: Cab Sauv 87%, Merlot 8%, Cab Franc 3%, Petit Verdot 2% (varies by vintage)

*Tignanello:*
- varietal_category → Super Tuscan
- label_designation → "Toscana IGT"
- effervescence → null (category default: still)
- wine_grapes: Sangiovese 80%, Cab Sauv 15%, Cab Franc 5%

*Veuve Clicquot Yellow Label:*
- varietal_category → Champagne Blend
- label_designation → "Brut"
- effervescence → null (category default: sparkling)
- wine_grapes: Pinot Noir 50%, Chardonnay 28%, Pinot Meunier 22%

**Seeding note:** `varietal_categories` will be pre-populated with a comprehensive list before any pipeline runs. Estimated ~130-150 entries: single varietals (~80-100), named blends (~20-30), generic blends (~5), regional designations (~10-15), proprietary catch-alls. To be built during implementation.

---

### 5. Weather & vintage: hybrid appellation + wine level

**Decision: Weather data splits across three levels — appellation-vintage for macro weather, wines for static microclimate, wine-vintages for AI synthesis.**

The core insight: weather is regional, but microclimate is hyper-local. Two vineyards in the same appellation can have completely different elevation, fog exposure, and slope. Combining both levels tells the full story.

---

**`appellation_vintages` table** *(new)*
- `appellation_id` (FK → appellations)
- `vintage_year` (integer)
- PK: composite (`appellation_id`, `vintage_year`)
- Weather metrics: GDD, total rainfall, harvest rainfall, harvest avg temp, spring frost days, heat spike days (exact fields TBD during implementation)
- Compare-to-normal baselines (long-term averages for this appellation — no longer hardcoded in pipeline)
- Growing season dates (appellation-level, hemisphere-aware — no longer hardcoded Apr 1 – Oct 31)
- *The macro weather story. "2022 was a hot, dry year in Stags Leap District." Fetched from Open-Meteo ERA5 once per appellation per vintage. All wines in the same appellation share this data.*

**`wines` table** *(static microclimate fields)*
- `elevation_m` (integer, nullable) — fetchable from elevation API using wine lat/lng. Fully automatable.
- `aspect` (text, nullable — "south-facing") — AI-enriched from producer descriptions or Claude training data.
- `slope` (text, nullable) — AI-enriched.
- `fog_exposure` (text, nullable) — AI-enriched. Mostly static pattern (Sonoma Coast gets morning fog every summer). Vintage variation handled in the AI narrative, not as separate data.
- Additional microclimate fields TBD.
- *The site-specific geography. These are properties of the vineyard location — they don't change vintage to vintage. Elevation is API-derived (high confidence). Aspect, slope, fog are AI-enriched from producer descriptions and Claude's training knowledge (lower confidence, flagged as AI-enriched).*

**`wine_vintages` table** *(replaces current `vintages` table)*
- `wine_id` (FK → wines)
- `vintage_year` (integer)
- PK: composite (`wine_id`, `vintage_year`)
- AI-synthesized fields — **structured, not a single text blob.** Individual fields for different aspects of the vintage story, e.g.:
  - `ai_vintage_summary` — headline assessment
  - `ai_weather_impact` — how weather affected this wine
  - `ai_microclimate_impact` — how site characteristics interacted with this year's weather
  - `ai_drinking_window` — suggested timeline
  - `ai_aging_potential` — cellaring assessment
  - `ai_flavor_impact` — how conditions shaped flavor
  - `ai_quality_assessment` — vintage quality relative to appellation norms
  - `ai_comparison_to_normal` — how this year compared to long-term averages
  - (exact fields TBD during AI layer discussion)
- *The synthesized story. The AI takes appellation weather + wine microclimate + blend composition and generates structured output for each wine-vintage. Structured fields mean the frontend can display pieces independently, fields can be updated individually, and you can compare across wines.*

**Data flow at query time for "2022 Artemis":**
1. Pull static microclimate from `wines` (elevation, aspect, fog)
2. Pull macro weather from `appellation_vintages` (Stags Leap District, 2022)
3. Pull factual vintage data from `wine_vintages` (alcohol, pH, aging regime)
4. Pull blend composition from `wine_vintage_grapes` (that year's specific blend)
5. Pull synthesized analysis from `wine_vintage_insights` (AI narrative, drinking window, etc.)
→ Multiple sources, one story.

**Pipeline flow for enrichment:**
1. Fetch appellation weather from Open-Meteo ERA5
2. Fetch elevation from elevation API (using wine lat/lng)
3. AI-enrich microclimate factors (aspect, fog, slope) from producer descriptions + Claude training data
4. AI-synthesize structured insight fields combining all of the above + blend data
→ Synthesis is the last step — depends on all other data being in place first.

**Blend composition is vintage-specific:**
- `wine_vintage_grapes` — keyed on wine_id + vintage_year + grape_id. The blend can change year to year (common in Bordeaux, many Napa wines).
- `wine_grapes` — general/typical blend, used as fallback when vintage-specific data isn't available.
- AI synthesis uses vintage-specific blend when available, falls back to general blend with lower confidence.

---

### 6. AI layer separation

**Decision: AI-synthesized content lives in separate insights tables. AI-inferred facts stay on main tables with `_source` companion fields.**

**Core principle:** Main tables contain factual, queryable data. AI-synthesized analysis lives in dedicated insights tables. The line is clear — if it's in an insights table, it's AI-generated.

---

#### Source tracking

**`source_types` table** *(new, seeded)*
- `id` (PK, slug)
- `name` (display name)
- `description`
- `category` (first_party, third_party, ai, manual)
- `default_confidence` (decimal 0.0-1.0)

**Seed list:**
- `producer_stated` — first_party, high confidence (human verified this came from the producer)
- `manual` — manual, high confidence (entered by hand)
- `api_derived` — third_party, high confidence (elevation API, weather API)
- `ai_scraped` — ai, medium-high confidence (AI read a real source like a tech sheet and extracted data)
- `ai_inferred` — ai, medium confidence (AI judgment from training knowledge, no specific source)
- `wine_database` — third_party, medium confidence (CellarTracker, Vivino, etc.)
- `publication` — third_party, medium-high confidence (Wine Spectator, Decanter, etc.)
- `importer_stated` — first_party, medium-high confidence (from importer/distributor)

**`_source` companion fields** on main tables. Selective — only for fields where the source actually varies:

On `wines`:
- `varietal_category_id` + `varietal_category_source` (FK → source_types)
- `aspect` + `aspect_source`
- `slope` + `slope_source`
- `fog_exposure` + `fog_exposure_source`

On `wine_grapes` / `wine_vintage_grapes`:
- `percentage` + `percentage_source`

On `producers`:
- `overview` + `overview_source`

**Re-enrichment logic:** If current source confidence > new source confidence → don't overwrite. If current ≤ new → overwrite. `producer_stated` and `manual` never get overwritten by `ai_scraped` or `ai_inferred`.

---

#### Insights tables (AI-synthesized content)

Every insights table has these common fields:
- `confidence` (decimal 0.0-1.0) — per record, not per field. Reflects quality of inputs.
- `enriched_at` (timestamp)
- `refresh_after` (timestamp, nullable — null means no scheduled refresh needed)

**`wine_vintage_insights`**
- `wine_id` + `vintage_year` (composite FK)
- `ai_vintage_summary` — headline assessment
- `ai_weather_impact` — how weather shaped this wine
- `ai_microclimate_impact` — how site interacted with vintage
- `ai_flavor_impact` — how conditions shaped flavor
- `ai_aging_potential` — cellaring assessment
- `ai_drinking_window_start` (integer, year)
- `ai_drinking_window_end` (integer, year)
- `ai_quality_assessment` — quality relative to appellation norms
- `ai_comparison_to_normal` — comparison to long-term averages

**`wine_insights`**
- `wine_id` (FK)
- `ai_wine_summary` — what makes this wine distinctive across vintages
- `ai_style_profile` — house style, what to expect
- `ai_terroir_expression` — how the site shows up in the wine
- `ai_food_pairing` — general pairing suggestions
- `ai_cellar_recommendation` — general aging guidance (not vintage-specific)
- `ai_comparable_wines` — "if you like this, try..."

**`appellation_insights`**
- `appellation_id` (FK)
- `ai_overview` — what defines this appellation
- `ai_climate_profile` — climate characteristics
- `ai_soil_profile` — dominant soil types and effect on wine
- `ai_signature_style` — what wines from here typically taste like
- `ai_key_grapes` — which grapes thrive here and why
- `ai_aging_generalization` — do wines from here age well
- `ai_notable_producers_summary` — who's making the best wine here

**`region_insights`**
- `region_id` (FK)
- `ai_overview` — what defines this region
- `ai_climate_profile` — climate characteristics
- `ai_sub_region_comparison` — how sub-regions differ
- `ai_signature_style` — general wine style
- `ai_history` — winemaking history and evolution

**`producer_insights`**
- `producer_id` (FK)
- `ai_overview` — who they are, philosophy, history
- `ai_winemaking_style` — what defines their approach
- `ai_reputation` — standing in the industry
- `ai_value_assessment` — price-to-quality perspective
- `ai_portfolio_summary` — overview of what they make

**`grape_insights`**
- `grape_id` (FK)
- `ai_overview` — what this grape is, where it comes from
- `ai_flavor_profile` — typical characteristics
- `ai_growing_conditions` — where and how it thrives
- `ai_food_pairing` — general pairing
- `ai_regions_of_note` — where it's most celebrated
- `ai_aging_characteristics` — how wines from this grape typically age

---

#### Trend tables (market/buyer-focused, separate from insights)

**Distinction:** Insights are analytical and relatively stable ("what defines Stags Leap District"). Trends are market-focused and go stale ("2026 buyers are discovering Douro"). Insights have nullable `refresh_after`. Trends always have `refresh_after` populated.

Separate tables per entity, each with the same structure:
- `{entity}_id` (FK)
- `trend_type` (market_trend, emerging_narrative, buyer_sentiment, price_movement)
- `content` (text)
- `confidence` (decimal 0.0-1.0)
- `enriched_at` (timestamp)
- `refresh_after` (timestamp, required)

Tables:
- `appellation_trends`
- `region_trends`
- `country_trends`
- `producer_trends`
- `grape_trends`
- `varietal_category_trends`

---

#### Existing `_meta` table content

**Decision:** The `_meta` tables (country_meta, region_meta, appellation_meta) contain narrative content (overviews, climate descriptions, notable producers). This content migrates into the new insights tables. The AI regenerates it — with 75 wines we only need insights for appellations/regions/countries that have wines in our DB, not all 1,517 appellations.

---

#### Future consideration: blend-level insights

Not building now, but the joins support it. A `wine_grape_insights` or `wine_vintage_grape_insights` table could provide per-grape analysis ("what does the 1.5% Cab Franc contribute to Artemis?" or "why did the winemaker increase Merlot from 5% to 12% in 2022?"). This would join to `wine_grapes` via `wine_id` + `grape_id` or `wine_vintage_grapes` via `wine_id` + `vintage_year` + `grape_id`. Can be added without breaking anything.

---

### 7. Soil modeling

**Decision: Soil is a core differentiator for Loam (the name is literally a soil type). Structured, seeded soil types with quantitative properties that cross-reference with weather data. Three-tier fallback: wine → appellation → region.**

---

**`soil_types` table** *(seeded)*
- `id` (PK, slug)
- `name` ("volcanic", "limestone", "clay", "gravel", "slate", "schist", "loam", "sand", "chalk", "alluvial", "granite", "marl", "silex", "galestro", "tufa", "sandstone", etc.)
- `description`
- `drainage_rate` (decimal 0.0-1.0 — relative scale, 1.0 drains fastest)
- `heat_retention` (decimal 0.0-1.0 — how much heat the soil absorbs and radiates)
- `water_holding_capacity` (decimal 0.0-1.0 — how much moisture it stores)

*Quantitative properties are general characteristics of the soil type, not vineyard-specific measurements. Used for cross-referencing with weather data:*
- *Drainage × rainfall / harvest rainfall*
- *Heat retention × GDD / heat spike days*
- *Water holding capacity × rainfall / drought*

**`soil_type_insights`** *(AI-synthesized)*
- `soil_type_id` (FK)
- `ai_overview` — what this soil type is geologically
- `ai_wine_impact` — how it affects wine character
- `ai_notable_regions` — where this soil is most celebrated
- `ai_drainage_explanation` — why it drains the way it does
- `ai_best_grapes` — which grapes thrive on this soil and why
- `confidence`, `enriched_at`, `refresh_after` (nullable)

**`wine_soils`** — most specific
- `wine_id` + `soil_type_id` (composite PK)
- `role` (nullable — primary, secondary, subsoil, bedrock. Captures soil layering when available. Most sources just list soil types without layering, so this is nullable.)
- `source` (FK → source_types)

**`appellation_soils`** — fallback
- `appellation_id` + `soil_type_id` (composite PK)
- `role` (nullable)
- `source` (FK → source_types)

**`region_soils`** — broadest fallback
- `region_id` + `soil_type_id` (composite PK)
- `role` (nullable)
- `source` (FK → source_types)

**Fallback logic:** wine_soils → appellation_soils → region_soils. Confidence in insights reflects which level was used.

**Soil × weather AI synthesis example:** "The volcanic topsoil provided excellent drainage during the wet 2021 vintage, while the clay subsoil retained enough deep moisture to sustain the vines through the dry summer." This cross-reference is generated in `wine_vintage_insights` using soil data + weather data.

**Note:** `soil_type_trends` table deferred for now. Soil types don't trend the way regions or producers do — "Etna wines are trending" is captured in `region_trends`, the volcanic soil correlation is implicit. Can add later if needed.

---

### 8. Terroir fields on wines

**Decision: Additional terroir attributes on the `wines` table that contribute to the full picture of why a wine tastes the way it does.**

---

**Vine age:**
- `vine_planted_year` (integer, nullable) — earliest or primary planting year. Age derived from current year minus this. More stable than storing age directly.
- `vine_age_description` (text, nullable) — captures complexity like "vines ranging from 20 to 80 years, average 45"
- `vine_planted_year_source` (FK → source_types)

**Irrigation:**
- `irrigation_type` (enum: dry_farmed, irrigated, deficit_irrigation — nullable)
- `irrigation_type_source` (FK → source_types)

*Dry-farmed vines produce more concentrated fruit. Deficit irrigation is a controlled middle ground. Common in New World regions. Often stated by producers.*

**Farming certifications:**

**`farming_certifications` table** *(seeded)*
- `id` (PK, slug)
- `name` ("USDA Organic", "Demeter Biodynamic", "SIP Certified", "LIVE Certified", "Conventional", etc.)
- `description` (nullable)

**`wine_farming_certifications` join table**
- `wine_id` + `farming_certification_id` (composite PK)
- `source` (FK → source_types)

*A wine can hold multiple certifications (organic + biodynamic). No rows = unknown. A row for "Conventional" = confirmed no certifications. Distinguishes "we don't know" from "we know they're conventional."*

---

**Water bodies:**

**`water_bodies` table** *(seeded)*
- `id` (PK, slug)
- `name` ("Pacific Ocean", "Gironde Estuary", "Lake Geneva", etc.)
- `type` (ocean, sea, river, lake, estuary)
- `description` (nullable)

**`water_body_insights`** *(AI-synthesized)*
- `water_body_id` (FK)
- `ai_overview` — what this water body is
- `ai_wine_impact` — how it affects nearby vineyards (temperature moderation, fog, humidity)
- `ai_notable_regions` — which wine regions it influences
- `confidence`, `enriched_at`, `refresh_after` (nullable)

**`wine_water_bodies`** — most specific
- `wine_id` + `water_body_id` (composite PK)
- `distance_km` (decimal, nullable)
- `direction` (text, nullable — "west", "southwest")
- `source` (FK → source_types)

**`appellation_water_bodies`** — fallback
- `appellation_id` + `water_body_id` (composite PK)
- `distance_km` (nullable)
- `direction` (nullable)
- `source` (FK → source_types)

**`region_water_bodies`** — broadest fallback
- `region_id` + `water_body_id` (composite PK)
- `distance_km` (nullable)
- `direction` (nullable)
- `source` (FK → source_types)

---

**Harvest dates** (on `wine_vintages`):
- `harvest_start_date` (date, nullable)
- `harvest_end_date` (date, nullable)
- `harvest_date_source` (FK → source_types)

*Factual dates when available (some producers publish on tech sheets). AI harvest analysis lives in `wine_vintage_insights.ai_harvest_analysis` — cross-references harvest timing with weather data.*

**`wine_vintage_insights`** additional field:
- `ai_harvest_analysis` — AI cross-references harvest dates with weather to generate insight ("dodged the October rains", "harvested two weeks early for the vintage")

---

**Diurnal temperature range** (on `appellation_vintages`):
- Can be calculated from Open-Meteo daily min/max temperature data
- Average daily high minus daily low over the growing season
- Big diurnal range = ripe fruit + preserved acidity (high altitude, continental climates)
- Small diurnal range = faster ripening, less acidity retention (maritime climates)
- Exact field TBD during implementation — may be `avg_diurnal_range_c` (decimal)

---

**Still to discuss (terroir):**
- **Vegetation and land use** — surrounding environment, biodiversity. Hard to quantify. May be better as AI-inferred context in insights rather than structured data. Revisit later.

---

## Foundational Principle: Don't Create When Content Already Exists

**This is a core Loam principle that governs the entire AI layer.**

Producer-written content is always better than AI-generated prose. A winemaker's own description of their wine — their harvest story, their terroir narrative, their tasting notes — is authentic, authoritative, and carries the voice of the producer. AI should never replace this.

**The hierarchy:**
1. **Link to original content** — if a producer has a tech sheet, fact sheet, or vintage narrative, link to it and display it. This is the gold standard.
2. **Scrape facts into structured fields** — extract blend percentages, alcohol, production volume, harvest dates, winemaking methods into normalized fields for querying and comparison.
3. **AI fills gaps only** — when no original content exists, AI generates insights and descriptions. These are clearly marked as AI-generated and carry confidence scores.

**Example:** The 2013 Cain Five fact sheet (cainfive.com) contains blend percentages, production volume, release date, alcohol, harvest narrative, winemaking details, farming practices, and ingredients — all in the producer's own words. The pipeline should:
- Link to the PDF
- Extract structured data (51% Cab Sauv, 22% Merlot, 14.3% ABV, 5,447 cases, etc.)
- Store the winemaker's harvest narrative as a reference, not regenerate it with AI
- Only use AI for cross-referencing (weather × soil × blend) and filling gaps

**This principle applies everywhere:** Don't generate an AI overview of a producer when their "About" page exists. Don't write AI tasting notes when the winemaker published their own. Don't create AI terroir descriptions when the appellation authority has published one. Link first, scrape second, generate last.

---

### 9. Source documents

**Decision: Track and link to original source documents (tech sheets, fact sheets, press releases) as first-class entities. These are the authoritative content that AI supplements, not replaces.**

**`wine_vintage_documents`** — links to original source material
- `id` (PK)
- `wine_id` + `vintage_year` (FK)
- `url` (text — link to the PDF or page)
- `document_type` (enum: tech_sheet, fact_sheet, press_release, tasting_note, vintage_report, winemaker_note)
- `title` (text, nullable — "2013 Cain Five Factsheet")
- `source` (FK → source_types — typically producer_stated)
- `discovered_at` (timestamp)
- `last_verified_at` (timestamp, nullable — when we last confirmed the URL still works)

**`producer_documents`** — producer-level documents (about pages, philosophy statements, history)
- `id` (PK)
- `producer_id` (FK)
- `url` (text)
- `document_type` (enum: about_page, philosophy, history, press_kit)
- `title` (text, nullable)
- `source` (FK → source_types)
- `discovered_at` (timestamp)
- `last_verified_at` (timestamp, nullable)

**`appellation_documents`** — appellation authority publications
- `id` (PK)
- `appellation_id` (FK)
- `url` (text)
- `document_type` (enum: regulatory, overview, map, vintage_report)
- `title` (text, nullable)
- `source` (FK → source_types)
- `discovered_at` (timestamp)
- `last_verified_at` (timestamp, nullable)

**Pipeline behavior:**
1. Discover source documents (producer websites, search)
2. Extract structured data into normalized fields
3. Store the document link as a reference
4. AI generates insights only for gaps not covered by source documents
5. Display original content prominently, AI content as supplement

---

## Still to decide

- **Wine attributes** — sensory profile, production methods, chemical data, aging details (comprehensive wine data for dataset sales)
- **Pricing and market data** — release price, current market price, price history (structure identified, data sourcing TBD)
- **Remaining terroir details** — vegetation/land use, any additional microclimate factors
- **Schema implementation** — actual SQL, migrations, seed data
- **Pipeline updates** — how enrichment pipeline adapts to new schema
- **Frontend implications** — how the new data model affects the UI