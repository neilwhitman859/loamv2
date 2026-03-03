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

- **Terminology audit** — verify all field names and enums against industry standard vocabulary before final implementation
- **CellarTracker data access** — private API, limits free access. Alternative sources for community scores and pricing TBD.
- **Wine name field** — confirmed as stored field, exact naming conventions TBD
- **Schema implementation** — actual SQL, migrations, seed data
- **Pipeline architecture** — complete redesign, separate discussion once schema is locked

---

### 11. Critic and community scores

**Decision: Dedicated `wine_vintage_scores` table with full provenance. Publications as a seeded FK table. Multiple scores per wine-vintage supported — superseded scores flagged rather than deleted.**

---

**`publications` table** *(seeded)*
- `id` PK, slug
- `name` — "Wine Spectator", "Wine Advocate", "Decanter", "James Suckling", "CellarTracker", etc.
- `country` text, nullable
- `url` text, nullable
- `type` enum (critic_publication, community, auction_house)

---

**`wine_vintage_scores` table**
- `id` PK
- `wine_id` + `vintage_year` — vintage_year nullable for NV wines
- `score` integer
- `score_low` integer, nullable — for ranges (e.g. barrel sample "92-95")
- `score_high` integer, nullable
- `score_scale` integer — original scale as published (100, 20, 5). No normalization — display layer handles conversion.
- `publication_id` FK → publications
- `critic` text, nullable — individual critic name where applicable
- `tasting_note` text, nullable — descriptive tasting note
- `review_text` text, nullable — general commentary, context, non-tasting note content
- `drinking_status` enum (too_young, approachable, at_peak, declining), nullable — critic's qualitative call on where the wine is right now
- `blind_tasted` boolean, nullable — whether the wine was tasted blind
- `critic_drink_window_start` integer, nullable — critic's raw window recommendation as published
- `critic_drink_window_end` integer, nullable
- `review_date` date, nullable
- `review_type` enum (final, barrel_sample, early_release, retrospective)
- `is_community` boolean — true for CellarTracker and aggregated community scores, false for individual critic scores
- `rating_count` integer, nullable — number of community ratings, populated for community scores only
- `is_superseded` boolean, default false — flagged true when a critic publishes an updated score for the same wine-vintage. Keeps history intact, newer row is the active one.
- `url` text, nullable — link to original review
- `source` FK → source_types
- `discovered_at` timestamp

**Data sources:**
- CellarTracker API — community scores, rating count, community average price
- Producer websites — release prices, sometimes critic scores published as accolades
- Both are free and legitimate

**Notes:**
- Multiple scores from the same publication/critic for the same wine-vintage are supported — re-reviews happen. `is_superseded` distinguishes active from historical.
- Community scores (CellarTracker) displayed differently from critic scores in the UI — rating count is as meaningful as the score itself for community data.
- Critic drink window fields on this table are the raw per-review data. `critic_drinking_window_start/end` on `wine_vintage_insights` is Loam's synthesized view across all critic input.

---

### 10. Wine attributes

**Decision: Sensory, chemical, and production method fields split across `wines` (house style) and `wine_vintages` (vintage-specific). All nullable. Source tracking grouped by category.**

---

#### Sensory profile — on `wine_vintages`

Stored as integer 1–5 using WSET-aligned scale. Vintage-specific because vintage conditions meaningfully affect expression year to year.

- `acidity` integer 1–5 (1 = flat, 2 = soft, 3 = medium, 4 = firm, 5 = brisk)
- `tannin` integer 1–5 (1 = none, 2 = soft, 3 = medium, 4 = firm, 5 = grippy)
- `body` integer 1–5 (1 = light, 2 = light-medium, 3 = medium, 4 = medium-full, 5 = full)
- `alcohol_level` integer 1–5 (1 = low <11%, 2 = medium-low 11–12.5%, 3 = medium 12.5–13.5%, 4 = medium-high 13.5–14.5%, 5 = high >14.5%)
- `alcohol_pct` decimal — precise % from label or tech sheet

Scale definitions live in code/docs, not the DB. AI-inferred or extracted from tech sheets. Source tracked per field via sensory source grouping (TBD during pipeline design).

---

#### Chemical data — on `wine_vintages`

All nullable decimals. Never AI-inferred — either on the tech sheet or null. Single shared source field covers all chemical fields.

- `ph` decimal
- `ta_g_l` decimal — titratable acidity in g/L
- `rs_g_l` decimal — residual sugar in g/L
- `va_g_l` decimal — volatile acidity in g/L
- `so2_free_mg_l` decimal
- `so2_total_mg_l` decimal
- `chemical_data_source` FK → source_types

**Availability note:** pH and alcohol_pct are most commonly published. TA sometimes. RS matters for sweet/semi-sweet styles. VA and SO2 are rarely published proactively — more commonly stated when absent ("unfined, unfiltered") or when low SO2 is a selling point.

---

#### Production methods

Split between house style (wine-level) and vintage-variable fields (vintage-level). Source tracking grouped into two buckets.

**On `wines` (house style — rarely changes vintage to vintage)**
- `oak_origin` enum (french, american, slavonian, hungarian, mixed, none), nullable
- `yeast_type` enum (native, commercial, mixed), nullable
- `fining` enum (unfined, fined, partial), nullable
- `filtration` boolean, nullable
- `closure` enum (cork, screwcap, diam, wax, other), nullable
- `fermentation_vessel` enum (barrel, stainless, concrete, amphora, foudre, mixed), nullable
- `oak_source` FK → source_types — covers all oak-related fields on `wines`

**On `wine_vintages` (vintage-specific or ambiguous)**
- `duration_in_oak_months` integer, nullable
- `new_oak_pct` integer, nullable
- `neutral_oak_pct` integer, nullable — middle (1-2yr oak) is implied: 100 - new_oak_pct - neutral_oak_pct
- `whole_cluster_pct` integer, nullable
- `bottle_aging_months` integer, nullable
- `carbonic_maceration` boolean, nullable
- `mlf` enum (full, partial, none), nullable
- `winemaking_source` FK → source_types — covers all winemaking fields on `wine_vintages`

**Availability note:** Duration in oak, closure, and alcohol_pct are most commonly published. % new oak common for premium wines. Whole cluster % increasingly common for Pinot/Syrah producers. Yeast type common for natural-leaning producers, rarely stated by conventional ones. Fining/filtration often stated only when absent ("unfined"). VA rarely published proactively.

---

#### Aging details

Three tiers of aging guidance, each with distinct provenance. Drink now is a frontend calculation (today's date vs. window fields) — not stored.

**On `wine_insights` (vintage-agnostic baseline)**

Relative years from vintage, not absolute years. Used for NV wines and as a house style reference when vintage-specific data isn't available. AI-synthesized from varietal, appellation, and producer house style.

- `typical_drinking_window_years` integer — how many years this wine typically needs before drinking
- `typical_aging_potential_years` integer — outer bound of how long it can age
- `typical_peak_start_years` integer — years from vintage to peak start
- `typical_peak_end_years` integer — years from vintage to peak end

**On `wine_vintage_insights` (vintage-specific)**

Three parallel sets of fields — critic assessment, weather-based calculation, and AI synthesis. Stored separately so the user can compare provenance and see where consensus lies or diverges.

*Critic:*
- `critic_drinking_window_start` integer (year)
- `critic_drinking_window_end` integer (year)
- `critic_peak_start` integer (year)
- `critic_peak_end` integer (year)
- `critic_window_source` FK → source_types

*Calculated (weather-based — Loam's own projection):*
- `calculated_drinking_window_start` integer (year)
- `calculated_drinking_window_end` integer (year)
- `calculated_peak_start` integer (year)
- `calculated_peak_end` integer (year)
- `calculated_window_explanation` text — AI-generated prose explaining how the calculation was derived. References actual data inputs (GDD, harvest conditions, appellation aging curve, oak regime, blend) so the user can follow the reasoning.

*AI synthesis:*
- `ai_drinking_window_start` integer (year)
- `ai_drinking_window_end` integer (year)
- `ai_peak_start` integer (year)
- `ai_peak_end` integer (year)
- `ai_window_explanation` text — AI-generated prose explaining the holistic judgment. Not an average of critic and calculated — a reasoned synthesis that weights all inputs.

**Display principle:** Show all three tiers with provenance labels. Disagreement between tiers is itself informative — surfaces as a signal to the user ("critics are more bullish on this vintage than the weather data suggests").
---

## Foundational Principle: Model Industry Norms

**This principle governs all naming, vocabulary, and structural decisions.**

Loam is designed to be sold as a dataset. That means the schema must be recognizable and trustworthy to wine industry buyers — sommeliers, importers, critics, developers building wine tools. Idiosyncratic naming or structure creates friction and reduces value.

**What this means in practice:**
- Use industry-standard vocabulary throughout — WSET terminology for sensory descriptors, standard appellation/region names, recognized certification names
- Follow how major wine databases model their data (Wine-Searcher, CellarTracker, Wine Advocate) where known
- Standard score scales (100-point primary)
- Standard geographic hierarchy (country → region → appellation) — already implemented
- No invented terminology when an industry term exists
- Field names should be self-explanatory to a wine professional without a data dictionary

**Terminology audit** — before final schema implementation, all enums and field names should be verified against industry standard vocabulary. This is a "still to decide" item.

---

### 12. Grapes table — fact/insight split

**Decision: `grapes` table stays lean as a botanical fact table. All interpretive content moves to `grape_insights`.**

**Reasoning:** Consistent with the fact/insight split applied across all entities. At 10,000 wines, the discipline of the separation pays off — re-enrichment is cleaner, source provenance is explicit, and the pattern is predictable for anyone reading the schema.

**`grapes` table — botanical facts only:**
- `id` PK, slug
- `name`
- `aliases` array — recognized synonyms (Syrah = Shiraz = Hermitage)
- `color` enum (red, white, pink, grey)
- `origin_country_id` FK → countries — where the grape originated

**Dropped from v1:**
- `is_blend`, `blend_components`, `common_blends` — redundant with `varietal_categories`, which handles blend identity
- `description`, `climate`, `acidity`, `tannin`, `body`, `sweetness`, `oak_treatment`, `aging_potential` — interpretive, move to `grape_insights`
- `primary_flavors`, `secondary_flavors`, `tertiary_flavors` — interpretive, move to `grape_insights`
- `key_regions`, `region_notes`, `blend_notes` — interpretive, move to `grape_insights`
- `sort_order` — not needed

**`grape_insights` table** (already defined in Decision 6) covers all interpretive content.

---

### 13. NV (Non-Vintage) wines

**Decision: `is_nv` boolean on `wines`. NV wines use null `vintage_year` throughout the schema. NV-specific fields added to `wine_vintages`.**

**Industry standard:** Major wine databases (Wine-Searcher, CellarTracker, Wine Advocate) use vintage year as the primary identifier with NV as a special case — not a parallel track. Loam follows this convention.

**NV wines are not all the same:**
- **Annual releases** (Champagne NV) — different disgorgement each year, collectors care about disgorgement date
- **Continuous production** (Sherry Solera) — ongoing blend, no meaningful release date
- **Age statements** (Tawny Port 10yr, 20yr) — not a vintage but meaningful time information

**On `wines`:**
- `is_nv` boolean — flags this as a non-vintage wine

**On `wine_vintages` — additional NV fields (all nullable):**
- `disgorgement_date` date — for Champagne and similar annual NV releases
- `age_statement_years` integer — for Tawny Port, aged Sherry
- `solera_system` boolean, default false — for continuous blend wines

**Behavior:**
- NV wines have `vintage_year` = null on `wine_vintages`
- Weather data (`appellation_vintages`) does not apply to NV wines — no vintage year to key on
- Aging/drinking window fields on `wine_vintage_insights` still apply — NV wines can have drinking windows
- `wine_insights` typical aging fields are the primary aging reference for NV wines

---

### 14. Pricing

**Decision: Dedicated `wine_vintage_prices` table for valuation data. Separate from commerce/affiliate data (future `wine_vintage_listings` table). Simple for v1, infrastructure in place for expansion.**

**Data sources (free):**
- CellarTracker API — community average price, price range
- Producer websites — release price, scraped per wine

**`wine_vintage_prices` table:**
- `id` PK
- `wine_id` + `vintage_year` FK → wine_vintages (vintage_year nullable for NV)
- `amount` decimal
- `currency` text (USD, EUR, GBP, etc.)
- `price_type` enum (release, community_average, community_low, community_high, auction_avg)
- `source` FK → source_types
- `recorded_at` timestamp — when this price was recorded/scraped
- `url` text, nullable

**Value assessment** — AI judgment on whether the wine is fairly priced relative to quality — lives on `wine_vintage_insights` as `ai_value_assessment` text. Not in the price table — it's interpretation, not data.

**Future:** A separate `wine_vintage_listings` table for retail/affiliate links when partnerships exist. Pricing table stays pure valuation data.

---

### 15. Search and disambiguation

**Decision: Lightweight `wine_candidates` table for search disambiguation before enrichment. On-demand enrichment triggered after user selects the correct wine.**

**Problem:** Full enrichment takes 30+ seconds. Users shouldn't wait through enrichment only to find they searched for the wrong wine. Disambiguation comes first.

**Flow:**
1. User searches "Stag's Leap Cabernet"
2. Query runs against `wine_candidates` — fast, lightweight
3. User picks specific wine ("Armillary 2020")
4. Check if wine exists in full `wines` table
5. If not — trigger enrichment pipeline
6. Show enriched wine page

**`wine_candidates` table** *(seeded with ~20,000 wines)*
- `id` PK
- `producer_name` text
- `wine_name` text
- `primary_grape` text
- `vintage_years` integer array — list of known available vintages
- `source_url` text, nullable
- `wines_id` FK → wines, nullable — populated once this candidate has been fully enriched

**Note:** Matching logic between `wine_candidates` and `wines` (how a candidate maps to a full wine record) to be defined during pipeline implementation.

---

### 16. Vegetation, land use, and biodiversity

**Decision: Wine-level narrative field in `wine_insights`. Producer content scraped first, AI fills gaps. Biodiversity certifications as a seeded join table.**

**Why vegetation matters to wine:**
- **Pest and disease management** — native vegetation supports beneficial insects, reduces intervention, affects sulfite levels
- **Microclimate modulation** — trees and water features moderate temperature, affecting ripeness and acidity
- **Soil biology** — diverse vegetation supports mycorrhizal fungi and bacteria, affecting mineral expression
- **Water retention** — surrounding land affects vineyard water availability

Wine-level (not appellation-level) because a specific vineyard's surroundings matter more than regional averages. Winemakers often describe this themselves on websites and tech sheets — scrape first, AI fills gaps.

**On `wine_insights`:**
- `ai_vegetation_and_land_use` text — narrative description of surrounding ecosystem, biodiversity, land management. Example: "The vineyard sits within a mixed agricultural landscape interspersed with native Mediterranean oak forest and garrigue scrubland. This biodiversity supports natural pest predation and moderates summer heat through forest proximity."
- `vegetation_source` FK → source_types — `producer_stated` if from producer materials, `ai_inferred` if Claude-generated
- `vegetation_confidence` decimal 0.0–1.0

**`biodiversity_certifications` table** *(seeded)*
- `id` PK, slug
- `name` ("Regenerative Organic Certified", "Wildlife Friendly Farming", "Audubon Certified", "Land to Market Verified", "Carbon Neutral Certified", etc.)
- `description` text, nullable
- `url` text, nullable — link to certification body

**`wine_biodiversity_certifications` join table**
- `wine_id` FK
- `biodiversity_certification_id` FK
- `source` FK → source_types
- PK: composite (wine_id, biodiversity_certification_id)

---

### 17. Primary keys — UUIDs everywhere

**Decision: UUIDs as primary key on every table. Separate `slug` field on human-facing entities for URL display.**

**Reasoning:**
- Globally unique — no collisions when merging datasets or integrating with external systems
- Industry standard for sellable datasets
- No sequence exposure — buyers cannot infer dataset size from IDs
- Safe for distributed pipeline runs — IDs generated independently never conflict

**Implementation:**
- Every table PK is a UUID, generated automatically by Postgres (`gen_random_uuid()`)
- `slug` field (text, unique) added to: `wines`, `producers`, `appellations`, `varietal_categories`, `regions`, `countries`
- Slug is the URL-friendly identifier (e.g. `stags-leap-armillary`). Can change without breaking anything because UUID is the real FK reference throughout the schema.
- Slugs generated from name at insert time, normalized (lowercase, no accents, hyphens for spaces)

**Note:** Section 12 (`grapes` table) shows `id` PK, slug — this should be read as UUID PK + separate slug field, consistent with this decision.

---

### 18. Soft deletes

**Decision: `deleted_at` timestamp on all core tables. Null = active, populated = deleted. No hard deletes.**

**Reasoning:**
- Pipeline runs frequently and can make mistakes — soft deletes allow recovery
- Audit trail for dataset integrity — buyers expect to know what changed
- Consistent pattern across all tables

**Implementation:**
- `deleted_at` timestamptz, nullable, default null — on every core table
- All standard queries filter `WHERE deleted_at IS NULL`
- Consider a Postgres view per table that applies this filter automatically
- `deleted_at` is never overwritten once set — if a record needs to be restored, set it back to null

**Core tables this applies to:** `wines`, `producers`, `wine_vintages`, `appellations`, `regions`, `countries`, `grapes`, `varietal_categories`, `wine_grapes`, `wine_vintage_grapes`, `appellation_vintages`, and all insights/scores/pricing tables.

---

### 19. Duplicate detection

**Decision: Three-layer approach. Normalize + slug match as first pass, AI-assisted candidate review as second pass, external IDs as confirmation when available.**

**The problem:** No global standard wine identifier exists. Wine names vary ("Lopez de Heredia" vs "R. Lopez de Heredia"), and the pipeline will encounter the same wine described multiple ways across different sources.

**Three distinct duplicate problems:**
- **Producers** — same producer, different name strings
- **Wines** — same wine, different label descriptions
- **Vintages** — unambiguous once wine is correctly identified

**Layer 1 — Normalization + slug matching**
Before any insert, normalize producer name and wine name: remove accents, lowercase, strip punctuation, handle common abbreviations. Generate normalized slug. Check for existing match. Catches most cases.

**Layer 2 — AI-assisted candidate review**
When normalized slug match isn't found, query for wines with similar producer + region + varietal. Claude reviews candidates: "Is this new wine the same as any of these existing records?" Only runs when Layer 1 doesn't find a clean match.

**Layer 3 — External IDs as confirmation**
Store external database IDs as nullable fields. When two records share an external ID they are definitively the same wine.

**Schema additions:**

On `wines`:
- `normalized_name` text — cleaned, accent-stripped, lowercase version of wine name used for matching
- `duplicate_of` UUID FK → wines, nullable — if a duplicate is detected after the fact, points to the canonical record rather than deleting

On `producers`:
- `normalized_name` text — same normalization approach

On `wine_vintages`:
- `cellartracker_id` text, nullable
- `wine_searcher_id` text, nullable
- `vivino_id` text, nullable

On `grapes`:
- `vivc_number` text, nullable — VIVC (Vitis International Variety Catalogue) number. The global standard for grape variety identification used by researchers and regulators worldwide.

**Note on CellarTracker:** CellarTracker's API is private — used only by select third-party apps. It is not a freely accessible public API. Data access options to be determined. This affects the scores and pricing data sourcing strategy.

**Ongoing concern:** Duplicate detection is not fully solved. No free, accessible, comprehensive external ID system for wines exists. Monitor real-world pipeline behavior and revisit as better data sources are found.

---

### 20. Enrichment status tracking

**Decision: Dedicated `wine_enrichment_log` table tracking each pipeline stage independently. Simple status fields on `wines` for overall state.**

**Reasoning:** At 10,000 wines with a multi-stage pipeline, a single status field on `wines` doesn't give enough visibility. You need to know which stage failed, resume from failure points, and avoid re-running expensive stages unnecessarily. Null fields in a complete record are expected and legitimate — not every field will be populated for every wine.

**On `wines` (overall status):**
- `enrichment_status` enum (pending, partial, complete, failed) — overall pipeline state
- `enriched_at` timestamptz — when enrichment last ran
- `enrichment_notes` text, nullable — failure details for debugging

**`wine_enrichment_log` table (per-stage tracking):**
- `id` UUID PK
- `wine_id` UUID FK → wines
- `vintage_year` integer, nullable — null for wine-level stages, populated for vintage-specific stages
- `stage` enum:
  - `candidate_matching`
  - `document_discovery`
  - `document_extraction`
  - `weather_fetch`
  - `elevation_fetch`
  - `external_ids`
  - `scores_fetch`
  - `pricing_fetch`
  - `ai_microclimate`
  - `ai_wine_insights`
  - `ai_vintage_insights`
- `status` enum (pending, running, complete, failed, confirmed_null, not_applicable)
- `started_at` timestamptz
- `completed_at` timestamptz, nullable
- `error_message` text, nullable
- `source` UUID FK → source_types, nullable

**Null handling philosophy:**
- `enriched_at` is always populated when a pipeline stage runs, even if it finds nothing
- `enrichment_status` = complete means the pipeline finished, not that every field is populated
- A null field on a complete record means the pipeline looked and couldn't find the data — confirmed null by implication
- `status` = confirmed_null on a log row means: we looked, it genuinely doesn't exist. Pipeline skips on re-enrichment.
- `status` = not_applicable means: this stage doesn't apply to this wine (e.g. weather_fetch for an NV wine)
