# Loam — v2 Context Document

*This document captures the state of v1 and the direction for v2. It is a reference for the AI pipeline, schema design, and anyone picking up the project fresh.*

---

## What Is Loam

Loam is a wine data product. The core proposition: every wine record is enriched with the full story of *why* it tastes the way it does — the place, the vintage weather, the soil, the grapes, the producer's choices. It is not a review site or a marketplace. It is a structured wine intelligence layer.

The name is a soil type — a signal that terroir is central to the product's identity.

---

## Tech Stack

- **Database:** Supabase (Postgres)
- **Enrichment pipeline:** Node.js scripts, run locally or on a schedule
- **AI models:** Anthropic Claude (Haiku for extraction, Opus/Sonnet for synthesis and narrative)
- **Weather data:** Open-Meteo ERA5 historical climate API
- **Elevation data:** Elevation API (coordinate-based lookup)
- **Frontend:** Separate codebase — consumes Supabase directly. Not changing in v2.

---

## v1 Data Snapshot

At the time of the v2 rebuild:

| Entity | Count |
|---|---|
| Wines | 75 |
| Vintages (wine-level weather + AI content) | 75 |
| Grapes | 149 |
| Appellations (`appellation_meta`) | 1,517 |
| Regions (`region_meta`) | 317 |
| Countries (`country_meta`) | 59 |

The 75 wines are the actual catalog — the 1,517 appellations are a reference list, most of which have no wines attached.

---

## v1 Data Model — What Existed

### Core tables
- **`wines`** — one row per wine. Fields: id (slug), name, producer (text), varietal (text array), varietal_pct (jsonb), lat/lng, soil_type (text), elevation_m, aspect, appellation_names (text array), is_nv, price range, oak_aging_years, aging_potential range, wine_searcher_url.
- **`vintages`** — one row per wine-vintage. Held *both* weather data *and* AI-synthesized content. This conflation is the primary structural problem v2 fixes.
- **`grapes`** — 149 rows. Botanical grape entities with viticultural characteristics, flavor profiles, key regions. Well-structured; largely carried into v2.

### Geography (as `_meta` content tables, not structured entities)
- **`country_meta`**, **`region_meta`**, **`appellation_meta`** — flat content tables with overview text, climate descriptions, notable producers as text arrays. Regions and appellations were soft-linked to wines via join tables (`wine_regions`, `wine_countries`, `wine_appellations`) but the geo tables themselves were not structured core entities.

### Join tables
- `wine_regions`, `wine_countries`, `wine_appellations` — many-to-many between wines and the meta tables.

### What was missing in v1
- No `producers` table — producer was a plain text field on `wines`
- No `varietal_categories` — wine style classification was a text field (`style`, largely unpopulated) and a text array (`varietal`)
- No structured soil table — `soil_type` was a free-text field on `wines`
- No appellation-level weather — weather was fetched per wine, per vintage, using the wine's lat/lng. 10 Rioja wines = 10 weather calls with slightly different results
- No source tracking — AI-synthesized content had `_confidence` and `_source` text fields, but no normalized source taxonomy
- No separation of AI insights from factual data — everything lived in `vintages`

---

## v1 Enrichment Pipeline — How It Worked

### Overview
Node.js scripts, run sequentially per wine. The pipeline was wine-centric: given a wine, enrich it.

### Steps (in order)
1. **Seed wines** from `wines.js` — a hand-curated JS file with wine objects (id, name, producer, varietal, lat/lng, appellation names, basic facts)
2. **Fetch weather** — call Open-Meteo ERA5 using the wine's lat/lng for the vintage year. Compute GDD, total rainfall, harvest rainfall, harvest avg temp, spring frost days, heat spike days. Store on `vintages`.
3. **Fetch elevation** — elevation API lookup using lat/lng. Store on `wines.elevation_m`.
4. **AI enrichment (Haiku)** — extract structured fields: blend percentages, alcohol, acidity, tannin, body, climate type, production notes, food pairing, flavor arrays. Source: Claude's training knowledge + any data sheets available.
5. **AI synthesis (Sonnet/Opus)** — generate narrative fields: ai_headline, ai_terroir_note, ai_vintage_note, ai_flavor_intel, ai_drink_window, ai_value_verdict. These combined weather data + terroir context + varietal knowledge into prose.
6. **Confidence + source tagging** — each AI field got a `_confidence` (decimal) and `_source` (text: "established", "inferred", "uncertain") companion field.

### Wine data sheet analysis (carried into v2)
A key v1 capability: when a producer publishes a tech sheet or fact sheet (PDF or web page), the pipeline could ingest it and extract structured data — blend percentages, alcohol, harvest dates, production volume, winemaking notes, aging regime. This is higher-confidence than AI inference from training knowledge. The pipeline used Claude Haiku to parse the document and extract fields into structured JSON before writing to the DB.

This capability is preserved and extended in v2. Source documents are now first-class entities (`wine_vintage_documents`, `producer_documents`). Extracted data carries `producer_stated` source type (highest confidence). AI synthesis only fills gaps not covered by source documents.

### What didn't work well in v1
- **Weather noise** — per-wine weather created meaningless variation between wines in the same appellation. ERA5 grid resolution meant wines 2km apart got slightly different numbers that were artifacts of the grid, not real climate differences.
- **No appellation weather baseline** — growing season dates were hardcoded (Apr 1 – Oct 31 for Northern Hemisphere), not appellation-aware. No long-term normals for compare-to-average analysis.
- **AI content mixed with facts** — `vintages` held both `growing_degree_days` (factual, API-derived) and `ai_vintage_note` (synthesized prose) in the same table with no clear separation. Re-enrichment was messy.
- **No re-enrichment logic** — the pipeline could overwrite AI content with lower-confidence data. No source hierarchy enforced.
- **Producer as text** — couldn't query "all wines by Lopez de Heredia" structurally. No producer pages, no producer-level enrichment.
- **Soil as text** — `soil_type` was a descriptive string ("Volcanic tuff and rocky alluvial soils"). Couldn't cross-reference with weather data. Couldn't filter by soil type.
- **No varietal classification** — `varietal` was a text array (["Cabernet Sauvignon", "Merlot"]). No normalized classification layer to answer "show me all Bordeaux Blends."

---

## v2 Direction — Summary

The full schema spec is in `schema-decisions.md`. Summary of the structural changes:

### What changes
| v1 | v2 |
|---|---|
| `_meta` content tables for geo | Structured core entities: `countries`, `regions` (self-referencing), `appellations` |
| Producer as text field | `producers` table, wines FK to it |
| Varietal as text array | `varietal_categories` (pre-seeded controlled list) + `wine_grapes` join table |
| Wine-level weather | `appellation_vintages` — weather fetched once per appellation per year |
| All enrichment in `vintages` | Factual data on `wine_vintages`; AI synthesis in `wine_vintage_insights` |
| Soil as free text | `soil_types` seeded table + `wine_soils`, `appellation_soils`, `region_soils` join tables |
| No source tracking | `source_types` table + `_source` companion fields on AI-inferred facts |
| No document tracking | `wine_vintage_documents`, `producer_documents`, `appellation_documents` |
| AI content mixed with facts | Dedicated insights tables per entity: `wine_insights`, `wine_vintage_insights`, `appellation_insights`, `producer_insights`, `grape_insights`, etc. |

### What stays the same
- `grapes` table structure (mostly) — joins and keys are right, content carries over
- The wine data sheet extraction capability — extended, not replaced
- The frontend — new schema, same queries reshaped to fit
- The general pipeline architecture — Node.js, Claude, Open-Meteo, elevation API

### New pipeline order (v2)
1. Seed core entities: countries, regions, appellations, producers, varietal_categories, soil_types, water_bodies, farming_certifications
2. Seed wines with FKs to all of the above
3. Fetch appellation-level weather (Open-Meteo ERA5, one call per appellation per vintage year)
4. Fetch elevation per wine (coordinate lookup)
5. Discover and ingest source documents (tech sheets, fact sheets) — extract structured facts with Haiku, store with `producer_stated` source
6. AI-infer microclimate fields (aspect, fog, slope) from producer descriptions + training knowledge — stored with `ai_inferred` source
7. AI-synthesize insights (Sonnet/Opus) — wine_vintage_insights, wine_insights, appellation_insights, producer_insights — using all structured data as input. Synthesis is always the last step.
8. Trend tables — separate pass, shorter TTL, refresh more frequently

### Source hierarchy (confidence order, high → low)
1. `producer_stated` — from tech sheets, fact sheets, producer websites
2. `manual` — hand-entered
3. `api_derived` — elevation API, weather API
4. `ai_scraped` — AI extracted from a real source document
5. `wine_database` — CellarTracker, Vivino, etc.
6. `publication` — Wine Spectator, Decanter, etc.
7. `importer_stated` — from importer/distributor
8. `ai_inferred` — AI judgment from training knowledge, no specific source

Higher confidence never gets overwritten by lower confidence during re-enrichment.

---

## Foundational Principle: Don't Create When Content Already Exists

Producer-written content is always better than AI-generated prose. The pipeline hierarchy:

1. **Link to original content** — if a producer published a tech sheet, link to it and display it
2. **Scrape facts into structured fields** — extract blend percentages, alcohol, harvest dates, etc.
3. **AI fills gaps only** — when no original content exists. Marked as AI-generated, carries confidence scores.

AI is for cross-referencing (weather × soil × blend → insight), not for replacing producer voice.
