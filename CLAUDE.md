# Loam v2 — Claude Context

Loam is a wine intelligence platform. Users look up a wine and get the full story — place, vintage weather, soil, grapes, producer choices. All the scattered information brought together and connected by AI synthesis. The name is a soil type. Terroir is central.

**Supabase project:** `vgbppjhmvbggfjztzobl` (us-east-1)
**GitHub:** github.com/neilwhitman859/loamv2
**Stack:** Supabase (Postgres), Python pipeline, Anthropic Claude, Open-Meteo, Vite/React frontend

---

## Docs — When to Consult Each

- `docs/SCHEMA.md` — Table-by-table field reference. Read when working with DB structure or writing queries.
- `docs/PRINCIPLES.md` — Product philosophy. Read when making judgment calls about what to build or how.
- `docs/DECISIONS.md` — Append-only log of human decisions with reasoning. Read when you need to understand why something was done a certain way. Never re-litigate settled decisions without the user raising it.
- `docs/VOICE.md` — Voice, tone, and food pairing guidance for all AI-generated content. Read before writing any enrichment prompts or insight content.
- `docs/ENRICHMENT.md` — Letter-grade enrichment architecture (F/D/C/B/A), cost model, on-demand pipeline, wine-not-found flow. Read before building or modifying the enrichment pipeline.
- `docs/SOURCES.md` — Master reference for all external data sources (evaluated, integrated, planned, rejected). Read when working on data acquisition or import pipelines.
- `docs/ROADMAP.md` — Phased development plan. Read at session start to know what phase we're in and what's next.
- `docs/MERGE_STRATEGY.md` — Merge pipeline decisions: Python migration, merge layer sequencing, COLA risks, wine identity definition, AI matching approach, product direction. Read before building merge/matching infrastructure.
- `docs/WORKFLOW.md` — Human-facing session checklist. You don't need to read this, but follow the behavioral instructions below.
- `docs/reference/` — Retired docs kept for historical reference, not actively updated. Includes LWIN_STRATEGY.md (superseded by SOURCES.md + ROADMAP.md), SCHEMA_ASSESSMENT.md (Phase 1a spec, fully executed).

---

## Behavioral Instructions

### Session Briefings
When starting a session or recovering from compaction, give a medium briefing:
```
SESSION BRIEFING
- Last session: [what was accomplished]
- Current DB state: [query the DB for row counts — never rely on hardcoded numbers]
- Open items: [anything left mid-stream]
- Suggested next step: [what makes sense to pick up]
```
Query the database for current state. Do not guess or use stale numbers from this file.

### Auto-Update CLAUDE.md
Update this file at natural breakpoints — after a pipeline run, a schema change, a significant decision, or when wrapping up a session. Tell the user what changed: "Updated CLAUDE.md with [summary]."

### Auto-Log Decisions
When the user makes a judgment call (choosing between options, setting a direction, defining how something should work), append it to `docs/DECISIONS.md` automatically. Notify briefly: "Logged to DECISIONS.md: [one-line summary]."

If the user says **"log that"**, force an entry even if you didn't think it was significant.

### Auto-Update SCHEMA.md
When you modify the database schema (CREATE TABLE, ALTER TABLE, DROP, etc.), update `docs/SCHEMA.md` to reflect the change, including the reasoning.

### Commit at Milestones
When something is important enough to update CLAUDE.md, it's important enough to commit. Commit with a clear message after meaningful milestones.

### Always Recommend
When asking the user a clarifying question, **always give a recommendation**. If the answer is unclear, explain the case for each option. Don't just ask — propose a direction.

### Nudge the User
If the user is going a long stretch without wrapping up, if decisions are being made but not logged, or if a session is ending without updating files — say something. Be direct: "We've made some decisions this session that aren't logged yet. Want me to update DECISIONS.md and CLAUDE.md before we stop?"

---

## Current State

### Pipeline Language
**Python** for all data pipeline work (2026-03-20). Node.js retired. All 116 Node.js scripts archived to `scripts_archive/node/` and being converted to Python in `pipeline/`.

Pipeline structure:
- `pipeline/lib/` — shared libraries (db.py, normalize.py, resolve.py, importer.py, merge.py)
- `pipeline/fetch/` — data fetchers and web scrapers
- `pipeline/load/` — staging table loaders
- `pipeline/promote/` — staging → canonical promotion
- `pipeline/enrich/` — AI enrichment scripts
- `pipeline/reference/` — reference data seeding
- `pipeline/geo/` — geographic boundary scripts
- `pipeline/vivino/` — Vivino-specific pipeline (archive/reference)
- `pipeline/analyze/` — analysis and utility scripts

See `docs/MERGE_STRATEGY.md` for rationale.

### Architecture
The database has two layers:
- **Canonical tables** (`producers`, `wines`, `wine_vintages`, etc.) — curated, high-quality data. 78 canonical tables. Trial imports + KL bulk + retailer imports complete. Quality bar is high.
- **source_* staging tables** — per-source raw data for multi-source merge. `source_ttb_colas` (TTB COLA registry, Phase 1 running), `source_kansas_brands` (31,216 wine records loaded), `source_lwin` (184,497 records loaded). Each has merge tracking columns (canonical_wine_id, canonical_producer_id, processed_at).
- **xwines_* tables** — bulk X-Wines dataset dump (~530K wines, ~2.2M vintages, ~32K producers). Kept as reference but not actively maintained. Data quality is lower.

### Reference Tables (complete)
Countries (62), regions (386 — 62 catch-all, 218 L1 named, 106 L2), appellations (3,662 — 3,205 PDO/DOC/AOC + 457 IGT/IGP/PGI/VR/Landwein/base-tier), grapes (9,693 from VIVC + 34,820 synonyms), varietal categories (161 + 162 grape mappings), source types (27), publications (71), attribute definitions (73), tasting descriptors (304), farming certifications (19, incl. HVE added 2026-03-16), biodiversity certifications (7), soil types (39).

Regions rebuilt from scratch (2026-03-12): two-level hierarchy sourced from WSET L3 spec + Federdoc/MAPA/official wine authorities. All X-Wines leftover regions purged. Data file: `data/regions_rebuild.json`. Expanded (2026-03-13): 13 new regions added from Sonnet review triage — L2 subregions for Canada, South Africa, Austria, Spain + L1 regions for Portugal, UK (Scotland).

Appellation→region attribution 96.4% complete (3,090/3,205). Three-pass strategy: Pass 1 containment trace (1,915), Pass 3 direct lookup (1,174). ~115 remain on catch-all by design (multi-state US AVAs, minor countries without named regions). L2 attribution complete: 0 empty L2 regions. Sonnet review round 1 applied (2026-03-13): 10 appellation re-attributions + Southwest France rename + 48 appellations moved to 10 new L2 regions.

### Insights (partially populated)
Grape insights (707), region insights (202 — 126 deleted with leftover regions), appellation insights (82), country insights (62). Producer insights and wine insights are empty.

### Geographic Data
Geographic boundaries with PostGIS geometry. Appellation containment hierarchy (2,158 relationships).

**Region boundaries (2026-03-13):** 323/324 named regions have geographic data (99.7%). Full rebuild from scratch + Sonnet review expansion:
- **Official:** 38 regions (copied from wine authority appellation boundaries — UC Davis, Wine Australia, IPONZ, Eurac EU PDO)
- **Derived:** 181 regions (ST_Union of child appellation polygons — most accurate for wine platform)
- **Approximate:** 84 regions (Nominatim admin boundaries + EU PDO copied from appellations)
- **Geocoded:** 20 regions (centroid-only — mostly SA wine wards with no polygon source)
- **No data:** 1 (South Eastern Australia — cross-state super-zone, skipped by design)

**Wine expert Sonnet review completed and triaged (2026-03-13):** All regions reviewed by country. 115 potential corrections identified. Applied: 10 appellation re-attributions, 1 rename (Southwest France), Cava moved to Spain catch-all, 13 new regions created with boundaries. Parked: Switzerland L2 restructuring, Italy L2 restructuring, Croatia/Hungary restructuring, England sub-regions. Germany, Slovenia, Czech Republic passed clean.

Scripts: `scripts/geocode_regions.mjs` (Nominatim geocoding), `scripts/fix_region_geocodes.mjs` (Swiss/AR fixes), `scripts/review_region_boundaries.mjs` (Sonnet review), `scripts/geocode_new_regions.mjs` (targeted geocoding for new regions).
Data files: `data/region_nominatim_queries.json` (Nominatim query overrides), `data/region_review_report.json` (full Sonnet review report).

### Schema Hardening (2026-03-14) — Phase 1a COMPLETE
24 new tables created + ~45 columns added to 10 existing tables. All scrape data cleared (wines, vintages, scores, producers) — starting fresh.

New tables (Phase 1a): entity_attributes, external_ids, wine_appellations, grape_synonyms, classifications, classification_levels, entity_classifications, appellation_grapes, varietal_category_grapes, producer_farming_certifications, producer_biodiversity_certifications, vineyards, vineyard_producers, vineyard_soils, wine_vintage_tasting_insights, wine_vintage_nv_components, tasting_descriptors, wine_vintage_descriptors, importers, producer_importers. (attribute_definitions pre-existed.)
New tables (Phase 1b): label_designations, label_designation_rules, wine_label_designations, region_grapes, country_grapes.

Key deviations from original spec: vineyards got region_id + country_id + CHECK constraint; wine_vintage_components renamed to wine_vintage_nv_components. appellation_grapes `is_required` boolean replaced with `association_type` text ('required'/'typical') — same column added to region_grapes and country_grapes.

**Post-KL-import refinements (2026-03-15):** 5 schema changes from bulk import stress test:
- `wines.varietal_category_id` made nullable (no external source provides varietal categories natively)
- `producer_farming_certifications.certification_status` added (certified/practicing/transitioning)
- `producers.latitude/longitude` added (GPS coords from grower profiles)
- `wines.vinification_notes` added (free text winemaking approach)
- `appellation_aliases` table created and seeded with 17,558 aliases from 4 sources:
  - INAO OpenDataSoft API: 2,557 official French AOC product variants (color, style, cru)
  - Mechanical color suffixes: 9,866 (FR/IT/ES/PT/DE/US/AU/NZ/ZA/CL/AR)
  - Mechanical designation suffixes: 3,193 (appellation + AOC/DOC/DOCG/etc.)
  - Slash-form variants + informal/industry aliases + translations: 1,942
  - Script: `scripts/seed_appellation_aliases.mjs`
  - KL appellation resolution improved: 10.8% → 67.0% (983/1,468 wines)

**Schema sharpening (2026-03-15):** 8 data integrity and normalization fixes:
- CHECK constraints added: wines.color (red/white/rose/orange), wines.wine_type, wines.effervescence, producers.producer_type
- Color standardized: 'rosé' → 'rose' (ASCII, matches varietal_categories)
- Dropped from wine_vintages: vivino_id, wine_searcher_id, cellartracker_id (use external_ids table), alcohol_pct, alcohol_level (redundant with abv)
- Dropped from wines: oak_origin, yeast_type, fining, filtration, closure, fermentation_vessel + _source columns (winemaking lives on wine_vintages only; wines.vinification_notes for defaults), vineyard_id, vineyard_name (use wine_vineyards table), latitude, longitude (wines get geography from appellation/vineyard)
- Scores dedup index: UNIQUE on (wine_id, vintage_year, publication_id, critic, review_date) with COALESCE for nulls

### Reference Data Progress (Phase 1b, 2026-03-14)

**Classifications:** 13 systems, 32 levels. Audited by two independent wine expert passes. France: Bordeaux 1855 Médoc (5), Sauternes (3), Saint-Émilion (3), Graves (1), Burgundy Vineyard (2), Alsace Grand Cru (1), Champagne Cru (2), Cru Bourgeois (3), Cru Artisan (1), Provence Cru Classé (1). Germany: VDP (4). Austria: ÖTW Erste Lagen (2). Australia: Langton's (4). Systems: 11 government, 2 industry.

**Label designations:** 98 designations across 14 categories, 200 rules. Audited by two independent passes. Schema: label_designations, label_designation_rules (appellation_id nullable for national-level rules), wine_label_designations. Categories: aging_tier (15), sweetness_style (17), sparkling_type (14), pradikat_tier (12), production_method (10), estate_bottling (7), vineyard_age (6), late_harvest (4), botrytis_sweet (3), ice_wine (3), vineyard_designation (3), early_release (2), quality_tier (1), geographic_qualifier (1). Key rule sets:
- Italian Superiore: 31 rules (DOC/DOCG ABV/yield thresholds)
- Italian Riserva: 23 rules (22 DOCGs + 1 DOC aging requirements)
- German Prädikats: 78 rules (13 Anbaugebiete × 6 levels, Zone A/B Oechsle minimums)
- Portuguese Reserva/Grande Reserva: 14 rules (ABV thresholds by DOC)
- Spanish aging tiers: national defaults + Rioja/Ribera del Duero/Navarra deviations
- Austrian Prädikats: 8 rules (KMW minimums from Weingesetz 2009)
- EU sparkling sweetness: 7 rules (g/L RS from EU Reg 2019/33)

**Grapes:** VIVC import complete — 9,690 grapes imported from VIVC cache, 34,833 synonyms, parentage resolved (~3,000+ grapes with parent links). Three-tier display name strategy: 26 Tier 1 overrides (Merlot, Malbec, Grenache, etc.), 154 Tier 2 family-preserved (Pinot Noir, Cabernet Sauvignon), 9,510 Tier 3 auto. Country-specific synonyms added (Zinfandel/US, Primitivo/IT, Garnacha/ES, Monastrell/ES, Alvarinho/PT, Gouveio/PT). `display_name` column added to grapes table. VIVC Phase 5 (reconnect varietal categories) still pending.

**Publications:** 71 publications rebuilt from authoritative sources (66 original + 5 added 2026-03-15: View From the Cellar, Prince of Pinot, International Wine Review, Jasper Morris MW, Farr Vintners). Scoring systems, scale ranges, active status. Types: critic_publication, community, auction_house, competition, aggregator. Two-pass audit applied: 4 set inactive (Tanzer absorbed by Vinous, Dias Blue defunct, IWR/Connoisseurs' Guide ceased), Weinwisser country fixed (DE→CH), 3 scale_min fixes.

**Attribute definitions:** 73 definitions across 6 categories (chemistry 8, winemaking 23, viticulture 13, production 15, service 6, business 8). Sources: OIV International Code (chemistry), WSET L3/L4 (winemaking/viticulture), real producer/retailer websites (production/business). Two-pass audit: 2 renames (serving_temp, aging_potential), 10 additions, 1 citation fix.

**Tasting descriptors:** 304 descriptors in 3-tier hierarchy. Sources: WSET SAT (primary), UC Davis Wine Aroma Wheel, CMS Deductive Tasting Grid. Top-level categories use deterministic UUIDs (10000000-... prefix). Structure: 12 top-level categories → ~35 subcategories (20000000-... prefix) → ~257 leaf descriptors. Aroma categories: Fruit, Floral, Herbal/Vegetal, Spice, Oak, Earthy/Mineral, Nutty/Oxidative, Chemical/Other, Yeast/MLF. Palate categories: Sweetness, Acidity, Tannin, Body, Finish, Texture. Two-pass audit: 3 deletions (duplicates), 7 moves/fixes, ~12 additions.

**Appellation grapes:** 9,233 rows across all 3,206 appellations (100% coverage). Grape varieties associated with each appellation — regulated varieties for EU appellations (INAO/disciplinari/Consejo Regulador sources), key planted varieties for non-EU geographic appellations (US AVAs, AU GIs, SA WOs have no grape restrictions). Coverage by country: France 361 (detailed per-appellation), Italy 408, Germany 1,288 (Riesling/Spätburgunder/Müller-Thurgau), Spain 105, US 277, SA 142, AU 106, plus 32 other countries. Notable gaps: Blaufränkisch not in grapes table (affects Austrian/Hungarian entries), Hondarrabi Zuri missing (Txakoli), Tintilia missing (Molise).

**Region grapes:** 1,673 rows across all 324 named regions (100% coverage). Seeded from Anderson & Aryal dataset (University of Adelaide, 2000–2023 hectare plantings by variety) for L1 regions + authoritative sources for L2 subregions (Wine Australia, NZ Winegrowers, SAWIS, INAO, DOC/DOCG disciplinari, Consejo Regulador DO regulations, DWI, USDA/TTB). All entries `association_type = 'typical'`. Script: `scripts/seed_region_country_grapes.mjs`. Data: `scripts/insert_region_grapes.sql` (backup). **Two-pass expert audit completed (2026-03-14):** Pass 1 (training data), Pass 2 (web sources). 10 wrong entries removed (Rhône: Chardonnay/Gamay/Merlot, Rioja: Merlot, Etna: Nero d'Avola, Coastal Croatia: Grenache, Madeira: Sémillon, Abruzzo/Marche: Korinthiaki Lefki, Western Australia: Verdelho Tinto). 16 critical/high additions (Lodi: Zinfandel, Roero: Arneis, Vaud/Geneva: Chasselas, Coastal Croatia: Plavac Mali, Epirus: Debina, etc.). ~90 medium/low issues identified and parked (naming conventions, minor omissions). **Cross-table validation audit (2026-03-15):** 6 removals (Asturias: Albariño→Albarín Blanco, Epirus: Cab Sauv→Vlachiko, Iowa/Minnesota: vinifera→cold-hardy hybrids) + 20 additions (Swiss cantons: Chasselas/Müller-Thurgau, Bierzo: Godello, Wien: Welschriesling/Pinot Blanc, Côte Chalonnaise: Aligoté, Mâconnais: Pinot Noir, Wales: Bacchus, Arkansas: Cynthiana, Iowa/Minnesota/Wisconsin: Marquette/Frontenac).

**Country grapes:** 541 rows across all 62 countries (100% coverage). Seeded from Anderson & Aryal dataset for 46 major wine countries + manual additions for 16 minor countries. All entries `association_type = 'typical'`. **Audit additions (2026-03-14):** 18 country-level fixes — Italy: Nebbiolo + Corvina, France: Sémillon + Chenin Blanc + Viognier + Riesling + Gewürztraminer + Mourvèdre, Spain: Albariño + Mencía + Viura + Pedro Ximénez, Australia: Grenache, NZ: Riesling + Syrah, US: Cabernet Franc, Croatia: Plavac Mali, UK: Pinot Noir.

**Soil types:** 39 soil types with drainage_rate, heat_retention, water_holding_capacity, geological_origin properties.

### Content Tables (Phase 1c/1d, 2026-03-18)
- **948 producers**, **4,488 wines**, 1,498 vintages, 1,237 scores, 0 prices, 4,801 wine_grapes, 3,350 external_ids, 90 entity_classifications, 31 winemakers, 169 farming certifications, 90 label designation links, 116 label designations, 11 wine_aliases
- **96 region aliases**, **75 label designation aliases** seeded (WSET L3 naming conventions, translations, abbreviations)
- **New tables (2026-03-16):** wine_relationships (0 rows), producer_timeline (0 rows), wine_lookups (0 rows — analytics/enrichment promotion)
- **wine_insights columns added:** ai_hook, ai_vinification_summary, enrichment_tier (0-3), is_verified
- wine_vintage_id FK backfilled: scores and prices 100% linked to wine_vintages
- **Staging-first architecture (2026-03-18):** All data now goes through per-source staging tables before canonical promotion. KL and retailer data moved from canonical to staging. **19 staging tables total (889K rows, ~647K with COLA, ~27K with UPC).** See "Multi-Source Merge Infrastructure" section below.
- **Trial imports (6 producers, Phase 1c) — retained as seed data:**
  - Fort Ross Vineyard (US/Sonoma, estate): 15 wines, 112 vintages, 84 scores
  - Sea Slopes (US/Sonoma, child of Fort Ross): 2 wines, 24 vintages, 15 scores
  - Moone Tsai (US/Napa, negociant): 10 wines, 83 vintages, 48 scores
  - López de Heredia (Spain/Rioja, estate): 9 wines, 115 vintages, 67 scores
  - Marchesi Antinori (Italy/Tuscany, estate): 23 wines, 76 vintages, 98 scores
  - Louis Jadot (France/Burgundy, negociant): 44 wines, 149 vintages, 209 scores, 40 classifications
- **Kermit Lynch bulk import (193 producers, 1,467 wines):** First multi-producer portfolio import. France + Italy only. Tested importers table, bulk producer creation, grape parsing from blend strings, farming certification mapping. Appellation resolution at 11% (159/1,467) — drove creation of appellation_aliases table. Schema: `import_kl.mjs` + `fetch_kl_catalog.mjs` + `data/imports/kermit_lynch_catalog.json`.
- **Shopify retailer imports (2026-03-15):** Three retailers imported via Shopify JSON API (`/products.json`). Generic importer: `scripts/import_shopify_wines.mjs`. Learnings: title parsing handles "Producer Grape Region Vintage" patterns; tag formats vary wildly (flat, key:value, operational-only); appellation resolution correlates with wine price segment; grape resolution consistently 78-90%.
  - **Last Bottle Wines** (flash sale): 234 wines, 212 producers, 139 scores (extracted from marketing copy), 234 prices ($10-$2,199). Appellation: 68%, Grape: 90%. Script: `scripts/import_last_bottle.mjs`. Data: `data/imports/last_bottle_raw.json`.
  - **The Best Wine Store** (value, ≤$15): 752 wines, 216 producers, 752 prices ($2.99-$15). Appellation: 4% (mass-market wines list "California" not appellations), Grape: 78%. Data: `data/imports/best_wine_store_raw.json`.
  - **Domestique Wine** (natural/organic): 245 wines, 167 producers, 245 prices ($19-$85). Appellation: 8% (natural wines use VdF/IGT), Grape: 90%. Excellent key:value tag structure (`country:Italy`, `grape:nebbiolo`, `region:Piedmont`). Data: `data/imports/domestique_wine_raw.json`.
- **Wine-type stress test imports (10 producers across 8 countries, 2026-03-16):**
  - Louis Roederer (France/Champagne, sparkling): 13 wines, 36 vintages. Tests NV (vintage_year=0), disgorgement, dosage via rs_g_l, lees aging.
  - Dönnhoff (Germany/Nahe, Riesling): 31 wines, 171 vintages. Tests VDP hierarchy, Prädikat auto-detection, Einzellage appellations.
  - Château d'Yquem (France/Sauternes, dessert): 2 wines, 17 vintages. Tests high RS, 1855 Sauternes classification, botrytis.
  - Taylor's Port (Portugal/Porto, fortified): 11 wines, 30 vintages. Tests age_statement_years (Tawny 10-40yr), NV, Colheita.
  - Penfolds (Australia, multi-region): 11 wines, 34 vintages. Tests Langton's classification, null appellation for GI zones.
  - Royal Tokaji (Hungary/Tokaj, dessert): 7 wines, 20 vintages. Tests puttonyos metadata, ultra-high RS (up to 300 g/L).
  - Felton Road (NZ/Central Otago, Pinot Noir): 8 wines, 30 vintages. Tests biodynamic/organic certs, whole cluster, single block.
  - Catena Zapata (Argentina/Mendoza): 6 wines, 22 vintages. Tests high-altitude viticulture.
  - Château Miraval (France/Provence, rosé): 5 wines, 11 vintages. Tests rosé, cross-region production, 5-grape blends.
  - Vega Sicilia (Spain/Ribera del Duero): 4 wines, 16 vintages. Tests extreme oak aging, multi-vintage NV blend.
- **Global coverage stress test (5 producers, 2026-03-16):**
  - Kanonkop (South Africa/Stellenbosch): 6 wines, 23 vintages, 19 scores. Tests Pinotage, Cape Blend, Simonsberg-Stellenbosch ward.
  - Château Musar (Lebanon): 5 wines, 19 vintages, 12 scores. Tests no-appellation country, indigenous grapes (Obaideh, Merwah), 20-point JR scores.
  - Pheasant's Tears (Georgia/Kakheti): 6 wines, 17 vintages. Tests qvevri/orange wine, rare indigenous varieties (Shavkapito, Tavkveri, Chinuri).
  - Blandy's (Portugal/Madeira): 7 wines, 10 vintages, 9 scores. Tests fortified NV (vintage_year=0), age_statement_years, Madeira noble varieties.
  - Billecart-Salmon (France/Champagne): 7 wines, 13 vintages, 17 scores, 9 label designations. Tests rosé sparkling, NV + vintage, disgorgement dates, Blanc de Blancs/Noirs.
- **Metadata promotion (2026-03-16):** 849 fields moved from JSONB metadata to proper columns:
  - 583 vinification notes → wines.vinification_notes (from KL data)
  - 74 release dates → wine_vintages.release_date
  - 15 first vintage years → wines.first_vintage_year
  - 177 production cases → producers.total_production_cases
  - 4 VDP classifications → entity_classifications
  - 616 HTML entities cleaned from KL vinification text (&ldquo; → ", &ocirc; → ô, etc.)
  - Scripts: `scripts/promote_metadata.mjs`, `scripts/promote_classifications.mjs`, `scripts/fix_html_entities.mjs`, `scripts/audit_metadata.mjs`
- Import architecture: `pipeline/lib/importer.py` (shared library, converted from `lib/import.mjs`) + `data/imports/{slug}.json` (per-producer data)
- `--replace` mode: deletes all existing producer data in FK dependency order, then fresh insert
- `parseDate()` helper converts informal dates ("August 2024" → "2024-08-01")
- Field name flexibility: accepts both `oak_duration_months`/`oak_months`, `production_cases`/`cases_produced`, `g.grape`/`g.name`, etc.
- Parent-child producer relationship working: Sea Slopes → Fort Ross Vineyard & Winery
- **Classification linkage**: Importer resolves `classification.system` + `classification.level` → `entity_classifications`. System alias map for flexible matching (e.g., "Langton's" → "Langton's Classification of Australian Wine").
- **Prädikat auto-detection**: Importer scans wine name and label_designations for German Prädikat levels.
- **Grape aliases**: Expanded to cover Portuguese (Tinto Cão, Tinta Barroca), Champagne (Meunier), Hungarian (Sárgamuskotály, Hárslevelű) varieties.
- **Publication aliases**: Short forms (WA, JS, WE, JD) resolve to full publication names.
- **NV convention**: `vintage_year=0` for non-vintage wines (Champagne NV, Tawny Port, multi-vintage blends).
- **wine_type normalization**: `'still'` auto-corrected to `'table'` by importer (common JSON authoring mistake).
- **rs_g_l falsy fix**: `0` correctly stored (not converted to null by JavaScript falsy evaluation).
- **Critic drinking windows**: `critic_drinking_window_start/end` on `wine_vintage_scores`.
- **Wine aliases**: `wine_aliases` table for tracking name evolution.
- **Vineyard sourcing**: `wine_vineyards` + `wine_vintage_vineyards` import support in place.
- **Region aliases (in-code)**: ~75 entries mapping English/alternative names (Piedmont→Piemonte, Burgundy→Bourgogne, etc.) + Greek, Lebanese, Georgian regions.
- **Score validation**: Multi-scale aware — handles 100-point, 20-point (Jancis Robinson), 5-point scales.
- **Pre-import validation**: `--validate` flag catches CHECK constraint violations, unusual ABV/RS, missing required fields.
- **Accent-tolerant resolution**: All grape, appellation, and region resolvers use `normalize()` to strip accents and standardize whitespace.

### Migrations Applied (2026-03-16, previously queued while MCP offline)
All 5 pending migrations executed successfully:
- **3 alias tables**: region_aliases, producer_aliases (enhanced with alias_type), label_designation_aliases
- **3 Lebanon regions**: Bekaa Valley, Mount Lebanon, Batroun (Lebanon now has 4 regions)
- **16 new label designations**: Nykteri, Colheita, En Rama, Blanc de Blancs, Blanc de Noirs, Vieilles Vignes, Goldkapsel, Cape Blend, Qvevri, 5 Madeira sweetness styles (Sercial/Verdelho/Bual/Malmsey/Terrantez), Rainwater, Canteiro. Total: 115 designations.
- **9 new wine columns**: soil_description, vine_age_description, vineyard_area_ha, commune, altitude_m_low, altitude_m_high, aspect, slope_pct, monopole
- **1 new producer column**: address

### Metadata Promotion (2026-03-16) — Phase 2 COMPLETE
4,611 fields promoted from JSONB metadata to proper columns:
- 1,488 soil_description, 1,465 vine_age_description, 1,313 vineyard_area_ha
- 53 commune, 23 altitude (low/high), 23 aspect, 22 slope_pct, 8 monopole
- 193 producers.address
- Remaining in metadata: classification (28 unmapped Italian DOC/DOCG), cooperage (~80), vineyard_sources (~79)

### Drinking Window Schema Fix (2026-03-16)
- Renamed `critic_drink_window_*` → `critic_drinking_window_*` on `wine_vintage_scores` (naming consistency)
- Replaced `wine_insights.typical_drinking_window_years` (single int) with `typical_drinking_window_min_years` / `typical_drinking_window_max_years` (range)
- Added `peak_drinking_window_start/end` on `wine_vintage_insights` (synthesized optimal peak window)
- Hierarchy: per-score critic → per-vintage producer → per-vintage aggregated (critic/AI/calculated + peak) → per-wine typical range

### Pre-Import Schema Expansion (2026-03-17)
11 new columns + 2 reference data rows added to support new data sources:
- **`wines.barcode`** TEXT, indexed — GTIN/EAN for scan-to-lookup (Vinmonopolet, PA, SAQ, COLA Cloud)
- **`wine_vintage_scores.medal`** TEXT, CHECK — competition medals (IWSC, Berliner, TEXSOM, DWWA, etc.)
- **`wine_vintages.ingredients`** TEXT — EU e-label ingredient list
- **`wine_vintages.allergens`** TEXT[] — EU e-label allergen declarations
- **`wine_vintages.energy_kcal_per_100ml`** NUMERIC — EU e-label nutrition
- **`wine_vintages.nutrition_data`** JSONB — full EU e-label nutrition breakdown
- **`wine_vintages.maceration_technique`** TEXT — cold_soak/extended/saignee/etc. (importers)
- **`wine_vintages.aging_vessel_size_l`** INTEGER — barrique(225)/puncheon(500)/foudre(2000+)
- **`wine_vintages.maturity_status`** TEXT, CHECK — expert-assessed readiness (BBR, etc.)
- **`wine_vintages.maturity_status_source`** TEXT — who assessed maturity
- **`wine_vintage_prices.notes`** TEXT — auction provenance/condition/lot context
- **`farming_certifications`**: Kosher + Fair Trade rows added (now 21 certifications)

### Multi-Source Merge Infrastructure (2026-03-18)
Staging-first architecture: all external data goes through per-source staging tables, then a match engine promotes to canonical tables. Prevents dedup crisis at scale.

**New tables:**
- `match_decisions` — audit trail for cross-source matching decisions (AI review, confidence, extracted data)
- `source_polaner` (1,680 rows), `source_kermit_lynch` (1,468 rows), `source_kermit_lynch_growers` (193 rows)
- `source_skurnik` (5,541 rows), `source_winebow` (536 rows), `source_empson` (279 rows), `source_european_cellars` (443 rows)
- `source_last_bottle` (160 rows), `source_best_wine_store` (1,658 rows), `source_domestique` (247 rows)
- Pre-existing: `source_lwin` (189,359), `source_ttb_colas` (0 — Phase 1 running), `source_kansas_brands` (65,476)
- **New (2026-03-19 session):**
  - `source_pro_platform` (346,080 rows) — 12 US states via PRO Platform XLSX export. Unique on cola_number, `states` TEXT[] tracks which states each COLA appeared in. Fields: cola_number, brand, label_description, vintage, appellation, abv, supplier, distributors, states.
  - `source_tabc` (182,933 rows) — Texas TABC via Socrata API. 100% TTB numbers, 99.8% ABV.
  - `source_wv_abca` (55,093 rows) — West Virginia ABCA via REST API. 96.7% TTB IDs, vintage 63.8%. Detail endpoint has appellation + varietal (not yet scraped).
  - `source_openfoodfacts` (5,176 rows) — UPC barcodes, 62% French. Crowdsourced.
  - `source_horizon` (6,441 rows) — UPC barcodes from Horizon Beverage (SGWS MA/RI distributor).
  - `source_winedeals` (3,200 rows, 2,760 with UPC) — Retailer, Puppeteer scrape.
  - `source_lcbo` (7,030 rows) — Ontario LCBO, UPC barcodes. Pre-existing.
  - `source_pa` (5,905 rows) — Pennsylvania PLCB, 10,297 UPCs. Pre-existing.
  - `source_systembolaget` (12,646 rows) — Sweden. Pre-existing.
- **Total staging rows: 889,686** across 19 source tables. **~647K with COLA, ~27K with UPC barcodes.**

**RPC functions:** `match_producer_fuzzy()`, `match_wine_fuzzy()` — pg_trgm similarity search for the match engine.

**Active Python scripts (all under `pipeline/`):**
- `python -m pipeline.load.staging --source kl,skurnik,...` — loads raw JSON catalogs into staging tables
- `python -m pipeline.promote.staging --source skurnik [--dry-run]` — matches staging → canonical, creates/links records
- `python -m pipeline.promote.lwin [--analyze|--dry-run|--promote]` — LWIN staging → canonical promotion
- `python -m pipeline.load.pro_staging --state ar,co,...` — loads PRO Platform XLSX into staging
- `python -m pipeline.load.tabc_staging` — loads TX TABC into staging
- `python -m pipeline.load.wv_staging` — loads WV ABCA into staging
- `python -m pipeline.load.upc_staging` — loads Open Food Facts, Horizon, WineDeals into staging
- `python -m pipeline.fetch.wv_details` — WV ABCA detail fetcher with resume support
- `python -m pipeline.analyze.db_counts` — row counts across all tables

**Promotion results (5 importer catalogs promoted 2026-03-18):**
- KL: 1,468 wines → 830 new wines created, 638 matched to existing
- Skurnik: 5,541 wines → 2,605 new wines created, 2,912 matched, 24 skipped/errors
- Winebow: 536 wines → 340 new wines, 59 matched, 137 skipped (no name field)
- Empson: 279 wines → 178 new wines, 96 matched, 5 skipped
- EC: 443 wines → 324 new wines, 71 matched, 48 skipped

**Polaner deprioritized (2026-03-20):** All 1,680 titles parsed via Haiku (producer + wine_name extracted). Data in `source_polaner`. Removed from active promotion pipeline — catalog is small and metadata-thin compared to other importers. Data retained for reference.

**Canonical data after migration:**
- KL + retailer data moved from canonical to staging tables
- 33 curated seed producers (6 trial + 15 wine-type + 5 global + 7 additional) retained in canonical
- ~4,140 new wines + 348 seed wines = 4,488 total canonical wines

### What's Not There Yet
- Most insight tables empty (wine, producer, soil, water body)
- All weather data (appellation_vintages) — Open-Meteo schema design pending
- All document tables
- All soil/water body link tables
- wine_relationships, producer_timeline tables created but empty (0 rows)
- Enrichment pipeline not yet built (architecture designed in `docs/ENRICHMENT.md`)
- Reference data complete — all tables seeded and cross-table validated (2026-03-15). ABV column added to wine_vintages.

---

## Current Focus

**Phase 1: Foundation** — Schema hardening + reference data completion + trial producer imports. See `docs/ROADMAP.md` for full phased plan.

### Strategic Context (updated 2026-03-19)
- **Backbone IDs:** Three identifier systems anchor every wine: **COLA** (US regulatory, ~1.2M labels), **LWIN** (fine wine trade, 189K wines — already in canonical), **UPC** (retail barcode, fragmented sources). All stored in `external_ids`. Cross-referencing Backbone IDs is the primary dedup mechanism. See `docs/SOURCES.md` for the formal definition.
- **Multi-source data strategy:** LWIN (canonical backbone, already loaded) → TTB COLA direct (Phase 1 running) → State DBs (COLA + UPC bridge) → Importer catalogs (enrichment) → COLA Cloud (barcode enrichment) → Retailer sitemaps.
- **Letter-grade enrichment:** F (identity) → D (has scores/prices) → C (batch Haiku) → B (on-demand Sonnet) → A (curated). See `docs/ENRICHMENT.md`.
- **Identity-first, accuracy-first:** User explicitly chose slow/methodical over quick MVP. On-demand enrichment for user searches. Barcodes considered from the start to avoid re-matching later.
- **Vertical slice:** California + Burgundy as first enrichment targets.
- **User lookup triggers B enrichment:** On-demand Sonnet for every search landing on a wine below Grade B. C is batch pre-warming. See ENRICHMENT.md.

### Next Steps
1. ~~Review schema assessment decisions item by item~~ ✓
2. ~~Execute schema migrations (21 new tables, ~45 columns)~~ ✓
3. ~~Seed classifications (8 systems, 22 levels)~~ ✓ → expanded to 13 systems, 32 levels after two-pass audit
4. ~~Seed label designations (73 entries, 153 rules)~~ ✓ → expanded to 98 designations, 200 rules after two-pass audit
5. ~~VIVC Phase 1 crawl (9,400 wine grapes cached)~~ ✓
6. ~~VIVC Phases 2-5~~ ✓ (9,690 grapes, 34,833 synonyms, parentage resolved, varietal categories reconnected)
7. ~~Seed publications, attribute definitions, tasting descriptors~~ ✓ (66 pubs, 73 attrs, 304 descriptors — two-pass audit applied)
7b. ~~Audit varietal categories (161) with wine expert common sense check~~ ✓ — cross-table validation + expert audit: 31 grape mappings added, 2 removed (White Burgundy: Aligoté, Asturias: Albariño). Regional designations populated (Madeira, Marsala, Vinho Verde, Vin Santo, White Port, VDN, Beaujolais). 131→161 mappings.
8. ~~Populate appellation_grapes from disciplinari/WSET~~ ✓ (9,233 rows, 3,206 appellations, 100% coverage)
9. ~~Populate region_grapes and country_grapes~~ ✓ (1,671 region rows, 541 country rows — 100% coverage at all 3 geographic levels)
10. ~~Expert audit of region/country grape data~~ ✓ — two-pass audit (training data + web sources), 10 deletions, 34 additions. ~90 medium/low parked. Cross-table validation (2026-03-15): +14 net changes to region_grapes.
11. ~~Trial producer imports~~ ✓ — Phase 1c complete (2026-03-15). 6 producers across 4 countries:
    - **Fort Ross** (US/Sonoma, estate): 15 wines, 112 vintages, 84 scores. Winemaker: Jeff Pisoni.
    - **Sea Slopes** (US/Sonoma, child of Fort Ross): 2 wines, 24 vintages, 15 scores. parent_producer_id working.
    - **Moone Tsai** (US/Napa, negociant): 10 wines, 83 vintages, 48 scores. Winemaker: Philippe Melka (consulting).
    - **López de Heredia** (Spain/Rioja, estate): 9 wines, 115 vintages, 67 scores. Winemaker: Mercedes López de Heredia. Label designations linked.
    - **Antinori** (Italy/Tuscany, estate): 23 wines, 76 vintages, 98 scores. Winemaker: Renzo Cotarella. 5 estates.
    - **Louis Jadot** (France/Burgundy, negociant): 44 wines, 149 vintages, 209 scores, 40 classifications. Winemakers: Frédéric Barnier, Christine Botton.
    - Schema additions: winemakers, bottle_formats, wine_vineyards, wine_vintage_vineyards, wine_aliases, parent_producer_id, classification linkage, critic drinking windows. 5 new publications added. Import library hardened.
12. Schema improvements from trial imports (2026-03-15):
    - ~~Classification linkage~~ ✓ — entity_classifications populated for Jadot wines
    - ~~Critic drinking windows~~ ✓ — columns already existed, importer now maps to them
    - ~~Wine aliases table~~ ✓ — wine_aliases created for name evolution tracking
    - ~~Vineyard sourcing~~ ✓ — import support added for wine_vineyards/wine_vintage_vineyards
    - Multi-estate structure: use parent-child pattern (no schema change needed)
    - Clone data: stays in metadata JSONB (deferred)
13. ~~Data acquisition research~~ ✓ — comprehensive survey of all sources. See `docs/SOURCES.md`.
14. ~~Enrichment architecture finalized~~ ✓ — letter grades (F/D/C/B/A), Sonnet for B, Haiku for C. See `docs/ENRICHMENT.md`.
15. ~~LWIN import script~~ ✓ — `scripts/import_lwin.mjs` with `--analyze`/`--dry-run`/`--import` modes. Analysis: 189K wines, 100% country, 94% region, 62% appellation resolution. Import not yet run (needs user approval for 189K wines).
16. ~~Importer fetchers built and run~~ ✓ (2026-03-17) — 6 importer catalog fetchers, all complete:
    - **Polaner**: ✅ 1,680 wines via WP REST API. Country 99.6%, region 99.6%, appellation 98.2%.
    - **Skurnik**: ✅ 5,394 wines via FacetWP REST API (rebuilt from sitemap scraping). Grape 100%, appellation 97%.
    - **Winebow**: ✅ 536 wines from 153 brands. Best chemistry data — ABV 98%, pH 86%, acidity 93%, RS 89%.
    - **Empson**: ✅ 279 wines. Richest per-wine data (27+ fields) — soil 92%, altitude 87%, winemaker 94%.
    - **European Cellars**: ✅ 443 wines. 100% grape/soil/farming/vinification/aging, 80% scores.
    - **FirstLeaf**: ✅ 1,770 products via Shopify JSON API.
    - Total: ~10,102 catalog wines in JSON files ready for DB import.
    - Scripts: `fetch_skurnik.mjs`, `fetch_polaner.mjs`, `fetch_winebow.mjs`, `fetch_empson.mjs`, `fetch_european_cellars.mjs`
    - Output: `data/imports/{source}_catalog.json`
17. ~~Build merge infrastructure~~ ✓ (partial) — `lib/merge.mjs` MergeEngine class built. 3-tier matching, additive merging, grade calculation, all resolvers. Not yet tested.
18. ~~50-state UPC/COLA survey~~ ✓ (2026-03-19) — comprehensive overnight survey of all 50 US state alcohol control boards. See section below.
19. ~~PRO Platform 12-state XLSX download + parse + load~~ ✓ (2026-03-19) — 346,080 unique COLAs loaded into source_pro_platform.
20. ~~TX TABC load~~ ✓ — 182,933 wines loaded into source_tabc.
21. ~~WV ABCA load~~ ✓ — 55,093 wines loaded into source_wv_abca.
22. ~~UPC sources load~~ ✓ — OFF (5,176), Horizon (6,441), WineDeals (3,200) loaded.
23. ~~Kansas reload~~ ✓ — 65,476 records loaded (was 0 due to prior truncation).
24. **NJ OPRA request** — File for UPC+COLA data (7-day response). Portal: www-njlib.nj.gov. Or call 609-984-2830.
25. **TTB COLA Phase 1 (CSV harvest)** — IN PROGRESS. User running locally (~16 hours). 1955-present.
26. **TTB COLA Phase 2 (detail scrape)** — fetch grape varietals + applicant data from detail pages.
27. **TTB COLA Phase 3 (AI parse)** — Haiku extracts vintage, wine name, appellation from fanciful names. ~$10.
28. **LWIN import** — match against TTB COLA backbone for dedup. Script ready, not yet run.
29. **WV ABCA detail scraper** — batch fetch appellation/varietal/vineyard for 55K labels (~15 hours).
30. **Importer catalog merge** — merge 10K catalog wines against TTB+LWIN backbone.
31. **COLA Cloud barcode enrichment** — on-demand for Grade B enrichment, not bulk. $39/mo Starter.
32. **Remaining importer scrapers** — Kysela, Louis/Dressner, Broadbent
33. **Enrichment pipeline** — Edge Function + prompts + enrichment_log
34. **Frontend** — Vite/React PWA

### Schema Post-Import Hardening (2026-03-15)
- **Metadata → columns:** 4 fields promoted from metadata JSONB: `release_date` (wine_vintages), `first_vintage_year` (wines), `style` (wines), `philosophy` (producers). 150+ metadata entries identified for migration to proper table links (classifications, vineyards, estates).
- **Enrichment log rebuilt:** Original was a basic job queue. Rebuilt with model tracking, cost tracking (input_tokens/output_tokens/cost_usd), prompt template versioning, field-level change tracking, review workflow.
- **Appellation rules:** New `appellation_rules` table with flexible JSONB `rules` column for winemaking requirements (ABV, yield, aging, methods). One row per appellation.
- **`updated_at` triggers:** `set_updated_at()` function + 36 BEFORE UPDATE triggers. Every table with `updated_at` now auto-sets on UPDATE.
- **Orphan validation:** `validate_polymorphic_fks()` function checks entity_classifications, entity_attributes, external_ids, enrichment_log for orphaned rows. Currently clean.
- **Soft delete consistency:** Audited — all 15 entity tables have `deleted_at`, all junction/log tables correctly don't. Added missing `deleted_at` to winemakers.
- **Multi-source merging:** Architecture designed (source priority tiers, field provenance sidecar table, merge mode on import). Implementation deferred to dedicated session.

### Schema Scan & Hardening Round 2 (2026-03-15)
Full cross-reference of actual DB schema vs all 10 import scripts. 29 issues identified and triaged.
- **Deleted scripts:** `scrape_ridge.mjs` (referenced dropped columns), `fetch_producer_wines.mjs`, `create_wines_from_vivino.mjs`, `match_vivino_to_loam.mjs` (referenced non-existent `grapes.aliases` column and `region_name_mappings` table from xwines era). Remaining scrapers (`scrape_stags_leap.mjs`, `scrape_tablas_creek.mjs`) may break on schema changes — will fix when re-used.
- **New table: `retailers`** — normalized retailer reference (id, slug, name, website_url, country_id, retailer_type CHECK, metadata, timestamps, deleted_at). FK from `wine_vintage_prices.retailer_id`.
- **`wines.country_id` made nullable** — retailer imports often can't determine country. Better null than wrong.
- **`wines.effervescence` DEFAULT 'still'** — 95%+ of wines are still; reduces code in every importer.
- **`wines.label_designation` column DROPPED** — `wine_label_designations` junction table is canonical.
- **`wine_vintages.acidity/tannin/body` DROPPED** — superseded by `wine_vintage_tasting_insights`. Zero data loss (all existing data was from rescrapeable sources).
- **`wine_vintage_scores.score_provenance`** added — CHECK (direct/retailer_quote/aggregated/community). Distinguishes critic-direct scores from marketing copy extractions.
- **`wine_vintage_scores.wine_vintage_id`** added — FK to wine_vintages, backfilled 100%. Normalizes the denormalized wine_id+vintage_year pattern.
- **`wine_vintage_prices.wine_vintage_id`** added — same normalization, backfilled 100%.
- **`wine_vintage_prices.compare_at_price_usd`** added — original MSRP for discount retailers (Last Bottle, flash sales).
- **`grapes.name_normalized`** added — NOT NULL, indexed. Consistent with producers/wines pattern.
- **`biodiversity_certifications.url`** added — basic reference data for certification websites.
- **`enrichment_log` status CHECK** expanded — completed/failed/pending/needs_review/superseded.
- **CHECK constraints documented vs actual DB:** wines.effervescence, wines.wine_type, producers.producer_type all have broader CHECK values in DB than previously documented. SCHEMA.md updated to match.
- **`grape_plantings` table** documented — existed but was missing from SCHEMA.md. 0 rows (ready for Anderson & Aryal data).
- **`wine_regions`/`producer_regions`** — tables exist but no import pipeline populates them. Noted for future multi-region support.

### Technical Debt (pre-frontend)
- **RLS policies:** ✅ COMPLETE. 94/94 canonical tables have RLS enabled (91 original + 3 new tables this session). Policy pattern: `public_read_*` (anon+authenticated SELECT), `service_write_*` (service_role ALL). wine_lookups also has `anon_insert` for anonymous page views.
- **Search infrastructure:** ✅ COMPLETE. `search_vector` tsvector columns + GIN indexes on wines, producers, appellations, regions, grapes. Trigram indexes on all searchable name columns. Auto-update triggers on INSERT/UPDATE. Two RPC functions: `search_catalog(query, limit, entity_types[])` for unified cross-entity search bar, `search_wines(query, filter_*, sort_by, limit, offset)` for filtered wine browse. Both granted to anon+authenticated.
- **API views:** 4 views created: `wine_detail_view`, `producer_detail_view`, `wine_vintage_detail_view`, `wine_search_view`.
- **Alias tables:** ✅ SEEDED. region_aliases (96), label_designation_aliases (75), appellation_aliases (17,558).
- **JSONB metadata:** ✅ CLEAN. All promotable fields moved to proper columns. Remaining metadata is appropriate for JSONB (import provenance, cooperage, clones, narrative notes).
- **Migrations in git:** All DDL via Supabase MCP. Need `supabase/migrations/` before multi-developer.
- **FK normalization (partially addressed):** `wine_vintage_scores` and `wine_vintage_prices` now have `wine_vintage_id` FK (backfilled). `wine_vintage_grapes` already had optional `wine_vintage_id`. Legacy `wine_id + vintage_year` columns kept as convenience but `wine_vintage_id` is now the preferred join path.

### Data Acquisition Research (2026-03-16) — COMPLETE
Comprehensive research across 17 source categories. Unified reference in `docs/SOURCES.md`.
- **LWIN database analyzed**: 186K wine identities, 37K producers. Fine wine bias ($30+). No grapes/ABV/barcodes. File: `data/LWINdatabase.xlsx`
- **COLA Cloud API identified**: Commercial wrapper on TTB COLA registry. $39/mo Starter tier. ~1.2M wine COLAs with 96% appellation, 54% grapes, 35% barcodes, 87% ABV.
- **State databases surveyed (all 50 states)**: Full 50-state survey completed 2026-03-18. PRO Platform is the #1 discovery (12 states, 1.56M brand registrations with COLA). TX TABC (201K wines via Socrata), WV ABCA (55K wines via REST API), PA PLCB (5.9K wines with UPCs), KS KDOR (31K wines). NJ has UPC+COLA (since Jan 2023, needs free account). CT has UPC+COLA bridge (WAF-blocked, needs phone call). 28 states confirmed dead ends (spirits-only, no public data, or licensee-restricted).
- **22 importers researched**: Skurnik (5K wines, easy), Winebow (1K, best per-wine data quality), European Cellars (724), Kysela (1K), Louis/Dressner (1.2K), Polaner (2-3K, best new discovery).
- **Retailer sitemaps**: Wine.com (262K URLs downloaded), Total Wine (9.5K), FirstLeaf (5.1K).
- **Vinmonopolet API** ⭐⭐: Norwegian state monopoly — the single richest structured wine data source globally. Free tier, barcodes, grape percentages, sugar/acid g/L, flavor scales, 20K+ wines. Official API with auth key.
- **Competition databases**: IWSC (CSV export available — easiest), Berliner Wine Trophy (74K wines), DWWA (20K wines/year), TEXSOM (40yr history). Most match via producer+wine+vintage.
- **Wine APIs**: VineRadar (40K+ wineries with vineyard GPS + terroir data — directly serves Loam's core mission), db.wine (10K wines REST API).
- **EU e-labels** (u-label.com): Massive new regulatory source (500K+ wines since Dec 2023) — ingredients, nutrition, allergens. Structured data, all EU wines.
- **Certification databases**: USDA Organic Integrity DB (quick win — free CSV, producer-level), Demeter biodynamic registry.
- **International retailers**: SAQ (Quebec, 14K wines, structured), Systembolaget (Sweden, 7K wines, barcodes).
- **Auction/trading**: Liv-ex (10K wines, LWIN integration, requires membership), Sotheby's/Bonhams/iDealwine (fine wine pricing data).
- **Merge architecture designed**: Staging tables (import_runs, staging_wines, match_decisions, field_provenance) + 4-layer matching + source priority + confidence thresholds.
- **FOIA filed** to TTB as backup parallel path.
- **Coverage projection**: ~200-250K unique wines across $10-150 US market.
- **Import priority (revised 2026-03-18)**: PRO Platform (instant XLSX, 1.56M COLA) → TTB COLA direct → TX TABC → WV ABCA → LWIN → Importers → UPC sources (PA/OFF/Horizon/LCBO) → COLA Cloud (barcode enrichment) → Wine.com → Total Wine.
- **Files downloaded**: `kansas_active_brands.json` (24.6MB), `pa_wine_catalog.xlsx`, `wine_com_all_urls.txt` (20MB), `cola_demo.zip`, `utah_product_list.xlsx`, `lwin_database.csv`, plus all files from 2026-03-18 session below.
- **Critical gap identified**: Identity matching engine is #1 technical priority — without it, scaling beyond ~12K wines creates dedup crisis.

### TTB COLA Direct Scraping (2026-03-17) — Phase 1 IN PROGRESS
Discovered that TTB's public COLA registry has **structured grape varietal data as a native field** — not AI-extracted. This eliminates COLA Cloud as the primary F-tier data source.

**TTB COLA Online detail fields:** TTB ID, brand name, fanciful name, **grape varietals**, origin (state/country), class/type (red/white/rosé/sparkling/dessert), permit number, applicant name + full address, approval date, serial number, status.
**Not available from TTB:** ABV, barcodes/GTIN, structured appellation (only state/country-level origin), tasting notes.

**Phase 1 (CSV harvest):** User running locally — conservative rate limiting, ~16 hours estimated. Searching 1955-present by date range + wine class types 80-89. 4-day windows to stay under 1,000-row export cap. Expected output: 1.2-1.5M TTB IDs with basic metadata.

**Phase 2 (detail scrape):** Fetch detail pages by TTB ID. Predictable URL: `https://ttbonline.gov/colasonline/viewColaDetails.do?action=publicDisplaySearchBasic&ttbid={TTB_ID}`. Parse HTML for grape varietals, applicant data. Filter Phase 1 output first (skip expired/surrendered, deduplicate label refreshes). 3-7 days at polite rate.

**Phase 3 (AI parse):** Haiku extracts vintage year, wine name, appellation from fanciful name text. ~$5-10 total.

**COLA Cloud role revised:** Barcode + identity enrichment service for on-demand Grade B enrichment, not bulk F-tier source. Email drafted to request one-time data export (barcode data specifically). API key in `.env`.

**Merge infrastructure:** `pipeline/lib/merge.py` (converted from `lib/merge.mjs`) — MergeEngine class with reference data loading, 3-tier producer/wine matching (key → normalized name → fuzzy pg_trgm), additive field merging, grade calculation. Not yet tested.

**COLA Cloud API tested (2026-03-17):** 22 requests on free tier (500/mo). Search endpoint basic, detail endpoint rich. Known wines tested: Ridge, López de Heredia, Tignanello, Cristal, Yquem. Grape coverage imperfect (truncated names, French wines often missing grapes). Sample data saved: `data/imports/cola_cloud_sample.json`, `data/imports/cola_cloud_test2.json`.

**Illinois PRO Platform API discovered:** Public JSON POST endpoint at `/Search/ActiveBrandSearch`. Returns COLA numbers, ABV, vintage, appellation, distributors. Same platform as Kansas but requires session cookies (not fully open like Kansas JSON dump).

### 50-State UPC/COLA Survey + PRO Platform Discovery (2026-03-18) — COMPLETE
Comprehensive overnight survey of all 50 US state alcohol control boards for wine product data (UPC barcodes and/or TTB COLA identifiers). Combined with UPC source fetching from retailers and government monopolies.

**PRO Platform — #1 discovery (Tier 1 source):** Sovos/ShipCompliant Product Registration Online platform used by 12 US states. All use identical API (`POST /Search/ActiveBrandSearch`) and XLSX export (`GET /Export/DownloadActiveBrandsSummary` — no auth needed!). Combined 1.56M brand registrations, 99% with TTB COLA numbers, plus vintage, appellation, ABV, supplier, distributor. All 12 state exports downloaded as XLSX in ~3 minutes.

**PRO Platform XLSX files (12 states, ~270MB total):**
- AR: `ar_active_brands.xlsx` (4.6MB), CO: `co_active_brands.xlsx` (48MB — largest)
- IL: `il_active_brands.xlsx` (35MB), KY: `ky_active_brands.xlsx` (27MB)
- LA: `la_active_brands.xlsx` (7.8MB), MN: `mn_active_brands.xlsx` (13MB)
- NM: `nm_active_brands.xlsx` (17MB), NY: `ny_active_brands.xlsx` (6.5MB)
- OH: `oh_active_brands.xlsx` (37MB), OK: `ok_active_brands.xlsx` (8.2MB)
- SC: `sc_active_brands.xlsx` (39MB), SD: `sd_active_brands.xlsx` (5.5MB)
- Fields: Tax Trade Bureau ID (COLA), Brand Description, Label Description, Vintage, Appellation, Percent Alcohol, Container Type, Unit Size, Supplier Name, Distributor Name, Approval Date, Approval Number
- Note: Files contain ALL beverages (spirits + wine). Wine filtering needed during import. Rows are duplicated per supplier-distributor pair; dedup by COLA needed.

**Texas TABC (201K wines):** Socrata Open Data API at `data.texas.gov/resource/2cjh-3vae.json?type=WINE`. 201,165 wine records, 100% TTB numbers, 99.8% ABV. Pre-Sept 2021 labels (post-2021 in AIMS system, no bulk export). File: `tx_tabc_wines.json` (87MB).

**West Virginia ABCA (55K wines):** REST API at `api.wvabca.com/API.svc/`. Public API key: `2BB0C528-219F-49EE-A8B8-A5A2271BEF9D`. List endpoint (`/WineLabelSearch`): 55,378 wine labels, 96.4% TTB ID, 63.8% vintage. Detail endpoint (`/GetWineLabelDetails`) has appellation + varietal + vineyard — needs batch scraper (~15 hours). File: `wv_wines_list.json` (14MB).

**UPC barcode sources fetched:**
- **Open Food Facts**: 5,176 wines with EAN barcodes. REST API, 8 wine categories paginated. 62% French. File: `openfoodfacts_wines.json`. Script: `fetch_openfoodfacts.mjs`.
- **Horizon Beverage** (SGWS/MA+RI): 6,441 wines via undocumented JSON API at `horizonbeverage.com/api/products/GetProducts`. UPC in every record. File: `horizon_wines.json`. Script: `fetch_horizon.mjs`.
- **PA PLCB**: 5,905 wines with 10,297 UPCs (multiple per product). Parsed from Excel catalog. File: `pa_wines_parsed.json`.
- **LCBO** (Ontario, Canada): 3,513 wines with barcodes. Puppeteer scraper. File: `lcbo_wines.json`. Script: `fetch_lcbo.mjs`.
- **WineDeals.com**: 3,200 wines scraped (2,760 with UPC). Puppeteer product page scraper. File: `winedeals_catalog.json`. Script: `fetch_winedeals.mjs`.

**Connecticut DCP — NO UPC/COLA (confirmed 2026-03-19):** Original "Rosetta Stone" claim was wrong. Wholesaler XLSX price lists (e.g., Brescome Barton: 11,309 rows) contain wine name, vintage, proof, pack, prices — but NO UPC and NO TTB COLA columns. Supplier PDFs are similar. CT brand registration form does not collect UPC or COLA as structured fields. However, CT data IS valuable for pricing (~50-100K wholesale wine prices per month). Also downloaded: `ct_liquor_brands.json` (71,753 brand registrations from data.ct.gov Socrata API — brand name + wholesaler mappings, no UPC/COLA). Contact: Richard.Mindek@ct.gov, (860) 713-6229. FOIA portal: dcpct.govqa.us (4-day response).

**New Jersey POSSE — OPRA required (confirmed 2026-03-19):** Account created and tested. Product Search returns ZERO results for all queries — database appears empty despite UPC+COLA collection since Jan 2023. The data exists (confirmed by NJ ABC advisory notices AN 2024-04) but is not exposed through the web search interface. **Action: File NJ OPRA request** via www-njlib.nj.gov (7-day response, electronic records free). Request CSV export of all wine product registrations including UPC + COLA. Or call 609-984-2830.

**Vinmonopolet** — Email sent requesting API key. Waiting for response. (~20K wines with barcodes, richest structured data globally.)

**50-state dead ends (28 states confirmed no wine UPC/COLA data):**
- Spirits-only control: OR, VA, IA (since 1985), WA (wine privatized 2012)
- Spirits-only database: MI
- No wine control: AK, AZ, CA, DE, GA, HI, IN, MA, MO, NE, NV, ND (repealed 2005), RI, WI
- Fortified only: ID, VT, MT
- Licensee-restricted: WY, AL
- Low value (no COLA/UPC in output): UT, MD (Montgomery Co only), MS, TN, NC (wine control but COLA not in search results)

**New scripts:** `fetch_pro_states.mjs` (PRO Platform multi-state fetcher — paginated JSON + analyze mode), `fetch_ct_dcp.mjs` (CT DCP Puppeteer+PDF scraper), `fetch_openfoodfacts.mjs`, `fetch_horizon.mjs`, `fetch_lcbo.mjs`, `fetch_winedeals.mjs`.

### Open Questions (deferred)
- Data freshness strategy (how/when to re-import)
- Data licensing for scores (Wine Spectator, Parker, CellarTracker)
- UPC Data 4 Beverage Alcohol pricing inquiry
- VineRadar API pricing inquiry (vineyard GPS + terroir data)
- Vinmonopolet API key — email sent 2026-03-18, awaiting response
- Southern Hemisphere importer gap (no dedicated importers researched for AU/NZ/AR/CL/ZA)
- COLA Cloud Snowflake data share pricing (for barcode bulk access if email negotiation fails)
- COLA Cloud one-time export email (drafted, not yet sent)
- CT DCP bulk export — call Richard Mindek (860) 713-6229
- NJ POSSE account registration — UPC+COLA data since Jan 2023
- WV ABCA detail scraper — batch fetch appellation/varietal/vineyard for 55K labels (~15 hours)
- PRO Platform wine-only re-exports — current XLSX files include all beverages, need wine filtering
- Systembolaget/Alko barcode sources — still need investigation

---

## Key Phrases

- **"wrap up"** — End-of-session routine: **consider every doc file** for updates, then commit and push. Go through this checklist — skip only if genuinely nothing changed for that doc:
  - `CLAUDE.md` — always update (current state, what was accomplished)
  - `docs/DECISIONS.md` — append if any decisions were made
  - `docs/ROADMAP.md` — update if phase status or priorities changed
  - `docs/SCHEMA.md` — update if schema changed (CREATE/ALTER/DROP)
  - `docs/SOURCES.md` — update if source status changed (new source, fetcher built, data loaded)
  - `docs/ENRICHMENT.md` — update if enrichment architecture changed
  - `docs/PRINCIPLES.md` — update if product philosophy changed
  - `docs/VOICE.md` — update if tone/content guidance changed
  - `docs/WORKFLOW.md` — update if session workflow changed
- **"log that"** — Force a DECISIONS.md entry.
- **"briefing"** — Give current state summary anytime mid-session.
