# Loam v2 — Claude Context

Loam is a wine intelligence platform. Users look up a wine and get the full story — place, vintage weather, soil, grapes, producer choices. All the scattered information brought together and connected by AI synthesis. The name is a soil type. Terroir is central.

**Supabase project:** `vgbppjhmvbggfjztzobl` (us-east-1)
**GitHub:** github.com/neilwhitman859/loamv2
**Stack:** Supabase (Postgres), Node.js scripts, Anthropic Claude, Open-Meteo, Vite/React frontend

---

## Docs — When to Consult Each

- `docs/SCHEMA.md` — Table-by-table field reference. Read when working with DB structure or writing queries.
- `docs/PRINCIPLES.md` — Product philosophy. Read when making judgment calls about what to build or how.
- `docs/DECISIONS.md` — Append-only log of human decisions with reasoning. Read when you need to understand why something was done a certain way. Never re-litigate settled decisions without the user raising it.
- `docs/VOICE.md` — Voice, tone, and food pairing guidance for all AI-generated content. Read before writing any enrichment prompts or insight content.
- `docs/ROADMAP.md` — Phased development plan. Read at session start to know what phase we're in and what's next.
- `docs/LWIN_STRATEGY.md` — LWIN integration research and three-layer data strategy. Read when working on wine import.
- `docs/SCHEMA_ASSESSMENT.md` — Deep schema assessment with implementation spec (Part B). Read when doing schema work.
- `docs/WORKFLOW.md` — Human-facing session checklist. You don't need to read this, but follow the behavioral instructions below.

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

### Architecture
The database has two layers:
- **Canonical tables** (`producers`, `wines`, `wine_vintages`, etc.) — curated, high-quality data. 78 canonical tables. Trial imports + KL bulk + retailer imports complete. Quality bar is high.
- **xwines_* tables** — bulk X-Wines dataset dump (~530K wines, ~2.2M vintages, ~32K producers). Kept as reference but not actively maintained. Data quality is lower.

### Reference Tables (complete)
Countries (62), regions (386 — 62 catch-all, 218 L1 named, 106 L2), appellations (3,205), grapes (9,693 from VIVC + 34,820 synonyms), varietal categories (161 + 162 grape mappings), source types (27), publications (71), attribute definitions (73), tasting descriptors (304), farming certifications (19, incl. HVE added 2026-03-16), biodiversity certifications (7), soil types (39).

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

### Content Tables (Phase 1c trial imports + KL + retailer + wine-type imports, 2026-03-16)
- **857 producers**, **3,086 wines**, 2,750 vintages, 1,323 scores, 1,279 prices, 3,331 wine_grapes, 88 entity_classifications, 30 winemakers, 169 farming certifications, 88 label designation links, 116 label designations, 8 wine_aliases
- wine_vintage_id FK backfilled: scores and prices 100% linked to wine_vintages
- **Trial imports (6 producers, Phase 1c):**
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
- Import architecture: `lib/import.mjs` (shared library) + `data/imports/{slug}.json` (per-producer data)
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

### What's Not There Yet
- Most insight tables empty (wine, producer, soil, water body)
- All weather data (appellation_vintages)
- All document tables
- All soil/water body link tables
- Enrichment log
- Reference data complete — all tables seeded and cross-table validated (2026-03-15). ABV column added to wine_vintages.

---

## Current Focus

**Phase 1: Foundation** — Schema hardening + reference data completion + trial producer imports. See `docs/ROADMAP.md` for full phased plan.

### Strategic Context (established 2026-03-13)
- **Three-layer data strategy:** LWIN (identity) → Government registries (breadth) → Producer direct (depth). No crowdsourced platforms.
- **Tiered wine experience:** Tier 1 (fully enriched) → Tier 2 (AI-contextualized) → Tier 3 (just identified). Product handles each tier explicitly.
- **LWIN first, then TTB COLA:** LWIN establishes the dedup backbone. COLA enriches and expands.
- **Vertical slice:** California + Burgundy as first enrichment targets.
- **Enrichment on demand:** Possible architecture — keep most wines at Tier 3, enrich to Tier 2 when a user looks up a wine. Needs further design.

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
- **RLS policies:** ✅ COMPLETE. 91/91 canonical tables have RLS enabled. Policy pattern: `public_read_*` (anon+authenticated SELECT), `service_write_*` (service_role ALL). 3 previously over-permissive tables (country_grapes, region_grapes, appellation_containment) tightened — public write policies removed.
- **Search infrastructure:** pg_trgm indexes exist but no full-text search, no cross-entity search function.
- **API views:** 4 views created: `wine_detail_view`, `producer_detail_view`, `wine_vintage_detail_view`, `wine_search_view`.
- **Migrations in git:** All DDL via Supabase MCP. Need `supabase/migrations/` before multi-developer.
- **FK normalization (partially addressed):** `wine_vintage_scores` and `wine_vintage_prices` now have `wine_vintage_id` FK (backfilled). `wine_vintage_grapes` already had optional `wine_vintage_id`. Legacy `wine_id + vintage_year` columns kept as convenience but `wine_vintage_id` is now the preferred join path.

### Open Questions (deferred)
- Data freshness strategy (how/when to re-import)
- Dedup strategy across import sources
- Enrichment pipeline architecture (batch vs on-demand, cost model)
- Data licensing for scores (Wine Spectator, Parker, CellarTracker)
- TTB COLA access strategy (COLA Cloud API vs direct scraping, cost)

---

## Key Phrases

- **"wrap up"** — End-of-session routine: update CLAUDE.md, update DECISIONS.md if needed, commit and push.
- **"log that"** — Force a DECISIONS.md entry.
- **"briefing"** — Give current state summary anytime mid-session.
