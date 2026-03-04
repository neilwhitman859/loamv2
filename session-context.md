# Loam v2 — Session Context (March 3–4, 2026)

This document captures what was accomplished, key decisions, and next steps from the current working session. It's intended to give the next chat full context without needing to search prior conversations.

---

## What We Did

### 1. Imported 100,646 wines into `wine_candidates`

**Source:** XWines dataset (https://github.com/rogerioxavier/X-Wines), licensed CC0-1.0 (public domain).

**Field mapping from CSV:**
- WineryName → `producer_name`
- WineName → `wine_name`
- Type → `wine_type` (Red, White, Rosé, Sparkling, Dessert, Fortified)
- Grapes → `grapes` (TEXT[] array, converted from Python-style string)
- Grapes[0] → `primary_grape`
- Elaborate → `elaborate`
- ABV → `abv`
- Country → `country`
- RegionName → `region_name`
- Vintages → `vintage_years` (INTEGER[], stripped 'N.V.' entries)

All rows have `source_url` set to the X-Wines GitHub URL.

**Stats:** 62 countries, 30,190 unique producers, 2,160 unique region names, 6 wine types.

### 2. Seeded 62 countries

All 62 countries from the dataset inserted into `countries` with proper English names, ISO codes, and slugs.

**Top 5 by wine count:** France (24,371), Italy (19,358), United States (13,139), Spain (7,109), Portugal (4,958).

### 3. Seeded 258 regions with hierarchy

Designed and inserted a complete region taxonomy across all 62 countries. This was a significant design effort — we cross-referenced Wine-Searcher, Decanter, Jancis Robinson, and Wine Folly to ensure professional accuracy.

**Total: 258 regions (215 top-level, 43 with parent relationships)**

**Key hierarchy decisions:**
- **France:** Bordeaux → Left Bank / Right Bank; Rhône Valley → Northern / Southern Rhône; Languedoc-Roussillon → Languedoc / Roussillon
- **Italy:** Tuscany → Bolgheri; Sicily → Etna (both buzzy/emerging)
- **United States:** California → Napa Valley, Sonoma County, Central Coast, Mendocino, Sierra Foothills, Lodi; Central Coast → Paso Robles, Santa Barbara County, Monterey; Oregon → Willamette Valley; Washington → Columbia Valley, Walla Walla Valley, Yakima Valley; New York → Finger Lakes, Long Island
- **Spain:** Catalonia → Priorat, Penedès
- **Australia:** State-level parents (South Australia, Western Australia, NSW, Victoria) with children beneath
- **South Africa:** Coastal Region → Stellenbosch, Paarl, Franschhoek, Constantia
- **New Zealand:** Wairarapa → Martinborough

**US is intentionally more granular** than other countries because most users will be US-based. This is a deliberate asymmetry.

The full draft document is at `/mnt/user-data/outputs/regions_draft.md` and should be committed to the repo.

---

## Key Decisions & Principles

### Granularity target: "casual wine enthusiast"
Regions are at the level a moderately knowledgeable wine drinker would recognize — Napa Valley, Burgundy, Barossa Valley. NOT appellation-level (Pauillac, Gevrey-Chambertin). Those will live in the `appellations` table.

### Build from scratch, not migrate v1
We reviewed the v1 geography tables (uvlhbyhezdhphnwcxtil). They have 317 regions and 1,517 appellations with clean hierarchy, but use text slugs as PKs, lack parent_id nesting, and mix AI content with factual data. Not worth migrating.

### Professional naming conventions
- English names where that's the industry standard: Burgundy (not Bourgogne), Tuscany (not Toscana), Piedmont (not Piemonte)
- Local names where labels/professionals use them: Mosel, Pfalz, Tokaj, Barossa Valley
- Aligned with Wine-Searcher and Decanter conventions

### X-Wines region_name field is messy
The `region_name` column in `wine_candidates` contains a mix of true regions, sub-regions, appellations, and even vineyard sites (e.g., "Gevrey-Chambertin 1er Cru 'Les Cazetiers'"). France alone has 487 distinct values. This field will need mapping to our clean region/appellation structure during enrichment.

---

## Current Database State

**Supabase project:** `vgbppjhmvbggfjztzobl`

| Table | Rows |
|-------|------|
| wine_candidates | 100,646 |
| countries | 62 |
| regions | 258 |
| All other tables | 0 |

**Schema** is fully built (all tables from schema-decisions.md exist). Only seed data has been added so far.

---

## What's Next

### Immediate priorities
1. **Commit regions_draft.md to the repo** — the curated region list with hierarchy documentation
2. **Appellation seeding strategy** — decide how to seed the `appellations` table. Options: curate by hand like regions (labor intensive), use AI to extract from wine_candidates region_name field, or start with a known list (Wine-Searcher's appellation list as reference)
3. **Grape seeding** — the `grapes` table is empty. wine_candidates has grape data in TEXT[] arrays. Need to extract unique grapes, normalize names, assign colors, and seed the grapes table
4. **Region mapping** — map the 2,160 unique `region_name` values in wine_candidates to the 258 seeded regions. This is the bridge between raw data and clean taxonomy

### Pipeline work (after seeding)
5. **Producer dedup** — 30,190 producer names need normalization and deduplication (three-tier system: deterministic → pg_trgm → Haiku)
6. **Wine promotion** — move confirmed wines from wine_candidates into the main `wines` table with proper FKs
7. **Enrichment pipeline** — the main event. Fetch weather data, scrape tech sheets, generate AI insights. This is where Loam's value prop comes alive

### Not yet decided
- Whether to add Colorado as a US region (getting press but still small)
- Appellation seeding depth — how many of the 1,517 v1 appellations are worth bringing into v2
- Grape alias handling (e.g., Syrah/Shiraz, Pinot Grigio/Pinot Gris)

---

## Files & Resources

- **GitHub repo:** https://github.com/neilwhitman859/loamv2
- **Supabase v2 project:** vgbppjhmvbggfjztzobl
- **Supabase v1 project (reference only):** uvlhbyhezdhphnwcxtil
- **X-Wines dataset:** https://github.com/rogerioxavier/X-Wines (CC0-1.0)
- **Key repo files:** PROJECT.md, schema-decisions.md, schema-summary.md, regions_draft.md (pending commit)
