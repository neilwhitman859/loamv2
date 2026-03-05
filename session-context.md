# Loam v2 — Session Context (March 3–5, 2026)

> **⚠️ IMPORTANT: GitHub is the source of truth for this project.**
> Claude's conversation memory is unreliable across sessions — decisions and context get lost.
> This file and `schema-decisions.md` must be updated and pushed to GitHub **before ending any working session**.
> At minimum, commit after: any schema decision, any seeding milestone, any design change.
> If in doubt, commit. It's free and prevents hours of re-discovery.

This document captures what was accomplished, key decisions, and next steps from the current working sessions. It's intended to give the next chat full context without needing to search prior conversations.

---

## Current Database State

**Supabase project:** `vgbppjhmvbggfjztzobl`

| Table | Rows | Status |
|-------|------|--------|
| wine_candidates | 100,646 | Seeded from X-Wines dataset |
| countries | 62 | Complete |
| regions | 328 | 266 real + 62 catch-alls |
| appellations | 529 | 52 countries covered |
| grapes | 707 | Complete with synonym mapping |
| varietal_categories | 154 | 101 single varietals, 23 named blends, 23 regional, 7 generic |
| source_types | 26 | Complete |
| publications | 22 | 17 critics, 2 community, 3 auction houses |
| farming_certifications | 18 | Complete |
| biodiversity_certifications | 7 | Complete |
| soil_types | 39 | Complete |
| producers | 30,418 | Canonical producers, deduplicated |
| producer_aliases | 266 | Alias names for merged producers |
| wines | 100,440 | Linked to producers, varietal categories, regions |
| wine_vintages | 2,146,955 | One per wine×year, with ABV |
| wine_grapes | 151,462 | Wine-to-grape links (all 707 grapes resolved) |
| region_name_mappings | 1,140 | Maps wine_candidates region_name to region_id/appellation_id |
| producer_dedup_staging | 30,684 | Dedup staging (completed) |
| producer_dedup_pairs | 8,208 | Fuzzy match verdicts (completed) |
| All other tables | 0 | Awaiting enrichment pipeline |

**Schema** is fully built (all tables from schema-decisions.md exist).

---

## What We Did

### 1. Imported 100,646 wines into `wine_candidates`

**Source:** XWines dataset (https://github.com/rogerioxavier/X-Wines), licensed CC0-1.0 (public domain).

**Field mapping from CSV:**
- WineryName → `producer_name`
- WineName → `wine_name`
- Type → `wine_type` (Red, White, Rosé, Sparkling, Dessert, Dessert/Port)
- Grapes → `grapes` (TEXT[] array)
- Grapes[0] → `primary_grape`
- Elaborate → `elaborate`
- ABV → `abv`
- Country → `country`
- RegionName → `region_name`
- Vintages → `vintage_years` (INTEGER[])

**Stats:** 62 countries, 30,190 unique producers, 2,160 unique region names, 6 wine types, 777 distinct grape names.

### 2. Seeded 62 countries

All 62 countries with proper English names, ISO codes, and slugs.

### 3. Seeded 266 real regions with hierarchy

Cross-referenced Wine-Searcher, Decanter, Jancis Robinson, and Wine Folly for accuracy. 215 top-level, 43 with parent relationships. US intentionally more granular (most users US-based).

### 4. Added 62 catch-all regions

One per country. `is_catch_all BOOLEAN` column distinguishes them. Slug pattern: `{country-slug}-country`. Purpose: wines without specific regional designation get `region_id` pointing here.

### 5. Seeded 529 appellations across 52 countries

Every appellation has `region_id` (NOT NULL) and `country_id` FKs. 25 designation types used (AOC, DOCG, DOC, DO, AVA, GI, WO, DAC, VQA, PDO, PGI, etc.).

**Top countries:** France 168, Italy 86, United States 63, Spain 27, Portugal 15, Australia 14, Germany 14, Chile 13, Austria 11, South Africa 10.

**10 countries with 0 appellations** (genuinely no formal systems): Azerbaijan, Belarus, Colombia, Denmark, Jordan, Liechtenstein, Myanmar, San Marino, Sweden, Syria.

**Country-level catch-all appellations:** Vin de France, Vino d'Italia, Vino de España, Deutscher Wein, South Eastern Australia, Niederösterreich.

### 6. Seeded 707 grapes with synonym mapping

777 distinct names → 707 canonical records after synonym merging. Every grape has color (368 red, 335 white, 3 grey, 1 pink), slug, and optional origin country (69 have origin set). 47 grapes have aliases (92 total aliases).

**Key synonym merges:**

| Canonical | Aliases |
|---|---|
| Syrah | Shiraz |
| Grenache | Garnacha, Cannonau, Garnacha Tinta |
| Pinot Noir | Spätburgunder, Blauburgunder, Pinot Nero |
| Pinot Gris | Pinot Grigio, Grauburgunder, Ruländer, Szürkebarát, Tocai Friulano, Tocai Italico |
| Pinot Blanc | Weissburgunder, Klevner |
| Mourvèdre | Monastrell, Mataro, Mourvedre |
| Tempranillo | Tinta Roriz, Tinta de Toro, Tinto Fino, Cencibel, Aragonez, Ull de Llebre, Tinta del Pais |
| Gamay | Gamay Noir |
| Carignan | Cariñena, Mazuelo, Samsó |
| Sangiovese | Morellino, Nielluccio, Prugnolo Gentile |
| Blaufränkisch | Lemberger, Kékfrankos |
| Albariño | Alvarinho |
| Malbec | Côt |
| Mencía | Jaen |
| Vermentino | Rolle/Rollo |
| Trebbiano Toscano | Ugni Blanc, Procanico |
| Muscat Blanc à Petits Grains | 14 aliases (Moscato, Muscatel, Gelber Muskateller, etc.) |
| Muscat of Alexandria | Zibibbo, Hanepoot |

**Kept separate:** Zinfandel and Primitivo (genetically identical, industry treats as distinct).

**Muscat family:** 7 distinct grapes (Muscat Blanc à Petits Grains, Muscat of Alexandria, Muscat Ottonel, Muscat of Hamburg, Moscato Giallo, Moscato Rosa, Moscato di Scanzo).

**Malvasia family:** All sub-varieties kept separate (9 distinct grapes).

**Trebbiano family:** Sub-varieties separate, generic Trebbiano as catch-all.

---

## Decisions Made (Varietal Category Seeding — Pending Implementation)

1. **Blend granularity:** Rich set ~25-30 named blend categories + generic catch-alls
2. **Dessert/fortified:** Own varietal categories (Port, Sherry, Madeira, Champagne, Sauternes, Tokaji, Vin Santo, Ice Wine, Late Harvest)
3. **Rosé:** Grape-specific rosé categories for major grapes + Rosé Blend catch-all (~10-15 entries)
4. **Orange wine:** Include in color enum, create categories as wines appear

---

## What's Next

### Immediate priorities (seeding)
1. ~~Grape seeding~~ ✅ Done
2. ~~Varietal categories~~ ✅ Done — 154 entries — ~130-150 entries (single varietals, named blends, regional designations, rosé categories, generic catch-alls)
3. ~~Source types~~ ✅ Done — 26 entries
4. ~~Publications~~ ✅ Done — 22 entries
5. ~~Farming certifications~~ ✅ Done — 18 entries
6. ~~Biodiversity certifications~~ ✅ Done — 7 entries
7. ~~Soil types~~ ✅ Done — 39 entries

### Processing wine_candidates into real entities
8. ~~**Producers**~~ ✅ Done — 30,418 canonical producers with 266 aliases
9. ~~**Wines**~~ ✅ Done — 100,440 wines with FKs to producers, varietal categories, regions
10. ~~**Wine vintages**~~ ✅ Done — 2,146,955 vintage records
11. ~~**Wine grapes**~~ ✅ Done — 151,462 wine-to-grape links

### Data quality improvements
12. ~~**Region mapping expansion**~~ ✅ Done — 612 new mappings, 91.6% wines on real regions

### Pipeline work
13. **Enrichment pipeline** — weather, tech sheets, AI insights

---

## Files & Resources

- **GitHub repo:** https://github.com/neilwhitman859/loamv2
- **Supabase v2 project:** vgbppjhmvbggfjztzobl
- **Supabase v1 project (reference only):** uvlhbyhezdhphnwcxtil
- **X-Wines dataset:** https://github.com/rogerioxavier/X-Wines (CC0-1.0)
- **Key repo files:** PROJECT.md, schema-decisions.md, schema-summary.md, session-context.md, regions_draft.md

---

## Completed: Producer Dedup Pipeline (March 5, 2026)

**Status:** ✅ Complete. 30,418 canonical producers created.

**Pipeline (run via Claude Code):**
1. Ported Python pipeline to Node.js (`producer_dedup_pipeline.mjs`) — Python not available on this machine
2. Ran 8,208 fuzzy pairs through Claude Haiku in batches of 50 (cost: $0.43)
3. Results: 393 merges, 7,815 keep_separate
4. Deep manual review of all merges — two parallel research agents web-searched each pair
5. Flipped 107 false merges to keep_separate (famous estates like Latour vs Latour à Pomerol, etc.)
6. Flipped 26 more transitive chain false links (Union-Find transitivity created bad groups)
7. Final state: 260 merges, 7,948 keep_separate, 0 pending

**Producer creation (`create_producers.mjs`):**
- Union-Find merges fuzzy pairs + 12 exact-match edges (same norm after accent/hyphen normalization)
- False exact-match exclusion list (e.g., "Château Belle-Vue" ≠ "Château Bellevue")
- Canonical name = most wines in each group
- Slug generation with country-suffix disambiguation for 331 collisions
- Result: 30,418 producers, 255 with aliases (266 alias records total)
- All producers have country_id, unique slugs, normalized names

**Key table:** `producer_aliases` — stores alternate spellings pointing to canonical producer_id.

**Scripts in repo:**
- `producer_dedup_pipeline.mjs` — Haiku verdict pipeline
- `producer_dedup_pipeline.py` — Original Python version (unused)
- `create_producers.mjs` — Producer creation from dedup results
- `create_wines.mjs` — Wine, vintage, and wine_grapes creation from wine_candidates
- `expand_region_mappings.mjs` — Region mapping expansion (5 strategies + manual dictionary)

## Completed: Wine Creation Pipeline (March 5, 2026)

**Status:** ✅ Complete. 100,440 wines, 2,146,955 vintages, 151,462 wine_grapes created.

**Pipeline (`create_wines.mjs`):**
1. Fetches all reference data (producers, aliases, grapes, varietal_categories, regions, region_name_mappings, countries)
2. Fetches all 100,646 wine_candidates with pagination
3. Resolves producer_id via canonical name + alias fallback (99.999% resolved — 1 typo unresolved)
4. Deduplicates on (producer_id, wine_name_normalized) → 100,440 unique wines (205 merged dups)
5. Assigns varietal_category_id via `elaborate` field (named blends like Bordeaux Blend, Rhône Blend) or primary_grape → single varietal match
6. Maps region_name to region_id via region_name_mappings (528 mappings); catch-all region for unmapped
7. Sets appellation_id from region_name_mappings where available (29,538 wines)
8. Generates globally unique slugs: `{producer-slug}-{wine-name-slug}` (zero collisions)
9. Batch inserts: 500/batch for wines, 2000/batch for vintages and wine_grapes

**Coverage (after region mapping expansion):**
- 30,418 of 30,418 producers have at least one wine
- 120 of 154 varietal categories in use
- 92,018 wines on real regions (91.6%), 8,422 on catch-all (8.4%)
- 31,646 wines have appellation_id (31.5%)
- 7,353 wines flagged as sparkling (effervescence = 'sparkling')
- All 62 countries represented

## Completed: Region Mapping Expansion (March 5, 2026)

**Status:** ✅ Complete. 612 new region_name_mappings added, 1,140 total.

**Pipeline (`expand_region_mappings.mjs`):**
1. Loads all regions (328), appellations (529), existing mappings (528)
2. Fetches all wine_candidates and identifies 1,633 unmapped region_name+country combos (15,848 wines)
3. Applies 5 matching strategies:
   - Appellation exact match (0)
   - Appellation normalized match — strips accents (5)
   - Sub-appellation pattern stripping — French 1er Cru/Grand Cru, Italian Classico/Superiore/Ripasso (345)
   - Region name exact/normalized match (7)
   - Manual mapping dictionary — ~300 curated entries covering France, Italy, Spain, Portugal, Germany, Austria, USA, Australia, South Africa, NZ, Chile, Argentina, Canada, Greece, Switzerland, and others (255)
4. Total matched: 612 of 1,633 unmapped combos (covering 11,234 wines)
5. Inserted as region_name_mappings with correct match_type constraint values
6. SQL UPDATE applied to wines table — moved 9,462 wines off catch-all regions

**Result:**
- Wines on catch-all: 17,884 → 8,422 (53% reduction)
- Wines with appellation: 29,538 → 31,646 (+2,108)
- Remaining 8,422 on catch-all are mostly: obscure sub-regions (<20 wines each), wines mapped to correct country catch-all (Spain, Austria, etc.)
