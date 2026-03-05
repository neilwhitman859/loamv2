# Loam v2 — Session Context (March 3–4, 2026)

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
| All other tables | 0 | Awaiting wine/producer processing |

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
8. **Producers** — extract, deduplicate (three-tier), create records
9. **Wines** — create with proper FKs
10. **Wine vintages** — from vintage_years arrays

### Pipeline work
11. **Region mapping** — map 2,160 region_name values to 328 regions
12. **Appellation mapping** — match wines to appellations
13. **Enrichment pipeline** — weather, tech sheets, AI insights

---

## Files & Resources

- **GitHub repo:** https://github.com/neilwhitman859/loamv2
- **Supabase v2 project:** vgbppjhmvbggfjztzobl
- **Supabase v1 project (reference only):** uvlhbyhezdhphnwcxtil
- **X-Wines dataset:** https://github.com/rogerioxavier/X-Wines (CC0-1.0)
- **Key repo files:** PROJECT.md, schema-decisions.md, schema-summary.md, session-context.md, regions_draft.md
