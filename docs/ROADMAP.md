# Loam — Roadmap

*Established 2026-03-13. Updated as phases complete.*

---

## Guiding Principles

- **Do it right the first time.** Fast is slow, slow is fast.
- **Real data only.** First-hand sources and government registries. No crowdsourced platforms.
- **Schema before data.** All structural work completed before mass import.
- **Terroir is central.** Burgundy is the benchmark — if we can tell that story well, we can handle anything.

---

## Phase 1: Foundation

**Status:** Not started
**Goal:** Schema is production-ready. Reference tables are fully populated. Trial imports prove the schema handles diverse wine data.

### 1a. Schema Hardening ✓ (2026-03-14)
Executed the full implementation spec from `docs/SCHEMA_ASSESSMENT.md` Part B:
- All 21 new tables (Tier 0 structural through Tier 2) ✓
- All ~45 column additions on existing tables ✓
- `lwin` columns on `wines` and `wine_vintages` ✓
- All scrape data cleared — starting fresh ✓
- Reviewed each decision point before executing ✓

### 1b. Reference Data Completion
Fill the reference tables that power AI enrichment:
- `appellation_grapes` — structured allowed varieties per appellation
- `varietal_category_grapes` — blend composition
- Grape parentage (`parent1_grape_id`, `parent2_grape_id`) from VIVC
- Grape synonyms table populated
- Soil physical properties backfilled
- Remaining appellation/region/grape insights
- Publications scoring metadata (`score_scale_min/max`)
- Classification system seeded (Bordeaux 1855, Burgundy Grand/Premier Cru)

### 1c. Trial Producer Imports
Scrape 4-5 producer websites to stress-test the schema before mass import:
- **Moone Tsai** (CA) — boutique, small production
- **Fort Ross** (CA) — small US vineyard
- **López de Heredia** (Spain) — traditional Rioja, Reserva/Gran Reserva, long aging
- **TBD Burgundy producer** — vineyard-level classification, shared vineyards
- **TBD Tuscany producer** — DOCG/DOC + IGT Super Tuscans

If any producer provides data the schema can't capture, fix the schema before proceeding.

---

## Phase 2: LWIN Import

**Status:** Not started
**Depends on:** Phase 1 complete
**Goal:** 187K wine skeletons in canonical tables. Identity backbone established. Dedup anchor for future imports.

- Import LWIN database (187K wines after filtering spirits/beer)
- Bulk-create ~37K producer records
- Build region mapping (`data/lwin_region_mapping.json`, ~100 entries)
- Appellation matching (fuzzy name match, ~85% expected auto-match)
- Assign LWIN-7 codes to `wines.lwin`, LWIN-11 to `wine_vintages.lwin`
- Match and merge with existing 267 wines (Ridge, Tablas Creek, Stag's Leap + trial imports)
- All imported wines enter at Tier 3 (just identified)

---

## Phase 3: Source Research + TTB COLA Import

**Status:** Not started
**Depends on:** Phase 2 complete (LWIN as dedup backbone)
**Goal:** Everyday wine coverage. Dedicated source research session before any import.

### 3a. Source Research Session
Deep dive into available data sources:
- **TTB COLA** — access strategy (COLA Cloud API vs direct scraping), cost, data quality
- **EU e-labels** — mandatory since Dec 2023, structured data, emerging ecosystem
- **Other catalog-level sources** — Wine Australia, INAO, INV, SAG
- Schema impact assessment — do any sources reveal gaps?

### 3b. TTB COLA Import
- Enrich existing LWIN wines with grape data, label images, importer info
- Create new records for everyday wines not in LWIN
- Dedup: match COLA records against LWIN backbone by producer + wine name + appellation

---

## Phase 4: Vertical Slice Enrichment — California + Burgundy

**Status:** Not started
**Depends on:** Phases 2-3 provide the wine catalog
**Goal:** Every wine in California and Burgundy moves from Tier 3 to Tier 2 (AI-enriched from reference data). Select producers move to Tier 1 (producer-scraped).

- AI enrichment pipeline design (batch vs on-demand, cost model)
- Tier 3 → Tier 2: Claude synthesizes appellation context, grape profiles, regional climate from reference data
- Tier 2 → Tier 1: Targeted producer scraping for winemaking depth
- Burgundy: test vineyard-level classification, Grand Cru/Premier Cru, négociant vs domaine
- California: test breadth across price points, AVA hierarchy

---

## Phase 5: Label Scanner

**Status:** Not started
**Depends on:** Large wine catalog to match against (Phases 2-3)
**Goal:** User scans a wine label, Loam identifies the wine.

- OCR approach first (photo → text extraction → fuzzy match against wines table)
- Leverage trigram indexes on producer name + wine name
- Build and test — may evolve to include visual matching later
- Label images from TTB COLA as potential reference set

---

## Phase 6: Frontend

**Status:** Not started
**Depends on:** Data worth showing (Phase 4 enrichment proves the model)
**Goal:** Beautiful, information-rich wine pages. Search + label scan as entry points.

- Tiered experience: Tier 1/2/3 wines show different levels of detail
- Not rushed. When it ships, it ships right.

---

## Open Items (Not Phased Yet)

- **Data freshness strategy** — how/when to re-import from LWIN, COLA, etc.
- **Dedup strategy** — consistent matching approach across all import sources
- **Enrichment pipeline architecture** — batch vs on-demand, queue system, cost model
- **Score data licensing** — Wine Spectator, Parker, CellarTracker terms
- **Weather data** — Open-Meteo integration (needs appellation lat/lng)
- **Remaining insight tables** — wine insights, producer insights, soil insights, water body insights
