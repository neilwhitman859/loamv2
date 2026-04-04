# Loam — Roadmap

*Established 2026-03-13. Updated 2026-03-25.*

---

## Guiding Principles

- **Do it right the first time.** Fast is slow, slow is fast.
- **Real data only.** First-hand sources and government registries. No crowdsourced platforms.
- **Schema before data.** All structural work completed before mass import.
- **Terroir is central.** Burgundy is the benchmark — if we can tell that story well, we can handle anything.
- **Identity first, accuracy first.** Slow and methodical over quick MVP. Prioritize accuracy. On-demand enrichment for user searches.

---

## Phase 1: Foundation ✓ (2026-03-14 – 2026-03-17)

**Status:** COMPLETE
**Goal:** Schema is production-ready. Reference tables are fully populated. Trial imports prove the schema handles diverse wine data.

### 1a. Schema Hardening ✓ (2026-03-14)
Executed the full implementation spec from `docs/SCHEMA_ASSESSMENT.md` Part B:
- All 24 new tables (Tier 0 structural through Tier 2) ✓
- All ~45 column additions on existing tables ✓
- Two rounds of schema hardening from import stress testing ✓
- Reviewed each decision point before executing ✓

### 1b. Reference Data Completion ✓ (2026-03-14 – 2026-03-15)
All reference tables seeded and cross-validated:
- Appellations: 3,662 (3,205 PDO/DOC/AOC + 457 IGT/IGP/PGI/VR/Landwein/base-tier) ✓
- Appellation aliases: 17,558 ✓
- Grapes: 9,693 (VIVC) + 34,820 synonyms ✓
- Appellation grapes: 9,233 (100% coverage) ✓
- Region grapes: 1,673 (100% coverage) ✓
- Country grapes: 541 (100% coverage) ✓
- Classifications: 13 systems, 32 levels ✓
- Label designations: 116 + 75 aliases ✓
- Publications: 71 ✓
- Attribute definitions: 73 ✓
- Tasting descriptors: 304 ✓
- Soil types: 39 ✓
- Farming/biodiversity certifications: 21 + 7 ✓
- Geographic boundaries: countries 100%, appellations 88.8%, regions 99.7% ✓

### 1c. Trial Imports + Schema Stress Testing ✓ (2026-03-15 – 2026-03-16)
- 6 trial producer imports (4 countries) ✓
- KL bulk import (193 producers, 1,467 wines) ✓
- 3 Shopify retailer imports (1,231 wines) ✓
- 10 wine-type stress tests (champagne, port, dessert, fortified, etc.) ✓
- 5 global coverage stress tests (SA, Lebanon, Georgia, Madeira, Champagne) ✓
- Total: 858 producers, 3,095 wines, 2,777 vintages in canonical tables ✓
- Import library hardened across all edge cases ✓

### 1d. Source Research ✓ (2026-03-16 – 2026-03-17)
- 17 source categories researched and documented in `docs/SOURCES.md` ✓
- 6 importer catalog fetchers built and run (~10K wines in JSON files) ✓
- COLA Cloud API tested (22 requests, search vs detail endpoint analysis) ✓
- TTB COLA direct strategy identified (grape varietals are native field) ✓
- Enrichment architecture designed (`docs/ENRICHMENT.md`) ✓
- Multi-source merge architecture designed (`pipeline/lib/merge.py`) ✓
- Full Python migration: 133 pipeline scripts, Node.js archived ✓

### 1e. Search + API Infrastructure ✓ (2026-03-16)
- Full-text search vectors + trigram indexes on all searchable entities ✓
- RPC functions: `search_catalog()` and `search_wines()` ✓
- 4 API views for frontend consumption ✓
- RLS policies on all 94 canonical tables ✓

---

## Phase 2: Multi-Source Data Population (IN PROGRESS)

**Status:** In progress
**Goal:** ~200K+ wines in canonical tables from multiple authoritative sources. Backbone ID matching and dedup working. Every wine has a data grade (F/D/C/B/A).

### Architecture
Per-source staging tables (`source_*`) preserve raw data. Merge layer reconciles into canonical tables. Three-tier matching: Backbone IDs (COLA, LWIN, UPC) → normalized name → fuzzy pg_trgm. See `docs/SOURCES.md` for Backbone ID definitions.

### 2a. Staging Tables + Raw Data Loading ✓
- 30 staging tables, ~4.35M total rows ✓
- `source_ttb_colas`: 3,283K records (Phase 1 CSV + Phase 2 detail/printable scrape) ✓
- `source_pro_platform`: 346K rows (12 US states via PRO Platform) ✓
- `source_tabc`: 183K rows (Texas TABC) ✓
- `source_lwin`: 189K records loaded and promoted to canonical ✓
- `source_kansas_brands`: 65K records ✓
- `source_wv_abca`: 55K records (API dead, archival only) ✓
- `source_berliner`: 74K records (competition data) ✓
- `source_texsom`: 47K records (competition data) ✓
- `source_specs`: 22K records (100% UPC barcodes) ✓
- `source_wallys`: 19K records (prices, distributor mapping) ✓
- UPC sources: OFF 5.2K, Horizon 6.4K (dead), WineDeals 3.2K, PA 5.9K, LCBO 7K, BC Liquor 3.3K, Systembolaget 12.6K ✓
- Importer catalogs: KL, Skurnik, Winebow, Empson, EC promoted to canonical ✓
- Polaner: deprioritized (thin metadata, data retained in staging)

### 2b. TTB COLA Pipeline ✓ SCRAPE COMPLETE
- **Phase 1 (CSV harvest):** ✓ COMPLETE. 3,283,319 records loaded. All years 1955-2026.
- **Phase 2a (initial detail + printable scrape):** ✓ COMPLETE. Detail: 2M records. Printable: 1.55M records.
- **Phase 2b (gap fill + image re-scrape):** ✓ COMPLETE. Detail: 3.18M/3.28M (96.8%) — remaining 104K are non-wine class types. Printable: 1.82M/1.83M 001-format (99.86%) — remaining 2,635 are pre-1997.
- **TTB ID format discovery (2026-03-27):** Non-001 IDs (000/002/003 format, 1.35M records) have no printable page on TTB — confirmed by live browser testing. Appellation/ABV for these must come from COLA-keyed cross-reference with state databases.
- **Image separation:** `application_scan_urls` (full form scans from detail pages) vs `label_image_urls` (individual label photos from printable pages — front, back, strip, neck).
- **Phase 3 (AI parse):** Lower priority now — printable scraper got 96% appellation coverage on 001-format. Useful for 1.35M non-001 records. ~$5-10.

### 2c. COLA-to-COLA Consolidation — DEPRIORITIZED (2026-04-02)
Original plan: JOIN TTB + PRO + TABC + Kansas on shared COLA numbers to enrich TTB staging. Analysis showed near-zero value — where TTB and PRO overlap (211K records), TTB already has the data from the printable scrape. The records TTB is missing data on (1.35M non-001 format) don't overlap with state sources either. Skipping this step.

### 2c. Tiered TTB → Canonical Promotion (revised 2026-04-02)
New approach: promote TTB data in confidence tiers rather than the original 3-layer merge.

**Tier B — Deterministic match to existing canonical:** ✓ COMPLETE (2026-04-04)
- 284,291 canonical wines linked to 666,317 TTB records
- COLA IDs, vintages, grapes, ABV promoted to canonical
- Scripts: `ttb_wine_link_v2.py` (cursor-based fanciful_name matching), `cola_depth.py` (cursor pagination for depth promotion)

**Phase B — New producers + wines from TTB:** ✓ COMPLETE (2026-04-04)
- 4,430 new producers created from unmatched TTB brand names
- 30,493 new wines created from TTB fanciful names for Phase B producers
- Script: `phase_b_wines.py` (UUID slugs, retry logic)

**Tier C — New wines from clean TTB records:** ✓ COMPLETE (2026-04-03)
- Tier C1: +80K wines from TTB with resolved appellation + grapes (existing producers)
- Tier C2: +105K wines via SQL migration (broader criteria)
- Total canonical wines: 470,820

**Tier D — Fuzzy tail (agent work):**
- Ambiguous names, applicant-vs-producer confusion, unresolved appellations
- ~25K distinct TTB (producer, fanciful) pairs unmatched. Promotion agent processes daily. Ongoing.

### 2d. Importer Re-Linking (IN PROGRESS)
- 8,267 unlinked importer wines: KL 928, Skurnik 2,239, Winebow 525, Empson 279, EC 437
- Being handled in separate enrichment sessions
- Richest per-wine metadata — grapes, soil, vinification, scores, chemistry
- Depth promotion (vintages, grapes, scores → canonical tables) runs as links land

### 2e. Competition + Retailer Linking
- Berliner 74K + TEXSOM 47K + Enofile 9K → competition scores/medals
- Spec's 22K UPC barcodes → scan-to-lookup
- Wally's 19K → first price data
- Agent work, runs parallel with Tier D

### 2f. Data Grade Assignment
- F: identity only (producer + wine + country)
- D: has scores or prices
- C: batch Haiku enrichment (appellation context, grape profiles)
- B: on-demand Sonnet enrichment (triggered by user search)
- A: curated (manual verification)

### 2g. Automated Data Quality (BUILT 2026-04-02)
- Scheduled Claude Code task (`data-accuracy-agent`), currently paused — enable after merge pipeline has linked staging → canonical
- Two modes: accuracy (validates existing records against staging sources) + enrichment (promotes unlinked staging, parses TTB names, WebFetches producer websites)
- Infrastructure: `accuracy_audit` table, `last_validated_at` tracking, `sample_wines_for_validation()` RPC
- Runs on Max subscription ($0 incremental), ~15-20 min/session

### Readiness Metric (established 2026-04-02)
- Mystery shopper test: sample N wines from real retailer staging tables (Spec's, Wally's), attempt canonical lookup, score the user experience
- First test: 50 random Spec's bottles → ~56% producer found, ~30% exact wine found, 0% depth, ~14% false matches
- **Current readiness: ~8/100.** Target: 50+ before frontend resumes.

### SQL Consistency Fixes (2026-04-02)
- ✓ 75,774 wines refined from L1 to L2 regions (Burgundy→Côte de Beaune, California→Napa Valley, etc.)
- 3,224 remaining complex cross-boundary mismatches (agent work)
- 1,388 producer-country mismatches identified (mixed — name collisions + legitimate subsidiaries)
- 4 null-country wines confirmed correct (multi-country collaborative blends)

---

## Phase 3: Enrichment Pipeline

**Status:** MVP deployed (2026-04-04). `enrich-wine` Edge Function live. On-demand B-grade enrichment tested on 2 wines (~$0.018/wine, ~25s). Voice/prompt refinement needed before batch spend.
**Depends on:** Phase 2 provides the wine catalog
**Goal:** On-demand enrichment for user searches. Every wine a user looks up gets Grade B content within 5-15 seconds.

- ✅ Edge Function for on-demand Sonnet enrichment (deployed)
- Batch Haiku for pre-warming popular wines to Grade C (pending prompt refinement)
- Vertical slice: California + Burgundy as first enrichment targets
- Enrichment log with cost tracking, prompt versioning, review workflow
- Weather data integration (Open-Meteo, appellation-level)
- See `docs/ENRICHMENT.md` for full architecture

---

## Phase 4: Frontend

**Status:** PAUSED (2026-04-01) — pages built, waiting on data
**Depends on:** Phase 2 merge + Phase 3 enrichment to fill canonical tables
**Goal:** Beautiful, information-rich wine pages. Search + label scan as entry points.

- Vite/React PWA, mobile-first, deployed on Render at loam.onrender.com ✓
- Consumer pages built (2026-03-31): Wine, Producer, Appellation, Region, Grape, Country, Vineyard ✓
- Data-dense dossier design: structured fact grids, minimal prose, maps, knowledge graph connections ✓
- Search + home page ✓
- Design principle: structured data in DB → structured display in UI (Principle #9) ✓
- **Paused because canonical tables are nearly empty** — 189K wines but 1 vintage, 3 scores, 1 grape link. Pages show identity-only shells. Resuming after importer re-promotion + COLA merge fill the tables.

---

## Phase 5: Label Scanner + Barcode

**Status:** Not started
**Depends on:** Large wine catalog with barcodes (Phase 2)
**Goal:** User scans a wine label or barcode, Loam identifies the wine.

- Barcode scan → GTIN/EAN lookup against `wines.barcode`
- OCR approach for labels (photo → text → fuzzy match)
- Leverage trigram indexes on producer name + wine name
- Barcode sources: Vinmonopolet API, state databases (PA), COLA Cloud

---

## Open Items (Not Phased Yet)

- **Data freshness strategy** — how/when to re-import from TTB, LWIN, etc.
- **Score data licensing** — Wine Spectator, Parker, CellarTracker terms
- **Weather data** — Open-Meteo integration (needs appellation lat/lng)
- **COLA Cloud email** — request one-time barcode data export
- **Vinmonopolet API** — Norwegian state monopoly, richest structured source globally
- **EU e-labels** — 500K+ wines with ingredients, nutrition, allergens
- **VineRadar API** — vineyard GPS + terroir data
- **Southern hemisphere importers** — no dedicated importers researched for AU/NZ/AR/CL/ZA
- **Remaining insight tables** — wine, producer, soil, water body insights
