# Loam — Roadmap

*Established 2026-03-13. Updated 2026-03-17.*

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
- Import library (`lib/import.mjs`) hardened across all edge cases ✓

### 1d. Source Research ✓ (2026-03-16 – 2026-03-17)
- 17 source categories researched and documented in `docs/SOURCES.md` ✓
- 6 importer catalog fetchers built and run (~10K wines in JSON files) ✓
- COLA Cloud API tested (22 requests, search vs detail endpoint analysis) ✓
- TTB COLA direct strategy identified (grape varietals are native field) ✓
- Enrichment architecture designed (`docs/ENRICHMENT.md`) ✓
- Multi-source merge architecture designed (`lib/merge.mjs`) ✓

### 1e. Search + API Infrastructure ✓ (2026-03-16)
- Full-text search vectors + trigram indexes on all searchable entities ✓
- RPC functions: `search_catalog()` and `search_wines()` ✓
- 4 API views for frontend consumption ✓
- RLS policies on all 94 canonical tables ✓

---

## Phase 2: Multi-Source Data Population (IN PROGRESS)

**Status:** In progress
**Goal:** ~200K+ wines in canonical tables from multiple authoritative sources. Identity matching and dedup working. Every wine has a data grade (F/D/C/B/A).

### Architecture
Per-source staging tables (`source_*`) preserve raw data. Merge layer reconciles into canonical tables. Three-tier matching: key-based (COLA ID, LWIN, barcode) → normalized name → fuzzy pg_trgm.

### 2a. Staging Tables + Raw Data Loading (IN PROGRESS)
- `source_ttb_colas`: Phase 1 CSV harvest running on local machine (~16 hours) ⏳
- `source_kansas_brands`: 31,216 wine records loaded ✓
- `source_lwin`: 184,497 records loaded ✓
- Importer catalogs: 10K wines in JSON files, not yet in staging tables

### 2b. TTB COLA Pipeline
- **Phase 1 (CSV harvest):** Running locally. 1955-present, wine class types 80-89. ⏳
- **Phase 2 (detail scrape):** Fetch grape varietals + applicant data from detail pages. Filter Phase 1 output first (skip expired/surrendered, deduplicate label refreshes). 3-7 days at polite rate.
- **Phase 3 (AI parse):** Haiku extracts vintage, wine name, appellation from fanciful names. ~$5-10.

### 2c. Key-Based Joins (Layer 1)
- JOIN `source_ttb_colas` + `source_kansas_brands` ON cola_id — trivial SQL join
- Produces enriched staging view: TTB identity + Kansas ABV/appellation/vintage
- Group COLAs into wine identities (many COLAs → one wine)
- Store COLA IDs in `external_ids`

### 2d. LWIN Overlay (Layer 2)
- Match LWIN records against Layer 1 by normalized producer + wine name
- Adds LWIN codes to existing records, creates new for fine wines not in TTB/Kansas
- ~187K wines, 30-50% estimated overlap with TTB

### 2e. Rich Source Merge (Layer 3)
- Importer catalogs (10K wines) merge against Layers 1+2
- Adds depth: soil, vinification, farming certs, scores
- Mostly enrichment, not identity creation
- Also: retailer imports, state databases

### 2f. Data Grade Assignment
- F: identity only (producer + wine + country)
- D: has scores or prices
- C: batch Haiku enrichment (appellation context, grape profiles)
- B: on-demand Sonnet enrichment (triggered by user search)
- A: curated (manual verification)

---

## Phase 3: Enrichment Pipeline

**Status:** Not started
**Depends on:** Phase 2 provides the wine catalog
**Goal:** On-demand enrichment for user searches. Every wine a user looks up gets Grade B content within 5-15 seconds.

- Edge Function for on-demand Sonnet enrichment
- Batch Haiku for pre-warming popular wines to Grade C
- Vertical slice: California + Burgundy as first enrichment targets
- Enrichment log with cost tracking, prompt versioning, review workflow
- Weather data integration (Open-Meteo, appellation-level)
- See `docs/ENRICHMENT.md` for full architecture

---

## Phase 4: Frontend

**Status:** Not started
**Depends on:** Enrichment pipeline working (Phase 3)
**Goal:** Beautiful, information-rich wine pages. Search + label scan as entry points.

- Vite/React PWA, mobile-first
- Tiered experience: Grade F/D/C/B/A wines show different levels of detail
- Input methods: text search, barcode scan (later), label photo (later)
- Not rushed. When it ships, it ships right.

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
