# 30K Plan Journal

Append-only. Every session adds an entry. This is the detailed narrative — what was attempted, what worked, what broke, what surprised us. Different from the session log in the plan (which is a summary table). This is institutional memory.

---

### Session 1: Phase 0 — Archive & Schema — 2026-04-08

**What happened:** Full archive and schema rebuild. Renamed all existing canonical tables to `archive_*`, recreated fresh empty canonical tables with 9 new 30K quality columns, rebuilt all views/RPCs/triggers/RLS/indexes from scratch.

**Key steps:**
- Pre-flight: captured row counts (518,096 wines, 41,758 producers) and dependency scan (all views, RPCs, triggers, FKs, indexes enumerated)
- Archived ~130 indexes on archive_* tables to `archive_` prefix (critical — Postgres doesn't auto-rename indexes on table rename)
- Created fresh canonical tables: wines, producers, wine_vintages, wine_grapes, external_ids, and 50+ child tables
- Added 9 new columns to wines: `display_name`, `confirmation`, `completeness`, `enrichment`, `identity_complete`, `blend_complete`, `appellation_confirmed`, `grapes_confirmed`, `color_confirmed`
- Created `data_provenance` and `ai_suggestions` tables
- Rebuilt 4 views: wine_detail_view, producer_detail_view, wine_vintage_detail_view, wine_search_view
- Built `pipeline/analyze/thirty_k_validate.py` — 21-check validation script

**Surprises:**
- Postgres does NOT auto-rename indexes when you rename a table (ALTER TABLE wines RENAME TO archive_wines leaves `wines_pkey` named `wines_pkey` — creating a fresh `wines` table then gets `wines_pkey1`). Required a dedicated migration to rename ~130 indexes before recreating canonical tables.
- producer_detail_view: fresh producers table has no `data_grade` column (archive did) — had to remove from view definition
- search_catalog RPC column selection syntax differs in psycopg2 context — fixed by using `SELECT *` instead of `SELECT etype, wid`

**Results:**
- 21/21 validation checks passed (100%)
- archive_wines: 518,096 rows preserved
- archive_producers: 41,758 rows preserved
- wines: 0 rows (clean start)
- producers: 0 rows (clean start)
- All reference tables intact (3,662 appellations, 9,695 grapes, 1,165 appellation_rules)
- All staging tables untouched

**Numbers:** 0 new wines, 0 new producers. This was architecture work.

**Next session:** Session 2 — Identity Design + Josh Test Sample. Use Opus. Design country-aware display_name rules for 13+ countries, define cuvée extraction algorithm, build Josh Test sample list.

---

### Planning Session — 2026-04-08

**What happened:** Full strategic planning session. Evolved from a López de Heredia producer scrape walkthrough into a fundamental rethink of the entire data approach.

**Key realizations:**
- The existing 518K wines are 93% garbage. 170K empty shells. Junk producers. Mangled names.
- LWIN isn't as clean as assumed — 1K dedup groups, 14% have no wine name, 37% missing appellation, zero cross-links to COLA.
- Every attempt to fix data in place made things worse (17 rounds of follow-ups, inference revert disaster).
- The wine `name` field conflates cuvée, producer, appellation, grape, and vintage into one string.
- Wine identity varies fundamentally by country — France is appellation-driven, US is varietal-driven, etc.

**What was decided:**
- Build from scratch. Archive existing data. Target 20-25K quality wines.
- Three-metric grading: confirmation, completeness, enrichment.
- AI suggests, sources confirm. Nothing enters canonical on AI confidence alone.
- Price-tier coverage targets: $30-100 is the core (100%).
- Iterative batch approach: Batch 0 prototype (50 producers), then scale.
- 4 sessions planned in detail, rest decided after Batch 0.
- Josh Test as the real-world benchmark.
- Provenance on every data point.
- Display name as computed column, country-aware.
- NV only for actual NV. Blend percentages require source confirmation.
- Varietal name in wine name = source-confirmed grape (label regulation rule).

**Surprises:**
- The "30K LWIN wines with commercial signal" cohort is a natural ~30K group.
- TTB Wine Producer Permit list exists as a free CSV (17,940 US wineries) — we had TTB COLA but never the permit registry.
- Wikidata has CC0 wine producer data with GPS, founding year, websites.

**What to watch for:**
- LWIN licensing — verify commercial use is allowed.
- Cuvée cleaning is the hardest unsolved problem.
- Session continuity is the biggest human risk.
- The suggest+confirm model might limit completeness if too few staging sources confirm Haiku's suggestions.

**Numbers:** 0 new wines, 0 new producers. This was a planning session.
