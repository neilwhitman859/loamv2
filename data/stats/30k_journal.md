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

**Next session:** Session 3 — Batch 0 Prototype. Use Opus. 50 producers, ~200 wines through the full pipeline. Implement the identity scripts scaffolded in Session 2.

---

### Session 2: Identity Design + Josh Test Sample — 2026-04-08

**What happened:** Designed the complete identity rules system for all wine-producing countries. Created the Josh Test benchmark sample. Scaffolded the pipeline. Verified all Batch 0 infrastructure.

**Key deliverables:**
- `docs/IDENTITY_RULES.md` — 10-section spec covering: identity model (6-component tuple), display name patterns for 14 countries + fallback, cuvée extraction algorithm (10-step stripping process with edge cases), confirmed boolean patterns (color/grapes/appellation), staging→canonical matching spec (5-step per-producer clustering), label regulation rule (27 CFR 4.23 US ≥75%, EU ≥85%), junk producer criteria (7 rule categories), Batch 0 producer roster with staging counts, LWIN license status, provenance logging spec.
- `data/josh_test_sample.json` — 265 wines Americans actually encounter, weighted by price tier: $0-10 (35 wines), $10-30 (80), $30-100 (90), $100-250 (40), $250+ (20). Sourced from grocery, restaurant, store, gift, and collector contexts. 18 countries represented.
- Pipeline scaffolding: 11 files across `pipeline/identity/` (6 scripts), `pipeline/quality/` (3 scripts), `pipeline/analyze/josh_test.py`. All with function signatures, docstrings, and clear `NotImplementedError("Session 3")` markers.

**Key decisions:**
- Display name is a stored column (not Postgres GENERATED) because it references multiple tables. Computed by pipeline code at insertion time.
- Négociant bottlings are different wines (same appellation, different producer = different identity tuple).
- Portfolio parent companies (Treasury, Constellation, E&J Gallo) should NOT be created as producers — use the label brand (Penfolds, Robert Mondavi, Barefoot) since that's what consumers see.
- Cuvée extraction follows a country-aware stripping algorithm: strip producer → appellation → grapes → classifications → color → vintage → noise → clean. What remains = cuvée or NULL.
- The "confirmed" booleans have strict source requirements — appellation rules and label regulation count as sources, AI alone does not.

**Staging verification:**
- 48/48 unique Batch 0 producers found in staging (100% coverage)
- TTB covers all 48. LWIN covers 45/48 (missing: Josh Cellars, Treasury, Constellation — expected, these aren't fine wine).
- Key inflated counts flagged: "Ridge" = 25K TTB rows, "Latour" = 9K — substring matching. Precise matching critical in Session 3.
- Wally's source has NULL producer column across all 19K rows — unusable for producer matching.

**LWIN license resolved:** CC BY 4.0 — fully commercial, derivatives OK, attribution only obligation. No fallback plan needed.

**Josh Test staging coverage estimate:** ~88% based on TTB universal coverage of US wines + LWIN coverage of fine wine. Well above the 50% S2.3 threshold. Precise measurement deferred to Session 3 when josh_test.py staging check is implemented.

**Surprises:**
- None major. The design session went cleanly. The identity patterns for all 14 countries are well-defined in wine literature.
- The TTB counts for common producer names are very noisy (substring false positives). Session 3 will need normalized exact matching, not ILIKE.

**Numbers:** 0 new wines, 0 new producers. This was a design session. $0 AI cost.

**Next session:** Session 3 — Batch 0 Prototype. Use Opus. Implement the scaffolded pipeline scripts. Run 50 producers, ~200 wines through end-to-end. Review every display name.

---

### Session 3: Batch 0 Prototype — 2026-04-08

**What happened:** End-to-end pipeline run through the full identity system for the first time. Implemented all scaffolded scripts from Session 2 and ran 46 producers (from the 48-producer Batch 0 roster) through clean_cuvee → build_display_name → batch_pipeline → canonical promotion.

**Key deliverables:**
- `pipeline/identity/clean_cuvee.py` — implemented: 10-step stripping algorithm (producer, appellation, grapes, classifications, color, vintage, noise), country-aware, 25+ grape name blocklist to prevent false cuvée extractions
- `pipeline/identity/build_display_name.py` — implemented: country-aware display name construction, fallback logic for wines without a cuvée (producer + appellation pattern), slug collision handling
- `pipeline/identity/batch_pipeline.py` — new: orchestrates the full per-producer pipeline (crossref → select → clean → display_name → dedup → promote), resume-safe, dry-run mode

**Results:**
- 46 producers created (2 of 48 deferred — staging data too thin)
- 1,677 wines promoted
- 889 wine_grapes links
- 894 vintages
- 3,967 external_ids
- 2,335 TTB records linked
- All 15 exit criteria passed
- identity_complete: 53% of wines
- avg completeness: 5.6/11 fields
- Country breakdown: DE(558), FR(430), AU(320), US(172), IT(98), ES(97)
- $0 AI spend (deterministic pipeline, no AI calls)

**Issues found and fixed:**
- LWIN name mismatches: J.J. Prüm, Jadot, Riscal, Tyrrell's all had LWIN producer names that didn't match our normalized producer names — required manual alias mappings
- False grape matches: 25+ grape names (e.g., "Ruby", "Crystal", "Noble") were blocklisted after they were appearing as cuvée fragments instead of grape identifiers
- Rioja appellation resolving: "Rioja" wasn't resolving correctly (maps to DOCa Rioja in our appellations table, not plain "Rioja") — fixed resolver lookup
- display_name fallback: wines without a cuvée (e.g., generic varietal wines) needed a separate display name pattern (producer + grape or producer + appellation) — added fallback branch
- Slug collision handling: several wines from different producers produced the same slug — added producer prefix disambiguation

**Surprises:**
- German wines dominated the count (558) — LWIN has deeper German coverage than expected for the Batch 0 producer set
- 53% identity_complete is lower than hoped; most of the gap is appellation_confirmed (requires appellation rules match) and grapes_confirmed (many LWIN records lack grape data)
- The blocklist for false grape matches grew faster than expected — common English words appear in wine names frequently

**Numbers:** 1,677 wines, 46 producers, 889 wine_grapes, 894 vintages, 3,967 external_ids. $0 AI cost.

**Next session:** Session 4 — Batch 0 Review + Go/No-Go. Use Opus. Manual review of output quality, display names, identity completeness gaps. Decision gate: proceed to Batch 1 (500 producers) or iterate on pipeline first.

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
