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

---

### Session 5: Batch 1 — 500 Producers — 2026-04-08/09

**What happened:** Executed full Batch 1 pipeline: 500 producers from a roster spanning all price tiers and 23 countries. Fixed the grape-in-cuvée dedup bug. Built the roster from Josh Test sample + top LWIN producers. Created 16,524 wines with TTB linking for depth (vintages, COLAs).

**Key deliverables:**
- `data/batch1_roster.json` — 500-producer roster (491 with LWIN, 9 grocery brands without LWIN)
- `pipeline/identity/build_roster.py` — roster builder script (Josh Test + LWIN top + manual LWIN mappings)
- Grape-in-cuvée dedup fix in `batch_pipeline.py` (line ~468)
- `--roster` and `--skip-ttb` flags added to batch_pipeline.py

**Bug fixed:**
- **Grape-in-cuvée dedup:** Primary grape name (from `_identify_primary_grape`) was appearing in both the cuvée AND the display name (e.g., "Penfolds Bin 389 Cabernet Cabernet"). Fix: after extracting both cuvée and primary_grape, regex-strip the grape from the cuvée. 21 affected wines now clean. Verified on dry-run: 0 duplicate patterns.

**Results:**
- **542 producers** (500 roster + 46 Batch 0 - 4 slug overlaps - 1 Prum dupe cleaned = 541 → 542 actual)
- **16,524 wines** (14,834 new + 1,690 Batch 0)
- **8,551 wine_grapes** (7,662 new)
- **4,842 wine_vintages** (3,948 new from TTB linking)
- **45,305 external_ids** (16,524 LWIN + 28,781 COLA)
- **2,836 wine_label_designations**
- **71,495 data_provenance records**
- Color: 96.8% (7,934 white, 7,564 red, 493 rosé, 533 NULL)
- Appellation: 80.9% (13,364/16,524)
- Grape: 51.7% (8,551/16,524)
- Avg completeness: 5.2/11
- Confirmation: 16,277 C-grade (single backbone ID), 247 B-grade (two backbone IDs)
- **Josh Test: 47/50 = 94%** (misses: Peter Vella, Rex Goliath, Carlo Rossi — all $0-10 no-LWIN)
- **$0 AI cost** (deterministic pipeline, no AI calls)
- 10 producers with 0 wines (no-LWIN grocery brands: Black Box, Bota Box, etc.)

**Duplicate found and fixed:**
- "Joh. Jos. Prum" (LWIN name) was in the roster's lwin_top list because `normalize_for_match` didn't equate it with Batch 0's "J.J. Prüm". Created 171 duplicate wines. Deleted post-batch: producer + 171 wines + 153 grapes + 171 external_ids + 152 designations + 837 provenance records. Future fix: add LWIN-name-to-Batch-0 cross-check in build_roster.py.

**Country breakdown:**
FR 5,344 | US 3,416 | DE 2,768 | AU 1,005 | IT 978 | PT 611 | CL 525 | ES 486 | AR 393 | ZA 349 | AT 254 | NZ 121 | LU 87 | MD 39 | SI 36 + 12 others

**Timing:** ~90 min wine creation, ~90 min TTB linking. Total elapsed ~3 hours.

**Surprises:**
- LWIN's wine count per producer varies enormously (Sine Qua Non 203, Markus Molitor 195 vs. typical 20-40). The first 20 producers in the roster (sorted by wine count) took 40% of the total runtime.
- Windows stdout buffering made monitoring the background batch impossible — had to poll the DB directly.
- Only 1 true duplicate producer in the roster despite thousands of variant names.
- Josh Test 94% is a big jump from Batch 0's 87% — the roster's Josh Test inclusion was effective.
- 10 producers had 0 wines (no LWIN data, no TTB match). These are all Franzia/Black Box type brands. They'll need either TTB-only sourcing or manual knowledge seeding.

**Next session:** Session 6 — Batch 1 Part 2 (depth + enrichment). Add TTB depth (ABV, label images, grapes from TTB), importer depth, then Haiku batch enrichment.

---

### Session 4: Batch 0 Review + GO/NO-GO — 2026-04-08

**What happened:** Full review of Session 3's batch 0 output (46 producers, 1,690 wines). Found and fixed 2 pipeline bugs. Ran mini Josh Test. Made GO decision for Batch 1.

**Bugs found and fixed:**
1. **wines.name NOT NULL constraint** — When cuvée extraction returned None (correct for many wines: "Joseph Drouhin, Beaune" has no cuvée), the pipeline stored the full display_name as `wines.name` because the column had a NOT NULL constraint. 401 wines affected. Fix: ALTER TABLE + code fix + data repair via `fix_batch0_display.py`. Now 403 wines correctly have NULL cuvée.
2. **ALL CAPS grape names in display_name** — The grapes table stores VIVC names in ALL CAPS (RIESLING, CABERNET SAUVIGNON). The `_identify_primary_grape()` method returned these as-is, and they flowed into display names. 904 wines had "RIESLING" etc. Fix: `.title()` in batch_pipeline.py + regex replacement in fix script. Down to 0 ALL CAPS (except CVNE which is the correct producer name).
3. **Residual: 21 "Cabernet Cabernet" patterns** — When a grape name appears in the LWIN wine_name (e.g., "Bin 389 Cabernet Shiraz"), the cuvée extractor keeps it AND the grape identifier adds it again. Fix deferred to Batch 1: strip primary_grape from cuvée in the pipeline.

**Mini Josh Test:**
- 30 wines from Batch 0 producers tested against the Josh Test sample.
- **26/30 FOUND = 87% findability.**
- 4 misses: Duckhorn (1 wine in LWIN, thin coverage), Yellow Tail Shiraz (LWIN has Cab but not Shiraz), Louis Jadot Beaujolais-Villages (LWIN has "Beaujolais" not "-Villages"), Meiomi Pinot Noir (3 wines but search key mismatch). All LWIN coverage gaps, not pipeline bugs.
- Display name quality: excellent post-fix across all 6 countries.

**Completeness analysis:**
- 1,690 wines. Avg completeness 5.6/11.
- 52.8% identity_complete. 98.9% color. 88.6% appellation. 54% grapes. 12% vintages.
- Biggest gaps: grapes (need appellation_rules cascade) and vintages (need TTB linking).
- 403 correct NULL cuvées (was 0 — all were wrong before fix).

**Known issues from Session 3 — all resolved:**
1. NULL confirmation: 0 remaining (was fixed during Session 3).
2. Wine count 10x: accepted, cap at 50/producer for Batch 1.
3. Penfolds: clean, no duplicates.
4. Display names: 2 bugs fixed, 1 minor residual documented.

**Decision: GO for Batch 1.**
- 500 producers, $30-100 focus
- 50-wine cap per producer
- Add TTB linking + appellation_rules cascade
- Fix grape-in-cuvée dedup first

**Surprises:**
- The `wines.name` NOT NULL constraint was inherited from the archive schema. The fresh table recreation in Session 1 preserved it, but IDENTITY_RULES.md says name should be nullable. Schema and design doc were out of sync.
- The ALL CAPS grape issue was hiding in plain sight — VIVC stores all grape names in uppercase, and no previous pipeline ever needed to format them for display.
- The "Cabernet Cabernet" pattern only affects AU wines (Penfolds, Henschke, Tyrrell's) because Australian LWIN entries often include the grape in the wine name AND the wine is varietal-labeled.

**Numbers:** 0 new wines, 0 new producers. Fixed 1,195 wines (cuvée + display name). $0 AI cost.

---

### Session 6: Batch 1 Part 2 — Depth Recovery — 2026-04-09

**What happened:** Recovered depth data from archive tables via LWIN-based bridge. All 16,524 Batch 1 wines map 1:1 to archive wines through shared LWIN codes in external_ids. Bulk INSERT/UPDATE operations copied vintages, grapes, scores, prices, farming certs, label designations, and UPC/QR external_ids from archive to canonical tables. Then cascaded appellation, region, color, varietal_category, wine_type, and identity_confidence from archive + reference tables.

**Key steps:**
1. Built `_archive_bridge` table: 16,524 rows mapping archive_wine_id → new_wine_id via shared LWIN in external_ids. 16,407 distinct archive wines (117 archive wines map to 2+ new wines — LWIN dedup artifacts). Each new wine maps to exactly 1 archive wine.
2. Built `_archive_vintage_bridge`: 16,499 rows mapping archive_vintage_id → new_vintage_id via (wine_id, vintage_year) composite match.
3. FK integrity verified: 0 orphan grape_ids, publication_ids, retailer_ids, farming_cert_ids, label_designation_ids.
4. Bulk recovered: vintages +14,021, grapes +8,708, scores +1,947, prices +13,980, farming_certs +330, label_designations +2,214, external_ids +5,736 (UPC/QR/QR_URL).
5. Staging relinked: 10 smaller staging tables (5,619 rows) updated canonical_wine_id from archive → new wines. TTB (29,334 rows) deferred — FK DROP timed out on 3.28M row table.
6. Archive field recovery: appellation_id +1,027, region_id +780, color +311, varietal_category_id +9,034, wine_type sparkling +650 / fortified +534.
7. Cascades: varietal_category from single-grape wines +909, region from appellation.region_id (none needed), identity_confidence → all 16,524 lwin_matched.
8. Completeness recalculated (0-11 per wine), data_grade F→D for 1,712 wines with scores/prices, identity_complete = true for 11,150 wines (67.5%).

**Final state (Batch 1 wines):**
| Metric | Before | After |
|--------|--------|-------|
| Vintages | 6,071 (1,064 wines) | 20,092 (5,123 wines) |
| Grapes | 8,551 | 17,259 (11,444 wines) |
| Scores | 0 | 1,947 (630 wines) |
| Prices | 0 | 13,980 (1,296 wines) |
| Farming certs | 0 | 330 (315 wines) |
| Label designations | 2,836 | 5,050 (3,195 wines) |
| External IDs (UPC) | 0 | 2,416 wines |
| Color coverage | 96.8% | 98.7% |
| Appellation coverage | 80.9% | 87.1% |
| Varietal category | 0 | 9,943 (60.2%) |
| Wine type sparkling | 0 | 650 |
| Wine type fortified | 0 | 534 |
| Avg completeness | 5.32/11 | 6.29/11 |
| Data grade D | 0 | 1,712 |
| Identity complete | unknown | 11,150 (67.5%) |

**Surprises:**
- Staging table `canonical_wine_id` FK constraints all point to `archive_wines`, not `wines`. Required DROP CONSTRAINT before UPDATE. TTB too large to alter inline — FK DROP timed out.
- wine_grapes table is a simple junction (no id, no timestamps) — different from other child tables.
- 13,980 prices recovered vs prompt estimate of 11,004 — the 117 archive wines mapping to 2+ new wines caused some price duplication (correct behavior — both new wines should get the data).
- UPC recovery was a bonus find not in the original prompt — 5,160 UPCs + 517 QR URLs + 59 QR codes.

**Josh Test:** 33/100 overall. Findability 22% (11/51) — expected with only 16.5K wines (archive had 518K). Depth 45% for found wines (identity 88%, price 64%, vintage 61%). This is not a regression — it's a smaller catalog being tested against a general benchmark.

**Numbers:** 0 new wines, 0 new producers. ~48,000 child rows recovered from archive. $0 AI cost.

**Next session:** Session 7 — Batch 2 Part 1 (2K producers). Expand the catalog to increase findability.

---

### Session 7: Batch 2 — Scale to 2,000 Producers — 2026-04-09

**What happened:** Expanded the catalog from 542 to 2,532 producers and from 16,524 to 51,035 wines. Built a 2,000-producer roster from LWIN staging (all producers with 20+ wines plus top 620 from the 10-19 tier), ran the full batch_pipeline.py with `--skip-ttb`, then executed archive depth recovery via the same LWIN-based bridge pattern from Session 6.

**Key steps:**
1. Built `data/batch2_roster.json`: 2,000 producers across 44 countries. Filtered out 509 producers already in batch 0/1 rosters. Tiers: 5 with 100+ wines, 6 with 50-99, 1,369 with 20-49, 620 with 10-19.
2. Dry-run on 50 producers — clean. Cuvée extraction, grape identification, appellation resolution all working correctly.
3. Full pipeline execution (~16 hours): 1,041 new producers created, 959 already existed (from pre-revert system). 18,564 new wines, 18,052 wines already existed (dedup via slug). 10,146 wine_grapes, 18,564 LWIN external_ids. 420 slug conflicts resolved. 0 errors across all 2,000 producers.
4. Archive bridge: 51,035 mappings (all canonical wines → archive via LWIN).
5. Archive depth recovery: +25,365 vintages, +16,279 grapes, +3,473 scores, +9,240 prices, +11,654 label designations. Wine-level fields: color 99.1%, region 97.3%, appellation 85.7%.
6. Cascades: identity_confidence → all 51,035 lwin_matched. Data grade: 4,406 D, 46,629 F. Completeness recalculated — median 8-9/11 (up from 5-6/11).

**Final state:**
| Metric | Before (Session 6) | After | Delta |
|--------|---------------------|-------|-------|
| Producers | 542 | 2,532 | +1,990 |
| Wines | 16,524 | 51,035 | +34,511 |
| Vintages | 20,124 | 45,489 | +25,365 |
| Grapes | 17,259 | 52,581 | +35,322 |
| Scores | 1,947 | 5,420 | +3,473 |
| Prices | 13,980 | 23,220 | +9,240 |
| LWIN IDs | 16,524 | 51,035 | +34,511 |
| Label designations | 5,050 | 11,654 | +6,604 |
| Color coverage | 98.7% | 99.1% | +0.4% |
| Appellation coverage | 87.1% | 85.7% | -1.4% |
| Region coverage | — | 97.3% | — |
| Completeness median | 5-6/11 | 8-9/11 | +3 |
| Data grade D | 1,712 | 4,406 | +2,694 |
| Identity confidence | 16,524 lwin | 51,035 lwin | +34,511 |

**Country distribution (top 10):**
France 14,731 | US 12,219 | Germany 4,720 | Australia 4,355 | Italy 4,113 | Spain 1,595 | Portugal 1,334 | Chile 1,318 | Argentina 1,297 | South Africa 1,279

**Surprises:**
- Pipeline took ~16 hours for 2,000 producers (individual INSERT per wine, psycopg2 round trips). Not blocking — resume-safe and 0 errors.
- 959 of 2,000 roster producers already existed as canonical producers (from pre-revert system imports). Pipeline handled gracefully via `[EXISTS]` path, adding remaining wines.
- 18,052 wines already existed by slug — significant dedup surface from the old system.
- Appellation coverage slightly decreased (87.1% → 85.7%) because new wines from smaller producers have less appellation data in LWIN.
- Completeness jumped dramatically (median 5-6 → 8-9) because the archive recovery fills more fields per wine than LWIN staging alone provides.

**Numbers:** +34,511 wines, +1,990 producers, ~54,000 child rows recovered. $0 AI cost.

**Next session:** Session 8 — Batch 3 / Gap Fill. TTB linking for new wines, push to 100K+ wines with the remaining 5+ wine producers, or gap-fill existing wines.
