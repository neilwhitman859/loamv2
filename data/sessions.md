# Session Whiteboard

Active and recent sessions. Read at session start, append when starting/finishing work.

## Active

(none)

## Done

- **2026-04-09 30K Session 6: Batch 1 Part 2 (Depth Recovery)** (Opus) — Archive recovery via LWIN bridge (16,524 1:1 mappings). +14,021 vintages (5,123 wines), +8,708 grapes (11,444 wines), +1,947 scores (630 wines), +13,980 prices (1,296 wines), +330 farming certs, +2,214 label designations, +5,736 external_ids (UPC/QR). Cascades: +1,027 appellation, +780 region, +311 color, +9,943 varietal_category, +1,184 wine_type. Staging relinked (10 tables, TTB deferred — too large). Avg completeness 5.32→6.29. Data grade: 1,712 F→D. Josh Test 33/100 (22% findability expected with 16.5K subset, depth 45% for found wines). $0 AI. Tables: wines, wine_vintages, wine_grapes, wine_vintage_scores, wine_vintage_prices, wine_farming_certifications, wine_label_designations, external_ids, source_* staging tables.

- **2026-04-08/09 30K Session 5: Batch 1 (500 producers)** (Opus) — 542 producers, 16,524 wines, 8,551 wine_grapes, 4,842 vintages, 45,305 external_ids (16K LWIN + 29K COLA). Fixed grape-in-cuvée dedup. Josh Test 47/50=94%. Cleaned 1 Joh. Jos. Prum dupe. $0 AI. Tables: wines, producers, wine_grapes, external_ids, wine_vintages, wine_label_designations, data_provenance.

- **2026-04-08 30K Session 4: Batch 0 Review** (Opus) — Reviewed 1,690 wines. Fixed 2 bugs. Mini Josh Test 26/30=87%. GO for Batch 1. $0 AI.

- **2026-04-08 30K Session 2: Identity Design** (Opus) — Designed complete identity rules for 14 wine countries + fallback (`docs/IDENTITY_RULES.md`). Created 265-wine Josh Test sample (`data/josh_test_sample.json`). Scaffolded 11 pipeline scripts (`pipeline/identity/`, `pipeline/quality/`, `pipeline/analyze/josh_test.py`). Verified 48/48 Batch 0 producers in staging (100%). Confirmed LWIN CC BY 4.0 license. $0 AI cost. No DB writes.

- **2026-04-08 30K Session 1: Phase 0 Archive & Schema** (Sonnet) — Archived 518K wines + 42K producers. Fresh canonical tables with 9 new columns. 21/21 validation checks. $0 AI cost.

- **2026-04-08 Dedup merge session:** Executed `wine_merge.py` on 10,469 ai_accepted groups (25,977 dupe wines). Child data consolidated: 56 vintage merges, 1,453 child repoints, 805 external_id repoints, 918 conflict drops, 1,952 NULL fills. Backfilled `duplicate_of` on 19,552 already-deleted wines. 2 errors (score constraint). Active wines: 477,151. Tables written: `wines`, `wine_vintages`, `wine_grapes`, `external_ids`, `wine_vintage_prices`, `wine_vintage_scores`, `match_decisions`, + all child tables.

- **2026-04-07 Appellation rules Phase 3 loop (cycles 36-40):** Completed the FR/AU/SA/ES/GR appellation_rules seeding loop. Processed 77 appellations across cycles 36-40. Loop self-terminated with 0 remaining for all 5 countries. Final state: **1,165 appellation_rules**, **10,413 appellation_grapes**, 100% provenance, 0 duplicates. Notable: Savennières Coulée de Serrant + La Tâche monopoles, Tursan Baroque 90%+, Mavrodaphne of Patras fortified PDO, Monemvasia-Malvasia dessert PDO, Tegea corrected to Bordeaux-variety PGI (not Moschofilero), Altenberg de Bergheim corrected to white-only (not red-permitted). Gap flagged: Prensal Blanc/Moll absent from grapes table. Tables written: `appellation_rules`, `appellation_grapes`, `wines.color`.

- **2026-04-08 Knowledge seed pipeline:** Built `pipeline/promote/knowledge_seed.py` — multi-stage pipeline to populate notable wines from Claude training data. 920 generated across 32 categories, 656 already existed, 204 promoted after dual Haiku+Sonnet validation. +200 wines, +31 producers, +321 wine_grapes. Backbone IDs: 9/200 (COLA). Sonnet LWIN matching ($0.22) found 43 same-wine pairs but all conflicted with existing entries (deferred to dedup session). Total pipeline cost: ~$2.50.

- **2026-04-07 Producer website scrape + accent cleanup:** Built `pipeline/fetch/producer_site_scrape.py` (v1: 77/100, v2: +7 via Playwright, Haiku fuzzy matching). Built `pipeline/analyze/accent_cleanup.py` (23K+ names fixed). Total: +522 wines, +356 vintages, +560 grapes, +28 winemakers, ~23K accent corrections. Cost: ~$1.84 Haiku. Tables: `wines`, `wine_vintages`, `wine_grapes`, `winemakers`, `producer_winemakers`, `producers`.

- **2026-04-07 Data gap fills + Haiku loop (~$23):** Free fills: +15,653 white-grape→white cascade, +11,171 name-keyword colors, +134,877 appellation_vintages from weather. Haiku tracks: Track A color classify ~$22 (4 parallel runs, 83.5% coverage up from 62.4%), Track B appellation soils $0.34 (930 links across 304 appellations), Track C dupe reclassify $0.68 (2,017 of 2,682 unclear→1,982 true_duplicate + 35 distinct), Track D grape extract $0.35 killed (2% hit rate). Scripts: `pipeline/enrich/haiku_color_classify.py`, `pipeline/enrich/haiku_appellation_soils.py`, `pipeline/enrich/haiku_dupe_reclassify.py`, `pipeline/enrich/haiku_grape_extract.py`. Tables written: `wines.color`, `appellation_vintages`, `appellation_soils`, `match_decisions`.

- **2026-04-07 Incremental singles session:** Parallel data improvements on non-overlapping tables. +3,030 wine_vintage_formats (6 sources, 9 new bottle_formats), +33 producer websites (marquee JSON files), Berliner score dedup (1,530 dated + 126 deleted + 11 resolved → 0 NULL review_dates), +4,238 ABV (Systembolaget/WineDeals/LCBO/BC Liquor/Empson), +3,840 producer region_id + 125 country_id (single-region cascade), +499 wine descriptions (BC Liquor), +8,141 wine_label_designations (unaccent + post-script sweep), +2 retailer websites, +3 publication URLs. Script: `pipeline/promote/label_designation_fill.py`. Zero inference, all conservative NULL-fills.

- **2026-04-07 Color fill session:** +200 wines from appellation_rules single-color cascade (exhausted — only 200 remain in single-color appellations). +40,538 wines white from TTB class_type prefix 81→white (96% validated accuracy, matching-error mismatches only). Skipped prefix 80→red (rosé contamination ~3%), 82→rose (insufficient data). Coverage 54.6%→62.4%. Script: `pipeline/promote/ttb_color_fill.py`. Tables written: `wines.color`.

- **2026-04-07 Session whiteboard design:** Explored parallel session checkout system (10 domains, compatibility matrix, JSON manifest) — decided it was over-engineered. Stripped to simple whiteboard approach: this file + one CLAUDE.md rule. No DB writes.
- **2026-04-07 OCR bake-off session:** Ran OCR bake-off (EasyOCR, RapidOCR, Claude Vision) on 20 test labels. EasyOCR 80%, RapidOCR 74% of Claude baseline. Assessed ROI — tabled label OCR as "someday" in favor of enrichment pipeline work. No DB writes.
