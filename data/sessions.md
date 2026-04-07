# Session Whiteboard

Active and recent sessions. Read at session start, append when starting/finishing work.

## Active

- **2026-04-07 Knowledge seed pipeline:** Built `pipeline/promote/knowledge_seed.py` — multi-stage pipeline to populate notable wines from Claude training data. 920 generated across 32 categories, 656 already existed, 204 promoted after dual Haiku+Sonnet validation. +200 wines, +31 producers, +321 wine_grapes. Backbone IDs: 9/200 (TTB COLA). Sonnet LWIN matching found 43 same-wine pairs but all conflicted with existing entries (deferred to dedup session). Tables: `source_claude_knowledge`, `wines`, `producers`, `wine_grapes`, `external_ids`.
## Done

- **2026-04-07 Producer website scrape + accent cleanup:** Built `pipeline/fetch/producer_site_scrape.py` (v1: 77/100, v2: +7 via Playwright, Haiku fuzzy matching). Built `pipeline/analyze/accent_cleanup.py` (23K+ names fixed). Total: +522 wines, +356 vintages, +560 grapes, +28 winemakers, ~23K accent corrections. Cost: ~$1.84 Haiku. Tables: `wines`, `wine_vintages`, `wine_grapes`, `winemakers`, `producer_winemakers`, `producers`.

- **2026-04-07 Data gap fills + Haiku loop (~$23):** Free fills: +15,653 white-grape→white cascade, +11,171 name-keyword colors, +134,877 appellation_vintages from weather. Haiku tracks: Track A color classify ~$22 (4 parallel runs, 83.5% coverage up from 62.4%), Track B appellation soils $0.34 (930 links across 304 appellations), Track C dupe reclassify $0.68 (2,017 of 2,682 unclear→1,982 true_duplicate + 35 distinct), Track D grape extract $0.35 killed (2% hit rate). Scripts: `pipeline/enrich/haiku_color_classify.py`, `pipeline/enrich/haiku_appellation_soils.py`, `pipeline/enrich/haiku_dupe_reclassify.py`, `pipeline/enrich/haiku_grape_extract.py`. Tables written: `wines.color`, `appellation_vintages`, `appellation_soils`, `match_decisions`.

- **2026-04-07 Incremental singles session:** Parallel data improvements on non-overlapping tables. +3,030 wine_vintage_formats (6 sources, 9 new bottle_formats), +33 producer websites (marquee JSON files), Berliner score dedup (1,530 dated + 126 deleted + 11 resolved → 0 NULL review_dates), +4,238 ABV (Systembolaget/WineDeals/LCBO/BC Liquor/Empson), +3,840 producer region_id + 125 country_id (single-region cascade), +499 wine descriptions (BC Liquor), +8,141 wine_label_designations (unaccent + post-script sweep), +2 retailer websites, +3 publication URLs. Script: `pipeline/promote/label_designation_fill.py`. Zero inference, all conservative NULL-fills.

- **2026-04-07 Color fill session:** +200 wines from appellation_rules single-color cascade (exhausted — only 200 remain in single-color appellations). +40,538 wines white from TTB class_type prefix 81→white (96% validated accuracy, matching-error mismatches only). Skipped prefix 80→red (rosé contamination ~3%), 82→rose (insufficient data). Coverage 54.6%→62.4%. Script: `pipeline/promote/ttb_color_fill.py`. Tables written: `wines.color`.

- **2026-04-07 Session whiteboard design:** Explored parallel session checkout system (10 domains, compatibility matrix, JSON manifest) — decided it was over-engineered. Stripped to simple whiteboard approach: this file + one CLAUDE.md rule. No DB writes.
- **2026-04-07 OCR bake-off session:** Ran OCR bake-off (EasyOCR, RapidOCR, Claude Vision) on 20 test labels. EasyOCR 80%, RapidOCR 74% of Claude baseline. Assessed ROI — tabled label OCR as "someday" in favor of enrichment pipeline work. No DB writes.
