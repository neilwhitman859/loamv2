# Session Whiteboard

Active and recent sessions. Read at session start, append when starting/finishing work.

## Active

- **2026-04-07 Data gap fills + Haiku loop ($20):** Free fills (grape→color cascade, name keyword colors, appellation_vintages from weather) + 4-track Haiku loop (color disambiguation, appellation soils, dupe resolution, grape extraction from names). Tables writing: `wines.color`, `appellation_vintages`, `appellation_soils`, `wine_grapes`, `match_decisions`.

## Done

- **2026-04-07 Incremental singles session:** Parallel data improvements on non-overlapping tables. +3,030 wine_vintage_formats (6 sources, 9 new bottle_formats), +33 producer websites (marquee JSON files), Berliner score dedup (1,530 dated + 126 deleted + 11 resolved → 0 NULL review_dates), +4,238 ABV (Systembolaget/WineDeals/LCBO/BC Liquor/Empson), +3,840 producer region_id + 125 country_id (single-region cascade), +499 wine descriptions (BC Liquor), +8,141 wine_label_designations (unaccent + post-script sweep), +2 retailer websites, +3 publication URLs. Script: `pipeline/promote/label_designation_fill.py`. Zero inference, all conservative NULL-fills.

- **2026-04-07 Color fill session:** +200 wines from appellation_rules single-color cascade (exhausted — only 200 remain in single-color appellations). +40,538 wines white from TTB class_type prefix 81→white (96% validated accuracy, matching-error mismatches only). Skipped prefix 80→red (rosé contamination ~3%), 82→rose (insufficient data). Coverage 54.6%→62.4%. Script: `pipeline/promote/ttb_color_fill.py`. Tables written: `wines.color`.

- **2026-04-07 Session whiteboard design:** Explored parallel session checkout system (10 domains, compatibility matrix, JSON manifest) — decided it was over-engineered. Stripped to simple whiteboard approach: this file + one CLAUDE.md rule. No DB writes.
- **2026-04-07 OCR bake-off session:** Ran OCR bake-off (EasyOCR, RapidOCR, Claude Vision) on 20 test labels. EasyOCR 80%, RapidOCR 74% of Claude baseline. Assessed ROI — tabled label OCR as "someday" in favor of enrichment pipeline work. No DB writes.
