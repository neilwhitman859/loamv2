# Session Whiteboard

Active and recent sessions. Read at session start, append when starting/finishing work.

## Active


## Done

- **2026-04-07 Color fill session:** +200 wines from appellation_rules single-color cascade (exhausted — only 200 remain in single-color appellations). +40,538 wines white from TTB class_type prefix 81→white (96% validated accuracy, matching-error mismatches only). Skipped prefix 80→red (rosé contamination ~3%), 82→rose (insufficient data). Coverage 54.6%→62.4%. Script: `pipeline/promote/ttb_color_fill.py`. Tables written: `wines.color`.

- **2026-04-07 Session whiteboard design:** Explored parallel session checkout system (10 domains, compatibility matrix, JSON manifest) — decided it was over-engineered. Stripped to simple whiteboard approach: this file + one CLAUDE.md rule. No DB writes.
- **2026-04-07 OCR bake-off session:** Ran OCR bake-off (EasyOCR, RapidOCR, Claude Vision) on 20 test labels. EasyOCR 80%, RapidOCR 74% of Claude baseline. Assessed ROI — tabled label OCR as "someday" in favor of enrichment pipeline work. No DB writes.
