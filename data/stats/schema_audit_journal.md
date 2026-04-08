# Schema Audit Journal

Append-only log of nightly schema audit runs. Each entry compares to the previous day and flags regressions.

---

### 2026-04-08 (nightly automated run)
- DB size: 8,459 MB (+84 MB vs yesterday — open-meteo drip + dedup merge writes)
- Issues flagged: 12 categories (down from 47 — FK index gaps resolved)
- Regressions vs yesterday: none. Duplicate wine groups dropped 12,671 → 12,546 (−125, dedup merge working)
- New findings: `wine_vintage_scores` at 14.7% dead tuples (>10% threshold — VACUUM needed); wines/producers vacuumed by --fix (were above threshold)
- Fixes applied: VACUUM ANALYZE wines, producers; 0 new indexes

---

### 2026-04-07 (nightly automated run + follow-up)
- DB size: 8,375 MB (+32 MB vs yesterday)
- Issues flagged: 47 categories (down from 58 — whitespace and value violations resolved)
- Regressions vs yesterday: `source_specs` dead tuples new at 6.4%; `wines` dead tuples grew +2.5% to 6.9% (open-meteo drip writes)
- New findings: 12,671 duplicate wine groups (+19 vs baseline — minor, likely new wines added); FK index gaps reduced to 36 columns (8 indexes added last session)
- Follow-up: VACUUM ANALYZE on source_kansas_brands/source_berliner/source_specs/external_ids/wines; added 36 missing FK indexes (migration: add_missing_fk_indexes)

---

### 2026-04-06 (nightly automated run)
- DB size: 8,343 MB
- Issues flagged: 58 categories
- Regressions vs baseline: none — all metrics match the earlier manual run exactly
- New findings: none (first automated run; baseline established)

---

### 2026-04-06 (baseline — manual run)
- DB size: 8,343 MB
- Issues flagged: 58 categories
- Key findings:
  - 44 FK columns without indexes (including wines.region_id, wine_vintage_prices.retailer_id)
  - 12,652 duplicate wine groups (same name + producer)
  - 2,278 grape names with double spaces, 1 appellation with double space
  - 4,446 wine names with double spaces (not in grape check — see whitespace check for wines)
  - 1 vintage_year violation (value outside 0-2026 range)
  - Dead tuples: source_kansas_brands 19.2%, producers 17.1%, external_ids 6.1%
  - wines.varietal_category_id only 39.8% filled
  - wine_vintage_scores.score only 6.4% filled (most are medal-only, no numeric score)
  - 0 orphan FKs, 0 polymorphic FK orphans (clean)
