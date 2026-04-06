# Schema Audit Journal

Append-only log of nightly schema audit runs. Each entry compares to the previous day and flags regressions.

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
