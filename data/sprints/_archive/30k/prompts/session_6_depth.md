# Session 6: Batch 1 Part 2 — Depth Recovery + Enrichment

**Model:** Sonnet
**Budget:** ~$15 Haiku for Grade C batch (if reached)
**Prerequisite:** Session 5 complete (16,524 wines, 542 producers)

---

## Current State (query DB to verify)

- **16,524 wines**, 542 producers — all from LWIN, all with LWIN external_ids
- **35,963 COLA external_ids** from Session 5 TTB linking
- **8,551 wine_grapes** — 7,973 wines (48%) still missing grapes
- **5,960 wine_vintages** — but only 1,037 distinct wines have a vintage (94% gap!)
- **0 prices, 0 scores, 0 farming certs**
- **Enrichment: all 0 (Grade F)**
- Confirmation: 15,258 C, 1,266 B

### The Big Lever: Archive Recovery

All 16,524 new wines match archive wines via shared LWIN in `external_ids` / `archive_external_ids` (both use `system = 'lwin_7'`, `external_id` = the LWIN code). The archive has rich depth data from 17 rounds of promotion work:

| Data type | Archive rows recoverable | Wines covered |
|-----------|-------------------------|---------------|
| wine_grapes | 11,894 rows | 10,716 wines |
| wine_vintages | 16,499 rows | 4,895 wines |
| -- with ABV | 8,522 | |
| -- with label_image | 8,036 | |
| wine_vintage_scores | 1,947 rows | 630 wines |
| wine_vintage_prices | 11,004 rows | 1,296 wines |
| farming_certs | 330 rows | |

### Staging Sources (all still pointing to archive wines)

All `canonical_wine_id` columns in `source_*` tables still point to `archive_wines` IDs. Need re-linking via LWIN bridge before any staging depth can flow.

---

## Steps

### Step 1: Build the Archive Bridge Table

Create a temporary mapping: `archive_wine_id → new_wine_id` via shared LWIN.

```sql
CREATE TEMP TABLE archive_bridge AS
SELECT DISTINCT ae.entity_id as archive_wine_id, e.entity_id as new_wine_id
FROM archive_external_ids ae
JOIN external_ids e ON e.external_id = ae.external_id
    AND e.system = 'lwin_7' AND ae.system = 'lwin_7'
WHERE ae.entity_type = 'wine' AND e.entity_id IN (SELECT id FROM wines);
```

Verify: should be 16,524 rows (1:1 mapping). If any archive wines map to multiple new wines, investigate before proceeding.

### Step 2: Recover Vintages from Archive

This is the biggest gap (94% of wines have no vintage). Copy `archive_wine_vintages` rows, remapping `wine_id` via the bridge. Key columns to carry over:

- `wine_id` (remapped), `vintage_year`, `abv`, `label_image_url`
- Chemistry: `ph`, `total_acidity`, `residual_sugar`, `volatile_acidity`, `total_so2`, `free_so2`, `brix`
- Winemaking: `fermentation_vessel`, `yeast_type`, `malolactic`, `oak_type`, `oak_duration_months`, `oak_origin`, `oak_percentage_new`, `aging_description`
- Production: `production_bottles`, `serving_temperature_low_c`, `serving_temperature_high_c`, `closure_type`
- Other: `drink_window_start`, `drink_window_end`, `winemaker_notes`, `description`

**Dedup rule:** Skip if a `(wine_id, vintage_year)` row already exists in the new `wine_vintages` table (Session 5's TTB linking already created some).

**Provenance:** Log as `source = 'archive_recovery'` in `data_provenance`.

### Step 3: Recover Grapes from Archive

Copy `archive_wine_grapes` rows, remapping `wine_id`. Columns: `wine_id`, `grape_id`, `percentage`, `percentage_source`.

**Dedup rule:** Skip if `(wine_id, grape_id)` already exists. The pipeline's `_identify_primary_grape()` already created 8,551 rows — don't overwrite those, only add missing ones.

**Important:** Verify that `grape_id` values from archive still exist in the current `grapes` table (they should — reference tables were not archived).

### Step 4: Recover Scores from Archive

Requires wine_vintage_id mapping (archive vintage IDs ≠ new vintage IDs).

Strategy:
1. Build `archive_vintage_bridge`: `archive_vintage_id → new_vintage_id` by matching `(archive_bridge.new_wine_id, vintage_year)`.
2. Copy `archive_wine_vintage_scores` rows, remapping both `wine_id` and `wine_vintage_id`.
3. **Dedup rule:** Use the `idx_scores_dedup(wine_id, vintage_year, publication_id, critic, review_date)` unique constraint — skip conflicts.

### Step 5: Recover Prices from Archive

Same vintage bridge approach as scores.

Copy `archive_wine_vintage_prices` rows, remapping `wine_id` and `wine_vintage_id`. Columns: `wine_id`, `vintage_year`, `price_usd`, `price_original`, `currency`, `price_type`, `source_id`, `merchant_name`, `price_date`, `retailer_id`, `wine_vintage_id`.

### Step 6: Recover Farming Certifications

Simple wine_id remap. Copy `archive_wine_farming_certifications` rows via bridge.

### Step 7: Recover Label Designations from Archive

Check if archive has additional label designations beyond Session 5's 2,836. Copy any extras.

### Step 8: Re-link Staging Sources (Optional — only if time)

Update `canonical_wine_id` in key staging tables to point to new wines:
```sql
UPDATE source_ttb_colas s
SET canonical_wine_id = b.new_wine_id
FROM archive_bridge b
WHERE s.canonical_wine_id = b.archive_wine_id;
```

Repeat for: `source_skurnik`, `source_empson`, `source_kermit_lynch`, `source_winebow`, `source_european_cellars`, `source_texsom`, `source_berliner`, etc.

This enables future importer depth promotion runs against the new wines.

### Step 9: Fill Remaining Gaps

After archive recovery:
- **Color gaps** (~533): cascade from `appellation_rules` for single-color appellations
- **Appellation gaps** (~3,160): check if archive wines had appellation_id set — recover via bridge
- **Grape gaps:** cascade from `appellation_rules` for 100%-single-variety appellations

### Step 10: Update Grades + Metrics

Recalculate:
- `completeness` (0-11 based on filled identity fields)
- `confirmation` (D/C/B/A based on backbone IDs)
- `enrichment` (0 → should jump for wines with scores/prices)
- `identity_complete` boolean

Run mini Josh Test (50-wine sample). Target: maintain 94% findability, depth should jump significantly.

### Step 11: Haiku Batch Enrichment for Grade C (if budget allows)

If Steps 1-10 go smoothly and there's session time remaining:
- Build `pipeline/enrich/batch_c_enrich.py` — Haiku batch for Grade C
- Run on ~500 wines as proof of concept
- Write: `wine_insights.ai_hook`, `wine_insights.ai_style`, `wine_vintage_tasting_insights` (body, sweetness, acidity)
- Budget: ~$1.50-2.50 for 500 wines

---

## Exit Criteria

1. [ ] Archive bridge verified (16,524 1:1 mappings)
2. [ ] Vintage recovery: >4,000 wines with vintages (was 1,037)
3. [ ] Grape recovery: >12,000 wines with grapes (was 8,551)
4. [ ] Score recovery: >500 wines with scores (was 0)
5. [ ] Price recovery: >1,000 wines with prices (was 0)
6. [ ] Label image count: >5,000 vintages with label_image_url
7. [ ] ABV count: >5,000 vintages with ABV
8. [ ] Average completeness: >6.5/11 (was 5.3)
9. [ ] All provenance logged
10. [ ] Mini Josh Test: 94%+ findability, depth >50%
11. [ ] Grades recalculated
12. [ ] 30k_sessions.json, 30k_journal.md, sessions.md updated
13. [ ] Committed and pushed

---

## Cautions

- **Archive grape_id FK:** Verify all grape_ids from archive exist in current `grapes` table before bulk insert.
- **Archive publication_id FK:** Same — verify `publication_id` values still exist.
- **Archive retailer_id FK:** Same for `retailer_id` in prices.
- **Vintage dedup:** Session 5's TTB linking already created ~5,960 vintages. Don't double-create.
- **Score unique constraint:** `idx_scores_dedup` will reject duplicate `(wine_id, vintage_year, publication_id, critic, review_date)`. Use `ON CONFLICT DO NOTHING`.
- **No inference on canonical columns.** Recovery = copying exact data from archive. No probabilistic fills.

---

## Wrap-Up

Update `data/stats/30k_sessions.json` (session 6 → done), `data/stats/30k_journal.md` (full entry), `data/sessions.md`, `memory/30k_status.md`. Commit and push.
