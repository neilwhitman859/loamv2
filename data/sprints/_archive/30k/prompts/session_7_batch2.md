# Session 7: Batch 2 — Scale to 2,000 producers

**Model:** Sonnet (or Opus)
**Budget:** $0 AI (pipeline is deterministic)
**Prerequisite:** Session 6 complete (16,524 wines with depth, 542 producers)

---

## Current State (query DB to verify)

- **542 producers**, **16,524 wines** — all from LWIN, all with LWIN external_ids
- Depth recovered in Session 6: 20K vintages, 17K grapes, 2K scores, 14K prices
- **172,881 LWIN wines remaining** across **32,724 producers** not yet promoted
- source_lwin.canonical_wine_id all point to archive_wines (stale — ignore this column)
- Staging FK constraints on canonical_wine_id: **all dropped** (DECISIONS.md 2026-04-09)
- Batch pipeline: `pipeline/identity/batch_pipeline.py` — proven on 542 producers

### Remaining LWIN Pool

| Tier | Producers | Wines |
|------|-----------|-------|
| 100+ wines | 14 | 2,018 |
| 50-99 | 21 | 1,386 |
| 20-49 | 1,514 | 39,186 |
| 10-19 | 3,955 | 53,570 |
| 5-9 | 5,903 | 39,427 |
| 1-4 | 21,317 | 37,268 |

---

## Goal

Add **~2,000 producers** and their wines from LWIN staging. Target: 60,000-80,000 total wines after this session. This should push Josh Test findability from 22% to 50%+.

---

## Steps

### Step 1: Build the 2,000-producer roster

Strategy: take ALL producers with 5+ LWIN wines (11,407 producers, ~135K wines). This covers the meaningful catalog — the 21K producers with 1-4 wines are the long tail and can wait for Batch 3.

But 11K producers is too many for one session. Prioritize:

1. **All 14 producers with 100+ wines** — large portfolios (négociants, co-ops)
2. **All 21 producers with 50-99 wines** — significant producers
3. **All 1,514 producers with 20-49 wines** — core catalog
4. **Top ~450 from 10-19 tier** — to reach ~2,000 total

Selection criteria for the 10-19 tier cutoff:
- Prefer producers appearing in Josh Test sample (`data/josh_test_sample.json`)
- Prefer producers with TTB-linked staging records (more depth potential)
- Prefer producers from underrepresented countries

Build the roster as `data/batch2_roster.json` in the same format as `data/batch1_roster.json`:
```json
{
  "_meta": {"description": "...", "total": N, "session": "30K Session 7"},
  "producers": [
    {"canonical_name": "...", "lwin_name": "...", "country_code": "XX", "price_tier": "...", "source": "lwin_staging", "lwin_wines": N}
  ]
}
```

**Important:** The `lwin_name` field must EXACTLY match `source_lwin.producer_name`. The pipeline uses this for lookup. Query source_lwin to get the exact producer_name values.

**Skip:** Any producer already in the canonical `producers` table (542 from Batch 0+1).

### Step 2: Modify batch_pipeline.py to accept JSON roster

Currently the pipeline has a hardcoded `BATCH_0_PRODUCERS` list. Modify to accept a `--roster` flag:
```
python -m pipeline.identity.batch_pipeline --roster data/batch2_roster.json --execute
```

The roster JSON replaces the hardcoded list. Each entry maps to the same `(canonical_name, lwin_name, country_code)` tuple the pipeline already uses.

If the pipeline already supports `--roster`, skip this step.

### Step 3: Dry-run on 50-producer sample

Pick 50 producers across different wine counts and countries. Dry-run through the pipeline. Check:
- Display names look correct
- Cuvée extraction working
- No grape-in-cuvée duplicates
- Appellation resolution reasonable
- No crashes on large producers (100+ wines)

### Step 4: Execute Batch 2

Run the full roster. The pipeline is resume-safe by slug — if it crashes, just re-run. Monitor progress.

**Expected output:** ~2,000 new producers, ~50,000-70,000 new wines, proportional wine_grapes and external_ids.

### Step 5: TTB linking

Run TTB linking for the new wines (Step 3 in batch_pipeline). This gives COLA IDs, vintages, ABV, label images.

Validate on a 20-producer sample first, then execute in bulk.

### Step 6: Archive depth recovery (repeat Session 6 pattern)

After wines are created and TTB-linked, run the same archive bridge recovery:

1. Build `_archive_bridge` (archive_wine_id → new_wine_id via shared LWIN)
2. Recover: vintages, grapes, scores, prices, farming certs, label designations, UPC/QR external_ids
3. Recover wine-level fields: appellation_id, region_id, color, varietal_category_id, wine_type
4. Cascade: region from appellation, identity_confidence, completeness, data_grade, identity_complete
5. Drop bridge tables

This is the same SQL pattern from Session 6 — can be scripted if there's time.

### Step 7: Measure

After recovery:
- Total wines, producers, all child table counts
- Completeness distribution
- Josh Test (50-wine sample) — target 50%+ findability
- Country distribution

---

## Exit Criteria

1. [ ] 2,000+ new producers created
2. [ ] 50,000+ total wines
3. [ ] TTB linked (COLA IDs on new wines)
4. [ ] Archive depth recovered (vintages, grapes, scores, prices)
5. [ ] Completeness recalculated
6. [ ] Josh Test findability >40%
7. [ ] 30k_sessions.json, 30k_journal.md, sessions.md updated
8. [ ] Committed and pushed

---

## Cautions

- **Pipeline memory:** 2,000 producers × ~30 wines avg = ~60K wines. The pipeline loads reference data into memory. If it OOMs, batch into 500-producer chunks.
- **TTB matching:** Fuzzy brand_name matching can produce false positives. Validate a sample.
- **Archive bridge:** Some new wines may not have LWIN matches in archive (if they're from producers the old system didn't have). Bridge will be smaller than total new wines — that's fine.
- **Slug collisions:** The pipeline handles these, but monitor for warnings.
- **No inference on canonical columns.** All data from LWIN staging or archive recovery.

---

## Wrap-Up

Update `data/stats/30k_sessions.json` (session 7 → done), `data/stats/30k_journal.md` (full entry), `data/sessions.md`, `memory/30k_status.md`. Commit and push.
