# Session 7.2: TTB Linking + Josh Test

**Model:** Sonnet (or Opus)
**Budget:** $0 AI (pipeline is deterministic)
**Prerequisite:** Session 7 complete (51,035 wines, 2,532 producers, all with LWIN)

---

## Current State (query DB to verify)

- **2,532 producers**, **51,035 wines** — all with LWIN external_ids
- Archive depth recovered: 45K vintages, 53K grapes, 5.4K scores, 23K prices
- **TTB linking not yet run on batch 2 wines** — only batch 0 (46 producers) had TTB linked via the pipeline
- source_ttb_colas: 3.28M records with brand_name, fanciful_name, appellation, grapes, vintage, ABV, label images
- TTB linking adds: COLA IDs, vintages, ABV, label image URLs, grape data

---

## Goal

1. Link batch 2 wines to TTB for COLA depth data
2. Run Josh Test to measure findability at the 51K wine mark
3. Assess whether Batch 3 (long-tail producers) is needed or if we should pivot to enrichment

---

## Steps

### Step 1: TTB Linking via batch_pipeline

The pipeline's `link_ttb_for_producer` method is already built (batch_pipeline.py lines 633-685). It:
1. Gets all wines for a producer
2. Searches source_ttb_colas by brand_name (with prefix/suffix stripping)
3. Matches TTB fanciful_name to wine cuvée
4. Creates COLA external_ids, vintages (with ABV), and label image URLs

**Approach A — Re-run pipeline without --skip-ttb:**
```
python -m pipeline.identity.batch_pipeline --roster data/batch2_roster.json --execute
```
This will find all producers via `[EXISTS]`, skip wine creation (wines exist by slug), and run TTB linking. It's the simplest approach but re-processes 2,000 producers serially.

**Approach B — Standalone TTB linker:**
Write a focused script that iterates canonical producers and runs `link_ttb_for_producer` for each. Faster because it skips wine discovery/creation entirely.

**Recommendation:** Approach B. Write a small script `pipeline/identity/ttb_link_batch.py` that:
1. Loads the batch2 roster
2. For each producer, looks up the canonical producer by slug
3. Calls the TTB linking logic directly
4. Commits per-producer

This avoids the overhead of wine discovery and should run 3-5x faster.

**IMPORTANT:** The TTB linking has a `LIMIT 1000` on TTB records per producer. For large producers (e.g., Louis Jadot with 289 wines), this might not be enough. Check if any producer has >1000 TTB records and raise the limit if needed.

### Step 2: Validate TTB linking on sample

After linking, check:
- How many wines now have COLA external_ids?
- How many new vintages were created?
- How many label_image_urls were set?
- Spot-check 10 producers across different countries for matching quality

### Step 3: Run Josh Test

```
python -m pipeline.analyze.josh_test
```

This tests ~265 wines from `data/josh_test_sample.json` against the canonical DB. Report:
- Overall findability (target: 50%+, up from 22%)
- Findability by price tier
- Findability by country
- Depth scores for found wines
- Which wines are still NOT found (to inform whether Batch 3 is needed)

### Step 4: Assess next steps

Based on Josh Test results:

**If findability >= 50%:**
- Skip Batch 3 (long-tail 1-4 wine producers)
- Proceed to enrichment (Sessions 8-10)
- The long tail can be added later if needed

**If findability < 40%:**
- Identify which MISSING wines come from producers we don't have
- Assess whether those producers are in LWIN (just in the 1-4 wine tier) or not in LWIN at all
- If mostly in LWIN: consider Batch 3 with bulk INSERT optimization
- If mostly not in LWIN: need TTB-first promotion (different pipeline)

**If findability 40-50%:**
- Targeted gap fill — add specific missing producers rather than the full long tail
- Then proceed to enrichment

### Step 5: Pipeline performance assessment

If Batch 3 is recommended:
- Profile the current pipeline's bottleneck (individual INSERT round trips)
- Prototype a bulk INSERT version of `create_wine_from_lwin`
- Estimate time for remaining ~9,500 producers with 5-9 wines (~39K wines)
- If bulk INSERT gets it under 4 hours, proceed; otherwise consider whether the ROI justifies the effort

If Batch 3 is NOT recommended:
- Document the decision in DECISIONS.md
- Note the remaining LWIN pool size for future reference

---

## Exit Criteria

1. [ ] TTB linking complete for batch 2 wines
2. [ ] COLA external_ids count increased
3. [ ] New vintages/ABV/label_images from TTB
4. [ ] Josh Test run with results documented
5. [ ] Next-step decision made (Batch 3 vs enrichment)
6. [ ] 30k_sessions.json, 30k_journal.md, sessions.md updated
7. [ ] Committed and pushed

---

## Cautions

- **TTB LIMIT 1000:** The pipeline limits TTB records per producer to 1000. Large négociants (Louis Jadot, Bouchard) may have more. Check and raise if needed.
- **TTB matching is fuzzy:** brand_name matching uses prefix/suffix stripping but can produce false positives (e.g., "Ridge" matching "Blue Ridge"). Validate a sample.
- **Josh Test is the decision point:** Don't commit to Batch 3 without seeing the Josh Test numbers first.
- **No inference on canonical columns.** TTB data is direct source data (COLA IDs, vintages, ABV from the federal registry).
