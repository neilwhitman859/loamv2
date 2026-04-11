This is Session 5: Batch 1 — Scale to 500 producers. Read docs/30K_PLAN.md, docs/IDENTITY_RULES.md, and data/stats/30k_journal.md.

Do the gate check: verify Session 4 is done in data/stats/30k_sessions.json, mark Session 5 as in_progress.

**Context from Session 4:**
- Batch 0: 46 producers, 1,690 wines. Pipeline works. 87% Josh Test findability.
- 2 bugs fixed: wines.name now nullable (cuvée = NULL is correct), grape names Title Cased.
- 1 bug remaining: grape-in-cuvée dedup (21 wines have "Cabernet Cabernet" pattern).
- Decision: no per-producer cap. Spread across all price tiers. TTB linking added cautiously.

---

**Step 1: Fix grape-in-cuvée dedup (do this first)**

In `pipeline/identity/batch_pipeline.py`, the `create_wine_from_lwin` method identifies a primary grape from the wine name AND the cuvée extractor sometimes leaves that same grape in the cuvée. Result: "Penfolds Bin 389 Cabernet Cabernet" (grape in cuvée + grape in display name).

Fix: after extracting both `cuvee` and `primary_grape_name`, strip the primary grape from the cuvée if it appears there. Then the display name builder gets a clean cuvée without the grape. Dry-run on Batch 0 producers to verify — the 21 affected wines should lose the duplicate.

**Step 2: Build the 500-producer roster**

Build a roster of ~500 producers for Batch 1. Spread across all price tiers:
- $0-10 grocery (Sutter Home, Woodbridge, Apothic, Cupcake, Bogle, etc.)
- $10-30 popular (Kim Crawford, Oyster Bay, Santa Margherita, Cakebread, etc.)
- $30-100 core (the bulk — Domaine Tempier, Cloudy Bay, Clos du Val, etc.)
- $100-250 premium (Domaine de la Romanée-Conti, Screaming Eagle already in Batch 0, etc.)
- $250+ collectible (Petrus, Le Pin, Rayas, etc.)

Source the roster from:
1. The Josh Test sample (`data/josh_test_sample.json`) — every producer in there should be in Batch 1
2. LWIN staging data — producers with the most wines in `source_lwin`
3. Wine knowledge — fill gaps for well-known producers not in LWIN

For each producer, need: canonical_name, lwin_name (how LWIN stores them), country_code.
Verify each producer exists in `source_lwin` staging before adding (like Session 2 did for Batch 0).
Skip producers already created in Batch 0 (46 producers).

Store the roster in the BATCH_0_PRODUCERS style list in batch_pipeline.py, or better: a separate JSON file like `data/batch1_roster.json`.

**Step 3: Dry-run on a 20-producer sample**

Before running all 500, dry-run on ~20 producers from different countries and price tiers. Review the output: display names, cuvées, grape identification, appellation resolution. Fix any issues.

**Step 4: Execute Batch 1**

Run the full 500-producer roster through batch_pipeline.py with --execute. No per-producer cap. Monitor for errors and resume if needed (the pipeline is resume-safe by slug).

**Step 5: TTB linking (cautious)**

After wines are created, link TTB records for depth data (COLA IDs, vintages, ABV, label images). This is Step 3 in the pipeline (`link_ttb_for_producer`). Run on a 10-producer sample first, verify the linked data looks correct, then scale.

Be cautious: TTB matching is fuzzy (brand_name matching). Verify a sample of links before bulk execution. Bad links = wrong data on the wrong wine.

**Step 6: Measure**

After Batch 1 completes:
- Total wines, producers, wine_grapes, vintages, external_ids
- Completeness distribution
- Identity_complete percentage
- Mini Josh Test (sample 50 wines across tiers)
- Compare to Batch 0 baseline

**Exit criteria:**
- [ ] Grape-in-cuvée dedup fixed and verified
- [ ] 500-producer roster built and verified against staging
- [ ] Dry-run on 20-producer sample reviewed
- [ ] Full Batch 1 executed
- [ ] TTB linking validated on sample, then executed
- [ ] Post-batch metrics documented
- [ ] Mini Josh Test results

**Do not skip the end-of-session wrap-up.** Update 30k_sessions.json, 30k_journal.md, memory/30k_status.md. Commit and push.
