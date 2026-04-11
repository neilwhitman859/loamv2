This is Session 4: Batch 0 Review. Read docs/30K_PLAN.md, docs/IDENTITY_RULES.md, and data/stats/30k_journal.md.

Do the gate check: verify Session 3 is done in data/stats/30k_sessions.json, mark Session 4 as in_progress.

Session 3 created 46 producers and 1,677 wines via pipeline/identity/batch_pipeline.py. This session reviews that output and decides GO/NO-GO for scaling.

**Step 1: Investigate known issues (from Session 3)**

The 30K_PLAN.md Session 4 section has 4 known issues. Investigate each:

1. Find the 2 wines with NULL confirmation — diagnose and fix.
2. Wine count is 1,677 (target was 150-250). The big producers pulled full LWIN catalogs (J.J. Prüm 171, Dönnhoff 117, Penfolds 114). Is this a problem? Recommend whether future batches should cap per-producer.
3. Check Penfolds — it already existed canonically. Are there duplicate wines or orphaned entries?
4. Spot-check 10 display names per country (FR, US, IT, ES, DE, AU) for correctness.

**Step 2: Mini Josh Test**

Sample 50 wines Americans actually encounter (restaurants, grocery stores, wine shops). Query the DB: can Loam find them? What's the display_name quality? What's the completeness?

**Step 3: Decide next steps**

Based on the review:
- Is the pipeline working well enough to scale? 
- What batch 1 size makes sense?
- Any pipeline fixes needed before scaling?

Update docs/30K_PLAN.md with findings and decision.

**Do not skip the end-of-session wrap-up.** Follow the iterative dry-run loop from 30K_PLAN.md. Mark Session 4 done, update 30k_sessions.json, append to 30k_journal.md, update memory/30k_status.md, commit and push.
