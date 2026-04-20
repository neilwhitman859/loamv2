# External AI Audit Prompt — Loam Sprint 6 Producer Dedup

Paste the entire contents of this file (including the `CONTEXT`, `TASK`, and `OUTPUT` sections) into a fresh AI session (ChatGPT, Gemini, a new Claude, etc.) along with the bundle URL. The prompt is self-contained — it doesn't assume the AI has any Loam context.

---

```
CONTEXT
=======

I am reviewing a dedup plan for a wine data platform called Loam before it
gets applied to the production database. Nothing has been executed yet. The
plan is frozen at https://github.com/neilwhitman859/loamv2/tree/main/data/sprints/dedup/execution_bundle

Loam's producers table has ~33,281 rows. A significant fraction are known
duplicates (accent variants, country-split global brands, generational
name changes, merchant-prefix rows, JV labels, etc.). Sprint 6 built a
dedup pipeline (AI ladder: Haiku → Gemini → Sonnet → optional Chrome-per-
pair validation) and classified 151,150 producer-pair candidates.

The dedup is split into TWO PHASES:

PHASE 1 — Chrome-validated (493 pairs)
  File: verdict_ledger.jsonl (JSON lines, one per pair)
  Summary: verdict_ledger_summary.md, scorecard.md
  
  These are pairs the pipeline flagged as ambiguous and escalated to
  Chrome-per-pair validation. They were validated ONCE in B6.5a, then
  RE-VALIDATED in B6.6 (this audit's motivation). The B6.6 re-Chrome
  flipped 17% of prior decisions. Every entry has:
    - ledger_key, pair_id, tier, pattern_cluster
    - original_verdict, override_source, override_action
    - final_verdict (what will be applied)
    - final_survivor_name / final_parent_name
    - canonical_redirect_id (when the rename target already exists in DB)
    - Chrome evidence URL
  Final state: 109 MERGE + 50 PARENT_CHILD + 296 SKIP + 33 KEEP_AS_IS +
  5 DEFERRED_SPRINT_7.

PHASE 2 — Pipeline auto-decided, NOT Chrome-validated (150,657 pairs)
  File: phase2_actionable_decisions.jsonl (4,679 rows: MERGE + PC + review queues)
  File: phase2_skip_sample.jsonl (500 random SKIP samples)
  Summary: phase2_summary.md, phase2_risk_analysis.md
  
  The AI ladder auto-decided these without Chrome validation. Breakdown:
    - 3,154 auto_apply_merge   (would mutate DB)
    - 785  auto_apply_pc       (would mutate DB)
    - 740  user_review_*       (flagged for human judgment)
    - 146,435 auto_apply_skip  (no-ops; sample of 500 for spot-check)
    - ~80 smaller buckets
  Each entry has l1/l1.5/l2/l2.5/web verdicts + confidences and the final
  stage3_action routing. Phase 2 is the LARGER slice and has NOT been
  audited.

IDENTITY RULES (condensed from docs/IDENTITY_RULES.md §11):
  A producer is the brand printed on the label.
  - MERGE:        same brand on label (accents, spelling variants, short/full
                  forms, country-split global brands, rename-with-continuity,
                  generational succession of same estate)
  - PARENT-CHILD: distinct brands but one owns/contains the other (holdco,
                  JV with dominant partner, second label, sub-brand of parent)
  - SKIP:         distinct producers with no ownership relation
                  (shared-surname family split, coincidental same-name,
                  different estates in different regions)

KNOWN FAILURE MODES the B6.6 re-Chrome discovered (these patterns flipped
  17% of prior MERGE/PC decisions):

  1. Shared-surname family split (§11.4.m): multiple estates share a
     surname but have distinct brands/regions. Example: "de Montille"
     (Volnay estate) vs "Deux Montille" (Maison Deux Montille négociant)
     -> SKIP. Example: "Willi Brundlmayer" vs "Josef & Philip
     Brundlmayer" (Kamptal sibling estates) -> SKIP.

  2. Generic-château-name dumpster (§11.4.h variant): a single row
     labeled e.g. "Beausejour" contains wines from ≥6 unrelated châteaux
     sharing the name across Fronsac / Puisseguin-SE / Montagne-SE /
     Saint-Estèphe / Chinon / Touraine -> SKIP the proposed merge, flag
     the row for per-wine re-linking in a future sprint.

  3. Cross-region same-name (§11.4.b): "Chalk Hill" Sonoma (US) vs
     "Chalk Hill" McLaren Vale (AU) are unrelated despite same name ->
     SKIP. BUT same-individual cross-border (Philippe Melka, California
     + Bordeaux consulting) -> MERGE.

  4. Brand sub-label mis-classified as MERGE (§11.4.s): "The 75 Wine
     Company" is a distinct on-label brand under Tuck Beckstoffer
     umbrella -> PC not MERGE.

  5. Vineyard-name vs winery-name confusion: "Bishop Creek" (vineyard
     in Yamhill-Carlton that Erath sources) vs "Bishop Creek Cellars"
     (independent winery of same name) -> SKIP.

  6. Commune name misread as brand: "Passopisciaro" is a commune on
     Etna; many producers use it. "Santo Spirito di Passopisciaro" is
     Moretti Cuseri's estate, not a Passopisciaro cuvée -> SKIP.

  7. Retailer/distributor prefix: "Taillevent (Joseph Drouhin)" is a
     Drouhin wine curated for Taillevent restaurant -> MERGE into Drouhin.
     But "Epicure (Franck Massard)" is Massard's distribution company ->
     MERGE into Massard.

THE SINGLE STRONGEST RED FLAG for false MERGE: DB wine-list region/
  appellation incompatibility. If both sides' wine lists are populated and
  they span different regions (e.g. side_a is all Rhône, side_b is all
  Burgundy), the proposed MERGE is almost certainly wrong. This signal is
  visible directly in each JSONL entry's wine lists — no web lookup needed
  to catch it.


TASK
====

Audit the dedup decisions for correctness. Do these passes in order:

PASS 1 — Phase 1 spot check (verdict_ledger.jsonl, 493 pairs)
  - Load verdict_ledger.jsonl.
  - For 30 pairs drawn stratified across (tier × final_verdict), verify:
    a. The final_verdict is correct given the names + pattern_cluster +
       override_reasoning + chrome_evidence.
    b. If canonical_redirect_id is set, does the redirect make sense (the
       existing row should be the correct survivor)?
    c. If it's a MERGE, do the two row names plausibly describe one
       producer?
    d. If it's a PC, does the parent genuinely own/contain the child?
  - Flag any pair you'd overturn (MERGE→SKIP, PC→SKIP, SKIP→MERGE,
    direction-flip, etc.).
  - Focus extra attention on the 17 canonical-row redirects — those are
    where the heuristic could go most wrong.

PASS 2 — Phase 2 auto_apply_merge spot check (3,154 rows)
  - Open phase2_actionable_decisions.jsonl, filter to stage3_action=
    "auto_apply_merge".
  - Sample 50 pairs stratified by country (at least 20 non-US).
  - For each: apply the 7 known failure modes above plus the wine-list
    region-incompatibility heuristic. Flag any that look wrong.
  - Rough-estimate the flip rate: if >10% flipped in your sample, the
    auto_apply_merge queue needs per-pair Chrome validation before
    execution. If <5%, the queue is safe enough to execute after fixing
    the specific flags.

PASS 3 — Phase 2 auto_apply_pc spot check (785 rows)
  - Filter to stage3_action="auto_apply_pc". Sample 30.
  - For each: does the parent genuinely own/contain the child per §11
    rules? Or are they shared-surname siblings that should be SKIP?
  - Flip-rate estimate.

PASS 4 — Phase 2 user_review_pc + user_review_merge_lowconf (716 rows)
  - These are pairs the pipeline itself flagged as uncertain. Sample 20
    from each.
  - For each: give a provisional verdict (MERGE / PC / SKIP) with reasoning.
  - If you can't decide from the JSONL alone and a web lookup is needed,
    note that — don't guess.

PASS 5 — Phase 2 SKIP sample (500 rows in phase2_skip_sample.jsonl)
  - Sample 30 random SKIPs. For each, do the names look like two
    distinct producers (verify that SKIP was right), or do any look like
    missed-MERGE candidates (false negatives)?
  - A false-SKIP rate of >5% suggests the SKIP thresholds in Stage 2 were
    too aggressive and we're missing legitimate dedup work.

PASS 6 — Systemic patterns
  - Across all your flags, what patterns emerge? New failure modes that
    should be codified in §11.4? Clusters of related errors (e.g., all
    flipping on the same surname)?
  - If your flip-rate estimates are significantly higher than Phase 1's
    17% on the auto-apply queue, that changes the "execute Phase 2" vs
    "defer Phase 2" decision.


OUTPUT
======

Produce a single markdown report with:

1. Executive summary (1 paragraph): your overall confidence in the bundle,
   estimated flip rate per pass, any red-flag patterns.

2. Per-pass sections:
   - PASS 1: flags table (pair_id | current final_verdict | proposed |
     reasoning | chrome_evidence check). Number of flags / sample size.
   - PASS 2-5: similar tables.
   - PASS 6: systemic patterns with representative pair_ids.

3. Recommendations for execution:
   - Phase 1: execute as-is? execute-with-fixes? needs more review?
   - Phase 2 auto_apply_merge: execute? sampled-Chrome-validate first?
     defer entirely?
   - Phase 2 auto_apply_pc: same question.
   - Phase 2 review queues: how to handle?
   - New §11.4 amendments you'd propose.

4. Biggest-risk specific flags (Top 10): pair_ids where getting it wrong
   has the highest blast radius (biggest wine counts + most confident
   current verdict). These deserve the most scrutiny before any execute.

Be direct. If the bundle has problems, say so. If it looks clean, say
that. Don't pad. If you find systemic issues that invalidate the
methodology, flag them prominently.

Budget: aim for a report under 3,000 words. Focus on actionable flags,
not narrative.
```

---

## Usage notes

- **Before pasting:** confirm the reviewer AI can fetch GitHub raw files
  (via WebFetch or similar). If not, download the key JSONL files locally
  and attach them to the conversation.
- **File sizes** to be aware of:
  - `verdict_ledger.jsonl` — ~380KB (493 lines)
  - `phase2_actionable_decisions.jsonl` — ~6.4MB (4,679 lines)
  - `phase2_skip_sample.jsonl` — ~265KB (500 lines)
- **If the reviewer can't load 6.4MB files** (ChatGPT-4o has a ~500MB file
  upload limit but the web-chat input has tighter limits), break Phase 2
  into slices by stage3_action and have them audit one slice at a time.
- **Expected report turnaround** for a rigorous audit: 30-60 minutes of
  model time depending on how deeply the reviewer fetches Chrome evidence.
- **Cost on Anthropic API (Claude Opus):** likely $3-8 per audit. Cheaper
  on Gemini/ChatGPT.

## Multi-pass strategy (recommended)

Don't run a single giant audit. Run one reviewer per pass and compare. If
three independent reviewers agree a pair is wrong, high confidence. If
they split, flag for human judgment.

Suggested reviewer set:
- GPT-4o or GPT-4.1 — strong general wine knowledge, web access
- Gemini 2.5 Pro — strong at structured-data reasoning + web grounding
- Fresh Claude Opus — matches Loam's internal session models

Each reviewer should get this same prompt but with a fresh context.
