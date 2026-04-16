# Sprint 6: Dedup (Producers) — journal

**Opened:** 2026-04-16
**Status:** Active — LWIN import first, then dedup evaluation + execution
**Current block:** B6.1 complete; B6.2 next (LWIN import)

---

## Block log

- **B5.8 (2026-04-16):** Sprint 5 closed, Sprint 6 opened. Preliminary research
  gathered in `plan.md` so B6.1 can dive into design. User directive: "throw the
  kitchen sink at this — use AI in new and creative ways." Session prompt for
  B6.1 saved to `data/session_prompts/b6_1_planning.md`.

- **B6.1 (2026-04-16):** Planning + design. Resolved 10 initial + 8 clarifying
  + 4 definition-edge-case design questions across multiple rounds of dialog.
  Plan underwent three major reshapes during the session in response to user
  input:

  **Reshape 1:** from "evaluation-only" to "tiered AI ladder with in-sprint
  execution" after user clarified the quality bar is final-state correctness.

  **Reshape 2:** from web-grounded-on-all-pairs to tiered ladder with
  web-grounding only at the rigor tier (L3) after cost analysis showed
  $500-1,000 for web on all pairs vs $50-100 at L3 only.

  **Reshape 3:** from "L3 + L4 cascade" to "B6.2 = LWIN import only, then
  dedup" after discovering 24,762 unlinked LWIN producers in staging. Sprint
  restructured around LWIN-first sequencing.

  **Key decisions (final):**

  - **Sprint shape:** single sprint covering LWIN import + evaluation +
    execution. B6.2 LWIN → B6.3 schema/rules/L1 → B6.4 L2/L3/anchor → B6.5
    L4/review → B6.6 execution → B6.7+ iterate → B6.N close.
  - **Producer identity = brand-on-label.** MERGE = same brand. PARENT-CHILD
    = distinct brands with ownership. SKIP = unrelated. Edge cases resolved:
    renames → MERGE+alias; dissolved+reopened → one continuous producer;
    private-labels → producer with per-wine actual_vintner in metadata;
    retailers (Trader Joe's, Costco) → never producers; second wines → wines
    not producers; négociant+estate → PARENT-CHILD; accents → MERGE with
    survivor matching actual label form.
  - **Tiered AI ladder:** L1 Haiku batched (10/call) on all pairs in
    definitive list → L2 Haiku batched (5/call) on L1 uncertain → L3 Sonnet
    4.6 with Anthropic native `web_search_20250305` tool on L2 MERGE/UNCERTAIN
    → L4 Opus-inline-1M cross-pair audit (free in-session).
  - **Claude models direct via Anthropic SDK**, not OpenRouter. Unlocks 1-hour
    prompt caching TTL. Non-Claude models (if any) stay on OpenRouter.
  - **Candidate-list generation:** union of 9 recall-maximizing blocking
    strategies. Lever 4 applied — B6.3 runs blocking only (no LLM) first,
    reports actual pair counts, then commits to L1 spend.
  - **TTB primary US signal:** `permittee_basic_permit` federally unique;
    pre-compute TTB fingerprint (permittee, address, brand list, COLA count)
    per producer; inject verbatim into every LLM prompt for US pairs.
  - **LWIN import as B6.2 prerequisite:** 24,762 unlinked LWIN producers
    (~69K wine rows) currently in staging. Simple matching method (exact
    normalized name + country — same as prior `lwin_long_tail.py`) per user
    decision. No AI during import. Dedup catches any import-created dupes.
  - **IDENTITY_RULES.md:** existing file (Session 2, wine identity) gets a
    new Section 11 (Producer Identity Rules) drafted by Claude in B6.3. User
    reviews before L1 Haiku runs. Rules embedded verbatim in every prompt.
  - **Schema:** extend `producer_dedup_pairs` with producer_id_a/_b,
    method_name, confidence, reasoning, cost_cents, signals/ttb_evidence/
    web_evidence jsonb, flag_reason. CREATE `producer_merge_history` for
    full JSON snapshot + repointed-rows audit (programmatic rollback).
  - **Parent-child schema:** use existing `producers.parent_producer_id`
    column. Dedicated `producer_relationships` table deferred post-S6.
  - **Quality bar = final-state correctness**, not per-method 99%. Achieved
    via unanimous-method auto-apply (~0 FPR) + user review of 50-150 curated
    toughest pairs + UNCERTAIN pairs flagged as known-open.
  - **Review model:** curated toughest pairs (disagreements, mid-confidence,
    policy edges, high-wine-count), Claude presents each with context pack
    (website, Wikipedia, LWIN, TTB, sample wines) + recommendation + evidence.
    User signs off. Target 50-150 pair review load.
  - **5 review upgrades:** prefetched context pack per pair, batched by
    pattern not random, decision log with notes, flag-for-later (approach A
    + C — FLAGGED verdict + open_questions.md pattern log), calibration
    exercise before B6.3.
  - **2 safety nets:** unblocked spot-check (~$1, B6.4) + post-execution
    leftover scan ($5-10, B6.N).
  - **7 gates:** IDENTITY_RULES review → schema migration on branch → blocking
    dry-run → TTB fingerprint spot-check → L1 pilot on 200 pairs → L2+L3
    ablation + cache hit rate → merge execution dry-run on branch.
  - **Cost levers:** Lever 1 (skip LLM on exact matches) OFF per user (risk
    of commune-overlap misses). Lever 2 (Opus pre-filter L3) OFF per user
    (undermines rigor tier). Lever 3 (batching at L1+L2) ON. Lever 4
    (blocking first see actuals) ON.

  **Budget:** $100-220 projected, $250 ceiling.

  **Block cadence:** B6.2 LWIN import (~$0) → B6.3 schema + IDENTITY_RULES +
  blocking + L1 ($60-130) → B6.4 L2 + L3 + anchor + ablation ($40-95) →
  B6.5 L4 + review ($0-10) → B6.6 execution ($0) → B6.7+ iterate reserve
  $30-60 → B6.N close.

  **Feasibility rating given:** auto-apply ~0 FPR doable with unanimous
  ensemble. 99% on 203-pair gold is stretch (best Task 1 single model
  94.5%); ensembles get there. Final-state ~100% achievable with ensemble
  + TTB signal + user review. Honest quality worry: obscure non-US
  producers with diverged names + thin wine catalogs may slip through
  blocking. Safety net B catches those post-execution.

  Tables: NONE (planning only). Files: `data/sprints/dedup/plan.md`
  (rewritten 3x), `data/sprints/dedup/journal.md`, `data/sprints/dedup/
  sessions.json`, `data/sprints/dedup/budget.json`, `data/session_prompts/
  b6_2_lwin_import.md` (new), `CLAUDE.md`, `data/dashboard.html`,
  `docs/DECISIONS.md`, `data/sessions.md`.

---

## Active

(B6.1 done. B6.2 queued — LWIN long-tail producer import via
`pipeline/promote/lwin_long_tail.py` with `--resume-unlinked` extension.)

---

## Done

- **B6.1 — Sprint 6 planning.** Plan locked at `data/sprints/dedup/plan.md`.
  Multiple rounds of design dialog (10 initial + 8 clarifying + 4 edge-case
  questions). Sprint reshaped three times in response to user input. Final
  shape: LWIN import first (B6.2), then tiered AI dedup ladder (B6.3-B6.5),
  then in-sprint execution (B6.6), iterate if needed (B6.7+). Budget ceiling
  $250. $0 spent in B6.1.
