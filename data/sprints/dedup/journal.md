# Sprint 6: Dedup (Producers) — journal

**Opened:** 2026-04-16
**Status:** Active — LWIN import done, dedup evaluation next
**Current block:** B6.2 complete; B6.3 next (schema + IDENTITY_RULES Section 11 + blocking + L1)

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

- **B6.2 (2026-04-16):** LWIN long-tail producer import. Added
  `--resume-unlinked` flag + NULL-country handling to
  `pipeline/promote/lwin_long_tail.py` (same simple matching as Session 13:
  exact normalized name + country; junk filter stays disabled per B6.1).
  Dry-run sample of ~164 producers looked clean, ran full execute.

  **Script run (2h 51m, $0):** processed 24,762 producers, 69,444 source_lwin
  rows. 2,164 producers matched, 22,598 created, 0 failed. Wines: 471 matched,
  42,095 created, 3 failed (wine_names that normalize to empty string — `?`,
  `+`, `"..."`; acceptable LWIN garbage). 42,566 LWIN external_ids upserted,
  42,566 source_lwin rows marked processed.

  **Post-run gap discovered:** 26,878 source_lwin rows whose producer is now
  canonical still had `canonical_producer_id IS NULL`. Root cause: the main
  script skips rows where `wine_name` is NULL/empty (all 26,875 of 26,878),
  which leaves `source_lwin` untouched for those rows. Built
  `pipeline/promote/lwin_backfill_producer_id.py` as a closeout helper —
  builds an in-memory index of `(name_normalized, country_id) → producer_id`,
  looks up each skipped row, bulk-updates `canonical_producer_id +
  processed_at`. Dry-run: 26,878 matched / 0 unresolvable. Execute: 26,878
  source_lwin rows backfilled, $0, 15 seconds.

  **Final state:**
  - Producers: **10,683 → 33,281 total (33,225 active)** — inside the 20-40K
    expected range. +22,598 new canonical producers from LWIN long tail.
  - source_lwin: 0 distinct unlinked producer_names (down from 24,762). 26
    rows still NULL canonical_producer_id, all of them `producer_name IS
    NULL` (pre-existing LWIN garbage, can't be linked — well under the <100
    residual threshold).
  - 157,346 canonical wines now carry an LWIN external_id; 99.86% of active
    producers (33,177 of 33,225) have at least one source_lwin row pointing
    at them.

  **Acceptance gate passed cleanly:** 0 unlinked producers, 33K in range, 10
  newly-created rows spot-checked sane (Zurab Topuridze, Zunino Basilio, zum
  Sternen, Zuccotti, Zorzettig, Zohlhof, Zohar, Zironda, Zio Porco,
  Zimmermann-Graeff), no duplicate `(name_normalized, country_id)` pairs
  among producers created today, 0 Python exceptions.

  **Notes for B6.3 dedup:** simple matching is known-inclusive — some
  spelling variants created new rows when a canonical fuzzy-match already
  existed. Examples surfaced during dry-run: US `'Ridge'` created despite
  existing `'Ridge Vineyards'`; `'Cape Winemakers Guild (Bruwer Raats)'`
  created despite existing Bruwer Raats etc. Dedup (B6.3-B6.6) catches
  these. No attempt to clean them up at import time per B6.1 directive.

  Tables: `producers` (+22,598 rows), `source_lwin` (69,444 rows updated),
  `external_ids` (42,566 lwin_7 rows upserted), `wines` (42,095 rows
  created). Files: `pipeline/promote/lwin_long_tail.py` (extended),
  `pipeline/promote/lwin_backfill_producer_id.py` (new), `data/stats/
  lwin_b6_2_log.txt` (progress log).

---

## Active

(B6.2 done. B6.3 queued — schema migration + IDENTITY_RULES Section 11 +
blocking dry-run + L1 Haiku batched on all definitive-list pairs.)

---

## Done

- **B6.1 — Sprint 6 planning.** Plan locked at `data/sprints/dedup/plan.md`.
  Multiple rounds of design dialog (10 initial + 8 clarifying + 4 edge-case
  questions). Sprint reshaped three times in response to user input. Final
  shape: LWIN import first (B6.2), then tiered AI dedup ladder (B6.3-B6.5),
  then in-sprint execution (B6.6), iterate if needed (B6.7+). Budget ceiling
  $250. $0 spent in B6.1.

- **B6.2 — LWIN long-tail producer import.** Main sweep (2h 51m, $0) + tail
  backfill (~15s, $0). Producers 10,683 → 33,281; 0 unlinked LWIN producers
  remain. Plus: caught main-script edge case where wine_name-NULL rows left
  canonical_producer_id empty, built
  `pipeline/promote/lwin_backfill_producer_id.py` as closeout, backfilled
  26,878 rows. Dedup universe ready for B6.3.

- **B6.2.1 — Pre-B6.3 hygiene (search_vector trigger fix + Opus 4.7 review).**
  Triggered by user asking "is new LWIN data in the same spot as existing data
  before dedup?" + "revisit anything with 4.7 upgrade?" Audited 5 search-vector
  tables; found pre-existing INSERT-trigger bug on producers + wines (functions
  self-queried NEW.id, got zero rows on BEFORE INSERT, NULLed the vector).
  22,598 + 42,095 B6.2 rows had NULL search_vectors; pre-B6.2 rows didn't
  because prior pipelines UPDATEd after INSERT. Grapes / appellations /
  regions triggers already correct. Migration
  `supabase/migrations/2026-04-16_fix_producer_wine_search_vector_trigger.sql`
  rewrites both trigger functions to compute inline + backfills. Applied;
  verified 0 NULLs across all 5 tables; probe-insert + search_catalog smoke
  test pass. Also noted for B6.3 to handle (not fix pre-dedup): 7 NULL-country
  meta-entities (Sotheby's, LVMH, partnerships) to flag/soft-delete in
  review; 5,372 zero-wine LWIN producers (24% of new cohort) to treat
  normally with weak signal. Logged 2 DECISIONS.md entries (Opus 4.7
  carry-forwards, trigger-bug root cause). Updated `docs/SCHEMA.md` §16 with
  search infrastructure documentation including 2026-04-16 patch note. $0.
