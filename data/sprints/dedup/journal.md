# Sprint 6: Dedup (Producers) — journal

**Opened:** 2026-04-16
**Status:** Active — B6.3 L1 run in progress (background)
**Current block:** B6.3 full L1 Haiku classification running on 150,950 pairs; B6.4 queued (L2 rich + L3 web-grounded + anchor set + ablation).

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

- **B6.3 (2026-04-17):** Schema + IDENTITY_RULES §11 + blocking + L1 pilot +
  full L1 run. Six-hour execution session.

  **Part A — Schema:** Migration
  `2026-04-16_b6_3_producer_dedup_schema.sql` applied directly to production
  (additive, zero-risk; Supabase branch reserved for B6.6 merge execution
  dry-run per plan). Extended `producer_dedup_pairs` with 12 new columns
  (producer_id_a/_b, method_name, confidence, reasoning, cost_cents,
  signals/ttb_evidence/web_evidence jsonb, flag_reason, created_at,
  updated_at) + UNIQUE INDEX on (producer_id_a, producer_id_b, method_name).
  Created `producer_merge_history` table with RLS for reversible merge audit.
  Follow-up migration `b6_3_producer_dedup_pairs_id_default` added
  `producer_dedup_pairs_id_seq` sequence for id column (pre-existing integer
  PK had no default). Total schema work: two migrations, ~50 lines SQL.

  **Part B — IDENTITY_RULES §11 drafted:** Appended ~140 lines (8
  subsections) to `docs/IDENTITY_RULES.md`:
  - 11.0 Core principle: brand-on-label
  - 11.1 MERGE criterion + signal table
  - 11.2 PARENT-CHILD criterion + signal table
  - 11.3 SKIP criterion + signal table
  - 11.4 Edge cases (a-l): renames, dissolved+reopened, private labels,
    retailers-never-producers, second wines, négociant+estate, corporate
    holdcos (with 1E carve-out for label-appearing brands like E. & J. Gallo),
    accent variants, importer prefixes, TTB permits, commune overlap, JVs
  - 11.5 UNCERTAIN/FLAGGED verdict semantics + propagation
  - 11.6 Survivor selection priority (label > accent > metadata > wine count
    > LWIN > older)
  - 11.7 Verbatim LLM-embed policy preamble
  - 11.8 In-sprint rule amendment process (DECISIONS.md log)

  User reviewed all 6 judgment calls (abbreviations + no-translate, JV
  standalone, rename = label-continuous, private labels, holdcos, survivor
  priority). Approved 5 as-is; asked for 1E amendment (holdco carve-out for
  names that DO appear on consumer labels, e.g. E. & J. Gallo). Amendment
  applied to 11.4.g.

  **Part C — Blocking (`pipeline/identity/producer_blocking.py`):** 8 active
  strategies; S3 (embeddings, no producer embedding col; revisit post-L1)
  and S4 (first-3-char; too permissive at 33K scale, timed out at all
  tuning levels) dropped.

  Final strategy counts (method_name='blocking'):
  - S1 exact-norm same-country: 0 (B6.2 already caught these)
  - S2 trigram ≥0.35 same-country: 114,494 (was 247K at 0.30; tightened per plan lever 4)
  - S5 shared wine-LWIN_7: 1
  - S6 shared TTB BW permit (BW-only filter, capped 10 producers/permit): 7,595
    (was 4M unfiltered; BW-only fix excludes importer/wholesaler permits;
    per-permit cap excludes custom-crush facilities like Bronco)
  - S7 cross-country exact-OR-trigram≥0.5 OR shared LWIN: 8,227
  - S8 shared distinguishing wines ≥30% overlap (name in ≤20 producers globally): 2,018
    (was 1.78M at raw ≥30% with generic grape names; distinguishing-name filter essential)
  - S9 same-country substring containment: 11,786
  - S10 shared rare wine name (appears in ≤5 producers globally): 15,693
  - S11 cross-country word-subset containment: 3,619

  **Union total: 151,150 pairs.** Multi-strategy agreement: 11,521 at 2-sig,
  283 at 3-sig, 64 at 4-sig, 1 at 5-sig.

  Deep-think audit mid-session (after user pushback on "are we testing US
  retail producers") surfaced two structural gaps that required new
  strategies:
  1. **Cross-country exact-name dupes** missed — B6.2's country-aware
     matching created "Cupcake Vineyards" × 3 (US/IT/NZ) and "Josh" × 2
     (US/IT) as distinct rows that no prior strategy caught. Fix: broadened
     S7 from shared-LWIN-only to also include cross-country exact-name
     AND cross-country trigram ≥0.5. +8,227 pairs.
  2. **Abbreviation / initialism dupes** missed — DRC ↔ de la Romanee-Conti
     trigram=0.03, no shared BW permit, no substring relation, only 1
     shared wine. Previously 0 pairs. Fix: added S10 shared-rare-wine with
     name-frequency filter; catches via shared 'marey monge' lieu-dit.
     +15,693 pairs. Also added S11 cross-country word-subset for Mondavi
     ↔ Mondavi & Frescobaldi case. +3,619 pairs.

  **Part D — TTB spot-check:** 15 random US producers pulled from S6 pool.
  BW permits + permittee names + addresses + brand names all look clean
  (Kenneth Volk BW-CA-7040 + BW-MI-23, Emeritus BW-CA-6867, Ransom multi-
  state BW-OR-278/BW-OR-26/BW-NY-882, Ridge legacy+modern BW forms, etc.).
  One edge case flagged: foreign producers like Errazuriz (CL) and Hospices
  de Beaune (FR) have US BW permits — these are US importer/rebottler
  filings on their behalf; LLM will have enough context to correctly SKIP
  these via brand-name + country cues.

  **Part E — L1 pipeline (`pipeline/identity/producer_dedup_l1.py`):** Built
  claude-haiku-4-5 classifier with prompt caching (§11 preamble ~5.4K
  tokens marked `cache_control: ephemeral`), batched 10 pairs/call,
  concurrent workers. Static preamble: §11 verbatim + output schema + 5
  few-shot examples (Ridge MERGE, Stag's Leap SKIP, Silver Oak+Twomey
  PARENT_CHILD, Jordan US+ZA SKIP, DRC MERGE). Per-pair context: producer
  name/country/wine count/LWIN link count/top 5 wine names/TTB fingerprint
  (BW permits, permittees, addresses, brand names, cola count)/blocking
  signals fired. Output: JSON array per batch with verdict, confidence,
  reasoning per pair.

  **Pilot (200 pairs, 20 calls, $0.12, 3.3 min):**
  - Cache behavior confirmed: first call wrote cache (~6.3K tokens);
    batches 2+ read cache at 90% discount.
  - Verdict distribution: MERGE=15 (avg conf 0.92), PARENT_CHILD=21
    (0.87), SKIP=161 (0.93), UNCERTAIN=3 (0.56).
  - **Anchor accuracy: 7/7.** Ridge/Duckhorn/Caymus/DRC/Silver Oak all MERGE
    at 0.94-0.98; Stag's Leap WC vs Stags' Leap Winery SKIP 0.96 despite
    0.72 trigram; Silver Oak + Twomey PARENT_CHILD 0.93 via shared
    BW-CA-5455.
  - **Interesting find: Wakefield ↔ Taylors [AU] MERGE 0.97** — L1
    correctly identified these as the same Clare Valley producer (sold as
    Wakefield in UK/US due to Taylor's Port trademark conflict).
  - **Defensible SKIP on Cupcake Vineyards US/IT/NZ:** L1 said SKIP despite
    these being the same Gallo global brand — from L1's local evidence
    (no TTB overlap, no shared LWIN, no shared wines due to global
    sourcing), SKIP is defensible. L3 web search in B6.4 should flip to
    MERGE after verifying Cupcake is a Gallo brand.
  - Per-pair cost: 0.06¢. Extrapolated full-corpus cost: $89.79 for 151K.

  **Threshold discussion + decision deferral:** User noted avg confidences
  for non-SKIP verdicts (MERGE 0.92, PARENT_CHILD 0.87) are lower than
  SKIP (0.93), and questioned whether raising the threshold for L2
  escalation would reduce L1 overconfidence. Walked through 6 symmetric
  thresholds (0.85, 0.88, 0.90, 0.92, 0.93, 0.95); at 0.92 symmetric 35%
  escalation ≈ 53K pairs to L2 ≈ $74 L2 cost ≈ $214 sprint total (fits
  $250 ceiling); at 0.93 ≈ 50% escalation ≈ $256 (slightly over ceiling).
  User decision: **run L1 on all pairs first, pick threshold at B6.4**
  with full distribution + anchor set calibration in hand. Thresholds
  are post-processing decisions; no L1 re-run needed. User preference:
  **symmetric not asymmetric** ("do it right the first time" — willing to
  pay L2 to verify SKIPs at same threshold as MERGE/PARENT_CHILD).

  **Full L1 run launched 2026-04-17** with 8 concurrent workers, budget
  cap $130. Processing 150,950 remaining blocking pairs. ETA ~5 hours
  (throughput ~2.5 pairs/sec with 8 workers). Projected cost: $75-90.
  Post-cache-warmup per-batch cost stabilized at ~0.5¢/batch (vs 1.1¢
  for initial 8 concurrent cache-write calls). Run uses `--resume` filter
  (NOT EXISTS on producer_dedup_pairs.method_name='l1_haiku_batch'), so
  interruption + restart is idempotent.

  **Files:** `supabase/migrations/2026-04-16_b6_3_producer_dedup_schema.sql`
  (new), `supabase/migrations/b6_3_producer_dedup_pairs_id_default.sql`
  (new), `docs/IDENTITY_RULES.md` (§11 appended + 11.4.g carve-out),
  `pipeline/identity/producer_blocking.py` (new, 870 lines), `pipeline/
  identity/producer_dedup_l1.py` (new, 670 lines), `data/stats/
  b6_3_blocking_run.log`, `data/stats/b6_3_ttb_spotcheck.log`, `data/stats/
  b6_3_l1_pilot.log`, `data/stats/b6_3_l1_full.log` (running), `data/
  session_prompts/b6_4_l2_l3_anchor.md` (new).

  **Tables touched:** `producer_dedup_pairs` (151,150 'blocking' rows +
  200 'l1_haiku_batch' pilot rows + ongoing full L1 writes), `producer_
  merge_history` (created, empty). No existing data changed.

---

## Active

- **B6.3 full L1 run** (background): 150,950 pairs, 8 workers, $130 budget.
  ETA ~5 hrs. Check progress via `data/stats/b6_3_l1_full.log`. Final
  count will be in `producer_dedup_pairs WHERE method_name='l1_haiku_batch'`
  when done. Sum `cost_cents` for actual spend.

(B6.4 queued once L1 completes — see `data/session_prompts/b6_4_l2_l3_anchor.md`.)

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

- **B6.2.2 — LWIN wine recovery via display_name + corpus-wide display_name
  backfill.** Triggered by a multi-turn user push: "zero-wine producers" are
  actually real wines that LWIN models with wine_name=NULL but
  display_name populated ("Comte Senard, Meursault"). Reading the LWIN
  Guide V1.2 + auditing the source_lwin schema revealed a `display_name`
  column we had been ignoring, populated on 189,348 of 189,359 rows (Liv-ex's
  own authoritative combined-wine name, including Premier Cru / Grand Cru /
  lieu-dit info). Every LWIN-7 is a real for-sale bottled product per
  Liv-ex — Wikipedia-analog of an ISBN.

  **Design decision (logged to DECISIONS.md):** (a) fix the 26,878
  skipped rows by using display_name (producer prefix stripped) as the
  wine identifier; (b) backfill wines.display_name on all LWIN-linked
  wines from source_lwin.display_name; (c) explicitly REJECT rewriting
  wines.name on the existing 162,458 (would churn slugs, invalidate
  enrichment caches, risk uniqueness collisions, deliver zero
  user-visible gain).

  **Script changes:** added `derive_wine_identifier(display_name,
  wine_name)` helper + `--recover-missing-wines` mode to
  `pipeline/promote/lwin_long_tail.py`. Threads `display_name` through to
  `match_or_create_wine` for storage on INSERT. Resume filter uses
  `canonical_wine_id IS NULL` instead of `processed_at IS NULL` since
  the prior closeout helper populated processed_at on wine_name-NULL rows.

  **Recovery run:** 93.9 min, 10,365 producers (all matched to existing
  canonical), 26,878 rows processed. 26,616 wines created, 248 matched,
  14 failed (wine_names normalizing to empty string: `?`, `+`, `"..."`
  + 11 rows with all-NULL wine/display/sub-region fields). 26,864
  source_lwin rows linked. $0.

  **Display_name backfill:** single SQL migration
  `2026-04-16_backfill_wines_display_name_from_lwin.sql` matching
  `wines.id` ↔ `external_ids.entity_id` ↔ `source_lwin.lwin` (all 189,319
  LWIN external_ids are 7-digit; single-column JOIN avoids the planner
  blowup an OR-join caused on first attempt). Populated display_name on
  the remaining pre-B6.2 wines. Every LWIN-linked canonical wine (and
  ~40K non-LWIN wines from other sources) now has display_name.

  **Final state (verified):**
  - Canonical wines: **224,316** (up from ~197,700 pre-recovery)
  - Wines with display_name: **224,316 (100%)**
  - LWIN rows linked to canonical wines: 189,319 (99.98%)
  - LWIN rows residual: 40 (12 pathological producer_name-includes-color
    + 3 garbage wine_name + 25 NULL-producer_name assortment/Madeira
    rows — all unsuitable for the canonical schema)
  - Zero-wine producers: **11** (down from 5,372 after B6.2; the
    remaining 11 are NULL-producer-name LWIN meta-entities)
  - `search_catalog('leroy nuits-saint-georges')` returns the plain
    village Nuits-Saint-Georges by Leroy as rank #1, followed by lieu-dit
    and Premier Cru variants — authoritative display names preserved.

  **Marquee wines recovered:** Leroy / Nuits-Saint-Georges (village);
  William Fèvre / Chablis; Louis Jadot / Nuits-Saint-Georges +
  Gevrey-Chambertin + Chassagne-Montrachet; Olivier Leflaive /
  Puligny-Montrachet + Chassagne-Montrachet; Dominique Laurent /
  Nuits-Saint-Georges; Henri Gouges / Nuits-Saint-Georges; Antinori / Vin
  Santo del Chianti; Dujac Fils et Pere / Clos de la Roche Grand Cru;
  Pierre Morey / Montrachet Grand Cru; and ~26,600 more.

  **Meta-lesson logged to CLAUDE.md + DECISIONS.md:** the multi-turn
  back-and-forth ("zero-wine producers as residuals" → "fallback chain" →
  "display_name in the schema") was entirely avoidable by doing a proper
  web search of the LWIN data dictionary + a `SELECT column_name FROM
  information_schema.columns WHERE table_name='source_lwin'` up front.
  New Behavioral Instruction: Check Assumptions with Web Search — Often,
  and Early. Triggers: any claim about an external system's data shape,
  any invented fallback rule when the upstream source may already ship
  the answer, any "I don't think X supports Y" before actually looking.
  Overuse is cheaper than underuse.

  Tables: `wines` (+26,616 rows, 100% display_name coverage), `source_lwin`
  (26,864 rows linked), `external_ids` (+26,864 lwin rows). Files:
  `pipeline/promote/lwin_long_tail.py` (extended with --recover-missing-wines +
  derive_wine_identifier), `supabase/migrations/2026-04-16_backfill_wines_display_name_from_lwin.sql`
  (new), `data/stats/lwin_recover_log.txt` (progress log), `CLAUDE.md` (new
  Check Assumptions with Web Search section), `docs/DECISIONS.md` (3 new
  entries: web-search behavioral rule, wine-recovery scope decision,
  display_name-as-source-of-truth decision).

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
