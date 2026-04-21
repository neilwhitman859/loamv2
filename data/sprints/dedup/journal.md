# Sprint 6: Dedup (Producers) — journal

**Opened:** 2026-04-16
**Status:** Active — merge-only Codex rebuild; the best surviving artifact is still the fallback-safe Session 9.7 layered control, and production readiness remains unproven under the frozen gates.
**Current block:** Session 9.11 completed the capped full 152-case rerun of the three broader proof survivors. All three reopened false merges at full scale, so queue-building remains blocked. The evidence still points to the Session 9.7 fallback artifact as the best surviving non-production result, but the next session is now a high-level viability review before any freeze / closeout.

**Status addendum (2026-04-21):** Session 9.7 now supplies a layered fallback
contender that clears the fallback gate but still fails the frozen production
gate.
**Current block addendum:** Session 9.11 completed the full rerun at `$0.00`
new model spend. `merge_proposer_plus_veto_v1`,
`expanded_layered_router_v1`, and `evidence_digest_then_judge_v1` all improved
recall versus the Session 9.7 fallback control but reopened `5-9` false merges,
so none cleared the frozen production or fallback gates. The best surviving
artifact remains `session9_7_layered_safety_sonnet_r2_narrow`, but the user has
redirected the next session toward a higher-level viability discussion instead
of an automatic freeze / closeout memo.
**Strategic addendum:** the key next question is no longer just which artifact
to freeze. It is whether Loam should continue at all if producer dedup cannot
be made trustworthy enough for production.

---

## Block log

- **Session 9.11 (2026-04-21):** Ran the full 152-case rerun for the three
  broader method survivors that cleared the Session 9.10 proof subset.

  **Scope held:** kept `benchmark_v1`, the frozen Session 4 gates, and the
  Session 9.7 layered safety base fixed; used only the three Session 9.10
  survivors; did not queue-build; did not mutate the benchmark; and did not
  open any new contender family beyond the proof survivors.

  **Implementation:**
  - Added `pipeline/identity/bakeoff_method_bakeoff_full.py`.
  - Tightened `pipeline/identity/bakeoff_method_bakeoff_proof.py` so
    method-promoted `MERGE` rows carry the packet-recommended survivor id
    instead of being penalized as null-survivor merges at score time.
  - Added the top-level rerun memo
    `data/sprints/dedup/session9_11_full_method_bakeoff_rerun_if_approved.md`.
  - Added scored full-rerun artifacts
    `data/sprints/dedup/bakeoff_v2/scored/session9_11_full_method_bakeoff_rerun_if_approved.json`,
    `.md`, and `_manifest.json`.
  - Logged the user's rerun approval in `docs/DECISIONS.md`.

  **Full-rerun result vs Session 9.7 control:**
  - frozen control `layered_safety_sonnet_r2_narrow_v1` remains
    `0` false merges, `6` hard misses, `3` soft misses, `0.1316` flag rate,
    fallback gate `pass`
  - `merge_proposer_plus_veto_v1`: `5` recoveries, but `9` false merges
    (`6` blind-core / `1` known-false-merge / `2` tail), fallback gate `fail`
  - `expanded_layered_router_v1`: `4` recoveries, but `5` false merges
    (`3` blind-core / `0` known-false-merge / `2` tail), fallback gate `fail`
  - `evidence_digest_then_judge_v1`: `3` recoveries, but `5` false merges
    (`4` blind-core / `1` known-false-merge / `0` tail), fallback gate `fail`

  **Interpretation:** the broader method-class idea was real enough to improve
  recall, but not strong enough to survive the full benchmark honestly. The
  full rerun did not displace the Session 9.7 layered fallback control as the
  best surviving artifact, and it did not unlock queue-building.

  **Spend:** `$0.00` incremental external API cost. Sprint 6 spend remains
  `$319.51` against the `$450` ceiling.

  **Recommendation after Session 9.11:** hold a high-level viability /
  next-steps discussion before any freeze-closeout memo. Interim operational
  stance: keep `session9_7_layered_safety_sonnet_r2_narrow` as the best
  surviving non-production artifact, do not queue-build, and only consider
  further continuation if that discussion identifies a credible path to
  trustworthy producer dedup.

- **Session 9.10 (2026-04-21):** Implemented and ran the proof-first broader
  method comparison from Session 9.9 on a bounded trap-heavy subset before any
  full rerun.

  **Scope held:** kept `benchmark_v1`, the frozen Session 4 gates, and the
  Session 9.7 layered safety base fixed; did not queue-build; did not mutate
  the benchmark; and did not run the full 152-case rerun inside the proof
  session.

  **Implementation:**
  - Added `pipeline/identity/bakeoff_method_bakeoff_proof.py`.
  - Added the top-level proof memo
    `data/sprints/dedup/session9_10_method_bakeoff_proof_subset.md`.
  - Added scored proof artifacts
    `data/sprints/dedup/bakeoff_v2/scored/session9_10_method_bakeoff_proof_subset.json`,
    `.md`, and `_manifest.json`.
  - Logged the proof-first execution approval in `docs/DECISIONS.md`.

  **Proof subset composition:** `29` cases = `9` Session 9.7 residual misses +
  `5` Session 9.6 false merges + `5` Session 9.8 adjacent skip controls + `5`
  expanded-family negatives + `5` hold-set current wins.

  **Implemented contenders:** `expanded_layered_router_v1`,
  `signature_router_v1`, `merge_proposer_plus_veto_v1`, and
  `evidence_digest_then_judge_v1`.

  **Proof result vs control:**
  - frozen Session 9.7 control on this slice = `0` false merges, `9` missed
    merges
  - `merge_proposer_plus_veto_v1`: `5` recoveries, `4` blind-core blocker
    recoveries, `0` false merges, `0` lost current wins
  - `expanded_layered_router_v1`: `4` recoveries, `3` blind-core blocker
    recoveries, `0` false merges, `0` lost current wins
  - `signature_router_v1`: exact same decision vector as
    `expanded_layered_router_v1`; dropped as redundant
  - `evidence_digest_then_judge_v1`: `3` recoveries, `3` blind-core blocker
    recoveries, `0` false merges, `0` lost current wins

  **Downselect:** any later full rerun should keep only
  `merge_proposer_plus_veto_v1`, `expanded_layered_router_v1`, and
  `evidence_digest_then_judge_v1`.

  **Interpretation:** Session 9.9's broader method-class continuation is now
  evidence-backed enough to justify one capped full rerun. Queue-building is
  still blocked, but freezing immediately at the Session 9.7 fallback endpoint
  is no longer the best-supported next move.

  **Spend:** `$0.00` incremental external API cost. Sprint 6 spend remains
  `$319.51` against the `$450` ceiling.

  **Recommendation after Session 9.10:** run the full 152-case rerun on only
  the three downselected survivors. If they all fail the frozen production
  gate, freeze at the best surviving non-production artifact rather than
  opening another redesign inside the same session.

- **Session 9.9 (2026-04-21):** Converted the Session 9.8 freeze finding into a
  broader, budget-bounded method-bakeoff design after the user explicitly kept
  Sprint 6 aimed at production readiness.

  **Scope held:** kept `benchmark_v1`, the frozen Session 4 gates, and the
  Session 9.7 layered safety base fixed; did not queue-build; did not mutate
  the benchmark; and did not run another proof inside the design session.

  **Implementation:**
  - Added the design memo
    `data/sprints/dedup/session9_9_method_bakeoff_design.md`.
  - Added the follow-on proof prompt
    `data/session_prompts/s9_10_method_bakeoff_proof_if_approved.md`.
  - Logged the continuation decision in `docs/DECISIONS.md`.

  **Design result:**
  - the honest continuation is now a **method-class bakeoff**, not another
    plain model rerun and not another narrow rescue
  - the fixed control remains
    `session9_7_layered_safety_sonnet_r2_narrow`
  - the recommended contender classes are:
    `expanded_layered_router_v1`, `signature_router_v1`,
    `merge_proposer_plus_veto_v1`, and `evidence_digest_then_judge_v1`
  - the next execution step is a proof subset built from the `9` remaining
    misses, the `5` Session 9.6 false merges, signature-adjacent skip controls,
    and a small hold set of Session 9.7 recall wins

  **Budget posture:**
  - Session 9.10 proof subset target: `$0-5`, hard cap `$8`
  - any later full 152-case rerun only if proof survivors exist
  - combined continuation target: `$3-15`, hard cap `$25`

  **Interpretation:** Session 9.8 still stands: there is no honest narrow fix.
  But because the user explicitly rejected fallback-only closure as the sprint
  goal, Sprint 6 now has one credible continuation left: compare a small number
  of materially different adjudication methods under the frozen benchmark and
  gates.

  **Spend:** `$0.00` incremental external API cost. Sprint 6 spend remains
  `$319.51` against the `$450` ceiling.

  **Recommendation after Session 9.9:** implement the method contenders and run
  the proof subset. If no contender survives cleanly, freeze at
  `session9_7_layered_safety_sonnet_r2_narrow`.

- **Session 9.8 (2026-04-21):** Audited the remaining misses in the Session 9.7
  layered fallback and tested whether one more narrow continuation was still
  honest.

  **Scope held:** kept `benchmark_v1`, the frozen Session 4 gates, and the new
  zero-false-merge safety structure fixed; did not queue-build; did not touch
  all-pairs execution; and did not run another proof unless the continuation
  stayed genuinely narrow.

  **Implementation:**
  - Added the audit memo
    `data/sprints/dedup/session9_8_recover_production_from_layered_fallback.md`.
  - Added the conditional follow-on prompt
    `data/session_prompts/s9_9_broader_redesign_if_not_freezing.md`.

  **Audit result:**
  - remaining disagreements: `9`, all missed merges (`6` hard, `3` soft)
  - production still needs at least `5` additional safe recoveries to clear the
    frozen gate
  - the remaining miss set spans `6` packet-ref signatures
  - `4` of the `9` misses sit outside the current routed-family specialist
    bundle altogether
  - the layered Session 9.7 run did not lose any recall versus Session 9.6; it
    only removed the `5` false merges

  **Why no proof ran:** a continuation confined to the current routed families
  could not recover production readiness even in the best case, because the
  still-blocking blind-core misses include `11.1`, `11.4.g`, and `11.4.b`
  cases outside the routed bundle. The repeat signatures that do exist are also
  entangled with benchmark skip controls and prior false-merge traps, so the
  next proof large enough to matter would already be a broader redesign rather
  than one more narrow recall-only continuation.

  **Interpretation:** Session 9.8 did not find another honest bounded proof.
  The current adjudication path remains useful as a fallback-only artifact, but
  the correct freeze point is now the stronger Session 9.7 layered fallback
  endpoint, not the weaker Session 9.6 specialist-only result.

  **Spend:** `$0.00` incremental external API cost. Sprint 6 spend remains
  `$319.51` against the `$450` ceiling.

  **Recommendation after Session 9.8:** freeze the current adjudication path at
  `session9_7_layered_safety_sonnet_r2_narrow`. Only reopen Sprint 6 if the
  user explicitly wants a broader multi-family redesign rather than one more
  narrow continuation.

- **Session 9.7 (2026-04-21):** Broadened the redesign beyond the failed
  Session 9.6 specialist proof and tested a layered safety-gate architecture.

  **Scope held:** kept `benchmark_v1` and the frozen Session 4 gates unchanged,
  did not queue-build, did not touch all-pairs execution, and stayed inside a
  bounded proof-first continuation. The broader redesign was limited to layered
  routing logic on top of the existing Session 9.6 specialist artifact.

  **Implementation:**
  - Added `pipeline/identity/bakeoff_layered_safety_gate.py`, a reusable proof
    runner that starts from the Session 9.6 routed-specialist outputs, applies
    deterministic anti-trap vetoes, optionally runs a skeptical review stage on
    a chosen subset of routed `MERGE` proposals, reverts vetoed proposals back
    to the safe Session 9.3 Gemini base path, and scores the composite result
    against the frozen 152-case benchmark.
  - Added the Session 9.7 closeout memo
    `data/sprints/dedup/session9_7_layered_safety_redesign.md`.
  - Added follow-on prompt
    `data/session_prompts/s9_8_recover_production_from_layered_fallback.md`.

  **Rounds run:**
  - `session9_7_layered_safety_det_only`
    - deterministic vetoes only
    - false merges `5 -> 2`
    - blind-core missed merges unchanged at `5`
  - `session9_7_layered_safety_gpt5mini_r1`
    - invalid transport round
    - OpenRouter returned provider-routing `404` failures on all review calls
    - kept only as a failure artifact, not model evidence
  - `session9_7_layered_safety_sonnet_r1`
    - broad Sonnet skeptical review over all surviving `11.4.f` specialist
      merges
    - removed false merges but over-vetoed true merges
  - `session9_7_layered_safety_sonnet_r2_narrow`
    - Sonnet skeptical review only on the suspicious one-anchor `11.4.f`
      continuity traps
    - best Session 9.7 result

  **Best result (canonical Session 9.7 endpoint):**
  - run: `session9_7_layered_safety_sonnet_r2_narrow`
  - false merges overall: `0`
  - blind-core false merges: `0`
  - blind-core missed merges: `5`
  - routed merge recoveries: `42 / 47`
  - full-benchmark flag rate: `0.1316`
  - exact verdict accuracy: `0.8289`
  - fallback gate: `pass`
  - production gate: `fail`

  **Interpretation:** the broader redesign succeeded in finding a better
  architecture than the raw Session 9.6 specialist composite. The winning shape
  is: keep the routed specialist for recall, keep deterministic anti-trap
  vetoes for the obvious false merges, and use skeptical review only as a
  narrow scalpel on the remaining one-anchor `11.4.f` continuity traps. That
  combination eliminated the Session 9.6 false merges without giving back the
  recovered recall, but it still did not clear the frozen production gate.
  Queue-building remains blocked. The path is now meaningful as a fallback-only
  contender rather than a dead-end artifact.

  **Spend:** `$0.44` incremental external API cost across the two substantive
  Sonnet review rounds (`$0.357219` + `$0.078549`, rounded in user-facing docs
  to `$0.44`). Sprint 6 spend is now `$319.51` against the `$450` ceiling.

  **Recommendation after Session 9.7:** if Sprint 6 continues, continue from
  the Session 9.7 layered fallback state rather than the weaker Session 9.6
  specialist-only state. The next narrow target is recall, not safety: recover
  the remaining blind-core / hard missed merges without undoing the new
  zero-false-merge behavior. If the user does not want another continuation
  session, freeze at the Session 9.7 fallback state.

- **Session 9.6 (2026-04-21):** Ran the bounded routed pattern-specialist proof
  from `data/session_prompts/s9_6_pattern_specialist_proof_if_approved.md`.

  **Scope held:** kept `gemini_guardrailed_v2` as the conservative base path,
  routed only the approved `73` benchmark cases in `11.4.h`, `11.4.f`,
  `11.4.n`, and `11.4.p`, kept the remaining `79` benchmark cases on the
  frozen Session 9.3 base path, did not touch `benchmark_v1`, did not change
  the frozen Session 4 gates, did not queue-build, and did not drift into
  all-pairs work.

  **Implementation:**
  - Added `pipeline/identity/bakeoff_pattern_specialist_proof.py`, a proof-only
    helper that reuses the frozen v2.1 packets, builds routed request files for
    the four approved families, runs family-specific Gemini specialist prompts
    only on those 73 cases, normalizes the routed rows without the generic
    sparse-official merge veto, and scores the composite 152-case result
    through the frozen Session 4 scorer.
  - Wrote durable request, raw, normalized, scored, memo, and manifest
    artifacts under `data/sprints/dedup/bakeoff_v2/` for canonical run
    `session9_6_pattern_specialist_proof_if_approved`.

  **Canonical outputs written:**
  - `data/sprints/dedup/bakeoff_v2/scored/session9_6_pattern_specialist_proof_if_approved.json`
  - `data/sprints/dedup/bakeoff_v2/scored/session9_6_pattern_specialist_proof_if_approved.md`
  - `data/sprints/dedup/bakeoff_v2/scored/session9_6_pattern_specialist_proof_if_approved_memo.md`
  - `data/sprints/dedup/bakeoff_v2/scored/session9_6_pattern_specialist_proof_if_approved_manifest.json`
  - routed raw / normalized family files plus the composite normalized rows
    under `data/sprints/dedup/bakeoff_v2/raw/` and `.../normalized/`

  **Headline result:** the family-routed proof was materially different and
  dramatically improved recall, but it still failed the frozen bar because it
  reintroduced `5` false merges overall.

  **Composite scorecard:**
  - false merges overall: `5`
  - blind-core false merges: `0`
  - blind-core missed merges: `5`
  - targeted routed merge recoveries: `42 / 47`
  - full-benchmark flag rate: `0.1053`
  - exact verdict accuracy: `0.8224`

  **Family breakdown:**
  - `11.4.h`: `24 / 28` merges recovered, but `3` false merges
  - `11.4.f`: `7 / 8` merges recovered, but `2` false merges
  - `11.4.n`: `7 / 7` merges recovered, `0` false merges
  - `11.4.p`: `4 / 4` merges recovered, `0` false merges

  **Interpretation:** the routed-specialist idea did solve the recall wall.
  It recovered `42` of the `47` targeted merges, drove blind-core missed merges
  down from `30` to `5`, and cut the full-benchmark flag rate from `0.3882` to
  `0.1053`. But the same orthographic / generational families that needed the
  recall lift also reopened the benchmark's protected false-merge traps, with
  `5` wrong merges (`4` in known-false-merge patterns plus `1` tail false
  merge). Under the frozen benchmark and gates, that means the bounded
  continuation has now been honestly tested and has failed.

  **Spend:** `$0.12` incremental external API cost for the full routed proof,
  comfortably inside the user's `$20` exploration cap. Sprint 6 spend is now
  `$319.07` against the `$450` ceiling.

  **Recommendation after Session 9.6:** freeze the current adjudication path as
  a non-execution-ready benchmark artifact unless the user explicitly authorizes
  a broader redesign beyond the bounded specialist proof.

- **Session 6 (2026-04-20):** First real adjudication bakeoff completed through the
  frozen Session 5 packet/request/normalize/score path.

  **Scope held:** existing `pipeline/identity/bakeoff_packet_v1.py` +
  `pipeline/identity/bakeoff_harness_v1.py` plumbing retained; no contender-set
  changes, no benchmark/spec/score-math changes, no queue build, no merge SQL,
  no `PARENT_CHILD` expansion.

  **Runner/build work:**
  - Added `pipeline/identity/bakeoff_run_v1.py` to orchestrate the canonical run
    name `session6_first_real_bakeoff_v1`.
  - Added real contender runners/adapters for `haiku_single_v1`,
    `gemini_single_v1`, `gpt5mini_single_v1`, `haiku_gemini_consensus_v1`, and
    `sonnet_single_v1`.
  - Extended `pipeline/identity/bakeoff_harness_v1.py` normalization to
    preserve `runtime_error:*` transport/model failures as auditable fail-closed
    `FLAGGED` rows instead of generic invalid payloads.

  **Availability guardrail:** preflight verified all frozen model IDs before the
  run and stopped on failure in code, but all were available in practice:
  `claude-haiku-4-5-20251001`, `google/gemini-3-flash-preview`,
  `openai/gpt-5.4-mini`, and `claude-sonnet-4-5-20250929`.

  **Canonical outputs written:**
  - requests, raw, normalized, and scored artifacts grouped under
    `data/sprints/dedup/bakeoff_v1/` for run `session6_first_real_bakeoff_v1`
  - `session6_first_real_bakeoff_v1.md`
  - `session6_first_real_bakeoff_v1.json`
  - `session6_first_real_bakeoff_v1_manifest.json`
  - `session6_first_real_bakeoff_v1_error_ledger.jsonl`

  **Run integrity:**
  - 152/152 request rows per contender
  - 152/152 raw rows per contender
  - 152/152 normalized rows per contender
  - hidden overlay leaks: 0
  - no silent model substitution

  **Headline result:** no frozen contender passed the production gate or the
  fallback gate. Queue-building is therefore still blocked.

  **Score summary:**
  - `haiku_single_v1`: exact acc 22.4%, false merge 0, hard missed 5,
    soft missed 44, safe flag 69, auditability 0.2632
  - `gemini_single_v1`: exact acc 59.2%, false merge 10, hard missed 4,
    soft missed 13, safe flag 35, auditability 0.7105
  - `gpt5mini_single_v1`: exact acc 57.9%, false merge 14, hard missed 16,
    soft missed 7, safe flag 27, auditability 0.8026
  - `haiku_gemini_consensus_v1`: exact acc 20.4%, false merge 0,
    hard missed 0, soft missed 49, safe flag 72, auditability 0.2697
  - `sonnet_single_v1`: exact acc 73.7%, false merge 11, hard missed 9,
    soft missed 10, safe flag 10, auditability 0.8882

  **Interpretation:** Sonnet led on accuracy but still failed on false merges
  plus auditability/schema-validity. Gemini and GPT-5.4-mini were more accurate
  than Haiku but still false-merged enough to fail. Haiku and the consensus path
  were conservative but generated too much `FLAGGED` burden and too many missed
  merges. The v1 packet plus contender lineup is therefore informative but not
  execution-ready.

  **Spend:** $2.89 total for the real bakeoff run (Haiku $0.33, Gemini $0.22,
  GPT-5.4-mini $0.53, Haiku+Gemini consensus $0.56 combined child cost,
  Sonnet $1.24). Sprint 6 spend now $313.37 against the current $450 ceiling.

  **Files:** `pipeline/identity/bakeoff_run_v1.py`,
  `pipeline/identity/bakeoff_harness_v1.py`,
  `data/sprints/dedup/bakeoff_v1/raw/session6_first_real_bakeoff_v1/`,
  `data/sprints/dedup/bakeoff_v1/normalized/session6_first_real_bakeoff_v1/`,
  `data/sprints/dedup/bakeoff_v1/scored/session6_first_real_bakeoff_v1.{md,json}`,
  `data/sprints/dedup/bakeoff_v1/scored/session6_first_real_bakeoff_v1_manifest.json`,
  `data/sprints/dedup/bakeoff_v1/scored/session6_first_real_bakeoff_v1_error_ledger.jsonl`.

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

  **Full L1 run COMPLETE 2026-04-17:** 8 concurrent workers, runtime
  ~5h10m, **$78.32 spent** (vs $75-90 projected — right in target range).
  Processed **151,120 of 151,150 pairs (99.98%)** — 30 parse failures
  acceptable at this scale. Per-pair cost 0.052¢ (post-cache-warmup was
  stable at 0.5¢/batch). Cache performance confirmed — initial 8
  concurrent cache-writes, then cache_read dominated (90% discount on
  5.4K-token §11 preamble per call).

  **Final L1 verdict distribution:**
  - MERGE: **2,606** (1.72%, avg conf 0.88)
  - PARENT_CHILD: **2,121** (1.40%, avg conf 0.86)
  - SKIP: **145,310** (96.16%, avg conf 0.95)
  - UNCERTAIN: **1,083** (0.72%, avg conf 0.56)
  - **Total MERGE+PC candidates for ladder: 4,727**

  **Multi-strategy blocking agreement (gold signal quality):**
  - 1 strategy: 139,254 pairs — mostly SKIP (98% are S2 trigram-only, expected)
  - 2 strategies: 11,518 pairs — MERGE 1,517, PC 671, SKIP 8,787
  - 3 strategies: 283 pairs — MERGE 175, PC 49, SKIP 43 (overwhelmingly non-SKIP)
  - 4 strategies: 64 pairs — MERGE 55, PC 4, SKIP 4 (near-certain)
  - 5 strategies: 1 pair — MERGE (definitively)

  **Scenario E routing preview (user's chosen thresholds, as default):**
  - L1 auto-accept ≥0.97: **29,301 pairs (19.4%)** — mostly high-conf SKIPs
  - L1.5 Gemini cross-check 0.92-0.97: **101,018 pairs (66.8%)**
  - Direct to L2 <0.92 OR UNCERTAIN: **20,801 pairs (13.8%)**

  These volumes match B6.4 planning; budget projections hold.

  **Threshold discussion mid-session:** User initially proposed symmetric
  0.92-0.93 for L2 escalation. Walked through 6 threshold scenarios with
  real distribution data. User refined to 0.96/0.90 for L2 (ceiling/floor)
  with principled reasoning: "<0.90 after rich L2 context = knowledge gap
  → skip L2.5, go direct to L3 web search". Extended ladder to include
  L2.5 Gemini rich-prompt mirroring L1.5's cross-model pattern.
  **Final decision: thresholds are DEFAULTS, not commitments.**
  Calibration in B6.4 will tune them from measured accuracy-per-confidence
  curves using L3-oracle labels (NOT user hand-labeling — user not a
  wine expert for obscure producers).

  **Vendor-neutrality audit triggered by user:** User questioned whether
  Haiku choice was biased toward Anthropic products. Pulled Task 1 bakeoff
  data; Haiku 93.5% accuracy tied with gpt-5.4-mini but Haiku's
  confidence correlation (0.53) much better than gpt-5.4-mini (0.21).
  Calibration advantage matters for threshold-based escalation but isn't
  dispositive. Decision: keep Haiku for L1 (already 18% in, $30-60
  potential savings not worth mid-run switch), broaden vendor
  consideration at L2/L3 via B6.4 bake-off.

  **Cross-model architecture (user-designed):** L1.5 Gemini and L2.5
  Gemini as complementary verification tiers. Logic validated by Task 1
  bake-off showing Haiku and Gemini have inverse error profiles
  (Haiku: low FPR 0.7%, high FNR 21% — cautious; Gemini: FPR 9%, FNR 3.5%
  — aggressive). Joint agreement collapses: FPR ~0.06%, FNR ~0.7%.
  Extra L3 routing rule: L1.5 disagreements where Gemini said MERGE but
  Haiku said SKIP at high confidence → go directly to L3 (FNR candidates
  needing web grounding).

  **B6.4 plan updated** via `data/session_prompts/b6_4_l2_l3_anchor.md`:
  - Phase A: build 500-700 pair synthetic ground truth (L3 oracle, $4-10)
  - Phase B-C: L1 + L1.5 calibration analysis
  - Phase D-F: L2 + L2.5 runs on full escalated set
  - Phase G: L3 Sonnet + web + ablation on web-grounding value
  - Phase H: cross-method agreement matrix
  - Phase I: Safety Net A
  - Phase J: final threshold commitment + held-out validation
  Projected B6.4 cost: $80-95. Sprint total $158-173, $77-92 headroom.

  **L1 run stable throughout:** 0 errors beyond the 30 parse failures
  (0.02% failure rate). Run uses `--resume` filter (NOT EXISTS on
  producer_dedup_pairs.method_name='l1_haiku_batch') so restart is
  idempotent if needed.

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

(B6.5a PARTIAL — stopped after L2+L2.5+Stage 2 routing + $16 analysis probes per user reassessment directive. Full analysis report + 3 recommendation paths at `data/sprints/dedup/b6_5a_stage2_analysis.md`. Awaiting user decision between Path 1 (pragmatic $218), Path 2 (balanced $313, my rec), Path 3 (rigor $451) before resuming L3 + execution.)

---

- **B6.4 (2026-04-17):** Calibration only — built 600-pair calibration set,
  gold-labeled 367 pairs (200 proxy + 167 Sonnet 4.6 + web_search_20250305
  oracle before hitting $30 budget cap at pair 167), ran 4 classifier tiers
  (L1 already done in B6.3, plus L1.5 Gemini basic, L2 Haiku rich, L2.5
  Gemini rich), measured accuracy against gold, committed thresholds.

  **Headline finding:** any Haiku-tier + any Gemini-tier both saying MERGE
  at conf ≥0.85 is 100% precision across 70-78 gold-labeled pairs per
  pairing. Cross-family agreement is the reliability anchor, not confidence
  magnitude.

  **Per-tier calibration results:**
  - L1 Haiku: MERGE 98.7% (75/76), PC 6.7% (5/75), SKIP@≥0.97 = 100% (100/100)
  - L1.5 Gemini basic (595 pairs, $0.13): MERGE 100% at any conf ≥0.85 (88/88), PC 9.2%, SKIP@≥0.97 = 96.2% (76/79)
  - L2 Haiku rich (600 pairs, $0.59): MERGE 97.6% (82/84), MERGE@≥0.92 = 100% (70/70), PC 9.9%
  - L2.5 Gemini rich (600 pairs, $0.19): MERGE 98.8% (83/84), MERGE@≥0.97 = 100% (42/42), PC 9.8%

  **Cross-family agreement (MERGE precision when both agree):**
  - L1 Haiku × L1.5 Gemini basic: 70/70 = 100%
  - L1 Haiku × L2 Haiku rich: 75/76 = 98.7% (1 FP — same-family)
  - L1 Haiku × L2.5 Gemini rich: 73/73 = 100%
  - L1.5 Gemini × L2 Haiku rich: 76/76 = 100%
  - L1.5 Gemini × L2.5 Gemini rich: 78/78 = 100% (same family, no FP)
  - L2 Haiku × L2.5 Gemini rich: 77/78 = 98.7% (1 FP — same-family blip)

  **4-way unanimous:**
  - All 4 MERGE (min conf ≥0.85): 97 pairs, 63/63 gold-labeled = **100%**
  - All 4 SKIP (min conf ≥0.90): 235 pairs, 151/156 = 96.8% (5 FN MERGEs)
  - All 4 PC: 86 pairs — still only ~10% precision (PC is noise at every tier)

  **Calibration routing:**
  - 66% auto-handled (auto-MERGE + auto-SKIP at Stage 1 or 2)
  - 32% user review (PC at any tier, UNCERTAINs)
  - 2% L3 rigor

  **Ablation results:**
  - L3 web vs no-web: 4 overlap pairs, 100% agreement. Thin sample.
    Cost delta $0.147/pair (web) vs $0.012/pair (no-web). Decision: defer
    web choice to mid-B6.5a based on actual Stage 2 residual count.
  - Safety Net A: 100 unblocked random same-country pairs run through
    Haiku + Gemini. **0 flagged as MERGE at conf >0.85** — blocking recall
    looks solid on the sample.

  **Committed thresholds (symmetric, cross-family):**
  - Stage 1 auto-MERGE: L1 MERGE ≥0.88 AND L1.5 MERGE ≥0.88 (user chose 0.88
    over calibration-supported 0.85 for safety margin)
  - Stage 1 auto-SKIP: L1 SKIP ≥0.97 AND L1.5 SKIP ≥0.97
  - Stage 2 auto-MERGE: L2 MERGE ≥0.90 AND L2.5 MERGE ≥0.90
  - Stage 2 auto-SKIP: L2 SKIP ≥0.95 AND L2.5 SKIP ≥0.95
  - Stage 3 (L3 Sonnet): MERGE ≥0.92 auto, SKIP ≥0.90 auto, else user review
  - L3 web: DEFERRED — decided mid-run based on residual count
  - PC at any tier any conf: user review always
  - SKIP audit in B6.5a: 200 random auto-SKIPs through L2+L3 to verify FN rate

  **Key process changes vs B6.3 plan:**
  1. L1.5 Gemini on ALL 151K (was only the 0.92-0.97 L1 band)
  2. L1 auto-MERGE now requires cross-family agreement (was L1 alone ≥0.97)
  3. MERGE threshold LOWERED from 0.97 solo to 0.88 cross-family
  4. PC always escalates (was unspecified)
  5. L3 web decision deferred (was always-on)
  6. B6.5 split into B6.5a (automated) + B6.5b (interactive review)
  7. SKIP audit added to validate FN rate at scale

  **Spend:** $24.51 (oracle $22.93 + Gemini basic $0.13 + L2 Haiku rich $0.59
  + Gemini rich $0.19 + L3 no-web ablation $0.65 + Safety Net A $0.05 +
  calibration set build $0).

  **Under budget:** $56-71 below the $80-95 plan. Oracle came in 2x high
  per-pair; L2 calibration runs came in ~70x low because calibration was
  only 600 pairs (full escalated run moves to B6.5a).

  **Scripts built:**
  - `pipeline/identity/build_calibration_set.py` — stratified 5-tier sampler
  - `pipeline/identity/calibration_oracle.py` — Sonnet 4.6 + web_search_20250305 gold labeler
  - `pipeline/identity/producer_dedup_gemini.py` — Gemini 3 Flash via OpenRouter (basic or rich mode)
  - `pipeline/identity/producer_dedup_l2.py` — L2 Haiku rich prompt, batched 5/call
  - `pipeline/identity/producer_dedup_l3.py` — L3 Sonnet rigor (web or no-web)
  - `pipeline/identity/sync_calibration.py` — DB → calibration_set.json sync
  - `pipeline/identity/calibration_analysis.py` — per-tier accuracy reports
  - `pipeline/identity/crossmodel_agreement.py` — pairwise agreement matrix
  - `pipeline/identity/agreement_matrix.py` — production routing classifier
  - `pipeline/identity/l3_ablation.py` — web vs no-web comparison
  - `pipeline/identity/safety_net_a.py` — unblocked spot-check
  - `pipeline/identity/final_thresholds.py` — threshold sweep + commitment

  **Files:** `data/sprints/dedup/b6_4_analysis.md` (full + simplified analysis),
  `data/sprints/dedup/final_thresholds.json` + `.md` (committed rules),
  `data/sprints/dedup/calibration_set.json` (600 pairs + verdicts + gold),
  per-tier calibration reports (`l1_calibration.md`, `l1_gemini_basic_calibration.md`,
  `l2_haiku_rich_calibration.md`, `l2_gemini_rich_calibration.md`),
  `data/sprints/dedup/crossmodel_agreement.md`,
  `data/sprints/dedup/agreement_matrix.md`, `data/sprints/dedup/l3_ablation.md`,
  `data/sprints/dedup/safety_net_a.md`, logs in `data/stats/b6_4_*.log`,
  `data/session_prompts/b6_5a_production_ladder.md` (new),
  `data/session_prompts/b6_5b_interactive_review.md` (new).

  **Tables touched:** `producer_dedup_pairs` gained 3 new method_names:
  `l1_gemini_basic_calibration` (595 rows), `l2_haiku_rich` (600),
  `l2_gemini_rich_calibration` (600), `l3_sonnet_noweb_ablation_calibration`
  (45). Zero production `producers` / `wines` changes.

---

- **B6.5a-partial (2026-04-17 → 2026-04-18):** Production ladder through
  Stage 2 + $16 of exploratory probes. STOPPED after L2+L2.5 per user
  reassessment directive; L3 + L4 + review_queue deferred pending user Path
  decision.

  **Step 1 — L1.5 Gemini basic on all 151K pairs ($32.13):** 150,885 of
  151,120 pairs classified (99.84%, 235 parse failures at Gemini 3 Flash
  Preview). Mid-run OR key cap hit at $100 after 48,675 pairs; user raised
  to $200; resumed, completed cleanly. Verdict distribution: SKIP 139,470
  (92.4% @ avg 0.97), MERGE 5,658 (3.75% @ avg 0.93), PC 5,233 (3.47% @
  avg 0.89), UNCERTAIN 524 (0.35% @ avg 0.73). Gemini more aggressive on
  non-SKIP than Haiku (2.17x MERGE, 2.47x PC), as expected from calibration.

  **Step 2 — Stage 1 routing threshold change (0.97/0.97 → 0.95/0.95)
  based on production-scale SKIP audit.** Initial bucket-sort at 0.97/0.97
  gave 24,178 auto-skip vs 100-125K projected — tripping plan's "auto-SKIP
  <80K = pause" rule. Root: Haiku's median rich-SKIP conf is 0.94, below
  0.97. Ran 600-pair band-stratified SKIP audit (200 each at ≥0.97 / 0.95-
  0.96 / 0.93-0.94, $8 total) through L2 + L3 no-web. Measured **0 auto-
  apply MERGE FNs across all bands** — the 4 L3-MERGE dissents were all
  BBR/Hospices de Beaune négociant patterns at L3 conf 0.72-0.82, below
  auto-merge threshold. User chose Option 2 (0.95/0.95 symmetric) over
  keeping 0.97 (too strict, $80-100 extra L2 burn), lowering to 0.93
  (same-night ambiguity, belongs in user review), or asymmetric (principled
  but unvalidated). Final Stage 1 routing: auto-skip 91,555, auto-merge
  1,520, escalations 57,810 (40% of total, in-range). DECISIONS.md entry
  logged.

  **Steps 4+5 — L2 Haiku rich + L2.5 Gemini rich on 57,810 escalations
  (parallel, $51.14 combined):** L2 Haiku rich 57,459 at $38.04; L2.5
  Gemini rich 57,570 at $13.10. Second OR cap hit mid-L2.5 at $200; user
  raised to $300; resumed, completed. L2 verdicts: SKIP 53,872 (93.8%),
  MERGE 1,393 (2.4%), PC 1,709 (3.0%), UNCERTAIN 482. L2.5 verdicts:
  SKIP 52,217 (90.7%), MERGE 2,493 (4.3%), PC 2,807 (4.9%), UNCERTAIN 53.
  Cross-family MERGE consensus: 1,097 pairs; SKIP consensus: 51,357 pairs
  (89.2%). Aggressive-Gemini bucket (L2 SKIP × L2.5 MERGE): 1,055 pairs.

  **Step 6 — Stage 2 routing ($0):** Applied committed thresholds (MERGE
  0.90/0.90, SKIP 0.95/0.95, refined PC rule). Distribution: auto-skip
  34,078 (59.2%), auto-merge 676 (1.2%), PC user-review 3,226 (5.6%),
  residual 19,575 (34.0%), missing-tier 255 (0.4%). **Residual above 10K
  trigger point** — same root cause as Stage 1: Haiku median rich-SKIP
  0.94 doesn't clear 0.95/0.95 joint threshold. 88% of residual (17,214
  pairs) is both-tier SKIP consensus below threshold. Lowering SKIP to
  0.93/0.93 would drop residual to ~7,330 (in range).

  **Stage 2 analysis probes ($16.01, 470 no-web + 65 web):** Six probe
  categories to validate decisions and measure L3 strategy trade-offs.

  - Probe 1 (Stage 2 SKIP residual 0.93-0.94 band, 100 pairs): **2% L3
    MERGE rate** — threshold can drop to 0.93 safely.
  - Probe 2 (Stage 2 auto-SKIP control, 100 pairs): **1% L3 MERGE FN rate**
    — current SKIP threshold is safe; Safety Net B catches tail.
  - Probe 3 (Stage 2 auto-MERGE validation, 100 pairs): **🚨 10% L3 SKIP
    + 7% UNCERTAIN = 17% dissent.** Real patterns cross-family misses:
    shared-surname splits (Faustino vs Faustino Rivero Ulecia, Janisson
    Baradon vs Janisson, Tertre Daugay vs Daugay, Andre Lorentz vs
    Lorentz, Jacques Saumaize vs Saumaize), HdB négociant variants (Morey
    Blanc vs Cecile Tremblay, Lejeune vs short form), same-name cross-
    country (McPherson US/AU), collaboration labels (Savart & Chartogne vs
    Savart), Weingut Johannisberg vs Schloss Johannisberg ambiguity.
    **Auto-applying 676 would merge ~68 wrong producers.** Recommend
    routing all Stage 2 auto-MERGEs through L3 validation ($9) before
    execution.
  - Probe 4 (random residual preview, 100 pairs): 6% L3 MERGE yield → L3
    on 19,575 residual = ~1,175 MERGEs to find; 87% are confirmed SKIPs.
  - Probe 5 (cross-family disagreement L2 SKIP × L2.5 MERGE, 50 pairs):
    **60% L3 MERGE** — Gemini catches real merges Haiku missed. 1,055
    such pairs → ~633 real merges in production. Need L3 arbitration.
  - Probe 6 (random residual A/B web vs no-web, 20 pairs): **5% web delta**
    (4 MERGE web vs 3 MERGE no-web). Marginal value on random residuals.
  - Probe 7 (négociant patterns, 20 web pairs): 20% MERGE — mostly SKIP,
    L3 web confirms BBR/HdB remain distinct variants.
  - Probe 8 (cross-family disagreement, 15 web pairs): **67% L3 MERGE**
    — web strongly arbitrates disagreements. Worth the 11x cost premium
    on this specific bucket.
  - Probe 9 (cross-country same-brand, 10 web pairs): 30% MERGE — some
    global brands (Gallo-owned), most are distinct country producers.

  **Key findings distilled:**
  1. Stage 2 auto-MERGE has ~10% FP rate — do NOT auto-apply without L3.
  2. Stage 2 SKIP threshold is too tight; 0.93/0.93 safe and drops
     residual from 19.5K to 7.3K.
  3. L3 web earns 11x cost only on disagreements + négociant patterns;
     random residual delta is 5%.
  4. §11 needs amendments (shared-surname-split, cross-country same-name,
     collaboration-label) before B6.6 execution.
  5. Stage 2 Haiku+Gemini cross-family precision at ≥0.90 on MERGE is
     ~81-90% in production (vs calibration's 98.7%); need rigor-tier
     confirmation on the applied set.

  **Recommendations delivered in `b6_5a_stage2_analysis.md`:** three paths
  with cost/quality tradeoffs:
  - Path 1 (Pragmatic $218, in ceiling): lower SKIP to 0.93, L3 validate
    the 676 auto-MERGEs, skip rest of L3, user reviews ~11K pairs in B6.5b.
  - Path 2 (Balanced $313, need $320 ceiling raise, MY REC): lower SKIP
    to 0.93, L3 no-web on 7.3K residual + 676 auto-MERGEs, user reviews
    ~2-3K pairs.
  - Path 3 (Rigor $451, need $500 raise): hybrid L3 no-web + web on
    disagreements/négociants; smallest user pile ~1.5-2K.
  
  **Cheaper web search researched:** Anthropic web_search $10/1K; Serper.dev
  $1/1K (10x cheaper); pre-fetch + Haiku rich architecture would drop L3
  web from $0.147/pair to $0.008/pair (18x cheaper). Worth investing 1
  session in Sprint 7 opening for wine dedup ($600-1,200 savings projected).

  **Meta-tooling built this session:**
  - `pipeline/analyze/session_tokens.py` — parse transcript + log cost
  - `pipeline/analyze/update_dashboard.py` — live dashboard updater with
    `<!--markers-->` for session strip, progress block, step list, spend
    breakdown
  - Extended `producer_dedup_l2.py`, `producer_dedup_l3.py`,
    `producer_dedup_gemini.py` with `--pair-ids-file` + `--method-name`
  - `data/stats/spend_ledger.md` — running spend ledger
  - `data/dashboard.html` — fully rewritten light-mode, single-column
    current-sprint, combined roadmap+done, live token strip, 10-min
    auto-tick, spend breakdown at bottom

  **Spend this block: $32.13 + $51.14 + $6.57 (Stage 1 SKIP audit) + $16.01
  (analysis probes) = $105.85.** Sprint 6 total: ~$208.80 of $250 ceiling,
  $41.20 remaining. Path 2/3 need ceiling raise.

  **Files:** `data/sprints/dedup/b6_5a_stage2_analysis.md` (recommendations
  report), `b6_5a_stage1_escalations.json`, `b6_5a_skip_audit_pair_ids.json`,
  `b6_5a_probe_*.json` (9 files), `b6_5a_routing_sql.md`, `b6_5a_l4_audit_queries.sql`.
  
  **Tables:** 7 new method_names in `producer_dedup_pairs`
  (l1_gemini_basic, l2_haiku_rich prod, l2_gemini_rich prod, l2_skip_audit,
  l3_skip_audit, l3_probe_noweb, l3_probe_web). 2 working tables:
  `producer_dedup_routing_stage1` (151K), `producer_dedup_routing_stage2`
  (57K). Zero `producers`/`wines` changes.

  **DECISIONS.md:** 2 entries added — Stage 1 SKIP threshold 0.97→0.95
  based on production-scale audit; OR key cap hit twice with resume plan.

---

- **Session 7 (2026-04-20):** Published
  `data/sprints/dedup/session7_bakeoff_v2_design.md`, the durable redesign memo
  for the first v2 adjudication rebuild. Locked conclusions without changing
  `benchmark_v1` or the frozen Session 4 gates: v1 packets had no real
  official-domain retrieval, the citation contract exposed too few legal refs,
  the consensus path inherited child-schema failures, and Sonnet remained
  unsafe on shared-surname / holdco false merges. Recommended v2 scope:
  packet v2 with flat `evidence_refs`, official-domain retrieval, ref-safe
  adjudicator prompts, normalized-child consensus, and merge vetoes on the
  highest-risk contradiction families. Queue-building stayed blocked pending a
  real v2 rerun.

- **Session 8 (2026-04-20):** Built
  `pipeline/identity/bakeoff_packet_v2.py`,
  `pipeline/identity/bakeoff_harness_v2.py`, and
  `pipeline/identity/bakeoff_run_v2.py`, then ran the canonical v2 proof subset
  plus the full rerun under `data/sprints/dedup/bakeoff_v2/`. Contract hygiene
  was fixed: hidden-field leaks stayed at zero, schema-validity and citation
  integrity hit 1.0, and normalized-child consensus no longer inherited child
  ref breakage. But the full rerun still failed the frozen gates because
  continuity / alias evidence remained too permissive and drove 31 false merges
  for Gemini, 26 for Sonnet, and 18 for consensus. Result: queue-building still
  blocked; the new root problem was continuity trust, not packet contract
  hygiene.

- **Session 9 (2026-04-21):** Audited those v2 false merges and published
  `data/sprints/dedup/session9_v3_continuity_audit.md`. Core finding: 37 of 39
  unique false-merge cases carried `official_continuity_*` refs; alias
  cross-mentions and shared-domain continuity were doing far too much work.
  Locked the minimum v3 redesign: split hard vs soft continuity, stop treating
  organic-domain matches as official continuity, require exact full-name alias
  proof on hard-official pages, downgrade shared-domain continuity unless
  page-level brand identity aligns, add a narrow ownership/acquisition risk
  flag, and expand the proof subset to 36 cases with 8 continuity stress cases.

- **Session 9.1 (2026-04-21):** Implemented the minimum v3 continuity redesign
  and added a proof-only runner path. The first proof cycle failed on the
  unresolved-official / secondary-evidence cluster and correctly stopped before
  any 152-case rerun. Session 9.2 later established that those proof artifacts
  still depended on stale pre-v3 packet files, so Session 9.1 remains useful as
  a diagnosis memo but not the final verdict on the packet layer.

- **Session 9.2 (2026-04-21):** Forced a fresh packet rebuild to `v2.1` and
  added the unresolved-official / secondary-evidence backstop in the packet and
  harness layers. Fresh proof artifacts under
  `session9_2_unresolved_official_backstop_proof_subset` passed cleanly for
  Sonnet, Gemini, and consensus. The dangerous continuity false-merge cluster
  was now controlled on a rebuilt proof slice, but the full rerun still
  required explicit approval.

- **Session 9.3 (2026-04-21):** Ran the approved 152-case rerun on the
  proof-cleared `v2.1` path and published the canonical scorecard, diff,
  manifest, and gate memo under `data/sprints/dedup/bakeoff_v2/`. Safety
  improved sharply: Gemini and consensus reached `0` false merges, Sonnet fell
  to `1`, and auditability stayed perfect. But recall / queue burden remained
  far outside the frozen gates: Sonnet finished `1 false merge / 5 hard missed /
  44 soft missed`, Gemini `0 / 9 / 42`, consensus `0 / 4 / 47`. Queue-building
  stayed blocked.

- **Session 9.4 (2026-04-21):** Published
  `data/sprints/dedup/session9_4_post_rerun_failure_audit.md`, the post-rerun
  failure audit memo. The audit showed the new blocker is mainly soft missed
  merges / over-flagging, not residual false merges: 49 of 51 expected-MERGE
  benchmark cases were missed by all three contenders, and 39 cases that both
  single models had merged in Session 8 became non-MERGE for both in Session
  9.3. Dominant miss families were `11.4.h` orthographic variants, `11.4.f`
  generational succession, `11.4.n` global multi-country brands, and `11.4.p`
  merchant prefixes. The tempting rescue of relaxing
  `risk_sparse_official_evidence` was rejected because the same packet
  signature overlaps previously fixed false merges. Recommendation: treat the
  current adjudication path as a non-execution-ready artifact and require an
  explicit user decision before any further Sprint 6 spend.

- **Session 9.5 (2026-04-21):** Published
  `data/sprints/dedup/session9_5_freeze_or_rebuild_strategy.md`, the larger
  redesign-selection memo, plus the follow-on prompt
  `data/session_prompts/s9_6_pattern_specialist_proof_if_approved.md`. The memo
  compared the three remaining redesign families under the frozen Session 4
  benchmark and gates plus the new `$20` exploration cap. Recommendation: if
  Sprint 6 continues, test **pattern-family specialists** layered on the safe
  `gemini_guardrailed_v2` base rather than another global packet tweak.
  Evidence: the four dominant families `11.4.h`, `11.4.f`, `11.4.n`, and
  `11.4.p` cover `47 / 51` merge cases and `26` skip cases in the benchmark,
  including all `16` known false-merge pattern cases. Smallest honest proof:
  route only those `73` cases through specialists, keep the remaining `79` on
  the base path, and score the composite result across the full `152`.
  Estimated external spend stays well below cap: measured `Haiku + Serper`
  economics imply roughly `$0.44` per full 73-case pass and a conservative
  `$5-10` proof budget. Queue-building remains blocked; the next user decision
  is now specific: freeze the adjudication path, or approve the bounded
  specialist proof.

---

## Done

- **S7 - adjudication bakeoff v2 design.** Produced
  `data/sprints/dedup/session7_bakeoff_v2_design.md`, a durable postmortem +
  redesign memo that keeps `benchmark_v1` and the Session 4 hard gates frozen,
  explains why `session6_first_real_bakeoff_v1` failed, and defines the exact
  packet / runner / contender changes to test next. Headline findings: (1) all
  152 v1 packets had `retrieval = missing` and zero official-domain hits, so
  the hardest merge-vs-split cases lacked the main disambiguator; (2) the
  output contract made citation failure easy because the packet exposed nested
  facts but too few legal citeable refs, which drove auditability collapse via
  `broken_support_refs` / `broken_contradiction_refs`; (3) the
  `haiku_gemini_consensus_v1` contender was built from raw child outputs, so it
  inherited child-schema failures instead of absorbing them; (4) Sonnet was the
  closest single contender on exact accuracy, but still false-merged shared-
  surname splits and holdco / product-tier cases often enough to stay unsafe.
  Recommended v2: packet v2 with a flat citeable evidence ledger + real
  official-domain retrieval, ref-safe adjudicator prompt, normalized-child
  consensus, explicit merge veto on the highest-risk contradiction families,
  and a narrowed lineup centered on `sonnet_guardrailed_v2`,
  `gemini_guardrailed_v2`, and `sonnet_gemini_consensus_v2` (with
  `gpt5mini_guardrailed_v2` as backup swap if Gemini still looks too
  merge-happy on the proof subset). Queue-building remains blocked until a full
  v2 rerun clears both the production and fallback gates. Added next-session
  prompt `data/session_prompts/s7_1_bakeoff_v2_build_and_run.md`. Spend: $0.

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
