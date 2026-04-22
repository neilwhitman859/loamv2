# Sprint 7: Producer Identity ER — journal

**Opened:** 2026-04-21
**Status:** Active
**Current block:** Session 10.11 landed the first audited truth-expansion pack:
`ground_truth_pack_001_v1` built `36` records, including `29` net-new
scoreable additions and `3` quarantined disputes. That starts the truth-growth
loop honestly, but the corpus is still only `446` scoreable pairs, so breadth
is still the blocker rather than method search. Keep `benchmark_v1`, the frozen
Session 4 production/fallback gates, `no DB writes`, `no human pair review at
scale`, and the rule that abstention beats a false merge fixed. The next honest
move is Pack 002 under the same strict evidence bar unless the user explicitly
wants to reopen Pack 001 and trade strictness for more volume.

---

## Block log

- **Session 10.1 (2026-04-21):** launched Sprint 7 as the new active producer
  identity effort.

  **Scope held:** no implementation of dossier builders, no benchmark reruns,
  no queue-building, no merge execution, and no human-review workflow.

  **Decisions locked at launch:**
  - no human pair review at scale
  - fixed budget
  - AI-only escalation allowed only on a narrow frontier
  - abstention is better than a false merge
  - the new live method name is `identity_dossier_select_v1` ("Identity
    Dossier Select")

  **Why the sprint exists:** Sprint 6 proved the pairwise merge-only framing is
  useful as benchmark context but not trustworthy enough as the live path.
  Sprint 7 therefore starts from reusable producer dossiers, shortlist
  selection, explicit relationship typing, and accepted-edge-first merging.

- **Session 10.2 (2026-04-21):** turned Sprint 7's method name into a usable
  spec package.

  **Scope held:** no dossier-builder implementation, no shortlist code, no
  benchmark reruns, no queue-building, and no merge execution.

  **Artifacts produced:**
  - `producer_dossier_v1.md`
  - `source_signal_inventory.md`
  - `edge_taxonomy_v1.md`

  **Decisions locked in the spec:**
  - the cheap dossier is backbone-first and regulatory-aware, not website-first
  - the cheap path should rely on names, place, portfolio shape, and US
    regulatory/market clues because canonical producer relationship/history
    tables are still effectively empty
  - website/profile/history evidence is reserved for escalation only
  - the selector label set is fixed at `SAME_AS`, `RELATED_BUT_DISTINCT`,
    `NONE`, `UNSURE`
  - only accepted `SAME_AS` edges are merge-eligible; `RELATED_BUT_DISTINCT`
    is a first-class stored non-merge edge

  **Why this matters:** Sprint 7 can now design shortlist generation against a
  stable evidence object instead of re-litigating what the model should see on
  every later session.

- **Session 10.2a (2026-04-21):** tightened the handoff contract for any
  autonomous continuation.

  **Scope held:** no shortlist design, no selector work, no implementation,
  no benchmark reruns, and no merge execution.

  **Artifacts produced:**
  - explicit `Autonomous continuation: yes/no` protocol in `data/sessions.md`
  - matching repo-wide rule in `AGENTS.md`
  - heartbeat prompt hardened to require the explicit field

  **Decisions locked in the protocol:**
  - automation should advance only when the latest **Done** session explicitly
    says `Autonomous continuation: yes`
  - missing or ambiguous autonomy language counts as blocked
  - the current Sprint 7 handoff remains blocked until the shortlist
    seed-source decision is explicitly cleared

  **Why this matters:** the heartbeat can now follow a narrow machine-readable
  gate instead of trying to infer permission from narrative wrap-up prose.

- **Session 10.3 (2026-04-21):** turned Sprint 7's shortlist idea into a fixed
  retrieval contract.

  **Scope held:** no shortlist-builder implementation, no selector prompt
  design, no benchmark proof, no queue-building, and no merge execution.

  **Artifacts produced:**
  - `shortlist_generation_v1.md`
  - `s10_4_selector_harness_design.md`

  **Decisions locked in the spec:**
  - only Tier A sources may seed candidate IDs in `v1`: canonical lexical,
    LWIN producer names, TTB brand/applicant names, and state-registration
    brand/legal names
  - importer, retailer, merchant, profile, website, and history sources are
    re-rank-only or escalation-only, not seed generators
  - the raw candidate union is capped at `24` unique candidates and the final
    selector shortlist is capped at `12`
  - the suppression layer must proactively drop weak fuzzy hits, shared-surname
    / first-word collisions, permit-only adjacency, and merchant/importer
    context-only candidates before selector work
  - empty shortlist is a valid `v1` outcome; safe abstention beats a forced
    noisy candidate list

  **Why this matters:** Session 10.2a's autonomy blocker is now cleared by an
  explicit shortlist-source decision, and Session 10.4 can design the selector
  against a stable candidate universe instead of reopening retrieval scope.

- **Session 10.4 (2026-04-21):** turned Sprint 7's selector idea into a fixed
  harness contract.

  **Scope held:** no selector implementation, no proof execution, no
  escalation-dossier design, no accepted-edge storage implementation, no
  queue-building, and no merge execution.

  **Artifacts produced:**
  - `selector_harness_v1.md`
  - `s10_5_escalation_layer_and_accepted_edge_rules.md`

  **Decisions locked in the spec:**
  - the selector sees one anchor selector card plus up to `12` candidate
    mini-cards with explicit retrieval-basis and comparison blocks, not raw
    source dumps
  - the selector must choose **one candidate or `none`** before applying the
    Sprint 7 verdict label
  - `NONE` is now an explicit stop condition, while `UNSURE` is reserved for
    cases where escalation still has a plausible path to change the answer
  - patterned reason codes and packet-visible evidence references replace long
    prose explanations
  - the first proof is split into a selector-only frozen-packet phase and a
    shortlist-integration smoke phase so retrieval failures cannot hide inside
    selector scoring

  **Why this matters:** Sprint 7 now has a fixed middle layer between
  shortlist generation and escalation. Session 10.5 can design the heavy-path
  evidence and accepted-edge rules without reopening what the selector sees,
  what it returns, or how the first bounded proof should score it.

- **Session 10.5 (2026-04-21):** turned Sprint 7's selector and escalation
  handoff into a full control package.

  **Scope held:** no selector implementation, no model runs, no proof execution,
  no DB writes, no merge execution, and no graph-schema migration work.

  **Artifacts produced:**
  - `escalation_dossier_v1.md`
  - `accepted_edge_rules_v1.md`
  - `selector_proof_v1.md`
  - `s10_6_bounded_proof_build.md`

  **Decisions locked in the spec:**
  - escalation is now `UNSURE`-only, one-pass, and limited to
    `candidate_frontier` or `shortlist_gap_probe`
  - the heavy path may add only bounded `web_identity`, `profile_snippets`,
    `people_history`, `vineyard_profile`, `raw_supporting_rows`, and
    `retrieval_gap_diagnostics` blocks, and it keeps the same four-label output
    contract as the cheap selector
  - durable graph state now splits between accepted pairwise edges
    (`SAME_AS`, `RELATED_BUT_DISTINCT`, `NONE`) and non-edge frontier records;
    selector-side `NONE` fans out only to candidates actually shown in the
    packet, while empty shortlists and post-escalation `UNSURE` stay out of the
    accepted-edge graph
  - the next proof is frozen as a four-phase bundle: Phase A 48-case selector
    proof, Phase B escalation replay on the 8 frontier cases, Phase C shortlist
    integration smoke, and Phase D accepted-edge write simulation

  **Why this matters:** Sprint 7 now has a coherent control layer instead of a
  loose stack of separate specs. Session 10.6 can build proof artifacts and
  local scaffolding without quietly inventing escalation, edge, or proof rules
  during implementation.

- **Session 10.6 (2026-04-21):** built the first frozen local proof bundle for
  Sprint 7.

  **Scope held:** no model calls, no DB writes, no benchmark edits, no policy
  widening, and no merge execution.

  **Artifacts produced:**
  - `proof/selector_proof_case_sources_v1.json`
  - `proof/selector_proof_hidden_key_v1.json`
  - `proof/phase_a_selector_packets/` (`48` packets)
  - `proof/phase_b_escalation_packets/` (`8` packets)
  - `proof/phase_c_shortlist_manifest.json`
  - `proof/selector_proof_result_schema_v1.json`
  - `proof/selector_proof_scorecard_template_v1.md`
  - `proof/phase_d_oracle_write_simulation_v1.json`
  - `proof/selector_proof_build_validation_v1.json`
  - `proof/selector_proof_build_memo_v1.md`
  - `pipeline/identity/selector_proof_v1.py`
  - `s10_7_bounded_proof_execution.md`

  **Implementation facts locked by the build:**
  - the proof bundle now persists a hidden key, visible packet trees, shortlist
    smoke manifest, score schema, and accepted-edge/frontier write simulation
    in one local directory
  - the builder uses live DB reads only and preserves the Session 10.5 policy
    contract without adding new labels, new escalation modes, or new proof
    strata
  - Phase C scoring now keys off manifest strata instead of hardcoded case
    ranges, so the smoke checker follows the frozen proof object instead of an
    accidental numbering convention
  - oracle self-check passed all four local phases, proving the scaffolding is
    coherent even though no real model outputs have been run yet

  **Why this matters:** Sprint 7 now has a real artifact boundary between
  design and execution. Session 10.7 can spend the first proof budget on
  bounded packet execution and scoring instead of inventing file layout,
  schema glue, or write-simulation logic mid-run.

- **Session 10.7 (2026-04-21):** ran the first real bounded proof against the
  frozen `selector_proof_v1` bundle.

  **Scope held:** one model pass only on the frozen Phase A / Phase B packets,
  no DB writes, no policy widening, no proof-set mutation, no benchmark edits,
  and no Phase C shortcut implementation.

  **Artifacts produced:**
  - `proof/selector_proof_phase_a_results_v1.jsonl`
  - `proof/selector_proof_phase_b_results_v1.jsonl`
  - `proof/selector_proof_phase_a_raw_v1.jsonl`
  - `proof/selector_proof_phase_b_raw_v1.jsonl`
  - `proof/selector_proof_execution_summary_v1.json`
  - `proof/selector_proof_scorecard_v1.md`
  - `proof/selector_proof_phase_c_runnability_v1.md`
  - `proof/selector_proof_go_no_go_memo_v1.md`
  - `s10_8_selector_proof_failure_analysis.md`

  **Execution facts locked by the run:**
  - the first real proof used `claude-sonnet-4-6` and spent `$2.08`, staying
    inside the Sprint 7 Phase 1 ceiling
  - Phase A stayed safe on false `SAME_AS` (`0`) but still failed the selector
    gate: `7 / 16` `SAME_AS` misses, `12 / 12` `RELATED_BUT_DISTINCT` misses,
    and all `8` frontier `UNSURE` cases collapsed to non-`UNSURE` labels
  - Phase B also failed: `4 / 8` exact hits, `4 / 6` resolvable-frontier
    recoveries, `1` false `RELATED_BUT_DISTINCT`, and both expected-`UNSURE`
    shortlist-gap probes resolved to `NONE`
  - Phase D reopened `4` contradictory overwrite attempts because escalation
    tried to recover cases the selector had already written as `NONE`
  - Phase C is now explicitly blocked, not just deferred: no reusable
    shortlist-builder implementation exists yet outside the proof scaffolding

  **Why this matters:** Sprint 7 now has its first real outcome, and it is not
  strong enough to justify builder implementation. The next move is not "build
  more"; it is an explicit decision on whether the failure families deserve one
  narrow analysis pass or whether this sprint should freeze here.

- **Session 10.8 (2026-04-21 to 2026-04-22):** ran the broad autonomous method
  bakeoff after the Session 10.7 no-go.

  **Scope held:** keep `benchmark_v1` and the Session 4 production/fallback
  gates fixed, do not write to the DB, do not open human pair review at scale,
  do not mutate the benchmark mid-bakeoff, and do not quietly soften the trust
  bar to make a candidate look viable.

  **Artifacts produced:**
  - `method_bakeoff/session10_8_method_scoreboard_v1.md`
  - `method_bakeoff/session10_8_hybrid_guarded_stack_confirmation_v1.md`
  - `method_bakeoff/session10_8_hybrid_guarded_stack_stress_audit_v1.md`
  - zero-cost overlay contenders under `pipeline/identity/`

  **Findings locked by the bakeoff:**
  - the next path became a broad method bakeoff, not narrow failure analysis
  - `identity_dossier_select_v1` moved to failed-contender status rather than
    staying the live frame for Sprint 7
  - `hybrid_guarded_fils_person_alias_v1` emerged as the strongest benchmark
    survivor
  - the final stack cleared the frozen benchmark on Sonnet, Sonnet rerun, Opus,
    and Opus rerun at `0` false merges / `0` hard misses / `0` soft misses
  - frozen-file confirmation then reached its honest limit because the late
    overlays were not independently covered by the existing 24-case holdout

  **Why this matters:** the method search itself reached a natural stop. The
  bottleneck moved from "find a better benchmark survivor" to "test whether the
  benchmark winner survives independent scrutiny."

- **Session 10.9 (2026-04-22):** pressure-tested the benchmark-clearing stack
  against live canonical snapshots plus fresh web evidence.

  **Scope held:** no model calls, no DB writes, no benchmark edits, no policy
  widening, and no pretending ambiguous outside evidence is stronger than it is.

  **Artifacts produced:**
  - `method_bakeoff/session10_9_case_snapshots_v1.json`
  - `method_bakeoff/session10_9_web_validation_ledger_v1.md`
  - `method_bakeoff/session10_9_web_validation_memo_v1.md`
  - `pipeline/identity/build_session10_9_case_snapshots.py`

  **Findings locked by web validation:**
  - `place_alias` held up best and has real external support (`Stadt Krems` /
    `Krems`)
  - the `maison_alias` benchmark win did **not** survive independent scrutiny:
    `Ardhuy Cabotte` / `de la Cabotte` now looks like a bad merge between a
    Rhône Cabotte estate under the d'Ardhuy family and a separate Burgundy
    `Maison la Cabotte` line
  - `fils_person_alias` did not independently clear; one exact negative remains
    plausibly distinct, but the analogue controls are historically messy rather
    than cleanly separate

  **Why this matters:** the benchmark-clearing stack is no longer the trusted
  recommendation. The next honest session is truth repair on the challenged late
  benchmark wins, not more bakeoff cycling or implementation work.

- **Session 10.10 (2026-04-22):** shifted Sprint 7 from a narrow truth-repair
  memo into a broader ground-truth foundation.

  **Scope held:** no DB writes, no benchmark reruns, no new method bakeoff,
  no relabeling of machine verdicts as truth, and no pretending singleton
  sanity cases count toward the pair target.

  **Artifacts produced:**
  - `ground_truth_program_v1.md`
  - `ground_truth_seed_pairs_v1.jsonl`
  - `ground_truth_seed_singletons_v1.jsonl`
  - `ground_truth_seed_summary_v1.md`
  - `pipeline/identity/build_ground_truth_seed_v1.py`
  - `s10_11_ground_truth_pack_001.md`

  **Decisions locked by the foundation:**
  - Sprint 7 should optimize against a broader audited ground-truth program,
    not just the old `152`-case frozen benchmark
  - the reusable seed now starts from `422` validated pair rows still mapping
    to today's `producer_dedup_pairs`
  - only scoreable pair labels (`SAME_AS`, `RELATED_BUT_DISTINCT`, `NONE`)
    count toward the new `1,000`-pair target; `DEFERRED` rows and singleton
    `KEEP_AS_IS` checks stay in side ledgers
  - the live `producer_dedup_pairs` table still does **not** count as truth
    because `verdict_source` is blank
  - Pack 001 should absorb the challenged late overlay wins and use them as
    part of a broader audited truth expansion, not as isolated special cases

  **Why this matters:** Sprint 7 finally has a reproducible measurement object.
  Future method claims can now be judged against the same truth files instead
  of against whichever benchmark slice happens to look decisive in the moment.

- **Session 10.11 (2026-04-22):** built the first audited truth-expansion pack.

  **Scope held:** no DB writes, no benchmark reruns, no new method search, no
  queue-building, and no attempt to force disputed-history cases into
  scoreable labels.

  **Artifacts produced:**
  - `ground_truth_pack_001_v1.jsonl`
  - `ground_truth_pack_001_summary_v1.md`
  - `pipeline/identity/build_ground_truth_pack_001_v1.py`
  - `s10_12_ground_truth_pack_002.md`

  **Results locked by the pack:**
  - Pack 001 requested `72` candidate cases and built `36` audited records
  - `29` of those records are net-new scoreable additions:
    - `9` `SAME_AS`
    - `8` `RELATED_BUT_DISTINCT`
    - `12` `NONE`
  - `6` records are explicit seed repair / reaffirm outputs, including
    `3` quarantined disputes:
    - `Ardhuy Cabotte` / `de la Cabotte`
    - `Protheau & Fils` / `Jean-Francois Protheau`
    - `Fery-Meunier` / `Jean Fery & Fils`
  - the scoreable truth corpus now moves from `417` to `446`, leaving a
    documented gap of `+554` to the `1,000`-pair target
  - the strongest new growth came from two reusable evidence lanes:
    - strict TTB shared-permit precision traps (`11.4.j`)
    - web-backed subbrand / collaboration / foreign-domain families
      (`11.4.s`, `11.4.o`)

  **Why this matters:** Sprint 7 is no longer blocked on "build the first
  pack." It is now blocked on growing the truth base fast enough without
  weakening the evidence bar. Session 10.12 should therefore focus on more
  `SAME_AS` recall and additional non-`FR` coverage, not on relitigating the
  bakeoff or pretending Pack 001 solved the breadth problem.

- **Session 10.12 (2026-04-22):** built the second audited truth-expansion pack.

  **Scope held:** no DB writes, no benchmark reruns, no new method search,
  no Pack 001 relabeling without direct contradictory evidence, and no
  dilution of the Pack 001 evidence bar.

  **Artifacts produced:**
  - `ground_truth_pack_002_v1.jsonl`
  - `ground_truth_pack_002_summary_v1.md`
  - `pipeline/identity/build_ground_truth_pack_002_v1.py`
  - `s10_13_ground_truth_pack_003.md`

  **Results locked by the pack:**
  - Pack 002 requested `21` candidate cases and landed `21` audited records
  - all `21` records are net-new scoreable additions:
    - `11` `SAME_AS`
    - `5` `RELATED_BUT_DISTINCT`
    - `5` `NONE`
  - the scoreable truth corpus now moves from `446` to `467`, leaving a
    documented gap of `+533` to the `1,000`-pair target
  - the new builder switches from Pack 001's name-keyed lookup to explicit
    producer IDs so cross-country same-name merchant-house cases can be
    audited without collapsing the two sides together
  - the strongest reusable evidence lanes were:
    - global merchant-house cross-country continuity (`11.4.n`)
    - non-`FR` parent / collaboration families (`11.4.s`, `11.4.o`)
    - non-`FR` TTB shared-permit precision traps (`11.4.j`)

  **Why this matters:** Sprint 7 now has two audited expansion packs and a
  truth corpus of `467` scoreable pairs. The bottleneck is no longer whether
  the Pack 001 bar can be reused; it is whether future packs can broaden the
  corpus beyond the easiest core-heavy families without weakening that bar.
