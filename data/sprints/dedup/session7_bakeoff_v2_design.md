# Session 7 - adjudication bakeoff v2 design

**Date:** 2026-04-20  
**Goal:** turn the failed `session6_first_real_bakeoff_v1` result into a tightly scoped v2 bakeoff plan with a plausible path to clearing the frozen Session 4 production and fallback gates  
**Primary deliverable:** one durable v2 design memo that explains why v1 failed, names the exact changes to test next, and defines the minimum next run required before any queue-building work can start  
**Non-goals:** changing `benchmark_v1`, softening the Session 4 score math or hard gates, pretending v1 was close enough, or starting queue construction

---

## 1. Frozen facts

- `benchmark_v1` stays frozen at 152 merge-only cases.
- The Session 4 hard gates stay frozen. v2 is not allowed to "win" by moving the goalposts.
- `session6_first_real_bakeoff_v1` remains the canonical v1 artifact.
- Queue-building is still blocked.

### v1 scoreboard, in one line each

| contender | exact acc | false merge | hard missed | soft missed | auditability | diagnosis |
|---|---:|---:|---:|---:|---:|---|
| `haiku_single_v1` | 0.2237 | 0 | 5 | 44 | 0.2632 | safe only because it over-flagged itself out of usefulness |
| `gemini_single_v1` | 0.5921 | 10 | 4 | 13 | 0.7105 | materially too merge-happy, plus heavy schema/citation breakage |
| `gpt5mini_single_v1` | 0.5789 | 14 | 16 | 7 | 0.8026 | still too merge-happy and too willing to over-read lexical/catalog overlap |
| `haiku_gemini_consensus_v1` | 0.2039 | 0 | 0 | 49 | 0.2697 | consensus was built on raw child outputs, so it inherited child schema failures and massively over-flagged |
| `sonnet_single_v1` | 0.7368 | 11 | 9 | 10 | 0.8882 | best accuracy, but still unsafe on false merges and still below the auditability/schema bar |

### The strongest hard facts behind the failure

1. **Packet retrieval was effectively empty.**
   - All 152 packets had `completeness.retrieval = "missing"`.
   - All 152 packets had **0 official-domain hits**.
   - Every packet carried `sparse_web`.

2. **The output contract was stricter than the packet made practical.**
   - Models were allowed to cite only `official_domain_hits[].ref_id`, `secondary_hits[].ref_id`, `support_signals[].code`, and `contradiction_flags[].code`.
   - The models naturally cited lexical/geography/catalog facts that existed in the packet but were **not** citeable refs.
   - Result: auditability collapsed mostly from `broken_support_refs` / `broken_contradiction_refs`, not from JSON parse failure.

3. **The consensus contender was wired at the wrong layer.**
   - `haiku_gemini_consensus_v1` combines **raw child outputs**, unions child refs, then gets normalized afterward.
   - That means invalid child refs poison the consensus row instead of being filtered out first.

4. **The strongest model still had real decision-quality problems, not just contract problems.**
   - `sonnet_single_v1` was closest overall, but still posted 11 false merges and 9 hard misses.
   - So v2 cannot be just "fix the citation contract and rerun the same thing."

---

## 2. Why v1 failed

## 2.1 Packet blind spots

The main packet problem was not hidden-field leakage or DB resolution. Those parts worked. The problem was that the visible packet did not give contenders enough grounded, citeable evidence to separate:

- same-surname family splits from true aliases,
- holdco/product-line relationships from same-brand merges,
- cross-country global-brand continuity from coincidental same-name rows.

In practice, the packet gave rich nested facts but poor citeability:

- geography facts existed, but not as citeable `ref_id`s
- lexical facts existed, but not as citeable `ref_id`s
- portfolio-shape claims existed, but not as citeable `ref_id`s
- official retrieval was absent, so the most important disambiguator in hard cases never arrived

This made the models do one of two bad things:

- cite invalid refs and get fail-closed to `FLAGGED`
- ignore the missing official proof and over-merge anyway

## 2.2 Contender-by-contender diagnosis

### `haiku_single_v1`

- Main failure mode: over-conservative escalation.
- 112 of 152 rows were schema-invalid after normalization, mostly `broken_support_refs`.
- Even if the contract breakage were fixed, the core behavior is still too timid: 44 soft misses and 69 safe flags.
- Conclusion: not a plausible v2 production or fallback path.

### `gemini_single_v1`

- Main failure mode: confident false merges on sparse lexical/catalog evidence.
- 10 false merges, including repeated family-split and product-tier mistakes.
- 44 schema-invalid rows show the citation contract also hurt it.
- Conclusion: still worth keeping as a cheap cross-family signal, but only behind stronger guardrails.

### `gpt5mini_single_v1`

- Main failure mode: similar to Gemini, but even more willing to merge on lexical/catalog shape alone.
- 14 false merges and 16 hard misses means it was aggressive in the wrong places and still not decisive enough on hard real merges.
- Auditability was better than Gemini but still below bar.
- Conclusion: not the best primary v2 cheap candidate.

### `haiku_gemini_consensus_v1`

- Main failure mode: architecture failure, then over-flagging.
- 111 schema-invalid rows; auditability 0.2697.
- Because consensus happened on raw child outputs, it amplified child contract breakage instead of absorbing disagreement safely.
- Even beyond schema invalidity, the decision rule was too escalation-heavy to meet the fallback flag-rate gate.
- Conclusion: consensus is still worth testing in v2, but only if rebuilt on **normalized** child outputs with explicit merge/skip rules.

### `sonnet_single_v1`

- Main failure mode: best judge, but still too willing to merge when lexical/catalog evidence "looked right."
- Repeated false-merge cluster on family splits and product-tier / umbrella-brand cases.
- 17 schema-invalid rows were material but not the whole story.
- Conclusion: this is still the best production candidate, but only if v2 gives it better evidence and an explicit anti-false-merge safety rail.

## 2.3 Cross-contender failure clusters

### Cluster A - shared-surname family splits (`11.4.m`)

Examples:
- `blind_core_audit_064` `Ginglinger-Fix` vs `Ginglinger`
- `blind_core_audit_067` `Franck Bonville` vs `Camille Bonville`
- `blind_core_audit_041` `Dancer` vs `Theo Dancer`

Pattern:
- lexical overlap + same geography tempted Gemini/GPT/Sonnet into false merges
- Haiku usually flagged or skipped
- packet had no official-domain proof to show whether the shorter form was truly the same label or a different family member

### Cluster B - holdco / product-tier / umbrella-brand confusion (`11.4.g`)

Examples:
- `blind_core_audit_042` `Lafite Rothschild` vs `Barons Rothschild Lafite Reserve Speciale Pauillac`
- several Rothschild/DBR cases across blind core and tail

Pattern:
- models over-read brand-token overlap and catalog subset signals
- packet did not make "same parent family, distinct on-label brand" explicit enough

### Cluster C - true merges needing official continuity proof

Examples:
- `known_missed_merge_patterns_001` `Protheau & Fils` vs `Jean-Francois Protheau`
- `blind_core_audit_025` `Barons de Rothschild (Lafite)` vs `Lafite Rothschild`
- `tail_random_sample_003` `Barons Rothschild (Lafite)` vs `Barons de Rothschild (Lafite)`

Pattern:
- all or nearly all contenders missed or flagged these
- these are exactly the cases where official-domain continuity or explicit historical-brand evidence is needed
- v1 had none

### Cluster D - auditability collapse from ref mismatch, not reasoning collapse

Examples:
- `haiku_single_v1`: 112 invalid rows
- `haiku_gemini_consensus_v1`: 111 invalid rows
- `gemini_single_v1`: 44 invalid rows
- `sonnet_single_v1`: 17 invalid rows

Pattern:
- contenders cited facts like `same_country`, `same_region`, `shared_core_tokens`, `region differs`, `exact_overlap_count`, or even rule ids inside support refs
- those concepts existed in the packet but were not legal refs

This is a design bug in the packet/runner contract, not just a model-quality bug.

---

## 3. Exact v2 changes to test

## 3.1 Packet changes

### Change 1 - move from implicit nested facts to a flat citeable evidence ledger

Add a top-level packet section such as `evidence_refs[]`, where **every fact the model is expected to cite** gets a stable `ref_id`.

Minimum ref classes:

- lexical refs
  - `lex_contains`
  - `lex_near_exact`
  - `lex_shared_core_tokens`
- geography refs
  - `geo_same_country`
  - `geo_same_region`
  - `geo_same_appellation`
  - `geo_country_conflict`
- catalog refs
  - `catalog_subset_match`
  - `catalog_exact_overlap`
  - `catalog_asymmetry`
  - `catalog_portfolio_shape`
- contradiction refs
  - `risk_shared_surname_split`
  - `risk_holdco_or_product_tier`
  - `risk_sparse_official_evidence`
- retrieval refs
  - `official_a_1`, `official_b_1`, etc.
  - `official_unresolved_a`, `official_unresolved_b` when no official domain can be resolved

The important v2 rule is simple: if the model can mention a fact in `reason`, it must also be able to cite it legally.

### Change 2 - populate real official-domain retrieval

v2 should not allow another 152/152 `retrieval = missing` run.

Minimum requirement:

- attempt official-domain retrieval for both sides on every case
- store either:
  - 1-2 official hits with claim summaries, or
  - an explicit unresolved record explaining why no official domain was resolved

This does **not** mean live search during adjudication. It means packet generation must do the retrieval work once, up front, and materialize the result into the packet.

### Change 3 - surface the highest-risk contradiction patterns as first-class refs

The packet already hints at these patterns, but v2 should make them unavoidable:

- shared-surname split risk
- holdco / product-tier risk
- cross-country same-name without continuity proof

These need explicit citeable refs because they are the exact places where v1 false merges clustered.

## 3.2 Runner and prompt changes

### Change 4 - make the adjudicator prompt ref-safe

The prompt should:

- include the exact list of legal `ref_id`s for that packet
- say that `key_support_refs` / `key_contradiction_refs` must be drawn only from that list
- include one tiny positive example and one tiny negative example

Negative example:
- bad: citing `same_country` or `11.4.m` in `key_support_refs`

Positive example:
- good: citing `geo_same_country` and `risk_shared_surname_split`

### Change 5 - rebuild consensus on normalized child rows, not raw child rows

`haiku_gemini_consensus_v2` should:

- normalize each child output first
- treat child schema-invalid rows as child failure states, not as consensus evidence
- only allow `MERGE` if:
  - both child rows are schema-valid
  - both say `MERGE`
  - both agree on survivor
- allow `SKIP` only when the child evidence is both valid and consistent
- otherwise emit a **contract-valid** `FLAGGED` row with explicit `follow_up`

This change alone should remove the current consensus auditability collapse.

### Change 6 - add a merge veto for the highest-risk contradiction patterns

For v2 contenders intended to be production-eligible, do not allow a final `MERGE` when:

- `risk_shared_surname_split` is present, or
- `risk_holdco_or_product_tier` is present, or
- `geo_country_conflict` is present

unless the packet also contains explicit official-domain continuity support.

This is not a score-math change. It is a contender-method change, and it directly targets the false-merge clusters that kept v1 unsafe.

## 3.3 Contender-lineup changes

### Recommended v2 lineup

Keep:
- `deterministic_control_v1` as shadow baseline only
- a Sonnet-based primary path
- one cheaper cross-family single-model path
- one rebuilt consensus path

Retire from the **minimum** v2 run:
- `haiku_single_v1`
- the old raw-output-based consensus logic

Recommended production-eligible contenders for v2:

1. `sonnet_guardrailed_v2`
   - same model family as the strongest v1 performer
   - add packet v2 + ref-safe prompt + merge veto

2. `gemini_guardrailed_v2`
   - cheaper cross-family comparison path
   - same packet/prompt/veto changes

3. `sonnet_gemini_consensus_v2`
   - built from normalized child rows
   - intended as the most plausible fallback candidate if it can keep flag rate under control

### Why not keep Haiku in the minimum v2 run

Haiku was not "almost there." It was too far from the fallback gate even before accounting for the contract bug:

- 44 soft misses
- 69 safe flags
- 0.7129 core flag rate

The next run should spend cycles on contenders with a realistic gate-clearing path.

### Backup contender if Gemini still looks too merge-happy in the proof subset

Use `gpt5mini_guardrailed_v2` instead of `gemini_guardrailed_v2`.

Reason:
- GPT-5.4-mini was not better than Gemini in v1 overall, but it did show slightly better auditability and may respond better to the stricter ref/citation prompt once the packet is fixed.

---

## 4. Recommended v2 run spec

## 4.1 Recommended path

### Artifact names

- packet spec/output: `evidence_packet_v2`
- canonical run name: `session7_first_real_bakeoff_v2`

### Step A - build packet v2 on the same 152 cases

Must-haves:

- same `benchmark_v1`
- same hidden-overlay discipline
- same scoring harness outputs
- real citeable `evidence_refs`
- official-domain retrieval populated or explicitly unresolved

### Step B - run a small proof subset before the full benchmark

Purpose:
- validate the packet/contract/consensus redesign before spending time on the full run

Suggested subset:
- all v1 false-merge cases from Sonnet
- all-vs-mostly missed-merge cluster cases
- at least 4 clean control cases

Target size:
- 20-30 cases

Stop criteria:
- any hidden-field leak
- any contender with `schema_valid_rate < 1.0` on the proof subset
- consensus still inheriting child ref breakage

### Step C - run the full 152-case v2 bakeoff

Minimum full lineup:
- `deterministic_control_v1` (shadow only)
- `sonnet_guardrailed_v2`
- `gemini_guardrailed_v2`
- `sonnet_gemini_consensus_v2`

Publish the same artifacts as v1, plus:
- a v1 vs v2 diff table for every contender
- a packet-completeness summary showing how many cases now have real official-domain retrieval

### Step D - only then decide on queue-building

Queue-building may start **only if** the full v2 run produces:

- one production-eligible contender, and
- one fallback-eligible contender

If the proof subset passes but the full 152-case run still fails the gates, queue-building remains blocked.

## 4.2 Backup option

If the recommended lineup fails at Step B because Gemini remains too unstable or too merge-happy even with the new packet/prompt/veto layer, swap:

- out: `gemini_guardrailed_v2`
- in: `gpt5mini_guardrailed_v2`

Do **not** reopen the packet design in that swap. The point of the backup option is to hold packet v2 constant and change only the cheaper single-model family.

---

## 5. Minimum next run before any queue-building work can start

The minimum run that can actually unblock execution is **not** the proof subset. The proof subset is only a contract check.

The minimum run that matters is:

1. packet v2 build on the same 152-case benchmark
2. proof subset pass
3. full `session7_first_real_bakeoff_v2` on all 152 cases

Anything less still leaves queue-building blocked by the frozen Session 4 rules.

---

## 6. Recommendation

Build a real v2, not a cosmetic rerun.

The smallest viable v2 is:

- packet v2 with citeable fact refs and real official-domain retrieval
- ref-safe adjudicator prompt
- normalized-child consensus
- explicit merge veto on the three highest-risk contradiction families
- narrowed contender set centered on Sonnet + one cheaper cross-family path + one rebuilt consensus path

That is the first v2 with a plausible path to producing both:

- one production path, and
- one fallback path

under the frozen benchmark and the frozen Session 4 gates.

---

## 7. Explicit status

**Queue-building remains blocked.**

v1 did not produce a safe production path or a safe fallback path, and the minimum acceptable next step is a full v2 bakeoff after the packet/runner redesign above.
