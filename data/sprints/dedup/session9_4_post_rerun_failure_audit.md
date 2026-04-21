# Session 9.4 - post-rerun failure audit

Date: 2026-04-21
Source run: `data/sprints/dedup/bakeoff_v2/scored/session9_3_full_rerun_if_approved.{json,md}`

## Goal

Audit the fresh Session 9.3 full rerun and answer one narrow question:

- is there still one small, evidence-backed redesign with a credible chance to
  clear the frozen Session 4 gates without reopening the continuity-driven
  false-merge problem?

## Short answer

No.

The current failure is **mainly soft missed merges / over-flagging**, not a
single residual hard-miss bug. The misses are concentrated in a few rule
families, but they are **not cleanly separable from the exact packet
signatures that the new backstop needed in order to kill unsafe false merges**.

Recommendation: treat the current adjudication path as a **non-execution-ready
benchmark artifact**, freeze queue-building, and do not spend another rerun on
one more narrow packet-side tweak.

## Method

Reviewed:

- scored summaries and gate memo for `session9_3_full_rerun_if_approved`
- normalized outputs for `sonnet_guardrailed_v2`,
  `gemini_guardrailed_v2`, and `sonnet_gemini_consensus_v2`
- full stored `v2.1` packets in
  `data/sprints/dedup/bakeoff_v2/packets/benchmark_v1_packets_full_v2.jsonl`
- delta versus the earlier canonical v2 full run
  `session7_first_real_bakeoff_v2`
- packet and harness logic in:
  `pipeline/identity/bakeoff_packet_v2.py`,
  `pipeline/identity/bakeoff_harness_v2.py`,
  `pipeline/identity/bakeoff_run_v2.py`

## Findings

### 1. The post-backstop problem is mostly soft missed merges and queue burden

Session 9.3 already proved the safety-side win:

- `gemini_guardrailed_v2`: `0` false merges, `0` blind-core false merges
- `sonnet_gemini_consensus_v2`: `0` false merges, `0` blind-core false merges
- `sonnet_guardrailed_v2`: down to `1` false merge total

What still fails is recall plus flag rate:

- `sonnet_guardrailed_v2`: `5` hard missed + `44` soft missed, `40.13%` flag rate
- `gemini_guardrailed_v2`: `9` hard missed + `42` soft missed, `38.82%` flag rate
- `sonnet_gemini_consensus_v2`: `4` hard missed + `47` soft missed, `48.68%` flag rate

So the blocker is not "the old dangerous false-merge cluster survived." It is
"too many true merges now die as `FLAGGED` or `SKIP`."

### 2. The misses are concentrated, but not in one safely-fixable bucket

There are `51` expected-`MERGE` benchmark cases. On Session 9.3:

- `49` were missed by **all three** contenders
- only `2` were merged by any contender at all
- `39` cases were `MERGE` for both single models in Session 7, then became
  non-`MERGE` for both single models in Session 9.3

Dominant all-three-miss clusters:

- `27` cases: `11.4.h` accent / orthographic / short-full-form variants
- `8` cases: `11.4.f` generational succession / historical name forms
- `7` cases: `11.4.n` global brands with multi-country sourcing
- `3` cases: `11.4.p` merchant / curation prefixes

This is concentrated enough to diagnose, but not concentrated enough to call
it "one packet bug."

### 3. `risk_sparse_official_evidence` is the dominant packet-side pressure point

Across merge misses:

- `risk_sparse_official_evidence` appears in `36` merge misses for **every**
  contender
- Sonnet's merge-veto guardrail actually fired on `38` of its `49` merge misses
- Gemini still missed `35` merge cases even after the packet changes, despite
  producing `0` false merges

That makes the central mechanism clear:

- the v2.1 packet now refuses to let weak continuity claims carry a merge
- without a `hard_official_continuity_*` ref, models mostly degrade to
  `FLAGGED` or `SKIP`

### 4. The obvious "relax sparse official evidence" fix is not safe

The tempting move is:

- recover the low-risk-looking cases where the packet has
  `risk_sparse_official_evidence` but no explicit high-risk contradiction flag

That bucket does contain real recall losses such as:

- `De Stefani <-> Stefani`
- `Vocoret <-> Vocoret et Fils`
- `Edouard Delaunay <-> Edouard`
- `Protheau & Fils <-> Jean-Francois Protheau`
- `Rust en Vrede <-> Rust Verde`

But the same sparse-only signature also contains previously fixed false merges,
including:

- `Baron Philippe de Rothschild <-> Mouton Baronne Philippe`
- `Lafite Rothschild <-> Barons Rothschild Lafite Reserve Speciale Pauillac`
- `Decelle Villa <-> Decelle & Fils`
- `Ginglinger-Fix <-> Ginglinger`
- `Confuron Gindre <-> Edouard Confuron`
- `Gauffroy Marc & Fils <-> Gauffroy-Jacob`

That overlap is the key audit result:

- the rescue bucket is **not** disjoint from the safety bucket
- one more narrow relaxation would likely buy recall by reopening the exact
  kinds of false merges that Session 9.1-9.3 were built to kill

### 5. The country-conflict / global-brand bucket is real, but too small

The `11.4.n` global-brand multi-country cases are also visible:

- `Selaks`
- `Tussock Jumper`
- `Cupcake Vineyards`
- `Prophecy`
- `Thomson & Scott`

In theory, a stronger official-domain continuity resolver could recover some of
these cleanly.

But this is not enough to save the current path:

- even perfect recovery of the no-explicit-risk bucket plus the
  country-conflict/global-brand bucket would still leave **17 blind-core
  misses** for every serious contender
- the frozen production gate needs blind-core `hard_missed_merge = 0` and
  blind-core `soft_missed_merge <= 1`

So even the most generous plausible narrow rescue still fails the gate by a
wide margin.

### 6. The remaining blind-core misses are the hardest kind

After granting those best-case recoveries, the blind-core residue is still
dominated by cases like:

- `Baron de Rothschild <-> Barons de Rothschild`
- `Guy Castagnier <-> Castagnier`
- `Robert Mondavi <-> Mondavi`
- `Amiot Bonfils <-> Guy Amiot et Fils`
- `Gassier <-> Michel & Tina Gassier`
- `Bart <-> Andre Bart`
- `Francois Carillon <-> Jacques et Francois Carillon`
- `Comte Senard <-> Daniel Senard`
- `Florent Rouve <-> Jean Rijckaert`

These are not "missing one extra ref id" cases. They are the exact family-
split / succession / shorthand / qualifier cases where the benchmark wants
high recall but the new safety posture correctly refuses to trust thin
continuity evidence.

## Answers to the session questions

### 1. Are the remaining misses/flags dominated by one or two packet-side patterns, or broadly distributed?

They are concentrated in a handful of rule families, but the underlying packet
failure shape is broader than one bug:

- sparse or unresolved official continuity
- orthographic / short-full-form families
- succession / family-split ambiguity
- global-brand country splits

So: concentrated enough to describe, **not** concentrated enough for one safe
micro-fix.

### 2. Is the post-backstop problem mainly hard missed merges, soft missed merges / over-flagging, or both?

Mainly **soft missed merges / over-flagging**.

Hard misses remain, but the big number is the soft-miss / queue-burden wall.

### 3. Is there one minimal redesign with a credible chance of improving recall while preserving the new false-merge safety?

No credible one-step redesign emerged from this audit.

The closest candidate would be "relax sparse-official-evidence handling for
apparently low-risk same-country orthographic-variant packets," but the audit
shows that bucket is entangled with multiple previously fixed false merges.

### 4. If not, should the current adjudication path be treated as a non-execution-ready artifact and paused?

Yes.

That is the recommended decision.

## Recommendation

Freeze the current adjudication path as a **non-execution-ready rebuild
artifact**.

Do:

- preserve `session9_3_full_rerun_if_approved` and this memo as the benchmarked
  endpoint of the current merge-only adjudicator path
- keep queue-building blocked
- require an explicit user decision before any further Sprint 6 spend

Do not:

- run another narrow rerun
- relax the merge veto just because it recovers some obvious misses
- treat Session 9.3 as "almost there"

The audit says it is **not** almost there under the frozen gates.

## What a future continuation would require

If Sprint 6 continues, it likely needs a **larger redesign**, for example:

- a different decomposition by pattern family instead of one general
  adjudicator
- stronger deterministic same-brand / same-portfolio evidence synthesis that is
  explicitly benchmarked against the already-fixed false-merge set
- or a re-think of the benchmark / gate shape itself

Those are higher-level design choices, not one more packet-side tweak.
