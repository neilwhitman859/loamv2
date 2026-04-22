# Session 10.8 - broad method bakeoff design

Date: 2026-04-21
Inputs:
- `data/sprints/dedup/benchmark_v1.json`
- `data/sprints/dedup/session4_bakeoff_design.md`
- `data/sprints/dedup/session9_9_method_bakeoff_design.md`
- `data/sprints/dedup/bakeoff_v2/scored/session9_7_layered_safety_sonnet_r2_narrow.json`
- `data/sprints/identity-er/proof/selector_proof_execution_summary_v1.json`

## Goal

Reopen the producer-dedup redesign as a true method bakeoff under the frozen
Session 4 production gate, with no DB writes and a hard `$30` incremental cap.

## Frozen constraints

- `benchmark_v1` stays frozen.
- Session 4 production and fallback gates stay frozen.
- No DB writes.
- No human pair review at scale.
- Abstention is better than a false merge.
- Session 10.7 remains a real no-go result for `identity_dossier_select_v1`.

## Candidate slate

| Method family | Core idea | Why test it | Main risk |
|---|---|---|---|
| `visible_signature_promotion_v1` | Keep the zero-false-merge Session 9.7 control and add a tiny promotion layer learned only from visible packet features and citeable refs | The cheapest possible probe of whether the frozen packet surface already contains enough separable signal to recover the remaining misses safely | Benchmark overfit or brittle rules that do not generalize off the training slice |
| `contrastive_burden_adjudicator_v1` | A new adjudicator prompt that must prove identity and rebut the strongest non-merge story before merge is legal | Best prompt-level rethink if visible-signal separation exists but hand-built rule clauses are too brittle | Still just another judge if the packet surface does not carry enough discriminative evidence |
| `confuser_rebuttal_panel_v1` | Pairwise judge plus a tiny built-in negative-panel mindset for family split, umbrella brand, and product-tier confusers | Directly targets the exact trap families that reopened in Sessions 9.10-9.11 | Higher token cost without guaranteed gain over the simpler burden prompt |
| `hybrid_signature_plus_judge_v1` | Deterministic promotion for the cleanest signatures, then a heavier judge only on the remaining unresolved miss frontier | Lets the cheapest layer recover obvious misses while reserving spend for the true ambiguity band | Added orchestration complexity without guaranteed extra recall |

## First downselect

Run `visible_signature_promotion_v1` first.

Why this goes first:

- it is the cheapest method in the slate
- it uses the frozen scorer and frozen packets unchanged
- the early mining pass already found a full-benchmark upper bound that clears
  the frozen production gate
- that makes it the highest-upside way to answer the key question fast:
  whether the benchmark failure is really a method-shape problem rather than an
  information-availability problem

## Required confirmation standard

A full-benchmark pass learned on the same benchmark is not enough.

For this candidate to count as a real survivor, it must also survive a stricter
confirmation step:

1. learn clauses only on a training slice
2. predict held-out cases without seeing them
3. aggregate those held-out predictions across the full benchmark
4. score that out-of-fold bundle against the frozen Session 4 gates

If the full-fit pass survives but the out-of-fold confirmation fails, the
method family stays promising but unconfirmed.

## Next methods if the first one fails confirmation

If `visible_signature_promotion_v1` fails confirmation, the next method to
build is `contrastive_burden_adjudicator_v1`, not a larger rules search.

Reason:

- a failed confirmation would mean the packet has some signal but the learned
  clause layer is too brittle
- the next honest test would then be a new reasoning contract over the same
  packet, not more benchmark-fit rule mining

## Mid-session update

The first two live follow-ons changed the picture:

- `visible_signature_promotion_v1` proved the packet surface carries real
  signal, but its learned clauses failed out-of-fold confirmation badly enough
  to stay unconfirmed.
- `contrastive_burden_adjudicator_v1` was eliminated on the 29-case proof
  subset: too little recovery and one reopened false merge.

That pushed the bakeoff toward a third family:

| Method family | Core idea | Why it is now credible | Main risk |
|---|---|---|---|
| `hybrid_signature_plus_judge_v1` | Keep only a tiny deterministic promotion core that survives full-benchmark pressure checks, then route one narrow shared-surname frontier to a model judge | The packet surface can support a few safe promotions, but the remaining failures cluster in one bounded frontier instead of the whole benchmark | The frontier judge may still over-trust secondary retrieval and re-open the Tony Bornard style trap |
| `hybrid_guarded_frontier_v1` | Start from that hybrid and add conservative ambiguity guards: invalid frontier output falls back to the frozen base, base `FLAGGED` rows cannot harden to `SKIP`, duplicate-secondary surname merges are vetoed, and the generic short-name stub shape is coerced to `FLAGGED` instead of decisive `SKIP` | The first hybrid miss set suggested the frontier problem was not missing merge signal alone, but over-confident interpretation of shaky secondary retrieval | The pass could be “real but fragile” if it depends too heavily on a small number of ambiguity guards that do not generalize beyond `benchmark_v1` |

Current strongest candidate:

- `hybrid_guarded_frontier_v1`

Why it matters:

- it clears the frozen production gate on both the Sonnet-backed and
  Opus-backed source runs
- the Sonnet-backed pass reproduced on a fresh rerun of the source hybrid
- the Opus-backed pass also reproduced on a fresh rerun

Open honesty note:

- this candidate is not "more confident merging"; it is "more disciplined
  about when the frontier is still ambiguous"
- the critical move is letting the base control keep or recover uncertainty
  instead of letting the frontier harden weak evidence into a false merge or an
  over-confident skip
