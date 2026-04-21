# Session 9.9 - broader method bakeoff design

Date: 2026-04-21
Inputs:
- `data/sprints/dedup/session9_8_recover_production_from_layered_fallback.md`
- `data/sprints/dedup/session9_7_layered_safety_redesign.md`
- `data/sprints/dedup/session4_bakeoff_design.md`
- `data/sprints/dedup/rebuild_roadmap.md`
- `data/sprints/dedup/benchmark_v1.json`
- `data/sprints/dedup/bakeoff_v2/scored/session9_7_layered_safety_sonnet_r2_narrow.{json,md}`
- `data/sprints/dedup/bakeoff_v2/scored/session9_7_layered_safety_sonnet_r2_narrow_memo.md`
- `data/sprints/dedup/budget.json`

## Goal

Define the smallest broader redesign that is still honest about the Session 9.8
finding: the current path cannot reach production readiness through one more
narrow recall-only continuation.

## Decision summary

Yes, Sprint 6 should continue if the goal is still production readiness.

But the continuation should now be a **method bakeoff**, not another plain
model bakeoff and not another benchmark-shaped narrow rescue. The architecture
is the blocker.

Session 9.8 already established why:

- the remaining production blocker needs at least `5` more safe recoveries
- the `5` blind-core blockers span `5` family/signature shapes
- `4` of the `9` remaining misses sit outside the current routed-family bundle
- the repeating signatures are entangled with benchmark skip controls and prior
  false-merge traps

That means the next credible move must compare **different adjudication
architectures** on top of the fixed Session 9.7 safety base.

## What stays frozen

This continuation keeps the non-gameable parts fixed:

- `benchmark_v1`
- the Session 4 production and fallback gates
- the Session 9.7 layered safety base
- the deterministic anti-trap vetoes
- the out-of-scope rule against queue-building during redesign work

This continuation does **not** justify:

- a new model zoo
- benchmark mutation
- weakening the Session 9.7 safety vetoes
- a proof that special-cases current case ids

## Method classes to test

Use `session9_7_layered_safety_sonnet_r2_narrow` as the frozen control and
compare at most four broader method classes.

| Method | What changes | Why it is credible | Main safety risk |
|---|---|---|---|
| `expanded_layered_router_v1` | Keep the Session 9.7 safety stack, but extend positive-control routing beyond `11.4.h / 11.4.f / 11.4.n / 11.4.p` into the blocked shapes `11.1`, `11.4.g`, `11.4.b`, and `11.4.o` | Directly attacks the production-blocking misses that Session 9.8 proved sit outside the current router | Over-broad family additions could recreate old false merges under familiar labels |
| `signature_router_v1` | Route by packet-signature shape instead of rule-family label | Session 9.8 found the remaining misses collapse more cleanly by repeated packet signatures than by rule family | Signature buckets may still be too mixed if they are defined loosely |
| `merge_proposer_plus_veto_v1` | Add a broader positive proposer layer, then run the fixed deterministic vetoes and narrow skeptical review as hard backstops | Tests whether recall can be recovered safely when optimism and skepticism are separated into different stages | A too-aggressive proposer could overwhelm the veto layer and waste proof budget |
| `evidence_digest_then_judge_v1` | Keep the evidence sources fixed, but add a deterministic digest that summarizes official overlap, wine-list coherence, and contradiction flags before final adjudication | Tests whether the remaining misses are partly evidence-presentation failures instead of pure reasoning failures | Upside may be smaller than routing-based methods if the blocker is really missing positive control |

## Model policy

Keep models as constant as practical so the bakeoff isolates method choice.

- Do not duplicate every method across multiple models.
- A cross-model stage is allowed only when it is intrinsic to the method.
- Reuse the current reliable adjudication stack where possible instead of
  reopening model selection.

## Recommended bakeoff shape

### 1. Proof-first, not full-rerun-first

The next execution session should build the new contenders and run a bounded
proof subset before any full 152-case rerun.

### 2. Proof subset composition

Build the proof subset by construction:

- all `9` remaining misses from Session 9.7
- all `5` false merges from Session 9.6
- the signature-adjacent skip controls named in Session 9.8
- a small hold set of current Session 9.7 recall wins so we can detect
  regressions immediately

That keeps the proof honest: every method has to show upside on the actual
production blocker while still surviving the known trap zones.

### 3. Proof kill criteria

Any contender is eliminated immediately if it:

- introduces any false merge on the proof subset
- fails to recover at least `2` of the `5` production-blocking blind-core
  misses
- only improves by weakening the deterministic vetoes
- depends on case-specific patches instead of reusable routing/signature logic

### 4. Downselect before the full rerun

Run the full 152-case bakeoff only for proof survivors, with a hard cap of
three full contenders.

### 5. Frozen outcome labels

After the full rerun, classify the path cleanly:

- `production-ready` if a contender clears the frozen production gate
- `fallback-only` if a contender clears only the fallback gate
- `freeze` if no contender clears production and the survivors do not improve
  the go/no-go answer enough to justify more redesign

## Budget

Sprint 6 remaining ceiling after Session 9.8: `$130.49`.

Recommended continuation budget:

- Session 9.10 proof subset: target `$0-5`, hard cap `$8`
- full 152-case rerun only if proof survivors exist: target `$2-10`, hard cap
  `$15`
- combined continuation target: `$3-15`, hard cap `$25`

That keeps the broader redesign comfortably inside the remaining Sprint 6
budget while still allowing a real architectural comparison.

## Recommendation

The honest continuation is now:

- a **broader method-class bakeoff**
- built on the fixed Session 9.7 safety base
- proof-first
- budget-bounded
- explicitly aimed at production readiness rather than preserving a
  fallback-only artifact

What not to do next:

- do not run another plain model bakeoff on the same method
- do not mutate `benchmark_v1`
- do not queue-build
- do not reopen the deterministic anti-trap vetoes

## Stop rule

If no new method survives the proof subset without false merges, freeze at
`session9_7_layered_safety_sonnet_r2_narrow` and stop the adjudication rebuild.
