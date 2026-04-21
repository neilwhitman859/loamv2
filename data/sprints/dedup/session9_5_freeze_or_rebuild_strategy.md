# Session 9.5 - freeze or rebuild strategy

Date: 2026-04-21
Inputs:
- `data/sprints/dedup/session9_4_post_rerun_failure_audit.md`
- `data/sprints/dedup/session4_bakeoff_design.md`
- `data/sprints/dedup/rebuild_roadmap.md`
- `data/sprints/dedup/bakeoff_v2/scored/session9_3_full_rerun_if_approved.{json,md}`
- `docs/DECISIONS.md` 2026-04-21 exploration-cap entry

## Goal

Choose one materially different next redesign family, if any, that is worth a
bounded proof on the frozen 152-case benchmark before any all-pairs scale-up.

## Short answer

If Sprint 6 continues, the most justified next redesign family is:

- **pattern-family specialists layered on top of the safest current base path**

Recommendation:

- keep `benchmark_v1` and the frozen Session 4 gates unchanged
- keep queue-building blocked
- do **not** spend on another general packet-side relaxation
- do **not** spend on benchmark/gate redesign first
- if the user wants one last exploration step, run a **single routed
  specialist proof** under the `$20` cap

The safest current base is still `gemini_guardrailed_v2` because it reached:

- `0` false merges overall
- `0` blind-core false merges
- perfect auditability

Its problem is not safety. Its problem is that it merged **none** of the `51`
true-merge benchmark cases.

## Why pattern-family specialists are the best next family

### 1. The remaining misses are concentrated in a few rule families

Session 9.3 plus the Session 9.4 audit show:

- `49 / 51` expected-`MERGE` cases were missed by all three serious contenders
- those `49` all-three misses cluster into:
  - `11.4.h` orthographic / short-full-form variants: `27`
  - `11.4.f` generational succession / historical name forms: `8`
  - `11.4.n` global multi-country brands: `7`
  - `11.4.p` merchant / curation prefixes: `3`
  - everything else combined: `4`

Those same four families cover:

- `47 / 51` total expected-`MERGE` cases in the benchmark
- `26` expected-`SKIP` cases
- **all `16` known false-merge pattern cases** (`12` in `11.4.h`, `4` in
  `11.4.f`)

That is exactly the shape where a family-routed design makes sense:

- high missed-merge concentration
- direct overlap between the recall cases and the safety cases
- one general adjudicator is currently treating too many distinct problems as
  one sparse-official-evidence problem

### 2. This is materially different from the failed narrow-fix path

The failed Session 9.1-9.4 line was:

- one general packet
- one general adjudicator per contender
- one global continuity / merge-veto posture
- repeated global tightening and backstops

The recommended redesign is different in structure, not just in strictness:

- keep a conservative base adjudicator
- add a **deterministic family router**
- send only routed families to **family-specific specialists**
- let those specialists use family-specific merge requirements
- allow overrides only inside those routed families

That changes the decomposition of the problem rather than making one more
global packet-side tweak.

### 3. The other two candidate families are weaker next bets

#### Stronger deterministic evidence synthesis

This is promising as an ingredient, but weaker as the **next** redesign family.

Why:

- it still has to answer different questions for different failure families
- the same sparse-evidence signature currently contains both:
  - real missed merges like `De Stefani <-> Stefani`
  - previously fixed false merges like `Baron Philippe de Rothschild <-> Mouton Baronne Philippe`
- a stronger general synthesis layer would still risk becoming another broad
  packet-side relaxation unless it is scoped by family

Conclusion:

- deterministic synthesis should support the specialists
- it should not be the first standalone redesign family to test

#### Benchmark / gate redesign

This is the weakest next step.

Why:

- Session 9.5 explicitly keeps `benchmark_v1` and the Session 4 hard gates
  frozen
- the current misses are not benchmark noise or auditability noise
- changing the gate now would mostly move the finish line rather than prove the
  adjudicator is safer and sharper

Conclusion:

- not recommended as the next Sprint 6 move

## Pattern-family specialists over a safe base

Recommended shape:

1. Use `gemini_guardrailed_v2` as the default base verdict.
2. Add a deterministic router that triggers only on four benchmark-relevant
   families:
   - `11.4.h` orthographic / short-full-form specialist
   - `11.4.f` generational / historical-form specialist
   - `11.4.n` global multi-country brand specialist
   - `11.4.p` merchant / curation-prefix specialist
3. Each specialist should use a tighter family-specific rubric rather than the
   current one-size-fits-all sparse-official-evidence posture.
4. Specialists may upgrade `FLAGGED` or `SKIP` to `MERGE` only when the
   required family-specific evidence is present.
5. Outside those routed families, the base verdict stands.

Why this is the right order:

- it preserves the current false-merge safety base
- it targets the families where recall collapsed
- it tests whether recovery is possible **without** reopening the exact
  skip-families the benchmark uses as safety traps

## Smallest proof worth running

The smallest proof that genuinely de-risks this redesign is:

- a **73-case routed specialist bundle**, scored back against the full frozen
  152-case benchmark

### Proof scope

Route only the benchmark cases in:

- `11.4.h` (`46` cases total: `28` merge, `18` skip)
- `11.4.f` (`13` cases total: `8` merge, `5` skip)
- `11.4.n` (`7` cases total: `7` merge, `0` skip)
- `11.4.p` (`7` cases total: `4` merge, `3` skip)

Combined routed proof bundle:

- `73` total cases
- `47` expected merges
- `26` expected skips
- includes **all `16` known false-merge benchmark cases**

Keep the remaining `79` benchmark cases on the existing safe base path and
score the composite result across all `152`.

### Why this is the minimum honest proof

Anything smaller would be misleading:

- testing only merge-positive examples would not prove the specialists stay off
  the old false-merge landmines
- testing only one family would not answer whether the current recall wall is
  broad enough to justify the redesign
- rerunning the full generic contender set would be unnecessary spend because
  the point is to test the **family-routed** idea, not re-measure the already
  failed general path

## Proof success bar

The routed-specialist proof should count as credible only if all of these hold:

1. `0` false merges overall on the composite 152-case scorecard
2. `0` blind-core false merges
3. blind-core missed merges fall from `30` to **`<=5`**
4. the specialist bundle captures at least **`30 / 47`** of its targeted
   expected merges
5. full-benchmark `flag_rate_total` falls to **`<=0.25`**

If the proof misses any of those bars, the honest move is:

- freeze the adjudication path
- do not spend on an all-pairs version

## Spend estimate

The repo already contains measured cost evidence for the cheapest plausible
proof shape:

- B6.5a `Haiku + Serper` ran at about **`$0.006/pair`** on 3,403 pairs

At that measured rate:

- one full 73-case specialist pass is about **`$0.44`**
- two to three proof passes plus a verification pass still land comfortably
  below **`$2`** in direct model/search spend

Conservative Session 9.6 budgeting:

- reserve **`$5-10` actual API spend**
- hard stop remains the user's **`$20` exploration cap**

So yes: this proof can be done safely inside the cap.

## Exact approval needed before any all-pairs scale-up

The next approval should be explicit and narrow:

- **Approve one Session 9.6 routed-specialist proof on the frozen 152-case benchmark, capped at `$20` external spend.**

And equally important:

- that approval is **not** approval for all-pairs scale-up

Before any all-pairs run, the user should review a proof memo showing:

- the composite 152-case scorecard
- whether false merges stayed at zero
- how much blind-core recall actually recovered
- whether the routed design looks strong enough to justify a larger build

No auto-scale rule:

- even a successful Session 9.6 proof should come back for explicit user
  approval before any all-pairs implementation or queue-building work starts

## Recommendation

If the user wants one final bounded exploration step, do this:

- run **one** routed pattern-specialist proof on the four dominant families
- keep the cap at `$20`
- keep queue-building blocked
- stop immediately after the proof memo

If the user does **not** want another exploration step, do this instead:

- freeze the current adjudication path as a non-execution-ready benchmark
  artifact

## Bottom line

The current path should still be treated as **not execution-ready**.

But unlike Session 9.4, there is now one bounded continuation that is both:

- materially different from the failed narrow-fix line
- cheap enough to justify under the new exploration cap

That continuation is **pattern-family specialists**, not one more global packet
relaxation and not a gate rewrite.
