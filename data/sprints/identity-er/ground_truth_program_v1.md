# Sprint 7 Ground Truth Program v1

**Status:** active  
**Opened:** 2026-04-22  
**Purpose:** make producer-identity progress measurable and loopable against a frozen truth set instead of against whatever the last promising method happened to clear.

## Recommendation

Yes, Loam likely needs roughly **1,000 scoreable producer pairs** with durable ground truth.

But the useful unit is **not** "1,000 random pairs." The useful unit is:

- a **provenance-backed** pair ledger
- normalized to the Sprint 7 label contract:
  - `SAME_AS`
  - `RELATED_BUT_DISTINCT`
  - `NONE`
- split into a **development pool** and a **frozen blind holdout**
- expanded in **audited packs**, not by ad hoc one-off fixes

If we skip those constraints, the number `1,000` sounds rigorous while still letting us fool ourselves.

## Current baseline

Sprint 7 already has several truth-like artifacts, but they live in different shapes:

- `ground_truth_seed_pairs_v1.jsonl`
  - `422` validated pair records carried forward from Sprint 6's final execution ledger
  - `417` of those are scoreable today
  - label mix at seed time:
    - `96` `SAME_AS`
    - `46` `RELATED_BUT_DISTINCT`
    - `275` `NONE`
    - `5` `DEFERRED`
- `ground_truth_seed_singletons_v1.jsonl`
  - `71` singleton sanity records
  - useful for producer-card correctness
  - **not counted** toward the pair target
- `benchmark_v1`
  - frozen `152`-case pair holdout
  - merge-only subset of the seed ledger
- `selector_proof_v1`
  - `56` selector/escalation truth cases
  - useful for shortlist/selector evaluation
  - **not a substitute** for pairwise ground truth

## Why the current baseline is not enough

The current seed is strong enough to stop us from drifting, but not strong enough to trust broad claims:

- it is still **France-heavy** (`230 / 422` pair rows, or about `55%`)
- it underweights `RELATED_BUT_DISTINCT`
- it comes mostly from the old pairwise path
- it is strong on adjudication and failure families, but still thin on broad coverage

The live DB also contains `600,103` rows in `producer_dedup_pairs`, but those are **not** ground truth. The table currently has no populated `verdict_source`, so prior machine verdicts cannot honestly be treated as labels.

## Program rules

1. **Only provenance-backed labels count.**
   Every new truth record must cite the source artifact or evidence block that justifies the label.
2. **Only scoreable pair labels count toward 1,000.**
   `SAME_AS`, `RELATED_BUT_DISTINCT`, and `NONE` count.
   `DEFERRED`, singleton sanity checks, and unresolved disputes do not.
3. **The blind holdout stays frozen.**
   If a case is wrong, repair it explicitly; do not silently rotate it out after a failed run.
4. **Every loop reports the same cuts.**
   Minimum reporting:
   - overall
   - by label
   - by tier
   - by pattern family
   - by country bucket
5. **Truth disputes are first-class output.**
   If a case turns out historically messy, quarantine it. Do not force it into the scoreable pool just to hit a round number.

## The 1,000-pair target

The target should be **1,000 scoreable pairs**, plus a separate quarantine/singleton annex.

Recommended scoreable mix:

- `300` `SAME_AS`
- `200` `RELATED_BUT_DISTINCT`
- `500` `NONE`

Recommended tier balance:

- `340` core
- `330` mid
- `330` tail

Recommended country-balance rule:

- expansion should overweight non-`FR` until France is below **45%** of the scoreable pair corpus

Why this shape:

- `NONE` should remain the plurality because false merges are the real cost
- `RELATED_BUT_DISTINCT` must be much larger than it is now because that is the label family Sprint 7 most obviously underlearned
- tier balance should stay roughly even so we do not overfit to the high-visibility core alone

## Gap from today

Starting from the current scoreable seed (`417`), the path to `1,000` needs about **`+583` new scoreable pairs**.

By label, the gap is:

- `SAME_AS`: `+204`
- `RELATED_BUT_DISTINCT`: `+154`
- `NONE`: `+225`

That means the next packs should deliberately **overweight merges and related-but-distinct cases**, not just collect more easy skips.

## Loop design

Use the ground-truth program in two layers:

### Development pool

- open for audited expansion until it reaches roughly `800` scoreable pairs
- used for day-to-day iteration and failure-family analysis

### Frozen blind holdout

- `200` scoreable pairs
- start from today's `152` frozen benchmark cases
- expand later with `48` additional audited cases only after the ground-truth process itself is stable

Loop protocol:

1. add one audited pack
2. rerun the candidate method on the full development pool
3. inspect failures by label/family/tier/country
4. iterate only if the development result improves without creating a new truth dispute
5. checkpoint on the frozen holdout, not on the development pool

## Proposed acquisition packs

Four audited `150`-pair packs is the cleanest path. If a few cases get quarantined as disputed, we still clear `1,000` with a small buffer.

### Pack 001

- benchmark truth repair and adjacent analogues
- underrepresented `RELATED_BUT_DISTINCT` families:
  - `11.4.g`
  - `11.4.j`
  - `11.4.m`
  - `11.4.o`
  - `11.4.s`
- first non-`FR` precision traps

### Pack 002

- `SAME_AS` recall families:
  - historical continuity
  - legal-vs-label identity
  - importer-prefix / merchant-prefix absorption
  - multi-country same-brand continuity

### Pack 003

- non-`FR` `NONE` traps from `US`, `IT`, `ES`, `PT`, `AU`, and cross-country pairs
- high-similarity false-merge risk cases from the unlabeled pool

### Pack 004

- tail/global balance
- label-gap fill
- final holdout promotion candidates

## What counts as honest progress

The next loop should be considered successful if it produces all of:

- a reproducible seed manifest
- a frozen scoring contract
- an audited first expansion pack
- method reports that run against the **same** truth files each time

That is much more valuable than another apparently-winning method run against a benchmark we still do not trust enough.
