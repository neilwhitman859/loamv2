# Session 9.12 - high-level next steps viability review

Date: 2026-04-21
Inputs:
- `data/sprints/dedup/session9_11_full_method_bakeoff_rerun_if_approved.md`
- `data/sprints/dedup/session9_7_layered_safety_redesign.md`
- `data/sprints/dedup/bakeoff_v2/scored/session9_11_full_method_bakeoff_rerun_if_approved.json`
- `data/sprints/dedup/bakeoff_v2/scored/session9_11_full_method_bakeoff_rerun_if_approved.md`
- `docs/PRINCIPLES.md`
- `data/sprints/audit/findings/findings_business.md`

## Goal

Step back from the adjudication line and answer the project-level question that
Session 9.11 surfaced: if trustworthy producer dedup still is not production
ready after the broader method bakeoff, should Loam continue, freeze, pivot, or
shut down?

## Short answer

Recommendation: **freeze at the best non-production artifact and stop active
Sprint 6 build work.**

That means:

- keep `session9_7_layered_safety_sonnet_r2_narrow` as the best surviving
  producer-dedup artifact
- do **not** queue-build
- do **not** authorize another near-term redesign in the same method family
- do **not** treat "one more clever bakeoff" as the default next step

This is **not** the same thing as shutting Loam down immediately.

The evidence says the current producer-dedup continuation is spent. It does
**not** say the broader Loam asset is worthless. The right posture is freeze
first, then make an explicit founder-level project decision instead of drifting
into more technical iteration by inertia.

## What Session 9.11 proved

Session 9.11 answered the narrow adjudication question cleanly:

- the three broader-method survivors from Session 9.10 were real enough to
  recover recall
- none of them were strong enough to hold false-merge safety at full scale
- the Session 9.7 layered control remains the only fallback-passing artifact

Frozen scorecard:

| Artifact | False merge | Hard missed | Soft missed | Fallback gate |
|---|---:|---:|---:|---|
| `layered_safety_sonnet_r2_narrow_v1` | 0 | 6 | 3 | pass |
| `merge_proposer_plus_veto_v1` | 9 | 3 | 1 | fail |
| `expanded_layered_router_v1` | 5 | 4 | 1 | fail |
| `evidence_digest_then_judge_v1` | 5 | 5 | 1 | fail |

That result matters strategically because it closes the main escape hatch that
had been keeping Sprint 6 alive:

- Session 9.8 already showed there was no honest narrow continuation
- Session 9.10 showed broader methods could look promising on a bounded proof
- Session 9.11 showed those broader methods still fail the real benchmark

So the current question is no longer "which contender should we try next?" The
question is whether Loam is still worth pursuing when trustworthy producer dedup
is not yet credible at production scale.

## How central producer dedup is to Loam

Producer dedup is not a side quest.

It sits underneath several parts of Loam's stated product promise:

1. **Trustworthy identity.** Loam is supposed to be structured facts, not
   plausible prose. If the producer entity is wrong, then the wine page,
   producer page, provenance layer, and any future API all inherit that error.
2. **Connected exploration.** `docs/PRINCIPLES.md` makes the knowledge-graph
   behavior central: wine -> producer -> region -> appellation -> weather ->
   soil. Producer identity is one of the main graph anchors.
3. **Future downstream work.** Sprint 7 wine dedup and any execution-ready merge
   queue both assume the producer layer is trustworthy enough to build on.
4. **Commercial credibility.** A B2B/data-licensing or trade-facing story is
   much weaker if the producer identity layer cannot be trusted.

So if producer dedup is wrong, it is not a cosmetic defect. It directly breaks
the "organized facts rendered clearly" thesis.

## Why this does not force an immediate shutdown

Loam still has real assets outside the failed continuation:

- `33,225` active producers
- `224,316` active wines
- `125,934` vintages
- `3,662` appellations
- `40,193` price rows and `30,339` score rows
- `5,485` wine insights
- strong backbone identifiers (`LWIN`, `lwin_7`, `COLA`, `UPC`)
- geographic and weather structure that most consumer wine products do not have

There is also at least a small live signal that the product surface exists:

- `wine_lookups = 7`
- `14` active producers now carry any real metadata at all
- `8,495` wines have at least one price (`3.8%` of active wines)
- `7,586` wines have at least one score (`3.4%`)

That is not enough signal to justify blind continuation. But it **is** enough
to say the project still owns a non-trivial data asset and should not be killed
automatically just because this one adjudication line failed.

## Why continuing by default would be a mistake

The project-level case for caution is stronger than it was earlier in Sprint 6:

- producer dedup is central enough that "good enough for later" is not honest
- the current method class has already consumed the cheap, credible redesigns
- Session 9.11 failed after the exact kind of bounded proof-first discipline
  that was supposed to protect us from false hope
- the old business audit is still directionally true: Loam remains
  correctness-constrained and distribution-constrained more than
  cost-constrained

Put differently:

- a further rerun would not be "perseverance"
- it would be **sunk-cost drift**

## Realistic options from here

### Option A - Freeze at the best non-production artifact

Meaning:

- freeze the producer-dedup line at
  `session9_7_layered_safety_sonnet_r2_narrow`
- stop technical continuation inside Sprint 6
- keep the broader Loam project alive only if the next decision session finds a
  narrower, honest product posture that does not pretend producer dedup is
  solved

Pros:

- matches the evidence
- preserves the best artifact
- avoids more spend on a played-out line
- creates room for an explicit product/ICP decision

Cons:

- full producer-merge execution stays blocked
- Sprint 7 dependency assumptions need rethinking
- Loam loses the default "just keep building toward full corpus trust" story

### Option B - Authorize one clearly different future continuation

Meaning:

- not another packet tweak
- not another model bakeoff
- not another narrow rescue
- only a **new thesis** with a new success argument, such as:
  - a sharply narrower execution scope
  - a human-reviewed core subset
  - a different identity backbone / operational model
  - a different product scope that does not require full-corpus producer merges

Pros:

- keeps open the possibility that producer trust can still be earned later

Cons:

- there is **no evidence-backed design for this today**
- if authorized now, it would still be hope-first rather than evidence-first

### Option C - Shut down / shelve Loam

Meaning:

- stop active project investment
- preserve artifacts and data
- treat the current state as a finished exploration, not an active roadmap

Pros:

- cleanest response if trustworthy producer identity is judged non-negotiable
- prevents more founder time from disappearing into unresolved infrastructure

Cons:

- gives up a real data asset before an explicit narrower-scope decision has
  been made
- treats the current producer-dedup failure as proof that the whole project has
  no remaining wedge, which the evidence does not quite support

## Recommendation

Choose **Option A now**.

Freeze the producer-dedup adjudication line at the Session 9.7 artifact and
make the next move an explicit founder-level decision about Loam's scope, not a
technical continuation.

Why Option A is the best fit:

1. It is the most honest reading of Session 9.11.
2. It protects the project from sunk-cost drift.
3. It preserves the broader Loam asset while admitting that producer dedup is
   currently unresolved.
4. It leaves room for a future continuation only if that continuation is
   materially different enough to deserve a new thesis and a new gate.

## What should happen immediately after this memo

1. Freeze Sprint 6 operationally.
2. Do not queue-build.
3. Do not run new producer-dedup proofs/reruns without a new project thesis.
4. Hold one explicit decision session on Loam's post-Sprint-6 posture.

That next session should answer:

- Is Loam still worth pursuing as a narrower, truth-first data product while
  producer dedup remains frozen?
- Or does the failure to reach trustworthy producer dedup mean the project
  should be shelved?

## Exact user decision needed

Pick one of these three paths explicitly:

1. **Recommended:** freeze producer dedup at
   `session9_7_layered_safety_sonnet_r2_narrow`, keep Loam alive only under a
   narrower/honest scope review, and forbid more dedup build work until a new
   thesis exists.
2. Shelve Loam now and stop active work.
3. Reopen producer dedup later only if you are willing to treat it as a
   genuinely new strategy effort with a new scope, new success argument, and a
   fresh approval gate.
