# Producer Dedup Session Playbook

This is the operating guide for moving through the merge-only producer dedup rebuild without drifting.

## Recommendation

Do **not** try to do this as one long rolling session.

Default cadence:
- **Build/audit sessions:** 60-120 minutes
- **Interactive review sessions:** 30-60 minutes
- **Closeout/decision sessions:** 20-40 minutes

Long sessions are still fine for one narrow task, but not for mixing strategy, code, audit, and adjudication. Quality drops when those get blended together.

## What Makes a Good Session

Each session should have:
- one primary deliverable
- one explicit stop condition
- one written handoff to the next session

If we cannot name those three things at the start, the scope is too loose.

## Session Types

### Type A - Build

Purpose:
- create or change code/specs/docs needed for the next phase

Good outputs:
- strategy audit
- benchmark file
- evidence-packet spec
- bakeoff harness

Bad pattern:
- building and immediately trying to solve the whole dedup problem in the same sitting

### Type B - Audit

Purpose:
- inspect an existing artifact and decide what survives, what changes, and what gets discarded

Good outputs:
- keep/tune/drop table
- failure-mode list
- sample-quality report

### Type C - Review

Purpose:
- make a small number of high-leverage decisions

Good outputs:
- approval of a benchmark definition
- approval of evidence-packet fields
- approval of bakeoff contenders

Keep these small. If a review session starts generating code changes, spin that into the next build session.

### Type D - Closeout

Purpose:
- summarize what changed and lock the next target

Good outputs:
- updated checklist
- next-session prompt
- note on what is blocked and what is not

## Proposed Session Sequence

### Session 1 - Pair Corpus Audit

Primary deliverable:
- a keep/tune/drop report for the current blocking strategies

Scope:
- inspect the current candidate generators
- compare them against known false-merge and missed-merge clusters
- identify which strategies are:
  - worth keeping
  - need retuning
  - should be removed from the rebuild path

Stop condition:
- every current strategy has a disposition and rationale

From your end:
- mostly hands-off
- review the final keep/tune/drop table
- answer only if a strategy has a real tradeoff that needs your product judgment

### Session 2 - Benchmark Freeze

Primary deliverable:
- a frozen benchmark set for the bakeoff

Scope:
- combine the 100-pair blind core audit
- add known false-merge patterns
- add known missed-merge patterns
- add a clean tail sample
- define strata clearly

Stop condition:
- the benchmark file exists and we agree not to keep mutating it mid-bakeoff

From your end:
- sanity-check that the benchmark reflects the real product risk

### Session 3 - Evidence Packet Design

Primary deliverable:
- the standard evidence-packet schema

Scope:
- define exactly what an adjudicator sees per pair
- include contradiction flags and survivor recommendation fields
- keep it compact enough to audit quickly

Stop condition:
- we have a stable packet format and one worked example

From your end:
- confirm it feels easy to reason from

### Session 4 - Bakeoff Design

Primary deliverable:
- the bakeoff plan: methods, models, and metrics

Scope:
- decide what gets compared
- decide what "wins"
- decide what cost/performance tradeoff is acceptable

Stop condition:
- we can run the bakeoff without changing the rules halfway through

From your end:
- approve the contenders and the pass/fail metrics

### Session 5 - Candidate Rebuild v1

Primary deliverable:
- rebuilt candidate-generation output plus comparison to old corpus

Scope:
- implement or query the revised pair builder
- compare coverage and noise vs old corpus

Stop condition:
- we know whether the rebuilt corpus is actually better

From your end:
- review the comparison summary, not the raw pairs

### Session 6 - Bakeoff Run and Readout

Primary deliverable:
- measured winner + fallback

Scope:
- run evaluation on the frozen benchmark
- compare false merges, missed merges, survivor correctness, cost

Stop condition:
- one production path and one fallback are selected

From your end:
- approve the winner unless a tradeoff feels wrong for Loam

### Session 7 - Core Queue Build

Primary deliverable:
- core merge queue
- core skip queue
- core flagged queue

Stop condition:
- core is adjudicated tightly enough for scorecarding

From your end:
- this is the best place for any limited human attention

### Session 8 - Tail Queue Build

Primary deliverable:
- tail merge/skip/flagged queues

Stop condition:
- tail meets the intended conservative posture

From your end:
- light-touch review only

### Session 9 - Pre-Execution Scorecard

Primary deliverable:
- go/no-go-quality report for execution readiness

Stop condition:
- we can say exactly what remains risky and why

From your end:
- decide whether to execute, defer, or narrow scope

## How To Stay Focused

At the **start** of each session, write these three lines:

1. `Deliverable:` what artifact will exist by the end
2. `Not doing:` what is explicitly out of scope this session
3. `Stop when:` the exact condition that ends the session

At the **end** of each session, write these four lines:

1. `Done:` what changed
2. `Open:` what is still unresolved
3. `Next:` the single next session target
4. `User review needed:` yes/no, and for what

If the next session is already clear, add:

5. `Next session prompt:` a ready-to-run prompt file path plus the exact prompt text you hand to the user

## What You Should Do From Your End

Your role should be light but decisive:
- keep us honest on product risk
- answer narrow judgment calls
- review summaries, not raw dumps
- avoid re-opening already-settled scope unless the evidence changes

The main thing to avoid is turning every session into "maybe we should rethink everything." The place for that is only at phase boundaries.

## Practical Rule Of Thumb

If a session starts producing more than one major artifact, split it.

Examples:
- pair-corpus audit + benchmark freeze = split
- evidence-packet design + bakeoff run = split
- core queue build + tail queue build = split

That keeps momentum high and makes it much easier for you to know whether a session actually succeeded.
