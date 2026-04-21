# Session 9.7 - layered safety redesign after the failed specialist proof

Date: 2026-04-21
Inputs:
- `data/sprints/dedup/bakeoff_v2/scored/session9_6_pattern_specialist_proof_if_approved.{json,md}`
- `data/sprints/dedup/bakeoff_v2/scored/session9_6_pattern_specialist_proof_if_approved_memo.md`
- `data/sprints/dedup/session9_4_post_rerun_failure_audit.md`
- `data/sprints/dedup/session9_5_freeze_or_rebuild_strategy.md`
- `pipeline/identity/bakeoff_pattern_specialist_proof.py`

## Goal

Test whether a broader redesign can keep the Session 9.6 recall gains while
removing the false-merge regressions that caused the bounded specialist proof
to fail.

## Short answer

Yes, but only to a fallback-quality posture.

Session 9.7 found a materially better architecture than the raw Session 9.6
specialist composite:

- keep the routed Session 9.6 specialist as the recall engine
- add deterministic anti-trap vetoes for the obvious false-merge signatures
- add a very narrow skeptical safety gate only for the remaining one-anchor
  `11.4.f` continuity traps

Best result:
- `session9_7_layered_safety_sonnet_r2_narrow`
- `0` false merges overall
- `0` blind-core false merges
- blind-core missed merges stayed at `5`
- targeted routed merge recoveries stayed at `42 / 47`
- exact verdict accuracy improved to `0.8289`
- fallback gate = `pass`
- production gate = `fail`

So the broadened redesign **does** keep Sprint 6 alive as a real fallback-only
contender. It is still **not production-ready** under the frozen Session 4
gates, so queue-building remains blocked.

## What changed versus Session 9.6

### 1. Deterministic anti-trap vetoes were worth keeping

The first no-cost layer already removed `3` of the `5` Session 9.6 false
merges:

- `Bosio <-> Luca Bosio`
- `Bastida <-> Familia Bastida`
- `La Tour du Pin <-> Tour du Pin Figeac`

These were not the hard ambiguity cases. They were obvious trap families where
the routed specialist had been allowed to over-credit weak continuity.

Result after deterministic vetoes alone:
- false merges: `5 -> 2`
- blind-core missed merges: unchanged at `5`
- routed recoveries: unchanged at `42 / 47`

This means the broader redesign should preserve a deterministic negative-control
layer. It should not spend model tokens to rediscover these easy vetoes.

### 2. Broad skeptical review was too blunt

`session9_7_layered_safety_sonnet_r1` reviewed all surviving `11.4.f` specialist
merges after the deterministic vetoes.

It correctly vetoed the two remaining false merges:
- `Giovanni Giordano <-> Luigi Giordano`
- `Confuron Gindre <-> Edouard Confuron`

But it also vetoed too many true merges:
- blind-core missed merges rose from `5` to `9`
- routed recoveries fell from `42 / 47` to `36 / 47`

That run proved the skeptical gate had the right failure intuition, but the
wrong scope.

### 3. Narrow skeptical review was the right shape

The winning Session 9.7 configuration was:

- deterministic vetoes first
- then skeptical Sonnet review only on a narrow `11.4.f` pattern:
  - `catalog_exact_overlap`
  - `catalog_subset_match`
  - `exact_overlap_count == 1`
  - no `lex_contains`
  - no `risk_secondary_relationship_without_identity`

That isolated exactly the two remaining trap families without reopening the
rest of the recovered merge set.

Canonical narrow-run outputs:
- `data/sprints/dedup/bakeoff_v2/scored/session9_7_layered_safety_sonnet_r2_narrow.json`
- `data/sprints/dedup/bakeoff_v2/scored/session9_7_layered_safety_sonnet_r2_narrow.md`
- `data/sprints/dedup/bakeoff_v2/scored/session9_7_layered_safety_sonnet_r2_narrow_memo.md`

## Round log

### Round 0 - deterministic only

Run:
- `session9_7_layered_safety_det_only`

Outcome:
- strong improvement
- not enough
- false merges still `2`

### Round 1 - GPT-5 mini safety gate

Run:
- `session9_7_layered_safety_gpt5mini_r1`

Outcome:
- invalid as an evaluation round
- OpenRouter returned provider-routing `404` failures on all review calls
- useful only as a transport failure artifact, not as model evidence

### Round 2 - Sonnet safety gate on all surviving `11.4.f` specialist merges

Run:
- `session9_7_layered_safety_sonnet_r1`

Outcome:
- removed the remaining false merges
- over-vetoed true merges
- proved the skeptical doctrine was directionally right but too broad

### Round 3 - Sonnet safety gate on only the suspicious one-anchor `11.4.f` traps

Run:
- `session9_7_layered_safety_sonnet_r2_narrow`

Outcome:
- best Session 9.7 result
- eliminated all remaining false merges
- preserved the Session 9.6 recall gains
- passed the fallback gate
- still failed the production gate

## Why the final narrow design works

The key discovery from Session 9.7 is structural:

- the Session 9.6 routed specialists were **good at recovering recall**
- the Session 9.6 failures were **not** evenly distributed across all routed
  merges
- a small negative-control layer can isolate the real trap family without
  forcing the whole routed family back into over-conservative `FLAGGED`
  behavior

In other words:
- specialist recall recovery is worth preserving
- deterministic anti-trap vetoes are worth preserving
- skeptical review should be used as a scalpel, not a blanket rule

## What is now worth preserving

Keep these as the current best rebuild line:

- `pipeline/identity/bakeoff_pattern_specialist_proof.py`
- `pipeline/identity/bakeoff_layered_safety_gate.py`
- deterministic veto rules for:
  - shared-surname / no-catalog / no-same-region traps
  - secondary-relationship / no-name-bridge traps
- the narrow `11.4.f` skeptical safety gate pattern from
  `session9_7_layered_safety_sonnet_r2_narrow`
- `session9_7_layered_safety_sonnet_r2_narrow` as the new leading fallback
  benchmark artifact

Do **not** preserve as the leading path:

- the raw Session 9.6 specialist composite without the layered veto/gate logic
- blanket skeptical review across all `11.4.f` merges
- the invalid GPT-5 mini routing round as if it were model evidence

## Exact posture after Session 9.7

### What is true now

- the old "freeze immediately after Session 9.6" posture is no longer the only
  evidence-backed option
- a broader redesign **did** produce a better adjudication path
- that path is good enough to be taken seriously as a fallback-quality contender

### What is still true

- no production-ready path exists yet
- queue-building is still blocked
- the frozen Session 4 production gate still fails
- the remaining blocker is recall, not false-merge safety

## Recommendation

Do **not** build queues yet.

Do **not** throw away the rebuild as a dead end either.

Recommended next move:
- treat `session9_7_layered_safety_sonnet_r2_narrow` as the new leading
  fallback artifact
- run one narrow follow-on session that targets only the remaining production
  blocker: the `5` blind-core missed merges / `6` total hard missed merges
  that survived even after the layered safety fix
- keep the new zero-false-merge safety structure fixed while doing that

If the user does **not** want another narrow continuation session, then the
honest freeze point is now:
- freeze the adjudication path at the Session 9.7 layered fallback state, not
  the weaker Session 9.6 specialist-only state

