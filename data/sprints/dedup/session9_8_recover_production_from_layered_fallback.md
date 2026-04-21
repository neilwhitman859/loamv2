# Session 9.8 - recover production readiness from the layered fallback

Date: 2026-04-21
Inputs:
- `data/sprints/dedup/session9_7_layered_safety_redesign.md`
- `data/sprints/dedup/bakeoff_v2/scored/session9_7_layered_safety_sonnet_r2_narrow.{json,md}`
- `data/sprints/dedup/bakeoff_v2/scored/session9_7_layered_safety_sonnet_r2_narrow_memo.md`
- `data/sprints/dedup/bakeoff_v2/scored/session9_6_pattern_specialist_proof_if_approved.{json,md}`
- `data/sprints/dedup/bakeoff_v2/scored/session9_3_full_rerun_if_approved.{json,md}`
- `data/sprints/dedup/benchmark_v1.json`
- `pipeline/identity/bakeoff_layered_safety_gate.py`

## Goal

Decide whether one more narrow recall-only continuation on top of
`session9_7_layered_safety_sonnet_r2_narrow` is credible enough to recover the
remaining production-gate misses without reintroducing false merges.

## Short answer

No.

Session 9.7 should now be treated as the honest freeze point for the current
adjudication path:

- status: `fallback-only`
- production readiness: `not recovered`
- recommendation: `freeze at the Session 9.7 layered fallback state`

I did **not** run another proof. The remaining miss set is too fragmented to
justify one more "narrow" proof:

- the layered run still has `9` disagreements, all missed merges
- production would still require at least `5` additional safe recoveries
- the `5` production-blocking blind-core cases span `5` distinct family or
  signature shapes
- `4` of the `9` remaining misses sit **outside** the current routed-family
  specialist bundle
- the multi-case signatures that do repeat are entangled with benchmark skip
  controls and prior false-merge traps

That means the smallest continuation that could honestly change the production
answer is no longer narrow. It would be a broader multi-family positive-control
redesign.

## Remaining miss set

`session9_7_layered_safety_sonnet_r2_narrow` leaves `9` benchmark misses:

| Case | Cluster | Tier | Expected | Current | Pair |
|---|---|---|---|---|---|
| `blind_core_audit_001` | `11.1` | core | `MERGE` | `FLAGGED` | `De Stefani <-> Stefani` |
| `blind_core_audit_012` | `11.4.g` | core | `MERGE` | `SKIP` | `Louis Jadot (Jacques) <-> des Heritiers Louis Jadot` |
| `blind_core_audit_016` | `11.4.h` | core | `MERGE` | `SKIP` | `Stadt Krems <-> Krems` |
| `blind_core_audit_019` | `11.4.f` | core | `MERGE` | `SKIP` | `Francois Carillon <-> Jacques et Francois Carillon` |
| `blind_core_audit_024` | `11.4.b` | core | `MERGE` | `FLAGGED` | `Melka <-> Melka` |
| `known_missed_merge_patterns_001` | `11.4.h` | mid | `MERGE` | `SKIP` | `Protheau & Fils <-> Jean-Francois Protheau` |
| `known_missed_merge_patterns_002` | `11.4.o` | mid | `MERGE` | `FLAGGED` | `Lombardi <-> Tendil & Lombardi` |
| `known_missed_merge_patterns_008` | `11.4.h` | mid | `MERGE` | `SKIP` | `Ardhuy Cabotte <-> de la Cabotte` |
| `known_missed_merge_patterns_011` | `11.4.h` | mid | `MERGE` | `SKIP` | `Baron de Rothschild <-> Barons de Rothschild Collection` |

Important constraint:

- Session 9.7 did **not** lose any recall versus Session 9.6
- the layered safety work removed the `5` false merges from Session 9.6
- the miss set above is unchanged from the Session 9.6 routed-specialist
  composite

So the question is not "should we loosen the new safety gate?" The question is
"is there one more safe positive-control layer worth adding?" The evidence says
no, not in a narrow way.

## Gate math

The current production-gate failures are:

- blind-core hard missed merge: `3` (must reach `0`)
- blind-core soft missed merge: `2` (must reach `<= 1`)
- known-missed hard merge: `3` (must reach `<= 2`)

Minimum additional safe recoveries required:

- `3` blind-core `SKIP -> MERGE`
- `1` blind-core `FLAGGED -> MERGE`
- `1` known-missed hard `SKIP -> MERGE`

So production readiness still needs at least **`5` more safe recoveries**.

## Why no narrow continuation survived

### 1. The blind-core blockers are not concentrated enough

The `5` production-blocking blind-core cases are:

- `blind_core_audit_001` (`11.1`)
- `blind_core_audit_012` (`11.4.g`)
- `blind_core_audit_016` (`11.4.h`)
- `blind_core_audit_019` (`11.4.f`)
- `blind_core_audit_024` (`11.4.b`)

That is not one family. It is five separate shapes.

### 2. Perfecting the current routed families would still not clear production

The current routed-specialist proof only covers `11.4.h`, `11.4.f`, `11.4.n`,
and `11.4.p`.

Of the `9` remaining misses, only `5` are in currently routed families:

- `blind_core_audit_016`
- `blind_core_audit_019`
- `known_missed_merge_patterns_001`
- `known_missed_merge_patterns_008`
- `known_missed_merge_patterns_011`

Even if a new proof recovered **all five** of those without introducing any new
false merges, the path would still leave:

- `blind_core_audit_001` as a blind-core soft miss
- `blind_core_audit_012` as a blind-core hard miss
- `blind_core_audit_024` as a blind-core soft miss
- `known_missed_merge_patterns_002` as a residual known-missed soft miss

That still fails the frozen production gate.

So a continuation that stays inside the current routed-family frame cannot
recover production readiness.

### 3. Expanding the continuation enough to matter is already a broader redesign

To change the production answer, the next continuation would have to go beyond
the current routed families and add new positive-control logic for at least:

- `11.1`
- `11.4.g`
- `11.4.b`
- `11.4.o`

That is already broader than the bounded Session 9.6 or Session 9.7 shape.
Calling that "one more narrow continuation" would understate what the work is.

### 4. The repeat signatures are entangled with skip controls

The remaining misses collapse to `6` packet-ref signatures, not one clean
bucket. The repeating signatures are the dangerous part:

- `blind_core_audit_012`, `known_missed_merge_patterns_001`, and
  `known_missed_merge_patterns_011` share the exact same sparse-official /
  portfolio-shape signature, and that same signature is also used by benchmark
  skip control `known_false_merge_patterns_009`
- `blind_core_audit_016` and `known_missed_merge_patterns_002` share another
  repeated signature that also appears on skip controls such as
  `Haselgrove <-> James Haselgrove`, `Dancer <-> Theo Dancer`,
  `Tony Bornard <-> Bornard`, and `Passopisciaro <-> Santo Spirito Passopisciaro`

Those are not disjoint rescue buckets. They are mixed-signature zones where the
benchmark already proves that a generic positive override would hit real skips.

### 5. The only clean-looking micro-candidates are too small

Two misses look relatively isolated:

- `blind_core_audit_024` (`Melka <-> Melka`) behaves more like an exact-name
  cross-country global-brand packet than a family-split case
- `blind_core_audit_001` (`De Stefani <-> Stefani`) is a same-country
  containment/subset case without the shared-surname-split risk flag

Those are the closest things to a narrow rescue.

But even if both were recovered cleanly, production would still remain blocked
by at least:

- `blind_core_audit_012`
- `blind_core_audit_016`
- `blind_core_audit_019`
- one of the remaining known-missed hard cases

So the best-looking micro-candidates still do not justify another proof. They
do not move the go/no-go answer far enough.

## Smallest credible continuation

The smallest continuation that is still honest about the remaining blocker is:

- a **broader multi-family positive-control redesign**
- layered on top of the fixed Session 9.7 safety base
- explicitly extending beyond the current routed-family set
- with fresh proof coverage across the newly added families and their skip
  controls

That is materially different from:

- another narrow tweak to `11.4.h` or `11.4.f`
- undoing the deterministic anti-trap vetoes
- loosening the Session 9.7 skeptical safety layer
- or running a benchmark-shaped rescue that only special-cases the current nine
  misses

This broader redesign might still be worth discussing someday, but it is **not**
the session the user scoped here.

## Proof decision

I did **not** run a new bounded proof.

Reason:

- any proof small enough to stay "narrow" would only chase one or two
  micro-candidates and would not change the production-readiness answer
- any proof large enough to matter would already be testing a broader redesign,
  not the narrow continuation this session was meant to validate

So the honest stop point is the memo itself.

## Conclusion

The adjudication path is **not production-ready**.

It is still valuable as a **fallback-only artifact**, and the best preserved
endpoint remains:

- `session9_7_layered_safety_sonnet_r2_narrow`

Recommended disposition:

- freeze the current adjudication path at the Session 9.7 layered fallback
  state
- keep queue-building blocked
- do not spend on another narrow proof
- only reopen Sprint 6 adjudication if the user explicitly wants a broader
  multi-family redesign, not a small continuation
