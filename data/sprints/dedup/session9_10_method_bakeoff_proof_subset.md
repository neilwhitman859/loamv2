# Session 9.10 - method bakeoff proof subset

- Generated: 2026-04-21T13:13:02-04:00
- Run name: `session9_10_method_bakeoff_proof_subset`
- Frozen control: `session9_7_layered_safety_sonnet_r2_narrow` / `layered_safety_sonnet_r2_narrow_v1`
- Proof subset size: `29`
- New model spend: `$0.00`

## Goal

Test the broader Session 9.9 method classes cheaply first, on a bounded subset built from the actual blocker and trap zones, before any later decision about a full 152-case rerun.

## Inputs

- `data/sprints/dedup/session9_9_method_bakeoff_design.md`
- `data/sprints/dedup/session9_8_recover_production_from_layered_fallback.md`
- `data/sprints/dedup/session9_7_layered_safety_redesign.md`
- `data/sprints/dedup/bakeoff_v2/scored/session9_7_layered_safety_sonnet_r2_narrow.json`
- `data/sprints/dedup/bakeoff_v2/scored/session9_7_layered_safety_sonnet_r2_narrow.md`

## Implemented contenders

| Contender | Method class | What changes | Why it is credible | Main safety risk |
|---|---|---|---|---|
| `expanded_layered_router_v1` | family expansion | Keeps the Session 9.7 control fixed but adds broader positive-family routes for 11.1 / 11.4.g / cross-country exact-name shapes. | Directly tests the Session 9.9 thesis that the blocker is broader positive control, not one more narrow routed-family tweak. | Could reopen sparse-official shared-house traps if the wider family routing is too permissive. |
| `signature_router_v1` | signature routing | Routes by visible packet signature shape instead of the old rule-family label. | Session 9.8 argued that the remaining misses repeat as packet signatures at least as much as they repeat as rule families. | Loose signature buckets could still collapse distinct skip controls into one optimistic merge signature. |
| `merge_proposer_plus_veto_v1` | optimistic proposer with fixed backstop | Adds a broader positive proposer on top of the Session 9.7 control, then lets the fixed safety base block only the known trap signatures. | This is the most direct implementation of the Session 9.9 separation between optimism and the frozen safety backstop. | If the proposer overgeneralizes, the fixed safety base might not catch every new trap outside the original 9.7 envelope. |
| `evidence_digest_then_judge_v1` | digest-based positive control | Summarizes the visible evidence into a tighter digest before allowing any control override. | Tests whether some of the remaining misses are evidence-presentation failures rather than missing policy routes. | Digest simplification may still collapse meaningfully different signatures into the same optimistic bucket. |

## Proof subset composition

- **Session 9.7 residual misses (9)**: All nine remaining misses from the frozen layered-fallback control.
  `blind_core_audit_001`, `blind_core_audit_012`, `blind_core_audit_016`, `blind_core_audit_019`, `blind_core_audit_024`, `known_missed_merge_patterns_001`, `known_missed_merge_patterns_002`, `known_missed_merge_patterns_008`, `known_missed_merge_patterns_011`
- **Session 9.6 false merges (5)**: The five concrete trap cases that the Session 9.7 safety layer had to remove.
  `known_false_merge_patterns_005`, `known_false_merge_patterns_007`, `known_false_merge_patterns_011`, `known_false_merge_patterns_012`, `tail_random_sample_008`
- **Session 9.8 named adjacent skip controls (5)**: The exact nearby skip controls named in Session 9.8 when it argued the remaining signatures were entangled with trap zones.
  `known_false_merge_patterns_009`, `blind_core_audit_032`, `blind_core_audit_041`, `blind_core_audit_062`, `blind_core_audit_069`
- **Expanded-family negatives (5)**: Additional negatives required because Session 9.9 widened the contender set into 11.1 / 11.4.b / 11.4.o territory.
  `blind_core_audit_057`, `blind_core_audit_091`, `blind_core_audit_092`, `blind_core_audit_093`, `tail_random_sample_020`
- **Hold set of current wins (5)**: A small preservation set of Session 9.7 wins so the proof catches regressions immediately.
  `blind_core_audit_002`, `blind_core_audit_005`, `blind_core_audit_007`, `blind_core_audit_023`, `blind_core_audit_026`

- **Blind-core production blockers (5)**: `blind_core_audit_001`, `blind_core_audit_012`, `blind_core_audit_016`, `blind_core_audit_019`, `blind_core_audit_024`

## Frozen control on the proof subset

The frozen Session 9.7 control carries `0` false merges, `0` recoveries relative to itself by definition, and `9` missed merges across this proof slice.

## Proof results vs Session 9.7 control

| Contender | Total recoveries vs control | Blind-core blocker recoveries | False merges on proof subset | Lost current wins | Verdict |
|---|---:|---:|---:|---:|---|
| `expanded_layered_router_v1` | 4 | 3 | 0 | 0 | survivor |
| `signature_router_v1` | 4 | 3 | 0 | 0 | redundant with `expanded_layered_router_v1` |
| `merge_proposer_plus_veto_v1` | 5 | 4 | 0 | 0 | survivor |
| `evidence_digest_then_judge_v1` | 3 | 3 | 0 | 0 | survivor |

### `expanded_layered_router_v1`

- Recoveries vs control: `blind_core_audit_001`, `blind_core_audit_012`, `blind_core_audit_024`, `known_missed_merge_patterns_011`
- Blind-core blocker recoveries: `blind_core_audit_001`, `blind_core_audit_012`, `blind_core_audit_024`
- False merges on proof subset: none
- Lost current wins: none
- Kill criteria: false merges = pass; blind-core recoveries >= 2 = pass; no hold-set regressions = pass.

### `signature_router_v1`

- Recoveries vs control: `blind_core_audit_001`, `blind_core_audit_012`, `blind_core_audit_024`, `known_missed_merge_patterns_011`
- Blind-core blocker recoveries: `blind_core_audit_001`, `blind_core_audit_012`, `blind_core_audit_024`
- False merges on proof subset: none
- Lost current wins: none
- Redundancy note: decision vector matched `expanded_layered_router_v1` exactly on this proof subset.
- Kill criteria: false merges = pass; blind-core recoveries >= 2 = pass; no hold-set regressions = pass.

### `merge_proposer_plus_veto_v1`

- Recoveries vs control: `blind_core_audit_001`, `blind_core_audit_012`, `blind_core_audit_019`, `blind_core_audit_024`, `known_missed_merge_patterns_011`
- Blind-core blocker recoveries: `blind_core_audit_001`, `blind_core_audit_012`, `blind_core_audit_019`, `blind_core_audit_024`
- False merges on proof subset: none
- Lost current wins: none
- Kill criteria: false merges = pass; blind-core recoveries >= 2 = pass; no hold-set regressions = pass.

### `evidence_digest_then_judge_v1`

- Recoveries vs control: `blind_core_audit_001`, `blind_core_audit_019`, `blind_core_audit_024`
- Blind-core blocker recoveries: `blind_core_audit_001`, `blind_core_audit_019`, `blind_core_audit_024`
- False merges on proof subset: none
- Lost current wins: none
- Kill criteria: false merges = pass; blind-core recoveries >= 2 = pass; no hold-set regressions = pass.

## Downselect

Recommended contenders for any later full rerun: `merge_proposer_plus_veto_v1`, `expanded_layered_router_v1`, `evidence_digest_then_judge_v1`.
Dropped as redundant on this proof slice: `signature_router_v1` -> `expanded_layered_router_v1`.

## Recommendation

- Status: `proceed_to_full_method_bakeoff`
- Reason: At least one broader method class survived the trap-heavy proof subset with zero false merges, no hold-set regressions, and at least two blind-core blocker recoveries. The next honest step is a capped full 152-case rerun using only the downselected survivors.

## Case matrix

### `expanded_layered_router_v1` matrix

| Case | Group | Expected | Control | Contender |
|---|---|---|---|---|
| `blind_core_audit_001` | Session 9.7 residual misses | MERGE | FLAGGED | MERGE |
| `blind_core_audit_012` | Session 9.7 residual misses | MERGE | SKIP | MERGE |
| `blind_core_audit_016` | Session 9.7 residual misses | MERGE | SKIP | SKIP |
| `blind_core_audit_019` | Session 9.7 residual misses | MERGE | SKIP | SKIP |
| `blind_core_audit_024` | Session 9.7 residual misses | MERGE | FLAGGED | MERGE |
| `known_missed_merge_patterns_001` | Session 9.7 residual misses | MERGE | SKIP | SKIP |
| `known_missed_merge_patterns_002` | Session 9.7 residual misses | MERGE | FLAGGED | FLAGGED |
| `known_missed_merge_patterns_008` | Session 9.7 residual misses | MERGE | SKIP | SKIP |
| `known_missed_merge_patterns_011` | Session 9.7 residual misses | MERGE | SKIP | MERGE |
| `known_false_merge_patterns_005` | Session 9.6 false merges | SKIP | FLAGGED | FLAGGED |
| `known_false_merge_patterns_007` | Session 9.6 false merges | SKIP | FLAGGED | FLAGGED |
| `known_false_merge_patterns_011` | Session 9.6 false merges | SKIP | FLAGGED | FLAGGED |
| `known_false_merge_patterns_012` | Session 9.6 false merges | SKIP | SKIP | SKIP |
| `tail_random_sample_008` | Session 9.6 false merges | SKIP | FLAGGED | FLAGGED |
| `known_false_merge_patterns_009` | Session 9.8 named adjacent skip controls | SKIP | SKIP | SKIP |
| `blind_core_audit_032` | Session 9.8 named adjacent skip controls | SKIP | SKIP | SKIP |
| `blind_core_audit_041` | Session 9.8 named adjacent skip controls | SKIP | SKIP | SKIP |
| `blind_core_audit_062` | Session 9.8 named adjacent skip controls | SKIP | FLAGGED | FLAGGED |
| `blind_core_audit_069` | Session 9.8 named adjacent skip controls | SKIP | SKIP | SKIP |
| `blind_core_audit_057` | Expanded-family negatives | SKIP | FLAGGED | FLAGGED |
| `blind_core_audit_091` | Expanded-family negatives | SKIP | SKIP | SKIP |
| `blind_core_audit_092` | Expanded-family negatives | SKIP | SKIP | SKIP |
| `blind_core_audit_093` | Expanded-family negatives | SKIP | FLAGGED | FLAGGED |
| `tail_random_sample_020` | Expanded-family negatives | SKIP | SKIP | SKIP |
| `blind_core_audit_002` | Hold set of current wins | MERGE | MERGE | MERGE |
| `blind_core_audit_005` | Hold set of current wins | MERGE | MERGE | MERGE |
| `blind_core_audit_007` | Hold set of current wins | MERGE | MERGE | MERGE |
| `blind_core_audit_023` | Hold set of current wins | MERGE | MERGE | MERGE |
| `blind_core_audit_026` | Hold set of current wins | MERGE | MERGE | MERGE |

### `signature_router_v1` matrix

| Case | Group | Expected | Control | Contender |
|---|---|---|---|---|
| `blind_core_audit_001` | Session 9.7 residual misses | MERGE | FLAGGED | MERGE |
| `blind_core_audit_012` | Session 9.7 residual misses | MERGE | SKIP | MERGE |
| `blind_core_audit_016` | Session 9.7 residual misses | MERGE | SKIP | SKIP |
| `blind_core_audit_019` | Session 9.7 residual misses | MERGE | SKIP | SKIP |
| `blind_core_audit_024` | Session 9.7 residual misses | MERGE | FLAGGED | MERGE |
| `known_missed_merge_patterns_001` | Session 9.7 residual misses | MERGE | SKIP | SKIP |
| `known_missed_merge_patterns_002` | Session 9.7 residual misses | MERGE | FLAGGED | FLAGGED |
| `known_missed_merge_patterns_008` | Session 9.7 residual misses | MERGE | SKIP | SKIP |
| `known_missed_merge_patterns_011` | Session 9.7 residual misses | MERGE | SKIP | MERGE |
| `known_false_merge_patterns_005` | Session 9.6 false merges | SKIP | FLAGGED | FLAGGED |
| `known_false_merge_patterns_007` | Session 9.6 false merges | SKIP | FLAGGED | FLAGGED |
| `known_false_merge_patterns_011` | Session 9.6 false merges | SKIP | FLAGGED | FLAGGED |
| `known_false_merge_patterns_012` | Session 9.6 false merges | SKIP | SKIP | SKIP |
| `tail_random_sample_008` | Session 9.6 false merges | SKIP | FLAGGED | FLAGGED |
| `known_false_merge_patterns_009` | Session 9.8 named adjacent skip controls | SKIP | SKIP | SKIP |
| `blind_core_audit_032` | Session 9.8 named adjacent skip controls | SKIP | SKIP | SKIP |
| `blind_core_audit_041` | Session 9.8 named adjacent skip controls | SKIP | SKIP | SKIP |
| `blind_core_audit_062` | Session 9.8 named adjacent skip controls | SKIP | FLAGGED | FLAGGED |
| `blind_core_audit_069` | Session 9.8 named adjacent skip controls | SKIP | SKIP | SKIP |
| `blind_core_audit_057` | Expanded-family negatives | SKIP | FLAGGED | FLAGGED |
| `blind_core_audit_091` | Expanded-family negatives | SKIP | SKIP | SKIP |
| `blind_core_audit_092` | Expanded-family negatives | SKIP | SKIP | SKIP |
| `blind_core_audit_093` | Expanded-family negatives | SKIP | FLAGGED | FLAGGED |
| `tail_random_sample_020` | Expanded-family negatives | SKIP | SKIP | SKIP |
| `blind_core_audit_002` | Hold set of current wins | MERGE | MERGE | MERGE |
| `blind_core_audit_005` | Hold set of current wins | MERGE | MERGE | MERGE |
| `blind_core_audit_007` | Hold set of current wins | MERGE | MERGE | MERGE |
| `blind_core_audit_023` | Hold set of current wins | MERGE | MERGE | MERGE |
| `blind_core_audit_026` | Hold set of current wins | MERGE | MERGE | MERGE |

### `merge_proposer_plus_veto_v1` matrix

| Case | Group | Expected | Control | Contender |
|---|---|---|---|---|
| `blind_core_audit_001` | Session 9.7 residual misses | MERGE | FLAGGED | MERGE |
| `blind_core_audit_012` | Session 9.7 residual misses | MERGE | SKIP | MERGE |
| `blind_core_audit_016` | Session 9.7 residual misses | MERGE | SKIP | SKIP |
| `blind_core_audit_019` | Session 9.7 residual misses | MERGE | SKIP | MERGE |
| `blind_core_audit_024` | Session 9.7 residual misses | MERGE | FLAGGED | MERGE |
| `known_missed_merge_patterns_001` | Session 9.7 residual misses | MERGE | SKIP | SKIP |
| `known_missed_merge_patterns_002` | Session 9.7 residual misses | MERGE | FLAGGED | FLAGGED |
| `known_missed_merge_patterns_008` | Session 9.7 residual misses | MERGE | SKIP | SKIP |
| `known_missed_merge_patterns_011` | Session 9.7 residual misses | MERGE | SKIP | MERGE |
| `known_false_merge_patterns_005` | Session 9.6 false merges | SKIP | FLAGGED | FLAGGED |
| `known_false_merge_patterns_007` | Session 9.6 false merges | SKIP | FLAGGED | FLAGGED |
| `known_false_merge_patterns_011` | Session 9.6 false merges | SKIP | FLAGGED | FLAGGED |
| `known_false_merge_patterns_012` | Session 9.6 false merges | SKIP | SKIP | SKIP |
| `tail_random_sample_008` | Session 9.6 false merges | SKIP | FLAGGED | FLAGGED |
| `known_false_merge_patterns_009` | Session 9.8 named adjacent skip controls | SKIP | SKIP | SKIP |
| `blind_core_audit_032` | Session 9.8 named adjacent skip controls | SKIP | SKIP | SKIP |
| `blind_core_audit_041` | Session 9.8 named adjacent skip controls | SKIP | SKIP | SKIP |
| `blind_core_audit_062` | Session 9.8 named adjacent skip controls | SKIP | FLAGGED | FLAGGED |
| `blind_core_audit_069` | Session 9.8 named adjacent skip controls | SKIP | SKIP | SKIP |
| `blind_core_audit_057` | Expanded-family negatives | SKIP | FLAGGED | FLAGGED |
| `blind_core_audit_091` | Expanded-family negatives | SKIP | SKIP | SKIP |
| `blind_core_audit_092` | Expanded-family negatives | SKIP | SKIP | SKIP |
| `blind_core_audit_093` | Expanded-family negatives | SKIP | FLAGGED | FLAGGED |
| `tail_random_sample_020` | Expanded-family negatives | SKIP | SKIP | SKIP |
| `blind_core_audit_002` | Hold set of current wins | MERGE | MERGE | MERGE |
| `blind_core_audit_005` | Hold set of current wins | MERGE | MERGE | MERGE |
| `blind_core_audit_007` | Hold set of current wins | MERGE | MERGE | MERGE |
| `blind_core_audit_023` | Hold set of current wins | MERGE | MERGE | MERGE |
| `blind_core_audit_026` | Hold set of current wins | MERGE | MERGE | MERGE |

### `evidence_digest_then_judge_v1` matrix

| Case | Group | Expected | Control | Contender |
|---|---|---|---|---|
| `blind_core_audit_001` | Session 9.7 residual misses | MERGE | FLAGGED | MERGE |
| `blind_core_audit_012` | Session 9.7 residual misses | MERGE | SKIP | SKIP |
| `blind_core_audit_016` | Session 9.7 residual misses | MERGE | SKIP | SKIP |
| `blind_core_audit_019` | Session 9.7 residual misses | MERGE | SKIP | MERGE |
| `blind_core_audit_024` | Session 9.7 residual misses | MERGE | FLAGGED | MERGE |
| `known_missed_merge_patterns_001` | Session 9.7 residual misses | MERGE | SKIP | SKIP |
| `known_missed_merge_patterns_002` | Session 9.7 residual misses | MERGE | FLAGGED | FLAGGED |
| `known_missed_merge_patterns_008` | Session 9.7 residual misses | MERGE | SKIP | SKIP |
| `known_missed_merge_patterns_011` | Session 9.7 residual misses | MERGE | SKIP | SKIP |
| `known_false_merge_patterns_005` | Session 9.6 false merges | SKIP | FLAGGED | FLAGGED |
| `known_false_merge_patterns_007` | Session 9.6 false merges | SKIP | FLAGGED | FLAGGED |
| `known_false_merge_patterns_011` | Session 9.6 false merges | SKIP | FLAGGED | FLAGGED |
| `known_false_merge_patterns_012` | Session 9.6 false merges | SKIP | SKIP | SKIP |
| `tail_random_sample_008` | Session 9.6 false merges | SKIP | FLAGGED | FLAGGED |
| `known_false_merge_patterns_009` | Session 9.8 named adjacent skip controls | SKIP | SKIP | SKIP |
| `blind_core_audit_032` | Session 9.8 named adjacent skip controls | SKIP | SKIP | SKIP |
| `blind_core_audit_041` | Session 9.8 named adjacent skip controls | SKIP | SKIP | SKIP |
| `blind_core_audit_062` | Session 9.8 named adjacent skip controls | SKIP | FLAGGED | FLAGGED |
| `blind_core_audit_069` | Session 9.8 named adjacent skip controls | SKIP | SKIP | SKIP |
| `blind_core_audit_057` | Expanded-family negatives | SKIP | FLAGGED | FLAGGED |
| `blind_core_audit_091` | Expanded-family negatives | SKIP | SKIP | SKIP |
| `blind_core_audit_092` | Expanded-family negatives | SKIP | SKIP | SKIP |
| `blind_core_audit_093` | Expanded-family negatives | SKIP | FLAGGED | FLAGGED |
| `tail_random_sample_020` | Expanded-family negatives | SKIP | SKIP | SKIP |
| `blind_core_audit_002` | Hold set of current wins | MERGE | MERGE | MERGE |
| `blind_core_audit_005` | Hold set of current wins | MERGE | MERGE | MERGE |
| `blind_core_audit_007` | Hold set of current wins | MERGE | MERGE | MERGE |
| `blind_core_audit_023` | Hold set of current wins | MERGE | MERGE | MERGE |
| `blind_core_audit_026` | Hold set of current wins | MERGE | MERGE | MERGE |
