# Session 10.8 - contrastive burden proof subset

- Generated: 2026-04-21T21:14:42-04:00
- Run name: `session10_8_contrastive_burden_proof_v1`
- Model: `claude-sonnet-4-6`
- Frozen control: `session9_7_layered_safety_sonnet_r2_narrow` / `layered_safety_sonnet_r2_narrow_v1`
- Proof subset size: `29`
- Estimated spend: `$0.4858`

## Goal

Test whether a stronger contrastive reasoning contract can recover the actual blocker cases without reopening the trap-heavy proof subset.

## Proof result

- Recoveries vs control: `blind_core_audit_024`
- Blind-core blocker recoveries: `blind_core_audit_024`
- False merges on proof subset: `blind_core_audit_062`
- Lost current wins: `blind_core_audit_007`
- Kill criteria: false merges = fail; blind-core recoveries >= 2 = fail; no hold-set regressions = fail.

## Case matrix

| Case | Group | Expected | Control | Contender |
|---|---|---|---|---|
| `blind_core_audit_001` | Session 9.7 residual misses | MERGE | FLAGGED | SKIP |
| `blind_core_audit_012` | Session 9.7 residual misses | MERGE | SKIP | SKIP |
| `blind_core_audit_016` | Session 9.7 residual misses | MERGE | SKIP | SKIP |
| `blind_core_audit_019` | Session 9.7 residual misses | MERGE | SKIP | SKIP |
| `blind_core_audit_024` | Session 9.7 residual misses | MERGE | FLAGGED | MERGE |
| `known_missed_merge_patterns_001` | Session 9.7 residual misses | MERGE | SKIP | SKIP |
| `known_missed_merge_patterns_002` | Session 9.7 residual misses | MERGE | FLAGGED | SKIP |
| `known_missed_merge_patterns_008` | Session 9.7 residual misses | MERGE | SKIP | SKIP |
| `known_missed_merge_patterns_011` | Session 9.7 residual misses | MERGE | SKIP | SKIP |
| `known_false_merge_patterns_005` | Session 9.6 false merges | SKIP | FLAGGED | SKIP |
| `known_false_merge_patterns_007` | Session 9.6 false merges | SKIP | FLAGGED | FLAGGED |
| `known_false_merge_patterns_011` | Session 9.6 false merges | SKIP | FLAGGED | SKIP |
| `known_false_merge_patterns_012` | Session 9.6 false merges | SKIP | SKIP | SKIP |
| `tail_random_sample_008` | Session 9.6 false merges | SKIP | FLAGGED | SKIP |
| `known_false_merge_patterns_009` | Session 9.8 named adjacent skip controls | SKIP | SKIP | SKIP |
| `blind_core_audit_032` | Session 9.8 named adjacent skip controls | SKIP | SKIP | SKIP |
| `blind_core_audit_041` | Session 9.8 named adjacent skip controls | SKIP | SKIP | SKIP |
| `blind_core_audit_062` | Session 9.8 named adjacent skip controls | SKIP | FLAGGED | MERGE |
| `blind_core_audit_069` | Session 9.8 named adjacent skip controls | SKIP | SKIP | SKIP |
| `blind_core_audit_057` | Expanded-family negatives | SKIP | FLAGGED | SKIP |
| `blind_core_audit_091` | Expanded-family negatives | SKIP | SKIP | SKIP |
| `blind_core_audit_092` | Expanded-family negatives | SKIP | SKIP | SKIP |
| `blind_core_audit_093` | Expanded-family negatives | SKIP | FLAGGED | SKIP |
| `tail_random_sample_020` | Expanded-family negatives | SKIP | SKIP | SKIP |
| `blind_core_audit_002` | Hold set of current wins | MERGE | MERGE | MERGE |
| `blind_core_audit_005` | Hold set of current wins | MERGE | MERGE | MERGE |
| `blind_core_audit_007` | Hold set of current wins | MERGE | MERGE | SKIP |
| `blind_core_audit_023` | Hold set of current wins | MERGE | MERGE | MERGE |
| `blind_core_audit_026` | Hold set of current wins | MERGE | MERGE | MERGE |

## Recommendation

- Status: `eliminated_on_proof_subset`
- Reason: The contrastive burden adjudicator did not survive the trap-heavy proof subset cleanly enough to justify a full 152-case rerun.
