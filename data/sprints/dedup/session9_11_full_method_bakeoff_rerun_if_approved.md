# Session 9.11 - full method bakeoff rerun

- Generated: 2026-04-21T13:35:03-04:00
- Run name: `session9_11_full_method_bakeoff_rerun_if_approved`
- Benchmark: `producer_dedup_benchmark_v1`
- Cases scored: `152`
- Frozen control: `session9_7_layered_safety_sonnet_r2_narrow` / `layered_safety_sonnet_r2_narrow_v1`
- New model spend this session: `$0.00`

## Goal

Run the full 152-case rerun for the three Session 9.10 proof survivors, compare them against the frozen Session 9.7 fallback control, and decide whether Sprint 6 can move toward queue-building or should freeze at the best non-production artifact.

## Inputs

- `data/session_prompts/s9_11_full_method_bakeoff_rerun_if_approved.md`
- `data/sprints/dedup/session9_10_method_bakeoff_proof_subset.md`
- `data/sprints/dedup/bakeoff_v2/scored/session9_10_method_bakeoff_proof_subset.json`
- `data/sprints/dedup/bakeoff_v2/scored/session9_10_method_bakeoff_proof_subset.md`
- `data/sprints/dedup/bakeoff_v2/scored/session9_7_layered_safety_sonnet_r2_narrow.json`
- `data/sprints/dedup/bakeoff_v2/scored/session9_7_layered_safety_sonnet_r2_narrow_memo.md`

## Full-rerun scorecard

| Artifact | Exact acc | False merge | Hard missed | Soft missed | Safe flag | Survivor acc | Production gate | Fallback gate |
|---|---:|---:|---:|---:|---:|---:|---|---|
| `layered_safety_sonnet_r2_narrow_v1` | 0.8289 | 0 | 6 | 3 | 17 | 1.0000 | fail | pass |
| `merge_proposer_plus_veto_v1` | 0.8158 | 9 | 3 | 1 | 15 | 1.0000 | fail | fail |
| `expanded_layered_router_v1` | 0.8355 | 5 | 4 | 1 | 15 | 1.0000 | fail | fail |
| `evidence_digest_then_judge_v1` | 0.8224 | 5 | 5 | 1 | 16 | 1.0000 | fail | fail |

## Delta vs frozen Session 9.7 control

The frozen control remains the comparison baseline: `0` false merges, `6` hard misses, `3` soft misses, `0.1316` flag rate, fallback gate `pass`.

| Contender | Recoveries vs control | Blind-core blocker recoveries | New false merges | Changed cases | Exact acc delta | Flag-rate delta |
|---|---:|---:|---:|---:|---:|---:|
| `merge_proposer_plus_veto_v1` | 5 | 4 | 9 | 14 | -0.0131 | -0.0263 |
| `expanded_layered_router_v1` | 4 | 3 | 5 | 9 | +0.0066 | -0.0263 |
| `evidence_digest_then_judge_v1` | 3 | 3 | 5 | 8 | -0.0065 | -0.0198 |

### `merge_proposer_plus_veto_v1`

- Recoveries vs control: `blind_core_audit_001`, `blind_core_audit_012`, `blind_core_audit_019`, `blind_core_audit_024`, `known_missed_merge_patterns_011`
- New false merges vs control: `blind_core_audit_040`, `blind_core_audit_042`, `blind_core_audit_056`, `blind_core_audit_063`, `blind_core_audit_064`, `blind_core_audit_089`, `known_false_merge_patterns_010`, `tail_random_sample_006`, `tail_random_sample_014`
- Blind-core missed merges: `5` -> `1`
- Known-false-merge pattern false merges: `0` -> `1`
- Tail false merges: `0` -> `2`
- Gate result: production `fail`, fallback `fail`.

### `expanded_layered_router_v1`

- Recoveries vs control: `blind_core_audit_001`, `blind_core_audit_012`, `blind_core_audit_024`, `known_missed_merge_patterns_011`
- New false merges vs control: `blind_core_audit_040`, `blind_core_audit_042`, `blind_core_audit_064`, `tail_random_sample_006`, `tail_random_sample_014`
- Blind-core missed merges: `5` -> `2`
- Known-false-merge pattern false merges: `0` -> `0`
- Tail false merges: `0` -> `2`
- Gate result: production `fail`, fallback `fail`.

### `evidence_digest_then_judge_v1`

- Recoveries vs control: `blind_core_audit_001`, `blind_core_audit_019`, `blind_core_audit_024`
- New false merges vs control: `blind_core_audit_056`, `blind_core_audit_063`, `blind_core_audit_064`, `blind_core_audit_089`, `known_false_merge_patterns_010`
- Blind-core missed merges: `5` -> `2`
- Known-false-merge pattern false merges: `0` -> `1`
- Tail false merges: `0` -> `0`
- Gate result: production `fail`, fallback `fail`.

## Best surviving artifact

Best artifact after the full rerun: `layered_safety_sonnet_r2_narrow_v1` (fallback_only, production `fail`, fallback `pass`).

The Session 9.7 layered fallback control remains the best surviving artifact because it is still the only run that clears the fallback gate. Every broader Session 9.11 survivor recovered some missed merges but reopened 5-9 false merges on the full benchmark, which immediately blocks both production and fallback status.

## Recommendation

- Status: `freeze_at_best_non_production_artifact`
- Best surviving artifact: `layered_safety_sonnet_r2_narrow_v1`
- Queue-building: `do_not_proceed`
- Reason: All three broader methods fail the frozen production gate and also fail the fallback gate once scaled to all 152 cases. Sprint 6 should freeze at the best existing non-production artifact instead of opening another redesign in this session.
