# Session 10.8 - hybrid signature plus judge

- Generated: 2026-04-21T21:57:04-04:00
- Run name: `session10_8_hybrid_signature_plus_judge_opus_rerun_v1`
- Model: `claude-opus-4-6`
- Frozen control: `session9_7_layered_safety_sonnet_r2_narrow` / `layered_safety_sonnet_r2_narrow_v1`
- Deterministic ruleset: `hybrid_safe_rules_v1`
- Frontier version: `surname_frontier_v1`
- Frontier size: `13`
- Estimated spend: `$2.9964`

## Goal

Test a true hybrid contender: keep the frozen control, add only the small zero-false-merge deterministic promotions that survived full-benchmark pressure checks, then let one narrow shared-surname frontier judge try to finish the last unresolved band.

## Deterministic promotions

- `alpha_same_region_subset_dupsec` -> `blind_core_audit_001` (MERGE)
- `beta_two_core_not_same_region` -> `blind_core_audit_012` (MERGE)
- `delta_et_historical_joint_label` -> `blind_core_audit_019` (MERGE)
- `gamma_cross_country_all_secondary_same` -> `blind_core_audit_024` (MERGE)
- `beta_two_core_not_same_region` -> `known_missed_merge_patterns_011` (MERGE)

## Frontier

- Routed cases: `blind_core_audit_016`, `blind_core_audit_032`, `blind_core_audit_037`, `blind_core_audit_041`, `blind_core_audit_051`, `blind_core_audit_056`, `blind_core_audit_061`, `blind_core_audit_062`, `blind_core_audit_063`, `blind_core_audit_066`, `blind_core_audit_069`, `blind_core_audit_089`, `known_missed_merge_patterns_002`
- Judge-touched cases: `blind_core_audit_016`, `blind_core_audit_032`, `blind_core_audit_037`, `blind_core_audit_041`, `blind_core_audit_051`, `blind_core_audit_056`, `blind_core_audit_061`, `blind_core_audit_062`, `blind_core_audit_063`, `blind_core_audit_066`, `blind_core_audit_069`, `blind_core_audit_089`, `known_missed_merge_patterns_002`

## Scorecard

- Counts: false merge `1`, hard missed `4`, soft missed `0`, safe flag `22`.
- Rates: exact acc `0.8224`, merge capture `0.9216`, survivor acc `1.0000`, flag rate `0.1447`.
- Gates: production `fail`, fallback `fail`.

## Delta Vs Control

- Recoveries vs control: `blind_core_audit_001`, `blind_core_audit_012`, `blind_core_audit_019`, `blind_core_audit_024`, `known_missed_merge_patterns_011`
- Blind-core recoveries vs control: `blind_core_audit_001`, `blind_core_audit_012`, `blind_core_audit_019`, `blind_core_audit_024`
- New false merges vs control: `blind_core_audit_062`
- Lost control wins: none

## Frontier Case Matrix

| Case | Expected | Control | Contender |
|---|---|---|---|
| `blind_core_audit_016` | MERGE | SKIP | SKIP |
| `blind_core_audit_032` | SKIP | SKIP | FLAGGED |
| `blind_core_audit_037` | SKIP | SKIP | SKIP |
| `blind_core_audit_041` | SKIP | SKIP | FLAGGED |
| `blind_core_audit_051` | SKIP | SKIP | SKIP |
| `blind_core_audit_056` | SKIP | SKIP | SKIP |
| `blind_core_audit_061` | SKIP | SKIP | FLAGGED |
| `blind_core_audit_062` | SKIP | FLAGGED | MERGE |
| `blind_core_audit_063` | SKIP | SKIP | SKIP |
| `blind_core_audit_066` | SKIP | SKIP | FLAGGED |
| `blind_core_audit_069` | SKIP | SKIP | FLAGGED |
| `blind_core_audit_089` | SKIP | SKIP | FLAGGED |
| `known_missed_merge_patterns_002` | MERGE | FLAGGED | SKIP |

## Recommendation

- Status: `candidate_failed_full_gate`
- Reason: The hybrid contender did not clear the frozen production or fallback gate on the full benchmark.
