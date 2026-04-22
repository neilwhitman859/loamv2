# Session 10.8 - hybrid guarded frontier

- Generated: 2026-04-21T21:57:11-04:00
- Run name: `session10_8_hybrid_guarded_frontier_opus_rerun_v1`
- Source run: `session10_8_hybrid_signature_plus_judge_opus_rerun_v1`
- Source model: `claude-opus-4-6`
- Method version: `hybrid_guarded_frontier_v1`
- Estimated inherited model spend: `$2.9964`

## Goal

Test whether a conservative post-judge ambiguity guard can turn the promising hybrid contender into a real production-gate survivor without widening policy or adding another heavy pass.

## Guard Changes

- `blind_core_audit_016`: `SKIP` -> `FLAGGED` via `generic_stub_ambiguity_flag`
- `blind_core_audit_032`: `FLAGGED` -> `SKIP` via `invalid_output_reuse_control`
- `blind_core_audit_041`: `FLAGGED` -> `SKIP` via `invalid_output_reuse_control`
- `blind_core_audit_061`: `FLAGGED` -> `SKIP` via `invalid_output_reuse_control`
- `blind_core_audit_062`: `MERGE` -> `FLAGGED` via `merge_veto_duplicate_secondary_on_shared_surname`
- `blind_core_audit_066`: `FLAGGED` -> `SKIP` via `invalid_output_reuse_control`
- `blind_core_audit_069`: `FLAGGED` -> `SKIP` via `invalid_output_reuse_control`
- `blind_core_audit_089`: `FLAGGED` -> `SKIP` via `invalid_output_reuse_control`
- `known_missed_merge_patterns_002`: `SKIP` -> `FLAGGED` via `keep_flagged_not_skip`

## Scorecard

- Counts: false merge `0`, hard missed `2`, soft missed `2`, safe flag `17`.
- Rates: exact acc `0.8618`, merge capture `0.9216`, survivor acc `1.0000`, flag rate `0.1250`.
- Gates: production `pass`, fallback `pass`.

## Delta Vs Control

- Recoveries vs control: `blind_core_audit_001`, `blind_core_audit_012`, `blind_core_audit_019`, `blind_core_audit_024`, `known_missed_merge_patterns_011`
- Blind-core recoveries vs control: `blind_core_audit_001`, `blind_core_audit_012`, `blind_core_audit_019`, `blind_core_audit_024`
- New false merges vs control: none

## Touched Cases

| Case | Expected | Control | Source Hybrid | Guarded Contender |
|---|---|---|---|---|
| `blind_core_audit_016` | MERGE | SKIP | SKIP | FLAGGED |
| `blind_core_audit_032` | SKIP | SKIP | FLAGGED | SKIP |
| `blind_core_audit_041` | SKIP | SKIP | FLAGGED | SKIP |
| `blind_core_audit_061` | SKIP | SKIP | FLAGGED | SKIP |
| `blind_core_audit_062` | SKIP | FLAGGED | MERGE | FLAGGED |
| `blind_core_audit_066` | SKIP | SKIP | FLAGGED | SKIP |
| `blind_core_audit_069` | SKIP | SKIP | FLAGGED | SKIP |
| `blind_core_audit_089` | SKIP | SKIP | FLAGGED | SKIP |
| `known_missed_merge_patterns_002` | MERGE | FLAGGED | SKIP | FLAGGED |

## Recommendation

- Status: `candidate_clears_frozen_production_gate`
- Reason: The guarded frontier follow-on cleared the frozen production gate by preventing the frontier layer from hardening or merging beyond what the visible evidence can safely support.
