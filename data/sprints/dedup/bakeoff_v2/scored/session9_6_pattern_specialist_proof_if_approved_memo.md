# Session 9.6 - routed pattern specialist proof memo

- Generated: 2026-04-21T11:17:21-04:00
- Run name: `session9_6_pattern_specialist_proof_if_approved`
- Base path reused outside routed bundle: `gemini_guardrailed_v2` from `session9_3_full_rerun_if_approved`
- Routed families: `11.4.h`, `11.4.f`, `11.4.n`, `11.4.p`
- Routed specialist model: `google/gemini-3-flash-preview`
- Proof verdict: FAIL

## Composite scorecard

| Metric | Base Gemini | Routed specialist composite |
|---|---:|---:|
| False merges overall | 0 | 5 |
| Blind-core false merges | 0 | 0 |
| Blind-core missed merges (hard + soft) | 30 | 5 |
| Routed merge recoveries | 0 / 47 | 42 / 47 |
| Full-benchmark flag rate | 0.3882 | 0.1053 |
| Exact verdict accuracy | 0.5526 | 0.8224 |

## Routed family breakdown

| Family | Cases | Expected merges | Recovered merges | False merges | Hard missed | Soft missed | Safe flag |
|---|---:|---:|---:|---:|---:|---:|---:|
| 11.4.h | 46 | 28 | 24 | 3 | 4 | 0 | 0 |
| 11.4.f | 13 | 8 | 7 | 2 | 1 | 0 | 0 |
| 11.4.n | 7 | 7 | 7 | 0 | 0 | 0 | 0 |
| 11.4.p | 7 | 4 | 4 | 0 | 0 | 0 | 0 |

## Success bar check

- FAIL: 0 false merges overall
- PASS: 0 blind-core false merges
- PASS: blind-core missed merges <= 5
- PASS: at least 30 / 47 targeted merges recovered
- PASS: full-benchmark flag_rate_total <= 0.25

## Readout

The routed-specialist proof did not clear the Session 9.6 bar. The family-routed redesign improved recall, but not enough to justify further build without first accepting a lower quality bar or a broader redesign.
Session 9.6 incremental spend was `$0.12`. Reused base Gemini rows outside the routed bundle were carried forward at zero additional cost for this proof.

Recovered merge cases:
- `blind_core_audit_002`
- `blind_core_audit_003`
- `blind_core_audit_004`
- `blind_core_audit_005`
- `blind_core_audit_006`
- `blind_core_audit_007`
- `blind_core_audit_008`
- `blind_core_audit_009`
- `blind_core_audit_010`
- `blind_core_audit_011`
- `blind_core_audit_013`
- `blind_core_audit_014`
- `blind_core_audit_015`
- `blind_core_audit_017`
- `blind_core_audit_018`
- `blind_core_audit_020`
- `blind_core_audit_021`
- `blind_core_audit_022`
- `blind_core_audit_023`
- `blind_core_audit_025`
- `blind_core_audit_026`
- `blind_core_audit_027`
- `blind_core_audit_028`
- `blind_core_audit_029`
- `blind_core_audit_030`
- `known_missed_merge_patterns_003`
- `known_missed_merge_patterns_004`
- `known_missed_merge_patterns_005`
- `known_missed_merge_patterns_006`
- `known_missed_merge_patterns_007`
- `known_missed_merge_patterns_009`
- `known_missed_merge_patterns_010`
- `known_missed_merge_patterns_012`
- `known_missed_merge_patterns_013`
- `known_missed_merge_patterns_014`
- `known_missed_merge_patterns_015`
- `known_missed_merge_patterns_016`
- `tail_random_sample_001`
- `tail_random_sample_002`
- `tail_random_sample_003`
- `tail_random_sample_004`
- `tail_random_sample_005`
