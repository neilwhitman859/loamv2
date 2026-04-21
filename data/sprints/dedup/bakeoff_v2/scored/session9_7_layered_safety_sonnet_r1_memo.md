# Session 9.7 - layered safety gate memo

- Generated: 2026-04-21T12:00:52-04:00
- Run name: `session9_7_layered_safety_sonnet_r1`
- Contender id: `layered_safety_sonnet_r1_v1`
- Starting point: `session9_6_pattern_specialist_proof_if_approved` layered on `session9_3_full_rerun_if_approved`
- Safety reviewer: `sonnet_guardrailed_v2`
- Review scope after deterministic vetoes: `9` specialist merge proposals in `11.4.f`

## Composite scorecard

| Metric | Session 9.6 specialist composite | This run |
|---|---:|---:|
| False merges overall | 5 | 0 |
| Blind-core false merges | 0 | 0 |
| Blind-core missed merges (hard + soft) | 5 | 9 |
| Routed merge recoveries | 42 / 47 | 36 / 47 |
| Full-benchmark flag rate | 0.1053 | 0.1645 |
| Exact verdict accuracy | 0.8224 | 0.7895 |
| Production gate | fail | fail |
| Fallback gate | fail | pass |

## Session 9.6 Continuation Bar

- PASS: 0 false merges overall
- PASS: 0 blind-core false merges
- FAIL: blind-core missed merges <= 5
- PASS: at least 30 / 47 routed merges recovered
- PASS: full-benchmark flag rate <= 0.25

## Deterministic Vetoes

- `known_false_merge_patterns_005`: `shared_surname_without_catalog_or_region_bridge` (Bosio <-> Luca Bosio)
- `known_false_merge_patterns_007`: `shared_surname_without_catalog_or_region_bridge` (Bastida <-> Familia Bastida)
- `tail_random_sample_008`: `secondary_relationship_without_name_bridge` (La Tour du Pin <-> Tour du Pin Figeac)

## Safety Review Decisions

- `blind_core_audit_005`: `keep_merge` (Guy Castagnier <-> Castagnier)
- `blind_core_audit_008`: `veto_to_base` (Amiot Bonfils <-> Guy Amiot et Fils)
- `blind_core_audit_015`: `veto_to_base` (Henry Lamarche <-> Nicole Lamarche)
- `blind_core_audit_017`: `veto_to_base` (Benedikt Baltes <-> Bertram-Baltes)
- `blind_core_audit_030`: `veto_to_base` (Florent Rouve <-> Jean Rijckaert)
- `known_false_merge_patterns_011`: `veto_to_base` (Giovanni Giordano <-> Luigi Giordano)
- `known_false_merge_patterns_012`: `veto_to_base` (Confuron Gindre <-> Edouard Confuron)
- `known_missed_merge_patterns_009`: `veto_to_base` (Didier Herbert <-> Herbert & Co.)
- `known_missed_merge_patterns_012`: `veto_to_base` (Reyane et Pascal Bouley <-> Pierrick Bouley)

## Readout

The layered redesign eliminated the Session 9.6 false merges on the frozen benchmark.
Incremental reviewer spend in this round was `$0.36`. Composite contender cost reported in the scorecard includes carried specialist spend on routed cases and zero additional cost for reused base rows.
