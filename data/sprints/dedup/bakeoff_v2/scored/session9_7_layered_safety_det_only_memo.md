# Session 9.7 - layered safety gate memo

- Generated: 2026-04-21T11:58:30-04:00
- Run name: `session9_7_layered_safety_det_only`
- Contender id: `layered_safety_det_only_v1`
- Starting point: `session9_6_pattern_specialist_proof_if_approved` layered on `session9_3_full_rerun_if_approved`
- Safety reviewer: none (`deterministic_veto_only`)
- Review scope after deterministic vetoes: `9` specialist merge proposals in `11.4.f`

## Composite scorecard

| Metric | Session 9.6 specialist composite | This run |
|---|---:|---:|
| False merges overall | 5 | 2 |
| Blind-core false merges | 0 | 0 |
| Blind-core missed merges (hard + soft) | 5 | 5 |
| Routed merge recoveries | 42 / 47 | 42 / 47 |
| Full-benchmark flag rate | 0.1053 | 0.1250 |
| Exact verdict accuracy | 0.8224 | 0.8224 |
| Production gate | fail | fail |
| Fallback gate | fail | fail |

## Session 9.6 Continuation Bar

- FAIL: 0 false merges overall
- PASS: 0 blind-core false merges
- PASS: blind-core missed merges <= 5
- PASS: at least 30 / 47 routed merges recovered
- PASS: full-benchmark flag rate <= 0.25

## Deterministic Vetoes

- `known_false_merge_patterns_005`: `shared_surname_without_catalog_or_region_bridge` (Bosio <-> Luca Bosio)
- `known_false_merge_patterns_007`: `shared_surname_without_catalog_or_region_bridge` (Bastida <-> Familia Bastida)
- `tail_random_sample_008`: `secondary_relationship_without_name_bridge` (La Tour du Pin <-> Tour du Pin Figeac)

## Safety Review Decisions

- no model review decisions recorded

## Readout

The layered redesign improved safety versus Session 9.6, but it still left false merges on the frozen benchmark.
Incremental reviewer spend in this round was `$0.00`. Composite contender cost reported in the scorecard includes carried specialist spend on routed cases and zero additional cost for reused base rows.
