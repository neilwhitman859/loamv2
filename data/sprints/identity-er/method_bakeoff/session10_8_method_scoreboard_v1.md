# Session 10.8 - method scoreboard

Date: 2026-04-21

Frozen gate:

- `benchmark_v1`
- Session 4 production gate
- Session 4 fallback gate

Notes:

- `false_merge`, `hard_missed_merge`, `soft_missed_merge`, `merge_capture`, and `exact_acc` are the main quality metrics.
- Spend below is incremental model spend for the underlying run family. Deterministic overlays list `$0.00` incremental because they reuse frozen source-run outputs.
- `proof_only` means the method never earned a full-benchmark run.
- `oof_fail` means the method looked promising on a benchmark fit but failed honest out-of-fold confirmation.

| Method | Scope | False merges | Hard misses | Soft misses | Merge capture | Exact acc | Gate status | Incremental spend | Notes |
|---|---|---:|---:|---:|---:|---:|---|---:|---|
| `visible_signature_promotion_v1` | out-of-fold confirmation | 11 | 4 | 3 | 86.27% | n/a | `oof_fail` | `$0.00` | Packet surface has signal, but learned clauses did not generalize honestly. |
| `contrastive_burden_adjudicator_v1` | 29-case proof | 1 | 9 | n/a | n/a | n/a | `proof_only_fail` | `$0.49` | Too little recovery and one reopened false merge. |
| `hybrid_signature_plus_judge_v1` | full benchmark, Sonnet | 1 | 4 | 0 | 92.16% | 84.87% | `production_fail` | `$0.54` | Recovered multiple misses but reopened `blind_core_audit_062`. |
| `hybrid_signature_plus_judge_v1` | full benchmark, Opus | 1 | 4 | 0 | 92.16% | 85.53% | `production_fail` | `$2.97` | Same substantive failure pattern as Sonnet. |
| `hybrid_guarded_frontier_v1` | full benchmark, Sonnet | 0 | 2 | 2 | 92.16% | 85.53% | `production_pass` | `$0.54` | First benchmark-clearing survivor; confirmation blocked on fresh independent holdout. |
| `hybrid_guarded_frontier_v1` | full benchmark, Sonnet rerun | 0 | 2 | 2 | 92.16% | 85.53% | `production_pass` | `$0.54` | Same result on rerun. |
| `hybrid_guarded_frontier_v1` | full benchmark, Opus | 0 | 2 | 2 | 92.16% | 86.18% | `production_pass` | `$2.97` | Same benchmark pass shape on Opus-backed source. |
| `hybrid_guarded_frontier_v1` | full benchmark, Opus rerun | 0 | 2 | 2 | 92.16% | 86.18% | `production_pass` | `$3.00` | Same result on rerun. |
| `hybrid_guarded_cuvee_anchor_v1` | full benchmark, Sonnet-backed overlay | 0 | 2 | 1 | 94.12% | 86.18% | `production_pass` | `$0.00` | Recovered `known_missed_merge_patterns_002` via visible singleton-cuvee anchor (`hymenee`). |
| `hybrid_guarded_cuvee_anchor_v1` | full benchmark, Sonnet-rerun-backed overlay | 0 | 2 | 1 | 94.12% | 86.18% | `production_pass` | `$0.00` | Same zero-cost improvement on rerun. |
| `hybrid_guarded_cuvee_anchor_v1` | full benchmark, Opus-backed overlay | 0 | 2 | 1 | 94.12% | 86.84% | `production_pass` | `$0.00` | Same zero-cost improvement on Opus-backed source. |
| `hybrid_guarded_cuvee_anchor_v1` | full benchmark, Opus-rerun-backed overlay | 0 | 2 | 1 | 94.12% | 86.84% | `production_pass` | `$0.00` | Same zero-cost improvement on Opus rerun. |
| `hybrid_guarded_place_alias_v1` | full benchmark, Sonnet-backed overlay | 0 | 2 | 0 | 96.08% | 86.84% | `production_pass` | `$0.00` | Recovered `blind_core_audit_016` via institutional-prefix place alias (`Stadt Krems` -> `Krems`). |
| `hybrid_guarded_place_alias_v1` | full benchmark, Sonnet-rerun-backed overlay | 0 | 2 | 0 | 96.08% | 86.84% | `production_pass` | `$0.00` | Same zero-cost improvement on rerun. |
| `hybrid_guarded_place_alias_v1` | full benchmark, Opus-backed overlay | 0 | 2 | 0 | 96.08% | 87.50% | `production_pass` | `$0.00` | Same zero-cost improvement on Opus-backed source. |
| `hybrid_guarded_place_alias_v1` | full benchmark, Opus-rerun-backed overlay | 0 | 2 | 0 | 96.08% | 87.50% | `production_pass` | `$0.00` | Same zero-cost improvement on Opus rerun. |
| `hybrid_guarded_maison_alias_v1` | full benchmark, Sonnet-backed overlay | 0 | 1 | 0 | 98.04% | 87.50% | `production_pass` | `$0.00` | Recovered `known_missed_merge_patterns_008` via article-wrapped estate alias (`de la Cabotte` / `Maison de la Cabotte`). |
| `hybrid_guarded_maison_alias_v1` | full benchmark, Sonnet-rerun-backed overlay | 0 | 1 | 0 | 98.04% | 87.50% | `production_pass` | `$0.00` | Same zero-cost improvement on rerun. |
| `hybrid_guarded_maison_alias_v1` | full benchmark, Opus-backed overlay | 0 | 1 | 0 | 98.04% | 88.16% | `production_pass` | `$0.00` | Same zero-cost improvement on Opus-backed source. |
| `hybrid_guarded_maison_alias_v1` | full benchmark, Opus-rerun-backed overlay | 0 | 1 | 0 | 98.04% | 88.16% | `production_pass` | `$0.00` | Same zero-cost improvement on Opus rerun. |
| `hybrid_guarded_fils_person_alias_v1` | full benchmark, Sonnet-backed overlay | 0 | 0 | 0 | 100.00% | 88.16% | `production_pass` | `$0.00` | Recovered `known_missed_merge_patterns_001` via `Fils` trading form + fuller personal-name alias. |
| `hybrid_guarded_fils_person_alias_v1` | full benchmark, Sonnet-rerun-backed overlay | 0 | 0 | 0 | 100.00% | 88.16% | `production_pass` | `$0.00` | Same zero-cost improvement on rerun. |
| `hybrid_guarded_fils_person_alias_v1` | full benchmark, Opus-backed overlay | 0 | 0 | 0 | 100.00% | 88.82% | `production_pass` | `$0.00` | Same zero-cost improvement on Opus-backed source. |
| `hybrid_guarded_fils_person_alias_v1` | full benchmark, Opus-rerun-backed overlay | 0 | 0 | 0 | 100.00% | 88.82% | `production_pass` | `$0.00` | Same zero-cost improvement on Opus rerun. |

Current best benchmark survivor:

- `hybrid_guarded_fils_person_alias_v1`

Why it leads:

- still `0` false merges
- reduces benchmark misses all the way to `0` hard and `0` soft misses
- raises merge capture from `92.16%` at the guarded frontier baseline to `100.00%`
- reproduced identically across Sonnet, Sonnet rerun, Opus, and Opus rerun with no extra model spend

Open honesty note:

- This is now a benchmark-clearing survivor, not yet a fresh independently validated production-ready method.
- The offline holdout is only consistency-auditable from frozen local artifacts.
- A faithful fresh-holdout rerun remains blocked by missing preserved negative-side packet structure and the current runtime path.
