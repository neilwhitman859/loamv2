# session9_11_full_method_bakeoff_rerun_if_approved - Session 4 bakeoff proof scorecard

- Benchmark: `producer_dedup_benchmark_v1`
- Cases scored: 152 / 152
- Full benchmark run: yes

| Contender | Exact acc | False merge | Hard missed merge | Soft missed merge | Safe flag | Auditability | Gate status |
|---|---:|---:|---:|---:|---:|---:|---|
| layered_safety_sonnet_r2_narrow_v1 | 0.8289 | 0 | 6 | 3 | 17 | 1.0000 | fail |
| merge_proposer_plus_veto_v1 | 0.8158 | 9 | 3 | 1 | 15 | 1.0000 | fail |
| expanded_layered_router_v1 | 0.8355 | 5 | 4 | 1 | 15 | 1.0000 | fail |
| evidence_digest_then_judge_v1 | 0.8224 | 5 | 5 | 1 | 16 | 1.0000 | fail |

## Winner selection table

| Contender | Eligibility | Production gate | Fallback gate |
|---|---|---|---|
| layered_safety_sonnet_r2_narrow_v1 | fallback_only | fail | pass |
| merge_proposer_plus_veto_v1 | ineligible | fail | fail |
| expanded_layered_router_v1 | ineligible | fail | fail |
| evidence_digest_then_judge_v1 | ineligible | fail | fail |