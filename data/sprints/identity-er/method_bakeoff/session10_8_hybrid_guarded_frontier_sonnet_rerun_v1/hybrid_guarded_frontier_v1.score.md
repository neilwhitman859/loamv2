# session10_8_hybrid_guarded_frontier_sonnet_rerun_v1 - Session 4 bakeoff proof scorecard

- Benchmark: `producer_dedup_benchmark_v1`
- Cases scored: 152 / 152
- Full benchmark run: yes

| Contender | Exact acc | False merge | Hard missed merge | Soft missed merge | Safe flag | Auditability | Gate status |
|---|---:|---:|---:|---:|---:|---:|---|
| hybrid_guarded_frontier_v1 | 0.8553 | 0 | 2 | 2 | 18 | 1.0000 | pass |

## Winner selection table

| Contender | Eligibility | Production gate | Fallback gate |
|---|---|---|---|
| hybrid_guarded_frontier_v1 | production_eligible | pass | pass |