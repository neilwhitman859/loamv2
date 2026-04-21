# session9_7_layered_safety_det_only - Session 4 bakeoff proof scorecard

- Benchmark: `producer_dedup_benchmark_v1`
- Cases scored: 152 / 152
- Full benchmark run: yes

| Contender | Exact acc | False merge | Hard missed merge | Soft missed merge | Safe flag | Auditability | Gate status |
|---|---:|---:|---:|---:|---:|---:|---|
| layered_safety_det_only_v1 | 0.8224 | 2 | 6 | 3 | 16 | 1.0000 | fail |

## Winner selection table

| Contender | Eligibility | Production gate | Fallback gate |
|---|---|---|---|
| layered_safety_det_only_v1 | ineligible | fail | fail |