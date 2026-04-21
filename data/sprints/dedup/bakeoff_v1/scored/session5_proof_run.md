# session5_proof_run - Session 4 bakeoff proof scorecard

- Benchmark: `producer_dedup_benchmark_v1`
- Cases scored: 8 / 152
- Full benchmark run: no

| Contender | Exact acc | False merge | Hard missed merge | Soft missed merge | Safe flag | Auditability | Gate status |
|---|---:|---:|---:|---:|---:|---:|---|
| deterministic_control_v1 | 0.3750 | 0 | 3 | 2 | 0 | 1.0000 | not_applicable_incomplete_benchmark |

## Winner selection table

| Contender | Eligibility | Production gate | Fallback gate |
|---|---|---|---|
| deterministic_control_v1 | ineligible | not_applicable_incomplete_benchmark | not_applicable_incomplete_benchmark |