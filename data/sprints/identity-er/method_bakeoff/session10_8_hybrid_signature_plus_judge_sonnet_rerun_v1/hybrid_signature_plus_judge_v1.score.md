# session10_8_hybrid_signature_plus_judge_sonnet_rerun_v1 - Session 4 bakeoff proof scorecard

- Benchmark: `producer_dedup_benchmark_v1`
- Cases scored: 152 / 152
- Full benchmark run: yes

| Contender | Exact acc | False merge | Hard missed merge | Soft missed merge | Safe flag | Auditability | Gate status |
|---|---:|---:|---:|---:|---:|---:|---|
| hybrid_signature_plus_judge_v1 | 0.8487 | 1 | 4 | 0 | 18 | 0.9934 | fail |

## Winner selection table

| Contender | Eligibility | Production gate | Fallback gate |
|---|---|---|---|
| hybrid_signature_plus_judge_v1 | ineligible | fail | fail |