# session9_6_pattern_specialist_proof_if_approved - Session 4 bakeoff proof scorecard

- Benchmark: `producer_dedup_benchmark_v1`
- Cases scored: 152 / 152
- Full benchmark run: yes

| Contender | Exact acc | False merge | Hard missed merge | Soft missed merge | Safe flag | Auditability | Gate status |
|---|---:|---:|---:|---:|---:|---:|---|
| gemini_routed_pattern_specialist_v1 | 0.8224 | 5 | 6 | 3 | 13 | 1.0000 | fail |

## Winner selection table

| Contender | Eligibility | Production gate | Fallback gate |
|---|---|---|---|
| gemini_routed_pattern_specialist_v1 | ineligible | fail | fail |