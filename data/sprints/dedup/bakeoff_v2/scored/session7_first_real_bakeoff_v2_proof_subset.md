# session7_first_real_bakeoff_v2_proof_subset - adjudication bakeoff v2

- Generated: 2026-04-20T23:33:40-04:00
- Benchmark: `producer_dedup_benchmark_v1`
- Cases scored: 28 / 152
- Full benchmark run: no

| Contender | Exact acc | False merge | Hard missed | Soft missed | Safe flag | Auditability | Flag rate | Production gate | Fallback gate |
|---|---:|---:|---:|---:|---:|---:|---:|---|---|
| deterministic_control_v1 | 0.3214 | 9 | 0 | 6 | 4 | 1.0000 | 0.3571 | not_applicable_incomplete_benchmark | not_applicable_incomplete_benchmark |
| sonnet_guardrailed_v2 | 0.5357 | 10 | 3 | 0 | 0 | 1.0000 | 0.0000 | not_applicable_incomplete_benchmark | not_applicable_incomplete_benchmark |
| gemini_guardrailed_v2 | 0.7500 | 7 | 0 | 0 | 0 | 1.0000 | 0.0000 | not_applicable_incomplete_benchmark | not_applicable_incomplete_benchmark |
| sonnet_gemini_consensus_v2 | 0.5000 | 7 | 0 | 4 | 3 | 1.0000 | 0.2500 | not_applicable_incomplete_benchmark | not_applicable_incomplete_benchmark |
