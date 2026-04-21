# session9_2_unresolved_official_backstop_proof_subset - adjudication bakeoff v2

- Generated: 2026-04-21T07:48:42-04:00
- Benchmark: `producer_dedup_benchmark_v1`
- Cases scored: 36 / 152
- Full benchmark run: no

| Contender | Exact acc | False merge | Hard missed | Soft missed | Safe flag | Auditability | Flag rate | Production gate | Fallback gate |
|---|---:|---:|---:|---:|---:|---:|---:|---|---|
| deterministic_control_v1 | 0.4167 | 1 | 8 | 6 | 6 | 1.0000 | 0.3333 | not_applicable_incomplete_benchmark | not_applicable_incomplete_benchmark |
| sonnet_guardrailed_v2 | 0.3889 | 0 | 2 | 12 | 8 | 1.0000 | 0.5556 | not_applicable_incomplete_benchmark | not_applicable_incomplete_benchmark |
| gemini_guardrailed_v2 | 0.3889 | 0 | 6 | 9 | 7 | 1.0000 | 0.4444 | not_applicable_incomplete_benchmark | not_applicable_incomplete_benchmark |
| sonnet_gemini_consensus_v2 | 0.3056 | 0 | 1 | 14 | 10 | 1.0000 | 0.6667 | not_applicable_incomplete_benchmark | not_applicable_incomplete_benchmark |
