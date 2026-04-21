# session9_v3_continuity_proof_subset - adjudication bakeoff v2

- Generated: 2026-04-21T07:18:32-04:00
- Benchmark: `producer_dedup_benchmark_v1`
- Cases scored: 36 / 152
- Full benchmark run: no

| Contender | Exact acc | False merge | Hard missed | Soft missed | Safe flag | Auditability | Flag rate | Production gate | Fallback gate |
|---|---:|---:|---:|---:|---:|---:|---:|---|---|
| deterministic_control_v1 | 0.5000 | 3 | 8 | 3 | 4 | 1.0000 | 0.1944 | not_applicable_incomplete_benchmark | not_applicable_incomplete_benchmark |
| sonnet_guardrailed_v2 | 0.4444 | 3 | 4 | 6 | 7 | 1.0000 | 0.3611 | not_applicable_incomplete_benchmark | not_applicable_incomplete_benchmark |
| gemini_guardrailed_v2 | 0.5278 | 2 | 3 | 5 | 7 | 1.0000 | 0.3333 | not_applicable_incomplete_benchmark | not_applicable_incomplete_benchmark |
| sonnet_gemini_consensus_v2 | 0.3889 | 1 | 1 | 9 | 11 | 1.0000 | 0.5556 | not_applicable_incomplete_benchmark | not_applicable_incomplete_benchmark |
