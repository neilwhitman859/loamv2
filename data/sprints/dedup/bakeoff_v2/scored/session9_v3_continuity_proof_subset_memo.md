# session9_v3_continuity_proof_subset - proof memo

- Result: FAIL
- Reused base cases: 28
- Continuity add-on cases: 8

| Contender | Add-on false merges | Base false merges (prior -> current) |
|---|---|---|
| sonnet_guardrailed_v2 | blind_core_audit_048, blind_core_audit_080 | 10 -> 1 |
| gemini_guardrailed_v2 | blind_core_audit_048, tail_random_sample_008 | 7 -> 0 |
| sonnet_gemini_consensus_v2 | blind_core_audit_048 | 7 -> 0 |

Failures:
- sonnet_guardrailed_v2 false-merged continuity add-on cases: blind_core_audit_048, blind_core_audit_080
- gemini_guardrailed_v2 false-merged continuity add-on cases: blind_core_audit_048, tail_random_sample_008
- sonnet_gemini_consensus_v2 false-merged continuity add-on cases: blind_core_audit_048

Recommendation: stop here. Do not run the 152-case rerun.
