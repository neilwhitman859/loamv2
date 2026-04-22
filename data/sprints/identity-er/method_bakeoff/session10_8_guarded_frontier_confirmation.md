# Session 10.8 - guarded frontier confirmation

- Generated: 2026-04-21
- Candidate: `hybrid_guarded_frontier_v1`
- Frozen benchmark/gates: `benchmark_v1` + Session 4 production gate

## Method

`hybrid_guarded_frontier_v1` is a conservative follow-on to
`hybrid_signature_plus_judge_v1`.

Shape:

1. keep the frozen Session 9.7 layered-safety control
2. apply the four deterministic safe promotions already pressure-checked on the
   full benchmark
3. route only the narrow shared-surname frontier to the model judge
4. apply four ambiguity guards after the frontier output:
   - `invalid_output_reuse_control`
   - `keep_flagged_not_skip`
   - `merge_veto_duplicate_secondary_on_shared_surname`
   - `generic_stub_ambiguity_flag`

Interpretation:

- the method is not “more optimistic”
- the method is “more honest about uncertainty” when the frontier evidence is
  visibly ambiguous or retrieval-collapsed

## Why This Candidate Emerged

The raw hybrid source run taught two things:

- the deterministic promotion core was genuinely safe and recovered
  `blind_core_audit_001`, `blind_core_audit_012`, `blind_core_audit_019`,
  `blind_core_audit_024`, and `known_missed_merge_patterns_011`
- the frontier judge's failures came from over-hardening uncertainty:
  `blind_core_audit_062` was merged off duplicated secondary retrieval, and
  `blind_core_audit_016` was treated as a decisive `SKIP` even though the
  visible evidence stayed ambiguous enough that `FLAGGED` is the more honest
  outcome

## Quantified Runs

| Runner | Source model spend | False merges | Hard missed | Soft missed | Merge capture | Exact acc | Production | Fallback |
|---|---:|---:|---:|---:|---:|---:|---|---|
| `session10_8_hybrid_guarded_frontier_sonnet_v1` | $0.5392 | 0 | 2 | 2 | 0.9216 | 0.8553 | pass | pass |
| `session10_8_hybrid_guarded_frontier_sonnet_rerun_v1` | $0.5382 | 0 | 2 | 2 | 0.9216 | 0.8553 | pass | pass |
| `session10_8_hybrid_guarded_frontier_opus_v1` | $2.9662 | 0 | 2 | 2 | 0.9216 | 0.8618 | pass | pass |
| `session10_8_hybrid_guarded_frontier_opus_rerun_v1` | $2.9964 | 0 | 2 | 2 | 0.9216 | 0.8618 | pass | pass |

## Stable Guard Changes

The same two substantive ambiguity corrections held across the confirmed runs:

- `blind_core_audit_062`: source hybrid `MERGE` -> guarded `FLAGGED`
- `blind_core_audit_016`: source hybrid `SKIP` -> guarded `FLAGGED`

The remaining repeated correction was conservative anti-hardening:

- `known_missed_merge_patterns_002`: source hybrid `SKIP` -> guarded `FLAGGED`

Contract-failure cleanup also behaved as intended:

- when the frontier model produced a schema-invalid or truncated row, the
  guarded method reused the frozen base control verdict rather than scoring a
  broken child output

## Why The Pass Looks Real

- The pass is not hanging on one lucky model choice.
- The pass reproduced from both a Sonnet-backed and an Opus-backed source run.
- The Sonnet-backed source was rerun fresh and the guarded method reproduced
  the same pass.
- The Opus-backed source was rerun fresh and the guarded method reproduced the
  same pass.
- The decisive improvements are conservative, auditable guardrails rather than
  broad new merge permissions.

## Main Residual Risk

This candidate still depends on a small set of retrieval-ambiguity guardrails.
That is promising, but it means the next honest question is no longer “can we
find any passing method?” It is “does this passing method generalize beyond the
frozen benchmark and its known retrieval-collapse patterns?”
