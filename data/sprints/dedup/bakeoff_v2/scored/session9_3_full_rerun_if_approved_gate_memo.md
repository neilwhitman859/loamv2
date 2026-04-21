# Session 9.3 Gate Memo

Run: `session9_3_full_rerun_if_approved`
Date: 2026-04-21

## Result

The proof-first rerun succeeded technically and cleared the proof stop rule again on fresh `v2.1` packets, but **no contender cleared the frozen Session 4 production gate or fallback gate** on the full 152-case benchmark.

## What Improved

- The unresolved-official backstop held at full scale: the dangerous continuity false-merge cluster was materially reduced.
- `gemini_guardrailed_v2` reached **0 false merges** on the full benchmark and **0 blind-core false merges**.
- `sonnet_gemini_consensus_v2` also reached **0 false merges** on the full benchmark and **0 blind-core false merges**.
- `sonnet_guardrailed_v2` cut false merges to **1 total** and **1 blind-core false merge**.
- Auditability stayed perfect across the rerun: `schema_valid_rate = 1.00`, `citation_integrity_rate = 1.00`, and `rule_trace_rate = 1.00` for every contender.

## Why The Gates Still Failed

- `sonnet_guardrailed_v2` remained too permissive to be safe enough for the frozen bar: **1 false merge**, **5 hard missed merges**, **44 soft missed merges**, **40.13% flag rate**.
- `gemini_guardrailed_v2` became safe on false merges but too conservative to execute: **0 false merges**, **9 hard missed merges**, **42 soft missed merges**, **38.82% flag rate**.
- `sonnet_gemini_consensus_v2` stayed safest on false merges but over-flagged even harder: **0 false merges**, **4 hard missed merges**, **47 soft missed merges**, **48.68% flag rate**.
- The main blocker is no longer unsafe continuity merge logic. It is now **recall collapse plus queue burden**: too many real merges are being flagged or skipped under the new safety backstop.

## Cost

- Proof subset actual model spend: **$0.3846**
- Full rerun actual model spend: **$1.5660**
- Session 9.3 total spend: **$1.9506**

## Decision For Sprint 6

Queue-building remains blocked. The approved rerun answered the key question: the proof-cleared `v2.1` packet path is safer, but it is **still not execution-ready** under the frozen gates.

## Recommended Next Session

Run a focused post-rerun failure audit to determine whether one more small redesign can recover merge recall without reopening the false-merge problem, or whether the adjudication path should be frozen as non-execution-ready and reconsidered at a higher level.
