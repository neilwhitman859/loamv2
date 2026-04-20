# Phase 2 Risk Analysis

What could go wrong if the 3,939 auto-apply decisions in
`phase2_actionable_decisions.jsonl` are executed against the DB without
further review, based on what B6.6 learned from the Phase 1 re-Chrome.

## Phase 2 scope recap

| action | count | if executed |
|---|---|---|
| `auto_apply_merge` | 3,154 | 3,154 producer rows soft-deleted; FKs re-pointed |
| `auto_apply_pc` | 785 | 785 child rows get parent_producer_id set |
| `user_review_pc` | 470 | **not auto-applied** — require user review |
| `user_review_merge_lowconf` | 246 | **not auto-applied** |
| Other review queues | ~24 | **not auto-applied** |

So "auto-apply" at face value means 3,939 mutations. The review queues (~740)
are already routed to the user — they should flow through a human curation
step before any DB write.

## What Phase 1 re-Chrome revealed

Phase 1 Chrome-validated 493 pairs — these were the pipeline's hardest
escalations. B6.6 re-Chrome flipped **17% of them** (33 of 193 MERGE+PC
decisions). Most flips were on shared-surname / generic-château-name MERGEs
where the DB wine list spanned incompatible regions.

Critical observation: **the flipped pairs were in the `user_review_*`
escalation queues**, not in the `auto_apply` queues. The pipeline routed them
to human review precisely because Stage 1 and Stage 2 disagreed. So the
escalation logic was doing its job on the hard cases.

## The extrapolation question

If `auto_apply` decisions share the hardest cases' ~17% flip rate:
- 3,154 × 17% = ~536 bad MERGEs
- 785 × 17% = ~133 bad PCs

If `auto_apply` decisions are actually more reliable (because they represent
L1+L1.5 **consensus** on easier pairs):
- Flip rate could be 2-5%
- 3,154 × 3% = ~95 bad MERGEs
- 785 × 3% = ~24 bad PCs

Without sampling, we don't know which regime applies. **Best-case estimate:
~100 bad decisions across 3,939 auto-applies. Worst-case: ~700.**

## Why the auto-apply rate is probably lower than 17%

Several signals:

1. **The 17% was measured on Chrome-escalated pairs** — the pipeline
   specifically routed these to human review because they were ambiguous.
   They are, by construction, the hardest decisions.
2. **Auto-apply requires L1+L1.5 consensus at confidence ≥ 0.88** (B6.4
   committed thresholds). Cross-model agreement on clear-case pairs is high.
3. **Common patterns in auto-apply:**
   - §11.4.h identical-normalized name across country splits
   - §11.4.n global brand (Tussock Jumper, Selaks) with clear shared identity
   - Exact orthographic variants (accent / punctuation only)
4. **The weakest auto-applies surface in `phase2_summary.md`** — confidences
   typically ≥ 0.85 across multiple models.

## Why the auto-apply rate could still be non-trivial

1. **Shared-surname pattern failures weren't caught by the pipeline at Stage 1**
   — Boisson (Rhône / Burgundy), Beausejour (10+ châteaux), Jolivet
   (Sancerre / Saint-Joseph) all produced MERGE consensus in the ladder that
   only Chrome web lookup revealed as wrong.
2. **L1+L1.5 are both single-prompt models** — they rely on name + country
   signals. If both models mistake "Boisson (FR, Cairanne)" and "Boisson (FR,
   Meursault)" as variants of each other, consensus reinforces the error.
3. **The DB wine-list region incompatibility signal** (the single most
   reliable B6.6 red flag) **isn't in the L1+L1.5 prompts.** It showed up in
   L2+L2.5 rich prompts, which are only run on Stage 2 escalations.
4. **Cross-country same-brand auto-merges** (§11.4.n) are mostly correct but
   can misfire when two unrelated brands happen to share a name across
   countries (e.g. "Wolf Blass AU" vs "Wolf Blass US" = same; "Chalk Hill US"
   vs "Chalk Hill AU" = different).

## Recommended handling

**If Phase 1 is executed first (and passes user testing):**

1. **Sample the auto-apply queue** — 200-300 pairs stratified by:
   - pattern cluster (if identifiable from verdict overlap)
   - country match vs country split
   - wine-count bucket
2. **Do Chrome validation on the sample** — re-use the B6.5a per-pair Chrome
   protocol.
3. **Measure actual flip rate.** If <5%, proceed with the full auto-apply
   queue after fixing the sampled flips. If 5-15%, tighten thresholds or
   re-route the risky subset to review. If >15%, halt and treat the
   auto-apply queue like Phase 1 (full Chrome re-validation).
4. **Execute auto-apply SKIPs freely.** The 146,435 SKIPs are no-ops; their
   risk is only "false SKIP" (missing a real MERGE), which is a recall loss
   not a data corruption.

**If user review queues are handled separately:**

1. The 470 `user_review_pc` + 246 `user_review_merge_lowconf` + smaller
   review queues need a human (or another AI-assisted) pass. Format them
   as a single review queue with context bundles, similar to how B6.5a was
   scoped.
2. Estimated effort: ~2-3 hours of interactive review, $0 AI cost (inline
   Opus), or ~$3-5 if using a subagent approach.

## What's NOT at risk

- `auto_apply_skip` rows (146,435) — these are no-ops. Worst case is false
  negative (a real MERGE classified as SKIP). Phase 2 doesn't write anything
  for these, so there's no DB corruption risk.
- Already-executed decisions — there are none. `producer_merge_history` is
  empty.

## TL;DR

**Execute Phase 1 (this bundle's 493-pair ledger) freely** — it's been
Chrome-validated twice and has 17 canonical-row redirects to prevent
duplicate-creation.

**Do NOT execute Phase 2 (`phase2_actionable_decisions.jsonl`) without a
sampled audit.** The auto-apply MERGEs have a plausible-but-unmeasured error
rate; the user-review queues need human judgment. Best cost-risk trade-off
is a 200-pair sampled Chrome audit of the auto-apply queue before execution.

Phase 2 data is packaged here for external review — **not as a ready-to-run
execution plan.** The intent is to make the full pipeline state visible to
another AI reviewer.
