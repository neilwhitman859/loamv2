# Phase 2 — Pipeline Auto-Applied Decisions (routing_stage3)

These are decisions the AI ladder (L1→L1.5→L2→L2.5→L3) made without
needing Chrome-per-pair validation. They live in the DB table
`producer_dedup_routing_stage3` and are UNEXECUTED — nothing has been
written to the producers table yet (`producer_merge_history` is empty).

**Phase 1 (the 493-pair Chrome-validated ledger at `verdict_ledger.jsonl`)
is a SUBSET of this Phase 2 queue** — specifically, the pairs that were
escalated to Chrome validation. Phase 1 refines those 493 decisions with
per-pair web evidence.

## Action distribution

| stage3_action | Count | Type |
|---|---|---|
| `auto_apply_skip` | 146,435 | no-op |
| `auto_apply_merge` | 3,154 | mutation |
| `auto_apply_pc` | 785 | mutation |
| `user_review_pc` | 470 | needs review |
| `user_review_merge_lowconf` | 246 | needs review |
| `auto_apply_skip_missing` | 32 | no-op |
| `user_review_missing` | 23 | needs review |
| `auto_apply_skip_residual` | 4 | no-op |
| `user_review_merge_unvalidated` | 1 | needs review |
| **Total** | **151,150** | |

Actionable decisions (mutations + review queues): **4,679**
No-op decisions (SKIPs): **146,471**

## Core vs Tail (by `is_core`)

`is_core = TRUE` when max(wines_a, wines_b) ≥ 10 — i.e., the pair involves
at least one US-market-relevant producer.

| stage3_action | core_pairs | tail_pairs |
|---|---|---|
| `auto_apply_merge` | 2,086 | 1,068 |
| `auto_apply_pc` | 730 | 55 |
| `auto_apply_skip` | 100,602 | 45,833 |
| `auto_apply_skip_missing` | 0 | 32 |
| `auto_apply_skip_residual` | 2 | 2 |
| `user_review_merge_lowconf` | 136 | 110 |
| `user_review_merge_unvalidated` | 0 | 1 |
| `user_review_missing` | 0 | 23 |
| `user_review_pc` | 402 | 68 |

## Verdict agreement within actionable decisions

How the AI ladder classified each pair. `web_verdict` is present only when
the pair escalated to L3 Sonnet (w/o Chrome).

| stage3_action | L1 | L2 | web | count |
|---|---|---|---|---|
| `auto_apply_merge` | MERGE | - | MERGE | 1,280 |
| `auto_apply_merge` | SKIP | SKIP | MERGE | 652 |
| `auto_apply_merge` | MERGE | MERGE | MERGE | 565 |
| `auto_apply_merge` | PARENT_CHILD | MERGE | MERGE | 96 |
| `auto_apply_merge` | PARENT_CHILD | PARENT_CHILD | MERGE | 87 |
| `auto_apply_merge` | UNCERTAIN | UNCERTAIN | MERGE | 84 |
| `auto_apply_merge` | UNCERTAIN | SKIP | MERGE | 83 |
| `auto_apply_merge` | SKIP | MERGE | MERGE | 80 |
| `auto_apply_merge` | UNCERTAIN | MERGE | MERGE | 76 |
| `auto_apply_merge` | PARENT_CHILD | SKIP | MERGE | 37 |
| `auto_apply_merge` | SKIP | UNCERTAIN | MERGE | 31 |
| `auto_apply_merge` | SKIP | PARENT_CHILD | MERGE | 21 |
| `auto_apply_merge` | MERGE | SKIP | MERGE | 21 |
| `auto_apply_merge` | MERGE | UNCERTAIN | MERGE | 19 |
| `auto_apply_merge` | UNCERTAIN | PARENT_CHILD | MERGE | 15 |
| `auto_apply_merge` | - | - | MERGE | 5 |
| `auto_apply_merge` | SKIP | FLAGGED | MERGE | 1 |
| `auto_apply_merge` | PARENT_CHILD | UNCERTAIN | MERGE | 1 |
| `auto_apply_pc` | PARENT_CHILD | PARENT_CHILD | PARENT_CHILD | 341 |
| `auto_apply_pc` | SKIP | SKIP | PARENT_CHILD | 204 |
| `auto_apply_pc` | SKIP | PARENT_CHILD | PARENT_CHILD | 80 |
| `auto_apply_pc` | MERGE | MERGE | PARENT_CHILD | 35 |
| `auto_apply_pc` | PARENT_CHILD | SKIP | PARENT_CHILD | 26 |
| `auto_apply_pc` | PARENT_CHILD | MERGE | PARENT_CHILD | 24 |
| `auto_apply_pc` | MERGE | - | PARENT_CHILD | 15 |
| `auto_apply_pc` | UNCERTAIN | PARENT_CHILD | PARENT_CHILD | 14 |
| `auto_apply_pc` | UNCERTAIN | UNCERTAIN | PARENT_CHILD | 10 |
| `auto_apply_pc` | - | - | PARENT_CHILD | 7 |
| `auto_apply_pc` | UNCERTAIN | SKIP | PARENT_CHILD | 6 |
| `auto_apply_pc` | SKIP | UNCERTAIN | PARENT_CHILD | 6 |
| `auto_apply_pc` | UNCERTAIN | MERGE | PARENT_CHILD | 5 |
| `auto_apply_pc` | SKIP | MERGE | PARENT_CHILD | 4 |
| `auto_apply_pc` | MERGE | PARENT_CHILD | PARENT_CHILD | 3 |
| `auto_apply_pc` | MERGE | SKIP | PARENT_CHILD | 3 |
| `auto_apply_pc` | PARENT_CHILD | UNCERTAIN | PARENT_CHILD | 1 |
| `auto_apply_pc` | MERGE | UNCERTAIN | PARENT_CHILD | 1 |
| `user_review_merge_lowconf` | SKIP | SKIP | MERGE | 170 |
| `user_review_merge_lowconf` | MERGE | MERGE | MERGE | 14 |
| `user_review_merge_lowconf` | SKIP | MERGE | MERGE | 11 |
| `user_review_merge_lowconf` | UNCERTAIN | UNCERTAIN | MERGE | 10 |
| `user_review_merge_lowconf` | UNCERTAIN | SKIP | MERGE | 8 |
| `user_review_merge_lowconf` | SKIP | UNCERTAIN | MERGE | 6 |
| `user_review_merge_lowconf` | UNCERTAIN | MERGE | MERGE | 6 |
| `user_review_merge_lowconf` | PARENT_CHILD | MERGE | MERGE | 6 |
| `user_review_merge_lowconf` | PARENT_CHILD | PARENT_CHILD | MERGE | 5 |
| `user_review_merge_lowconf` | SKIP | PARENT_CHILD | MERGE | 2 |
| `user_review_merge_lowconf` | MERGE | - | MERGE | 2 |
| `user_review_merge_lowconf` | UNCERTAIN | PARENT_CHILD | MERGE | 2 |
| `user_review_merge_lowconf` | PARENT_CHILD | SKIP | MERGE | 2 |
| `user_review_merge_lowconf` | MERGE | SKIP | MERGE | 2 |
| _(+18 more combinations)_ | | | | |

## Country distribution of actionable decisions

| stage3_action | country | count |
|---|---|---|
| `auto_apply_merge` | FR | 1,581 |
| `auto_apply_merge` | IT | 430 |
| `auto_apply_merge` | US | 256 |
| `auto_apply_merge` | ES | 162 |
| `auto_apply_merge` | PT | 125 |
| `auto_apply_merge` | DE | 98 |
| `auto_apply_merge` | AU | 55 |
| `auto_apply_merge` | ZA | 38 |
| `auto_apply_merge` | AR | 35 |
| `auto_apply_merge` | CL | 29 |
| `auto_apply_merge` | _(120 more countries)_ | |
| `auto_apply_pc` | US | 277 |
| `auto_apply_pc` | FR | 265 |
| `auto_apply_pc` | IT | 56 |
| `auto_apply_pc` | ES | 25 |
| `auto_apply_pc` | AU | 20 |
| `auto_apply_pc` | FR/US | 14 |
| `auto_apply_pc` | PT | 12 |
| `auto_apply_pc` | AU/FR | 11 |
| `auto_apply_pc` | CL | 10 |
| `auto_apply_pc` | AR | 9 |
| `auto_apply_pc` | _(45 more countries)_ | |
| `user_review_pc` | FR | 174 |
| `user_review_pc` | US | 166 |
| `user_review_pc` | IT | 19 |
| `user_review_pc` | ES | 17 |
| `user_review_pc` | PT | 12 |
| `user_review_pc` | AU | 11 |
| `user_review_pc` | DE | 5 |
| `user_review_pc` | CL | 5 |
| `user_review_pc` | LU | 5 |
| `user_review_pc` | AT | 4 |
| `user_review_pc` | _(30 more countries)_ | |
| `user_review_merge_lowconf` | FR | 144 |
| `user_review_merge_lowconf` | IT | 22 |
| `user_review_merge_lowconf` | ES | 9 |
| `user_review_merge_lowconf` | US | 9 |
| `user_review_merge_lowconf` | DE | 6 |
| `user_review_merge_lowconf` | PT | 6 |
| `user_review_merge_lowconf` | AR | 3 |
| `user_review_merge_lowconf` | ZA | 2 |
| `user_review_merge_lowconf` | ES/PT | 2 |
| `user_review_merge_lowconf` | AU | 2 |
| `user_review_merge_lowconf` | _(36 more countries)_ | |
| `user_review_merge_unvalidated` | PT/FR | 1 |
| `user_review_missing` | FR | 16 |
| `user_review_missing` | AU | 2 |
| `user_review_missing` | ES | 1 |
| `user_review_missing` | IT | 1 |
| `user_review_missing` | AR/NZ | 1 |
| `user_review_missing` | PT/FR | 1 |
| `user_review_missing` | PT | 1 |

## Lowest-confidence auto-applies (potentially risky — should be reviewed)

The 20 `auto_apply_merge` / `auto_apply_pc` decisions with the lowest
confidence signals in the routing pipeline. These are the most likely to
be wrong and should be spot-checked before execution.

| pair_id | country | action | names | L1 (conf) | L2 (conf) | web (conf) |
|---|---|---|---|---|---|---|
| 117674 | US | `auto_apply_pc` | Dunham Cellars ⇔ Sinclair Estate Vineyards | PARENT_CHILD (0.89) | PARENT_CHILD (0.92) | PARENT_CHILD (0.91) |
| 118516 | US | `auto_apply_pc` | La Voix ⇔ Palmina | PARENT_CHILD (0.89) | PARENT_CHILD (0.92) | PARENT_CHILD (0.91) |
| 116452 | US | `auto_apply_pc` | Wallis Family Estate ⇔ Von Strasser | PARENT_CHILD (0.89) | PARENT_CHILD (0.9) | PARENT_CHILD (0.91) |
| 35434 | FR | `auto_apply_merge` | de Rabouchet ⇔ Rabouchet | MERGE (0.88) | - (-) | MERGE (0.92) |
| 35425 | FR | `auto_apply_merge` | Francois Martenot ⇔ Hospices de Beaune (Francois Martenot) | PARENT_CHILD (0.88) | PARENT_CHILD (0.9) | MERGE (0.92) |
| 17710 | FR | `auto_apply_merge` | Hospices de Beaune ⇔ Hospices de Beaune (Maison Leroy) | MERGE (0.87) | MERGE (0.94) | MERGE (0.92) |
| 35206 | FR | `auto_apply_merge` | Jean & Jean Louis Trapet ⇔ Pierre & Louis Trapet | SKIP (0.9) | SKIP (0.94) | MERGE (0.92) |
| 110 | DE | `auto_apply_merge` | Dr Thanisch ⇔ Wine of The Sea (Dr. H. Thanisch) | SKIP (0.7) | MERGE (0.88) | MERGE (0.92) |
| 26117 | US | `auto_apply_merge` | Premiere Napa Valley (Tierra Roja Vineyards ⇔ Premiere Napa Valley | MERGE (0.88) | - (-) | MERGE (0.92) |
| 34967 | DE | `auto_apply_merge` | F. & F. Peters (Felix Peters) ⇔ Felix Peters | MERGE (0.91) | - (-) | MERGE (0.92) |
| 351 | AU | `auto_apply_merge` | Riggs ⇔ Mr. Riggs | SKIP (0.88) | SKIP (0.92) | MERGE (0.92) |
| 106805 | FR | `auto_apply_merge` | Esclans ⇔ Caves d'Esclans | MERGE (0.87) | MERGE (0.92) | MERGE (0.92) |
| 353 | FR | `auto_apply_merge` | Cacheux Blee ⇔ Rene Cacheux | SKIP (0.85) | SKIP (0.93) | MERGE (0.92) |
| 135909 | US/NZ | `auto_apply_merge` | Cupcake Vineyards ⇔ Cupcake Vineyards | SKIP (0.95) | SKIP (0.96) | MERGE (0.92) |
| 78628 | AT | `auto_apply_merge` | Martin Nittnaus ⇔ Andreas & Martin Nittnaus | MERGE (0.85) | MERGE (0.92) | MERGE (0.92) |
| 38987 | FR | `auto_apply_merge` | Olivier Merlin ⇔ Merlin | SKIP (0.89) | SKIP (0.92) | MERGE (0.92) |
| 34947 | IT | `auto_apply_merge` | Montevibiano ⇔ Monte Vibiano Vecchio | SKIP (0.87) | SKIP (0.91) | MERGE (0.92) |
| 35145 | FR | `auto_apply_merge` | du Basty ⇔ Basty | SKIP (0.85) | SKIP (0.87) | MERGE (0.92) |
| 464 | IT | `auto_apply_merge` | Sergio Barale ⇔ Barale | SKIP (0.85) | SKIP (0.92) | MERGE (0.92) |
| 34968 | FR | `auto_apply_merge` | Servelle-Tachot ⇔ Amiot Servelle | SKIP (0.88) | SKIP (0.93) | MERGE (0.92) |

## Files in this directory

- `phase2_actionable_decisions.jsonl` — **4,679 rows.** Full data for
  every MERGE + PC + review-queue decision in the pipeline. Suitable for AI
  review at scale.
- `phase2_skip_sample.jsonl` — **500 rows.** Random sample of
  `auto_apply_skip` decisions to spot-check the SKIP classification.
- `phase2_summary.md` — this file.
- `phase2_risk_analysis.md` — what could go wrong if Phase 2 is executed.

## Relationship to Phase 1

The 493-pair Phase 1 ledger (`verdict_ledger.jsonl` at bundle root) was
produced by Chrome-per-pair validation of pairs that the pipeline flagged
for review (`user_review_*` actions in stage3). Phase 1 is the fully
human+Chrome-reviewed slice. Phase 2 covers everything else the pipeline
decided without Chrome.

If Phase 1 is executed as-is, the Phase 2 auto-apply decisions (3,154
MERGEs + 785 PCs) would still be unexecuted. They need either a separate
sampled audit before execution OR trust in the L1+L2 consensus agreement
that produced them.
