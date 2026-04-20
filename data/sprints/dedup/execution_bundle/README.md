# Sprint 6 Producer Dedup — Execution Bundle

**Status:** frozen for external review. **Nothing has been written to the DB.**

This bundle is the complete decision record for Sprint 6 producer dedup. It's
designed to be read by external AI reviewers (paste-able into ChatGPT,
Gemini, or a fresh Claude session) as well as human reviewers.

## What this bundle is

A **frozen, unexecuted** record of every producer-dedup decision Sprint 6
produced — 151,150 pipeline decisions + 493 Chrome-validated decisions —
packaged with full methodology, sprint writeup, risk analysis, and
reversibility plan.

## Start here

1. **Read `sprint_writeup.md` first.** It's the complete sprint narrative
   from B6.1 through B6.6: what was planned, what was executed, what was
   decided, what was learned, what's open.
2. Then read `methodology.md` for the decision-making framework.
3. Then open one of:
   - `verdict_ledger_summary.md` for Phase 1 (Chrome-validated) decisions
   - `phase2_summary.md` for Phase 2 (pipeline auto-decided) decisions
   - `scorecard.md` for Phase 1 aggregate stats + top-20 lists

## Phase 1 vs Phase 2

**Phase 1** = 493 pairs that the AI ladder flagged for human review. These
were Chrome-validated per-pair in B6.5a, then re-Chrome'd in B6.6 for
rigor. Result: 109 MERGE + 50 PC + 334 no-ops. **Ready for execution after
user testing.**

**Phase 2** = 150,657 pairs the AI ladder decided without Chrome. Of these:
- 146,471 are SKIP (no DB change)
- 3,939 are auto-apply MERGE+PC (would mutate DB)
- 740 are user-review queues (need human judgment)

**Phase 2 has NOT been Chrome-validated.** It's packaged here for external
review, with a risk analysis suggesting a sampled audit before execution.

## Files in this directory

### Top-level (sprint-wide)

| File | What |
|------|------|
| `README.md` | this file |
| `sprint_writeup.md` | **full B6.1→B6.6 narrative** — start here |
| `methodology.md` | decision-making framework and pipeline architecture |
| `open_questions.md` | Sprint 7+ agenda (schema changes, family cleanups) |
| `reversibility.md` | how to undo merges via `producer_merge_history` |
| `testing_guide.md` | suggested tests before executing |

### Phase 1 — Chrome-validated (493 pairs)

| File | What |
|------|------|
| `verdict_ledger.jsonl` | **primary artifact.** 493 records, one JSON line per pair |
| `verdict_ledger_summary.md` | counts + tier breakdown |
| `scorecard.md` | aggregate stats, FK surface, top-20 lists, flags |
| `execution_plan.md` | narrative of what SQL will run |
| `rechrome_flips.md` | every B6.6 override with Chrome evidence |
| `deferred_to_sprint_7.md` | pair-level deferrals with rationale |

### Phase 2 — pipeline auto-decided (151,150 decisions)

| File | What |
|------|------|
| `phase2_summary.md` | per-action breakdown, core/tail split, weakest auto-applies |
| `phase2_actionable_decisions.jsonl` | **4,679 records.** Full data for every MERGE + PC + review-queue decision |
| `phase2_skip_sample.jsonl` | 500 random `auto_apply_skip` pairs for spot-checking |
| `phase2_risk_analysis.md` | what could go wrong + recommended handling |

## The numbers (complete picture)

### Phase 1 (Chrome-validated)

| metric | value |
|---|---|
| Total verdicts in ledger | **493** |
| MERGE (to apply) | **109** |
| PARENT_CHILD (to apply) | **50** |
| SKIP (no-op) | **296** |
| KEEP_AS_IS (yellow no-op) | **33** |
| DEFERRED_SPRINT_7 | **5** |
| B6.6 overrides applied | **193** (189 subagent + 4 manual) |
|   — FLIP_TO_SKIP | 29 |
|   — FLIP_TO_MERGE | 2 |
|   — FLIP_TO_PC | 1 |
|   — FLIP_DIRECTION | 1 |
|   — NEEDS_HUMAN_REVIEW | 5 |
|   — KEEP (confirmed original) | 155 |
| Canonical-row redirects | **17** |
| Pairs flagged for Sprint 7 follow-up | **7** |

### Phase 2 (pipeline auto-decided)

| stage3_action | Count | If executed |
|---|---|---|
| `auto_apply_skip` | **146,435** | no-op |
| `auto_apply_merge` | **3,154** | **mutation** |
| `auto_apply_pc` | **785** | **mutation** |
| `user_review_pc` | 470 | needs human review |
| `user_review_merge_lowconf` | 246 | needs human review |
| `auto_apply_skip_missing` | 32 | no-op (edge case) |
| `user_review_missing` | 23 | needs human review |
| `auto_apply_skip_residual` | 4 | no-op |
| `user_review_merge_unvalidated` | 1 | needs human review |
| **Total** | **151,150** | |

### Cross-tier systemic finding

The B6.6 re-Chrome pass flipped **17-18%** of Chrome-validated MERGE+PC
verdicts across every tier. The single most reliable red flag for false
MERGE was **DB wine-list region/appellation incompatibility** — 13 of 22
Mid/Tail/Yellow flips had incompatible regions visible directly from the
wine list without any web lookup. Pipeline prompts don't see this signal
at L1/L1.5. This is the main Sprint 7 / prompt-engineering candidate.

## Status of the DB

**Nothing in this bundle has been written to the database yet.**

- `producer_merge_history` row count: 0
- No producer rows soft-deleted
- No `parent_producer_id` set
- No FKs re-pointed

The execute script (`scripts/sprint6_step10_execute.py`) is dry-run by
default; `--execute` flag is required for mutation.

## When execution happens (future)

Phase 1 execution (if Phase 2 is deferred):

```bash
python -m scripts.sprint6_step10_execute --execute \
    --ledger data/sprints/dedup/execution_bundle/verdict_ledger.jsonl
```

Phase 2 execution is not yet wired up in a script. It would require:
1. A sampled Chrome audit of the auto-apply queue (200-300 pairs)
2. A separate execute script that reads `phase2_actionable_decisions.jsonl`
3. Handling for the user-review queues (740 items)

## How to review (for external AI reviewers)

1. Read `sprint_writeup.md` for the narrative.
2. Scan `verdict_ledger_summary.md` and `phase2_summary.md` for counts.
3. Open `verdict_ledger.jsonl` and sample 20 entries — verify they make
   sense given the names, cluster, and final verdict.
4. Open `phase2_actionable_decisions.jsonl` and sample 30 entries (mix of
   `auto_apply_merge`, `auto_apply_pc`, `user_review_pc`) — assess whether
   the pipeline's confidence signals look adequate.
5. Read `rechrome_flips.md` to see the specific cases where rigor was
   needed.
6. Check `testing_guide.md` for red-flag patterns to watch for.
7. If you find a decision that looks wrong, flag it by pair_id — the
   ledger entries include `ledger_key` for Phase 1 and `pair_id` for Phase
   2, so flags are crisp.

## Provenance

- Sprint 6 Block 6.3 (2026-04-17): LWIN import + §11 identity rules drafted,
  L1 Haiku on 151K pairs
- Block 6.4 (2026-04-17): calibration + committed thresholds
- Block 6.5a (2026-04-18 → 2026-04-20): Chrome-per-pair validation of 493
  pairs, producing `{yellow,core,mid,tail}_verdicts.jsonl`
- Block 6.6 (2026-04-20): §11.4 amendments + re-Chrome of all Core (66) +
  Mid/Tail/Yellow (127) MERGE+PC verdicts. Full bundle produced.
- (Future) User testing → approval → Phase 1 execution → Phase 2 handling

Commit hash of this bundle is in the git log. All source files cited
(`data/sprints/dedup/chrome_validation/*_verdicts.jsonl`,
`producer_dedup_pairs`, `producer_dedup_routing_stage3`) are in-repo or in
DB.

---

*Sprint 6 paused 2026-04-20 pending external review.*
