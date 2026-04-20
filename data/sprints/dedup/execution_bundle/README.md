# Sprint 6 Producer Dedup — Execution Bundle

**Status:** frozen for review, NOT yet executed against the DB.

This bundle contains everything needed to (a) understand what dedup decisions
Sprint 6 proposes to apply, (b) review those decisions before execution, and
(c) execute them deterministically once approved. Every file is self-contained
and designed to be readable by another AI reviewer without access to the full
session history.

## What this bundle is

A **frozen ledger** of 493 Chrome-validated producer-dedup decisions plus all
overrides discovered during B6.6 re-verification, packaged with:
- The decision methodology (how we got here)
- The verdict ledger itself (one JSON record per pair)
- A scorecard (aggregate counts, FK surface, flags)
- An executable SQL file (commented line-by-line with verdict rationale)
- A Python execute script (dry-run-by-default, gated on `--execute`)
- A reversibility plan (how to undo any merge)
- A deferred-to-Sprint-7 list (pairs we deliberately did not resolve)
- A testing guide for reviewers

## Files in this directory

| File | What it is |
|------|------------|
| `README.md` | This file. |
| `methodology.md` | The full dedup pipeline (B6.1 → B6.6) and how decisions were made. |
| `verdict_ledger.jsonl` | **Primary artifact.** One JSON record per verdict, 493 rows. |
| `verdict_ledger_summary.md` | Human-readable summary of the ledger (counts, breakdown). |
| `scorecard.md` | Pre-execution scorecard: aggregate counts, FK surface, flags, top merges. |
| `execution_plan.sql` | All SQL that would be executed, commented with verdict rationale. |
| `execution_plan.md` | Step-by-step narrative of what the SQL does. |
| `deferred_to_sprint_7.md` | Pairs explicitly not resolved here. Rationale per pair. |
| `reversibility.md` | Design and usage of `producer_merge_history` for undoing merges. |
| `testing_guide.md` | How to test this bundle before approving execution. |
| `rechrome_flips.md` | Every B6.6 re-Chrome override with before/after evidence. |
| `open_questions.md` | Schema / product questions this surfaced for Sprint 7+. |

## How to review

If you're another AI asked to review this bundle:
1. Start with `methodology.md` to understand the decision-making framework.
2. Read `scorecard.md` for aggregate counts and any flags.
3. Open `verdict_ledger.jsonl` and spot-check ~20 random entries. Each has the
   original verdict, any override applied, and the final decision with evidence.
4. Skim `rechrome_flips.md` to see the specific cases where B6.6 found Chrome
   validation to be wrong.
5. Open `execution_plan.sql` and read through a representative slice — every
   transaction is commented with the `ledger_key` so you can cross-reference.
6. Identify any decisions that look wrong and add them to `open_questions.md`
   or flag them for human review.

If you're a human reviewer, same flow, but `scorecard.md` and
`rechrome_flips.md` are the highest-leverage starts.

## The numbers

| metric | value |
|---|---|
| Total verdicts in ledger | **493** |
| MERGE (to apply) | **109** |
| PARENT_CHILD (to apply) | **50** |
| SKIP (no-op) | **296** |
| KEEP_AS_IS (yellow no-op) | **33** |
| DEFERRED_SPRINT_7 | **5** |
| B6.6 overrides applied | **193** |
|   FLIP_TO_SKIP | 29 |
|   FLIP_TO_MERGE | 2 |
|   FLIP_TO_PC | 1 |
|   FLIP_DIRECTION | 1 |
|   NEEDS_HUMAN_REVIEW | 5 |
|   KEEP (confirmed original) | 155 |
| Canonical-row redirects | **17** |
| Unique producer rows soft-deleted | ~109 |
| Wines re-pointed | ~359 |
| Pairs flagged for Sprint 7 follow-up | 7 |

### Flip rate context

The 29 FLIP_TO_SKIP + 1 FLIP_DIRECTION + 2 FLIP_TO_MERGE + 1 FLIP_TO_PC = **33 verdicts changed**
out of the 193 Chrome-validated MERGE+PC decisions across all four tiers (17% flip rate).
Essentially identical rates on Core (18%) and Mid/Tail/Yellow (17%). The systemic
pattern: original Chrome over-MERGEd on shared-surname French/Italian/Spanish family
names. The single most reliable red flag is **DB wine-list region/appellation
incompatibility** — 13 of 22 Mid/Tail/Yellow flips had incompatible regions visible
directly from the wine list without needing web lookup.

## Status of the DB

**Nothing in this bundle has been written to the database yet.** The ledger is
frozen as-of this commit; the execute script exists but is dry-run by default.
The user is performing independent testing before approving execution.

## When execution happens

Run `python scripts/sprint6_step10_execute.py --execute --ledger
data/sprints/dedup/execution_bundle/verdict_ledger.jsonl`. It will:
1. Re-read the ledger
2. For each MERGE: start a transaction, re-point FKs, write
   `producer_merge_history` row with full snapshot, soft-delete loser.
3. For each PARENT_CHILD: set `producers.parent_producer_id` on child.
4. For each SKIP / KEEP_AS_IS / DEFERRED_SPRINT_7: no-op.

Every operation is individually reversible via `producer_merge_history`.

## Provenance

- Sprint 6 Block 6.3 (2026-04-17): LWIN import + §11 identity rules drafted, L1 Haiku on 151K pairs
- Block 6.4 (2026-04-17): calibration + committed thresholds
- Block 6.5a (2026-04-19 / 20): Chrome-per-pair validation of 450+ pairs (producer of this ledger)
- Block 6.6 (2026-04-20, this session): §11.4 amendments + re-Chrome of all 66 Core + 127 Mid/Tail/Yellow MERGE+PC verdicts
- Block 6.7 (future, after user testing): execution

Commit hash of this bundle is in the git log. Every source JSONL cited here is
also in `data/sprints/dedup/chrome_validation/`.
