# Sprint 6 B6.5a Chrome Validation — Plan Lock

**Locked 2026-04-19 by user.** Do not redesign. Do not propose "§11 auto-apply"
shortcuts on Core. Do not substitute Serper `web_reasoning` for Chrome.

## The plan (Path A)

Chrome-validate 100% of three queues, per-pair, with durable evidence recorded:

- **71 yellow-flag top producers** — see `yellow_working.json`, log to
  `yellow_verdicts.jsonl`. **DONE 2026-04-20.**
- **143 Core escalate pairs** — Core defined as `max(wines_a, wines_b) >= 10` on
  the `escalate` bucket from `step9_opus_verdicts`. Log to `core_verdicts.jsonl`.
- **138 Mid escalate pairs** (ADDED per user 2026-04-20 PM ET) — Mid defined as
  `max(wines_a, wines_b) BETWEEN 3 AND 9`. Log to `mid_verdicts.jsonl`.
- **98 Tail pairs** — ADDED per user 2026-04-20 PM ET. Chrome all. Log to `tail_verdicts.jsonl`.

**Total Chrome scope: 71 yellow + 143 Core + 138 Mid + 98 Tail = 450 pairs.** 100% Chrome-per-pair. No §11-default shortcuts. User directive: "maintain discipline and actually web search all pairs as planned."

## Approved in-rigor speedups

These preserve Chrome-per-pair:
1. Single combined DuckDuckGo query `"name_a" "name_b" winery` — if zero results,
   logged as SKIP with that null evidence. Follow-up navigate only on ambiguity.
2. Pre-loaded DB wine-portfolio context (read-only join to producers/wines).
3. Batched JSONL logging (flush every ~20 verdicts).
4. Cluster-processing: handle all typo-variants first, then JV/collab, then
   corporate-holdco, then shared-surname, then sub-brand ambiguity.

## NOT approved

- Bulk §11-default verdicts on Core pairs without per-pair Chrome evidence.
- Re-reading Serper `web_reasoning` as the primary evidence (that's what put
  them in escalate — re-reading won't resolve the disagreement).
- Inventing new §11.4 subsections mid-run. Stick to §11 as locked.
- Dropping Chrome coverage for time pressure. User said "we can be patient."

## Why the lock exists

On 2026-04-19 a prior Sonnet session drifted: it pattern-matched Serper text,
batched ~80 cross_tier pairs under invented rules (§11.4.j TTB-shared-permit
shortcut), and was about to write a 379-row apply script from heuristics. User
caught it, rejected the approach, and re-committed to Path A.

## Resume point

Read `yellow_verdicts.jsonl` to find the last logged `idx`. Resume at the next
yellow producer from `yellow_working.json`. For Core, build `core_pairs.jsonl`
in the same directory once yellow is complete.

## Downstream (after Chrome complete)

Sequential, per CLAUDE.md current-focus roadmap:
1. Apply §11.4 amendments + measure rule-change impact
2. Write `scripts/sprint6_step10_execute.py` + `sprint6_step10c_scorecard.py`
3. Regenerate routing_stage3
4. Step 10c-pre scorecard → user signoff
5. COMMIT #1 (amendments + verdicts + scorecard)
6. Step 10a execute merges + producer_merge_history
7. Step 10b Safety Net B rescan
8. Step 10c-post verification
9. COMMIT #2 (execution + merge history)
10. Step 11 sprint close
