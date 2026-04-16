# B6.1 — Sprint 6 Planning & Design Session

You're opening Sprint 6 for real. B5.8 closed Sprint 5 and wrote a preliminary
scope statement; B6.1 turns it into a concrete, agreed-upon plan.

**Sprint 6 goal (user directive from B5.8):**

> Dedup all 10,683 producers. Have a good method for deduping producers added
> in the future. Throw the kitchen sink at this — use AI in new and creative ways.

This is a planning session, not an execution session. No DB writes. Budget ~$0.
The deliverable is a plan the user signs off on.

---

## Read first (so you don't re-research)

- **`data/sprints/dedup/plan.md`** — sprint skeleton with research findings + open design questions
- **`bakeoff/scores/task1_scores.csv`** — Task 1 dedup bake-off results (17 models on 200 labeled pairs)
- **`bakeoff/scores/tournament_results.md`** — full bake-off context (Sprint 5)
- **`pipeline/promote/batch_matcher.py`** — existing fuzzy producer matcher (1,417 lines, real code)
- **`pipeline/promote/wine_merge.py`** — merge execution pattern (used in S4.1 producer merges and prior wine merges)
- **`pipeline/identity/dedup_deterministic.py`** — skeleton only (NotImplementedError)
- **`memory/project_bakeoff_outcome.md`** — why model rankings aren't locked

---

## What's already known

### Producer state (queried 2026-04-15)

- 10,683 producers total
- 67 exact-name dup groups (spacing/case normalization only) → 69 removable
- Producers merged in S4.1 (validation gold): Ridge (61+68→113), López de Heredia (8+11→18), CIRQ (4+1→4)

### Dedup model performance (Task 1, 200 labeled pairs)

| Model | Acc | FPR | FNR | $/4K |
|-------|----:|----:|----:|-----:|
| Sonnet 4.6 | 94.5% | 2.1% | 14.0% | $13.04 |
| Haiku 4.5 | 93.5% | **0.7%** | 21.1% | $3.25 |
| gpt-5.4-mini | 93.5% | 4.2% | 12.3% | $1.22 |
| gemini-3-flash-preview | 92.5% | 9.1% | 3.5% | $0.45 |
| gemini-3.1-flash-lite | 92.5% | 5.6% | 12.3% | $0.07 |
| DeepSeek | 89.0% | **14.7%** | 1.75% | $0.35 (unsafe) |

Haiku has the best FPR by 3×. DeepSeek is the cheapest "safe" option at 0.35/4K but its FPR (14.7%) disqualifies it for this task.

### Sprint sequence (locked)

```
Sprint 6 (this): producer dedup
Sprint 7: wine dedup (~4,079 suspected + 30-35 dangerous false-positive patterns from Q3 audit)
Sprint 8: prompt v2 + L3 fact-check gate + re-enrichment + share
```

---

## The 10 design questions to resolve in B6.1

These came out of B5.8 research; resolve each one with the user's input, then write the final plan.

1. **Signal fusion logic.** Each candidate pair gets multiple similarity signals (trigram, prefix-normalized match, wine-name overlap, region overlap, external-ID overlap, maybe embedding cosine). How do we combine them into a single MERGE/SKIP/VERIFY decision? Weighted sum? Per-signal thresholds with veto rules? A learned classifier trained on the S4.1 merges? Be concrete.

2. **Blocking strategy.** We can't compare all (10,683 choose 2) = 57M pairs. What's the blocking key — same country, same first-N chars of normalized name, same appellation? Check how many producers have NULL country first — a blocking strategy that requires country fails if 40% have it missing.

3. **Embedding-based clustering as a first-class signal.** Encode each producer as "name + country + region + wine names joined" via OpenAI ada-002 (~$1 total), cluster by cosine, use as a complementary signal to trigram. Worth it? Or stick to string-based signals plus LLM?

4. **LLM strategy on ambiguous pairs.**
   - (a) Single-pass Haiku on everything ambiguous (~$3-5)
   - (b) Two-stage: flash-lite triage → Haiku on genuinely-uncertain (~$1-3)
   - (c) Two-model disagreement: Haiku + gpt-5.4-mini independently, flag disagreements for human review (~$5-8)

5. **Web-grounded verification.** For high-stakes uncertain pairs (both producers have many wines), search the web and feed results to an LLM for entity resolution. Worth the effort? How to bound scope?

6. **S4.1 merges as validation gold.** The 3 prior merges (Ridge, López de Heredia, CIRQ) are hand-validated ground truth. Use them as (a) held-out test set to measure our pipeline's precision/recall, (b) few-shot examples in the LLM prompt, (c) both?

7. **Human-in-the-loop gate.** What threshold requires your explicit approval vs auto-apply? Options: all merges in review batches of 20, only merges affecting >N wines, only LLM-UNCERTAIN pairs, none (fully auto).

8. **Parent/child vs duplicate.** Some producers are legitimately related but distinct: LVMH ↔ Krug, Henschke ↔ Cyril Henschke, Torres ↔ Miguel Torres Chile. The schema needs a way to capture "related" separate from "duplicate." Does the `producers` table have `parent_id`? If not, do we add it now or defer?

9. **Merge reversibility.** If we merge A into B and find out later they're distinct, can we un-merge? `wine_merge.py` is a one-way soft-delete + repoint pattern. Do we need a pre-merge snapshot / undo log for this sprint specifically (high-stakes first-pass), or trust the audit trail?

10. **"Ongoing pipeline for future imports" scope.** Sprint 6 deliverable: is this (a) one-off cleanup of current 10,683 + hand notes for future, or (b) a reusable `pipeline/identity/producer_dedup.py` module that every future staging import runs through automatically?

---

## Suggested B6.1 workflow

1. **Read context** (plan.md, Task 1 scores, existing tooling) — 10 min
2. **Resolve the 10 questions** with the user — 45-60 min, use `AskUserQuestion` for each decision
3. **Write the final plan** to `data/sprints/dedup/plan.md` (overwrite the placeholder) with:
   - Phase breakdown (signal collection → deterministic → LLM → merge)
   - Specific thresholds and rules
   - Budget estimate
   - Validation plan (S4.1 merges as gold, target precision/recall)
   - Acceptance criteria (how do we know Sprint 6 is done?)
4. **Update sessions.json** with B6.1 entry
5. **Update CLAUDE.md + dashboard + journal.md** with Sprint 6 plan headline
6. **Commit + push** with message "B6.1: Sprint 6 plan — producer dedup approach locked"
7. **Propose B6.2** as the first execution block — e.g., "B6.2: build signal collection + run on full corpus + hand review 50 random candidates for calibration"

---

## Don't do in B6.1

- Don't run any merges
- Don't write any pipeline code (design, not build)
- Don't rebuild the bake-off — Task 1 results are final
- Don't open Sprint 7 / 8 planning — those are separate sprints
- Don't commit to a specific enrichment model — model selection is still open (see `memory/feedback_model_selection_open.md`)

---

## Hand-off to B6.2+

B6.2 will be the first execution block. Based on the plan B6.1 produces, expect B6.2 to either:
- Build signal collection (Phase 1 of the pipeline) and run on the full corpus
- Or run a calibration pass on the S4.1 merges + 50 hand-picked pairs to validate thresholds before running corpus-wide

The user expects real code execution starting in B6.2. B6.1's job is to agree on what to build.
