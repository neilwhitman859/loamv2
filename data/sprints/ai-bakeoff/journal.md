# Sprint 5 — AI Bakeoff (journal)

**Opened:** 2026-04-14
**Closed:** 2026-04-15
**Status:** CLOSED
**Total spend:** ~$55

---

## Goal at sprint open

Pick the right AI model family before running production-scale enrichment. Sonnet
4.6 had been the production baseline by inertia, but we'd never tested whether it
was cost-effective relative to OpenAI, Google, or the cheap open-weight tier.

The original plan: run three tasks (dedup, extraction, customer-facing prose),
score the outputs rigorously, pick a winner, then execute dedup + re-enrichment
with the winner and share with friends.

**Budget:** $25 cap. (Ran over by design — authorized in the tournament prompt.)

---

## What happened

### B5.1 Bake-off design (2026-04-14, $0)

Designed three tasks: Task 1 (dedup, 200 labeled pairs), Task 2 (extraction from
50 producer HTML pages with ground truth), Task 3 (customer-facing prose on 30
curated wines across 3 difficulty tiers). Opus-inline judging; 17 models for
Task 1/2 and 21 models for Task 3. Block plan drafted.

### B5.2 Build test data (2026-04-14, $0)

Built all three datasets. 200 pairs for dedup (50 same-varietal, 50 different-grape,
50 tier-differences, 50 ambiguous). 50 HTML wine pages scraped with rule-based
ground-truth extraction. 30 curated wine contexts using `batch_enrich.py`'s
bulk_preload_context().

### B5.3 + B5.4 Tasks 1 + 2 (dedup + extraction)

Executed per plan. Results captured in `bakeoff/scores/task1_*` and `task2_scores.csv`.
Dedup results informed Sprint 6 scope (producer dedup on ~4,079 suspected duplicates).

### B5.5 Task 3 prose run (2026-04-15, $11.86)

21 models × 30 wines → 630 prose outputs. 0 errors on the rerun; GPT-5-mini needed
a max_tokens bump (3000 → 8000) after initial 87% parse failures from mid-JSON
truncation. Results in `bakeoff/results/task3/`.

### B5.6 Task 3 tournament (2026-04-15, ~$40)

Built an Opus-judged tournament framework (`bakeoff/tournament.py`) with
cumulative scoring + tie-aware cuts + DeepSeek v3.2 protection from elimination
per user preference.

Six rounds actually ran (the prompt originally planned three):

- **R1** — 21 models × 3 wines (Shafer / Terroir Al Limit / Plantagenet). Cut to top 10 + ties + DeepSeek = 12 survivors. Judge cost $5.61.
- **R2** — 12 survivors × 3 new wines (Tyrrell's oak-fabrication test, des Bosquets grape-fabrication test, Howard Park Leston naming test). Clean cut at rank 5, next 0.121 below. DeepSeek protected at rank 12. 6 survivors. Judge cost $3.37.
- **R3** — 6 finalists × 24 remaining wines (144 new judge calls). gpt-5.4-mini at 3.960 edges out gpt-5.4 on voice/cost, Sonnet 4.6 production baseline eliminated at R2 rank 7. Judge cost ~$13.36.
- **R4 repechage** — gemini-3-flash-preview + gpt-5-mini full-30. Gemini-3-flash validated as cheap-tier value winner at 3.67/$159. ~$4.50.
- **R5 field-specialization test** — 3 wines × 3 cheap models (gpt-5.4-mini, deepseek, gemini-3-flash) scored field-by-field via `bakeoff/run_field_judge.py`. gpt-5.4-mini wins 7/11 correctness fields outright and is top-or-tied on all 11. Split-generation ruled out. $0.81.
- **R6 search-grounded + Chinese** — 8 models × 12 wines: gpt-5.4-mini:online, gemini-3-flash:online, deepseek:online, glm-4.6, kimi-k2 ± :online, sonar, sonar-reasoning-pro. Best :online (gpt-5.4-mini:online 3.89) still below gpt-5.4-mini base (3.96). DeepSeek base → :online lifted +0.23 but real cost with OR search fees is ~$1,069/170K. Sonar and Sonar-reasoning-pro were the worst performers. ~$13.

Dead models: perplexity/sonar-reasoning (HTTP 404), perplexity/llama-3.1-sonar-small-128k-online (retired endpoint).

### B5.7 Sprint wrap-up (2026-04-15, ~$0.82)

- Tabulated R4-R6 into `bakeoff/scores/tournament_results.md` (29 models total, all 6 rounds documented, production rec reframed).
- Tested prompt caching via OpenRouter + `cache_control`. Result: **not viable right now.** Opus 4.x requires 4,096-token minimum (our prefix is ~1,420); OpenRouter has a known open bug preventing Anthropic cache_control from working even on Sonnet where the minimum would be met. Documented in `tournament_results.md` for Sprint 6+ revisit.
- Updated CLAUDE.md, dashboard.html, sessions.md, memory/.
- Single final commit + push.

---

## Sprint-close takeaways (the important part)

**The bake-off answered a different question than we asked.** We asked "which model
wins?" The data answered "the *prompt* is the bigger lever." Under the current
prompt, gpt-5.4-mini wins. But:

- The best search-grounded variant (gpt-5.4-mini:online, 3.89) is still BELOW gpt-5.4-mini base (3.96) — search grounding doesn't rescue a mediocre prompt.
- DeepSeek fabricates terroir badly (2.67 on that field vs gpt-5.4-mini's 3.93) — a prompt that *forbids* fabricating soil/geology details would probably close most of the gap.
- Cheaper models land 0.3-0.6 composite behind the leader — a gap that's plausible to close with prompt v2 + L3 fact-check gate.

**Production model selection is NOT locked.** The right time to lock it is after
prompt v2 + L3 fact-check gate + pipeline-architecture work have landed in a
later sprint. Sprint 6 (producer dedup) is a separate model decision.

**Reframed takeaway:**
- Current-prompt leader: openai/gpt-5.4-mini ($452/170K)
- Cheap-tier alternative: google/gemini-3-flash-preview ($159/170K)
- Budget option: deepseek/deepseek-v3.2 ($93/170K, needs fact-check gate)

Sonnet 4.6 (old baseline) is not on the list — eliminated at R2 with 0.41 points
below gpt-5.4-mini at 13× the cost.

---

## Deferred to later sprints

- **Prompt v2** — explicitly forbid fabricating soils/geology; leverage grounded context; calibrate voice
- **L3 fact-check gate** — structural fact-check against ground truth / provenance before writing to DB
- **Re-running the bake-off under prompt v2** — ranking likely shifts; cheap models likely close or flip
- **Retry search grounding** — worth retesting when prompt v2 uses grounded context well
- **Retry field-split** — worth retesting when prompt v2 changes per-field winners
- **Prompt caching** — revisit when OpenRouter fixes the upstream bug, or when we switch judge/enrichment calls to direct Anthropic API, or when the L3 gate pushes the static portion past 4K tokens
- **Batch API** — cost-only optimization, not tested this sprint
- **Multi-judge ensemble** — hedging against judge noise, deferred
- **Structured output mode** — deferred per user
- **Sonar parse-error fixes** — deferred (won't use those models in Sprint 6)
- **Structural diversity metric** — whole-field entropy across 30 wines, deferred

---

## Budget detail

| Component | Cost |
|-----------|-----:|
| B5.1 Bake-off design | $0 |
| B5.2 Build test data | $0 |
| B5.3 Task 1 (dedup) | ~$1 |
| B5.4 Task 2 (extraction) | ~$2 |
| B5.5 Task 3 prose generation (21 × 30 + GPT-5-mini rerun) | $12.00 |
| B5.6 R1-R3 judge + tournament framework | $22.34 |
| B5.6 R4 repechage (prose + judge) | ~$4.50 |
| B5.6 R5 field-specialization | $0.81 |
| B5.6 R6 search-grounded + Chinese (prose + judge) | ~$13 |
| B5.7 Wrap-up including caching test | $0.82 |
| **Total** | **~$55** |

Went over the original $25 cap by ~$30, all user-authorized in the tournament
prompt. The delta was mostly R4-R6 expansion into tests not originally scoped
(repechage, field-split, search grounding).

---

## Files produced / touched

- `bakeoff/DESIGN.md` (B5.1)
- `bakeoff/build_test_data.py`, `build_task2.py`, `build_ground_truth.py` (B5.2)
- `bakeoff/run_task1.py`, `run_task2.py`, `run_task3.py` (B5.3/4/5)
- `bakeoff/run_task3_judge.py` (B5.5/6, extended with `--exact-models`)
- `bakeoff/run_field_judge.py` (B5.6 R5)
- `bakeoff/tournament.py` (B5.6)
- `bakeoff/test_prompt_caching.py` (B5.7)
- `bakeoff/data/task1/pairs.json`, `task2/*.html + *.json`, `task3/contexts.json + ground_truth.json`
- `bakeoff/results/task1/`, `task2/`, `task3/` (prose/classification outputs, 800+ JSONs)
- `bakeoff/scores/task1_scores.csv`, `task2_scores.csv`, `task3_scores.csv`, `task3_summary.csv`
- `bakeoff/scores/task3_judge/` (387 judge JSONs), `field_judge/` (9 R5 JSONs)
- `bakeoff/scores/task3_calibration_A.md`
- `bakeoff/scores/tournament_results.md` — the authoritative artifact
- `docs/WEB_GROUNDING_PATTERNS.md`
- `data/sprints/ai-bakeoff/plan.md`, `journal.md` (this file)
- `memory/feedback_model_selection_open.md`, `memory/project_bakeoff_outcome.md`
