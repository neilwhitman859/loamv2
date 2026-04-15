# B5.7 — Sprint 5 wrap-up

You're picking up Sprint 5 (AI Bake-off) at the wrap-up block. The 6-round tournament
and all model selection work is **DONE**. This block has 3 jobs:

1. Tabulate the full bake-off into a permanent record
2. Test prompt caching (last cost-optimization opportunity before Sprint close)
3. Tie up loose ends + close the sprint

After this, you'll hand off to **B5.8 — strategy session for Sprint 6**.

---

## Bake-off conclusion (read first; don't re-derive)

**30 models tested across 6 rounds. Production winner: `openai/gpt-5.4-mini`** at composite
3.96 / $452 per 170K corpus. Cheap alternative: `google/gemini-3-flash-preview` at
3.67 / $159 per 170K (best value).

Key findings that you should NOT re-litigate:

- **Search grounding does NOT close the gap.** Best `:online` model
  (deepseek-v3.2:online) hit 3.62 composite — still 0.27 below gpt-5.4-mini base.
  Real grounded cost is 3-11× higher than base when including ~$680/170K search fees.
- **Field-split production NOT viable.** gpt-5.4-mini dominates 8/11 fields broadly;
  splitting fields between models would save ~$70/170K minus 2× API overhead.
  DeepSeek fabricates terroir badly (corr 2.67 vs 5.4-mini 3.93).
- **Native Perplexity Sonar disappoints.** 2.85 composite, worse than DeepSeek base.
- **Sonar-reasoning + sonar-reasoning-pro have parse issues** (`<think>` tags).
  User's call: do NOT fix these — we won't use them.
- **Sprint 5 total spend: ~$52** (B5.5 prose $11.86 + B5.6 R1-R6 ~$36 + B5.1-B5.4 ~$4).
- **Production decision LOCKED:** gpt-5.4-mini base. Don't reopen this.

Authoritative artifact: [`bakeoff/scores/tournament_results.md`](bakeoff/scores/tournament_results.md)
(currently only documents R1-R3; you'll add R4-R6 in Job 1).

---

## Job 1 — Tabulate the full bake-off (target: 30 min)

### What

Update [`bakeoff/scores/tournament_results.md`](bakeoff/scores/tournament_results.md)
to include R4-R6 as new sections. The file currently ends at R3 + production
recommendation; add R4 + R5 + R6 sections, then update the production recommendation
to incorporate the new findings (no model change, but new cost/value framing).

### How

1. Read the existing file. Preserve R1-R3 sections.
2. Add new sections:

   **Round 4 — Repechage (full-30-wine for gemini-3-flash-preview + gpt-5-mini)**
   - gemini-3-flash-preview: 3.67 composite, $159/170K, tier-flat (0.04 spread)
   - gpt-5-mini: 3.44 composite, $819/170K, tier spread 0.48
   - Conclusion: gemini-3-flash-preview validated as the cheap-tier value winner

   **Round 5 — Field specialization test (3 wines × 3 cheap models, $0.81)**
   - gpt-5.4-mini wins majority of fields (8/11 correctness, 7/11 voice)
   - DeepSeek dominant only on 2 fields (comparable_wines, value_assessment)
   - Field-splitting savings ~$70/170K — not worth multi-model overhead
   - Per-field matrix data in `bakeoff/scores/field_judge/`

   **Round 6 — Search-grounded + cheap Chinese (8 models × 12 wines, ~$13 total)**
   Pull leaderboard from `bakeoff/scores/task3_summary.csv` for the 8 R6 models.
   Key numbers (composite | $/170K token-only | real $ inc search):
   ```
   gpt-5.4-mini:online      3.89  $844   ~$1,524
   gemini-3-flash:online    3.72  $474   ~$1,154
   deepseek-v3.2:online     3.62  $389   ~$1,069   ★ +0.23 vs base
   glm-4.6                  3.41  $1,867
   kimi-k2                  3.23  $640
   kimi-k2:online           3.15  $1,840  search HURT it (+23 wrong facts)
   sonar                    2.85  $433
   sonar-reasoning-pro      2.33  $4,664  worst + most expensive
   ```
   - Dead models: perplexity/sonar-reasoning (HTTP 404), perplexity/llama-3.1-sonar-small-128k-online (legacy retired)
   - Conclusion: search grounding NOT the breakthrough — gpt-5.4-mini base remains the winner

3. Update the **Production Recommendation** section:
   - Reaffirm gpt-5.4-mini base as primary
   - Add gemini-3-flash-preview as the validated cheap alternative
   - Add caveat about `:online` search-fee math
   - Note that prompt v2 / L3 fact-check gate / field-split are deferred to later sprints

4. Update the **Budget Summary** table to include R4-R6 lines and new ~$52 total.

### Acceptance

- File reads cleanly start-to-finish, no placeholders
- All 30 models accounted for somewhere
- Production rec is unambiguous
- Budget table totals ~$52

---

## Job 2 — Prompt caching test (target: 30 min, ~$1)

### Why

This is the last cost-optimization opportunity worth testing before sprint close.
The Opus judge prompt has ~2K static tokens (rubric + voice ref) repeated on every
call. Anthropic prompt caching gives 90% discount on cached read tokens.

If it works through OpenRouter, every future judge run is ~50-70% cheaper, and
production enrichment cost projections come down too. Worth ~30 min to validate
or rule out.

### How

1. **Verify OpenRouter cache pass-through.** Quick web check: as of mid-2025 OpenRouter
   supports Anthropic cache_control. If verified, proceed. If not, document why and skip.

2. **Modify `bakeoff/run_task3_judge.py`** to add cache_control to the static portion
   of JUDGE_PROMPT. The pattern is:
   ```python
   body = {
       "model": JUDGE_MODEL,
       "messages": [{
           "role": "user",
           "content": [
               {"type": "text", "text": STATIC_RUBRIC_PORTION,
                "cache_control": {"type": "ephemeral"}},
               {"type": "text", "text": dynamic_per_wine_portion},
           ]
       }],
       ...
   }
   ```
   Split the JUDGE_PROMPT into a static prefix (rubric + voice ref + format spec)
   and a dynamic suffix (context_data + ground_truth + model_output).

3. **Test on 3 wines × 1 model** (re-judge gpt-5.4-mini on the 3 R5 calibration wines).
   Compare cost + parse OpenRouter response for cache hit indicators (`cache_creation_input_tokens`,
   `cache_read_input_tokens` in usage object).

4. **Document findings** in `tournament_results.md` (new section: "Prompt caching: enabled / measured X% reduction"). If working, commit the change so future judge runs benefit. If broken via OpenRouter, note for direct-API use later.

### Acceptance

- One of three outcomes documented:
  - ✓ Caching works through OR, cost reduction measured + change committed
  - ✓ Caching works via direct Anthropic API only, change NOT committed but documented for Sprint 6+
  - ✓ Caching not viable now (with reason); skip

### Cost cap

If this becomes a multi-hour rabbit hole, STOP and document the partial state. Don't
spend more than $5 on this experiment.

---

## Job 3 — Sprint 5 close hygiene (target: 30 min)

Update each of the following. Skip files where genuinely nothing has changed.

1. **CLAUDE.md** — "Current Focus" section: change "Sprint 5 COMPLETE 2026-04-15"
   block to incorporate R4-R6 + final production decision + ~$52 spend. If you did
   the prompt caching test and it worked, add a one-line note about new cost projections.

2. **data/dashboard.html** — "Now" panel (replace with sprint-close summary), mark
   B5.7 ✓ in the blocks list, update budget figure to ~$52 (+ caching test cost).

3. **data/sessions.md** — Move B5.7 entry from Active to Done with summary.

4. **data/sprints/ai-bakeoff/journal.md** — Create if missing. Sprint-close summary:
   - Goal stated at sprint open
   - 6-round tournament outline
   - Production decision + projected savings vs Sonnet baseline
   - Items deferred to later sprints
   - Final spend

5. **memory/MEMORY.md + new memory file** — Add a project memory:
   `memory/project_bakeoff_outcome.md` with frontmatter:
   ```
   ---
   name: Bake-off outcome — gpt-5.4-mini wins
   description: Sprint 5 production model selection. gpt-5.4-mini base for enrichment; gemini-3-flash-preview as cheap alt; search/field-split tested + ruled out; prompting/enrichment deferred to later sprints
   type: project
   ---
   ```
   Body: 2-3 short paragraphs covering the production decision, what was tested + ruled out,
   what was deferred. Index entry in MEMORY.md under ## Project.

6. **data/sprints/current.json** — Update if Sprint 5 is closing. Otherwise leave for
   B5.8 to update when Sprint 6 opens.

7. **Final commit + push.** Single commit message:
   ```
   B5.7: Sprint 5 wrap — tournament tabulated, prompt caching tested, hygiene
   ```

---

## What NOT to do in B5.7

- Do NOT fix Sonar parse errors (won't use those models)
- Do NOT test structured output mode (deferred per user)
- Do NOT test batch API (cost-only, deferred)
- Do NOT add multi-judge ensemble (deferred)
- Do NOT redesign prompts (deferred to later sprint per user)
- Do NOT open Sprint 6 yet — that's B5.8's job

---

## Hand-off to B5.8 (strategy session)

After B5.7 is committed, B5.8 will be a pure strategy pass to:
- Confirm Sprint 6 scope = producer dedup (~4,079 suspected dupes per dashboard)
- Decide dedup approach (rule-based vs LLM-assisted; risk-mitigation for the
  30-35 dangerous false-positive patterns flagged in Q3 audit)
- Park for Sprint 7+: prompt v2, L3 fact-check gate, structural diversity metric,
  side-by-side human blind test
- Decide whether Sprint 6 should bundle data fill or stay narrow

You don't need to plan B5.8 — just leave clean handoff state.

---

## Files you'll touch

- bakeoff/scores/tournament_results.md (Job 1, 2)
- bakeoff/run_task3_judge.py (Job 2 if caching works)
- CLAUDE.md (Job 3)
- data/dashboard.html (Job 3)
- data/sessions.md (Job 3)
- data/sprints/ai-bakeoff/journal.md (Job 3, may need to create)
- memory/project_bakeoff_outcome.md (Job 3, new)
- memory/MEMORY.md (Job 3, append index entry)

## Files you'll READ but not edit

- bakeoff/scores/task3_summary.csv (R6 leaderboard data)
- bakeoff/scores/field_judge/*.json (R5 per-field data)
- bakeoff/scores/calibration_mapping.json (calibration anchor)
- bakeoff/scores/task3_calibration_A.md (hand-calibrated reference)

## Estimated total time/cost

- Job 1: 30 min, $0
- Job 2: 30 min, ~$1
- Job 3: 30 min, $0
- **Total: 1.5 hours, ~$1**

Sprint 5 final running total after this block: ~$53.

Begin with Job 1.
