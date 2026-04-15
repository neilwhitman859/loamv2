# B5.7 — Sprint 5 wrap-up

You're picking up Sprint 5 (AI Bake-off) at the wrap-up block. The 6-round tournament
and all model selection work is **DONE**. This block has 3 jobs:

1. Tabulate the full bake-off into a permanent record
2. Test prompt caching (last cost-optimization opportunity before Sprint close)
3. Tie up loose ends + close the sprint

After this, you'll hand off to **B5.8 — strategy session for Sprint 6**.

---

## Bake-off conclusion (read first; don't re-derive)

**30 models tested across 6 rounds. Current best-under-the-current-prompt:
`openai/gpt-5.4-mini`** at composite 3.96 / $452 per 170K corpus. Cheap alternative:
`google/gemini-3-flash-preview` at 3.67 / $159 per 170K (best value).

**The headline takeaway is NOT "gpt-5.4-mini won."** The takeaway is:

> Cheaper models (DeepSeek at $93/170K, gemini-3-flash at $159/170K) can do the
> work. The gap to gpt-5.4-mini ($452/170K) is small enough that a better prompt
> + a fact-check gate + better pipeline design could easily flip the production
> winner. Garbage-in/garbage-out applies regardless of model — the bake-off
> showed model differentiation under the CURRENT prompt, but the prompt itself
> is the bigger lever.

**Production model selection is NOT locked.** It will be revisited in a later
sprint after prompt v2 + L3 fact-check gate + pipeline-architecture work has
landed. For Sprint 6 (producer dedup), use whatever cheap model is appropriate
to the dedup task — that's a separate decision from enrichment-model selection.

Key findings worth recording (but don't re-litigate or re-test in B5.7):

- **Search grounding does NOT close the gap under the current prompt.** Best
  `:online` model (deepseek-v3.2:online) hit 3.62 composite — still 0.27 below
  gpt-5.4-mini base. Real grounded cost is 3-11× higher than base when including
  ~$680/170K search fees. Worth retesting once the prompt does a better job of
  using grounded results.
- **Field-split production NOT viable under the current prompt.** gpt-5.4-mini
  dominates 8/11 fields broadly; splitting would save ~$70/170K minus 2× API
  overhead. DeepSeek fabricates terroir badly (corr 2.67 vs 5.4-mini 3.93) —
  worth retesting after prompt v2 explicitly forbids fabricating soil/geology
  details not in context.
- **Native Perplexity Sonar disappoints.** 2.85 composite, worse than DeepSeek
  base. Search integration ≠ better wine writing.
- **Sonar-reasoning + sonar-reasoning-pro have parse issues** (`<think>` tags).
  User's call: do NOT fix these in B5.7 — we won't use those models in Sprint 6.
- **Sprint 5 total spend: ~$52** (B5.5 prose $11.86 + B5.6 R1-R6 ~$36 + B5.1-B5.4 ~$4).

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
   - Conclusion: search grounding NOT viable UNDER THE CURRENT PROMPT — worth
     retesting after prompt v2 + L3 gate work. gpt-5.4-mini base remains the
     current-prompt leader, but the gap to cheaper models is small enough that
     better prompting could flip it.

3. Update / rewrite the **Production Recommendation** section. Critical framing
   — **do not call this a locked decision**:
   - Frame as "current best under the current prompt" — gpt-5.4-mini, with
     gemini-3-flash-preview as the cheap alternative, DeepSeek as the
     budget-grounded option
   - State explicitly: enrichment-model selection will be revisited after
     prompt v2 + L3 fact-check gate + pipeline-architecture work in a later
     sprint. The bake-off ranked models under the CURRENT prompt — that ranking
     could shift meaningfully once the prompt + pipeline improve.
   - Headline takeaway to surface: the cheaper models can do the work; the
     prompt is the bigger lever; garbage-in/garbage-out applies regardless of
     model. The ~$360/170K gap between gpt-5.4-mini and gemini-3-flash isn't a
     quality moat — it's a current-prompt artifact.
   - Add caveat about `:online` search-fee math (token cost ≠ real cost; +$680/170K)
   - Note search-grounded + field-split are PROVISIONALLY ruled out — both
     worth retesting once prompting + pipeline improve

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
   name: Bake-off outcome — cheap models viable, prompt is the lever
   description: Sprint 5 tested 30 models. gpt-5.4-mini wins under the current prompt at $452/170K, but DeepSeek ($93) and gemini-3-flash ($159) come close enough that better prompting + L3 fact-check gate could flip the winner. Model selection NOT locked — revisit after prompt v2 work.
   type: project
   ---
   ```
   Body: 2-3 short paragraphs covering (a) the bake-off result was a current-prompt
   ranking, not a final production decision, (b) the real takeaway is that cheap
   models can do the work and the prompt is the bigger lever, (c) what was tested
   + provisionally ruled out (search grounding, field-split, native Perplexity
   Sonar) — all worth retesting once prompting improves. Index entry in MEMORY.md
   under ## Project.

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
- Plan the prompt v2 + L3 fact-check gate work that needs to land BEFORE
  enrichment-model selection is finalized (this is the "garbage-in/garbage-out"
  fix the bake-off pointed at — pipeline + prompts matter more than the marginal
  model differences seen in Sprint 5)
- Park for Sprint 7+: structural diversity metric, side-by-side human blind test
- Decide whether Sprint 6 should bundle data fill or stay narrow

Note that Sprint 6's enrichment-model decision (when re-enrichment eventually
happens) is OPEN — the bake-off ranked models under the current prompt; once
prompts + pipeline are improved, the cheap models are likely to close or even
flip the gap. Don't pre-commit to gpt-5.4-mini in any Sprint 6 plan.

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
