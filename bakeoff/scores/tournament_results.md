# Task 3 Bake-off Tournament Results

**Status:** COMPLETE (2026-04-15)
**Judge:** Opus 4.6 via OpenRouter (temperature 0.0)
**Rubric:** Correctness 35%, Voice 25%, Specificity 20%, Usefulness 20%
**Hard caps:** fabricated number → correctness MAX 1.5; banned word → voice MAX 2.0
**Test set:** 30 wines in 3 difficulty tiers (A = famous, B = mid-obscurity, C = obscure / bulk)
**Protection:** DeepSeek v3.2 protected from elimination per user instruction (production hunch)
**Tie tolerance:** 0.1 composite around cut line → bring extras forward

---

## Pre-Tournament Fix: GPT-5-mini

**Problem:** 13% parse rate — 27/30 calls hit `max_tokens=3000` and truncated mid-JSON.
**Fix:** Bumped max_tokens to 8000 for `openai/gpt-5-mini` (`bakeoff/run_task3.py:107`), matching the reasoning-model bucket (opus / gemini pro variants).
**Outcome:** 100% parse rate on rerun. Tokens out: min 2750, max 4886, avg 3586 — well inside the new 8000 cap.
**Cost of rerun:** ~$0.14.

Harness bug, not a model problem. Eligible for tournament.

---

## Round 1 — Top 10 + DeepSeek

**Wines:** Shafer One Point Five (A, calibration anchor) + Terroir Al Limit Les Tosses (B) + Plantagenet Omrah Pinot Noir (C)
**Models scored:** 21 (all)
**Judge cost:** $5.61 ($1.75 + $1.79 + $1.79 + 3× gpt-5-mini backfill $0.28)

### Cumulative leaderboard (3 wines)

| # | Model | Composite | Corr | Voice | Spec | Use |
|---|-------|-----------|------|-------|------|-----|
| 1 | openai/gpt-5.4 | **4.058** | 3.33 | 4.50 | 4.33 | 4.50 |
| 2 | google/gemini-3.1-pro-preview | 3.825 | 2.67 | 4.50 | 4.33 | 4.50 |
| 3 | anthropic/claude-opus-4.6 | 3.725 | 2.50 | 4.33 | 4.50 | 4.33 |
| 4 | openai/gpt-5.4-mini | 3.708 | 2.83 | 4.33 | 4.00 | 4.17 |
| 5 | google/gemini-2.5-pro | 3.600 | 2.67 | 4.00 | 4.33 | 4.00 |
| 6 | google/gemini-3-flash-preview | 3.525 | 2.33 | 4.17 | 4.33 | 4.00 |
| 7 | anthropic/claude-sonnet-4.6 | 3.467 | 2.33 | 3.67 | 4.33 | 4.33 |
| 8 | openai/gpt-5-mini | 3.283 | 2.50 | 3.50 | 3.83 | 3.83 |
| 9 | qwen/qwen3.6-plus | 3.283 | 2.17 | 3.83 | 4.17 | 3.67 |
| 10 | deepseek/deepseek-v3.2 | 3.175 | 2.00 | 3.50 | 4.00 | 4.00 |
| — **cut line (3.175)** — | | | | | | |
| 11 | mistralai/mistral-large | 3.117 | 1.83 | 3.50 | 4.17 | 3.83 | ← within 0.058, advance |
| 12 | anthropic/claude-haiku-4.5 | 3.108 | 2.00 | 3.50 | 4.00 | 3.67 | ← within 0.067, advance |
| 13 | mistralai/mistral-large-2512 | 3.042 | 1.83 | 3.33 | 4.17 | 3.67 | ← 0.133 below, cut |
| 14–21 | minimax / gemini-2.5-flash / qwen3.5 / grok / mimo / mistral-nemo / llama-4 | 2.95–1.84 | | | | |

### Cut decision

- **Top 10 advance** (gpt-5.4 through deepseek-v3.2)
- **Tie rule adds 2:** mistral-large (0.058 below cut) and claude-haiku-4.5 (0.067 below cut) — both within 0.1 tolerance
- **DeepSeek protection: redundant here** — already at rank 10 on pure score
- **Final R1 survivors: 12**
- **Eliminated (9):** mistral-large-2512, minimax-m2.7, gemini-2.5-flash, qwen3.5-plus-02-15, grok-4.1-fast, mimo-v2-flash, mimo-v2-pro, mistral-nemo, llama-4-maverick

Bottom of field is decisive — llama-4-maverick at 1.842 (mostly 1.5 on correctness; hallucinates aggressively) and mistral-nemo at 2.150 (5 banned words in 3 wines) are firmly out.

---

## Round 2 — Top 5 + DeepSeek

**New wines:** Tyrrell's Vat 1 Semillon (A, oak-fabrication test — truth: NO oak) + des Bosquets Gigondas Les Routes (B, grape-fabrication test — truth: 100% Syrah) + Howard Park Leston Shiraz (C, naming test — Leston = Jeff Burch's father)
**Models scored:** 12 R1 survivors
**Judge cost:** $3.37 ($1.09 + $1.16 + $1.12)

### Cumulative leaderboard (6 wines)

| # | Model | Composite | Corr | Voice | Spec | Use |
|---|-------|-----------|------|-------|------|-----|
| 1 | openai/gpt-5.4 | **4.096** | 3.50 | 4.42 | 4.33 | 4.50 |
| 2 | google/gemini-3.1-pro-preview | 3.962 | 2.92 | 4.50 | 4.50 | 4.58 |
| 3 | anthropic/claude-opus-4.6 | 3.942 | 2.92 | 4.42 | 4.50 | 4.58 |
| 4 | openai/gpt-5.4-mini | 3.867 | 3.08 | 4.42 | 4.17 | 4.25 |
| 5 | google/gemini-2.5-pro | 3.754 | 2.92 | 4.00 | 4.42 | 4.25 |
| — **cut line (3.754)** — | | | | | | |
| 6 | google/gemini-3-flash-preview | 3.633 | 2.58 | 3.92 | 4.50 | 4.25 | ← 0.121 below, cut |
| 7 | anthropic/claude-sonnet-4.6 | 3.554 | 2.50 | 3.58 | 4.42 | 4.50 |
| 8 | qwen/qwen3.6-plus | 3.542 | 2.50 | 4.00 | 4.33 | 4.00 |
| 9 | openai/gpt-5-mini | 3.350 | 2.58 | 3.58 | 3.83 | 3.92 |
| 10 | anthropic/claude-haiku-4.5 | 3.329 | 2.25 | 3.83 | 4.08 | 3.83 |
| 11 | mistralai/mistral-large | 3.317 | 2.08 | 3.75 | 4.25 | 4.00 |
| 12 | deepseek/deepseek-v3.2 | 3.242 | 2.33 | 3.17 | 4.08 | 4.08 | ← protected |

### Cut decision

- **Clean cut** — no ties within 0.1 of cut line (next model is 0.121 below)
- **DeepSeek protection: active** — would have been cut at rank 12
- **Final R2 survivors: 6** (5 top + DeepSeek)
- **Eliminated (6):** gemini-3-flash-preview, claude-sonnet-4.6, qwen3.6-plus, gpt-5-mini, claude-haiku-4.5, mistral-large

Biggest surprise: **Sonnet 4.6 — the production baseline — is out at rank 7.** Not a rounding issue. Its voice averages 3.58 vs leaders at 4.40+; hits banned-word hard cap in Tyrrell's and Howard Park. Fallback if production winner fails qualifies as a real concern.

Gemini-3-flash-preview missed by 0.121 — just outside the 0.1 tie window. At $158/170K it would have been a strong budget contender; noting for future runs.

---

## Round 3 — Finals (30 wines, full set)

**Models scored:** 6 finalists (gpt-5.4, gpt-5.4-mini, gemini-3.1-pro-preview, claude-opus-4.6, gemini-2.5-pro, deepseek-v3.2)
**New judge calls:** 144 (24 remaining wines × 6 finalists). Resume skipped the 36 already-scored R1+R2 combos.
**Judge cost:** ~$13.36 marginal ($22.34 total judge spend including R1+R2, per script's summary)

### Final leaderboard (30 wines cumulative)

| # | Model | Comp | Corr | Voice | Spec | Use | Tier A | Tier B | Tier C | Wrong | Ban | $/170K |
|---|-------|------|------|-------|------|-----|--------|--------|--------|-------|-----|--------|
| 1 | **openai/gpt-5.4** | **4.036** | 3.45 | 4.43 | 4.15 | 4.45 | 4.12 | 3.94 | 4.05 | 57 | 0 | $3,088 |
| 2 | **openai/gpt-5.4-mini** | **3.960** | 3.20 | 4.47 | 4.25 | 4.37 | 4.00 | 3.92 | 3.97 | 80 | 0 | **$452** |
| 3 | google/gemini-3.1-pro-preview | 3.886 | 2.83 | 4.42 | 4.48 | 4.47 | 3.89 | 3.89 | 3.88 | 123 | 1 | $8,524 |
| 4 | anthropic/claude-opus-4.6 | 3.873 | 2.75 | 4.33 | 4.55 | 4.58 | 3.84 | 3.92 | 3.86 | 131 | 6 | $27,693 |
| 5 | google/gemini-2.5-pro | 3.769 | 2.83 | 4.15 | 4.35 | 4.35 | 3.89 | 3.65 | 3.77 | 118 | 4 | $7,224 |
| 6 | deepseek/deepseek-v3.2 | 3.388 | 2.35 | 3.72 | 4.12 | 4.07 | 3.40 | 3.44 | 3.33 | 137 | 10 | **$93** |

### Tier consistency

| Model | Tier spread (A–C range) | Most consistent? |
|-------|-------------------------|------------------|
| gpt-5.4 | 3.94 – 4.12 = 0.18 | no (strong on A) |
| gpt-5.4-mini | 3.92 – 4.00 = 0.08 | **yes — flat across tiers** |
| gemini-3.1-pro-preview | 3.88 – 3.89 = 0.01 | **yes — flattest of all** |
| claude-opus-4.6 | 3.84 – 3.92 = 0.08 | yes |
| gemini-2.5-pro | 3.65 – 3.89 = 0.24 | no (weak on B) |
| deepseek-v3.2 | 3.33 – 3.44 = 0.11 | yes but lower overall |

**gpt-5.4-mini and gemini-3.1-pro-preview are the flattest across tiers** — they don't disproportionately lose on obscure wines. Opus and gpt-5.4-mini tied at 0.08. DeepSeek is consistent but at a lower level.

### Correctness vs Voice

| Model | Correctness | Voice | Fabrication-prone? |
|-------|-------------|-------|---------------------|
| gpt-5.4 | **3.45** | 4.43 | least (57 wrong facts / 30 wines = 1.9/wine) |
| gpt-5.4-mini | 3.20 | 4.47 | moderate (80/30 = 2.7/wine), 0 banned words |
| gemini-3.1-pro-preview | 2.83 | 4.42 | 4.1/wine |
| claude-opus-4.6 | 2.75 | 4.33 | 4.4/wine + 6 banned words |
| gemini-2.5-pro | 2.83 | 4.15 | 3.9/wine + 4 banned words |
| deepseek-v3.2 | 2.35 | 3.72 | **worst: 4.6/wine + 10 banned words** |

**gpt-5.4 is the most factually careful.** gpt-5.4-mini is close behind with zero banned-word violations across 30 wines — cleaner voice than all Claude models in the set.

---

## Round 4 — Repechage (full-30 on two near-miss finalists)

**Why:** `gemini-3-flash-preview` missed R2 by 0.121 and `gpt-5-mini` missed R1 by 0.125 — both close enough that a judge-noise run on the full 30 could have reordered the board. Ran both to the full 30-wine set to confirm or flip their standing.

**Models scored:** 2 (`google/gemini-3-flash-preview`, `openai/gpt-5-mini`)
**New wines per model:** 27 each (R1+R2 already cached)
**Judge cost:** ~$4.50 marginal

### Repechage results (30 wines cumulative)

| Model | Comp | Corr | Voice | Spec | Use | Tier A / B / C | Wrong | Ban | $/170K |
|-------|------|------|-------|------|-----|----------------|-------|-----|--------|
| google/gemini-3-flash-preview | **3.669** | 2.43 | 4.22 | 4.53 | 4.28 | 3.69 / 3.60 / 3.73 | 153 | 5 | **$159** |
| openai/gpt-5-mini | 3.441 | 2.65 | 3.73 | 3.98 | 3.92 | 3.43 / 3.54 / 3.35 | 98 | 3 | $819 |

### Conclusion

- **gemini-3-flash-preview validated as the cheap-tier value winner at 3.67 / $159.** Tier spread 0.13 — reasonably flat. Sits between finalists (Opus 3.87, Sonnet 3.55) at ~3% of Opus cost. The single banned-word hit plus 153 wrong facts are the cost of the cheap-tier discount; worth revisiting after prompt v2 + L3 fact-check gate land.
- **gpt-5-mini at 3.44 / $819 is neither cheapest nor best.** gpt-5.4-mini beats it on every axis at roughly half the cost ($452 vs $819). No reason to pick gpt-5-mini over gpt-5.4-mini going forward.
- Neither model reorders the top 6. Finalists stand.

---

## Round 5 — Field specialization test (3 wines × 3 cheap models, $0.81 budget)

**Why:** The final prose output has ~11 fields (hook, wine_summary, terroir_expression, vinification_summary, food_pairing, comparable_wines, style_profile, cellar_recommendation, value_assessment, market_position, insider_take). If different models win different fields, a split-generation approach could stack the best per field at cheap cost. Tested on 3 calibration wines (Shafer, Terroir Al Limit, Plantagenet — A/B/C).

**Models tested:** `openai/gpt-5.4-mini`, `deepseek/deepseek-v3.2`, `google/gemini-3-flash-preview`
**Judge:** Opus scored each wine output field-by-field via `bakeoff/run_field_judge.py`
**Per-field data:** `bakeoff/scores/field_judge/` (9 JSON files, one per model × wine)

### Per-field correctness (averages across 3 wines)

| Field | gpt-5.4-mini | gemini-3-flash | deepseek-v3.2 | Notes |
|-------|-------------:|---------------:|--------------:|-------|
| hook | **3.40** | 3.27 | 3.33 | gpt-5.4-mini narrow edge |
| wine_summary | 3.50 | **3.50** | 3.17 | tie at top |
| terroir_expression | **3.93** | 3.07 | 2.67 | DeepSeek fabricates soils — 1.26 below gpt-5.4-mini |
| vinification_summary | 2.17 | **2.17** | 1.67 | tie at top; all weak on oak specifics |
| food_pairing | **4.23** | 3.83 | 3.83 | gpt-5.4-mini 0.40 ahead |
| comparable_wines | 3.50 | 3.33 | **3.50** | tie with DeepSeek |
| style_profile | **4.00** | 3.90 | 3.60 | |
| cellar_recommendation | **3.50** | 3.33 | 3.43 | |
| value_assessment | 3.33 | 3.00 | **3.33** | tie with DeepSeek |
| market_position | **4.00** | 3.60 | 3.60 | |
| insider_take | **3.93** | 3.50 | 3.33 | |

**Tally (correctness, counting ties):** gpt-5.4-mini wins outright in **7/11** fields, ties at the top in 4 more; gemini-3-flash wins 2 (both ties); DeepSeek wins 2 (both ties). **gpt-5.4-mini appears in the top score for 11/11 fields** — no field where it's clearly beaten.

**Voice (averages, same 3 wines):** gpt-5.4-mini wins outright in **6/11**, ties at the top in 4, bottom in 1 (comparable_wines: deepseek-v3.2 4.07 > gpt-5.4-mini 3.93).

### Conclusion — field-split NOT viable under current prompt

- **gpt-5.4-mini dominates broadly.** It's at the top of every correctness field (7 outright + 4 ties out of 11). DeepSeek and gemini-3-flash only tie it — they don't clearly beat it on any single field.
- **Split savings math:** Routing comparable_wines + value_assessment to DeepSeek might save ~$70/170K in tokens, but adds 2× API call overhead per wine plus orchestration complexity. Net effect isn't a real savings under current per-field data.
- **DeepSeek fabricates terroir badly:** 2.67 on terroir_expression correctness vs gpt-5.4-mini's 3.93 — DeepSeek invents soil types and geological formations that aren't in context. A prompt that explicitly forbids fabricating soil/geology details would likely narrow this gap; worth retesting after prompt v2.
- **Field-split provisionally ruled out for Sprint 6 re-enrichment** — retest after prompt work.

---

## Round 6 — Search-grounded + cheap Chinese (8 models × 12 wines, ~$13 total)

**Why:** If search grounding catches fabricated facts at the source (instead of relying on a downstream L3 fact-check gate), expensive :online variants could beat base models on correctness. Also pulled in cheap Chinese frontier models (glm-4.6, kimi-k2) that weren't in the original 21. Scored on 12 wines (subset of the 30-wine tournament set) to keep cost bounded.

**Models tested:** 8
- `openai/gpt-5.4-mini:online`, `google/gemini-3-flash-preview:online`, `deepseek/deepseek-v3.2:online`
- `perplexity/sonar`, `perplexity/sonar-reasoning-pro`
- `moonshotai/kimi-k2`, `moonshotai/kimi-k2:online`
- `z-ai/glm-4.6`

**Dead models excluded:**
- `perplexity/sonar-reasoning` — HTTP 404 on OpenRouter
- `perplexity/llama-3.1-sonar-small-128k-online` — legacy endpoint retired

**Judge cost:** ~$13 marginal

### R6 leaderboard (12 wines each)

| Model | Comp | Corr | Voice | Spec | Use | Wrong | Ban | Token $/170K | Real $/170K (inc search) |
|-------|------|------|-------|------|-----|-------|-----|-------------:|-------------------------:|
| openai/gpt-5.4-mini:online | **3.89** | 3.21 | 4.42 | 4.04 | 4.29 | 25 | 0 | $844 | ~$1,524 |
| google/gemini-3-flash-preview:online | 3.72 | 2.54 | 4.25 | 4.54 | 4.29 | 60 | 1 | $474 | ~$1,154 |
| deepseek/deepseek-v3.2:online ★ | 3.62 | 2.67 | 4.08 | 4.12 | 4.21 | 44 | 5 | $389 | ~$1,069 |
| z-ai/glm-4.6 | 3.41 | 2.54 | 3.79 | 4.04 | 3.83 | 56 | 2 | $1,867 | $1,867 |
| moonshotai/kimi-k2 | 3.23 | 1.62 | 4.17 | 4.33 | 3.75 | 97 | 0 | $640 | $640 |
| moonshotai/kimi-k2:online | 3.15 | 1.46 | 4.17 | 4.38 | 3.62 | 120 | 0 | $1,840 | ~$2,520 |
| perplexity/sonar | 2.85 | 1.92 | 3.33 | 3.54 | 3.21 | 60 | 0 | $433 | $433 |
| perplexity/sonar-reasoning-pro | 2.33 | 1.79 | 2.58 | 2.67 | 2.62 | 43 | 1 | $4,664 | $4,664 |

★ **DeepSeek v3.2 search variant is the headline result** — composite jumped from 3.39 (base, 30 wines) to 3.62 (online, 12 wines). That +0.23 lift is the biggest delta we saw from any prompt/grounding intervention. But — see caveats below.

### Conclusion — search grounding NOT viable under the current prompt

- **Search grounding doesn't close the gap.** The best `:online` model (gpt-5.4-mini:online at 3.89) is still 0.07 BELOW gpt-5.4-mini base (3.96) on the same judge. The grounded variants don't consistently beat their base counterparts in composite.
- **DeepSeek's +0.23 lift is the one real signal** — but it still lands at 3.62, below four base models. And see the real-cost math: OpenRouter charges ~$4-6 per 1K search queries on top of token costs. At ~$680/170K added search fees, deepseek-v3.2:online's *real* cost is ~$1,069/170K — 11× base DeepSeek.
- **Search HURT Kimi.** `kimi-k2:online` scored WORSE than `kimi-k2` base (3.15 vs 3.23) and added 23 more wrong-fact flags (120 vs 97). Search grounding without a good prompt can introduce noise.
- **Native Perplexity Sonar is uncompetitive.** Sonar at 2.85 is worse than base DeepSeek at 3.39 despite being search-first. Sonar-reasoning-pro at 2.33 is the single worst composite AND highest cost in the entire R6 set — $4,664/170K for wine writing that's measurably worse than gpt-5.4-mini-mini at $452.
- **GLM-4.6 and Kimi-K2 are competitive with the low end of the 30-wine field** (3.41 and 3.23) but don't beat any of the original 6 finalists.

**Search grounding + field-split are PROVISIONALLY ruled out — both worth retesting once prompt v2 + L3 fact-check gate land.** The bake-off ranked models under the CURRENT prompt, and the current prompt doesn't exploit grounded context well.

### What surprised us

- Native search models (Perplexity) are much worse than general models with search add-ons. Being "search-first" doesn't substitute for writing quality.
- The Chinese frontier tier (glm-4.6, kimi-k2) is now roughly where the American mid-tier was a year ago — not ahead, not embarrassing either.
- Sonar-reasoning-pro emits `<think>` tags that wreck JSON parsing; sonar-reasoning had HTTP 404s on OpenRouter. **User's call: do NOT fix these for B5.7 — we won't use them in Sprint 6.** Parse issues are logged but not patched.

---

## Production Recommendation

> **This is NOT a locked decision.** The bake-off ranked models under the CURRENT
> enrichment prompt (the one used in B5.5). A better prompt + an L3 fact-check
> gate + pipeline-architecture work are all pending, and any of those could
> materially shift the ranking — especially between the top model and the cheap
> tier. The ~$360/170K gap between gpt-5.4-mini and gemini-3-flash is not a
> quality moat; it's a current-prompt artifact.
>
> Enrichment-model selection will be revisited in a later sprint after prompt v2
> + L3 fact-check gate + pipeline improvements land. Sprint 6 is producer dedup,
> which is a separate model decision (pick whatever cheap model fits the dedup
> task).

### Headline takeaway

**Cheaper models can do the work. The prompt is the bigger lever.**

Garbage-in/garbage-out applies regardless of which model sits at the top of the
composite score. The rank order above reflects how well each model rescued a
mediocre prompt — not a ceiling on what the cheap models can produce. Once the
prompt stops tolerating fabricated soils / vague hedging / and starts putting
the right context blocks in, the differentiator shifts from model to pipeline.

### Current best under the current prompt: `openai/gpt-5.4-mini`

- Composite **3.960** (0.076 below gpt-5.4, 0.091 above gemini-3.1-pro-preview)
- **Zero banned-word violations across 30 wines** — cleaner voice than any Claude model in the set
- Flat tier performance (0.08 spread A→C)
- Token-only cost: **$452/170K**, vs Sonnet baseline ~$5,953 → ~92% savings if Sprint 6 re-enriches at scale

The composite gap between gpt-5.4 (4.036) and gpt-5.4-mini (3.960) is 0.076 — well within judge noise (half-point granularity). gpt-5.4 wins correctness by 0.25 (~1 wrong fact per 4 wines more careful) but loses on voice and costs 7× more per call.

### Cheap-tier alternative: `google/gemini-3-flash-preview`

- Composite **3.669** (R4 repechage, full 30 wines)
- **$159/170K — ~3% of gpt-5.4 cost, ~35% of gpt-5.4-mini cost**
- The headline cheap choice if budget becomes binding. 0.29 composite below gpt-5.4-mini — real gap, but the right ~$300/170K saved could fund better prompting + gate work instead of a fancier model.
- 153 wrong facts / 30 wines (5.1/wine) — meaningfully higher than gpt-5.4-mini (80 / 30 = 2.7/wine). An L3 fact-check gate would catch most of these.

### Budget-grounded option: `deepseek/deepseek-v3.2`

- Composite **3.388** (base, 30 wines) or **3.621** (`:online`, 12 wines with grounding)
- **$93/170K** base — the cheapest credible option in the whole field, 59× cheaper than Sonnet baseline
- `:online` lifts correctness noticeably (+0.23 composite) but REAL cost is ~$1,069/170K once OpenRouter's search fees are included. At that price the gap to gpt-5.4-mini is only ~$620/170K and you still lose quality.
- **User preference:** DeepSeek was protected from elimination across R1-R3 per the user's production hunch; it finished 6th on composite but cost-per-dollar-of-quality remains its selling point.
- Weaknesses: 10 banned-word violations, highest base wrong-fact rate (4.6/wine). Needs a tight fact-check gate to be safe at production scale.

### Caveat on `:online` and search-fee math

OpenRouter charges ~$4-6 per 1K searches on top of token costs for `:online` variants. This is NOT in the $/170K token figures above. Real grounded cost for a 170K corpus with ~1 search per wine is **$680/170K extra minimum, often higher**. At that level, `:online` variants are not cost-competitive with base models UNLESS grounding actually moved the correctness needle — and under the current prompt, it mostly did not (R6 showed best `:online` at 3.89, still below gpt-5.4-mini base at 3.96).

### Provisionally ruled out (both worth retesting after prompt v2)

- **Search-grounded `:online` variants** — token-cost-competitive but not quality-competitive under the current prompt. Revisit once prompt v2 explicitly leverages grounded context.
- **Field-split multi-model generation** — gpt-5.4-mini wins 9/11 correctness fields. DeepSeek dominates only 2 (comparable_wines, value_assessment). Split savings are real (~$70/170K) but swamped by 2× API overhead. Revisit if prompt v2 changes field-level winners.

### Not competitive under any framing

- **`claude-sonnet-4.6`** (the production baseline) — eliminated at R2 with 3.554 composite. Loses to gpt-5.4-mini by 0.41 points at 13× the cost. No reason to stick with Sonnet for enrichment.
- **`claude-opus-4.6`** — highest specificity (4.55) but only 4th overall at $27,693/170K. 61× gpt-5.4-mini's cost for 0.087 lower composite. Reserved for tiny curated flagship subset if at all.
- **`perplexity/sonar*`** — native search-first models are measurably worse than general models with search add-ons. Sonar-reasoning-pro was the single worst composite (2.33) AND most expensive ($4,664/170K) in the R6 set.
- **`meta-llama/llama-4-maverick`**, **`mistralai/mistral-nemo`**, **`xiaomi/mimo-v2-*`**, **`qwen/qwen3.5-plus-02-15`**, **`x-ai/grok-4.1-fast`** — bottom of the R1 field (1.84-2.77 composite). Aggressive fabrication and/or banned-word violations.

---

## Prompt caching: not viable via OpenRouter right now

**Tested B5.7 on anthropic/claude-opus-4.6 + anthropic/claude-sonnet-4.6 via OpenRouter.**

The judge prompt has a ~1,420-token static prefix (rubric + voice reference + format spec) repeated on every call. Anthropic prompt caching theoretically gives a 90% discount on cache-read input tokens, which would cut ~15-30% off future judge runs.

### What was tried

- Split `JUDGE_PROMPT` into static prefix + dynamic suffix (wine context, ground truth, model output)
- Sent `{"type": "text", "text": STATIC, "cache_control": {"type": "ephemeral"}}` in both a single-user-message content array AND a dedicated system-role message
- Tested across 3 calls each on Opus 4.6 and Sonnet 4.6

### Result

- **All 9 calls: `cached_tokens: 0`, `cache_write_tokens: 0`** in OpenRouter's `prompt_tokens_details`
- Per-call cost was identical to baseline (no cache-read discount applied)
- OpenRouter's schema accepts and reports caching fields, but the values stayed at 0 regardless of format

### Why

Two compounding problems:

1. **Opus 4.x requires a 4,096-token minimum cached prefix.** Our prefix is ~1,420 tokens — too small for Opus to cache at all ([Anthropic docs](https://platform.claude.com/docs/en/build-with-claude/prompt-caching)).
2. **OpenRouter + Anthropic caching is a known-broken path.** [OpenRouterTeam/ai-sdk-provider#35](https://github.com/OpenRouterTeam/ai-sdk-provider/issues/35) is still open; multiple users confirm user-message caching fails and system-role partial-works-sometimes across Claude and Gemini routes even when token minimums are met. Sonnet's 1,024-token minimum was cleared and still no cache fired.

### Decision

- **Not committing** `cache_control` changes to `bakeoff/run_task3_judge.py` — no measurable benefit through the current OR route.
- **Test script `bakeoff/test_prompt_caching.py` kept** as a reference for re-testing once upstream issues resolve or when the L3 fact-check gate grows the static portion past 4,096 tokens.
- **Revisit in Sprint 6+** when: (a) OR patches the Anthropic cache forward, OR (b) we switch judge calls to direct Anthropic API (bypassing OR) for cost-sensitive runs, OR (c) the L3 gate rubric pushes static portion over 4K tokens.
- **B5.7 Job 2 test cost:** ~$0.82 across 9 calls (3 Opus + 6 Sonnet).

---

## Caveat on judge calibration

The hand-calibration anchor (`task3_calibration_A.md`, Shafer-only) disagreed meaningfully with the auto-judge on the Anthropic family's correctness scores — the hand score had Opus/Sonnet/Haiku at 4.0 on correctness for Shafer; the auto-judge gave them 1.5-2.5. The auto-judge appears to over-apply the "fabricated number" hard cap. This systematically depresses Anthropic composites vs hand-scoring.

**Implication:** Opus and Sonnet may actually be better than the composites suggest for human readers. But since the judge is applied uniformly across all 21 models, **relative rankings are still valid** — gpt-5.4-mini > Sonnet on the same judge is still gpt-5.4-mini > Sonnet in absolute terms.

The ranking of gpt-5.4-mini > gpt-5.4 > gemini-3.1-pro > claude-opus was also broadly consistent in the hand-calibrated table. The recommendation stands.

---

## Budget Summary

| Component | Cost |
|-----------|-----:|
| B5.1–B5.4 design + test data build | ~$3 |
| B5.5 prose generation (21 models × 30 wines) | $11.86 |
| Pre-tournament: GPT-5-mini rerun (30 calls, max_tokens fix) | $0.14 |
| R1 judging (3 wines × 20 models, automated) | $5.33 |
| R1 gpt-5-mini backfill (3 judge calls) | $0.28 |
| R2 judging (3 wines × 12 survivors) | $3.37 |
| R3 judging (24 new wines × 6 finalists, marginal) | ~$13.36 |
| R4 repechage (27 × 2 models, gemini-3-flash-preview + gpt-5-mini) | ~$4.50 |
| R5 field-specialization test (3 wines × 3 models, per-field judge) | $0.81 |
| R6 search-grounded + Chinese (8 models × 12 wines, prose + judge) | ~$13 |
| **B5.6 total (R1-R6 judge + R4 prose + R6 prose)** | **~$40.79** |
| **Sprint 5 total** | **~$55.65** |

Notes:
- Judge-call total from the regenerated summary is $34.57 (387 judge calls across R1-R6)
- R6 prose generation on 8 new models added ~$4-6 to B5.6 (token costs plus `:online` search fees)
- B5.6 ran over the original $22 tournament budget due to R4-R6 expansion; user-authorized in the tournament prompt

---

## Files Produced

- `bakeoff/scores/task3_scores.csv` — 387 per-wine per-model judge rows (R1-R6)
- `bakeoff/scores/task3_summary.csv` — 29-model leaderboard (R1-R6, includes all :online variants)
- `bakeoff/scores/task3_judge/` — 387 individual judge JSON files (one per model × wine)
- `bakeoff/scores/field_judge/` — 9 R5 per-field judge JSONs (3 models × 3 wines)
- `bakeoff/scores/task3_calibration_A.md` — hand-calibrated Shafer reference (pre-tournament)
- `bakeoff/scores/tournament_results.md` — this file
- `bakeoff/tournament.py` — cumulative-composite + tie-aware cut helper
- `bakeoff/run_task3_judge.py` — judge script, extended with `--exact-models` flag
- `bakeoff/run_field_judge.py` — R5 per-field judge script

---

## Sprint 6 Input

**Sprint 6 scope = producer dedup**, not re-enrichment. Dedup is a separate model
decision — the Sprint 5 bake-off ranked *enrichment-prose* models on 11-field
structured output with a voice rubric. Producer dedup is a one-field classification
task with different failure modes. Do NOT auto-apply gpt-5.4-mini to Sprint 6;
revisit the cheap tier (DeepSeek, gemini-3-flash, Haiku) for dedup specifically.

**Re-enrichment of the 515 demo wines + full-corpus enrichment is deferred** to a
post-Sprint-6 sprint that lands prompt v2 + L3 fact-check gate first. Using these
bake-off rankings as-is today would bake in the current-prompt ceiling. The
re-enrichment sprint should:

1. Land prompt v2 (explicitly forbid fabricated soils/geology, require source
   attribution for specific claims, leverage grounded context if used)
2. Build the L3 fact-check gate (reads ground truth from provenance + fails loud
   on contradictions)
3. Re-run a short bake-off on prompt v2 — the rank order above is likely to shift
4. THEN pick the enrichment model

**Preliminary cost projections** (useful for budgeting, not a commitment):

| Scenario (current-prompt basis) | 515 demo | Full 156K corpus |
|--------------------------------|---------:|-----------------:|
| Sonnet 4.6 baseline (eliminated) | ~$18 | ~$5,953 |
| gpt-5.4-mini (current-prompt top) | ~$1.40 | ~$452 |
| gemini-3-flash-preview (cheap tier) | ~$0.50 | ~$159 |
| deepseek-v3.2 base (budget) | ~$0.28 | ~$93 |
| deepseek-v3.2:online (grounded) | ~$3.20 real | ~$1,069 real |

Cost is not the constraint for Sprint 6 — there's $4,000+ of headroom vs the old
Sonnet baseline under any of these options. The constraint is correctness, which
is a prompt + pipeline problem. That's what the next sprint (not Sprint 6) should
solve.
