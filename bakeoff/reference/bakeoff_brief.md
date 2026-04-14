# Loam model bake-off brief

Hi Claude Code — please design and execute a model bake-off for Loam across three task tiers. I'd like you to handle the test design, harness, execution, and final write-up. This document gives you all the context and the final model list. I'm fine with the bake-off being slow and expensive — quality of the evaluation matters more than speed or cost to me.

---

## About Loam

Loam is a wine data platform. Users look up any wine and get the full story — place, vintage weather, soil, grape varieties, producer choices — as structured, connected data. The product is organized facts rendered clearly, not AI prose. Every wine page shows the same structured fields: chemistry (ABV, pH, TA), blend percentages, oak program, production volume, appellation rules, vintage weather, scores, and prices — displayed as labeled fact grids, not buried in paragraphs.

The database has 156,000 wines from 32 sources, 10,600 producers, 3,600 appellations with weather data, and 125,000 vintages. Data flows from government registries (TTB COLA, state liquor boards), importer catalogs, retailer feeds, and competition results through a staging-to-canonical promotion pipeline. The backbone is LWIN (London International Vintners Exchange) identifiers.

On top of the structured data sits an AI-generated prose layer: wine summaries, terroir explanations, food pairings, cellar recommendations, market positioning, and insider commentary. This prose must be grounded in the structured data already in the prompt — it explains and contextualizes the facts, it doesn't replace them. We've enriched 515 wines at high quality using Claude Sonnet 4.6 at ~$0.036/wine (about $37 total). Now we need to scale to the full corpus cost-effectively.

## The Loam voice

Specific and direct. It names particular soils, particular climate patterns, particular grapes — and draws the line between place and taste. It has a point of view ("Cru Beaujolais is the best value in Burgundy-style Pinot"), is comfortable being negative ("most wines from this region are unremarkable"), and explains technical concepts inline for enthusiasts who aren't sommeliers. Generic AI wine writing ("producing wines of remarkable quality," "an exquisite accompaniment") is a failure mode, not a baseline. The touchstone is producer fact sheets like Cain Five's notes by Christopher Howell — knowledgeable, unpretentious, place-centered.

## Why this bake-off

Task 3 (customer-facing prose) is 90%+ of total spend. A successful bake-off could cut the full-corpus enrichment cost from ~$1,760 (Sonnet 4.6 baseline) to under $500 if a cheaper model clears the quality bar. The economics hinge entirely on Task 3 prose quality, so I want that tested exhaustively. Tasks 1 and 2 get tested too but with less ceremony.

---

## The three tasks

### Task 1 — classification / dedup
- **Volume:** ~4,000 calls in production
- **Input:** Two wine names from the same producer, plus any available metadata (grape, color, appellation). May include web search results in the prompt for ambiguous cases.
- **Output:** Structured JSON verdict — same wine or different wine, with confidence score and reasoning.
- **Examples:**
  - `Armillary` vs `Armillary Cabernet Sauvignon` → SAME (varietal suffix pattern)
  - `York Creek Zinfandel` vs `York Creek Cabernet Sauvignon` → DIFFERENT (different grapes despite shared vineyard name)
  - `Bosconia` vs `Bosconia Gran Reserva` → DIFFERENT (in Rioja, classification tier changes the bottling entirely)
- **Quality bar:** 97%+ overall accuracy. **Under 1% false positive rate on merges** — a bad merge permanently destroys data (scores, prices, vintages, grape links all get combined incorrectly). I'd rather leave 100 duplicates unmerged than incorrectly merge 2 distinct wines. False positive rate is the hard metric here, not raw accuracy.
- **Approximate token budget per call:** 500 input + 200 output.

### Task 2 — structured data extraction
- **Volume:** ~10,000-15,000 calls in production
- **Input:** Raw HTML from producer websites, tech sheet PDFs, or web search results.
- **Output:** Structured JSON — blend percentages, ABV, oak program (% new, months, vessel type), cases produced, chemistry (pH, TA, RS, SO2), harvest dates, winemaker name, farming practices, vineyard details.
- **Quality bar:** 99%+ on numerical values. If a tech sheet says "14.2% ABV, 22 months in 60% new French oak, 3,200 cases produced," every single number must come through exactly right. 97%+ on narrative/contextual extraction. **Must output null when data isn't present** — hallucinating "pH 3.4" when the page doesn't mention pH is a disqualifying failure. These numbers end up in user-facing fact grids where one wrong value undermines the whole product's credibility.
- **Approximate token budget per call:** 3,000 input + 500 output.

### Task 3 — customer-facing wine prose (the main event)
- **Volume:** ~170,000 calls in production
- **Input:** ~2,000 tokens of structured context per wine (identity, grapes, scores, prices, appellation data, producer info, vintage weather, chemistry).
- **Output:** ~800-1,500 tokens of prose across 8-11 fields (wine summary, style profile, terroir expression, vinification summary, food pairing, cellar recommendation, comparable wines, hook line, market info, value assessment, insider take).
- **Quality bar:** Genuinely excellent writing that a knowledgeable wine person would read and think "this is well-written and accurate." The five things I care about:
  1. **Specific** — names soils, climate patterns, geological formations, not "well-drained soils."
  2. **Direct** — has a point of view, comfortable being negative about mediocre wines.
  3. **Factually grounded** — every claim traceable to data in the prompt, never fabricates numbers or percentages.
  4. **Structurally varied** — 10 consecutive wines must read differently. No template patterns, no repeated openers, no "mad libs with a wine skin."
  5. **Voice match** — sits comfortably next to Cain Five / Christopher Howell-style producer notes.
- **Approximate token budget per call:** 2,000 input + 1,200 output.

---

## Models to test

All models below resolve to OpenRouter slugs I've verified as of April 14, 2026. Preview models may shift — spot-check each slug on openrouter.ai/models before running. Run everything through OpenRouter for simplicity, even though native APIs would offer better batch+cache economics for production.

### Task 1 — dedup (17 models)

```
anthropic/claude-sonnet-4.6          (baseline)
anthropic/claude-haiku-4.5
deepseek/deepseek-v3.2
google/gemini-2.5-flash
google/gemini-2.5-pro
openai/o4-mini
cohere/command-r7b
meta-llama/llama-4-maverick
meta-llama/llama-4-scout
mistralai/mistral-nemo
openai/gpt-5.4
openai/gpt-5.4-mini
x-ai/grok-4.1-fast
qwen/qwen3.6-plus
google/gemini-3-flash-preview
google/gemini-3.1-flash-lite-preview
xiaomi/mimo-v2-flash
```

### Task 2 — extraction (17 models)

```
anthropic/claude-sonnet-4.6          (baseline)
anthropic/claude-haiku-4.5
deepseek/deepseek-v3.2
google/gemini-2.5-flash
google/gemini-2.5-pro
qwen/qwen3.5-plus
openai/gpt-5.4-mini
meta-llama/llama-4-maverick
mistralai/mistral-large
openai/gpt-5.4
qwen/qwen3.6-plus
x-ai/grok-4.1-fast
google/gemini-3.1-pro-preview
google/gemini-3-flash-preview
google/gemini-3.1-flash-lite-preview
xiaomi/mimo-v2-flash
minimax/minimax-m2.7
```

### Task 3 — prose (21 models, this is the main event)

```
anthropic/claude-sonnet-4.6          (baseline — current production)
anthropic/claude-haiku-4.5
anthropic/claude-opus-4.6
deepseek/deepseek-v3.2
google/gemini-2.5-flash
google/gemini-2.5-pro
meta-llama/llama-4-maverick
mistralai/mistral-large
mistralai/mistral-large-3
mistralai/mistral-nemo
openai/gpt-5.4
openai/gpt-5.4-mini
openai/gpt-5-mini
qwen/qwen3.5-plus
qwen/qwen3.6-plus
google/gemini-3.1-pro-preview
google/gemini-3-flash-preview
xiaomi/mimo-v2-pro
xiaomi/mimo-v2-flash
minimax/minimax-m2.7
x-ai/grok-4.1-fast
```

---

## Notes on specific models

- **`xiaomi/mimo-v2-pro`** is the standout wildcard. Currently #1 on OpenRouter by weekly token volume (4.65T/week). OpenRouter's own listing describes "perceived performance approaching Opus 4.6." No polled assistant suggested this — it's the highest-priority dark horse in Task 3.
- **`openai/gpt-5.4-mini`** was my highest-upside pick in the earlier research report. Estimated $483 optimized for Task 3 if quality holds. Please prioritize Phase 1 testing for this one.
- **`google/gemini-3.1-pro-preview`** is a preview model ($2/$12) that per April 2026 reporting leads 13 of 16 major benchmarks and ties GPT-5.4 Pro on the Intelligence Index at ~1/3 the cost. Marketing highlights structured-output strength.
- **`deepseek/deepseek-v3.2`** has cache-hit economics unmatched on the market ($0.028/M cached). JSON mode works but it's not strict schema-constrained — build validation into the harness.
- **Gemini thinking tokens bill as output.** For Flash-tier Gemini models, if thinking isn't explicitly disabled, output costs can inflate 30-80%. Disable it for fair comparison unless you're intentionally testing reasoning-enabled mode.
- **`x-ai/grok-4.1-fast`** has a 2M context window, which is nice for Task 2 (long tech sheets) but not distinctive for Tasks 1 or 3.

---

## What I'm asking you to do

You have latitude on test design. Some thoughts on what I'd find useful, but I trust your judgment:

**Phase 1 — Task 3 prose shootout (most important).** This is 90%+ of production spend and the decision that matters most. I'd suggest stratifying ~30 test wines across the corpus — some well-known (Napa Cab, Bordeaux classed growth), some mid-obscurity (Jura, Mosel, Etna), some deep cuts (natural-wine producers, obscure appellations). Run all 21 Task 3 models on the same prompts. Build a blind evaluation rubric scoring each on the five criteria above. I'll human-score the rubric myself if you produce clean side-by-side outputs. Flag anything that fails the voice match against Sonnet 4.6 — that's the pass/fail gate.

**Phase 2 — Task 2 extraction validation.** After Phase 1 winners are known, test Task 2 candidates against 100 tech sheets with ground-truth numeric data. Focus on 99%+ numerical accuracy and null-handling on missing fields. Prioritize providers that already survived Phase 1 for integration reuse.

**Phase 3 — Task 1 dedup validation.** 200 hand-labeled wine-name pairs, stratified toward hard cases: Rioja classification tiers, Burgundy single-vineyard vs village-level, grape-variant bottlings, vintage-specific cuvées. Measure false positive rate specifically — one FP on 100 different-wine pairs ≈ 1% FPR, which is right at the threshold. Zero FPs preferred.

**Open questions where I'd like your recommendation:**

1. Test wines for Phase 1 — should we use real Loam data (risks: baseline contamination if Sonnet has enriched them already) or synthetic/public wines (risks: less realistic)? I lean real but flag your take.
2. How to rate-limit against preview-model quotas without slowing the whole thing to a crawl. Some of these (especially Gemini 3.x previews and MiMo) may have lower rate limits.
3. Whether to test Sonnet 4.6 twice — once as baseline, once as a "did we reproduce the prior 515-wine quality" check. I think yes but you decide.
4. How to score "structural variety" across 10 consecutive wines — this is the trickiest criterion to measure without a human in the loop. Any automated proxies (embedding similarity, n-gram overlap) worth computing?

## Deliverables I'd like at the end

1. A Markdown report ranking all models per task with scores, cost actuals, and a recommendation for each task tier.
2. The raw outputs from every model for every test wine, saved to the repo so I can spot-check anything.
3. A scorecard CSV I can sort and filter.
4. A final production recommendation: one model per task tier, with projected total cost for the full 170K-wine enrichment run using batch + cache optimization on the chosen provider's native API.
5. Any surprises or anomalies worth flagging — especially if a cheap model beats an expensive one, or vice versa.

## Operational notes

- Repo: `neilwhitman859/loamv2`. Create a `bakeoff/` directory.
- Use OpenRouter for all API calls. One API key, one credit balance. Don't route through native APIs for this — we'll do that when we productionize the winner.
- Budget: no hard cap. Rough mental model is "spending $200 to avoid a bad $1,600 production decision is a good trade."
- Do NOT use production database data that would contaminate existing Sonnet 4.6 enrichments. If you need real wines, pull from sources we haven't enriched yet.
- Check in before Phase 2 and Phase 3 with a brief summary of Phase 1 results — I may want to adjust based on what we learn.

Thanks. I'm genuinely curious how this turns out. The MiMo-V2-Pro result specifically could be the whole story.
