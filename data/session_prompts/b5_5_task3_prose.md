# B5.5: Run Task 3 — Customer-Facing Prose (21 models × 30 wines)

## What This Is

Sprint 5 (AI Bakeoff) block 5.5. Send 30 wine context packages to 21 models via
OpenRouter and collect raw prose outputs. No scoring in this session — that's B5.6
(Opus inline judging). This block just generates the data.

## Context

- B5.3 (dedup) and B5.4 (extraction) are done. Scores in `bakeoff/scores/`.
- Test data: `bakeoff/data/task3/contexts.json` — 30 wines, 3 tiers (A=famous/rich,
  B=mid-obscurity, C=thin data), 10 wines each.
- The prompt is the Grade A production prompt from `pipeline/enrich/batch_enrich.py`
  (`build_grade_a_prompt` function). Use the same VOICE_PREAMBLE, identity block,
  data blocks, cascade context, and output schema. Every model gets the identical prompt.
- Design doc: `bakeoff/DESIGN.md` (Task 3 section).

## 21 Models (from `bakeoff/MODELS.md`, Task 3 list)

```
anthropic/claude-sonnet-4.6
anthropic/claude-haiku-4.5
anthropic/claude-opus-4.6
deepseek/deepseek-v3.2
google/gemini-2.5-flash
google/gemini-2.5-pro
meta-llama/llama-4-maverick
mistralai/mistral-large
mistralai/mistral-nemo
openai/gpt-5.4
openai/gpt-5.4-mini
openai/gpt-5-mini
qwen/qwen3.5-plus-02-15
qwen/qwen3.6-plus
google/gemini-3.1-pro-preview
google/gemini-3-flash-preview
xiaomi/mimo-v2-pro
xiaomi/mimo-v2-flash
minimax/minimax-m2.7
x-ai/grok-4.1-fast
mistralai/mistral-large-3
```

NOTE: Some slugs may be wrong on OpenRouter. Check the models API first:
`GET https://openrouter.ai/api/v1/models`. Lessons from B5.3/B5.4:
- `cohere/command-r7b` needed `-12-2024` suffix
- `qwen/qwen3.5-plus` needed `-02-15` suffix
- `google/gemini-3.1-pro-preview` needs reasoning enabled (don't disable thinking)
- `google/gemini-2.5-pro` needs reasoning enabled AND higher max_tokens (8000+)
- `mistralai/mistral-large-3` — verify slug exists
- `xiaomi/mimo-v2-pro` — verify slug exists
- `openai/gpt-5-mini` — verify slug exists (distinct from gpt-5.4-mini)

## Architecture

Build `bakeoff/run_task3.py` following the pattern of `run_task1.py` and `run_task2.py`:

1. **Load contexts** from `bakeoff/data/task3/contexts.json`
2. **Build prompts** using the Grade A prompt builder from `batch_enrich.py`
   - Import `build_grade_a_prompt` from `pipeline.enrich.batch_enrich`
   - Each wine context dict feeds directly into this function
3. **Call OpenRouter** for each model × wine combination
4. **Save raw results** to `bakeoff/results/task3/{model_slug}/{wine_id}.json`
   - Include: raw_content, tokens_in, tokens_out, elapsed, error, parse_ok
   - Parse the JSON response but also keep the raw text (the judge needs it)
5. **Resume support** — skip completed pairs (file exists)
6. **Parallel execution** — run 4 groups of ~5 models concurrently (same pattern as B5.3/B5.4)

### Key Differences from Task 1/2

- **Larger output**: Grade A prompt produces ~800-1500 tokens of output. Set max_tokens=3000
  (or 8000 for reasoning models).
- **Temperature**: Use temperature=0.7 (not 0.0) — prose benefits from some creativity.
  This is the production setting.
- **No automated scoring** — just collect outputs. B5.6 scores them via Opus inline.
- **Opus 4.6 is in the model list** — it's expensive ($15+$75/M) but we test it for
  quality ceiling. Limit to the 30 wines only.

### OpenRouter Details

- API key in `.env` as `OPENROUTER_API_KEY`
- Endpoint: `https://openrouter.ai/api/v1/chat/completions`
- Disable thinking/reasoning for Gemini models EXCEPT 2.5-pro and 3.1-pro-preview
- Headers: `HTTP-Referer: https://loam.onrender.com`, `X-Title: Loam Bake-off`

## Output

```
bakeoff/results/task3/{model_slug}/{wine_id}.json
```

Each file contains the raw model output + metadata. The judge (B5.6) reads these.

## Budget

~$8-12 for all 630 calls. Opus 4.6 will be the most expensive (~$3-4 for 30 calls alone).

## What NOT To Do

- Don't score the outputs — that's B5.6
- Don't modify the prompt per model — every model gets the identical Grade A prompt
- Don't use temperature=0 — prose needs variation
- Don't skip Opus — it sets the quality ceiling
- Don't run Opus in parallel with other models — it's expensive, run it solo to watch for errors

## When Done

- All 21 × 30 = 630 result files saved
- Print a summary: per-model completion count, error count, avg tokens, parse rate
- Update `data/dashboard.html`: mark B5.5 as done, update budget spent
