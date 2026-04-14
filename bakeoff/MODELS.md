# Bake-off Model List

OpenRouter slugs, verified 2026-04-14. All calls routed through OpenRouter.

---

## Task 1 — Dedup / Classification (17 models)

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

## Task 2 — Structured Data Extraction (17 models)

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

## Task 3 — Customer-Facing Prose (21 models)

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

## Notes

- **Gemini thinking tokens bill as output.** Disable thinking for fair comparison unless intentionally testing reasoning mode.
- **DeepSeek v3.2** has exceptional cache-hit economics ($0.028/M cached). JSON mode works but no strict schema — validate in harness.
- **xiaomi/mimo-v2-pro** is the dark horse — #1 on OpenRouter by weekly volume (4.65T/week). Task 3 only.
- **Preview models** (Gemini 3.x) may have lower rate limits. Build retry logic.
- **grok-4.1-fast** has 2M context window — useful for Task 2 long HTML pages.
