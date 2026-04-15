# Task 3 Calibration — Tier A: Shafer One Point Five

## Ground Truth (from prompt data)
- 100% Cabernet Sauvignon
- ABV 15.3% (2021, 2022 vintages)
- Prices: $127-140 (Wally's, Spec's)
- TEXSOM bronze medals (1985-1991 vintages)
- Napa Valley appellation
- Cascade: Napa Valley overview, Cab Sauv flavor profile
- **NOT in prompt**: pH, TA, cases produced, oak details, winemaker name

Any model citing pH, cases, or oak specifics is fabricating.

## Scores (1.0-5.0, half-points allowed)

| # | Model | Ground | Spec | Voice | Useful | Notes |
|---|-------|--------|------|-------|--------|-------|
| 1 | claude-haiku-4.5 | 4.0 | 4.0 | 4.0 | 4.0 | Solid all-around. Mentions "Reserve bottlings" (doesn't exist) — minor. Good soil names (Bale, Yolo, Hambright). Direct voice. |
| 2 | claude-opus-4.6 | 4.0 | 4.5 | 4.5 | 4.5 | Mentions "new oak" (not in prompt) — mild fabrication. Otherwise excellent grounding. Very specific geology (Franciscan Complex, Vaca Range). Strong voice: "unapologetically big." |
| 3 | claude-sonnet-4.6 | 4.0 | 4.5 | 4.5 | 4.5 | Cites competitor prices (Silver Oak $90-110, Duckhorn $80-100) — unsourced but plausible. "Ancient volcanic tuff" from cascade. Strong voice, explains naming origin well. |
| 4 | deepseek-v3.2 | 4.0 | 3.5 | 4.5 | 3.5 | Clean grounding, short output (1008 tok). Best insider take: "A Napa Cabernet for people who don't like waiting." Less teaching content due to brevity. |
| 5 | gemini-2.5-flash | 3.5 | 3.5 | 2.0 | 3.5 | Uses "sophisticated" (BANNED). Unsourced competitor prices. Geography OK. Voice killed by banned word. |
| 6 | gemini-2.5-pro | 4.0 | 4.0 | 3.5 | 3.5 | Zero banned words. Good geology (Hambright series). But verbose (3561 tok) — Loam voice should be tighter. Prose reads more like an essay than a sommelier friend. |
| 7 | gemini-3-flash-preview | 4.0 | 4.0 | 4.0 | 4.0 | Clean, concise (950 tok). Zero banned words. "Skip this wine if you prefer herbal, lean Bordeaux" — good directness. Solid all around. |
| 8 | gemini-3.1-pro-preview | 3.5 | 4.5 | 3.5 | 3.5 | Mentions "heavy oak regimen" (NOT in prompt = fabrication). Very specific geology but 5246 tokens — way too verbose for Loam voice. |
| 9 | llama-4-maverick | 3.0 | 3.0 | 2.0 | 2.5 | Uses "showcases" (BANNED). Geography error: says Mayacamas for Shafer vineyards (they're on Vaca Range side). Generic insider take. |
| 10 | minimax-m2.7 | 3.5 | 4.0 | 4.0 | 4.0 | Fabricates naming origin ("Bay Area joke about drive time"). But strong voice: "Skip the bronze medals from 1985-1991" is excellent, genuinely useful advice. |
| 11 | mistral-large | 3.5 | 4.0 | 4.5 | 4.5 | Geography slightly off ("southern Napa Valley floor"). But BEST insider take across all models: "Most people buy One Point Five because it's a Shafer wine that's not $300." |
| 12 | mistral-large-2512 | 3.5 | 4.0 | 4.0 | 4.0 | Claims fruit from "same vineyards as Hillside Select, younger vines" — unsourced. Good soil names. Clean voice. |
| 13 | mistral-nemo | 3.0 | 2.5 | 1.5 | 2.5 | Uses "embodies" AND "elegant" (2 BANNED words). "Nestled in the heart of Napa Valley" = textbook filler. Typo: "SHAfer." |
| 14 | gpt-5-mini | N/A | N/A | N/A | N/A | PARSE FAILED (13% overall parse rate). Cannot score. Dead for production. |
| 15 | gpt-5.4 | 4.0 | 4.0 | 4.5 | 4.5 | Zero banned words. "Shafer's main event for people who actually drink their Cabernet rather than collect labels" — perfect Loam voice. |
| 16 | gpt-5.4-mini | 4.0 | 4.0 | 4.0 | 4.0 | Zero banned words. Clear tradeoff framing. "You are paying for Napa Cabernet density and polish, not a lesson in terroir subtlety." |
| 17 | qwen3.5-plus-02-15 | 3.5 | 3.5 | 3.0 | 3.0 | Zero banned words BUT 7518 tokens — 4x ideal length. Verbosity is itself a voice violation. Good info buried in too much text. |
| 18 | qwen3.6-plus | 3.5 | 3.5 | 3.0 | 3.5 | Zero banned words but 5228 tokens. Better than Qwen 3.5 but still verbose. |
| 19 | grok-4.1-fast | 1.5 | 3.5 | 3.5 | 3.0 | FABRICATED: "20 months in French oak" — NOT in prompt data. Hard penalty: max 1.5 on grounding per rubric. Otherwise decent voice. |
| 20 | mimo-v2-flash | 2.5 | 3.0 | 3.0 | 2.5 | Wrong naming origin. "Extended oak aging" (unsourced). Compares to Cask 23/Martha's Vineyard ($200-300+ wines) — bad value comps. Tells reader wine is about "precision" — misleading for 15.3% ABV. |
| 21 | mimo-v2-pro | 4.0 | 3.5 | 4.0 | 4.0 | Good grounding. Practical advice: "chill it to 60F" — genuinely useful. "You'll feel the alcohol on the finish" — honest. |

## Composite Scores (weighted: Ground 30%, Voice 25%, Spec 20%, Useful 15%, Diversity TBD 10%)

Excluding diversity (scored at batch level later), provisional 4-dimension weighted:

| Model | Composite (4-dim) | Rank |
|-------|-------------------|------|
| gpt-5.4 | 4.28 | 1 |
| claude-opus-4.6 | 4.33 | 2 |
| claude-sonnet-4.6 | 4.33 | 2 |
| mistral-large | 4.18 | 4 |
| gpt-5.4-mini | 4.00 | 5 |
| claude-haiku-4.5 | 4.00 | 5 |
| gemini-3-flash-preview | 4.00 | 5 |
| deepseek-v3.2 | 3.95 | 8 |
| mistral-large-2512 | 3.88 | 9 |
| mimo-v2-pro | 3.88 | 9 |
| minimax-m2.7 | 3.88 | 11 |
| gemini-2.5-pro | 3.73 | 12 |
| gemini-3.1-pro-preview | 3.70 | 13 |
| qwen3.6-plus | 3.38 | 14 |
| qwen3.5-plus-02-15 | 3.25 | 15 |
| gemini-2.5-flash | 3.08 | 16 |
| llama-4-maverick | 2.58 | 17 |
| mimo-v2-flash | 2.78 | 18 |
| grok-4.1-fast | 2.73 | 19 |
| mistral-nemo | 2.33 | 20 |
| gpt-5-mini | N/A | 21 |
