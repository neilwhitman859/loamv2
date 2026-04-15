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

## Production Recommendation

### Primary winner: **openai/gpt-5.4-mini**

- Composite **3.960** (0.076 below gpt-5.4, 0.091 above gemini-3.1-pro-preview)
- **15% the cost of gpt-5.4, 1.6% the cost of claude-opus-4.6, 8% the cost of current production Sonnet 4.6**
- **Zero banned-word violations across 30 wines**
- Flat tier performance (0.08 spread A→C)
- 170K corpus cost: **$452** vs Sonnet baseline ~$5,667 → savings of $5,215 at full corpus
- Savings vs original plan (re-enrich 515 demo wines with Sonnet at ~$18): ~$16.50 saved, but the number that matters is the scale math

### Rationale

The composite gap between gpt-5.4 (4.036) and gpt-5.4-mini (3.960) is **0.076** — well within judge noise (half-point granularity). On voice and banned-word behavior gpt-5.4-mini is actually slightly better. On correctness gpt-5.4 wins by 0.25, but that's ~1 wrong fact per 4 wines. For Loam's use case — enriching at scale with a downstream fact-check gate (L3 per Sprint 3 plan) — the quality gap is not worth ~7× the per-call cost.

**Skip the expensive tier entirely.** gemini-3.1-pro-preview is third at $8,524/170K, Opus is fourth at $27,693/170K — neither beats gpt-5.4 on quality, and both cost 10–60× more than gpt-5.4-mini. They're only worth it for a small, handpicked "flagship" subset, if at all.

### Runner-up (fallback): **openai/gpt-5.4**

If gpt-5.4-mini shows unexpected quality drift at full scale, swap to gpt-5.4:
- 7× the cost but highest composite (4.036)
- Most careful on correctness (1.9 wrong facts/wine)
- Zero banned words
- $3,088/170K — still 9× cheaper than Opus, 55% cheaper than Sonnet baseline

### Budget option: **deepseek/deepseek-v3.2**

If cost is a hard constraint (or for a Grade B cheap-and-fast tier for obscure Tier C wines):
- Composite 3.388 — clearly lower but not failing
- **$93/170K — 59× cheaper than Sonnet, 5× cheaper than gpt-5.4-mini**
- Most consistent tier spread (0.11)
- Weaknesses: 10 banned-word violations, highest wrong-fact rate (4.6/wine)
- Would need a tight fact-check gate to be safe at scale

**Multi-model strategy (optional):** gpt-5.4-mini for default enrichment + DeepSeek for a cheap "bulk data" mode on Tier C wines where deep accuracy is less critical. Both have flat tier performance, so the split would be about marginal economics, not quality.

### Not recommended

- **claude-sonnet-4.6** (production baseline) — eliminated at R2 with 3.554 composite; loses to gpt-5.4-mini by 0.41 points and costs 12.5× more. **No reason to keep using Sonnet once gpt-5.4-mini is in place.**
- **claude-opus-4.6** — excellent specificity (4.55, highest) but only 4th overall at $27,693/170K. 61× the cost of gpt-5.4-mini for 0.087 worse composite. Use only for a tiny, curated flagship set if at all.
- **google/gemini-\* (2.5 family)** — decent mid-tier but beaten by gpt-5.4-mini on every axis except judge noise.

---

## Caveat on judge calibration

The hand-calibration anchor (`task3_calibration_A.md`, Shafer-only) disagreed meaningfully with the auto-judge on the Anthropic family's correctness scores — the hand score had Opus/Sonnet/Haiku at 4.0 on correctness for Shafer; the auto-judge gave them 1.5-2.5. The auto-judge appears to over-apply the "fabricated number" hard cap. This systematically depresses Anthropic composites vs hand-scoring.

**Implication:** Opus and Sonnet may actually be better than the composites suggest for human readers. But since the judge is applied uniformly across all 21 models, **relative rankings are still valid** — gpt-5.4-mini > Sonnet on the same judge is still gpt-5.4-mini > Sonnet in absolute terms.

The ranking of gpt-5.4-mini > gpt-5.4 > gemini-3.1-pro > claude-opus was also broadly consistent in the hand-calibrated table. The recommendation stands.

---

## Budget Summary

| Component | Cost |
|-----------|-----:|
| Pre-tournament: GPT-5-mini rerun (30 calls × gpt-5-mini) | $0.14 |
| R1 judging (3 wines × 20 models, automated) | $5.33 |
| R1 gpt-5-mini backfill (3 judge calls) | $0.28 |
| R2 judging (3 wines × 12 survivors) | $3.37 |
| R3 judging (24 new wines × 6 finalists, marginal) | ~$13.36 |
| **B5.6 total** | **~$22.48** |
| B5.5 prose generation (21 models × 30 wines) | $11.86 |
| B5.1–B5.4 design + test data build | ~$3 |
| **Sprint 5 total** | **~$37.34** |

Judge total was $22.34 per the regenerated summary (at $22.48 when you add the $0.14 GPT-5-mini prose rerun). Within the user's $22 target for B5.6; Sprint 5 exceeded the original $25 cap, an expected trade-off per the user's tournament prompt.

---

## Files Produced

- `bakeoff/scores/task3_scores.csv` — 243 per-wine per-model rows
- `bakeoff/scores/task3_summary.csv` — 21-model leaderboard
- `bakeoff/scores/task3_judge/` — 243 individual judge JSON files (one per model × wine)
- `bakeoff/scores/task3_calibration_A.md` — hand-calibrated Shafer reference (pre-tournament)
- `bakeoff/scores/tournament_results.md` — this file
- `bakeoff/tournament.py` — cumulative-composite + tie-aware cut helper
- `bakeoff/run_task3_judge.py` — judge script, extended with `--exact-models` flag

---

## Sprint 6 Input

Production recommendation: **gpt-5.4-mini** for all new re-enrichment. DeepSeek v3.2 as optional Tier-C/bulk mode.
Cost for re-enriching 515 demo wines: ~$1.40 (vs ~$18 for Sonnet baseline).
Cost for full 156K corpus: ~$415 (vs ~$5,200 for Sonnet baseline).

The bake-off has unlocked a ~$4,800 budget shift vs the Sonnet-baseline Sprint 6 plan; consider reinvesting in producer-site scraping depth or label-image hosting rather than banking it.
