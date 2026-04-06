# WineTest Session — Run Assessment + Act on Results

Read CLAUDE.md first, give a briefing. Then run WineTest and use results to guide targeted improvements.

## Phase 1: Run WineTest (~2 min, ~$0.60)

```bash
python -m pipeline.analyze.winetest --seed 42
```

Previous baseline (2026-04-06): **56/100** — Findability 83%, Depth 33%, Accuracy 88%, Story 1.8/5.

Read the full report. Note:
- Any findability regressions or improvements
- Blind spots (>50% miss rate by country/color/price_tier/category)
- Accuracy errors flagged by Haiku
- Story score distribution (how many 1s vs 3s vs 5s)

## Phase 2: Diagnose (5 min)

Compare to previous run in `data/stats/winetest/`. Identify the **single highest-impact lever** available right now. The dimensions and their likely fixes:

### Findability (83%)
- Check the "not found" list — are these wines we genuinely don't have, or matching failures?
- If matching failures: improve `scorer.py` fallbacks
- If genuinely missing wines: check if they're in staging tables but unlinked

### Depth (33%)
- This is driven by empty fields on found wines (prices, scores, grapes, appellations, images, etc.)
- Check which depth categories score lowest — that's where to focus
- Price coverage (8.4%) and score coverage (2.2%) are the main drags

### Accuracy (88%)
- Review Haiku's INCORRECT verdicts — are they real errors or Haiku mistakes?
- If real: fix the specific data (wrong region, wrong grape, wrong color)
- If Haiku mistakes: note for prompt tuning

### Story (1.8/5)
- This is mostly blocked on enrichment pipeline (Grade C/B)
- Without wine_insights populated, story scores will stay at 1-2
- If enrichment is available: run batch pre-warming on found wines first

## Phase 3: Act (remainder of session)

Pick ONE improvement track based on diagnosis. Execute it. Then re-run WineTest to measure impact:

```bash
python -m pipeline.analyze.winetest --seed 42
```

Compare before/after. Update CLAUDE.md with new score.

## Rules

- Don't try to improve all 4 dimensions at once — pick the one with best ROI
- Don't modify WineTest itself to inflate scores — the tool measures reality
- Strictly definitional data operations only (no probabilistic inference)
- If the biggest lever is enrichment and it's not built yet, shift to building enrichment infrastructure rather than chasing marginal gains elsewhere
- Commit at end with WineTest score in commit message

## Context

- WineTest code: `pipeline/analyze/winetest/`
- Results archive: `data/stats/winetest/`
- Grape backfill script: `pipeline/promote/grape_from_name.py`
- Key scoring bottlenecks: enrichment pipeline (Story), price sources (Depth), appellation backfill (Depth)
- No probabilistic inference rule: see `docs/DECISIONS.md` and memory
