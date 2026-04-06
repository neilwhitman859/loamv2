# Session: Recover Lost Data from 2026-04-04 Inference Revert

Read CLAUDE.md first. Give a briefing. Then read `docs/DECISIONS.md` entry "No probabilistic inference on canonical columns" and `memory/feedback_no_probabilistic_inference.md` before touching any canonical table.

**This session is about recovery, not new inference.** Every operation must use real source data (staging tables, APIs, scraped files). Do not fill NULLs with guesses. If a recovery method would require probabilistic inference, stop and flag it.

## IMPORTANT: Check current state first

Previous sessions may have already completed some of these recovery steps. Before executing anything:

1. Query the DB for current row counts on all affected tables
2. Compare against the "pre-revert" and "post-revert" targets below
3. Skip any recovery step that's already been done
4. Report what's already recovered vs what's still missing

**Pre-revert peaks (target to recover toward):**
- Prices: 55,335 rows, 25,898 distinct wines (5.25%)
- wine_grapes: 195,468
- Wines with color: 266,565

**Post-revert lows (what the revert left us at):**
- Prices: 26,727 rows, 16,683 distinct wines (3.36%)
- wine_grapes: 151,640
- Wines with color: 180,645

If current counts are already near the pre-revert peaks, the recovery is done and this session should focus on something else. Check CLAUDE.md "Content Tables" section for the latest numbers — they may already reflect recovery work done in other sessions.

## Recovery steps (execute only what's still needed)

### Recovery #1: Wally's vintage parsing → prices (BIGGEST WIN, ~17.5K wines)

**Problem:** Wally's `vintage` column is NULL for all 17,550 matched wines, BUT the vintage year is in the `title` column. Example:
```
title = "2023 de Montille Meursault 1.5L"
vintage = NULL
```

A blanket NV (year 0) fallback created 17,550 phantom NV price rows. All deleted in the revert.

**Check if already done:** `SELECT COUNT(*) FROM source_wallys WHERE vintage IS NOT NULL AND vintage != '0';` — if >10K, title parsing was already done.

**Recovery plan (if not done):**
1. Build a title parser: extract `^\d{4}` or `\b(19|20)\d{2}\b` from Wally's title, validate year is 1900-current+1
2. Update `source_wallys.vintage` with parsed year where currently NULL
3. Re-run promotion: create `wine_vintages` rows with actual year, link prices via real vintage_id
4. For wines where title has no year (true NV), skip or promote as NV only if wine_type='sparkling' or wine_type='fortified'

**Script to create:** `pipeline/promote/wallys_title_parser.py`

### Recovery #2: Wine grape links via real TTB data (~44K lost)

**Check if already done:** `SELECT COUNT(*) FROM wine_grapes;` — if near 195K, already recovered.

**Recovery plan (if not done):** Re-run existing pipelines that use TTB's real `grape_varietals` field:
1. `pipeline/promote/ttb_grape_promote.py`
2. `pipeline/promote/grape_from_helper.py`

### Recovery #3: Wine color from authoritative sources (~85K lost)

**Check if already done:** `SELECT COUNT(*) FROM wines WHERE color IS NOT NULL;` — if near 267K, already recovered.

**Recovery plan (if not done):**
1. Check `source_lwin` for color column — promote if exists
2. Check `xwines_wines` for color data
3. Check importer sources: European Cellars `color` (238), Skurnik `color` (~4K), Domestique/LastBottle/BestWineStore

### Recovery #4: NV prices for genuinely NV wines (smaller)

**Check if already done:** `SELECT COUNT(*) FROM wine_vintage_prices WHERE vintage_year = 0;` — compare against expected.

**Recovery plan (if not done):**
- Filter: wines where `wine_type IN ('sparkling', 'fortified')` AND source had explicit "NV"/"Non-Vintage"
- Only Enofile had explicit "NV" values (~473 rows)

## Validation before running anything

For each recovery step:
1. Count how many rows WILL be affected before UPDATE/INSERT
2. Run on a 10-row sample first, verify output looks right
3. Only then run full batch
4. Re-measure readiness after each recovery to see actual impact

## Do NOT do

- **No probabilistic inference.** Do not compute wine fields from related tables unless it's 1:1 schema-level (appellation→region is OK; majority-vote is NOT).
- **Do not fill NULLs with guesses.** NULL is a valid state.
- **Do not write to canonical columns without a source trail.**
- **Do not touch the items listed in "Kept (definitional)" in CLAUDE.md** — those are safe.
- **Do not enrich** (enrichment is a separate session track).

## Acceptance criteria

- All 4 recovery steps checked against current DB state
- Any remaining gaps filled from authoritative sources only
- Price coverage at or above 5% (3-run average)
- wine_grapes at or above 195K
- No new inference entries in the DB

## Wrapping up

When done: update CLAUDE.md price/grape/color numbers, log any decisions, commit, push.
