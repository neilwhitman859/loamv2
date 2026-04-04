# Session: Recover Lost Data from 2026-04-04 Inference Revert

Read CLAUDE.md first. Give a briefing. Then read `docs/DECISIONS.md` entry "No probabilistic inference on canonical columns" and `memory/feedback_no_probabilistic_inference.md` before touching any canonical table.

**This session is about recovery, not new inference.** Every operation must use real source data (staging tables, APIs, scraped files). Do not fill NULLs with guesses. If a recovery method would require probabilistic inference, stop and flag it.

## Current state (2026-04-04 end of day)

- 496,926 wines | 37,184 producers | 318,613 vintages
- **27,383 scores** (kept)
- **26,727 prices** (down from 55,335 before revert — 28.6K NV fallback rows deleted)
- **16,683 distinct wines with prices** (3.36%) — peaked at 25,898 (5.25%) before revert
- **151,640 wine_grapes** (pre-session was 195,468 — lost ~44K legit links as collateral from pattern-delete)
- **180,645 wines with color** (pre-session was 266,565 — lost ~85K non-TTB colors)
- **17,701 UPCs** (kept, no revert)
- **9,324 farming certifications** (kept)
- Data grade: F=461,449, D=35,474, C=0, B=3 (C was mis-used and cleared)

## What was lost and how to recover (authoritative sources only)

### Recovery #1: Wally's vintage parsing → prices (BIGGEST WIN, ~17.5K wines)

**Problem:** Wally's `vintage` column is NULL for all 17,550 matched wines, BUT the vintage year is in the `title` column. Example:
```
title = "2023 de Montille Meursault 1.5L"
vintage = NULL
```

I did a blanket NV (year 0) fallback, which created 17,550 phantom NV price rows for wines that actually have real vintages. All deleted in the revert.

**Recovery plan:**
1. Build a title parser: extract `^\d{4}` or `\b(19|20)\d{2}\b` from the Wally's title, validate year is 1900-current+1
2. Update `source_wallys.vintage` with parsed year where currently NULL
3. Re-run promotion: create `wine_vintages` rows with actual year, link prices via real vintage_id
4. For wines where title has no year (true NV wines like some Champagnes), either skip or promote as NV only if wine_type='sparkling' or wine_type='fortified' (those are legitimately often NV)

**Script to create:** `pipeline/promote/wallys_title_parser.py`

**Impact:** +17,500 real vintage-linked prices, recovering most of the lost price coverage.

### Recovery #2: Wine grape links via real TTB data (~44K lost)

**Problem:** The grape name pattern delete (#11/#12 revert) removed pre-session legitimate grape links along with the pattern-inferred ones.

**Recovery plan:** Re-run the existing proper pipelines that use TTB's real `grape_varietals` field (not name patterns):
1. `pipeline/promote/ttb_grape_promote.py` — TTB grape promotion, already handles encoding corruption
2. `pipeline/promote/grape_from_helper.py` — TTB helper, batched queries

These use the actual grape data TTB captured from wine labels, not name matching.

**Impact:** Should restore most of the ~44K lost links since they were likely TTB-sourced to begin with.

### Recovery #3: Wine color from authoritative sources (~85K lost)

**Problem:** The color revert cleared ~85K pre-session colors that came from non-TTB sources (probably LWIN, xwines, or importer catalogs) because the revert query matched on grape color patterns and couldn't distinguish provenance.

**Recovery plan:**
1. Already re-ran TTB `class_type_desc` (TABLE RED/WHITE WINE) during the revert session — those ~75K are back.
2. Check `source_lwin` for color column — LWIN has structured wine metadata that may include color/type. If yes, promote.
3. Check `xwines_wines` for color data — xwines is bulk data but has fields.
4. Check importer sources: European Cellars has `color` column (238 matched), Skurnik has `color` (~4K), Domestique/LastBottle/BestWineStore have `color`.

**Impact:** Could recover 30-50K colors depending on LWIN/xwines structure. The rest may be unrecoverable without re-scraping.

### Recovery #4: NV prices for genuinely NV wines (smaller)

**Problem:** Some of the 28,608 deleted NV prices were for wines that ARE genuinely non-vintage (Champagne NV, Port NV, Sherry, some Prosecco).

**Recovery plan:**
- Filter: wines where `wine_type IN ('sparkling', 'fortified')` AND source had explicit "NV"/"Non-Vintage" in vintage field (not NULL).
- Only Enofile had explicit "NV" values (337 + 115 + 21 = 473 rows). The rest were NULL fallbacks, which is what we wanted to revert.

**Impact:** Small (~500 prices for genuine NV wines). Low priority.

## Validation before running anything

For each recovery step:
1. Count how many rows WILL be affected before UPDATE/INSERT
2. Run on a 10-row sample first, verify output looks right
3. Only then run full batch
4. Re-measure readiness after each recovery to see actual impact

## Do NOT do

- **No probabilistic inference.** Do not compute wine fields from related tables unless it's 1:1 schema-level (appellation→region is OK; majority-vote is NOT).
- **Do not fill NULLs with guesses.** NULL is a valid state.
- **Do not write to canonical columns without a source trail.** If it came from the staging data, fine. If it came from "most common value" or "all grapes the same color", NOT fine.
- **Do not touch the items listed in "Kept (definitional)" in CLAUDE.md** — those are safe.
- **Do not enrich** (enrichment is a separate session track).

## Acceptance criteria

- Wally's title parser exists and handles >10K wines
- Price coverage recovers from 3.36% to at least 4.5% via real vintage parsing
- wine_grapes count recovers toward 195K (pre-session level) via TTB promotion
- No new inference entries in the DB
- If a recovery would lose collateral, flag it first and ask

## Wrapping up

When done: update CLAUDE.md price/grape numbers, log any decisions, commit, push.
