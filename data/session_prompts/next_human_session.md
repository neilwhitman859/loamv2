# Session: Push Price Coverage Past 5% + Batch Match Competition Sources

Read CLAUDE.md first. Give a briefing. Two goals: (1) get price coverage from 4.59% to 5%+, (2) batch_match competition sources for score coverage.

## Current State (post price-coverage session, 2026-04-04)
- 496,926 wines | 49,631 prices | 22,807 distinct wines with prices = 4.59%
- 18,732 scores | 202,033 wine_grapes | 320K vintages
- 0 orphaned prices (all linked via wine_vintage_id)
- All currently-matched staging records are promoted. Gap is unmatched records.

---

## Step 1: Add batch_matcher configs for price-bearing sources [HIGH]

These sources have prices but 0 matched wines (batch_matcher doesn't support them yet):
- `source_enofile` (9,166 total, price column: `price`) — competition wines with prices
- `source_best_wine_store` (1,658 total, price column: `price_usd`)
- `source_domestique` (247 total, price column: `price_usd`)
- `source_last_bottle` (160 total, price column: `price_usd`)
- `source_pa` unmatched (5,297 of 5,905, price column: `retail_price`, has `vintage`, `upcs`)

Add `run_enofile()`, `run_best_wine_store()`, etc. to `pipeline/promote/batch_matcher.py`.
Pattern: look at existing `run_flatiron()` or `run_specs()` for the template.
Then run batch_matcher, then promote prices via SQL (faster than retail_promote REST).

## Step 2: Batch match competition sources [MEDIUM — big score gains]

Unmatched competition records with medals:
- `source_berliner`: 69,898 unmatched (94.6% of total). Has `producer`, `country`, `grapes`, `medal`.
- `source_texsom`: 32,619 unmatched (69.6% of total). Has `producer`, `appellation`, `vintage`, `award`.

Add `run_berliner()` and `run_texsom()` to batch_matcher.py. Then promote matched scores.

## Step 3: Promote everything from new matches

After batch_matcher runs:
1. Prices via SQL (NOT retail_promote — SQL is 10x faster, see session notes)
2. Scores from competition matches
3. Grapes from Berliner/Enofile
4. UPCs from PA

---

## Acceptance Criteria

- Price coverage >= 5% (3-run average)
- batch_matcher configs added for >= 3 new sources
- Competition score count increases meaningfully

---

## Do NOT do this session
- Don't build enrichment pipeline (separate session)
- Don't start frontend work
- Don't touch the TTB scraper or barcode scanner

## Wrapping up
Update CLAUDE.md, DECISIONS.md if decisions made, commit, push.
