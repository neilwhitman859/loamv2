# Session: Maximize Price Coverage

Read CLAUDE.md first. Give a briefing. Single focus: maximize price coverage from current sources.

## Current State (post Riddler Run #6, 2026-04-04)
- 470,485 active wines | readiness 40.9/100 — price dimension ~1%
- 30,551 rows in wine_vintage_prices
- Only 7,343 accessible via wine_vintage_id join — 23,208 orphaned (join bug)
- ~1,538 COLA-linked wines with prices out of 292K = ~0.5% coverage

---

## Step 1: Fix the wine_vintage_id Backfill [HIGH — do this first]

~23K price rows have wine_id + vintage_year set but wine_vintage_id = NULL.
The readiness query joins through wine_vintage_id and misses them entirely.
Same bug was fixed for scores in Riddler Run #2. Apply same fix to prices.

```sql
-- Confirm the gap
SELECT
  COUNT(*) as total_prices,
  COUNT(wine_vintage_id) as have_vintage_id,
  COUNT(*) - COUNT(wine_vintage_id) as missing_vintage_id
FROM wine_vintage_prices;

-- Backfill where vintage_year matches
UPDATE wine_vintage_prices wvp
SET wine_vintage_id = wv.id
FROM wine_vintages wv
WHERE wvp.wine_vintage_id IS NULL
  AND wvp.wine_id = wv.wine_id
  AND wvp.vintage_year = wv.vintage_year;
```

After backfill: re-run readiness and confirm price_pct improves meaningfully.

---

## Step 2: Add Virginia ABC [MEDIUM — clean legal source]

Virginia DABC publishes a full product price list as a public government spreadsheet.
No ToS issues — public records. Updated weekly. ~8K wine SKUs.

Research the download URL:
- Try: https://www.abc.virginia.gov/products/wine
- Look for a CSV, Excel, or JSON download link

Build the fetcher + loader:
1. `pipeline/fetch/virginia_abc.py` — download and parse the price list
2. `pipeline/load/virginia_abc_staging.py` — load into `source_virginia_abc`
   - Columns needed: name, producer, vintage_year, size_ml, price_usd, upc (if present)
3. Run batch_matcher on the new source
4. Run retail_promote to pull prices for matched wines

Other state ABCs worth checking if time permits:
- New Hampshire NHSLC: https://www.liquorandwineoutlets.com
- Utah DABC: https://www.abc.utah.gov (has downloadable product list)

---

## Step 3: Run Full retail_promote [after Steps 1–2]

```bash
python -m pipeline.promote.retail_promote
```

This will crash at Systembolaget REST limit (~20K calls) — that's expected and known.
Run it anyway for the partial gains. Check output for new price/UPC counts.

Then run readiness 3x and record the new price_pct average.

---

## Step 4: Wine Creation from Staging [MEDIUM — needs decision]

~3K LCBO/Systembolaget/Flatiron records have a matched canonical producer but no
canonical wine. Once created, these wines would immediately get prices from their
staging source.

Creation policy (decide and log to DECISIONS.md):
- Minimum bar: producer_id matched + wine_name exists + at least one attribute (UPC/vintage/price)
- Dedup: fuzzy match against existing wines for the producer first
- Start with LCBO only (~2K wines, best quality, all have UPCs)

---

## Acceptance Criteria

This session is successful if:
- Price readiness goes from ~1% to 5%+ (3-run average)
- wine_vintage_id backfill complete (0 NULL wine_vintage_ids where wine_id + vintage_year exist)
- Virginia ABC prices loaded and partially promoted

---

## Do NOT do this session
- Don't touch grape promotion, country inference, or validation stamps (Riddler handles)
- Don't build enrichment pipeline (separate session)
- Don't start frontend work
- Don't add score sources (decision made to skip for now)

## Wrapping up
Update CLAUDE.md price coverage numbers, DECISIONS.md if decisions made, commit, push.
