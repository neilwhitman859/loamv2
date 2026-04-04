# Session: Prep Price Infrastructure for COLA UPC Arrival

Read CLAUDE.md first. Give a briefing. This is a focused work session.

## Context
Two things are landing in a few days:
1. **COLA UPC data** — barcode scan of 490K TTB label images, projected ~64K COLA→UPC bridges
2. **TTB UPC** — will also arrive via a separate data drop

When UPCs land on TTB records, the chain becomes:
  TTB COLA → UPC → Spec's/LCBO/BC Liquor/PA/OpenFoodFacts → price
That's ~50K+ prices currently sitting in staging that have no path to canonical wines.
This is the single biggest readiness lever remaining.

**Goal of this session: make the price infrastructure ready to absorb that data cleanly.**

Current state (post Riddler Run #6, 2026-04-04):
- 470,485 active wines | 195,468 grape links | 4,886 without country
- Readiness: 40.9/100 — vintage 55%, grapes 45%, score 2%, price 1%
- Price join bug found: 30,551 price rows exist but only 7,343 accessible via wine_vintage_id
- 23,208 prices linked via legacy wine_id + vintage_year, not wine_vintage_id

---

## Goals (ordered by impact)

### 1. Fix the Price Join Bug [HIGH — do this first, ~1 hour]

The root cause: wine_vintage_prices has a wine_vintage_id FK (the preferred join path) but
~23K rows were promoted with only wine_id + vintage_year set, wine_vintage_id left NULL.
The readiness query joins through wine_vintage_id and misses them entirely.

Same bug was fixed for scores in Riddler Run #2. Apply the same fix to prices.

Backfill script logic:
```sql
-- Find price rows missing wine_vintage_id
SELECT COUNT(*) FROM wine_vintage_prices WHERE wine_vintage_id IS NULL;

-- Backfill: match wine_id + vintage_year → wine_vintages
UPDATE wine_vintage_prices wvp
SET wine_vintage_id = wv.id
FROM wine_vintages wv
WHERE wvp.wine_vintage_id IS NULL
  AND wvp.wine_id = wv.wine_id
  AND wvp.vintage_year = wv.vintage_year;

-- For NV wines (vintage_year = 0 or NULL), match on wine_id + NV vintage
```

After backfill, re-run readiness to confirm price_pct improves.

### 2. Audit the UPC Match Path [MEDIUM — ~1 hour]

Before the barcode scan data lands, verify the pipeline is ready to use it.

Check:
- Where does the barcode scan write UPCs? (source_ttb_colas.upc? external_ids? a new table?)
- Does retail_promote.py have a UPC matching path, or does it only match by wine name?
- When a TTB record gets a UPC, does that UPC flow through to source_specs/source_lcbo/etc.?

Look at `pipeline/promote/retail_promote.py` and trace the UPC join path.
If the path doesn't exist, build it now so it's ready on day one.

The expected join when UPCs arrive:
```
source_ttb_colas.upc
  → match source_specs.upc / source_lcbo.upc / source_bc_liquor.upc / source_pa.upc
  → get price + retailer
  → promote to wine_vintage_prices
```

### 3. Add Virginia ABC Price Data [MEDIUM — clean legal source, ~1 hour]
Virginia DABC publishes a full product price list as a downloadable spreadsheet (public
government data, no ToS issues). Updated weekly. ~8K wine SKUs with prices.

Research and fetch:
- URL: https://www.abc.virginia.gov/products/wine (check for CSV/Excel download)
- Fields to capture: product name, producer, vintage, size, price, UPC if present
- Load into new `source_virginia_abc` staging table
- Run batch_matcher to link to canonical wines

Same pattern as existing state sources (PRO Platform, TABC, etc.).

### 4. Wine Creation from Staging [MEDIUM — needs decision]
~3K LCBO/Systembolaget/Flatiron records have a matched canonical producer but no canonical
wine. Once created, these wines would immediately get prices from their staging source.
This is more valuable than it sounds — these wines would also carry UPCs from LCBO/BC Liquor,
which sets up future UPC price matching.

Creation policy (decide and log to DECISIONS.md):
- Minimum bar: producer_id matched + wine_name exists + at least one attribute (UPC/vintage/price)
- Dedup: fuzzy match against existing wines for the producer first
- Start with LCBO only (~2K wines, best quality, all have UPCs)

### 5. Grade C Batch Enrichment [$15–30 — fill wine_insights while waiting]
The enrich-wine Edge Function is live but only 2 wines enriched.
Build `pipeline/enrich/batch_enrich_c.py` targeting wines with country + appellation + grape.
Start with 5K wines (~$15). Makes every searched wine page show something useful.

---

## Do NOT do this session
- Don't add CellarTracker or Vivino scores (legal issues — decision made, revisit later)
- Don't re-run Riddler phases (runs nightly, handles grape/country incrementally)
- Don't start frontend work until price join is fixed and confirmed

## Wrapping up
Update CLAUDE.md (especially price coverage numbers after backfill), DECISIONS.md if any
decisions made, commit, push.
