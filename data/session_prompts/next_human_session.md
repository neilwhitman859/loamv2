# Session: Close Data Gaps Before COLA UPC Arrives

Read CLAUDE.md first. Give a briefing. This is a focused work session.

## Context
COLA UPC data is arriving in a few days. That will unlock ~64K barcode-to-wine bridges and
a wave of new retail matches. Before it arrives, close the existing quality gaps so the
merge infrastructure is ready to absorb it cleanly.

Current state (post Run #6, 2026-04-04):
- 470,485 active wines | 195,468 grape links | 4,886 without country
- Readiness: 40.9/100 (3-run avg) — vintage 55%, grapes 45%, score 2%, price 1%
- Riddler hitting diminishing returns — structural root causes resolved

---

## Goals (ordered by impact)

### 1. Diagnose Price Join Gap [30 min — quick win]
30,551 prices exist in wine_vintage_prices but the COLA-matched wine readiness sample shows
0–1% price coverage. Something is wrong.

Investigate:
```sql
-- How are prices joined to wines?
SELECT COUNT(*) FROM wine_vintage_prices;
SELECT COUNT(DISTINCT wine_vintage_id) FROM wine_vintage_prices;
-- Are the wines in our readiness sample (COLA-linked) actually getting prices?
SELECT COUNT(DISTINCT wv.wine_id)
FROM wine_vintage_prices wvp
JOIN wine_vintages wv ON wv.id = wvp.wine_vintage_id
JOIN external_ids ei ON ei.entity_id = wv.wine_id AND ei.system = 'cola';
```
If the join works, the issue is that COLA-linked wines don't have matching retail prices
(Spec's/LCBO/etc. haven't been linked to these wines). Understand the gap and decide
whether to run retail_promote again or accept it.

### 2. CellarTracker Score Integration [HIGH — biggest lever for readiness]
Score readiness is stuck at 2%. CellarTracker has a free community API with ~8M ratings.
This is the single biggest data gap we can close without a licensing negotiation.

Research:
- CellarTracker API: `https://www.cellartracker.com/api.asp` — free for personal/research use
- Fields: wine name, vintage, community avg rating, # ratings
- Match path: wine name + producer → canonical wine_id via our fuzzy matcher

If API is accessible:
1. Pull top-rated wines for California + Burgundy (vertical slice priority)
2. Build `pipeline/fetch/cellartracker.py` fetcher
3. Load into `source_cellartracker` staging table
4. Promote ratings to wine_vintage_scores (publication: "CellarTracker Community")

Even 20K matched ratings would push score_pct from 2% to 10%+.

### 3. Xwines Score Promotion [MEDIUM — free, data already in DB]
530K wines in xwines_* tables include Vivino community ratings. Many match canonical wines.

```sql
SELECT COUNT(*) FROM xwines_wines WHERE average_rating IS NOT NULL AND ratings_count > 10;
```

Build a matching script: join xwines_wines to canonical wines by normalized name + producer.
For confident matches (>0.85 similarity), promote `average_rating` to wine_vintage_scores
(publication: "Vivino Community", check publication exists or create it).
This could add 50K+ scores from data we already have. Low risk — community ratings, not critic.

### 4. Wine Creation from Staging [MEDIUM — needs decision first]
~3K LCBO/Systembolaget/Flatiron records have a matched canonical producer but no matching
canonical wine. These are real wines that should exist.

Creation policy decision (make this call and log it):
- Minimum bar: must have producer_id + wine_name + at least one of (UPC, vintage, grape, price)
- Name normalization: strip producer name prefix from wine name
- Dedup check: fuzzy match against existing wines for the producer before creating
- Source tracking: set source = 'staging_retail' in metadata

If approved, build `pipeline/promote/create_missing_wines.py` and run on LCBO first
(~2K wines, best data quality of the three sources).

### 5. Grade C Batch Enrichment [$15-30 estimated — fill wine_insights]
The enrich-wine Edge Function is live but only 2 wines have been enriched. Grade C is
Haiku batch enrichment: terroir summary, food pairing, style notes.

Build `pipeline/enrich/batch_enrich_c.py`:
- Target: wines with country_id + appellation_id + at least 1 grape link (best candidates)
- Start with 5K wines as a test batch (~$15)
- Write to wine_insights table
- Update data_grade to 'C'

This makes every wine page show something useful instead of empty insights sections.

---

## What NOT to do this session
- Don't re-run Riddler phases (Riddler runs nightly)
- Don't re-run batch_matcher (exhausted for easy name matches)
- Don't run more grape promotion passes (Riddler handles this)
- Don't touch validation stamps or dedup (complete)
- Don't start frontend work until price/score gaps are diagnosed

## Wrapping up
Update CLAUDE.md current state, DECISIONS.md if decisions made, commit and push.
