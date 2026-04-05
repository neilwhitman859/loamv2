# Next Session — Post-Recovery: Start Enrichment or Seed Knowledge

Read CLAUDE.md first, give a briefing. The last session (2026-04-05) ran 17 rounds of conservative recovery + definitional fills. Direct-source cascades from existing staging are essentially exhausted. The next phase needs a real decision.

## Where we are

- 496,926 wines, 37,184 producers, 327,513 vintages
- 188,409 wine_grapes, 266,660 wines with color, 413,413 with region_id, 262,098 with appellation_id, 67,506 with varietal_category
- 167,164 wine_vintages with label images, 170,162 with ABV
- 53,128 wine_label_designations (was 0), 20,637 wine_vintage_formats (was 4,697)
- Identity confidence: 188,908 lwin_matched + 234,308 cola_matched + 12,289 upc_matched + 61,421 unverified
- 293,692 COLA external_ids + 189,363 LWIN external_ids + 17,701 UPC + 538 producer IDs
- Score coverage 2.24% (27,451), price coverage 3.94% (33,197) — both low
- Data grade: F=467,355, D=29,568, C=0, B=3

## What's empty but schema-ready (blocked on new input, not code)

1. **`appellation_vintages`** — 0 rows. Fields ready for Open-Meteo (gdd, rainfall, harvest_avg_temp, heat_spike_days, diurnal_range, growing_season_start/end, vintage_rating, vintage_summary). Free API. Biggest missing piece for terroir storytelling per Loam's mission.

2. **`appellation_rules`** and **`appellation_grapes`** — 0 rows. Wine law facts (Barolo=100% Nebbiolo, Chablis=100% Chardonnay, Beaujolais=100% Gamay, Chianti ≥70% Sangiovese). A weekend of careful seeding would cascade varietal_category + color + wine_type to ~30-50K more wines definitionally.

3. **`wine_vintage_insights`** (3 rows), **`producer_insights`** (0), **`grape_insights`** (0) — AI enrichment output tables. Sonnet enrichment pipeline is deployed but only 3 wines enriched.

4. **Chemistry on wine_vintages**: pH 255, TA 279, RS 219 out of 327K. Needs producer tech sheet extraction or CellarTracker.

## Three real paths for next session

### Path A — Seed appellation rules (no external sources)
High-impact, zero external data, strictly definitional once seeded. A careful operator seeds 200-500 AOC/DOC/DOCG/DO/AVA rules into `appellation_rules` + `appellation_grapes`. Each rule is a fact from wine law that then cascades. Haiku can draft the seeds from public legal text; a human reviews. Highest ROI for "still conservative, no new sources."

### Path B — Wire up Open-Meteo for vintage weather
Loam's whole pitch is terroir. Weather is the single missing piece and the schema is ready. Build `pipeline/fetch/open_meteo_weather.py` that takes (appellation_id, vintage_year), computes centroid from PostGIS geometry, fetches historical weather, writes `appellation_vintages`. 3,662 appellations × ~50 years = 183K API calls, free, no rate limits that matter. Could run overnight. This is the single most impactful external integration.

### Path C — Start AI enrichment for real
Grade C/B content is where Loam becomes an actual product. The `enrich-wine` Edge Function is deployed but only 3 wines enriched. Options:
- Refine the Grade B Sonnet prompt against VOICE.md on a 20-wine sample, iterate until output is editorially acceptable
- Then batch-pre-warm Grade C with Haiku on top 5-10K wines (~$15-30)
- Voice is the blocker per DECISIONS.md: "No batch spending over $16 until voice and prompt quality are dialed in"

### Recommendation order

1. **Start with Path A (appellation rules)** — it's cheap, fully conservative, and multiplies the value of existing data. ~1 day of focused work.
2. **Then Path B (Open-Meteo)** — external but free, and the canonical "terroir" story doesn't exist without it. ~1-2 days.
3. **Then Path C (enrichment)** — needs Path A + B to have enough context for good stories.

## Do NOT do

- More blind recovery rounds. Direct-source fills are exhausted — see CLAUDE.md round log 1-17.
- Probabilistic inference on canonical columns — the whole reason for the 2026-04-04 revert. See `memory/feedback_no_probabilistic_inference.md`.
- Chase CellarTracker / Wine-Searcher / Vivino rescrape before appellation_rules and weather are done — those sources are either blocked, paid, or lower ROI.
- Touch the two known canonical bugs without a dedicated session (see DECISIONS.md):
  - 66 producers named as appellations (Margaux, Chalk Hill, etc.) — magnet wines
  - batch_matcher fuzzy-match collision (Doyard Rouge/Blanc collapsed) — affects Skurnik 5.8%, BC Liquor 16.1%

## If Path A (appellation rules)

Start with the 100 most-linked appellations by wine count. For each, research:
- `required_grape_pct` per grape (for single-varietal regulated appellations like Barolo, Brunello, Chablis)
- `allowed_grapes` set (for blends — Bordeaux, Rioja, etc.)
- `min_alcohol_pct`, `max_yield_hl_ha`, `min_aging_months` from AOC/DOCG law
- `elevation_min_m` / `elevation_max_m` where regulated (many alpine appellations have these)

Then run the cascade:
- Single-grape required → varietal_category = that grape
- Single-grape 100% → add wine_grapes entry where missing (if wine has no grape yet, derive from appellation law — this IS definitional per wine law, not inference)
- Color → from allowed grape colors where all same color

## If Path B (weather)

Test on 10 appellations first. Verify Open-Meteo historical API returns what the schema needs. One call per (appellation, year). Use ST_Centroid on the boundary geometry. Store the centroid coords + API response in a temp staging table, compute aggregates (GDD, rainfall, harvest temp, frost days, heat spikes), then promote to `appellation_vintages`. Keep the raw responses for reproducibility.

## Wrap-up behavior

Follow CLAUDE.md "wrap up" keyword: update CLAUDE.md with session results, append DECISIONS.md for judgment calls, commit at milestones, push. Don't batch-commit everything at the end.
