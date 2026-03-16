# Loam — Enrichment Architecture

*Established 2026-03-16. Read this before building or modifying the enrichment pipeline.*

---

## Overview

Loam uses a letter-grade enrichment model. Most wines enter the system with minimal data (identity only) and are progressively enriched based on user demand and editorial curation. This keeps costs manageable while ensuring every wine lookup delivers a useful experience.

**Core principle:** The first person to look up a wine unlocks it for everyone.

**Grade tracking:** `wines.data_grade` (F/D/C/B/A) tracks the current enrichment level per wine. `wine_insights.enrichment_tier` mirrors this for the insight layer specifically.

---

## Enrichment Grades

### Grade F — Identity Only
**What it contains:** Wine name, producer, region/appellation, country, grapes, color, wine type. No AI-generated content. No scores.

**Source:** LWIN bulk import, COLA import, retailer catalog ingestion.

**AI cost:** $0

**Coverage target:** 200,000+ wines. This is the "we've heard of this wine" layer.

**Mobile experience:** Name, producer, region, grapes, country. Below that: appellation context from `appellation_insights`, region context from `region_insights`, grape info from `grape_insights`. The page uses *generic geographic context* rather than wine-specific content — it's not empty, just not personalized to the wine.

**Identity tracking:** `wines.identity_confidence` tracks how the wine was identified (unverified, lwin_matched, cola_matched, upc_matched, manual_verified).

---

### Grade D — Basic Info
**What it contains:** Everything in F, plus some structured data from import sources — scores, prices, basic winemaking notes, grape percentages. No AI-generated content yet.

**Source:** Retailer imports, producer website scrapes, score databases.

**AI cost:** $0

**Coverage target:** 50,000+ wines. Any wine with at least one score or price.

**Mobile experience:** Name, producer, region, grapes + scores and/or prices. Generic geographic context. More useful than F because you can see how critics rate it.

---

### Grade C — Quick Enrichment
**What it contains:**
- **The hook** (2-3 sentences): The "30-second story" — what makes this wine worth knowing about. Stored in `wine_insights.ai_hook`.
- **Structured tasting profile:** Body, sweetness, acidity levels from WSAT framework. Stored in `wine_vintage_tasting_insights`.
- **Basic food pairing categories.** Stored in `wine_food_pairings`.
- **Style classification** (e.g., "full-bodied dry red"). Stored in `wine_insights`.

**Trigger:** First user lookup of a Grade F or D wine. Runs in real-time (2-3 seconds).

**AI call:** One Haiku call. Input context: wine name, producer, region, appellation, grapes, appellation_insights, region_insights, grape_insights.

**Cost:** ~$0.003-0.005 per wine.

**Coverage target:** Grows organically from user demand. Estimated 20-50K within first year.

**Mobile experience:** Everything from D, plus the hook narrative, structured flavor profile, and food pairing. Feels complete for a casual user.

---

### Grade B — Standard Enrichment
**What it contains:**
- **Full wine narrative** (1-2 paragraphs): The rich, voice-aligned story paragraph. Why this wine matters, what makes the producer distinctive, how terroir shapes the wine. Stored in `wine_insights.ai_wine_summary`.
- **Terroir description:** Synthesized from soil, elevation, aspect, climate data. Stored in `wine_insights.ai_terroir_expression`.
- **Vinification context:** How the wine is made and why those choices matter. Stored in `wine_insights.ai_vinification_summary`.
- **Value assessment:** Price in context of appellation/region peers. Stored in `wine_insights`.
- **Comparable wines:** "If you like this, try..." Stored in `wine_insights.ai_comparable_wines`.
- **Vintage-specific notes** (if vintage data exists): How this year expressed in this wine.

**Trigger:** One of:
- Wine has been looked up (demand signal — any lookup count > 0 plus data richness)
- Wine has scores AND prices in the DB (data richness signal)
- Manual enrichment for priority producers/regions

**AI call:** One Sonnet call with full context — wine data, producer data, appellation data, region data, scores, vintage weather (if available), all related reference data.

**Cost:** ~$0.02-0.04 per wine.

**Coverage target:** 5-15K wines. Major producers, commonly searched wines.

**Mobile experience:** Full wine page with story, terroir card, vintage context. The "wow" experience.

---

### Grade A — Full Enrichment
**What it contains:** Everything in B, plus:
- **Cross-vintage comparison narratives:** How this vintage compares to neighbors. Stored in `wine_vintage_insights.ai_comparison_to_normal`.
- **Detailed terroir fingerprint:** Structured soil × climate × elevation signature.
- **Producer timeline context:** Key moments in the producer's history.
- **Winemaker career context:** Who makes this wine and what else they've made.
- **Drinking window AI estimates:** When to drink, when it peaks. Stored in `wine_vintage_insights.ai_drinking_window_start/end`.
- **Wine relationship discovery:** Connections to other wines (second labels, sister wines, successors).

**Trigger:** Manual curation only. These are showcase wines.

**AI calls:** 3-5 Sonnet calls (wine narrative, per-vintage narratives, terroir synthesis, relationship discovery, producer/winemaker context).

**Cost:** ~$0.10-0.20 per wine.

**Coverage target:** 500-2,000 wines. The vertical slice — California + Burgundy first.

**Mobile experience:** The complete Loam experience. Every section populated, deep vintage history, full terroir profile.

---

## Cost Model

| Grade | Per-wine cost | Target count | Total cost |
|---|---|---|---|
| F | $0 | 200,000 | $0 |
| D | $0 | 50,000 | $0 |
| C | ~$0.004 | 30,000 | ~$120 |
| B | ~$0.03 | 10,000 | ~$300 |
| A | ~$0.15 | 1,000 | ~$150 |
| **Total** | | | **~$570** |

Budget cap: configurable daily limit on Grade C on-demand enrichment. When hit, new lookups fall back to Grade F/D experience (generic geographic context). Revisit threshold as usage grows.

---

## Score Source Trust

Publications have a `source_trust_level` (1-5):
- **5:** Authoritative critic databases (Wine Advocate, Vinous, Jancis Robinson, RVF)
- **4:** Respected, well-established (Wine Spectator, Decanter, James Suckling, Jeb Dunnuck)
- **3:** Good but niche or less rigorous (Gambero Rosso, Gault&Millau, competition results)
- **2:** Aggregated or community-driven (CellarTracker, Vivino)
- **1:** Auction houses, unverified sources

Trust level affects: score display priority on mobile (higher trust shown first), weighted composite scoring (if ever built), and enrichment prompt context (higher-trust scores given more weight in AI narratives).

---

## Wine Identity & Dedup

All wines live in the `wines` table — there is no separate candidates/staging table. Identity confidence is tracked via `wines.identity_confidence`:
- `unverified` — created from import, not cross-referenced
- `lwin_matched` — matched to LWIN database (high confidence)
- `cola_matched` — matched to TTB COLA record
- `upc_matched` — matched via barcode/UPC
- `manual_verified` — human-confirmed identity

**Dedup strategy:** LWIN as primary dedup key when available. Fuzzy matching (producer name + wine name + country, trigram similarity >0.7) to catch duplicates from different sources. UPC/EAN as secondary identifier where available.

---

## Enrichment Pipeline Architecture

### On-Demand Flow (F/D → C)
1. User looks up wine via search, barcode, or label photo
2. Wine found at Grade F or D (no `ai_hook` in `wine_insights`)
3. Frontend requests enrichment via Supabase Edge Function
4. Edge Function calls Claude Haiku with wine context + reference data
5. Response parsed, validated, written to `wine_insights` + `wine_vintage_tasting_insights` + `wine_food_pairings`
6. `enrichment_log` entry created with model, cost, fields_updated
7. `wines.data_grade` updated to 'C', `wines.lookup_count` incremented
8. Frontend receives enriched data, updates display

**Latency target:** <3 seconds end-to-end.

### Promotion Flow (C → B)
Batch job (daily or weekly cron):
1. Query wines at Grade C with data richness signals (scores, prices, multiple vintages)
2. For each candidate, assemble full context (all related data across tables)
3. Call Claude Sonnet with rich prompt
4. Parse and write to insight tables
5. Update `wines.data_grade` to 'B'
6. Log enrichment with cost tracking

### Curated Flow (→ A)
Manual or semi-automated:
1. Editor selects wines for full enrichment (vertical slice targets)
2. System assembles maximum context including weather data
3. Multiple Sonnet calls per wine
4. Results reviewed before publishing (enrichment_log `needs_review` status)
5. Update `wines.data_grade` to 'A'

---

## Frontend Data Flow

**Hybrid architecture:**
- **Reads:** Direct Supabase client queries to views/RPC functions (fast, uses CDN)
- **Writes/enrichment:** Supabase Edge Functions (server-side, calls Claude API)
- **Search:** Direct RPC calls to `search_catalog` / `search_wines`

**Default landing behavior:**
- Text search → wine page (vintage selector at top, most recent highlighted)
- Barcode scan → specific vintage page (barcodes are vintage-specific)
- Label photo → specific vintage page if vintage detected, wine page if not

**Score display:** Raw scores grouped by publication, never averaged across scales. Lead with highest source_trust_level score on mobile. Scores are facts (displayable); full tasting note text is copyrighted (don't reproduce verbatim).

---

## Enrichment Freshness

- **All grades:** Refresh once per year, or when significant new data arrives (new scores, vintage data)
- **Staleness tracking:** `enrichment_log.enriched_at` timestamp. Frontend can show "Last updated: March 2026" for transparency.

---

## Prompt Design Principles

All enrichment prompts must follow `docs/VOICE.md`. Key requirements:
- Be specific (name soils, climate patterns, geological formations)
- Connect place to taste (every fact should point toward why the wine tastes the way it does)
- State what you know directly (have a point of view)
- Be honest about what you don't know
- Give real information, not atmosphere
- Include market and value perspective
- Explain technical concepts briefly inline

Prompt templates will be versioned in `enrichment_log.prompt_template_version` for reproducibility.

---

## "Wine Not Found" Flow

When a user searches for a wine not in the database:

1. If text search: show "no results" with suggestions from fuzzy matching
2. If barcode scan: check `external_ids` for UPC/EAN match
3. If label photo: send to Claude Vision API for label reading
   - Extract: producer name, wine name, vintage year, appellation, grape, country, classification text
   - Run extracted fields through existing fuzzy resolvers (trigram search on wines, producers, appellations)
   - If match found → show wine page
   - If no match → create Grade F wine entry directly in `wines` table + immediate Grade C enrichment
   - Some user back-and-forth to confirm identification is acceptable
   - User sees result in 5-10 seconds
4. Cost per label identification: ~$0.01-0.02 (Haiku vision + Grade C enrichment)

---

## Input Methods

1. **Text search** — `search_catalog` and `search_wines` RPC functions with trigram + full-text search
2. **Barcode scan** — UPC/EAN lookup via `external_ids` table (requires barcode data from LWIN or other source)
3. **Label photo recognition** — Claude Vision API extracts label text, feeds into fuzzy search pipeline

---

## Data Dependencies by Grade

| Data needed | F | D | C | B | A |
|---|---|---|---|---|---|
| Wine identity (name, producer, grapes) | Required | Required | Required | Required | Required |
| Scores / prices | — | At least one | Context input | Context input | Context input |
| Appellation/region/grape insights | — | — | Context input | Context input | Context input |
| Weather (Open-Meteo) | — | — | — | Optional | Required |
| Terroir data (soil, elevation) | — | — | — | Input if available | Required |
| Winemaker data | — | — | — | — | Required |
| Producer timeline | — | — | — | — | Required |

---

## Image Storage

Label photos, producer logos, and map tiles stored in Supabase Storage. Bucket structure TBD.

---

## Offline & Caching

- Service worker caches last 50 viewed wines for offline access
- Search requires connectivity (no realistic way to offline-index 200K+ wines)
- Future: cache top 1,000 wines by lookup_count for offline fuzzy matching

## SEO

- `schema.org/Wine` and `schema.org/Review` structured data on wine pages
- OpenGraph tags for social sharing
- Publicly accessible pages (anonymous-first) = search engine indexable
