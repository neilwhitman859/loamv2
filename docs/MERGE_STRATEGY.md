# Loam — Merge Strategy & Pipeline Decisions

*Established 2026-03-19. Decisions from Claude.ai design session, documented for Claude Code handoff.*

---

## Language Migration: Python for Data Pipeline

**Decision:** All new data pipeline work is written in Python. Node.js scripts are retired for new development.

**Boundary:**
- **Python**: all data ingestion, staging, matching, dedup, enrichment batch jobs, local AI inference, COLA parsing, data analysis, QA scripts
- **TypeScript/Deno**: Supabase Edge Functions only (on-demand enrichment, search RPCs)
- **TypeScript/React**: frontend (Phase 4)
- **Node.js**: existing scrapers stay as-is (data already collected in JSON files and staging tables), no new Node scripts

**Rationale:** The work ahead — ETL, dedup, fuzzy matching, AI calls, data quality analysis — is Python's home turf. Pandas beats JS arrays for tabular data exploration. Local Ollama bindings, embedding-based similarity, scikit-learn, sentence-transformers are all Python-native. The merge engine (`lib/merge.mjs`) hasn't been tested yet, so building it fresh in Python costs no extra time versus debugging untested JS.

**What doesn't need porting:** Existing scrapers already ran and produced JSON + staging data. `lib/import.mjs` matters less now that architecture is staging-first. Edge Functions are TypeScript regardless.

**Supabase client:** `supabase-py` has a near-identical API to the JS client.

---

## Merge Pipeline: Overall Strategy

### Source Priority & Merge Sequence

Merge in layers. Each layer builds on resolved identities from the previous:

1. **LWIN first** — highest identity confidence, cleanest structured data (189K records). Establishes the fine wine identity backbone.
2. **COLA groups second** — universal US identity backbone, noisy but comprehensive. Group label refreshes into wine identities before matching against canonical. (~647K records with COLA across staging tables.)
3. **State databases third** — COLA ID bridge, adds ABV/vintage/appellation. (PRO Platform 346K, TABC 183K, WV 55K, Kansas 65K.) Key-based joins on COLA number — trivial SQL, no fuzzy matching needed.
4. **Importer catalogs fourth** — richest metadata (soil, vinification, farming certs, scores). Mostly enrichment of existing identities, not identity creation. Five already promoted (KL, Skurnik, Winebow, Empson, EC). More importers to add.
5. **Retailer catalogs fifth** — prices, consumer-facing scores, broad coverage. This is the layer that makes the catalog feel real.
6. **xwines as reference index** — matching confidence overlay, never an identity source. See dedicated section below.

### More Data: When It Helps and When It Hurts

More data makes matching easier up to a point, then harder. At ~890K staging rows targeting ~200K canonical wines (4:1 ratio), we're approaching the inflection.

**Where more data helps:** Obscure wines that only appear in one source with thin data. A 2,000-case Jura producer in one importer catalog benefits from a second source confirming the identity.

**Where more data hurts:** Combinatorial explosion in fuzzy matching. Conflicting field values across sources requiring AI resolution. Diminishing returns when a well-known wine already has 8 source records.

**Sweet spot:** The government data provides the identity backbone. Add 5-10 more high-quality structured sources (importers, a couple retailers, a state monopoly or two) chosen for **depth of metadata** (soil, vinification, vineyard details, farming certs), not coverage breadth. The enrichment pipeline fills remaining gaps with AI.

**Priority additional sources:** More US importers (Frederick Wildman, Vineyard Brands, Wilson Daniels, Vintus, Palm Bay, Kobrand, Maisons Marques et Domaines), distributor portfolios (Southern Glazer's regional), state monopolies (Vinmonopolet — awaiting API key — is the richest structured wine dataset globally), and one broad retailer catalog for prices.

---

## COLA: Strengths and Risks

### Why COLA Is the Starting Point (Not UPC)

- ~647K records with COLA across staging tables vs. ~27K with UPC
- COLA is the join key that chains together Kansas, PRO Platform, TABC, West Virginia — key-based joins, no fuzzy matching
- Every wine sold across US state lines has a COLA
- Grape varietal is a native structured field on TTB detail pages
- Public domain, free, regulatory authority
- UPC is secondary — optional, inconsistently applied, changes with packaging. Valuable for barcode scanning (Phase 5), not for identity building.

### Known COLA Pain Points

- **Fanciful name parsing:** Producer, wine name, vintage, appellation, grape all in one text blob. AI parsing needed for the long tail (~20-30% of records after rule-based parsing handles obvious cases).
- **Label refreshes:** Same wine, new label, new COLA number. Must group before matching. Heuristic: same brand owner + normalized fanciful name + overlapping vintages = same wine identity.
- **Expired/surrendered COLAs:** Filter before merge. Recently surrendered may still be relevant; 1987 defunct brands are not.
- **Truncated grape names:** "CABERNET SAUVI" — need prefix matching or truncation-aware grape resolver.
- **French wines missing grapes:** AOC wines don't require grape on label. Regulatory artifact, not data quality problem. Infer from appellation.
- **False merge risk:** "Cuvée Tradition" vs. "Cuvée Tradition Réserve" — high trigram similarity, different wines. When in doubt, keep separate. Easier to merge later than untangle.

---

## Wine Identity Definition

A wine is a distinct commercial product from a single producer that maintains a consistent identity across vintages.

**Same wine (different vintages):**
- Blend percentages change year to year
- Vineyard sourcing shifts within the same appellation
- Winemaking technique evolves between vintages

**Different wine (separate canonical record):**
- Different tier (Reserve vs. non-Reserve)
- Different designated vineyard
- Different product line
- Second labels (Overture is not Opus One)

**Edge cases:**
- Label redesigns → same wine, new COLA (group these)
- Wine name changes over time → same wine, use `wine_aliases` table
- NV wines → `vintage_year=0`, differentiated by other attributes

Reference this definition in matching prompts and merge logic.

---

## xwines as Reference Index for Matching Confidence

**Decision:** Use xwines data (530K wines, 32K producers, 314K grape associations, 2.2M vintages) as a matching confidence signal, not as an identity source or data source for canonical fields.

**How it works:**
- When COLA parsing produces a candidate wine identity, check against xwines for a match
- A match increases confidence that the parse was correct
- xwines data never flows into canonical columns — field values come from higher-trust sources
- `xwines_wine_candidates` (100K rows with pre-parsed producer, wine name, grapes, country, region) is the most useful artifact — a ready-made matching dictionary

**Rationale:** xwines is Vivino-sourced. Crowdsourced data = trust level 2. Coverage breadth is enormous but identity data is inconsistent. Valuable as "does this wine exist and roughly what is it?" — not as source of truth.

**Vivino by extension:** Same policy. Useful for matching confirmation and community scores (often the only scores for wines no critic has reviewed). Never an identity source. Never creates a new canonical record.

---

## AI-Assisted Matching

### Local Models for Bulk Matching

**Decision:** Use Ollama with a small local model (Llama 3.1 8B or Mistral 7B) for bulk matching to eliminate API costs.

Matching is classification ("same or different?"), not generation. Structured prompts showing both records side by side. Free versus ~$15-20 in Haiku for 50K decisions. Slower per call (2-3 seconds locally) but cost is zero.

**Hybrid approach:** Local model on everything → flag low-confidence → send only those to Haiku for second opinion.

### Model Selection by Task

| Task | Model | Rationale |
|---|---|---|
| Producer matching (bulk) | Local 8B | Handles 90%+ of cases |
| Wine matching (bulk) | Local 8B + Haiku for edge cases | Subtler distinctions need occasional escalation |
| COLA fanciful name parsing | Local 8B or Haiku | AI's highest-value role in pipeline |
| Merge conflict resolution | Local 8B or Haiku | When sources disagree on a field |
| Enrichment writing (B/A) | Sonnet only | Voice quality matters for consumer-facing prose |

### Confidence Tracking

**Decision:** Field on canonical table, not separate confidence-tier tables.

`wines.identity_confidence` (categorical: unverified, lwin_matched, cola_matched, upc_matched, manual_verified) stays as-is. Consider adding `wines.identity_match_score` (numeric) for match strength — trigram similarity, key match count, corroborating source count. Use numeric score to prioritize manual review.

Multiple tables for confidence tiers rejected: promotion logic nightmares, FK updates everywhere, query complexity.

### AI's Highest-Value Role

Not matching — **parsing and normalization before matching.** Extracting (producer, wine name, vintage, appellation, grape) from COLA fanciful name strings is harder than deciding if two parsed records match.

Pipeline: deterministic normalization → rule-based parsing (~70%) → AI for remainder (~30%). Minimizes AI calls.

---

## Immediate Next Steps

### 1. Python Migration
Build new pipeline tools in Python. Not a full port of existing scripts.
- Supabase client setup (`supabase-py`)
- String normalization helpers (port from JS)
- Merge engine (build fresh — JS version untested)
- Data exploration with pandas for QA and analysis

### 2. COLA Label Image Scrape
TTB public COLA search at `ttbonline.gov` has label images for every approved COLA. Public domain.

**Phase 1 (fast):** Scrape label image URLs, store in DB. Lightweight, quick.
**Phase 2 (background):** Batch-download images locally. Can run for days. Small files, one-time cost.

Label images serve every product direction: consumer app polish, wine list enrichment, label recognition reference library, API customers, standalone commercial value.

### 3. Retailer Catalog Merge + Initial Frontend
Merge one large retailer catalog with broad coverage and structured data. Target: 5,000-10,000 additional canonical wines with prices.

**Evaluate retailers for:** ease of scraping + data quality + coverage breadth.
- **Wine.com** (~15K wines, broad, well-structured, prices + scores)
- **K&L Wines** (~8K wines, excellent data quality, detailed notes, fine wine focus)
- **Total Wine** (largest US retailer, huge coverage, scraping may be harder at scale)

Build minimal frontend after merge: Vite/React, search box, wine detail pages. Not the full PWA — just enough to see the data and show people. This is how the product direction reveals itself.

---

## Product Direction: Build What's Universal

The product shape isn't defined yet. Could be a consumer app, data API, wine list enrichment tool, or professional reference platform. Build what's on the critical path in every scenario:

### Universal (do now)
- Canonical wine identity at scale
- Reference data layer (appellations, grapes, classifications, boundaries)
- Enrichment content generation (AI insights)
- Fuzzy match/resolve from messy input
- Prices and scores
- Label images

### Product-Specific (defer)
- Specific frontend design decisions
- Barcode scanning infrastructure
- Offline/caching strategy
- Marketing, onboarding, billing
- Weather data integration

### Revenue Potential by Direction

| Direction | Path to Revenue | Estimated ARR at Early Traction |
|---|---|---|
| Wine list enrichment (B2B) | Fastest — concrete value prop, short sales cycle | $168K (0.1% of 70K restaurants × $200/mo) |
| Data API licensing | Second — least product work needed | $200K (20 customers × $10K/yr) |
| Professional reference | Niche but loyal — somms, students, buyers | $200K (2K subscribers × $100/yr) |
| Consumer app | Highest ceiling, longest path, most competitive | $1.9M (100K free users, 2% × $8/mo) |
| Label image library | Niche — retailers, apps, e-commerce | $100K (50K images × 10 licensees × $0.20) |

**Wine list enrichment** is the most interesting near-term: restaurants upload CSV → Loam matches and enriches → returns terroir context, pairings, scores, value positioning. Uses exactly the merge/matching technology being built. Low marginal cost.

### Strategic Principle
Show the product to people early. Beverage directors, wine app developers, sommeliers. Each conversation narrows direction without premature commitment. The pipeline, enrichment, and matching engine are the foundation regardless.

---

## Data Assets Worth Recognizing

Already-built assets with independent commercial value:

- **Regulatory data assembly:** TTB COLA + 12-state PRO Platform + TABC + state databases unified. Nobody has done this. Public domain = legally clean.
- **Reference data layer:** 3,662 appellations, 9,693 grapes with synonyms/parentage, 13 classification systems, 116 label designations with 200 rules. More comprehensive than most commercial wine databases.
- **PostGIS spatial data:** Queryable polygon boundaries for appellations and regions. Enables spatial queries almost nobody in wine tech can do.
- **COLA label images (once scraped):** Public-domain library keyed to canonical identities.

---

## Claude Involvement Patterns

### Claude.ai → Design Partner
- Strategic and architectural discussions before implementation
- Product direction, schema design, pipeline approach
- Challenge assumptions, think through tradeoffs
- Produces decision documents for Claude Code handoff
- Pattern: 20 min in Claude.ai before starting new Claude Code work

### Claude Code → Implementation Partner
- Executes against decision docs
- Data exploration with pandas (distributions, overlap, matching quality)
- QA passes on merged data (sample reviews, systematic problem detection)
- Pipeline, scrapers, enrichment implementation

### Project Context Sync

Keep stable docs in Claude.ai Project knowledge base: PRINCIPLES.md, VOICE.md, ENRICHMENT.md, WORKFLOW.md (rarely change). Don't upload volatile docs (CLAUDE.md, DECISIONS.md). Share GitHub repo at session start for live state.

**Automation script** (`sync_project_context.py` — to build):
1. Pull latest docs from GitHub or local clone
2. Concatenate stable docs + CLAUDE.md current state summary
3. Output single markdown file for Project knowledge base
4. Run manually when stable docs change

---

## Security Note

GitHub personal access tokens shared in chat should be rotated regularly. Current token accepted as operational risk for now. Rotate when convenient.
