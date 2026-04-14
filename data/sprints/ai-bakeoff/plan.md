# Sprint 5: Polish

## Context

Sprint 4 enriched 515 wines across 14 producers at Grade A ($37.44 total). Frontend wired, deployed. But the Q3 audit exposed a core tension: **AI prose is rich while structured data is thin.** Most wine pages show populated narrative alongside empty fact grids (no pH, no cases, no oak, no blend %). The gap between what the AI says and what the data can prove is the biggest credibility risk.

Sprint 5 addresses this in three sequential phases that must run in order.

**Scale target:** Decide after model bake-off. Options range from re-doing the 515 demo wines with real data to the full 156K corpus.

---

## Why This Order

```
Dedup → Data Fill → Re-enrichment
```

1. **Dedup first** — every duplicate you scrape or enrich is wasted budget. 4,079 suspected corpus dupes, ~55 in demo set.
2. **Data fill second** — scrape producer websites to fill blend %, ABV, oak, cases, winemaker notes. Then re-enrichment grounds prose in real data instead of LLM guesses.
3. **Re-enrichment last** — the model bake-off needs the new prompt (with real structured data) to be a valid comparison. Cheaper models with real data may outperform Sonnet guessing.

---

## Phase A: Dedup (1-2 sessions)

### Goal
Eliminate duplicate wines — starting with demo set, then corpus-wide.

### Approach
Multi-stage pipeline:

1. **Rule-based grouping** — exact name match, varietal suffix match (e.g., "Armillary" vs "Armillary Cabernet Sauvignon"), spelling variants (Cerreta/Cerretta)
2. **Safety checks** — grape/color guard prevents merging "York Creek Zinfandel" into "York Creek Cabernet Sauvignon" (~30-35 dangerous false positives identified in Q3 audit)
3. **AI verification** — Haiku reviews ambiguous pairs with web search for confirmation
4. **Merge execution** — winner keeps richer data, loser's unique data migrated, loser soft-deleted

### Scope
- **Demo set first:** ~55 well-understood merge groups. Known patterns, no AI needed for most.
- **Corpus-wide:** 4,079 suspected dupes. Needs AI verification + web search for ambiguous cases.

### Budget
~$2-5 (Haiku for ambiguous cases + optional web search)

### Critical traps
- Ridge "York Creek Zinfandel" vs "York Creek Cabernet Sauvignon" — different wines
- Multi-LWIN wines are valid (Ridge Lytton Springs has 2 LWINs — confirmed S4.1)
- NV wines need special handling (same name, no year to distinguish)

---

## Phase B: Data Fill (2-3 sessions)

### Goal
Fill the structured data gaps that make fact grids empty: blend %, ABV, oak %, cases produced, winemaker name, harvest dates.

### Track B1: Producer Website Scraping

`producer_site_scrape.py` exists but hasn't run on demo producers. Run on:
1. Demo 14 producers first
2. Top 100 producers by wine count

**Data targets per wine:**
- Blend percentages (currently 0 for 85%+ of wines)
- ABV (decent coverage already)
- Oak program (new oak %, aging months, vessel type)
- Cases produced
- Harvest dates
- Winemaker name

### Track B2: Grape Backfill

420 demo wines missing grape links:
- Huet: 57 wines → all Chenin Blanc
- Krug: 93 → Chardonnay/Pinot Noir/Pinot Meunier
- Ridge: 104 → varies by wine (need name-based assignment)
- Trimbach: 54 → Riesling/Gewürz/Pinot Gris (name tells you)
- CIRQ: 4 → all Pinot Noir

Use `grape_from_name.py` for name-based assignment + manual assign for single-varietal producers.

### Track B3: Chemistry from Scrape Results

Fill pH, TA, RS, oak details from scraped tech sheets. Many producers publish tech sheets as PDFs — the scraper extracts structured data from these.

### Budget
~$5-15 (Haiku for HTML parsing, depending on number of producers)

---

## Phase C: Prompt V3 + Model Bake-off (1-2 sessions)

### Goal
Find the cheapest model that produces 80%+ quality enrichment when real data is in the prompt.

### Track C1: Prompt Refinement

1. **Inject real data** — blend %, oak, cases, pH from Phase B now available in prompt context
2. **Few-shot examples** — break structural templates ("the trap with X" pattern, "genuinely" overuse)
3. **Structural diversity** — explicitly instruct variation in opener patterns

### Track C2: Model Bake-off

Test 20 wines across multiple models:

| Model | Provider | Est. cost/wine | Notes |
|-------|----------|----------------|-------|
| Sonnet 4.6 | Anthropic | ~$0.035 | Current baseline |
| Haiku 4.5 | Anthropic | ~$0.005 | 7x cheaper |
| Gemini Flash | Google | ~$0.002 | Cheapest option |
| DeepSeek V3 | DeepSeek | ~$0.003 | Strong reasoning |
| GPT-4o-mini | OpenAI | ~$0.005 | Mainstream option |

**Evaluation criteria:**
1. Factual accuracy (does it match the structured data in the prompt?)
2. Voice quality (does it sound like Loam?)
3. Structural diversity (do 4+ consecutive wines read differently?)
4. Cost per wine

### Track C3: Scale Decision

After bake-off results:
- Pick winning model
- Calculate corpus-wide cost
- Decide scope: 515 demo only? Top 5K? Full 156K?

### Budget
~$5-10 (20 wines x 5+ models)

---

## Phase D: Scale Re-enrichment (1-3 sessions)

### Goal
Re-enrich with the winning model + new prompt + real data.

### Scope
Decided after Phase C based on:
- Model cost
- Quality delta vs Sonnet
- User feedback from demo

### Execution
- Demo set first (515 wines) as validation
- Then expand per decision
- Monitor for quality drift across large batches

### Budget
Depends on model choice:
- Sonnet: ~$18 for 515, ~$620 for 156K
- Haiku: ~$2.50 for 515, ~$90 for 156K
- Gemini Flash: ~$1 for 515, ~$35 for 156K

---

## Session Plan

| Session | Phase | Work |
|---------|-------|------|
| S5.1 | A | Demo set dedup (55 groups, rule-based + safety checks) |
| S5.2 | A | Corpus-wide dedup (AI-verified, Haiku + web search) |
| S5.3 | B | Producer website scrape (demo 14 producers) + grape backfill |
| S5.4 | B | Data fill: chemistry, oak, production from scrape results |
| S5.5 | C | Prompt V3 + model bake-off (20 wines x 5 models) |
| S5.6 | D | Scale re-enrichment with winning model |

~6 sessions, can compress if some phases go fast.

---

## Success Criteria

- [ ] Demo set dupes merged (~55 groups)
- [ ] Corpus-wide dedup pass complete (4,079 candidates reviewed)
- [ ] Producer website data scraped for demo 14 producers
- [ ] Grape backfill complete for demo set (420 wines)
- [ ] Blend %, oak, cases fields populated from scrape
- [ ] Prompt V3 written with few-shot examples + real data injection
- [ ] Model bake-off complete (5+ models, 20 wines each)
- [ ] Scale re-enrichment executed with winning model
- [ ] Fact grids showing real data (not "Data not available")
- [ ] Total budget < $50 (excluding scale enrichment)

## What This Sprint Does NOT Include

- New data sources (no new scrapers or importers)
- User accounts or authentication
- Monetization decisions
- URL slugs (human-readable URLs)
- Label image hosting (Cloudflare R2)
- Frontend redesign (beyond current "Data not available" placeholders)

## Open Questions for Phase C

1. **Should we test local models?** (Llama 3.1, Mistral) — zero marginal cost but quality unknown
2. **Is there a quality floor?** Below which no model is acceptable regardless of cost
3. **Multi-model strategy?** Could use Sonnet for top 1K wines, Haiku for the rest
