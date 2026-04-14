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

## Block Plan (revised B5.1)

| Block | Work |
|-------|------|
| B5.1 | Design bake-off: model candidates, eval criteria, test sets, scoring methodology |
| B5.2 | Build test data (200 dedup pairs, 50 extraction pages, 30 prose contexts) |
| B5.3 | Run + score Task 1: dedup (17 models × 200 pairs, automated scoring) |
| B5.4 | Run + score Task 2: extraction (17 models × 50 pages, automated scoring) |
| B5.5 | Run Task 3: prose (21 models × 30 wines, save outputs) |
| B5.6 | Score Task 3: Opus inline judging (calibrate + score) |
| B5.7 | Final report: rankings, cost projections, production recommendation |

Bake-off design: `bakeoff/DESIGN.md`.
Model lists: `bakeoff_brief.md` (17/17/21 models across 3 tiers via OpenRouter).
Blocks can be combined in one session where it makes sense (e.g., B5.3+B5.4 are both
fast automated runs). B5.6 may need to split across sessions if context fills.

**Sprint 6 (planned, not scoped):** Dedup, data fill, re-enrichment, and share with
friends. Scope determined by bake-off results — model selection, cost projections,
and quality findings from B5.7 feed directly into Sprint 6 planning.

---

## Success Criteria (Sprint 5)

- [ ] Bake-off design complete with test sets, rubrics, and evaluation methodology
- [ ] Test data built: 200 labeled dedup pairs, 50 extraction pages + ground truth, 30 prose contexts
- [ ] Bake-off executed: 17+17+21 models tested via OpenRouter
- [ ] Opus inline judging complete with calibration protocol
- [ ] Report delivered: model ranking per tier with quantitative scores, cost projections, production recommendation
- [ ] Sprint 6 scope defined based on bake-off findings

## What This Sprint Does NOT Include

- New data sources (no new scrapers or importers)
- User accounts or authentication
- Monetization decisions
- URL slugs (human-readable URLs)
- Label image hosting (Cloudflare R2)
- Frontend redesign (beyond current "Data not available" placeholders)

## Open Questions

1. **Multi-model strategy?** Bake-off may show different winners per data tier (famous vs obscure wines). Design scoring to surface this.
2. **Prompt caching effectiveness?** Run Task 3 cold vs warm to measure actual cache hit rates per provider.
3. **JSON reliability?** Some models may produce good prose but inconsistent JSON. Track parse rate.
4. **Speed metric?** Tokens/sec matters for 170K-wine batch. Measure but don't weight heavily.
5. **Vintage scraping volume?** Producer portfolio pages could push Task 2 to 30K-50K calls. Bake-off tests extraction quality; volume decision follows.
