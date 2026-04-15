# Web-Grounding Patterns for Production Pipeline

Lessons from web-grounding 30 wines for the B5.6 bake-off judge. These patterns
should inform the production fact-checking pipeline.

---

## Source Hierarchy (best to worst)

1. **Producer tech sheets (PDF)** — gold standard. Shafer, Cakebread, Tolosa all
   have downloadable PDFs with blend, cooperage, ABV, winemaker, vineyard details.
   These are authoritative and structured. **Pattern: `{producer}.com/wp-content/uploads/` or `/assets/client/File/`.**

2. **Producer official product pages** — good for naming origins, vineyard stories,
   winemaking philosophy. Often lack tech specs. **Naming origins are almost always
   on the producer's own site, nowhere else.**

3. **Importer/distributor pages** — European Cellars, Skurnik, Kobrand, Winebow have
   solid tech sheets for their portfolio wines. Good for B-tier wines where the
   producer site is thin. **Pattern: search `"{wine name}" site:europeancellars.com`
   or check Skurnik/Winebow portfolios.**

4. **Wine-searcher.com** — aggregate data, good for verifying region/appellation/grape
   but NOT authoritative for blend details or naming origins. Good for price ranges.

5. **CellarTracker** — community data, useful for vintage availability confirmation
   but not for fact-checking. User-submitted data can be wrong.

6. **Wine.com / retail sites** — often copy-paste producer notes. Useful as a backup
   but can contain errors from stale data or wrong vintage.

## What's Easy vs Hard to Verify

### Easy (found in <2 minutes)
- **Grape variety** — almost always on the producer site
- **Region/appellation** — consistent across sources
- **ABV** — on tech sheets and most retail listings
- **Cooperage** (oak type, months) — on tech sheets
- **Winemaker name** — on producer or importer site

### Medium (2-5 minutes)
- **Blend percentages** — on tech sheets but CHANGE BY VINTAGE. Must note which vintage.
- **Vineyard sources** — single-vineyard vs multi-site is often stated but details vary
- **Producer history** (founding year, key dates) — Wikipedia or producer site

### Hard (5+ minutes or unverifiable)
- **Naming origins** — ONLY the producer knows this. Often buried in an old blog post,
  interview, or video. Models frequently fabricate plausible-sounding naming stories.
  This is the #1 fabrication vector in the bake-off.
- **Competitor pricing** — models cite specific prices for competitors (e.g., "Silver
  Oak runs $90-110"). These change frequently and are hard to verify.
- **Soil types at specific vineyard level** — general region soils are verifiable,
  but specific vineyard soil claims often require geological surveys.

## Fabrication Patterns (what models get wrong)

### 1. Naming Origins (most dangerous)
Models fabricate confident-sounding naming stories. Examples from the bake-off:
- Shafer "One Point Five": models claimed "1.5 miles from the winery", "Bay Area
  joke about drive time", "15-year commitment to the estate". Truth: "generation
  and a half" partnership between father and son.
- The Opus judge (Claude Opus 4.6) BELIEVED the wrong naming origins because they're
  in its training data. This means the fact-check pipeline CANNOT rely on LLM
  knowledge alone for naming origins.

**Production pattern:** For naming origins, always web-search the producer's own
site. If not found there, mark as "unverified" rather than using training data.

### 2. Oak/Cooperage Details
Models frequently invent specific oak regimes. Example: Grok said "20 months in
French oak" for Shafer OPF (actual: 18 months). Tyrrell's Vat 1 uses NO OAK at all
— any claim of oak aging is fabrication.

**Production pattern:** Cooperage is a hard-verify field. If not in prompt data,
web-search for the official tech sheet. If not found, leave as null.

### 3. Competitor Price Comparisons
Models cite specific prices for competitor wines ("Silver Oak runs $90-110",
"Caymus Special Selection at $150"). These are plausible but unsourced and change
with vintages/markets.

**Production pattern:** For value_assessment, only cite prices that are in the
prompt data or verified via wine-searcher. Don't cite competitor prices from
training data.

### 4. Vineyard Designation Errors
Models call multi-site wines "single-vineyard" (MIMO v2 Flash called Shafer OPF
"single-vineyard" when it sources from 3+ sites).

**Production pattern:** Verify single-vineyard claims against tech sheets. This
is a high-stakes error because it's provably wrong from publicly available data.

## Production Pipeline Recommendations

### Phase 1: Structured Verification (automated)
For each wine being enriched:
1. Check if we have a tech sheet URL in the DB (add `tech_sheet_url` column?)
2. If yes, fetch and extract key facts (blend, cooperage, ABV, winemaker)
3. Include extracted facts in the enrichment prompt as "verified data"
4. Post-enrichment: diff model output against verified data, flag mismatches

### Phase 2: Naming Origin Verification (semi-automated)
1. Web-search `"{wine name}" "{producer}" name origin story meaning`
2. Prioritize results from the producer's own domain
3. If found, include in prompt as verified context
4. If not found, instruct the model: "Do not invent a naming origin"

### Phase 3: Claim Extraction + Verification (future)
1. After model generates prose, extract all factual claims
2. Classify each as: from-prompt-data / from-training / unverifiable
3. Web-verify training-data claims (especially naming origins, competitor prices)
4. Flag or remove unverified claims before publishing

## Cost/Effort Estimates

- **Tech sheet lookup:** ~30 seconds per wine if URL is in DB, ~2 minutes if web search needed
- **Naming origin verification:** ~3-5 minutes per wine (often requires reading producer blog posts)
- **Full fact-check:** ~10 minutes per wine for a human reviewer

At scale (170K wines): automated tech-sheet extraction is feasible. Manual naming
origin verification is only feasible for top-tier wines (~500-1000). For the rest,
the instruction "do not invent naming origins" in the prompt is the best defense.

---

*Created 2026-04-15 during B5.6 bake-off judge calibration.*
