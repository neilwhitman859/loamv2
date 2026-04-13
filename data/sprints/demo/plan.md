# Sprint 4: Demo

## Context

Sprints 1-3 built a 156K-wine database with professional-grade schema, 32 sources, backbone IDs, and full audit+fix cycle. But: 0 user lookups, 14 demo-ready wines, 96.7% Grade F. The foundation is solid; nobody has experienced the product. Sprint 4 shifts from infrastructure to **showing the product to real humans**.

**Approach:** Producer-centric. Pick producers the user owns wines from, enrich ALL their wines + the reference entities they touch, and make every page in the click chain excellent. Manual review drives voice improvement iteratively.

**ICP:** Wine enthusiast who wants a great reference tool. Mix of wine-savvy and casual drinkers.

---

## Demo Producer Set

### User's collection
- **Stag's Leap Wine Cellars** (id: `531a3920-ccab-4f14-801f-df8f1ece51f0`) — 51 wines, 26 grapes, 5 insights, 10 prices
- **Fort Ross** (id: `9dba92bb-432a-4645-b359-97c604d81e4c`) — 28 wines, 15 grapes, 0 insights, 0 prices
- **López de Heredia** — 2 duplicate entries, MERGE NEEDED:
  - `a645cffc-65ec-41dc-a603-7fa55106694c` (R. Lopez de Heredia) — 11 wines, 0 grapes, 0 insights
  - `be0b2418-e978-4c73-85f0-6d49c9940935` (López de Heredia) — 8 wines, 7 grapes, 1 insight
- **CIRQ** — 2 duplicate entries, MERGE NEEDED:
  - `8316beee-c06a-4149-bdfa-ca0fc70eb143` (CIRQ) — 4 wines, 0 grapes
  - `436fbf31-0284-422a-870e-07fab8f4b82b` (CIRQ Kosta Browne) — 1 wine
- **Ridge Vineyards** — 2 duplicate entries, MERGE NEEDED:
  - `5af1f66d-ca37-4c7a-83e7-8ea741e069f8` (Ridge Vineyards) — 61 wines, 93 grapes, 19 insights, 15 prices
  - `ba893c1e-ae84-459b-b0b4-af955bc678e8` (Ridge) — 68 wines, 0 grapes, 16 prices

### French recommendations (all wines from each producer)
- **Domaine Tempier** (id: `ceebb2c1-3121-4067-bbfa-71e9e2e02b5c`) — 19 wines, Bandol Mourvèdre master
- **E. Guigal** (id: `1271aa01-0f8e-4abb-b8fb-6ae2da03a1b6`) — 30 wines, benchmark Rhône
- **Trimbach** (id: `9691ac1f-68c7-47b8-9ae1-d84db9a7d28d`) — 75 wines, Alsace Grand Cru Riesling
- **Domaine Huet** (id: `1801910f-375e-4e6b-962f-49f558897bf9`) — 58 wines, biodynamic Vouvray Chenin Blanc

### Benchmarks (all wines from each producer)
- **DRC** (id: `9507f989-a73c-4caa-b6a4-4f596ac6a0f2`) — 14 wines
- **Krug** (id: `95560c9b-e4c1-423b-8524-0e79f8f6bb54`) — 71 wines
- **Giacomo Conterno** (id: `9859dc71-c199-45c0-a1e3-0a73b95ff3f0`) — 32 wines
- **Chateau Margaux** (id: `2c863ed3-98cb-4474-8954-6259aa2793f8`) — 4 wines
- **Chateau Latour** (id: `1ceb6035-6a13-4698-880f-156e72726ab6`) — 3 wines

### Totals (pre-merge)
~450 wines across ~15 producers. After merging Ridge (129→~100 deduped), Lopez de Heredia (19→~15 deduped), CIRQ (5→~5), likely **~400 wines**.

**Estimated cost:** ~400 wines x $0.03 = ~$12 wine enrichment + ~$5-10 reference entities = **~$20-25 total**.

---

## Enrichment Cascade Design

Enrich top-down so each layer provides context to the next:

```
Countries → Regions → Appellations → Grapes → Producers → Wines
```

When enriching a wine, the prompt includes:
- Appellation `ai_overview` + `ai_soil_profile` (terroir context)
- Grape `ai_flavor_profile` (varietal character)
- Producer `ai_winemaking_style` (house philosophy)
- Structured data (vintage weather, scores, chemistry)

This makes wine enrichment **grounded in previously enriched context** rather than relying solely on LLM training data. A wine from Stags Leap District enriched AFTER the appellation has been enriched will reference the district's specific volcanic soils, benchland geography, and microclimate.

### Reference entity scope (driven by demo wines)
| Entity | Estimated count | Pipeline exists? |
|---|---|---|
| Countries | ~5-8 (US, FR, ES, IT, AU) | Yes: `pipeline/enrich/country_insights.py` |
| Regions | ~10-15 | Yes: `pipeline/enrich/region_insights.py` |
| Appellations | ~20-30 | Yes: `pipeline/enrich/appellation_insights.py` |
| Grapes | ~15-25 | Yes: `pipeline/enrich/grape_insights.py` |
| Producers | ~15 | **NO — must build `producer_insights.py`** |

---

## Track 0: Quick Fixes (session opener, ~30 min)

1. **Wire up wine_lookups** — `useEffect` in `WinePage.tsx` that INSERTs `(wine_id, wine_vintage_id, source='web')` on mount. Critical instrumentation.
2. **Merge duplicate producers** — Ridge (2→1), López de Heredia (2→1), CIRQ (2→1). Reassign wines + related data, delete duplicates.
3. **Drop 6 temp tables** + add RLS to 3 utility tables (re-audit items)
4. **Backfill producer search_vector** (same pattern as wines)

---

## Track 1: Wine Selection + Manifest

1. Query all wines from the 15 demo producers (post-merge)
2. Build reference entity dependency graph — which appellations, regions, grapes, countries the set touches
3. Gap analysis per wine: what's present, what needs enrichment
4. Save to `data/sprints/demo/manifest.json`
5. User reviews the list

---

## Track 2: Reference Entity Enrichment (top-down cascade)

Run in order. Check what already exists before re-enriching.

1. **Countries** — likely mostly done already. Verify and fill gaps.
2. **Regions** — `pipeline/enrich/region_insights.py` on demo set's ~10-15 regions
3. **Appellations** — `pipeline/enrich/appellation_insights.py` on ~20-30 appellations. **Review for volcanic confabulation** (known S2 issue).
4. **Grapes** — `pipeline/enrich/grape_insights.py` on ~15-25 grapes
5. **Producers** — **Build `producer_insights.py`** (modeled on existing reference enrichment scripts). Two passes:
   - Pass 1: Populate structured fields (website_url, year_established, winemaker_name, philosophy, hectares) via Opus inline web research
   - Pass 2: Generate AI insights (ai_overview, ai_winemaking_style, ai_reputation, ai_value_assessment, ai_portfolio_summary)

Manual review at each layer. Fix before moving to next.

---

## Track 3: Wine Enrichment (iterative voice loop)

### Enrichment pipeline update
Modify wine enrichment prompt to include reference entity context:
- Inject appellation `ai_overview` + `ai_soil_profile`
- Inject grape `ai_flavor_profile`
- Inject producer `ai_winemaking_style`

### Voice calibration (iterative human review)
1. Enrich first 5 wines from Stag's Leap (user knows these wines personally)
2. **User reviews, provides voice feedback**
3. Tune prompt based on feedback
4. Enrich next 10, review again
5. Iterate until voice is dialed in
6. Batch the rest
7. Each review round improves the voice — the manual review is generative, not just a gate

### Execution order
1. Stag's Leap Wine Cellars (user can verify against personal knowledge)
2. Ridge, Fort Ross, CIRQ, López de Heredia (rest of user's collection)
3. French recommendations (Tempier, Guigal, Trimbach, Huet)
4. Benchmarks (DRC, Krug, Conterno, Margaux, Latour)

---

## Track 4: Frontend Polish

1. Verify full click chains: Wine → Producer → Appellation → Region → Grape → Country
2. Fix rendering issues with enriched data
3. Mobile check
4. Demo entry point — curated list or specific URLs to share
5. Ensure producer pages look good (they've always been empty shells)

---

## Track 5: Deploy + Feedback

1. Deploy to loam.onrender.com
2. Share specific wine URLs with 5+ real humans
3. Collect feedback: what's useful, what's missing, what's confusing
4. Analyze wine_lookups
5. Document learnings, plan next sprint based on real feedback

---

## Session Plan

| Session | Track | Work |
|---------|-------|------|
| S4.1 | 0 + 1 | Quick fixes, producer merges, wine selection, manifest |
| S4.2 | 2 | Reference enrichment cascade: countries → regions → appellations → grapes |
| S4.3 | 2 + 3 | Build producer enrichment pipeline + structured producer data + voice calibration (first 5 wines) |
| S4.4 | 3 | Wine enrichment: user's producers (voice review loop continues) |
| S4.5 | 3 | Wine enrichment: French recs + benchmarks |
| S4.6 | 4 + 5 | Frontend polish + deploy + share |

~6 sessions. Could compress to 4-5 if enrichment goes smoothly.

---

## Success Criteria

- [ ] ~400 wines fully enriched and reviewed
- [ ] All linked reference entities (producers, appellations, regions, grapes, countries) enriched
- [ ] Every click chain renders a populated page
- [ ] wine_lookups wired up and logging
- [ ] Duplicate producers merged (Ridge, López de Heredia, CIRQ)
- [ ] Voice iteratively improved through human review
- [ ] Producer enrichment pipeline built (reusable for scale)
- [ ] Enrichment cascade working (wine prompts include reference context)
- [ ] Shared with 5+ real humans, feedback documented
- [ ] Budget < $30

## What This Sprint Does NOT Include

- Enrichment at scale (155K wines) — future sprint, informed by demo feedback
- Voice module (automated enforcement) — manual review at this scale
- L3 fact-check gate (automated) — manual review at this scale
- Dedup of all 4,079 duplicate wines — only merge demo producer dupes
- Monetization decisions — deferred to post-demo
- Full producer metadata for all 10,683 producers — only demo set
