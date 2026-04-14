# S4.7: Quality Gate Before Demo

## Context

Sprint 4 enriched 515 wines across 14 producers at Grade A ($36.73 total — v1 $17.72 + v2 re-enrichment $19.01). Frontend wired and deployed. Before sharing with real humans, this session addresses the data quality issues surfaced by the Q3 audit.

**Core tension:** Loam's product is "structured data rendered clearly" but right now the AI prose is rich while the structured data backing it is thin. Most wine pages show populated narrative alongside largely empty fact grids (no pH, no cases, no oak, no blend %). The gap between what the AI says and what the data can prove is the biggest credibility risk for the demo.

---

## Q3 Audit Findings (from S4.4 session)

### 1. Completeness Gaps

**Grapes — critical gaps:**
- Huet: 1/58 wines have grape links (2%). All 58 are Chenin Blanc — trivial backfill.
- CIRQ: 0/4 wines have grape links. All Pinot Noir.
- DRC: 6/14 have grapes. All are Pinot Noir or Chardonnay (known per wine).
- López de Heredia: 3/18 have grapes. All Tempranillo-based or Viura/Malvasia blends.
- Krug: 23/71 have grapes. All Chardonnay/Pinot Noir/Pinot Meunier blends.

**Prices — zero for 2 producers:**
- CIRQ: 0 prices across 4 wines.
- Fort Ross: 0 prices across 28 wines.
- Most French producers under 15% price coverage.

**Scores — zero for 9/14 producers:**
- Only Stag's Leap (9), Trimbach (8), Margaux (3), Fort Ross (3), CIRQ (2) have any scores.
- Guigal, Conterno, Krug, Huet, López de Heredia, DRC, Latour, Tempier: zero scores.

**Chemistry — almost nothing:**
- pH: zero across ALL 14 producers.
- Cases produced: zero except 2 Guigal vintages.
- Winemaker notes: zero except 21 Krug + 2 Guigal.
- New oak %: zero across all.
- ABV is the only chemistry field with meaningful coverage (Stag's Leap 163, Ridge 187, Guigal 73).

### 2. Vintage Depth

**Deep:** Latour (43.8 avg vintages/wine), Guigal (18.1), Stag's Leap (12.0), Conterno (12.5), Ridge (10.8)
**Thin:** Huet (0.7), Fort Ross (1.1), DRC (1.6), CIRQ (3.8)
**Implication:** Huet, Fort Ross, DRC, CIRQ wine pages will show mostly empty vintage comparison tables.

### 3. Dedup — 12% of demo set is duplicates

~55-60 true duplicate groups within the 515 demo wines, eliminating ~60-65 records.

**Patterns:**
- Varietal suffix dupes: "Armillary" vs "Armillary Cabernet Sauvignon" (Stag's Leap ~12 pairs, Ridge ~20+ pairs, Trimbach ~15 pairs)
- Exact name dupes: Conterno Cerretta x2, Cascina Francia x2; López de Heredia Tondonia x2; Ridge Lytton Estate PS x2; Krug Contraste x2
- Spelling variants: Cerreta/Cerretta, Petite/Petit Dynamite Hill

**Critical trap:** Ridge "York Creek Zinfandel" vs "York Creek Cabernet Sauvignon" — different wines that look like dupes. Any merge tool MUST check grape/color. ~30-35 dangerous false positives identified.

### 4. Enrichment Voice (from Q1 audit)

v2 prompt revision eliminated the worst patterns ("honest truth" openers: 99→0, "if a customer": 104→0). "Genuinely" dropped from 37%→11%. New pattern emerged: "the trap with X" opens multiple insider_takes. Individual pages read well; browsing 4+ consecutively reveals structural templates. Few-shot examples in the prompt is the likely fix (Sprint 5).

### 5. Accuracy Concerns

The enrichment model was effectively guessing for:
- Blend percentages (null for 85%+ of wines)
- pH, TA, RS (zero data — any chemistry claims from training data)
- Production volumes (zero data — claims like "fewer than 10,000 bottles" unverified)
- Oak programs (zero data — "70-80% new French oak" from LLM training)
- Winemaker names (not in DB — could be outdated)

---

## Questions for the User Before Execution

1. **Grape backfill scope:** Huet (57 wines → Chenin Blanc), DRC (8 wines → Pinot/Chard), CIRQ (4 → PN), López de Heredia (15 → Tempranillo/Viura). Should I just assign the primary grape, or try to get blend percentages from producer websites?

2. **Dedup approach:** We identified ~55 safe merge groups. Should I:
   - (a) Merge them now with a quick script (grape/color safety check), or
   - (b) Build the AI-verified dedup tool first and run that?
   Recommendation: (a) for the demo set — the patterns are well-understood, AI isn't needed for these obvious cases.

3. **Producer website scraping priority:** `producer_site_scrape.py` exists but hasn't run on demo producers. It would fill blend %, ABV, oak, cases, winemaker notes — the biggest data gaps. Should this be Sprint 5 or do we squeeze it into S4.7?

4. **Empty fact grids:** When a wine page shows "Winemaking" section with AI prose but the chemistry fact grid is empty, does that hurt credibility? Options:
   - (a) Hide empty sections entirely (show only what we have data for)
   - (b) Show "Data not yet available" placeholders
   - (c) Leave as-is — the AI prose fills the gap
   Recommendation: (a) — hide empties. Showing nothing is better than showing empty scaffolding.

5. **What to show your friends:** Given the data gaps, which producers make the strongest demo? Stag's Leap and Ridge have the most complete data (grapes, prices, vintages, ABV). Krug has vintage depth + winemaker notes. French producers are thinner. Should we focus the demo URLs on the data-rich producers?

6. **Score gap:** 9/14 producers have zero scores. This is a licensing issue. Should we accept this for the demo, or is there a quick source of community/competition scores we can import?

---

## Suggested Execution Order (if all approved)

1. Grape backfill (30 min, $0) — immediate completeness win
2. Demo set dedup merge (1 hour, $0) — eliminate the 12% dupe problem
3. Frontend: hide empty fact grids (30 min, $0) — don't show what we don't have
4. User spot-check (15 min of your time) — read 5 wines you know, flag errors
5. Re-deploy and share

## Parking Lot (Sprint 5, do not start in S4.7)
- Producer website scraping (blend %, oak, cases, notes)
- AI-verified dedup tool (web search + multi-model)
- LLM bake-off (8 models, tiered strategy)
- URL slugs
- Label images (Cloudflare R2)
- Prompt refinement (few-shot examples)
- Price data gap (SerpAPI / Wine-Searcher)
- Score licensing research
