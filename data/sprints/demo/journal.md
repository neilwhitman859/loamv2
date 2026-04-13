# Sprint 4: Demo — Journal

## S4.0 (2026-04-13)
Planning session. Producer set identified (15 producers, ~400 wines). Top-down enrichment cascade designed. 6-track plan written. $0.

## S4.1 (2026-04-13)

**Track 0: Quick fixes (all done)**
- Wired `wine_lookups` in WinePage.tsx — fire-and-forget INSERT on mount, RLS already in place
- Merged 3 duplicate producers via PL/pgSQL DO blocks:
  - Ridge (61+68→113 wines, 13 overlaps merged with full child-data consolidation)
  - López de Heredia (8+11→18, 1 overlap)
  - CIRQ (4+1→4, 1 overlap)
- Temp tables + RLS + search_vector already handled by S3.7b — verified clean

**Track 1: Wine selection manifest (done)**
- 518 wines across 14 producers (up from ~400 estimate — Ridge had more unique wines than expected)
- Reference dependencies mapped:
  - Countries: 4 (all already enriched)
  - Regions: 20 (12 enriched, 8 need)
  - Appellations: 51 (17 enriched, 34 need)
  - Grapes: 41 (0 enriched, all need)
- 127 wines missing appellation (Ridge single-vineyard designations)
- Manifest saved to `data/sprints/demo/manifest.json`
- Estimated enrichment cost: ~$22 (within $30 budget)

**Ridge internal dedup (follow-up investigation)**
- Pair 1: "Lytton" red + white → NOT a merge. Research confirmed two distinct wines:
  - Red = "Lytton Estate Petite Sirah" (renamed)
  - "White" = "Syrah Grenache Mataro" (renamed, color fixed to red — LWIN had wrong color)
- Pair 2: "Lytton Springs" x2 → MERGE. 18 identical COLAs, identical ABV on 13 overlapping vintages. LWIN had two entries ("Lytton Springs" + "Lytton Springs Zinfandel" with wrong Napa Valley geography). Kept Dry Creek Valley record, absorbed 6 scores + 3 unique vintages. Both LWINs (1135828 + 1123148) preserved on surviving wine.
- Pair 3: "Monte Bello" red + white → MERGE. Identical 24-vintage list. Both had Chardonnay grape (wrong — real wine is Cab Sauv blend). Merged white into red, fixed grapes (Chardonnay→Cab Sauv/Merlot/Petit Verdot/Cab Franc). Also merged empty "Monte Bello Cabernet Sauvignon" shell.
- Ridge 113→110 wines. Total demo: 518→515.

**Grape display names (country-based)**
- Implemented "what's on the bottle" principle: US wines show Zinfandel (not Primitivo), Petite Sirah (not Durif), Carignane (not Carignan Noir). Italian wines keep Primitivo, Nebbiolo. French wines keep Mourvèdre, Grenache.
- Mechanism: `grape_synonyms.is_primary_in_country` (already existed). Set US primary synonyms for Durif→Petite Sirah, Carignan Noir→Carignane. Primitivo→Zinfandel (US) was already set.
- Frontend: WinePage.tsx now chains a `grape_synonyms` lookup after loading grapes, overriding display_name with the country-primary synonym when available.

**Cost:** $0. All work was Opus inline + SQL + web research.
