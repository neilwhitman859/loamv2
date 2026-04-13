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

**Cost:** $0. All work was Opus inline + SQL.
