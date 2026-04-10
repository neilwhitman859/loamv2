=================================================================
  LOAM PRODUCT ROADMAP            2026-04-10 07:16:30
=================================================================

  OVERVIEW
  _____________________________________________________________
  Phases: 2 done  1 active  6 pending
  Catalog: 51,790 wines, 2,529 producers
  Enriched: 4,962 wines (9.6%)
  Josh Test: 85% findability
  Budget: $15.76 / $175 (9.0%)

  >>> CURRENTLY IN PHASE 3: Enrichment Pipeline

  [x] PHASE 1: Foundation  [DONE]  2026-03-17
      Schema, reference data, source research
      • 94 canonical tables
      • 3,662 appellations, 9,695 grapes, 389 regions
      • 17 source categories researched (docs/SOURCES.md)
      All reference tables seeded. Schema hardened across 3 rounds.

  [x] PHASE 2: Data Population (30K Plan)  [DONE]  2026-04-09
      Quality-over-coverage catalog rebuild
      • 2,529 producers, 51,790 wines
      • Josh Test: 85% findability
      • Completeness median: 8/11
      Sessions 1-8 of the 30K plan. Archive, identity rules, batch imports, TTB linking, quality gate.

  [>] PHASE 3: Enrichment Pipeline  [IN PROGRESS]
      AI narrative content for wine pages
      [x] Pipeline infrastructure (sequential + batch)  — batch_enrich.py, batch_api.py, batch_runner.py
      [x] Voice calibration  — 7 wines manual review, docs/VOICE.md aligned
      [x] Session 9: D-grade sweep  — 4,962 wines enriched, $15.76
      [!] Session 10: Final validation  — Josh Test, WineTest Story, S11 checks
      [ ] Grade C on F-grade wines  — ~46,688 wines, ~$93 est.
      [ ] Grade A showcase curation  — Top ~500 wines, manual review
      On-demand Edge Function stays for user-triggered enrichment. Batch runner handles scheduled refreshes.

  [ ] PHASE 4: Frontend Resume  [pending]
      Ship the consumer experience (loam.onrender.com)
      [ ] Verify API views against 30K tables
      [ ] Wire up Edge Function for on-demand enrichment
      [ ] Loading states for Grade F/D wines
      [ ] Test wine pages with real enrichment data
      [ ] Deploy + onboard first beta users
      Pages already built (Wine/Producer/Appellation/Region/Grape/Country/Vineyard). Paused since 2026-04-01. Dependencies resolved by Phase 3.

  [ ] PHASE 5: Input Methods  [pending]
      Barcode scan + label photo + wine-not-found
      [ ] Barcode scanner â†’ UPC lookup  — 106K UPCs already indexed
      [ ] Label photo â†’ Claude Vision â†’ fuzzy match
      [ ] Wine-not-found: create + enrich in 15s
      Data layer already supports this. Needs frontend + Vision API integration.

  [ ] PHASE 6: Data Expansion  [pending]
      Fill coverage gaps, add richer sources
      [ ] TTB COLA Phase 3 AI parse (1.35M non-001)  — ~$10 Haiku
      [ ] Score coverage push (Wine Spectator/Parker)  — Licensing TBD
      [ ] Southern Hemisphere importers (AU/NZ/AR/CL/ZA)
      [ ] CellarTracker integration
      [>] Weather data drip upgrade  — Scheduled Open-Meteo job running
      Not blocking frontend launch. Ongoing background work.

  [ ] PHASE 7: Quality & Maintenance  [pending]
      Automated drift detection + refresh cycles
      [ ] Weekly batch_runner cron (new wines)
      [ ] Monthly batch_runner cron (retry failed)
      [ ] Annual batch_runner cron (refresh stale)
      [ ] Voice drift monitor
      [-] Data accuracy agent re-enable  — Built, currently disabled
      Set up after Phase 3 completes. Runs unattended.

  [ ] PHASE 8: Product Layer  [pending]
      User features beyond search
      [ ] User accounts (optional)
      [ ] Saved wines / tasting notes
      [ ] Search filters (grape/region/price/style)
      [ ] Wine recommendations
      [ ] Cellar tracking
      Loam can stay anonymous-first at launch. Add accounts when users ask.

  [ ] PHASE 9: Launch  [pending]
      Public launch, SEO, first 1,000 users
      [ ] SEO (schema.org + OpenGraph)  — Already designed in
      [ ] Public launch
      [ ] First 1,000 users
      Target after Phase 4 completes with enough enrichment coverage.

  =============================================================
  Data: data/stats/loam_roadmap.json
  Script: pipeline/analyze/loam_roadmap.py
  =============================================================