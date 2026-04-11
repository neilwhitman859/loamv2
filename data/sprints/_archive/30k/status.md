# 30K Plan Status

**CURRENT PHASE: CLOSED**
**CLOSED: 2026-04-11 (end of Session 14 Phase B)**

## Summary

The 30K Plan ran for 14 sessions (2026-04-08 → 2026-04-11) across about 3½ calendar days. Final state:

- **155,623 active wines** (target was 30K; we 5x'd it because the LWIN long-tail sweep was so productive)
- **10,676 producers**
- **Budget spent: $23.33 / $175 (13.3%).** Unspent $151.67 carries to Sprint 2.
- **Josh Test: 222/265 (84%).** Down 1pp from the S10 peak after the S13 long-tail added 100K+ wines.
- **Data grade distribution:** B = 105 · C = 4,973 · D = 33 · F ≈ 150,512 (long-tail wines are identity-only F-grade).

## What's in the archive

Session-by-session narratives live in `data/sprints/_archive/30k/journal.md`. That file now ends with the Session 14 closure entry summarizing the final corpus state and the Sprint 2 handoff.

Key artifacts created during the sprint and still live in `public`:

- Canonical tables rebuilt from scratch, LWIN backbone promoted (189K entries)
- TTB COLA staging (3.28M records) wine-linked where possible (83K rows → 20,500 distinct wines)
- `appellation_rules` (1,165 rows) and `appellation_grapes` (10,414 rows) seeded from legal sources with full provenance
- `appellation_vintages` (134,877 rows) populated with weather data (1981-2025)
- Three-layer L1+L3 enrichment pipeline (`pipeline/enrich/build_facts_packet.py`, `enrich_prompts.py`, `fact_check_pass.py`) validated for Grade B in Session 12
- `wine_insights` populated for 5,108 wines (3.3% of corpus)
- Edge Function `enrich-wine` deployed behind `ENRICHMENT_ENABLED` feature flag (paused pending Sprint 2 architecture)

## Why the sprint ended here

The 30K target was met; the L3 fact-check approach was validated for Grade B; the backlog had shifted from "data coverage" to "enrichment architecture". Two things converged to close the sprint cleanly:

1. **Session 12's Grade C voice regression** revealed that wine-level enrichment on thin-packet wines fundamentally cannot work — the data is too sparse to describe with editorial voice. The fix isn't a better prompt, it's a different architecture.
2. **The reference layer is where the leverage is.** Enriching grapes / regions / appellations / producers once and letting wine pages be thin synthesis over that context changes the unit economics and the quality ceiling at the same time. That's Sprint 2 — Reference-First Enrichment.

## Next sprint

Sprint 2 planning kicks off in Session 15. Vertical slice selection (Sonoma Coast / California / Burgundy village), producer-layer strategy, synthesis-vs-inference boundary, and wine page minimum content contract all get resolved there. Execution starts in Session 16.

## Do NOT

- Reopen this sprint. File anything new that refers back to 30K work as a backlog entry against the current sprint, not a reopening of this one.
- Modify files under `data/sprints/_archive/30k/` except to correct factual errors in the journal.
- Re-litigate the Grade C voice regression — it is architecturally deprecated. See `docs/DECISIONS.md` 2026-04-11 "Grape percentages: label regulation minimums are not blend data" and the earlier Grade C deprecation entry for context.

## Sprint metadata

- Started: 2026-04-08
- Ended: 2026-04-11
- Sessions: 14 (all done)
- Final Josh Test: 222/265 (84%)
- Final budget: $23.33 / $175 (13.3%)
