# Loam Dashboard

**Updated:** 2026-04-12 (S2.10)

## Where We Are

| | |
|---|---|
| **Phase** | 3 — Fix |
| **Sprint** | 3 (not yet opened — S2.10 is planning) |
| **Roadmap** | Build (done) > Audit (done) > **Fix** > Deepen > Enrich |
| **Launch target** | Friends-and-family after Fix phase |

## Sprint 3 — Fix

**Goal:** Correct bugs, unlock archive data, make pages render structured data correctly.
**Scope source:** `data/sprints/audit/findings/synthesis.md` (re-sorted below)
**Budget:** $0 / $50 ceiling ($200 extension available)
**Sessions:** 0 completed

### Track checklist (priority order)

- [ ] **Track 2 — Staging archive relink** (~1 session, $0)
  Extend `relink_staging_to_current.py` from 1 to 30 tables. Unlock 140K archive prices + 27K scores.
  Done when: price coverage > 12%, dangling staging pointers < 10K.

- [ ] **Track 3 — Grape repair** (~2 sessions, $0)
  Delete PINOT BLANC polluting synonyms, fix varietal_categories, consolidate resolvers, re-run.
  Done when: Chardonnay+Pinot Blanc false-positive count ≤ 50 (currently 2,743).

- [ ] **Track 0B — UI hygiene P0s** (~1 session, $0)
  CountryPage column typo, WinePage display_name fallback, error boundary, 404, render 8 dead ai_* fields.
  Done when: CountryPage renders ai_overview; WinePage h1 never empty; error boundary catches failures.

- [ ] **Track 1 (hygiene only)** (~0.5 session, $0)
  Delete describe-chemical edge function. Vendor enrich-wine into git. Centralize model IDs.
  Done when: describe-chemical absent from list_edge_functions; supabase/functions/enrich-wine/ in git.

- [ ] **Track 0A — Doc hygiene** (~1 session, $0)
  23 S2.8 findings. CLAUDE.md drift, archive stale docs, fix broken paths, strip hardcoded counts.
  Done when: CLAUDE.md Current Focus reads Sprint 3; docs/architecture/ and docs/pipelines/ deleted.

- [ ] **Track 4 — Producer metadata** (~2 sessions, $50-100)
  "Producers are the artists." Deep metadata: founding story, winemaker, philosophy, holdings.
  Done when: ≥ 300 producers with website + year + coordinates. Top 15 marquee producers complete.

- [ ] **Delete xwines_* tables + Riddler** (~5 min)
  Drop xwines_wines, xwines_vintages, xwines_producers, etc. Delete Riddler scheduled task.

### Deferred to Sprint 4 (Deepen)

- Track 1 items 4-7 — voice module (pipeline/lib/voice.py)
- Track 5 — signal collection (landing page, email signup, wine_lookups)
- Track 6 — AI safety rail (AIBadge, /about, /known-issues)
- Track 7 — L3 fact-check gate
- Track 8 — food pairings restoration

### Sprint 3 done criteria (simplified)

1. describe-chemical deleted, enrich-wine vendored
2. Chardonnay/Pinot Blanc ≤ 50 wines (from 2,743)
3. Price coverage > 12% (from 1.81%)
4. UI P0s shipped (CountryPage, WinePage, error boundary)
5. Doc drift fixed (CLAUDE.md, SOURCES.md, stale docs archived)
6. ≥ 300 producers with deep metadata
7. xwines_* tables and Riddler deleted
8. ENRICHMENT_ENABLED flag still OFF

## Snapshot Metrics

Queried 2026-04-12. Re-query at session start — don't trust these numbers across sessions.

| Metric | Value | After Fix target |
|---|---|---|
| Active wines | 155,623 | — |
| Producers | 10,676 | — |
| Price coverage | 1.81% (2,818 wines) | >12% |
| Score coverage | 1.30% (2,030 wines) | >6% |
| Grape links | 22.6% (35,159 wines) | 25%+ |
| Chardonnay/PB bug | 2,743 wines | ≤50 |
| Producers with metadata | 0 | ≥300 |
| Wine lookups (telemetry) | 0 | — (deferred) |
| Archive prices waiting | 139,937 | 0 (unlocked) |
| Archive scores waiting | 27,325 | 0 (unlocked) |

## Session Log

| Session | Date | Tracks | Notes |
|---|---|---|---|
| S2.10 | 2026-04-12 | planning | Dashboard + reprioritization. Sprint 3 scope re-sorted. |

## Budget

| Sprint | Ceiling | Spent | Status |
|---|---|---|---|
| Sprint 2 (Audit) | $25 | $0.00 | CLOSED |
| Sprint 3 (Fix) | $50 | $0.00 | Active |
| Extension | +$200 | — | Available on request |

## Open Questions

- Sprint 3 session sequence not yet locked — execute in track-priority order above
- After Fix phase: run Audit 2 (re-audit) before moving to Deepen? Decision: yes, iterate audit→fix
