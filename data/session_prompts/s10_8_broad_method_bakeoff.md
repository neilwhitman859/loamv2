# Session 10.8 - broad method bakeoff

## Goal

Search for a materially different producer-dedup method that can clear the
frozen Session 4 production-quality gate under the same `benchmark_v1`, without
DB writes and without human pair review at scale.

## Primary deliverable

A durable bakeoff readout that:

- enumerates the new method families considered
- records the metrics for every method that was actually tested
- names the strongest surviving candidate, if any
- includes the bounded-proof artifact and scorecard for that candidate
- states whether any candidate honestly clears the production gate, the
  fallback gate, or neither

## Hard constraints

- keep `benchmark_v1` frozen
- keep the Session 4 production/fallback gates frozen
- no DB writes
- no human pair review at scale
- abstention is better than a false merge
- hard `$30` incremental budget ceiling for this autonomous run

## Operating loop

1. Strategize materially different method families.
2. Pick the one that looks most likely to clear the frozen production gate.
3. Build only the bounded proof needed to test that method honestly.
4. If it passes, run stricter confirmation before trusting it.
5. If it fails, go back to step 1.

## In scope

- Sprint 6 benchmark/gate docs and prior bakeoff artifacts
- Sprint 7 identity-er artifacts and failure evidence
- new bakeoff brief / scorecards / proof outputs
- evaluation harness changes needed to test new methods
- dashboard / journal / AGENTS / DECISIONS / session bookkeeping

## Out of scope

- queue-building
- merge execution
- benchmark mutation
- gate-softening
- DB writes

## Reporting

- use 15-minute thread heartbeats
- keep cumulative spend tracked against the `$30` ceiling
- keep per-method metrics, not just prose impressions

## Stop rule

Stop when one of these becomes true:

- the 11-hour autonomous window ends
- the `$30` run budget is exhausted
- the best surviving recommendation becomes clearly frozen
