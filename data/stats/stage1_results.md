# Stage 1 Enrichment Revalidation Results

**Run:** 2026-04-10T20:07:43.496238+00:00
**Models:** Grade B → `claude-sonnet-4-6`, Grade C → `claude-haiku-4-5-20251001`
**Auditor:** `claude-sonnet-4-6`
**Wines tested:** 34
**Cost:** $1.0458 (gen $0.6028 + audit $0.4430)

## Overall

- Original avg: 2.0/5
- Stage 1 avg:  **2.03/5**
- Delta: **+0.03**

## Grade B

- N: 7
- Avg: 2.0 → **3.29** (+1.29)
- Verdicts after: pass 0 / warn 7 / fail 0
- Fact-check: passed 0 / retried_passed 1 / partial 2 / failed 4

## Grade C

- N: 27
- Avg: 2.0 → **1.7** (-0.3)
- Verdicts after: pass 0 / warn 1 / fail 26
- Fact-check: passed 6 / retried_passed 2 / partial 2 / failed 17

## Fact-check impact

- Passed first try: 6
- Retried once and passed: 3
- Partial (low/medium flags only): 4
- Failed (high-severity persisted): 21
- Avg flags per wine: 1.65

## Per-wine deltas

| Grade | Wine | Original | Stage 1 | Δ | Status | Flags |
|-------|------|---------:|--------:|---:|--------|------:|
| B | Cuvaison — Cuvaison Durrell Chardonnay, L | 2 | 4 | +2 | partial | 0 |
| B | Henschke — Henschke Julius Riesling, Eden | 2 | 4 | +2 | failed | 4 |
| C | Bertani — Bertani Ognisanti di Novare, V | 2 | 3 | +1 | failed | 4 |
| B | Frank Family Vineyar — Frank Family Vineyards Chiles  | 2 | 3 | +1 | failed | 4 |
| B | Frei Brothers — Frei Brothers Chardonnay, Russ | 2 | 3 | +1 | failed | 2 |
| B | Landmark Vineyards — Landmark Vineyards Overlook Ch | 2 | 3 | +1 | partial | 0 |
| B | Taylor's — Taylor's Fine Ruby, Porto | 2 | 3 | +1 | retried_passed | 0 |
| B | Fess Parker — Fess Parker American Tradition | 2 | 3 | +1 | failed | 2 |
| C | Channing Daughters — Channing Daughters Research, L | 2 | 2 | 0 | passed | 0 |
| C | Gramona — Gramona Gessami, Penedès | 2 | 2 | 0 | passed | 0 |
| C | des Bosquets — des Bosquets, Gigondas La Font | 2 | 2 | 0 | passed | 0 |
| C | Two Hands — Two Hands Twelftree Road Tatac | 2 | 2 | 0 | retried_passed | 0 |
| C | J Vineyards — J Vineyards Eastside Knoll Pin | 2 | 2 | 0 | failed | 2 |
| C | Ochoa — Ochoa Gran Reserva, Navarra | 2 | 2 | 0 | failed | 2 |
| C | Adelaida — Adelaida Version, Paso Robles | 2 | 2 | 0 | failed | 2 |
| C | Taittinger — Taittinger Comte, Champagne | 2 | 2 | 0 | failed | 3 |
| C | Felton Road — Felton Road Block 2 Chardonnay | 2 | 2 | 0 | partial | 0 |
| C | Fairview — Fairview Goats Roam The Goatfa | 2 | 2 | 0 | failed | 1 |
| C | Louis Latour — Louis Latour, Chambolle-Musign | 2 | 2 | 0 | failed | 2 |
| C | Tyrrell's — Tyrrell's Vat 1 Semillon, Hunt | 2 | 2 | 0 | failed | 3 |
| C | Bruno Clair — Bruno Clair, Chambertin Grand  | 2 | 2 | 0 | failed | 1 |
| C | Frei Brothers — Frei Brothers Merlot, Dry Cree | 2 | 2 | 0 | passed | 0 |
| C | Red Newt — Red Newt Sawmill Creek South B | 2 | 2 | 0 | retried_passed | 0 |
| C | Valdivieso — Valdivieso Chardonnay | 2 | 2 | 0 | failed | 1 |
| C | Fox Creek — Fox Creek JSM, McLaren Vale | 2 | 2 | 0 | passed | 0 |
| C | Castra Rubra — Castra Rubra Via Diagonalis, T | 2 | 1 | -1 | failed | 4 |
| C | King Estate — King Estate Mountain Blocks Ro | 2 | 1 | -1 | failed | 4 |
| C | Kumeu River — Kumeu River Hunting Hill Pinot | 2 | 1 | -1 | failed | 4 |
| C | Babich — Babich Black Label Pinot Noir, | 2 | 1 | -1 | failed | 2 |
| C | Quinta do Noval — Quinta do Noval Black, Porto | 2 | 1 | -1 | passed | 0 |
| C | Oak Farm Vineyards — Oak Farm Vineyards Mohr-Fry Ra | 2 | 1 | -1 | failed | 5 |
| C | Louis Latour — Louis Latour, Romanée-Saint-Vi | 2 | 1 | -1 | failed | 2 |
| C | Château Latour — Château Latour, Pauillac | 2 | 1 | -1 | failed | 2 |
| C | Chehalem — Chehalem Three Pinot Noir, Wil | 2 | 1 | -1 | partial | 0 |