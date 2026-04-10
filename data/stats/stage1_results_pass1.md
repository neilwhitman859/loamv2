# Stage 1 Enrichment Revalidation Results

**Run:** 2026-04-10T19:06:21.370449+00:00
**Models:** Grade B → `claude-sonnet-4-6`, Grade C → `claude-haiku-4-5-20251001`
**Auditor:** `claude-sonnet-4-6`
**Wines tested:** 34
**Cost:** $1.0030 (gen $0.6014 + audit $0.4016)

## Overall

- Original avg: 2.0/5
- Stage 1 avg:  **1.74/5**
- Delta: **-0.26**

## Grade B

- N: 7
- Avg: 2.0 → **3.0** (+1.0)
- Verdicts after: pass 1 / warn 5 / fail 1
- Fact-check: passed 0 / retried_passed 0 / partial 1 / failed 6

## Grade C

- N: 27
- Avg: 2.0 → **1.41** (-0.59)
- Verdicts after: pass 0 / warn 0 / fail 27
- Fact-check: passed 2 / retried_passed 3 / partial 5 / failed 17

## Fact-check impact

- Passed first try: 2
- Retried once and passed: 3
- Partial (low/medium flags only): 6
- Failed (high-severity persisted): 23
- Avg flags per wine: 2.06

## Per-wine deltas

| Grade | Wine | Original | Stage 1 | Δ | Status | Flags |
|-------|------|---------:|--------:|---:|--------|------:|
| B | Fess Parker — Fess Parker American Tradition | 2 | 4 | +2 | failed | 2 |
| B | Frank Family Vineyar — Frank Family Vineyards Chiles  | 2 | 3 | +1 | failed | 4 |
| B | Cuvaison — Cuvaison Durrell Chardonnay, L | 2 | 3 | +1 | failed | 3 |
| B | Landmark Vineyards — Landmark Vineyards Overlook Ch | 2 | 3 | +1 | failed | 4 |
| B | Taylor's — Taylor's Fine Ruby, Porto | 2 | 3 | +1 | partial | 5 |
| B | Henschke — Henschke Julius Riesling, Eden | 2 | 3 | +1 | failed | 5 |
| C | Gramona — Gramona Gessami, Penedès | 2 | 2 | 0 | retried_passed | 0 |
| C | des Bosquets — des Bosquets, Gigondas La Font | 2 | 2 | 0 | retried_passed | 0 |
| C | Two Hands — Two Hands Twelftree Road Tatac | 2 | 2 | 0 | partial | 2 |
| C | Quinta do Noval — Quinta do Noval Black, Porto | 2 | 2 | 0 | passed | 0 |
| C | J Vineyards — J Vineyards Eastside Knoll Pin | 2 | 2 | 0 | partial | 0 |
| C | Ochoa — Ochoa Gran Reserva, Navarra | 2 | 2 | 0 | failed | 2 |
| C | Fairview — Fairview Goats Roam The Goatfa | 2 | 2 | 0 | failed | 2 |
| C | Tyrrell's — Tyrrell's Vat 1 Semillon, Hunt | 2 | 2 | 0 | passed | 0 |
| C | Frei Brothers — Frei Brothers Merlot, Dry Cree | 2 | 2 | 0 | partial | 0 |
| C | Red Newt — Red Newt Sawmill Creek South B | 2 | 2 | 0 | failed | 2 |
| C | Fox Creek — Fox Creek JSM, McLaren Vale | 2 | 2 | 0 | failed | 2 |
| B | Frei Brothers — Frei Brothers Chardonnay, Russ | 2 | 2 | 0 | failed | 5 |
| C | Channing Daughters — Channing Daughters Research, L | 2 | 1 | -1 | failed | 2 |
| C | Castra Rubra — Castra Rubra Via Diagonalis, T | 2 | 1 | -1 | failed | 4 |
| C | King Estate — King Estate Mountain Blocks Ro | 2 | 1 | -1 | failed | 3 |
| C | Kumeu River — Kumeu River Hunting Hill Pinot | 2 | 1 | -1 | failed | 2 |
| C | Bertani — Bertani Ognisanti di Novare, V | 2 | 1 | -1 | failed | 2 |
| C | Babich — Babich Black Label Pinot Noir, | 2 | 1 | -1 | failed | 2 |
| C | Adelaida — Adelaida Version, Paso Robles | 2 | 1 | -1 | failed | 2 |
| C | Taittinger — Taittinger Comte, Champagne | 2 | 1 | -1 | retried_passed | 0 |
| C | Felton Road — Felton Road Block 2 Chardonnay | 2 | 1 | -1 | partial | 2 |
| C | Oak Farm Vineyards — Oak Farm Vineyards Mohr-Fry Ra | 2 | 1 | -1 | failed | 3 |
| C | Louis Latour — Louis Latour, Chambolle-Musign | 2 | 1 | -1 | failed | 2 |
| C | Louis Latour — Louis Latour, Romanée-Saint-Vi | 2 | 1 | -1 | partial | 0 |
| C | Bruno Clair — Bruno Clair, Chambertin Grand  | 2 | 1 | -1 | failed | 1 |
| C | Château Latour — Château Latour, Pauillac | 2 | 1 | -1 | failed | 2 |
| C | Valdivieso — Valdivieso Chardonnay | 2 | 1 | -1 | failed | 3 |
| C | Chehalem — Chehalem Three Pinot Noir, Wil | 2 | 1 | -1 | failed | 2 |