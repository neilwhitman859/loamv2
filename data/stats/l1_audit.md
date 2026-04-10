# L1 Enrichment Validation — Audit Results

**Run:** 2026-04-10T14:16:09.275003+00:00
**Wines tested:** 10
**Cost:** $0.2151

## Before vs After

- **Original avg:** 2.0/5
- **L1 avg:** 3.2/5
- **Avg delta:** +1.2

## Per-wine comparison

| Grade | Producer — Wine | Original | L1 | Δ |
|-------|-----------------|---------:|---:|---:|
| B | Henschke — Henschke Julius Riesling, Eden | 2 (fail) | 4 (pass) | +2 |
| B | Cuvaison — Cuvaison Durrell Chardonnay, L | 2 (fail) | 4 (warn) | +2 |
| B | Landmark Vineyards — Landmark Vineyards Overlook Ch | 2 (fail) | 4 (warn) | +2 |
| B | Frank Family Vineyards — Frank Family Vineyards Chiles  | 2 (fail) | 4 (warn) | +2 |
| B | Frei Brothers — Frei Brothers Chardonnay, Russ | 2 (fail) | 3 (warn) | +1 |
| B | Fess Parker — Fess Parker American Tradition | 2 (fail) | 3 (warn) | +1 |
| B | Taylor's — Taylor's Fine Ruby, Porto | 2 (fail) | 3 (warn) | +1 |
| C | des Bosquets — des Bosquets, Gigondas La Font | 2 (fail) | 3 (warn) | +1 |
| C | Louis Latour — Louis Latour, Romanée-Saint-Vi | 2 (fail) | 2 (fail) | 0 |
| C | Felton Road — Felton Road Block 2 Chardonnay | 2 (fail) | 2 (fail) | 0 |

## Grade B Summary

- Sample: 7
- Overall avg: **3.57/5**
- Verdicts: {'pass': 1, 'warn': 6, 'fail': 0}
- Top issues: `vague_hedging` (20), `generic_filler` (16), `factual_error` (9), `sommelier_theater` (6), `voice_drift` (5)

## Grade C Summary

- Sample: 3
- Overall avg: **2.33/5**
- Verdicts: {'pass': 0, 'warn': 1, 'fail': 2}
- Top issues: `factual_error` (4), `generic_filler` (4), `voice_drift` (3), `vague_hedging` (2)
