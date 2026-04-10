# Enrichment Quality Audit

**Run:** 2026-04-10T12:03:18.215557+00:00  
**Model:** claude-sonnet-4-6  
**Cost:** $1.0476

## Grade C

- Sample: 50 (50 parsed, 0 failed)
- Overall avg: **2.48/5**
- Verdicts: pass 0 / warn 23 / fail 27

### Field scores (1-5)

| Field | Specificity | Voice | Accuracy |
|-------|-------------|-------|----------|
| hook | 4.2 | 3.26 | 2.5 |
| style | 3.06 | 3.9 | 2.76 |
| comparable | 4.0 | 3.34 | 2.64 |

### Top issues flagged

- `factual_error`: 111
- `generic_filler`: 32
- `voice_drift`: 31
- `vague_hedging`: 16
- `poetic`: 14
- `sommelier_theater`: 5

## Grade B

- Sample: 20 (20 parsed, 0 failed)
- Overall avg: **2.65/5**
- Verdicts: pass 0 / warn 13 / fail 7

### Field scores (1-5)

| Field | Specificity | Voice | Accuracy |
|-------|-------------|-------|----------|
| hook | 3.85 | 3.1 | 3.0 |
| summary | 4.0 | 3.2 | 2.4 |
| style | 2.3 | 2.9 | 3.35 |
| terroir | 3.95 | 3.4 | 2.8 |
| food | 4.45 | 3.1 | 3.95 |
| cellar | 2.4 | 3.3 | 3.3 |
| comparable | 3.9 | 3.5 | 2.95 |
| vinification | 3.25 | 2.85 | 2.45 |

### Top issues flagged

- `factual_error`: 91
- `vague_hedging`: 61
- `generic_filler`: 61
- `voice_drift`: 43
- `sommelier_theater`: 29
- `poetic`: 7

## Worst-rated samples (for spot-check)

- **Grade C** Channing Daughters — Research (United States)
  - Score: 2/5 (fail)
  - The copy has real ambition and occasionally hits Loam's voice, but invented specifics (unverified ABV, unconfirmed co-fermentation, a likely non-existent comparable SKU, and a false terroir claim for New Mexico) make all three fields unfit to publish without fact-checking against label and producer source data.

- **Grade C** Castra Rubra — Via Diagonalis (Bulgaria)
  - Score: 2/5 (fail)
  - The enrichment correctly spots a real data problem but responds by producing three near-empty fields dressed in hedging language — Loam's standard is to state what is known, name the gap plainly, and still give the buyer something actionable, which none of these fields do.

- **Grade C** Gramona — Gessami (Spain)
  - Score: 2/5 (fail)
  - The enrichment has good structural bones and naming specificity but is undermined by at least two likely fabricated facts (oak aging, soil type) and one clear geographic error (Viticultors del Priorat placed in Penedès), all of which must be verified or removed before publication.

- **Grade C** des Bosquets — La Font (France)
  - Score: 2/5 (fail)
  - Multiple factual errors (altitude, soil description, Château de Selle's appellation) undermine trust across all three fields, and with grapes unconfirmed the Grenache-forward framing is asserted rather than known — needs human review and soil/blend verification before publication.

- **Grade C** King Estate — Mountain Blocks Rose of (United States)
  - Score: 2/5 (fail)
  - The entry has structural bones — price, soil, regionality — but is undermined by a demonstrably wrong comparable (Cristal d'Arques), a vague non-wine ('A Nice Rosé'), repeated imprecise soil claims, and light sommelier-theater phrasing that violates Loam voice; needs a full comparable rewrite and factual review before publish.
