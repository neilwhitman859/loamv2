# L3 Sonnet 4.6: web vs no-web ablation

Pairs with both labels: 4

## Verdict agreement (web vs no-web)

Agreement: 4/4 = 100.0%

| web \ no-web | MERGE | PARENT_CHILD | SKIP | UNCERTAIN |
|---|---|---|---|---|
| **MERGE** | 1 | 0 | 0 | 0 |
| **SKIP** | 0 | 0 | 3 | 0 |

## Disagreements (0 pairs)

## Confidence distribution

Web:
- >=0.90: 4

No-web:
- >=0.90: 3
- 0.70-0.90: 1

## Interpretation

**Web search changed the verdict on 0/4 (0.0%) of ablation pairs.**

→ Web adds minimal signal (<5%). Consider dropping web at L3 (saves ~$0.14/pair).

Cost per pair:
- L3 with web (oracle): ~$0.147 / pair average
- L3 no-web: ~$0.012 / pair average (~92% cheaper)