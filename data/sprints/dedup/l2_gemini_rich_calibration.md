# L2.5 Gemini rich calibration against gold labels

Total labeled pairs: 315
Gold-label source: [('proxy', 200), ('oracle', 115)]

## Confusion matrix (rows=gold, cols=pred)

| gold \ pred | MERGE | PARENT_CHILD | SKIP | UNCERTAIN | TOTAL |
|---|---|---|---|---|---|
| **MERGE** | 83 | 16 | 11 | 0 | 110 |
| **PARENT_CHILD** | 0 | 9 | 0 | 0 | 9 |
| **SKIP** | 1 | 67 | 128 | 0 | 196 |
| **UNCERTAIN** | 0 | 0 | 0 | 0 | 0 |

**Overall accuracy**: 220/315 = 69.8%

## Per-verdict precision + recall

| verdict | N-gold | N-pred | TP | precision | recall | F1 |
|---|---|---|---|---|---|---|
| MERGE | 110 | 84 | 83 | 0.988 | 0.755 | 0.856 |
| PARENT_CHILD | 9 | 92 | 9 | 0.098 | 1.000 | 0.178 |
| SKIP | 196 | 139 | 128 | 0.921 | 0.653 | 0.764 |
| UNCERTAIN | 0 | 0 | 0 | 0.000 | 0.000 | 0.000 |

## Accuracy by predicted-verdict and confidence bucket

For each (predicted verdict × confidence bucket), how often does pred == gold?
This drives auto-accept thresholds: if MERGE@[0.95+] is 98%+ accurate, it's safe to auto-apply.

### Predicted = **MERGE**

| bucket | N | accuracy | agree vs gold |
|---|---|---|---|
| >=0.97 | 42 | 42/42 = 100.0% | {'MERGE': 42} |
| 0.92-0.97 | 40 | 39/40 = 97.5% | {'MERGE': 39, 'SKIP': 1} |
| 0.85-0.92 | 2 | 2/2 = 100.0% | {'MERGE': 2} |

### Predicted = **PARENT_CHILD**

| bucket | N | accuracy | agree vs gold |
|---|---|---|---|
| >=0.97 | 2 | 2/2 = 100.0% | {'PARENT_CHILD': 2} |
| 0.92-0.97 | 69 | 7/69 = 10.1% | {'MERGE': 13, 'SKIP': 49, 'PARENT_CHILD': 7} |
| 0.85-0.92 | 21 | 0/21 = 0.0% | {'MERGE': 3, 'SKIP': 18} |

### Predicted = **SKIP**

| bucket | N | accuracy | agree vs gold |
|---|---|---|---|
| >=0.97 | 113 | 109/113 = 96.5% | {'MERGE': 4, 'SKIP': 109} |
| 0.92-0.97 | 19 | 13/19 = 68.4% | {'MERGE': 6, 'SKIP': 13} |
| 0.85-0.92 | 7 | 6/7 = 85.7% | {'MERGE': 1, 'SKIP': 6} |

### Predicted = **UNCERTAIN**

| bucket | N | accuracy | agree vs gold |
|---|---|---|---|

## MERGE auto-accept threshold sweep

If we auto-accept pred=MERGE at confidence >= T, what's the precision and recall?

| threshold | N pred-MERGE | precision | recall vs gold-MERGE |
|---|---|---|---|
| >= 0.97 | 42 | 42/42 = 100.0% | 42/110 = 38.2% |
| >= 0.95 | 64 | 64/64 = 100.0% | 64/110 = 58.2% |
| >= 0.92 | 82 | 81/82 = 98.8% | 81/110 = 73.6% |
| >= 0.90 | 83 | 82/83 = 98.8% | 82/110 = 74.5% |
| >= 0.87 | 84 | 83/84 = 98.8% | 83/110 = 75.5% |
| >= 0.85 | 84 | 83/84 = 98.8% | 83/110 = 75.5% |
| >= 0.80 | 84 | 83/84 = 98.8% | 83/110 = 75.5% |

## PARENT_CHILD auto-accept threshold sweep

| threshold | N pred-PC | precision | recall vs gold-PC |
|---|---|---|---|
| >= 0.97 | 2 | 2/2 = 100.0% | 2/9 = 22.2% |
| >= 0.95 | 27 | 7/27 = 25.9% | 7/9 = 77.8% |
| >= 0.92 | 71 | 9/71 = 12.7% | 9/9 = 100.0% |
| >= 0.90 | 89 | 9/89 = 10.1% | 9/9 = 100.0% |
| >= 0.87 | 90 | 9/90 = 10.0% | 9/9 = 100.0% |
| >= 0.85 | 92 | 9/92 = 9.8% | 9/9 = 100.0% |
| >= 0.80 | 92 | 9/92 = 9.8% | 9/9 = 100.0% |

## SKIP auto-skip threshold sweep

Purpose: when the tier says SKIP, how often is it actually SKIP? High precision here means we can drop these pairs without further review.

| threshold | N pred-SKIP | precision | recall vs gold-SKIP |
|---|---|---|---|
| >= 0.97 | 113 | 109/113 = 96.5% | 109/196 = 55.6% |
| >= 0.95 | 128 | 119/128 = 93.0% | 119/196 = 60.7% |
| >= 0.92 | 132 | 122/132 = 92.4% | 122/196 = 62.2% |
| >= 0.90 | 134 | 124/134 = 92.5% | 124/196 = 63.3% |
| >= 0.87 | 136 | 126/136 = 92.6% | 126/196 = 64.3% |
| >= 0.85 | 139 | 128/139 = 92.1% | 128/196 = 65.3% |

## False negatives (tier SKIP but gold MERGE/PC): 11

| pair_id | tier | name_a | name_b | gold | conf | gold_src |
|---|---|---|---|---|---|---|
| 63812 | T1 | 'Hospices de Beaune (Benjamin Leroux)' | 'Hospices de Beaune (Louis Jadot)' | MERGE | 0.98 | proxy |
| 63797 | T1 | 'Hospices de Beaune (Morey Blanc)' | 'Hospices de Beaune (Louis Jadot)' | MERGE | 0.98 | proxy |
| 84286 | T1 | 'Hospices de Beaune (Joseph Drouhin)' | 'Hospices de Beaune (Benjamin Leroux)' | MERGE | 0.98 | proxy |
| 138717 | T1 | 'Beaumont' | 'Beaumont' | MERGE | 0.98 | proxy |
| 121509 | T1 | 'Curtis Winery' | 'Curtis' | MERGE | 0.95 | proxy |
| 87733 | T1 | 'Hospices de Beaune (Coche-Dury)' | 'Hospices de Beaune (Albert Bichot)' | MERGE | 0.95 | proxy |
| 103456 | T1 | 'Hospices de Nuits' | 'Hospices de Nuits (A. Ligeret)' | MERGE | 0.95 | proxy |
| 11951 | T1 | 'Giacosa' | 'Bruno Giacosa' | MERGE | 0.95 | proxy |
| 140124 | T1 | 'Sextant' | 'Sextant' | MERGE | 0.95 | proxy |
| 74701 | T1 | 'Hospices de Beaune' | 'Hospices de Beaune (Maison Champy)' | MERGE | 0.92 | proxy |
| 42711 | T1 | 'Hospices de Beaune (Albert Bichot)' | 'Hospices de Beaune (MGC)' | MERGE | 0.85 | proxy |

## False positives (tier MERGE but gold SKIP): 1

| pair_id | tier | name_a | name_b | conf | gold_src |
|---|---|---|---|---|---|
| 115057 | T3 | 'Kenneth-Crawford' | 'Transcendence' | 0.94 | oracle |

## Proxy-only segment (T1=MERGE, T2=SKIP): sanity check

Proxy accuracy: 173/200 = 86.5%

(If proxy accuracy is very high, proxies are reliable. If low, proxies misclassify edges — check blocking rules.)
