# L2 Haiku rich calibration against gold labels

Total labeled pairs: 315
Gold-label source: [('proxy', 200), ('oracle', 115)]

## Confusion matrix (rows=gold, cols=pred)

| gold \ pred | MERGE | PARENT_CHILD | SKIP | UNCERTAIN | TOTAL |
|---|---|---|---|---|---|
| **MERGE** | 82 | 13 | 14 | 1 | 110 |
| **PARENT_CHILD** | 0 | 9 | 0 | 0 | 9 |
| **SKIP** | 2 | 69 | 122 | 3 | 196 |
| **UNCERTAIN** | 0 | 0 | 0 | 0 | 0 |

**Overall accuracy**: 213/315 = 67.6%

## Per-verdict precision + recall

| verdict | N-gold | N-pred | TP | precision | recall | F1 |
|---|---|---|---|---|---|---|
| MERGE | 110 | 84 | 82 | 0.976 | 0.745 | 0.845 |
| PARENT_CHILD | 9 | 91 | 9 | 0.099 | 1.000 | 0.180 |
| SKIP | 196 | 136 | 122 | 0.897 | 0.622 | 0.735 |
| UNCERTAIN | 0 | 4 | 0 | 0.000 | 0.000 | 0.000 |

## Accuracy by predicted-verdict and confidence bucket

For each (predicted verdict × confidence bucket), how often does pred == gold?
This drives auto-accept thresholds: if MERGE@[0.95+] is 98%+ accurate, it's safe to auto-apply.

### Predicted = **MERGE**

| bucket | N | accuracy | agree vs gold |
|---|---|---|---|
| >=0.97 | 21 | 21/21 = 100.0% | {'MERGE': 21} |
| 0.92-0.97 | 49 | 49/49 = 100.0% | {'MERGE': 49} |
| 0.85-0.92 | 13 | 11/13 = 84.6% | {'MERGE': 11, 'SKIP': 2} |
| 0.75-0.85 | 1 | 1/1 = 100.0% | {'MERGE': 1} |

### Predicted = **PARENT_CHILD**

| bucket | N | accuracy | agree vs gold |
|---|---|---|---|
| 0.92-0.97 | 33 | 2/33 = 6.1% | {'MERGE': 5, 'SKIP': 26, 'PARENT_CHILD': 2} |
| 0.85-0.92 | 57 | 7/57 = 12.3% | {'MERGE': 7, 'SKIP': 43, 'PARENT_CHILD': 7} |
| 0.75-0.85 | 1 | 0/1 = 0.0% | {'MERGE': 1} |

### Predicted = **SKIP**

| bucket | N | accuracy | agree vs gold |
|---|---|---|---|
| >=0.97 | 102 | 102/102 = 100.0% | {'SKIP': 102} |
| 0.92-0.97 | 23 | 18/23 = 78.3% | {'MERGE': 5, 'SKIP': 18} |
| 0.85-0.92 | 10 | 2/10 = 20.0% | {'MERGE': 8, 'SKIP': 2} |
| <0.75 | 1 | 0/1 = 0.0% | {'MERGE': 1} |

### Predicted = **UNCERTAIN**

| bucket | N | accuracy | agree vs gold |
|---|---|---|---|
| <0.75 | 4 | 0/4 = 0.0% | {'MERGE': 1, 'SKIP': 3} |

## MERGE auto-accept threshold sweep

If we auto-accept pred=MERGE at confidence >= T, what's the precision and recall?

| threshold | N pred-MERGE | precision | recall vs gold-MERGE |
|---|---|---|---|
| >= 0.97 | 21 | 21/21 = 100.0% | 21/110 = 19.1% |
| >= 0.95 | 43 | 43/43 = 100.0% | 43/110 = 39.1% |
| >= 0.92 | 70 | 70/70 = 100.0% | 70/110 = 63.6% |
| >= 0.90 | 78 | 77/78 = 98.7% | 77/110 = 70.0% |
| >= 0.87 | 82 | 81/82 = 98.8% | 81/110 = 73.6% |
| >= 0.85 | 83 | 81/83 = 97.6% | 81/110 = 73.6% |
| >= 0.80 | 84 | 82/84 = 97.6% | 82/110 = 74.5% |

## PARENT_CHILD auto-accept threshold sweep

| threshold | N pred-PC | precision | recall vs gold-PC |
|---|---|---|---|
| >= 0.95 | 3 | 0/3 = 0.0% | 0/9 = 0.0% |
| >= 0.92 | 33 | 2/33 = 6.1% | 2/9 = 22.2% |
| >= 0.90 | 57 | 5/57 = 8.8% | 5/9 = 55.6% |
| >= 0.87 | 86 | 9/86 = 10.5% | 9/9 = 100.0% |
| >= 0.85 | 90 | 9/90 = 10.0% | 9/9 = 100.0% |
| >= 0.80 | 91 | 9/91 = 9.9% | 9/9 = 100.0% |

## SKIP auto-skip threshold sweep

Purpose: when the tier says SKIP, how often is it actually SKIP? High precision here means we can drop these pairs without further review.

| threshold | N pred-SKIP | precision | recall vs gold-SKIP |
|---|---|---|---|
| >= 0.97 | 102 | 102/102 = 100.0% | 102/196 = 52.0% |
| >= 0.95 | 113 | 111/113 = 98.2% | 111/196 = 56.6% |
| >= 0.92 | 125 | 120/125 = 96.0% | 120/196 = 61.2% |
| >= 0.90 | 129 | 121/129 = 93.8% | 121/196 = 61.7% |
| >= 0.87 | 135 | 122/135 = 90.4% | 122/196 = 62.2% |
| >= 0.85 | 135 | 122/135 = 90.4% | 122/196 = 62.2% |

## False negatives (tier SKIP but gold MERGE/PC): 14

| pair_id | tier | name_a | name_b | gold | conf | gold_src |
|---|---|---|---|---|---|---|
| 59102 | T1 | 'Trapet' | 'Trapet Pere et Fils' | MERGE | 0.96 | proxy |
| 140124 | T1 | 'Sextant' | 'Sextant' | MERGE | 0.95 | proxy |
| 138717 | T1 | 'Beaumont' | 'Beaumont' | MERGE | 0.94 | proxy |
| 63797 | T1 | 'Hospices de Beaune (Morey Blanc)' | 'Hospices de Beaune (Louis Jadot)' | MERGE | 0.92 | proxy |
| 84286 | T1 | 'Hospices de Beaune (Joseph Drouhin)' | 'Hospices de Beaune (Benjamin Leroux)' | MERGE | 0.92 | proxy |
| 63812 | T1 | 'Hospices de Beaune (Benjamin Leroux)' | 'Hospices de Beaune (Louis Jadot)' | MERGE | 0.90 | proxy |
| 121509 | T1 | 'Curtis Winery' | 'Curtis' | MERGE | 0.90 | proxy |
| 103456 | T1 | 'Hospices de Nuits' | 'Hospices de Nuits (A. Ligeret)' | MERGE | 0.90 | proxy |
| 21925 | T1 | 'Fleury' | 'Robert Fleury' | MERGE | 0.89 | proxy |
| 87733 | T1 | 'Hospices de Beaune (Coche-Dury)' | 'Hospices de Beaune (Albert Bichot)' | MERGE | 0.89 | proxy |
| 45921 | T1 | 'Henri Bonneau' | 'Bonneau' | MERGE | 0.89 | proxy |
| 28156 | T1 | 'Clavijo' | 'Monte Clavijo' | MERGE | 0.88 | proxy |
| 101267 | T1 | 'Mouton' | 'Mouton Gerard' | MERGE | 0.88 | proxy |
| 87022 | T5 | 'Bonacchi' | 'Bonnachi' | MERGE | 0.72 | oracle |

## False positives (tier MERGE but gold SKIP): 2

| pair_id | tier | name_a | name_b | conf | gold_src |
|---|---|---|---|---|---|
| 115057 | T3 | 'Kenneth-Crawford' | 'Transcendence' | 0.91 | oracle |
| 119311 | T3 | 'Drinkward Peschon' | 'Erna Schein' | 0.86 | oracle |

## Proxy-only segment (T1=MERGE, T2=SKIP): sanity check

Proxy accuracy: 175/200 = 87.5%

(If proxy accuracy is very high, proxies are reliable. If low, proxies misclassify edges — check blocking rules.)
