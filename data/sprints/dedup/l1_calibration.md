# L1 Haiku batched calibration against gold labels

Total labeled pairs: 315
Gold-label source: [('proxy', 200), ('oracle', 115)]

## Confusion matrix (rows=gold, cols=pred)

| gold \ pred | MERGE | PARENT_CHILD | SKIP | UNCERTAIN | TOTAL |
|---|---|---|---|---|---|
| **MERGE** | 76 | 20 | 11 | 3 | 110 |
| **PARENT_CHILD** | 0 | 9 | 0 | 0 | 9 |
| **SKIP** | 2 | 84 | 110 | 0 | 196 |
| **UNCERTAIN** | 0 | 0 | 0 | 0 | 0 |

**Overall accuracy**: 195/315 = 61.9%

## Per-verdict precision + recall

| verdict | N-gold | N-pred | TP | precision | recall | F1 |
|---|---|---|---|---|---|---|
| MERGE | 110 | 78 | 76 | 0.974 | 0.691 | 0.809 |
| PARENT_CHILD | 9 | 113 | 9 | 0.080 | 1.000 | 0.148 |
| SKIP | 196 | 121 | 110 | 0.909 | 0.561 | 0.694 |
| UNCERTAIN | 0 | 3 | 0 | 0.000 | 0.000 | 0.000 |

## Accuracy by predicted-verdict and confidence bucket

For each (predicted verdict × confidence bucket), how often does pred == gold?
This drives auto-accept thresholds: if MERGE@[0.95+] is 98%+ accurate, it's safe to auto-apply.

### Predicted = **MERGE**

| bucket | N | accuracy | agree vs gold |
|---|---|---|---|
| >=0.97 | 4 | 4/4 = 100.0% | {'MERGE': 4} |
| 0.92-0.97 | 34 | 34/34 = 100.0% | {'MERGE': 34} |
| 0.85-0.92 | 34 | 32/34 = 94.1% | {'MERGE': 32, 'SKIP': 2} |
| 0.75-0.85 | 6 | 6/6 = 100.0% | {'MERGE': 6} |

### Predicted = **PARENT_CHILD**

| bucket | N | accuracy | agree vs gold |
|---|---|---|---|
| 0.92-0.97 | 3 | 0/3 = 0.0% | {'MERGE': 1, 'SKIP': 2} |
| 0.85-0.92 | 102 | 9/102 = 8.8% | {'MERGE': 17, 'SKIP': 76, 'PARENT_CHILD': 9} |
| 0.75-0.85 | 8 | 0/8 = 0.0% | {'MERGE': 2, 'SKIP': 6} |

### Predicted = **SKIP**

| bucket | N | accuracy | agree vs gold |
|---|---|---|---|
| >=0.97 | 100 | 100/100 = 100.0% | {'SKIP': 100} |
| 0.92-0.97 | 13 | 9/13 = 69.2% | {'MERGE': 4, 'SKIP': 9} |
| 0.85-0.92 | 7 | 1/7 = 14.3% | {'MERGE': 6, 'SKIP': 1} |
| 0.75-0.85 | 1 | 0/1 = 0.0% | {'MERGE': 1} |

### Predicted = **UNCERTAIN**

| bucket | N | accuracy | agree vs gold |
|---|---|---|---|
| <0.75 | 3 | 0/3 = 0.0% | {'MERGE': 3} |

## MERGE auto-accept threshold sweep

If we auto-accept pred=MERGE at confidence >= T, what's the precision and recall?

| threshold | N pred-MERGE | precision | recall vs gold-MERGE |
|---|---|---|---|
| >= 0.97 | 4 | 4/4 = 100.0% | 4/110 = 3.6% |
| >= 0.95 | 18 | 18/18 = 100.0% | 18/110 = 16.4% |
| >= 0.92 | 38 | 38/38 = 100.0% | 38/110 = 34.5% |
| >= 0.90 | 45 | 45/45 = 100.0% | 45/110 = 40.9% |
| >= 0.87 | 72 | 70/72 = 97.2% | 70/110 = 63.6% |
| >= 0.85 | 72 | 70/72 = 97.2% | 70/110 = 63.6% |
| >= 0.80 | 73 | 71/73 = 97.3% | 71/110 = 64.5% |

## PARENT_CHILD auto-accept threshold sweep

| threshold | N pred-PC | precision | recall vs gold-PC |
|---|---|---|---|
| >= 0.92 | 3 | 0/3 = 0.0% | 0/9 = 0.0% |
| >= 0.90 | 21 | 2/21 = 9.5% | 2/9 = 22.2% |
| >= 0.87 | 88 | 8/88 = 9.1% | 8/9 = 88.9% |
| >= 0.85 | 105 | 9/105 = 8.6% | 9/9 = 100.0% |
| >= 0.80 | 113 | 9/113 = 8.0% | 9/9 = 100.0% |

## SKIP auto-skip threshold sweep

Purpose: when the tier says SKIP, how often is it actually SKIP? High precision here means we can drop these pairs without further review.

| threshold | N pred-SKIP | precision | recall vs gold-SKIP |
|---|---|---|---|
| >= 0.97 | 100 | 100/100 = 100.0% | 100/196 = 51.0% |
| >= 0.95 | 104 | 104/104 = 100.0% | 104/196 = 53.1% |
| >= 0.92 | 113 | 109/113 = 96.5% | 109/196 = 55.6% |
| >= 0.90 | 114 | 109/114 = 95.6% | 109/196 = 55.6% |
| >= 0.87 | 119 | 110/119 = 92.4% | 110/196 = 56.1% |
| >= 0.85 | 120 | 110/120 = 91.7% | 110/196 = 56.1% |

## False negatives (tier SKIP but gold MERGE/PC): 11

| pair_id | tier | name_a | name_b | gold | conf | gold_src |
|---|---|---|---|---|---|---|
| 140124 | T1 | 'Sextant' | 'Sextant' | MERGE | 0.94 | proxy |
| 138717 | T1 | 'Beaumont' | 'Beaumont' | MERGE | 0.93 | proxy |
| 17968 | T1 | "Anderson's Conn Valley Vineyards" | 'Conn Valley' | MERGE | 0.92 | proxy |
| 84286 | T1 | 'Hospices de Beaune (Joseph Drouhin)' | 'Hospices de Beaune (Benjamin Leroux)' | MERGE | 0.92 | proxy |
| 63797 | T1 | 'Hospices de Beaune (Morey Blanc)' | 'Hospices de Beaune (Louis Jadot)' | MERGE | 0.90 | proxy |
| 21925 | T1 | 'Fleury' | 'Robert Fleury' | MERGE | 0.88 | proxy |
| 59102 | T1 | 'Trapet' | 'Trapet Pere et Fils' | MERGE | 0.88 | proxy |
| 103456 | T1 | 'Hospices de Nuits' | 'Hospices de Nuits (A. Ligeret)' | MERGE | 0.88 | proxy |
| 45921 | T1 | 'Henri Bonneau' | 'Bonneau' | MERGE | 0.87 | proxy |
| 87733 | T1 | 'Hospices de Beaune (Coche-Dury)' | 'Hospices de Beaune (Albert Bichot)' | MERGE | 0.86 | proxy |
| 63812 | T1 | 'Hospices de Beaune (Benjamin Leroux)' | 'Hospices de Beaune (Louis Jadot)' | MERGE | 0.80 | proxy |

## False positives (tier MERGE but gold SKIP): 2

| pair_id | tier | name_a | name_b | conf | gold_src |
|---|---|---|---|---|---|
| 119311 | T3 | 'Drinkward Peschon' | 'Erna Schein' | 0.89 | oracle |
| 121624 | T3 | 'Red Tail Ridge' | 'Jean Foillard' | 0.88 | oracle |

## Proxy-only segment (T1=MERGE, T2=SKIP): sanity check

Proxy accuracy: 168/200 = 84.0%

(If proxy accuracy is very high, proxies are reliable. If low, proxies misclassify edges — check blocking rules.)
