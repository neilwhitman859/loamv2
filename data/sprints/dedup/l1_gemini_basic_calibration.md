# L1.5 Gemini basic calibration against gold labels

Total labeled pairs: 312
Gold-label source: [('proxy', 197), ('oracle', 115)]

## Confusion matrix (rows=gold, cols=pred)

| gold \ pred | MERGE | PARENT_CHILD | SKIP | UNCERTAIN | TOTAL |
|---|---|---|---|---|---|
| **MERGE** | 89 | 9 | 9 | 0 | 107 |
| **PARENT_CHILD** | 0 | 7 | 2 | 0 | 9 |
| **SKIP** | 0 | 72 | 124 | 0 | 196 |
| **UNCERTAIN** | 0 | 0 | 0 | 0 | 0 |

**Overall accuracy**: 220/312 = 70.5%

## Per-verdict precision + recall

| verdict | N-gold | N-pred | TP | precision | recall | F1 |
|---|---|---|---|---|---|---|
| MERGE | 107 | 89 | 89 | 1.000 | 0.832 | 0.908 |
| PARENT_CHILD | 9 | 88 | 7 | 0.080 | 0.778 | 0.144 |
| SKIP | 196 | 135 | 124 | 0.919 | 0.633 | 0.749 |
| UNCERTAIN | 0 | 0 | 0 | 0.000 | 0.000 | 0.000 |

## Accuracy by predicted-verdict and confidence bucket

For each (predicted verdict × confidence bucket), how often does pred == gold?
This drives auto-accept thresholds: if MERGE@[0.95+] is 98%+ accurate, it's safe to auto-apply.

### Predicted = **MERGE**

| bucket | N | accuracy | agree vs gold |
|---|---|---|---|
| >=0.97 | 33 | 33/33 = 100.0% | {'MERGE': 33} |
| 0.92-0.97 | 46 | 46/46 = 100.0% | {'MERGE': 46} |
| 0.85-0.92 | 10 | 10/10 = 100.0% | {'MERGE': 10} |

### Predicted = **PARENT_CHILD**

| bucket | N | accuracy | agree vs gold |
|---|---|---|---|
| >=0.97 | 1 | 0/1 = 0.0% | {'SKIP': 1} |
| 0.92-0.97 | 33 | 5/33 = 15.2% | {'MERGE': 4, 'SKIP': 24, 'PARENT_CHILD': 5} |
| 0.85-0.92 | 54 | 2/54 = 3.7% | {'MERGE': 5, 'SKIP': 47, 'PARENT_CHILD': 2} |

### Predicted = **SKIP**

| bucket | N | accuracy | agree vs gold |
|---|---|---|---|
| >=0.97 | 84 | 81/84 = 96.4% | {'MERGE': 3, 'SKIP': 81} |
| 0.92-0.97 | 42 | 37/42 = 88.1% | {'MERGE': 3, 'SKIP': 37, 'PARENT_CHILD': 2} |
| 0.85-0.92 | 9 | 6/9 = 66.7% | {'MERGE': 3, 'SKIP': 6} |

### Predicted = **UNCERTAIN**

| bucket | N | accuracy | agree vs gold |
|---|---|---|---|

## MERGE auto-accept threshold sweep

If we auto-accept pred=MERGE at confidence >= T, what's the precision and recall?

| threshold | N pred-MERGE | precision | recall vs gold-MERGE |
|---|---|---|---|
| >= 0.97 | 33 | 33/33 = 100.0% | 33/107 = 30.8% |
| >= 0.95 | 63 | 63/63 = 100.0% | 63/107 = 58.9% |
| >= 0.92 | 79 | 79/79 = 100.0% | 79/107 = 73.8% |
| >= 0.90 | 82 | 82/82 = 100.0% | 82/107 = 76.6% |
| >= 0.87 | 85 | 85/85 = 100.0% | 85/107 = 79.4% |
| >= 0.85 | 89 | 89/89 = 100.0% | 89/107 = 83.2% |
| >= 0.80 | 89 | 89/89 = 100.0% | 89/107 = 83.2% |

## PARENT_CHILD auto-accept threshold sweep

| threshold | N pred-PC | precision | recall vs gold-PC |
|---|---|---|---|
| >= 0.97 | 1 | 0/1 = 0.0% | 0/9 = 0.0% |
| >= 0.95 | 11 | 3/11 = 27.3% | 3/9 = 33.3% |
| >= 0.92 | 34 | 5/34 = 14.7% | 5/9 = 55.6% |
| >= 0.90 | 75 | 7/75 = 9.3% | 7/9 = 77.8% |
| >= 0.87 | 77 | 7/77 = 9.1% | 7/9 = 77.8% |
| >= 0.85 | 88 | 7/88 = 8.0% | 7/9 = 77.8% |
| >= 0.80 | 88 | 7/88 = 8.0% | 7/9 = 77.8% |

## SKIP auto-skip threshold sweep

Purpose: when the tier says SKIP, how often is it actually SKIP? High precision here means we can drop these pairs without further review.

| threshold | N pred-SKIP | precision | recall vs gold-SKIP |
|---|---|---|---|
| >= 0.97 | 84 | 81/84 = 96.4% | 81/196 = 41.3% |
| >= 0.95 | 117 | 109/117 = 93.2% | 109/196 = 55.6% |
| >= 0.92 | 126 | 118/126 = 93.7% | 118/196 = 60.2% |
| >= 0.90 | 135 | 124/135 = 91.9% | 124/196 = 63.3% |
| >= 0.87 | 135 | 124/135 = 91.9% | 124/196 = 63.3% |
| >= 0.85 | 135 | 124/135 = 91.9% | 124/196 = 63.3% |

## False negatives (tier SKIP but gold MERGE/PC): 11

| pair_id | tier | name_a | name_b | gold | conf | gold_src |
|---|---|---|---|---|---|---|
| 121509 | T1 | 'Curtis Winery' | 'Curtis' | MERGE | 0.99 | proxy |
| 140124 | T1 | 'Sextant' | 'Sextant' | MERGE | 0.98 | proxy |
| 138717 | T1 | 'Beaumont' | 'Beaumont' | MERGE | 0.98 | proxy |
| 42711 | T1 | 'Hospices de Beaune (Albert Bichot)' | 'Hospices de Beaune (MGC)' | MERGE | 0.95 | proxy |
| 84286 | T1 | 'Hospices de Beaune (Joseph Drouhin)' | 'Hospices de Beaune (Benjamin Leroux)' | MERGE | 0.95 | proxy |
| 11951 | T1 | 'Giacosa' | 'Bruno Giacosa' | MERGE | 0.95 | proxy |
| 117161 | T3 | 'Arrowood' | 'Stonestreet' | PARENT_CHILD | 0.95 | oracle |
| 118833 | T3 | 'Foley Johnson' | 'Lancaster Estate' | PARENT_CHILD | 0.95 | oracle |
| 63812 | T1 | 'Hospices de Beaune (Benjamin Leroux)' | 'Hospices de Beaune (Louis Jadot)' | MERGE | 0.90 | proxy |
| 63797 | T1 | 'Hospices de Beaune (Morey Blanc)' | 'Hospices de Beaune (Louis Jadot)' | MERGE | 0.90 | proxy |
| 87733 | T1 | 'Hospices de Beaune (Coche-Dury)' | 'Hospices de Beaune (Albert Bichot)' | MERGE | 0.90 | proxy |

## False positives (tier MERGE but gold SKIP): 0

## Proxy-only segment (T1=MERGE, T2=SKIP): sanity check

Proxy accuracy: 180/197 = 91.4%

(If proxy accuracy is very high, proxies are reliable. If low, proxies misclassify edges — check blocking rules.)
