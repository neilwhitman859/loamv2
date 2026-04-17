# Cross-model agreement matrix

Total calibration pairs: 600
Tiers with data: 4

## Pairwise verdict agreement

### L1 Haiku  vs  L1.5 Gemini basic
- Overall agreement: 463/595 = 77.8%

| L1 Haiku \ L1.5 Gemini basic | MERGE | PARENT_CHILD | SKIP | UNCERTAIN | TOTAL |
|---|---|---|---|---|---|
| **MERGE** | 105 | 6 | 9 | 0 | 120 |
| **PARENT_CHILD** | 27 | 101 | 24 | 1 | 153 |
| **SKIP** | 12 | 20 | 256 | 1 | 289 |
| **UNCERTAIN** | 17 | 4 | 11 | 1 | 33 |


**Precision/recall of AGREEMENT against gold (both methods same verdict):**

| joint verdict | N | accuracy vs gold | gold dist |
|---|---|---|---|
| both=MERGE | 70 | 70/70 = 100.0% | {'MERGE': 70} |
| both=PARENT_CHILD | 82 | 7/82 = 8.5% | {'MERGE': 5, 'SKIP': 70, 'PARENT_CHILD': 7} |
| both=SKIP | 115 | 109/115 = 94.8% | {'MERGE': 6, 'SKIP': 109} |


**When methods DISAGREE, who wins? (gold-labeled pairs only):**

- Total disagreements: 45
- L1 Haiku matches gold: 7/45 = 15.6%
- L1.5 Gemini basic matches gold: 34/45 = 75.6%
- Neither matches gold: 4/45

---

### L1 Haiku  vs  L2 Haiku rich
- Overall agreement: 535/600 = 89.2%

| L1 Haiku \ L2 Haiku rich | MERGE | PARENT_CHILD | SKIP | UNCERTAIN | TOTAL |
|---|---|---|---|---|---|
| **MERGE** | 115 | 1 | 6 | 1 | 123 |
| **PARENT_CHILD** | 8 | 122 | 20 | 3 | 153 |
| **SKIP** | 1 | 3 | 287 | 0 | 291 |
| **UNCERTAIN** | 4 | 4 | 14 | 11 | 33 |


**Precision/recall of AGREEMENT against gold (both methods same verdict):**

| joint verdict | N | accuracy vs gold | gold dist |
|---|---|---|---|
| both=MERGE | 76 | 75/76 = 98.7% | {'MERGE': 75, 'SKIP': 1} |
| both=PARENT_CHILD | 90 | 9/90 = 10.0% | {'MERGE': 13, 'SKIP': 68, 'PARENT_CHILD': 9} |
| both=SKIP | 119 | 109/119 = 91.6% | {'MERGE': 10, 'SKIP': 109} |
| both=UNCERTAIN | 1 | 0/1 = 0.0% | {'MERGE': 1} |


**When methods DISAGREE, who wins? (gold-labeled pairs only):**

- Total disagreements: 29
- L1 Haiku matches gold: 2/29 = 6.9%
- L2 Haiku rich matches gold: 20/29 = 69.0%
- Neither matches gold: 7/29

---

### L1 Haiku  vs  L2.5 Gemini rich
- Overall agreement: 509/600 = 84.8%

| L1 Haiku \ L2.5 Gemini rich | MERGE | PARENT_CHILD | SKIP | UNCERTAIN | TOTAL |
|---|---|---|---|---|---|
| **MERGE** | 116 | 2 | 5 | 0 | 123 |
| **PARENT_CHILD** | 12 | 116 | 25 | 0 | 153 |
| **SKIP** | 4 | 10 | 277 | 0 | 291 |
| **UNCERTAIN** | 15 | 4 | 14 | 0 | 33 |


**Precision/recall of AGREEMENT against gold (both methods same verdict):**

| joint verdict | N | accuracy vs gold | gold dist |
|---|---|---|---|
| both=MERGE | 73 | 73/73 = 100.0% | {'MERGE': 73} |
| both=PARENT_CHILD | 86 | 9/86 = 10.5% | {'MERGE': 12, 'SKIP': 65, 'PARENT_CHILD': 9} |
| both=SKIP | 116 | 109/116 = 94.0% | {'MERGE': 7, 'SKIP': 109} |


**When methods DISAGREE, who wins? (gold-labeled pairs only):**

- Total disagreements: 40
- L1 Haiku matches gold: 4/40 = 10.0%
- L2.5 Gemini rich matches gold: 29/40 = 72.5%
- Neither matches gold: 7/40

---

### L1.5 Gemini basic  vs  L2 Haiku rich
- Overall agreement: 489/595 = 82.2%

| L1.5 Gemini basic \ L2 Haiku rich | MERGE | PARENT_CHILD | SKIP | UNCERTAIN | TOTAL |
|---|---|---|---|---|---|
| **MERGE** | 115 | 18 | 23 | 5 | 161 |
| **PARENT_CHILD** | 6 | 98 | 23 | 4 | 131 |
| **SKIP** | 5 | 14 | 276 | 5 | 300 |
| **UNCERTAIN** | 0 | 0 | 3 | 0 | 3 |


**Precision/recall of AGREEMENT against gold (both methods same verdict):**

| joint verdict | N | accuracy vs gold | gold dist |
|---|---|---|---|
| both=MERGE | 76 | 76/76 = 100.0% | {'MERGE': 76} |
| both=PARENT_CHILD | 75 | 7/75 = 9.3% | {'MERGE': 5, 'SKIP': 63, 'PARENT_CHILD': 7} |
| both=SKIP | 124 | 117/124 = 94.4% | {'MERGE': 7, 'SKIP': 117} |


**When methods DISAGREE, who wins? (gold-labeled pairs only):**

- Total disagreements: 37
- L1.5 Gemini basic matches gold: 20/37 = 54.1%
- L2 Haiku rich matches gold: 11/37 = 29.7%
- Neither matches gold: 6/37

---

### L1.5 Gemini basic  vs  L2.5 Gemini rich
- Overall agreement: 518/595 = 87.1%

| L1.5 Gemini basic \ L2.5 Gemini rich | MERGE | PARENT_CHILD | SKIP | UNCERTAIN | TOTAL |
|---|---|---|---|---|---|
| **MERGE** | 132 | 19 | 10 | 0 | 161 |
| **PARENT_CHILD** | 8 | 101 | 22 | 0 | 131 |
| **SKIP** | 4 | 11 | 285 | 0 | 300 |
| **UNCERTAIN** | 0 | 1 | 2 | 0 | 3 |


**Precision/recall of AGREEMENT against gold (both methods same verdict):**

| joint verdict | N | accuracy vs gold | gold dist |
|---|---|---|---|
| both=MERGE | 78 | 78/78 = 100.0% | {'MERGE': 78} |
| both=PARENT_CHILD | 75 | 7/75 = 9.3% | {'MERGE': 6, 'SKIP': 62, 'PARENT_CHILD': 7} |
| both=SKIP | 128 | 119/128 = 93.0% | {'MERGE': 9, 'SKIP': 119} |


**When methods DISAGREE, who wins? (gold-labeled pairs only):**

- Total disagreements: 31
- L1.5 Gemini basic matches gold: 16/31 = 51.6%
- L2.5 Gemini rich matches gold: 14/31 = 45.2%
- Neither matches gold: 1/31

---

### L2 Haiku rich  vs  L2.5 Gemini rich
- Overall agreement: 531/600 = 88.5%

| L2 Haiku rich \ L2.5 Gemini rich | MERGE | PARENT_CHILD | SKIP | UNCERTAIN | TOTAL |
|---|---|---|---|---|---|
| **MERGE** | 121 | 5 | 2 | 0 | 128 |
| **PARENT_CHILD** | 7 | 109 | 14 | 0 | 130 |
| **SKIP** | 12 | 14 | 301 | 0 | 327 |
| **UNCERTAIN** | 7 | 4 | 4 | 0 | 15 |


**Precision/recall of AGREEMENT against gold (both methods same verdict):**

| joint verdict | N | accuracy vs gold | gold dist |
|---|---|---|---|
| both=MERGE | 78 | 77/78 = 98.7% | {'MERGE': 77, 'SKIP': 1} |
| both=PARENT_CHILD | 79 | 9/79 = 11.4% | {'MERGE': 9, 'SKIP': 61, 'PARENT_CHILD': 9} |
| both=SKIP | 127 | 119/127 = 93.7% | {'MERGE': 8, 'SKIP': 119} |


**When methods DISAGREE, who wins? (gold-labeled pairs only):**

- Total disagreements: 31
- L2 Haiku rich matches gold: 8/31 = 25.8%
- L2.5 Gemini rich matches gold: 15/31 = 48.4%
- Neither matches gold: 8/31

---

## Unanimous agreement among N tiers (on pairs with data from all tiers)

Pairs with data from all 4 tiers: 595
  of which gold-labeled: 312

All 4 tiers unanimous: 444 pairs (74.6%)
  Distribution: {'MERGE': 102, 'SKIP': 256, 'PARENT_CHILD': 86}
  Unanimous precision vs gold: 184/250 = 73.6%
