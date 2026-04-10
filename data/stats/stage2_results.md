# Stage 2 Enrichment Population Validation

**Run:** 2026-04-10T21:08:45.497411+00:00
**Models:** Grade B → `claude-sonnet-4-6`, Grade C → `claude-haiku-4-5-20251001`
**Auditor:** `claude-sonnet-4-6`
**Wines tested:** 60 (target 30B + 30C)
**Cost:** $2.7172 (gen $1.7159 + audit $1.0013)

## Results vs S11 baselines

| Grade | N | S11 baseline | Stage 2 avg | Δ | pass/warn/fail |
|---|---:|---:|---:|---:|---:|
| B | 29 | 2.65 | **3.31** | +0.66 | 3/26/0 |
| C | 29 | 2.48 | **1.76** | -0.72 | 0/1/28 |

## Fact-check status

| Grade | passed | retried_passed | partial | failed |
|---|---:|---:|---:|---:|
| B | 2 | 0 | 2 | 25 |
| C | 5 | 3 | 5 | 16 |

## Per-wine results

| Grade | Producer — Wine | Score | Verdict | Status | Flags |
|---|---|---:|---|---|---:|
| B | Campbells — Campbells Merchant Prince Rare Musca | 4 | warn | failed | 3 |
| B | Thomas Fogarty — Thomas Fogarty Solera Gewurztraminer | 4 | warn | failed | 2 |
| B | Grgich Hills Estate — Grgich Hills Estate Fume Blanc Dry S | 4 | pass | failed | 4 |
| B | Yalumba — Yalumba The Signature Cabernet Sauvi | 4 | pass | failed | 4 |
| B | Robert Mondavi — Robert Mondavi Private Selection Bou | 4 | warn | partial | 0 |
| B | Yellow Tail — Yellow Tail Pink Moscato | 4 | warn | failed | 4 |
| B | Edna Valley Vineyard — Edna Valley Vineyard Winemaker Serie | 4 | warn | failed | 2 |
| B | Sebastiani — Sebastiani Barbera, Sonoma Valley | 4 | pass | failed | 3 |
| B | Bogle Vineyards — Bogle Vineyards Phantom Chardonnay,  | 4 | warn | failed | 2 |
| B | Masciarelli — Masciarelli Villa Gemma, Montepulcia | 3 | warn | partial | 0 |
| B | Penfolds — Penfolds V Yattarna Five Blend Chard | 3 | warn | failed | 3 |
| B | Cline — Cline Small Berry Mourvedre, Contra  | 3 | warn | failed | 5 |
| B | Joel Gott — Joel Gott Sauvignon Blanc, Californi | 3 | warn | failed | 3 |
| B | Rodney Strong — Rodney Strong of Pinot Noir, Russian | 3 | warn | failed | 2 |
| B | Merryvale — Merryvale Dutton Ranch Chardonnay, R | 3 | warn | failed | 2 |
| B | Penfolds — Penfolds Grange, South Australia | 3 | warn | passed | 0 |
| B | Zaca Mesa — Zaca Mesa Mesa Syrah, Santa Ynez Val | 3 | warn | failed | 3 |
| B | Gloria Ferrer — Gloria Ferrer Jose S. Ferrer Selecti | 3 | warn | failed | 3 |
| B | Gruet — Gruet Grand Grand Blanc, New Mexico | 3 | warn | passed | 0 |
| B | Alma Rosa — Alma Rosa La Encantada Clone 115 Pin | 3 | warn | failed | 4 |
| B | Kim Crawford — Kim Crawford Spitfire SP Sauvignon B | 3 | warn | failed | 2 |
| B | Deloach — Deloach Heintz Chardonnay, Green Val | 3 | warn | failed | 2 |
| B | Fautor — Fautor Ice, Valul lui Traian | 3 | warn | failed | 2 |
| B | Sanford — Sanford & Benedict Chardonnay, Sta.  | 3 | warn | failed | 2 |
| B | Penfolds — Penfolds Koonunga Hill Seventy Six C | 3 | warn | failed | 3 |
| B | Girard — Girard Artistry, Napa Valley | 3 | warn | failed | 3 |
| B | Beringer — Beringer Alluvium, Knights Valley | 3 | warn | failed | 2 |
| B | Rosenblum Cellars — Rosenblum Cellars Continente Zinfand | 3 | warn | failed | 5 |
| B | Charles Krug — Charles Krug Selection Lot F1 Cabern | 3 | warn | failed | 3 |
| B | ERROR | — | — | — | — |
| C | Krug — Krug Grande Cuvee 165eme Edition, Ch | 3 | warn | retried_passed | 0 |
| C | Catena Zapata — Catena Zapata Catena Paraje Altamira | 2 | fail | failed | 1 |
| C | Arrowood — Arrowood Monte Cabernet Sauvignon, S | 2 | fail | failed | 2 |
| C | Kosta Browne — Kosta Browne Pinot Noir, Anderson Va | 2 | fail | partial | 2 |
| C | Gonzalez Byass — Gonzalez Byass Nectar Dulce, Andaluc | 2 | fail | retried_passed | 0 |
| C | All Saints Estate — All Saints Estate Durif, Victoria | 2 | fail | failed | 2 |
| C | Lenotti — Lenotti Chiaretto, Bardolino | 2 | fail | failed | 2 |
| C | Black Stallion — Black Stallion Merlot, Oakville | 2 | fail | passed | 0 |
| C | Chappellet — Chappellet Pritchard Hill Cabernet S | 2 | fail | failed | 2 |
| C | Messina Hof — Messina Hof Private Orange Orange Mu | 2 | fail | failed | 2 |
| C | Penley Estate — Penley Estate Hyland Shiraz, Coonawa | 2 | fail | partial | 0 |
| C | Ferrari-Carano — Ferrari-Carano Middleridge Ranch Pin | 2 | fail | passed | 0 |
| C | Trivento — Trivento Lejanamente Juntos Cabernet | 2 | fail | passed | 0 |
| C | Tommasi — Tommasi, Valpolicella | 2 | fail | failed | 2 |
| C | Ridge Vineyards — Ridge Vineyards Tepusquet Cabernet S | 2 | fail | failed | 4 |
| C | Paul Pernot — Paul Pernot, Bourgogne | 2 | fail | failed | 2 |
| C | Brokenwood — Brokenwood Howard Semillon, Hunter V | 2 | fail | partial | 3 |
| C | Arista — Arista UV Lucky Well Pinot Noir, Rus | 2 | fail | partial | 2 |
| C | Donnafugata — Donnafugata Sur Sur, Sicily | 2 | fail | passed | 0 |
| C | Bass Phillip — Bass Phillip Premium Pinot Noir, Gip | 2 | fail | failed | 2 |
| C | Dr. Loosen — Dr. Loosen Wehlener Sonnenuhr Alte R | 2 | fail | retried_passed | 0 |
| C | Craggy Range — Craggy Range Te Muna Aroha Pinot Noi | 1 | fail | failed | 2 |
| C | Marietta — Marietta OVR Red Lot Number 72, Cali | 1 | fail | passed | 0 |
| C | Santa Carolina — Santa Carolina Carolina Chardonnay,  | 1 | fail | failed | 2 |
| C | Stonestreet — Stonestreet Aurora Point Sauvignon B | 1 | fail | failed | 2 |
| C | Kosta Browne — Kosta Browne Cerise Pinot Noir, Ande | 1 | fail | partial | 0 |
| C | Miani — Miani Filip, Friuli Colli Orientali | 1 | fail | failed | 5 |
| C | Coche-Dury — Coche-Dury, Volnay | 1 | fail | failed | 1 |
| C | Keller — Keller Florsheim Frauenberg Spatburg | 1 | fail | failed | 1 |
| C | ERROR | — | — | — | — |