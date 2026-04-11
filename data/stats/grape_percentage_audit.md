# Grape percentage audit

**Generated:** 2026-04-11T08:38:05
**Script:** `pipeline/analyze/audit_grape_percentages.py` (read-only)

## Summary

- Active wines with at least one `wine_grapes` row: **35,137**
- Wines whose percentages sum to **> 100%**: **6,570** (18.7%)
- Of those, have a LWIN match (re-derivable): **6,570**
- Without LWIN: **0**

## Per-pattern breakdown

| Bucket | Wines | Avg sum | Avg links | multi-100 only | mix 100+partial | has NULL link | single 100 |
|---|---:|---:|---:|---:|---:|---:|---:|
| **101-150** | 118 | 147.3 | 2.5 | 0 | 1 | 35 | 0 |
| **151-175** | 4,451 | 174.9 | 2.0 | 0 | 4,382 | 33 | 0 |
| **176-200** | 1,871 | 185.2 | 2.0 | 22 | 1,838 | 19 | 0 |
| **201-250** | 49 | 247.4 | 3.1 | 0 | 44 | 5 | 0 |
| **251-299** | 52 | 270.5 | 3.2 | 0 | 48 | 9 | 0 |
| **300** | 1 | 300.0 | 6.0 | 0 | 0 | 1 | 0 |
| **301-400** | 23 | 348.9 | 4.3 | 0 | 22 | 3 | 0 |
| **>400** | 5 | 440.0 | 5.6 | 0 | 5 | 2 | 0 |

**Column key:**
- `multi_100 only` — all grape links are 100% (e.g. 3 × 100% = 300%). Classic duplicate-promotion pattern.
- `mix 100+partial` — some links at 100%, at least one with a <100% number (e.g. 100% + 75% = 175%).
- `has NULL link` — at least one row has `percentage IS NULL` alongside non-null totals.
- `single 100` — exactly one row, at 100%. Should never be here (sum = 100 = ok). If non-zero, data quality bug.

## Sample offenders per bucket

### 101-150 — sample of 10 wines

- **Tobin James James Gang Cabernet Franc, Paso Robles** — CABERNET FRANC 75%, DOLCETTO 75%, TERRANO NULL%
- **Columbia Crest Two Vines Chardonnay, Washington** — SAUVIGNON BLANC 75%, PINOT BLANC 75%
- **Garage Wine Co. Bagual Mataro field-blend Lot #116 Carignan, Central Valley** — CARIGNAN NOIR 75%, GARNACHA BLANCA 75%, GARNACHA TINTA NULL%
- **Castoro Cellars Whale Rock Cabernet Sauvignon, Paso Robles** — CABERNET SAUVIGNON 75%, CORBEAU 75%
- **Pedernales Lahey Graciano, Texas High Plains** — GRACIANO 75%, GARNACHA TINTA 75%
- **Chaberton Grown, Fraser Valley Madeleine Sylvaner** — SIEGERREBE 75%, GEILWEILERHOF 3- 28- 51 75%, MADELEINE SYLVANER NULL%
- **Tyrrell's Stevens Shiraz, Hunter Valley** — SYRAH 75%, HAENGLING BLAU 75%
- **Vineyard 29 29 Cabernet Sauvignon, St. Helena** — CABERNET SAUVIGNON 75%, FRANC 75%, CABERNET FRANC NULL%
- **Kunin Curtis Cabernet Franc, Santa Ynez Valley** — CABERNET FRANC 75%, MONASTRELL 75%
- **Matias Riccitelli Apple Cabernet Sauvignon, Mendoza** — CABERNET SAUVIGNON 75%, DOLCETTO 75%, CROATINA NULL%

### 151-175 — sample of 10 wines

- **Vina Cobos Felino Chardonnay, Mendoza** — CHARDONNAY BLANC 100%, PINOT BLANC 75%
- **Santa Carolina El Pacto Agreement No 2 Carmenere, Central Valley** — CARMENERE 100%, CABERNET FRANC 75%
- **Firestone Clone 174 Syrah, Santa Ynez Valley** — SYRAH 100%, DURIF 75%
- **Fowles Wine Are you Game? Chardonnay, Victoria** — PINOT NOIR 100%, PINOT BLANC 75%
- **Wakefield St. Andrews Chardonnay, Clare Valley** — CHARDONNAY BLANC 100%, PINOT BLANC 75%
- **Brancott Estate Chardonnay, Gisborne** — CHARDONNAY BLANC 100%, PINOT BLANC 75%
- **Quealy Musk Creek Pinot Gris, Mornington Peninsula** — PINOT GRIS 100%, PINOT NOIR 75%
- **Kelley Fox Freedom Hill Pinot Noir, Willamette Valley** — PINOT BLANC 100.0%, PINOT NOIR 75%
- **Plantagenet York Chardonnay, Great Southern** — CHARDONNAY BLANC 100%, PINOT BLANC 75%
- **Paul Lato Bien Nacido Il Padrino Syrah, Santa Maria Valley** — SYRAH 100%, DURIF 75%

### 176-200 — sample of 10 wines

- **J. M. Boillot, Pays d'Oc Les Roques** — SYRAH 100%, CARIGNAN NOIR 85%
- **Schumacher Selection Wormeldange Weinbour, Moselle Luxembourgeoise Pinot Gris** — PINOT GRIS 100%, PINOT NOIR 85%
- **Agathe Bursin, Alsace Bollenberg** — MUSCAT OF ALEXANDRIA 100.0%, MUSCAT A PETITS GRAINS ROSES 85%
- **Artisan Wines Chardonnay, California** — CHARDONNAY BLANC 100%, PINOT BLANC 85%
- **Gabriel Meffre, Pays d'Oc Laurus** — SYRAH 100%, DURIF 85%
- **Cuilleron, Collines Rhodaniennes Sybel** — SYRAH 100%, DURIF 85%
- **Domaines Vinsmoselle Bech-Kleimacher Naumberg, Moselle Luxembourgeoise Pinot Gris** — PINOT GRIS 100%, PINOT NOIR 85%
- **Durnberg Veltliner Eiswein, Niederösterreich** — VELTLINER GRUEN 100%, VELTLINER FRUEHROT 85%
- **Marc Kreydenweiss, Alsace Lerchenberg** — CANARI NOIR 100.0%, PINOT NOIR 85%
- **des Cassagnoles, Côtes de Gascogne Sauvignon** — CHARDONNAY BLANC 100%, PINOT BLANC 85%

### 201-250 — sample of 10 wines

- **Gitana Winery Autograf, Valul lui Traian Chardonnay** — CHARDONNAY BLANC 100%, PINOT BLANC 75%, FETEASCA REGALA 75%
- **Seven of Hearts Chateau Figareaux Tannat, Columbia Valley** — CABERNET SAUVIGNON 100.0%, TANNAT 75%, CABERNET FRANC 75%
- **Fantelli Serie Magno Ancellotta, Mendoza** — COT 100%, CABERNET SAUVIGNON 75%, ANCELLOTTA 75%
- **Messina Hof Private Blanc Du Bois, Texas** — CHARDONNAY BLANC 100%, BLANC DU BOIS 75%, PINOT BLANC 75%
- **Villa Maria East Coast Private Bin Gewurztraminer, Hawke's Bay** — CHARDONNAY BLANC 100.0%, PINOT NOIR 75%, GEWUERZTRAMINER 75%
- **Happs Three Hills Cabernet, Margaret River** — CABERNET SAUVIGNON 75%, TANNAT 75%, CABERNET FRANC 75%
- **Lui Wind Blend Cabernet Sauvignon, Mendoza** — COT 100%, CABERNET SAUVIGNON 75%, DOLCETTO 75%, MERLOT NOIR NULL%
- **Alfredo Roca Fincas Chardonnay, Mendoza** — CHARDONNAY BLANC 100%, FRIULANO 75%, PINOT BLANC 75%
- **Robert Ramsay McKinley Springs Cinsault, Horse Heaven Hills** — SYRAH 100%, CINSAUT 75%, DURIF 75%
- **Miguel Minni Premium Ancellotta, Mendoza** — CABERNET FRANC 75%, ANCELLOTTA 75%, DOLCETTO 75%

### 251-299 — sample of 10 wines

- **Martin Wassmer Markgraflerland SW Chardonnay, Baden** — CHARDONNAY BLANC 100%, PINOT GRIS 85%, PINOT BLANC 85%
- **Terranoble Cabernet Sauvignon, Central Valley** — CHARDONNAY BLANC 100.0%, SAUVIGNON BLANC 100.0%, CABERNET SAUVIGNON 75%, SYRAH NULL%, VERDOT PETIT NULL%, MARSELAN NULL%
- **De Martino Viejas Tinajas Muscat, Sur** — MUSCAT OF ALEXANDRIA 100.0%, MUSCAT A PETITS GRAINS BLANCS 100.0%, MUSCAT A PETITS GRAINS ROSES 75%
- **Potzinger Gelber Muskateller, Südsteiermark** — WELSCHRIESLING 100%, RIESLING WEISS 85%, MUSCAT A PETITS GRAINS BLANCS 85%
- **von Winning Royale Chardonnay, Pfalz** — CHARDONNAY BLANC 100%, PINOT BLANC 85%, KNIPPERLE 85%
- **2Naturkinder Pet-Nat Silvaner, Franken** — SILVANER ROT 100.0%, SILVANER GRUEN 85%, BACCHUS WEISS 85%, CLINTON NULL%
- **Domaines Vinsmoselle Coteaux de Schengen, Moselle Luxembourgeoise Pinot Gris** — AUXERROIS 100%, PINOT NOIR 85%, COT 85%
- **Sauska Birtok, Tokaj Furmint** — FURMINT 100%, HARSLEVELUE 85%, GRASA DE COTNARI 85%
- **90+ Cellars Lot 199 Nebbiolo, Langhe** — CROATINA 100.0%, DOLCETTO 85%, NEBBIOLO 75%
- **Kox Privilege Remich Fels, Moselle Luxembourgeoise Auxerrois** — PINOT GRIS 100%, PINOT NOIR 85%, COT 85%

### 300 — sample of 10 wines

- **des Muses Tradition, Valais Heida** — HUMAGNE BLANCHE 75%, SILVANER GRUEN 75%, VELTLINER FRUEHROT 75%, GEWUERZTRAMINER 75%, MALVASIA NULL%, SAVAGNIN BLANC NULL%

### 301-400 — sample of 10 wines

- **Knoll Blauer Burgunder Federspiel, Wachau** — CHARDONNAY BLANC 100%, MUSCAT A PETITS GRAINS BLANCS 85%, PINOT BLANC 85%, PINOT NOIR 85%, MUSCAT OF ALEXANDRIA NULL%, SAVAGNIN ROSE NULL%
- **Audrey Wilkinson Winemakers Selection Chardonnay, Hunter Valley** — CHARDONNAY BLANC 100%, PINOT BLANC 75%, GOUVEIO REAL 75%, MUSCAT A PETITS GRAINS ROSES 75%
- **Klet Brda Quercus, Goriška Brda Chardonnay** — CHARDONNAY BLANC 100%, RIBOLLA GIALLA 85%, SAUVIGNON BLANC 85%, PINOT BLANC 85%
- **Desom Remich Primerberg, Moselle Luxembourgeoise Auxerrois** — PINOT GRIS 100%, PINOT NOIR 85%, MUELLER THURGAU WEISS 85%, COT 85%
- **Solminer Gruner Veltliner, Santa Ynez Valley** — VELTLINER GRUEN 100%, MUSCAT A PETITS GRAINS BLANCS 100%, MUSCAT A PETITS GRAINS ROSES 75%, VELTLINER FRUEHROT 75%
- **Santa Julia Textual Innovacion Extrema Caladoc, Mendoza** — CARMENERE 100%, TEROLDEGO 75%, CALADOC 75%, CABERNET FRANC 75%
- **Diehl Eins Zu Eins Chardonnay, Pfalz** — CHARDONNAY BLANC 100%, PINOT GRIS 85%, PINOT BLANC 85%, KNIPPERLE 85%
- **Duhr Freres Molaris, Moselle Luxembourgeoise Auxerrois** — PINOT BLANC 100%, ELBLING WEISS 85%, MUELLER THURGAU WEISS 85%, COT 85%
- **Santolin Cosa Nostra Arneis, Yarra Valley** — PINOT GRIS 100%, FRIULANO 75%, ARNEIS 75%, PINOT NOIR 75%
- **Adelsheim Ribbon Springs Auxerrois, Ribbon Ridge** — CHARDONNAY BLANC 100%, PINOT NOIR 100.0%, PINOT BLANC 75%, COT 75%

### >400 — sample of 10 wines

- **Warburn Estate 1164 Durif, Riverina** — MUSCAT A PETITS GRAINS ROUGES 100.0%, MONTEPULCIANO 100%, LAGREIN 75%, SEIBEL 405 75%, DURIF 75%
- **Weinwurms Chardonnay, Niederösterreich** — CHARDONNAY BLANC 100%, WELSCHRIESLING 100%, RIESLING WEISS 85%, PINOT BLANC 85%, VELTLINER ROT 85%
- **Fautor 310 Altitude, Valul lui Traian Feteasca Regala** — CHARDONNAY BLANC 100%, RIESLING WEISS 100%, SAUVIGNON BLANC 75%, FETEASCA REGALA 75%, GEWUERZTRAMINER 75%
- **Heinrich Muscat** — WELSCHRIESLING 100%, NEUBURGER 85%, GEWUERZTRAMINER 85%, RIESLING WEISS 85%, MUSCAT A PETITS GRAINS ROSES 85%, SAVAGNIN ROSE NULL%, MUSCAT OF ALEXANDRIA NULL%
- **Scheiblhofer Chardonnay, Burgenland** — CHARDONNAY BLANC 100%, WELSCHRIESLING 100%, RIESLING WEISS 85%, MUSCAT A PETITS GRAINS BLANCS 85%, PINOT BLANC 85%, MOSCATO DI TERRACINA NULL%

## Repair strategy — decide per bucket

This is the review gate. Pick a strategy per bucket, then run `fix_grape_percentages.py --dry-run`.

Candidate strategies:

1. **NULL-out percentages** — keep the grape links, clear all `percentage` values on wines in this bucket. Conservative; loses blend proportion but keeps grape identity.
2. **Keep single highest-confidence row, drop others** — if one row has a meaningful percentage and the rest are redundant 100s, keep the one row and delete the 100s.
3. **Re-derive from LWIN** — for wines with a LWIN match, pull percentages from `source_lwin` (if LWIN carries clean values for that wine).
4. **Normalize to sum 100** — scale all non-null values proportionally so the sum becomes 100. Only valid if the current values encode real relative proportions and just need scaling.
5. **Leave alone** — if the pattern is edge-case and small (e.g. 110 bucket from rounding), may not be worth touching.

Log the chosen strategy per bucket in `docs/DECISIONS.md` before executing the fix script.