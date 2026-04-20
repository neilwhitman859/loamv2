# Sprint 6 B6.6 — Pre-execution Scorecard

Ledger entries: **493**

## Final verdict distribution

| Verdict | Count |
|---|---|
| MERGE | 109 |
| PARENT_CHILD | 50 |
| SKIP | 296 |
| KEEP_AS_IS | 33 |
| DEFERRED_SPRINT_7 | 5 |

- Unique producers soft-deleted: **110**
- Producers with parent_producer_id set: **50**
- Total wines re-pointed: **363**
- B6.6 overrides applied: **193**
  - Breakdown: {'FLIP_TO_SKIP': 29, 'FLIP_TO_MERGE': 2, 'NEEDS_HUMAN_REVIEW': 5, 'FLIP_TO_PC': 1, 'FLIP_DIRECTION': 1}
- Canonical-row redirects (existing DB row used as merge target): **17**

## Per-tier breakdown

| Tier | MERGE | PC | SKIP | KEEP_AS_IS | DEFERRED | Total |
|---|---|---|---|---|---|---|
| yellow | 13 | 4 | 21 | 33 | 0 | 71 |
| core | 30 | 22 | 88 | 0 | 3 | 143 |
| mid | 31 | 10 | 96 | 0 | 1 | 138 |
| tail | 35 | 14 | 91 | 0 | 1 | 141 |

## Pattern-cluster breakdown

| Cluster | MERGE | PC | SKIP | Total |
|---|---|---|---|---|
| 11.1 | 6 | 1 | 9 | 16 |
| 11.2 | 0 | 3 | 0 | 3 |
| 11.4.b | 1 | 0 | 3 | 4 |
| 11.4.d | 1 | 0 | 0 | 1 |
| 11.4.f | 13 | 0 | 5 | 18 |
| 11.4.g | 1 | 3 | 38 | 42 |
| 11.4.h | 64 | 0 | 23 | 90 |
| 11.4.j | 0 | 0 | 33 | 33 |
| 11.4.m | 0 | 0 | 121 | 121 |
| 11.4.n | 14 | 0 | 0 | 14 |
| 11.4.o | 1 | 19 | 22 | 42 |
| 11.4.p | 7 | 1 | 5 | 13 |
| 11.4.q | 0 | 1 | 17 | 18 |
| 11.4.s | 1 | 22 | 4 | 29 |
| data_state | 0 | 0 | 1 | 1 |
| (none) | 0 | 0 | 15 | 48 |

## FK surface impact (rows to re-point)

| Table.Column | Rows |
|---|---|
| source_ttb_colas.canonical_producer_id | 3,100 |
| wines.producer_id | 353 |
| source_lwin.canonical_producer_id | 327 |
| source_pro_platform.canonical_producer_id | 98 |
| source_wallys.canonical_producer_id | 87 |
| source_specs.canonical_producer_id | 78 |
| source_tabc.canonical_producer_id | 61 |
| source_kansas_brands.canonical_producer_id | 40 |
| source_wv_abca.canonical_producer_id | 18 |
| source_flatiron.canonical_producer_id | 16 |
| source_texsom.canonical_producer_id | 9 |
| source_bc_liquor.canonical_producer_id | 9 |
| source_enofile.canonical_producer_id | 7 |
| source_horizon.canonical_producer_id | 7 |
| source_systembolaget.canonical_producer_id | 6 |
| source_pa.canonical_producer_id | 4 |
| source_lcbo.canonical_producer_id | 2 |
| source_utah_dabs.canonical_producer_id | 2 |
| source_winedeals.canonical_producer_id | 1 |

## Top 20 largest MERGEs (by loser wine count)

| ledger_key | cluster | loser → survivor | loser wines | survivor wines | via canonical? |
|---|---|---|---|---|---|
| core#156806 | 11.4.f | Jean Rijckaert → Rijckaert | 54 | 8 | yes |
| core#141172 | 11.4.h | Barons de Rothschild → Barons de Rothschild (Lafite) | 39 | 3 | yes |
| core#156806 | 11.4.f | Florent Rouve → Rijckaert | 28 | 8 | yes |
| core#52229 | 11.4.f | Amiot Bonfils → Guy Amiot et Fils | 13 | 35 | no |
| core#71588 | 11.4.g | des Heritiers Louis Jadot → Louis Jadot | 12 | 332 | yes |
| mid#123759 | 11.4.f | Reyane et Pascal Bouley → Pierrick Bouley | 12 | 32 | no |
| yellow#14 | 11.4.h | de Chevalier → de Chevalier | 10 | 10 | no |
| core#25412 | 11.4.f | Guy Castagnier → Castagnier | 10 | 16 | no |
| mid#11105 | 11.4.h | Charles Thomas et Moillard → Moillard | 8 | 64 | yes |
| core#96369 | 11.4.h | Krems → Stadt Krems | 6 | 12 | no |
| core#136068 | 11.4.n | Selaks → Selaks | 6 | 35 | no |
| mid#22770 | 11.4.h | Coche Bouillot → Fabien Coche | 6 | 38 | yes |
| core#13568 | 11.4.h | Baron de Rothschild → Barons de Rothschild | 5 | 39 | no |
| mid#65055 | 11.4.h | des Sanzay → Antoine Sanzay | 5 | 9 | no |
| yellow#4 | 11.1 | Château Haut-Brion → Haut-Brion | 4 | 6 | no |
| core#37630 | 11.4.h | Mondavi → Robert Mondavi | 4 | 47 | no |
| mid#29246 | 11.4.h | Francesco Sobrero → Sobrero | 4 | 5 | no |
| yellow#9 | 11.4.s | CVNE (Contino) → CVNE | 3 | 71 | no |
| yellow#13 | 11.1 | Carneros → Carneros by Taittinger | 3 | 10 | no |
| core#645 | 11.1 | Stefani → De Stefani | 3 | 12 | no |

## Top 20 largest PARENT_CHILDs (by parent wine count)

| ledger_key | cluster | child → parent | child wines | parent wines |
|---|---|---|---|---|
| core#101840 | 11.4.s | Esprit Leflaive → Olivier Leflaive | 31 | 185 |
| core#44807 | 11.4.o | Devaux & Michel Chapoutier → M. Chapoutier | 2 | 161 |
| core#136220 | 11.4.o | Santos & Chapoutier → M. Chapoutier | 1 | 161 |
| core#137013 | 11.4.o | Bento & Chapoutier → M. Chapoutier | 1 | 161 |
| core#6721 | 11.4.s | Verget au Sud → Verget | 1 | 137 |
| yellow#51 | 11.2 | Robert Weil Junior → Robert Weil | 5 | 122 |
| yellow#52 | 11.2 | Selbach → Selbach-Oster | 2 | 117 |
| yellow#33 | 11.2 | Catena Zapata (DV Catena) → Catena Zapata | 2 | 101 |
| core#42479 | 11.4.o | David Duband & Louis Max → Louis Max | 2 | 83 |
| core#18712 | 11.4.q | Hospices de Beaune (Andre Pierre) → Pierre Andre | 1 | 81 |
| core#143638 | 11.4.o | Alex Gambal Peter Work → Alex Gambal | 3 | 74 |
| core#99815 | 11.4.o | Cooper's Hawk & Ste. Michelle → Cooper's Hawk | 1 | 58 |
| core#99816 | 11.4.o | Cooper's Hawk & LVE → Cooper's Hawk | 1 | 58 |
| core#40820 | 11.4.o | XU x Eva Fricke → Eva Fricke | 1 | 46 |
| core#38882 | 11.4.o | Wheeler & Fromm → Fromm | 6 | 44 |
| core#95181 | 11.4.o | Werner Nakel (Neil Ellis) → Neil Ellis | 1 | 41 |
| core#142038 | 11.4.o | Catena (Baron Rothschild) → Barons de Rothschild | 1 | 39 |
| core#31354 | 11.4.o | Wilson & Valdespino → Valdespino | 1 | 32 |
| core#100454 | 11.4.o | Francoise Martinot (Charles Dufour) → Charles Dufour | 11 | 27 |
| core#141630 | 11.4.o | Santos & Chapoutier → Chapoutier | 1 | 17 |

## Flags

- Entries flagged for Sprint 7 follow-up: **7**
  - core#57771: moreau_family_5_row_cleanup
  - core#62908: beausejour_row_needs_per_wine_split
  - core#103518: deferred
  - core#115931: deferred
  - core#141176: deferred
  - mid#4058: deferred
  - tail#114856: deferred
