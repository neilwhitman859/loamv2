# B6.6 Step 10c — Pre-execution Scorecard

Generated for 493 Chrome-validated verdicts.

## Summary

- Verdict distribution: {'SKIP': 267, 'PARENT_CHILD': 55, 'MERGE': 138, 'KEEP_AS_IS': 33}
- Tier distribution: {'yellow': 71, 'core': 143, 'mid': 138, 'tail': 141}
- MERGE pairs to apply: **139**
- PARENT_CHILD pairs to apply: **55**
- SKIP / KEEP_AS_IS (no-op): **300**

- Total wines to be re-pointed (sum of loser wine_counts): **505**
- Producers to be soft-deleted (§11.6): **132**
- Producers to have parent_producer_id set: **50**

## Flag severity summary

- **Blocking flags (need user decision):** 3
- **Soft flags (rename-on-merge / rename-on-parent-link, execute auto-handles):** 15
- **Chain-merge warnings (resolved by union-find, auto-handles):** 6

### Blocking flags

- **MERGE pair yellow#59** (Taylor's vs Taylor Fladgate): yellow_merge_target_by_name_only('Taylor Fladgate')
- **MERGE pair yellow#62** (Chateau Ausone vs ): yellow_merge_target_by_name_only('Chateau Ausone')
- **PC pair yellow#3** (Dalla Valle vs Dalla Valle & Ornellaia): yellow_parent_id_missing_or_invalid, yellow_no_child_side_resolvable

**Proposed handling for blocking items:**
- `yellow_merge_target_by_name_only(...)`: no actual merge partner in DB — these are rename-only operations. Options: (a) rename the subject producer to the target name (no row absorbed), or (b) skip. Default: **skip** during execute, flag for Sprint 7 revisit.
- `yellow_parent_id_missing_or_invalid` / `yellow_no_child_side_resolvable`: data integrity issue (e.g. string placeholder instead of UUID). Default: **skip** during execute, flag for Sprint 7 revisit.

## Flags

**24 flags requiring review.**

### MERGE resolution flags (12)

- pair yellow#18#extra (Murrieta vs Marques de Murrieta): yellow_extra_merge_from_unflagged_additional_merge_id
- pair yellow#59 (Taylor's vs Taylor Fladgate): yellow_merge_target_by_name_only('Taylor Fladgate')
- pair yellow#62 (Chateau Ausone vs ): yellow_merge_target_by_name_only('Chateau Ausone')
- pair 22770 (Coche Boulicault vs Coche Bouillot): rename_on_merge('Fabien Coche')
- pair 25553 (Marey & Liger-Belair vs Comte Liger Belair): rename_on_merge('Comte Liger-Belair')
- pair 29684 (Andre Cathiard vs Cathiard Molinier): rename_on_merge('Cathiard-Molinier')
- pair 75167 (A. D. Coutelas vs Damien Coutelas): rename_on_merge('A.D. Coutelas')
- pair 88568 (Baron de Rothschild vs Barons de Rothschild Collection): rename_on_merge('Barons de Rothschild (Champagne)')
- pair 107981 (Confuron Gindre vs Edouard Confuron): rename_on_merge('Confuron-Gindre')
- pair 7326 (Mouton Baron Philippe vs Mouton Baronne Philippe): rename_on_merge('Chateau d'Armailhac')
- pair 18763 (Vinyes Terrer vs Vins de Terrer): rename_on_merge('Vinyes del Terrer')
- pair 32416 (Thibault Ligier Belair vs Liger-Belair S.A.): rename_on_merge('Thibault Liger-Belair')

### PARENT_CHILD resolution flags (6)

- pair yellow#3 (Dalla Valle vs Dalla Valle & Ornellaia): yellow_parent_id_missing_or_invalid, yellow_no_child_side_resolvable
- pair 17949 (Quancard Pere et Fils vs de Paillet-Quancard): rename_on_parent_link('Cheval Quancard')
- pair 29511 (Nick Goldschmidt vs Chelsea Goldschmidt): rename_on_parent_link('Goldschmidt Vineyards')
- pair 52010 (Pichon Longueville Comtesse Lalande vs Pauillac De Pichon Lalande): rename_on_parent_link('Pichon Longueville Comtesse de Lalande')
- pair 63687 (Lys Lafaurie Peyraguey vs Lafaurie Peyraguey Exceptionnelle): rename_on_parent_link('Lafaurie-Peyraguey')
- pair 109451 (Casalino (Bonacchi) vs Bonacchi (Molino Suga)): rename_on_parent_link('Cantine Bonacchi')

### Chain-merge warnings (6)

The executor will resolve these to terminal survivors via union-find.

- chain: 60502039-bf51-4bb7-9ddf-5fa16d9a3e78 → c58310e3-a544-47e0-a757-2eee65618dfa → ... → 50672be0-2472-47a6-8b3e-ce179b46060f  (resolved to terminal)
- chain: d368ccb5-a3f4-4e7f-9865-9008a3b5f5ff → a6333e44-167f-4648-bf65-246c7aa0a16d → ... → d261d4f4-9b42-4c8c-8c94-87c8c6e460c1  (resolved to terminal)
- chain: 1cc53e05-44d4-4eae-aaa2-7bb74a9664ab → 00b9d6d9-1c0f-4a83-8269-98930c92445c → ... → d362fa58-25ec-4181-a3aa-5e3ed9362418  (resolved to terminal)
- chain: 80f33f70-5d56-4837-a5d7-74201fcb4528 → 60502039-bf51-4bb7-9ddf-5fa16d9a3e78 → ... → 50672be0-2472-47a6-8b3e-ce179b46060f  (resolved to terminal)
- chain: c67db24f-a726-4bca-bd07-737fe511518f → 8af07cbd-d4d7-4b38-a17b-151ba7a09571 → ... → 235a77e3-4d45-46d5-9ea3-0f0785dc1111  (resolved to terminal)
- chain: cabc9c1d-9621-4f06-b61d-6be345cc585f → 97151cf8-c801-406e-9ccf-1da0c537c749 → ... → 235a77e3-4d45-46d5-9ea3-0f0785dc1111  (resolved to terminal)

## Pattern-cluster breakdown (all verdicts)

| Cluster | MERGE | PC | SKIP | KEEP_AS_IS | Total |
|---|---|---|---|---|---|
| 11.1 | 8 | 0 | 8 | 0 | 16 |
| 11.2 | 0 | 3 | 0 | 0 | 3 |
| 11.4.b | 1 | 0 | 3 | 0 | 4 |
| 11.4.d | 1 | 0 | 0 | 0 | 1 |
| 11.4.f | 18 | 0 | 0 | 0 | 18 |
| 11.4.g | 1 | 3 | 38 | 0 | 42 |
| 11.4.h | 87 | 0 | 3 | 0 | 90 |
| 11.4.j | 0 | 0 | 33 | 0 | 33 |
| 11.4.m | 0 | 0 | 121 | 0 | 121 |
| 11.4.n | 14 | 0 | 0 | 0 | 14 |
| 11.4.o | 0 | 21 | 21 | 0 | 42 |
| 11.4.p | 6 | 2 | 5 | 0 | 13 |
| 11.4.q | 0 | 1 | 17 | 0 | 18 |
| 11.4.s | 2 | 25 | 2 | 0 | 29 |
| data_state | 0 | 0 | 1 | 0 | 1 |
| (none) | 0 | 0 | 15 | 33 | 48 |

## Top 20 largest MERGEs (by loser wine count — manual sanity-check list)

| pair_id | tier | cluster | loser → survivor | loser wines | survivor wines | combined |
|---|---|---|---|---|---|---|
| 142528 | core | 11.4.h | Chalk Hill → Chalk Hill | 46 | 13 | 59 |
| 13580 | core | 11.4.h | Barons de Rothschild → Barons de Rothschild (Lafite) | 39 | 1 | 40 |
| 141172 | core | 11.4.h | Barons de Rothschild → Barons de Rothschild (Lafite) | 39 | 1 | 40 |
| 141176 | core | 11.4.h | Barons de Rothschild → Barons de Rothschild (Lafite) | 39 | 1 | 40 |
| 136068 | core | 11.4.n | Selaks → Selaks | 35 | 6 | 41 |
| 156806 | core | 11.4.f | Florent Rouve → Jean Rijckaert | 28 | 54 | 82 |
| 54064 | core | 11.4.h | Gassier → Michel & Tina Gassier | 14 | 2 | 16 |
| 52229 | core | 11.4.f | Amiot Bonfils → Guy Amiot et Fils | 13 | 35 | 48 |
| 71588 | core | 11.4.g | des Heritiers Louis Jadot → Louis Jadot (Jacques) | 12 | 2 | 14 |
| 139102 | core | 11.4.p | Boutinot → Boutinot | 12 | 1 | 13 |
| 123759 | mid | 11.4.f | Reyane et Pascal Bouley → Pierrick Bouley | 12 | 32 | 44 |
| 62908 | core | 11.4.h | Beausejour → Beau-Sejour Becot | 11 | 4 | 15 |
| 25412 | core | 11.4.f | Guy Castagnier → Castagnier | 10 | 16 | 26 |
| 103518 | core | 11.4.h | Clavelier → Clavelier et Fils | 9 | 10 | 19 |
| 43596 | mid | 11.4.h | Comtesse de Cherisey → Martelet de Cherisey | 9 | 2 | 11 |
| 11105 | mid | 11.4.h | Charles Thomas et Moillard → Moillard | 8 | 64 | 72 |
| 54026 | mid | 11.4.h | Boisson → Anne Boisson | 8 | 7 | 15 |
| 147297 | core | 11.1 | Romuald Petit → Molozay Chateau de Vaux | 7 | 12 | 19 |
| 355 | mid | 11.4.f | Rene Cacheux → Patrice Cacheux | 7 | 2 | 9 |
| 49717 | mid | 11.4.h | Laborde → de Laborde | 7 | 6 | 13 |

## Top 20 largest PARENT_CHILD assignments (by parent wine count)

| pair_id | tier | cluster | child → parent | child wines | parent wines |
|---|---|---|---|---|---|
| 101840 | core | 11.4.s | Esprit Leflaive → Olivier Leflaive | 31 | 185 |
| 44807 | core | 11.4.o | Devaux & Michel Chapoutier → M. Chapoutier | 2 | 161 |
| 136220 | core | 11.4.o | Santos & Chapoutier → M. Chapoutier | 1 | 161 |
| 137013 | core | 11.4.o | Bento & Chapoutier → M. Chapoutier | 1 | 161 |
| 6721 | core | 11.4.s | Verget au Sud → Verget | 1 | 137 |
| yellow#51 | yellow | 11.2 | Robert Weil Junior → Robert Weil | 5 | 122 |
| yellow#52 | yellow | 11.2 | Selbach → Selbach-Oster | 2 | 117 |
| yellow#33 | yellow | 11.2 | Catena Zapata (DV Catena) → Catena Zapata | 2 | 101 |
| 42479 | core | 11.4.o | David Duband & Louis Max → Louis Max | 2 | 83 |
| 18712 | core | 11.4.q | Hospices de Beaune (Andre Pierre) → Pierre Andre | 1 | 81 |
| 143638 | core | 11.4.o | Alex Gambal Peter Work → Alex Gambal | 3 | 74 |
| 99815 | core | 11.4.o | Cooper's Hawk & Ste. Michelle → Cooper's Hawk | 1 | 58 |
| 99816 | core | 11.4.o | Cooper's Hawk & LVE → Cooper's Hawk | 1 | 58 |
| 57771 | core | 11.4.o | Alex et Benoit Moreau → Alex Moreau | 1 | 47 |
| 40820 | core | 11.4.o | XU x Eva Fricke → Eva Fricke | 1 | 46 |
| 38882 | core | 11.4.o | Wheeler & Fromm → Fromm | 6 | 44 |
| 95181 | core | 11.4.o | Werner Nakel (Neil Ellis) → Neil Ellis | 1 | 41 |
| 117740 | core | 11.4.s | Bishop Creek Cellars → Erath | 1 | 41 |
| 115931 | core | 11.4.s | Starside → Two Vintners | 1 | 39 |
| 142038 | core | 11.4.o | Catena (Baron Rothschild) → Barons de Rothschild | 1 | 39 |

## FK surface impact

Sum of rows across FK-referencing tables, aggregated over all loser producers:

| Table.Column | Rows to re-point |
|---|---|
| source_ttb_colas.canonical_producer_id | 1,127 |
| wines.producer_id | 505 |
| source_lwin.canonical_producer_id | 455 |
| source_pro_platform.canonical_producer_id | 135 |
| source_specs.canonical_producer_id | 74 |
| source_tabc.canonical_producer_id | 73 |
| source_kansas_brands.canonical_producer_id | 66 |
| source_wv_abca.canonical_producer_id | 46 |
| source_enofile.canonical_producer_id | 26 |
| source_texsom.canonical_producer_id | 25 |
| source_wallys.canonical_producer_id | 19 |
| source_horizon.canonical_producer_id | 9 |
| source_systembolaget.canonical_producer_id | 8 |
| source_pa.canonical_producer_id | 4 |
| source_utah_dabs.canonical_producer_id | 4 |
| source_winedeals.canonical_producer_id | 3 |
| source_best_wine_store.canonical_producer_id | 3 |
| source_flatiron.canonical_producer_id | 2 |
| source_kermit_lynch_growers.canonical_producer_id | 1 |
| source_lcbo.canonical_producer_id | 1 |

## Per-tier verdict breakdown

| Tier | MERGE | PC | SKIP | KEEP_AS_IS | Total |
|---|---|---|---|---|---|
| yellow | 13 | 4 | 21 | 33 | 71 |
| core | 40 | 26 | 77 | 0 | 143 |
| mid | 44 | 10 | 84 | 0 | 138 |
| tail | 41 | 15 | 85 | 0 | 141 |

---

**Next step:** user review + signoff. If clean, run `scripts/sprint6_step10_execute.py --execute`.
