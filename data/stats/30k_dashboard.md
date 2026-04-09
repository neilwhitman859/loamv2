=======================================================
  LOAM 30K PLAN                    2026-04-09
=======================================================

  SESSIONS
  ___________________________________________________
  [x]  1. Phase 0: Archive & Schema              2026-04-08
  [x]  2. Identity Design + Josh Test Sample     2026-04-08 ($0.00)
  [x]  3. Batch 0: Prototype (50 producers)      2026-04-08 ($0.00)
  [x]  4. Batch 0 Review + Go/No-Go              2026-04-08 ($0.00)
  [x]  5. Batch 1 Part 1 (500 producers)         2026-04-08 ($0.00)
  [x]  6. Batch 1 Part 2 (depth + enrichment)    2026-04-09 ($0.00)
  [x]  7. Batch 2 (2K producers + depth)         2026-04-09 ($0.00)
  [x]  7.2 TTB Linking + Josh Test               2026-04-09 ($0.00)
  [-]  8. Batch 3 / Gap Fill                     SKIPPED (89% findability)
  [ ]  9. Enrichment Sweep                       
  [ ] 10. Josh Test + Final Validation           

  PHASE STATUS
  ___________________________________________________
  0  Archive & Schema       DONE
  1  Producer Canon         2,532 producers
  2  Wine Identity          51,035 wines
  3  Vintage & Depth        67,310 vintages (+21,821 from TTB)
  4  Enrichment             PENDING (next step)
  5  Josh Test              89% findability, 6.5/8 depth

  PROGRESS
  ___________________________________________________
  Producers       2,532  [##########..........] 51%
  Wines          51,035  [####################] 100%
  w/ vintage     ~37K   [###############.....] 72%
  w/ grapes      35,704  [#############.......] 70%
  w/ COLA        10,906  [####................] 21%
  w/ label image ~37K   [###############.....] 72%
  w/ ABV         ~38K   [###############.....] 75%

  CONFIRMATION (updated post-TTB)
  ___________________________________________________
  Wines w/ COLA        10,906  (21.4%)
  Wines w/ LWIN        51,035  (100%)
  Vintages total       67,310

  JOSH TEST (Session 7.2)
  ___________________________________________________
  v1 (Python)     237/265  (89%)  — inflated, custom matching
  v2 (search_catalog) 223/265 (84%) — honest, uses real RPC
  $0-10            20/35   (57%)  v2
  $10-30           65/80   (81%)  v2
  $30-100          83/90   (92%)  v2
  $100-250         37/40   (92%)  v2
  $250+            18/20   (90%)  v2
  Avg depth         6.5/8  (v1 found set)
  Missing wines       42   (v2)

  BUDGET
  ___________________________________________________
  Spent: $0.00 / $175.00
  [....................] 0%

  REFERENCE TABLES (should be stable)
  ___________________________________________________
  Appellations        3,662
  App. rules          1,165
  Grapes              9,695
  Regions               389
  Countries              68

  TTB LINKING (Session 7.2)
  ___________________________________________________
  Producers w/ TTB   2,157/2,532 (85%)
  COLA IDs created   200,289
  Vintages created    21,821
  ABV filled           2,318
  Label URLs          37,200

  NEXT ACTION
  ___________________________________________________
    Session 9: Enrichment Sweep (Grade C Haiku batch)

=======================================================
