# Execution Plan (Narrative)

This is a human-readable walkthrough of what the execute script will do.
The source of truth is `scripts/sprint6_step10_execute.py`; this document is
a review aid.

## High-level

- **109 MERGEs** — absorb one producer row into another
- **50 PARENT_CHILDs** — set `parent_producer_id` on child row
- **334 SKIPs / KEEP_AS_IS / DEFERREDs** — no action this sprint

## Per-merge protocol

For each MERGE, the execute script runs this transaction:

```sql
BEGIN;
-- 1. Snapshot the loser row for reversibility
-- (recorded in producer_merge_history.merged_producer_json)

-- 2. Re-point FKs for every table referencing producers.id
UPDATE wines SET producer_id = :survivor WHERE producer_id = :loser;
UPDATE source_ttb_colas SET canonical_producer_id = :survivor WHERE canonical_producer_id = :loser;
-- ... (every FK-referencing table; row IDs captured in repointed_rows JSONB)

-- 3. Move alias (loser's name becomes an alias of survivor)
INSERT INTO producer_aliases (producer_id, name, name_normalized, source, alias_type)
VALUES (:survivor, :loser_name, lower(:loser_name), 'b6_6_merge', 'merged_from');

-- 4. Record the merge event (reversible)
INSERT INTO producer_merge_history
  (merged_producer_id, survivor_producer_id, merged_producer_json,
   repointed_rows, method_name, reasoning, reviewed_by)
VALUES (:loser, :survivor, :snapshot, :repointed, 'B6.6 Chrome-validated',
        :reasoning, 'b6_6_chrome');

-- 5. Soft-delete the loser
UPDATE producers SET deleted_at = NOW() WHERE id = :loser;
COMMIT;
```

Each pair runs independently — one failure does not roll back the rest.
Chain merges are pre-resolved via union-find so the terminal survivor
receives all absorbed rows.

## Per-PC protocol

```sql
BEGIN;
UPDATE producers SET parent_producer_id = :parent WHERE id = :child;
INSERT INTO producer_merge_history -- records the PC as a non-merge history event
  (merged_producer_id, survivor_producer_id, merged_producer_json,
   repointed_rows, method_name, reasoning)
VALUES (:child, :parent, :child_snapshot, '{"parent_child_link": true}',
        'B6.6 Chrome-validated', :reasoning);
COMMIT;
```

## MERGEs (detailed)

| ledger_key | cluster | loser → survivor | loser wines | survivor wines | canonical redirect? |
|---|---|---|---|---|---|
| yellow#4 | 11.1 | `Haut-Brion` → `Château Haut-Brion` | 6 | 4 | no |
| yellow#5 | 11.4.d | `Clos de la Coulee de Serrant` → `Nicolas Joly` | 1 | 6 | no |
| yellow#7 | 11.4.p | `Taillevent (Schloss Gobelsburg)` → `Gobelsburg` | 1 | 46 | no |
| yellow#9 | 11.4.s | `CVNE (Contino)` → `CVNE` | 3 | 71 | no |
| yellow#13 | 11.1 | `Carneros` → `Carneros by Taittinger` | 3 | 10 | no |
| yellow#14 | 11.4.h | `de Chevalier` → `de Chevalier` | 10 | 10 | no |
| yellow#18 | 11.1 | `Marques de Murrieta` → `de Murrieta` | 1 | 30 | no |
| yellow#38 | 11.4.h | `Lynch Bages Blanc` → `Lynch-Bages` | 0 | 5 | no |
| yellow#58 | 11.4.p | `Mouton Rothschild (Luze)` → `Château Mouton Rothschild` | 1 | 3 | no |
| yellow#59 | 11.1 | `None` → `Taylor's` | 0 | 57 | no |
| yellow#62 | 11.1 | `None` → `Château Ausone` | 0 | 2 | no |
| yellow#65 | 11.4.h | `Pauillac De Pichon Lalande` → `Pichon Longueville Comtesse de Lalande` | 1 | 6 | no |
| yellow#71 | 11.4.h | `Graillot` → `Alain Graillot` | 1 | 7 | no |
| core#645 | 11.1 | `Stefani` → `De Stefani` | 3 | 12 | no |
| core#4067 | 11.4.h | `Vocoret et Fils` → `Vocoret` | 2 | 15 | no |
| core#13568 | 11.4.h | `Baron de Rothschild` → `Barons de Rothschild` | 5 | 39 | no |
| core#13580 | 11.4.h | `Barons de Rothschild` → `Barons de Rothschild (Lafite)` | 39 | 3 | no |
| core#25412 | 11.4.f | `Guy Castagnier` → `Castagnier` | 10 | 16 | no |
| core#37630 | 11.4.h | `Mondavi` → `Robert Mondavi` | 4 | 47 | no |
| core#47775 | 11.4.p | `Taillevent (Joseph Drouhin)` → `Joseph Drouhin` | 1 | 238 | no |
| core#52229 | 11.4.f | `Amiot Bonfils` → `Guy Amiot et Fils` | 13 | 35 | no |
| core#54064 | 11.4.h | `Gassier` → `Michel & Tina Gassier` | 14 | 2 | no |
| core#55533 | 11.4.h | `Edouard` → `Edouard Delaunay` | 1 | 66 | no |
| core#68320 | 11.4.h | `Thomas-Moillard` → `Moillard` | 1 | 64 | no |
| core#71588 | 11.4.g | `Louis Jadot (Jacques)` + `des Heritiers Louis Jadot` → `Louis Jadot` | 2 + 12 | 332 | **yes** (→Louis Jadot) |
| core#71929 | 11.4.p | `Epicure (Franck Massard)` → `Franck Massard` | 3 | 15 | no |
| core#77572 | 11.4.h | `B. Bachelet` → `Bertrand Bachelet` | 1 | 12 | no |
| core#77833 | 11.4.f | `Henry Lamarche` → `Nicole Lamarche` | 2 | 32 | no |
| core#96369 | 11.4.h | `Krems` → `Stadt Krems` | 6 | 12 | no |
| core#96741 | 11.4.f | `Benedikt Baltes` → `Bertram-Baltes` | 3 | 10 | no |
| core#100006 | 11.4.h | `Andre Bart` → `Bart` | 1 | 33 | no |
| core#100909 | 11.4.f | `Jacques et Francois Carillon` → `Francois Carillon` | 1 | 52 | no |
| core#104522 | 11.4.h | `Hermitage Thieuley` → `Thieuley` | 0 | 11 | no |
| core#104541 | 11.4.h | `Barons de Rothschild (Lafite)` → `Lafite Rothschild` | 3 | 13 | no |
| core#123240 | 11.4.p | `La Barrique du Chat Botte` → `Frederic Cossard` | 2 | 71 | no |
| core#136068 | 11.4.n | `Selaks` → `Selaks` | 35 | 6 | no |
| core#137389 | 11.4.b | `Melka` → `Melka` | 1 | 10 | no |
| core#138090 | 11.4.h | `Barons de Rothschild (Lafite)` → `Lafite Rothschild` | 1 | 13 | no |
| core#139102 | 11.4.p | `Boutinot` → `Boutinot` | 12 | 1 | no |
| core#141172 | 11.4.h | `Barons de Rothschild (Lafite)` + `Barons de Rothschild` → `Barons de Rothschild (Lafite)` | 1 + 39 | 3 | **yes** (→Barons de Rothschild (Lafite)) |
| core#142095 | 11.4.n | `90+ Cellars` → `90+ Cellars` | 1 | 145 | no |
| core#151521 | 11.4.h | `Daniel Senard` → `Comte Senard` | 3 | 31 | no |
| core#156806 | 11.4.f | `Florent Rouve` + `Jean Rijckaert` → `Rijckaert` | 28 + 54 | 8 | **yes** (→Rijckaert) |
| mid#2188 | 11.4.p | `du Duc de Magenta (Louis Jadot)` → `Duchesse de Magenta` | 3 | 1 | no |
| mid#4784 | 11.4.h | `Jean-Francois Protheau` → `Protheau & Fils` | 3 | 7 | no |
| mid#10596 | 11.4.o | `Lombardi` → `Tendil & Lombardi` | 6 | 1 | no |
| mid#11105 | 11.4.h | `Thomas-Moillard` + `Charles Thomas et Moillard` → `Moillard` | 1 + 8 | 64 | **yes** (→Moillard) |
| mid#22770 | 11.4.h | `Coche Boulicault` + `Coche Bouillot` → `Fabien Coche` | 1 + 6 | 38 | **yes** (→Fabien Coche) |
| mid#24821 | 11.4.h | `Lunelli` → `Tenute Lunelli` | 4 | 3 | no |
| mid#25553 | 11.4.f | `Comte Liger Belair` → `Marey & Liger-Belair` | 1 | 4 | no |
| mid#29246 | 11.4.h | `Francesco Sobrero` → `Sobrero` | 4 | 5 | no |
| mid#29684 | 11.4.h | `Cathiard Molinier` → `Andre Cathiard` | 3 | 5 | no |
| mid#29968 | 11.4.h | `Monlot Capet` → `Monlot` | 1 | 8 | no |
| mid#32548 | 11.4.h | `Metrat B` → `Metrat et Fils ` | 1 | 7 | no |
| mid#39875 | 11.4.h | `Eguren` → `Familia Eguren` | 6 | 2 | no |
| mid#43596 | 11.4.h | `Martelet de Cherisey` → `Comtesse de Cherisey` | 2 | 9 | no |
| mid#45402 | 11.4.h | `Rust Verde` → `Rust en Vrede` | 1 | 6 | no |
| mid#54827 | 11.4.h | `Dutraive` → `Jean-Louis Dutraive` | 3 | 1 | no |
| mid#65055 | 11.4.h | `des Sanzay` → `Antoine Sanzay` | 5 | 9 | no |
| mid#67132 | 11.4.h | `Ardhuy Cabotte` → `de la Cabotte` | 2 | 6 | no |
| mid#67192 | 11.4.h | `Preignes le Vieux (Robert Vic)` → `Robert Vic` | 2 | 4 | no |
| mid#68215 | 11.4.f | `Didier Herbert` → `Herbert & Co.` | 3 | 3 | no |
| mid#69759 | 11.4.h | `Angela` → `Angela Vineyards` | 3 | 8 | no |
| mid#70658 | 11.4.h | `de Besombes Singla` → `Singla` | 1 | 5 | no |
| mid#73166 | 11.4.h | `Manzanos Wines` → `Manzanos` | 1 | 5 | no |
| mid#75167 | 11.4.h | `Damien Coutelas` → `A. D. Coutelas` | 1 | 6 | no |
| mid#75209 | 11.4.h | `Virecourt` → `Virecourt-Conte` | 3 | 1 | no |
| mid#83519 | 11.4.h | `Duchesse de Magenta` → `du Duc de Magenta` | 1 | 3 | no |
| mid#88568 | 11.4.h | `Barons de Rothschild Collection` → `Baron de Rothschild` | 1 | 5 | no |
| mid#94415 | 11.4.h | `Lauren Glen` → `Laurel Glen` | 1 | 5 | no |
| mid#107839 | 11.4.h | `Arnauton Blanc` → `Arnauton` | 1 | 3 | no |
| mid#113019 | 11.4.h | `Rey Fernando Castilla` → `Fernando de Castilla` | 1 | 3 | no |
| mid#123759 | 11.4.f | `Reyane et Pascal Bouley` → `Pierrick Bouley` | 12 | 32 | no |
| mid#156959 | 11.4.f | `Francois Gerard` → `Xavier Gerard` | 2 | 9 | no |
| tail#2586 | 11.4.h | `Pavillon Les Sept Chenes` → `Les Sept Chenes` | 1 | 1 | no |
| tail#4054 | 11.4.h | `Andre Chopin` → `A & A Chopin` | 1 | 1 | no |
| tail#4224 | 11.4.h | `Cilla Teresa` → `Villa Teresa` | 1 | 2 | no |
| tail#5455 | 11.4.h | `Sajazarra Castillo` + `Sajazarra` → `Castillo de Sajazarra` | 1 + 1 | 1 | **yes** (→Castillo de Sajazarra) |
| tail#7326 | 11.4.f | `Mouton Baronne Philippe` → `Mouton Baron Philippe` | 1 | 1 | no |
| tail#11044 | 11.4.h | `Moulin Cabanieux` → `Moulin de Cabanieu` | 1 | 1 | no |
| tail#14103 | 11.4.h | `Soulez` → `Yves Soulez` | 1 | 1 | no |
| tail#25192 | 11.4.h | `Colinas Sao Lourenco` + `Colinas` → `Colinas de Sao Lourenco` | 1 + 1 | 1 | **yes** (→Colinas de Sao Lourenco) |
| tail#30768 | 11.4.h | `Cormey-Figeac` → `Cormeil-Figeac` | 1 | 1 | no |
| tail#36774 | 11.4.h | `Colinas Sao Lourenco` → `Colinas de Sao Lourenco` | 1 | 1 | no |
| tail#39806 | 11.4.h | `du Mas de la Tour` → `Mas de La Tour` | 1 | 2 | no |
| tail#40974 | 11.4.h | `Bord'Eaux Cabernet` + `Bord'Eaux Merlot` → `Bord'Eaux` | 1 + 1 | 1 | **yes** (→Bord'Eaux) |
| tail#45711 | 11.4.h | `Clos Les Grandes Versannes` → `Les Grandes Versannes` | 1 | 1 | no |
| tail#54063 | 11.4.h | `Michel Gassier` → `Michel & Tina Gassier` | 2 | 2 | no |
| tail#59282 | 11.4.h | `Arthus` → `d'Arthus` | 1 | 2 | no |
| tail#68759 | 11.4.h | `Marquis Valette Brut` → `Marquis Valette Brut Rose` | 1 | 1 | no |
| tail#72755 | 11.4.h | `Cosson` → `Etienne Cosson` | 1 | 1 | no |
| tail#78207 | 11.4.h | `Grave Cour` → `Grave La Cour` | 1 | 1 | no |
| tail#78862 | 11.4.f | `Christian Menaut` → `Christian et Pascal Menaut` | 1 | 2 | no |
| tail#81644 | 11.4.h | `Verget au Sud` → `Verget Du Sud` | 1 | 1 | no |
| tail#91545 | 11.4.f | `Gaffeliere` + `Gaffeliere Naudes` → `La Gaffeliere` | 1 + 1 | 7 | **yes** (→La Gaffeliere) |
| tail#111743 | 11.4.h | `Reverie II` → `Reverie` | 1 | 1 | no |
| tail#136270 | 11.4.n | `Tussock Jumper` → `Tussock Jumper` | 1 | 2 | no |
| tail#136710 | 11.4.h | `Barons Rothschild (Lafite)` + `Barons de Rothschild (Lafite)` → `Barons de Rothschild (Lafite)` | 2 + 1 | 3 | **yes** (→Barons de Rothschild (Lafite)) |
| tail#137796 | 11.4.n | `Cupcake Vineyards` → `Cupcake Vineyards` | 2 | 1 | no |
| tail#138506 | 11.4.n | `Tussock Jumper` → `Tussock Jumper` | 2 | 2 | no |
| tail#138967 | 11.4.n | `Tussock Jumper` → `Tussock Jumper` | 1 | 1 | no |
| tail#138968 | 11.4.n | `Tussock Jumper` → `Tussock Jumper` | 1 | 2 | no |
| tail#138971 | 11.4.n | `Tussock Jumper` → `Tussock Jumper` | 1 | 2 | no |
| tail#139566 | 11.4.n | `Thomson & Scott` → `Thomson & Scott` | 2 | 1 | no |
| tail#140543 | 11.4.n | `Pieroth` → `Pieroth` | 1 | 1 | no |
| tail#141223 | 11.4.n | `Tussock Jumper` → `Tussock Jumper` | 1 | 2 | no |
| tail#141399 | 11.4.n | `Tussock Jumper` → `Tussock Jumper` | 1 | 1 | no |
| tail#143564 | 11.4.n | `Prophecy` → `Prophecy` | 1 | 1 | no |
| tail#143934 | 11.4.n | `Bernard Magrez` → `Bernard Magrez` | 1 | 2 | no |

## PARENT_CHILDs (detailed)

| ledger_key | cluster | child → parent | child wines | parent wines |
|---|---|---|---|---|
| yellow#3 | 11.4.o | `Dalla Valle & Ornellaia` → `Dalla Valle` | 1 | 6 |
| yellow#33 | 11.2 | `Catena Zapata (DV Catena)` → `Catena Zapata` | 2 | 101 |
| yellow#51 | 11.2 | `Robert Weil Junior` → `Robert Weil` | 5 | 122 |
| yellow#52 | 11.2 | `Selbach` → `Selbach-Oster` | 2 | 117 |
| core#6721 | 11.4.s | `Verget au Sud` → `Verget` | 1 | 137 |
| core#10187 | 11.4.s | `Haan Hanenhof` → `Haan` | 5 | 10 |
| core#12202 | 11.4.s | `Artesano de Argento` → `Argento` | 2 | 16 |
| core#18712 | 11.4.q | `Hospices de Beaune (Andre Pierre)` → `Pierre Andre` | 1 | 81 |
| core#31354 | 11.4.o | `Wilson & Valdespino` → `Valdespino` | 1 | 32 |
| core#38882 | 11.4.o | `Wheeler & Fromm` → `Fromm` | 6 | 44 |
| core#40820 | 11.4.o | `XU x Eva Fricke` → `Eva Fricke` | 1 | 46 |
| core#42479 | 11.4.o | `David Duband & Louis Max` → `Louis Max` | 2 | 83 |
| core#44807 | 11.4.o | `Devaux & Michel Chapoutier` → `M. Chapoutier` | 2 | 161 |
| core#79706 | 11.4.s | `Brothers Koerner` → `Koerner` | 1 | 14 |
| core#95181 | 11.4.o | `Werner Nakel (Neil Ellis)` → `Neil Ellis` | 1 | 41 |
| core#99815 | 11.4.o | `Cooper's Hawk & Ste. Michelle` → `Cooper's Hawk` | 1 | 58 |
| core#99816 | 11.4.o | `Cooper's Hawk & LVE` → `Cooper's Hawk` | 1 | 58 |
| core#100454 | 11.4.o | `Francoise Martinot (Charles Dufour)` → `Charles Dufour` | 11 | 27 |
| core#101840 | 11.4.s | `Esprit Leflaive` → `Olivier Leflaive` | 31 | 185 |
| core#112554 | 11.4.o | `M. Chapoutier & Giaconda` → `Giaconda` | 1 | 15 |
| core#136220 | 11.4.o | `Santos & Chapoutier` → `M. Chapoutier` | 1 | 161 |
| core#137013 | 11.4.o | `Bento & Chapoutier` → `M. Chapoutier` | 1 | 161 |
| core#141630 | 11.4.o | `Santos & Chapoutier` → `Chapoutier` | 1 | 17 |
| core#142038 | 11.4.o | `Catena (Baron Rothschild)` → `Barons de Rothschild` | 1 | 39 |
| core#143638 | 11.4.o | `Alex Gambal Peter Work` → `Alex Gambal` | 3 | 74 |
| core#162921 | 11.4.o | `Wheeler & Fromm` → `Fromm` | 6 | 15 |
| mid#1307 | 11.4.o | `Dampt-Dupas` → `Dampt` | 1 | 5 |
| mid#9707 | 11.1 | `The 75 Wine Company (by Tuck Beckstoffer)` → `Tuck Beckstoffer` | 3 | 8 |
| mid#81610 | 11.4.s | `Petit Fombrauge` → `Fombrauge` | 1 | 5 |
| mid#97833 | 11.4.s | `Camille Paquet` → `Famille Paquet` | 3 | 6 |
| mid#105418 | 11.4.s | `Elsa Bianchi` → `Valentin Bianchi` | 1 | 7 |
| mid#108518 | 11.4.s | `Dinastia Manzanos` → `Manzanos` | 1 | 5 |
| mid#139664 | 11.4.o | `Beates-Chapoutier` → `M. Chapoutier` | 1 | 4 |
| mid#140423 | 11.4.g | `Bernard Magrez (Muraires)` → `Bernard Magrez` | 3 | 4 |
| mid#142999 | 11.4.p | `Corney & Barrow (Sichel)` → `Corney & Barrow` | 3 | 3 |
| mid#143940 | 11.4.g | `Bernard Magrez (Muraires)` → `Bernard Magrez` | 3 | 1 |
| tail#12269 | 11.4.s | `Mont Vicomte` → `Vignerons Vicomte` | 1 | 2 |
| tail#17949 | 11.4.g | `de Paillet-Quancard` → `Quancard Pere et Fils` | 1 | 1 |
| tail#29511 | 11.4.s | `Chelsea Goldschmidt` → `Nick Goldschmidt` | 1 | 1 |
| tail#51466 | 11.4.s | `La Chapelle Condat` → `Condat` | 1 | 1 |
| tail#52010 | 11.4.s | `Pauillac De Pichon Lalande` → `Pichon Longueville Comtesse Lalande` | 1 | 1 |
| tail#63687 | 11.4.s | `Lys Lafaurie Peyraguey` → `Lafaurie Peyraguey Exceptionnelle` | 0 | 1 |
| tail#65795 | 11.4.s | `Bridge (Katz & Forrester)` → `Ken Forrester` | 1 | 1 |
| tail#80627 | 11.4.s | `des Rozets (Michel Bernard)` → `Michel Bernard` | 2 | 2 |
| tail#81995 | 11.4.s | `La Perle du Bregnet` → `Clos Le Bregnet` | 1 | 1 |
| tail#108519 | 11.4.s | `Dinastia Manzanos` → `Manzanos Wines` | 1 | 1 |
| tail#109451 | 11.4.s | `Bonacchi (Molino Suga)` → `Casalino (Bonacchi)` | 1 | 1 |
| tail#111336 | 11.4.s | `Badine de La Patache` → `La Patache` | 1 | 1 |
| tail#136226 | 11.4.s | `d'Henri (Laroche)` → `Laroche` | 1 | 1 |
| tail#161981 | 11.4.s | `Rashi Joyvin` → `Rashi` | 2 | 1 |
