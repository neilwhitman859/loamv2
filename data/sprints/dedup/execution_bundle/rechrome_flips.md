# B6.6 Re-Chrome Flips

Every verdict override applied during Sprint 6 B6.6, with evidence.

## Summary

Total overrides: **38** (out of 493 ledger entries)

### By action

| Action | Count |
|---|---|
| FLIP_TO_SKIP | 29 |
| NEEDS_HUMAN_REVIEW | 5 |
| FLIP_TO_MERGE | 2 |
| FLIP_TO_PC | 1 |
| FLIP_DIRECTION | 1 |

### By source

| Source | Count |
|---|---|
| subagent_rechrome | 34 |
| manual_b6_6 | 4 |

## FLIP_TO_SKIP (29)

### core#25960 — Cheurlin Noellat vs Maxime Cheurlin Noellat

- **Original:** MERGE (cluster 11.4.f)
- **Final:** SKIP
- **Survivor change:** `Maxime Cheurlin Noellat` → `None`
- **Source:** subagent_rechrome
- **Reasoning:** Two different producers with shared surname. Side_a 'Cheurlin Noellat' wines are CHAMPAGNE (Brut Rose, Carte d'or, Cuvee Privilege) = Champagne Richard Cheurlin in Celles-sur-Ource (Aube). Side_b 'Maxime Cheurlin Noellat' is BURGUNDY Vosne-Romanee (Domaine Georges Noellat successor). Different regions, unrelated.
- **Chrome evidence:** Champagne RICHARD CHEURLIN Proprietaire-Recoltant, 16 Rue des Huguenots 10110 Celles-sur-Ource (Aube) vs. Maxime Cheurlin Noellat = Domaine Georges Noellat Vosne-Romanee
- **Chrome URL:** https://www.champagne-cheurlin.com

### core#57771 — Alex Moreau vs Alex et Benoit Moreau

- **Original:** PARENT_CHILD (cluster 11.4.o)
- **Final:** SKIP
- **Parent change:** `Alex Moreau` → `None`
- **Source:** manual_b6_6
- **Reasoning:** Chrome re-validation: 'Alex et Benoit Moreau' is a single-wine cuvée of Domaine Bernard Moreau et Fils (father's Chassagne estate, separate 11w DB row), not a collab producer. Alex and Benoît are sibling winemakers with distinct DB rows (§11.4.m). Making the cuvée a child of Alex alone is factually wrong — it's equally Benoît's, and structurally a wine of Bernard Moreau et Fils per §11.4.e.
- **Chrome evidence:** PLOC lists the Fleurie as 'Domaine Bernard Moreau et Fils Fleurie Alex et Benoit Moreau' — the cuvée name on a Bernard Moreau bottling.
- **Chrome URL:** https://www.ploc.co/observintoire/vins/domaine-bernard-moreau-et-fils-fleurie-alex-et-benoit-moreau-2019-rouge-2364d
- **Sprint 7 flag:** `moreau_family_5_row_cleanup`

### core#59536 — Pascal Jolivet vs Jolivet

- **Original:** MERGE (cluster 11.4.h)
- **Final:** SKIP
- **Survivor change:** `Pascal Jolivet` → `None`
- **Source:** subagent_rechrome
- **Reasoning:** Domaine Jolivet (side_b) is Bastien Jolivet's Saint-Joseph (Rhone, Saint-Jean-de-Muzols) estate, founded 2014, imported by Kermit Lynch. Pascal Jolivet (side_a) is a Sancerre/Pouilly-Fume Loire producer. Different estates, different regions, shared surname only.
- **Chrome evidence:** Our latest discovery from Saint-Joseph comes from the talented hand of young Bastien Jolivet, working his family's vineyards in the hamlet of Saint-Jean-de-Muzols (Kermit Lynch)
- **Chrome URL:** https://kermitlynch.com/grower/domaine-jolivet

### core#61689 — Cordella vs Fabio Cordella

- **Original:** MERGE (cluster 11.4.h)
- **Final:** SKIP
- **Survivor change:** `Fabio Cordella` → `None`
- **Source:** subagent_rechrome
- **Reasoning:** Cordella Winery Montalcino (side_a) is Maddalena Cordella's 9-hectare Brunello estate, part of Consorzio del Brunello. Fabio Cordella (side_b) is a Puglia-based producer doing football-themed custom wines (Ronaldinho R One, Roberto Carlos, Wesley Sneijder, Buffon, Seba Frey). Side_b's Brunello labels are celebrity-themed bottlings, not the Cordella estate. Different producers, shared surname. §11.
- **Chrome evidence:** Maddalena Cordella runs this tiny production estate — crafting brilliant Brunello di Montalcino from only 9 hectares of sangiovese grosso (sete.wine). Fabio Cordella is a separate Puglia producer.
- **Chrome URL:** https://sete.wine/producer/cordella/

### core#62908 — Beausejour vs Beau-Sejour Becot

- **Original:** MERGE (cluster 11.4.h)
- **Final:** SKIP
- **Survivor change:** `Beau-Sejour Becot` → `None`
- **Source:** manual_b6_6
- **Reasoning:** Chrome re-validation: 'Beausejour' is an extremely common French château name spanning ≥10 distinct estates (Fronsac, Puisseguin-SE ×2, Montagne-SE, Saint-Estèphe, Pomerol, Chinon, Touraine, Crozes-Hermitage). 'Croix de Beausejour' is Duffau-Lagarrosse's second wine, not Bécot's. The losing row wines span 6+ unrelated estates.
- **Chrome evidence:** Wine-Searcher lists Ch. Beau-Séjour Bécot + Ch. Beausejour Duffau-Lagarrosse + Ch. Haut-Beausejour + ≥7 other distinct Beausejour estates
- **Chrome URL:** https://www.wine-searcher.com/find/beausejour
- **Sprint 7 flag:** `beausejour_row_needs_per_wine_split`

### core#102676 — Eric de Suremain vs de Suremain

- **Original:** MERGE (cluster 11.4.h)
- **Final:** SKIP
- **Survivor change:** `Eric de Suremain` → `None`
- **Source:** subagent_rechrome
- **Reasoning:** Domaine de Suremain (side_b) is Yves+Loic de Suremain's MERCUREY estate (Chateau du Bourgneuf, 7 generations, 18ha). Eric de Suremain (side_a) is a different branch, MONTHELIE (Chateau de Monthelie). Shared family name but distinct estates in different appellations. §11.4.a shared-surname split.
- **Chrome evidence:** Le Domaine de Suremain est une propriete familiale bourguignonne vieille de sept generations (Mercurey). Loic de Suremain, with the help of his parents Yves and Marie-Helene, manages the estate's 18 h
- **Chrome URL:** https://domaine-de-suremain.com

### core#110317 — Lafage vs La Fage

- **Original:** MERGE (cluster 11.4.h)
- **Final:** SKIP
- **Survivor change:** `Lafage` → `None`
- **Source:** subagent_rechrome
- **Reasoning:** Lafage (side_a) is Domaine Lafage in Roussillon (Perpignan) — huge Cotes du Roussillon/Catalanes portfolio. 'La Fage' single wine (side_b) is 'Chateau La Fage, Merlot, Bergerac' — Bergerac is southwest France (near Bordeaux, 400+km from Roussillon). Merlot is not a Roussillon variety. Different estate, shared-letters coincidence. §11.4.a.
- **Chrome evidence:** Lafage portfolio is all Cotes du Roussillon/Catalanes. Chateau La Fage Bergerac is a distinct Merlot estate.

### core#113145 — Passopisciaro vs Santo Spirito Passopisciaro

- **Original:** MERGE (cluster 11.4.s)
- **Final:** SKIP
- **Survivor change:** `Passopisciaro` → `None`
- **Source:** subagent_rechrome
- **Reasoning:** CRITICAL FLIP: 'Santo Spirito Passopisciaro' is NOT a cuvee of Passopisciaro (Andrea Franchetti's winery). It is 'Contrada Santo Spirito di Passopisciaro', owned by Antonio Moretti Cuseri (Tenuta Sette Ponti Tuscany). Separate Etna estate with its own Animalucente/Animardente bottlings. Both happen to have vineyards in the Contrada Santo Spirito cru.
- **Chrome evidence:** Contrada Santo Spirito is the latest project from Antonio Moretti Cuseri, the owner of the world class Tenuta Sette Ponti and Podere Orma in Tuscany and Feudo Maccari in Noto, Sicily.
- **Chrome URL:** https://nobleselection.kork.ca/en/our-wineries/santo-spirito-di-passopisciaro/etna-animalucente/2014

### core#117740 — Erath vs Bishop Creek Cellars

- **Original:** PARENT_CHILD (cluster 11.4.s)
- **Final:** SKIP
- **Parent change:** `Erath` → `None`
- **Source:** subagent_rechrome
- **Reasoning:** CRITICAL FLIP: Bishop Creek Cellars is an independent Oregon winery in Newberg with a 15-acre estate vineyard in Yamhill-Carlton — not a sub-label of Erath. Erath has a wine 'Erath Bishop Creek Pinot Noir' that uses fruit from Bishop Creek VINEYARD but the Bishop Creek Cellars winery is a separate producer (Facebook/CellarTracker confirm independent winery). Coincidence of vineyard and winery nami
- **Chrome evidence:** Bishop Creek Cellars, Newberg. 85 likes. Bishop Creek has a 15 acre estate vineyard in the Yamhill-Carlton District of the Willamette Valley. We make premium Pinot Noirs and Pinot Gris.
- **Chrome URL:** https://www.facebook.com/people/Bishop-Creek-Cellars/100069147255898/

### core#142528 — Chalk Hill vs Chalk Hill

- **Original:** MERGE (cluster 11.4.h)
- **Final:** SKIP
- **Survivor change:** `Chalk Hill` → `None`
- **Source:** subagent_rechrome
- **Reasoning:** CRITICAL FLIP: Chalk Hill Estate (Sonoma CA, William P. Foley II, Chalk Hill AVA) and Chalk Hill Wines (McLaren Vale SA, Harvey family, 6-generation estate since 1964) are COMPLETELY DIFFERENT producers. No ownership link. Different AOPs, different owners, different countries. §11.4.a shared-name coincidence.
- **Chrome evidence:** Chalk Hill Wines is a proudly family-owned winery with a history spanning six generations in McLaren Vale. The Harvey family has farmed the region's land since 1839 (mclarenvalecellars.com).
- **Chrome URL:** https://www.chalkhillwines.com.au

### core#147297 — Romuald Petit vs Molozay Chateau de Vaux

- **Original:** MERGE (cluster 11.1)
- **Final:** SKIP
- **Survivor change:** `Chateau de Vaux` → `None`
- **Source:** subagent_rechrome
- **Reasoning:** CRITICAL FLIP: Molozay Chateau de Vaux is in MOSELLE (near Metz), France — Norbert Molozay took over in 1999, relocated to Scy-Chazelles 2021. Completely different producer from Romuald Petit, who is a Burgundy producer in Mâcon/Saint-Véran/Morgon. The 'Les Gryphees' name overlap is coincidental (and Gryphees is a Saint-Véran single-vineyard of a different producer, Roger Lasserrat; Molozay has a
- **Chrome evidence:** Chateau de Vaux is an estate taken over in 1999 by flying winemaker Norbert Molozay and his wife Marie-Genevieve... relocated from its initial site in Vaux to the Villa Chazelles (in the town of Scy-C
- **Chrome URL:** https://viamosel.com/en/area/domaine-chateau-de-vaux/

### mid#2 — La Rousselle vs Rousselle

- **Original:** MERGE (cluster 11.4.h)
- **Final:** SKIP
- **Survivor change:** `La Rousselle` → `None`
- **Source:** subagent_rechrome
- **Reasoning:** Chateau La Rousselle is a Fronsac estate; Chateau Rousselle is a Cotes de Bourg estate. These are two unrelated Bordeaux chateaux with similar names but different appellations — the classic generic-chateau-name FP pattern. Original MERGE incorrectly conflated them.
- **Chrome evidence:** Chateau La Rousselle Fronsac (TheWineCellarInsider); Chateau Rousselle Cotes de Bourg are separate entries; no relationship found.
- **Chrome URL:** https://www.thewinecellarinsider.com/bordeaux-wine-producer-profiles/bordeaux/satellite-appellations/chateau-la-rousselle-fronsac-bordeaux-wine/

### mid#355 — Patrice Cacheux vs Rene Cacheux

- **Original:** MERGE (cluster 11.4.f)
- **Final:** SKIP
- **Survivor change:** `Cacheux` → `None`
- **Source:** subagent_rechrome
- **Reasoning:** Patrice Cacheux is the son of Jacques Cacheux (Domaine Jacques Cacheux & Fils), a different Vosne-Romanee Cacheux estate than Domaine Rene Cacheux & Fils. Two distinct Cacheux domaines in the same village — shared-surname family split, not father-son succession at the same estate.
- **Chrome evidence:** Banville Wine: "After he retired in 1994, his son Patrice took over as the fourth generation member of the family working at the estate" — referring to Jacques Cacheux estate. Rene Cacheux is a separa
- **Chrome URL:** https://www.banvillewine.com/pdf/en/producer/35-domaine-jacques-cacheux-fils.pdf

### mid#17186 — Audrey Brocard vs Brocard Pierre

- **Original:** MERGE (cluster 11.4.h)
- **Final:** SKIP
- **Survivor change:** `Brocard Pierre` → `None`
- **Source:** subagent_rechrome
- **Reasoning:** Audrey Brocard = young Champagne house in Trelou-sur-Marne, Vallee de la Marne (founded 2016, Pinot Meunier focus). Brocard Pierre = Thibaud Brocard in Celles-sur-Ource, Cote des Bar (Aube). Different regions, different people — classic shared-surname split. Brocard is a common Champagne surname.
- **Chrome evidence:** Audrey Brocard: "young winemaker from Chassins, a small village in the Vallee de la Marne". Brocard Pierre: "Thibaud Brocard is now in charge of the family heritage... Celles-sur-Ource in the Cote des
- **Chrome URL:** https://www.champagnebrocardpierre.fr/en/

### mid#27972 — Bosio vs Luca Bosio

- **Original:** MERGE (cluster 11.4.h)
- **Final:** SKIP
- **Survivor change:** `Luca Bosio` → `None`
- **Source:** subagent_rechrome
- **Reasoning:** A side lists Franciacorta wines (Lombardia) including Girolamo Bosio Pas Dose Riserva — a distinct Franciacorta producer. Luca Bosio (B side) = Bosio Family Estates in Langhe Piedmont, producing Barbera/Barolo/Barbaresco. Different regions, different brands.
- **Chrome evidence:** Luca Bosio Wine: third generation, center of the Langhe region of Piedmont. DB A side lists Franciacorta wines.
- **Chrome URL:** https://lucabosiowines.com

### mid#31312 — Carlo Boffa vs Boffa Nello

- **Original:** MERGE (cluster 11.4.h)
- **Final:** SKIP
- **Survivor change:** `Carlo Boffa` → `None`
- **Source:** subagent_rechrome
- **Reasoning:** Carlo Boffa e Figli is a distinct Barbaresco producer; Nello Boffa (Boffa Nello) is a separate Barolo producer. Shared-surname split.
- **Chrome evidence:** Carlo Boffa makes Barbaresco; Nello Boffa Barolo — distinct producers.
- **Chrome URL:** https://www.wine-searcher.com/find/carlo+boffa

### mid#41728 — Bastida vs Familia Bastida

- **Original:** MERGE (cluster 11.4.h)
- **Final:** SKIP
- **Survivor change:** `Familia Bastida` → `None`
- **Source:** subagent_rechrome
- **Reasoning:** Bodegas Bastida operates in Jumilla (Talma line). Familia Bastida wines are La Mancha. Different regions — likely different producers. Bastida is a common Spanish surname.
- **Chrome evidence:** A wines in Jumilla, B wines in La Mancha — no overlap.
- **Chrome URL:** https://www.wine-searcher.com/find/bastida

### mid#43616 — Tenuta Brunelli vs Brunelli

- **Original:** MERGE (cluster 11.4.h)
- **Final:** SKIP
- **Survivor change:** `Brunelli` → `None`
- **Source:** subagent_rechrome
- **Reasoning:** Tenuta Brunelli makes Brunello di Montalcino (Martoccia). The other Brunelli side has Amarone della Valpolicella — distinct Valpolicella producer. Two unrelated Italian producers with same surname.
- **Chrome evidence:** A wines Montalcino; B has Amarone Classico + Campo Titari — Brunelli Valpolicella estate.
- **Chrome URL:** https://www.wine-searcher.com/find/brunelli+valpolicella

### mid#47685 — Jean-Marc Brignot et Anders Frederik Steen vs Anders Frederik Steen et Anne Bruun Blauert

- **Original:** MERGE (cluster 11.4.f)
- **Final:** SKIP
- **Survivor change:** `Anders Frederik Steen` → `None`
- **Source:** subagent_rechrome
- **Reasoning:** Distinct natural-wine collaboration labels: A = Brignot & Steen; B = Steen & Blauert. Different partnerships = different brands per §11.4 collaboration rule.
- **Chrome evidence:** Collaboration partnerships are distinct brands.
- **Chrome URL:** https://www.wine-searcher.com/find/anders+frederik+steen

### mid#49717 — Laborde vs de Laborde

- **Original:** MERGE (cluster 11.4.h)
- **Final:** SKIP
- **Survivor change:** `de Laborde` → `None`
- **Source:** subagent_rechrome
- **Reasoning:** Chateau Laborde = Lalande de Pomerol/Haut-Medoc (Bordeaux). Chateau de Laborde = Chambolle-Musigny/Gevrey-Chambertin (Burgundy). Different regions — unrelated estates sharing common surname.
- **Chrome evidence:** A wines Bordeaux; B wines Cote de Nuits Burgundy. No overlap.
- **Chrome URL:** https://www.wine-searcher.com/find/chateau+de+laborde

### mid#54026 — Boisson vs Anne Boisson

- **Original:** MERGE (cluster 11.4.h)
- **Final:** SKIP
- **Survivor change:** `Anne Boisson` → `None`
- **Source:** manual_b6_6
- **Reasoning:** Chrome re-validation: 'Boisson' row is Domaine Boisson Cairanne (Rhône) + Château Boisson (Bordeaux). Anne Boisson is Meursault, Burgundy — one of three Meursault Boisson domaines (§11.4.m sibling split with Pierre Boisson and Boisson-Vadot). Zero overlap between rows.
- **Chrome evidence:** Anne Boisson daughter of Bernard Boisson-Vadot, 1.5ha of 8.5ha family Meursault estate; sibling Pierre Boisson runs a separate 3.5ha portion.
- **Chrome URL:** https://pleasurewine.com/en/brand/82-domaine-anne-boisson

### mid#78125 — Giovanni Giordano vs Luigi Giordano

- **Original:** MERGE (cluster 11.4.f)
- **Final:** SKIP
- **Survivor change:** `Luigi Giordano` → `None`
- **Source:** subagent_rechrome
- **Reasoning:** Luigi Giordano is the well-known Barbaresco Asili producer. Giovanni Giordano (Cavanna Riserva) is a different Giordano producer. Shared-surname split — common Piemonte surname.
- **Chrome evidence:** Luigi Giordano famous for Asili Barbaresco; Giovanni Giordano Cavanna is distinct producer.
- **Chrome URL:** https://www.wine-searcher.com/find/luigi+giordano+barbaresco

### mid#107981 — Confuron Gindre vs Edouard Confuron

- **Original:** MERGE (cluster 11.4.f)
- **Final:** SKIP
- **Survivor change:** `Confuron-Gindre` → `None`
- **Source:** subagent_rechrome
- **Reasoning:** Confuron is one of the most split families in Vosne-Romanee (Confuron-Cotetidot, Jean-Jacques Confuron, Confuron-Gindre, Edouard Confuron). Confuron-Gindre and Edouard Confuron are separate Confuron domaines. Shared-surname split.
- **Chrome evidence:** Confuron family has multiple independent Vosne domaines.
- **Chrome URL:** https://www.wine-searcher.com/find/confuron

### tail#12249 — Gauffroy Marc & Fils vs Gauffroy-Jacob

- **Original:** MERGE (cluster 11.4.h)
- **Final:** SKIP
- **Survivor change:** `Gauffroy Marc & Fils` → `None`
- **Source:** subagent_rechrome
- **Reasoning:** Gauffroy Marc & Fils (Chassagne Morgeot) and Maison Gauffroy-Jacob (Bourgogne Blanc) are likely different Gauffroy houses. Shared-surname split; no direct evidence linking them.
- **Chrome evidence:** Gauffroy is a Burgundy surname; Gauffroy-Jacob appears as a distinct negociant.
- **Chrome URL:** https://www.wine-searcher.com/find/gauffroy

### tail#18763 — Vinyes Terrer vs Vins de Terrer

- **Original:** MERGE (cluster 11.4.h)
- **Final:** SKIP
- **Survivor change:** `Vinyes del Terrer` → `None`
- **Source:** subagent_rechrome
- **Reasoning:** Vinyes Terrer (Tarragona, Terrer d Aubert) vs Vins de Terrer (Penedes). Different Catalan regions and names — Terrer means terroir in Catalan, common toponym. Likely unrelated.
- **Chrome evidence:** Vinyes Terrer Tarragona; Vins de Terrer Penedes — different DOs.
- **Chrome URL:** https://www.wine-searcher.com/find/vinyes+terrer

### tail#21177 — La Tour du Pin vs Tour du Pin Figeac

- **Original:** MERGE (cluster 11.4.h)
- **Final:** SKIP
- **Survivor change:** `La Tour du Pin Figeac` → `None`
- **Source:** subagent_rechrome
- **Reasoning:** Chateau La Tour du Pin (absorbed into Cheval Blanc in 2006) and Chateau Tour du Pin Figeac (a separate Figeac-neighbor chateau, itself split into Moueix/Giraud-Belivier branches) are different Saint-Emilion estates. Famous Bordeaux shared-name trap.
- **Chrome evidence:** Cheval Blanc absorbed La Tour du Pin; Tour du Pin Figeac was a separate chateau.
- **Chrome URL:** https://en.wikipedia.org/wiki/Ch%C3%A2teau_La_Tour_du_Pin

### tail#32416 — Thibault Ligier Belair vs Liger-Belair S.A.

- **Original:** MERGE (cluster 11.4.h)
- **Final:** SKIP
- **Survivor change:** `Thibault Liger-Belair` → `None`
- **Source:** subagent_rechrome
- **Reasoning:** Thibault Liger-Belair (Nuits-St-Georges domaine) vs Liger-Belair S.A. (Mercurey negociant arm) are distinct entities. The Liger-Belair name has multiple unrelated businesses. Thibault is separate from Comte (Louis-Michel) too.
- **Chrome evidence:** Thibault Liger-Belair is a distinct Nuits-St-Georges domaine.
- **Chrome URL:** https://www.thibaultligerbelair.com

### tail#75959 — Emilian Gillet vs Gillet

- **Original:** MERGE (cluster 11.4.h)
- **Final:** SKIP
- **Survivor change:** `Emilian Gillet` → `None`
- **Source:** subagent_rechrome
- **Reasoning:** Domaine Emilian Gillet = Vire-Clesse/Maconnais (Jean Thevenet's estate). Chateau Gillet = Bordeaux Blanc/Rouge — different producer entirely. Shared-surname + generic chateau name trap.
- **Chrome evidence:** Emilian Gillet is famous Jean Thevenet Macon; Chateau Gillet is a Bordeaux chateau — unrelated.
- **Chrome URL:** https://www.wine-searcher.com/find/emilian+gillet

### tail#98847 — Gaspare Buscemi vs Buscemi

- **Original:** MERGE (cluster 11.4.h)
- **Final:** SKIP
- **Survivor change:** `Gaspare Buscemi` → `None`
- **Source:** subagent_rechrome
- **Reasoning:** Gaspare Buscemi is a Friuli/Venezia Giulia producer (Alture, Braide). Buscemi on B side is a Sicilian producer (Terre Siciliane, Tartaraci). Different regions entirely — Buscemi is common Italian surname.
- **Chrome evidence:** Gaspare Buscemi Friuli; Buscemi (Sicily) = distinct Sicilian natural wine producer.
- **Chrome URL:** https://www.wine-searcher.com/find/buscemi

## FLIP_DIRECTION (1)

### mid#43596 — Comtesse de Cherisey vs Martelet de Cherisey

- **Original:** MERGE (cluster 11.4.h)
- **Final:** MERGE
- **Survivor change:** `Martelet de Cherisey` → `Comtesse de Cherisey`
- **Source:** manual_b6_6
- **Reasoning:** Chrome re-validation: same estate, flip direction. Jasper Morris confirms 'The domaine formerly known as Martelet de Cherisey is now officially the Domaine Comtesse de Cherisey.' Comtesse is current canonical name (the 9-wine row); Martelet is the historical form. Chrome originally chose Martelet as survivor — backwards.
- **Chrome evidence:** 'The domaine formerly known as Martelet de Cherisey is now officially the Domaine Comtesse de Cherisey but is subtitled Hélène et Laurent Martelet.'
- **Chrome URL:** https://insideburgundy.com/overview/domaine-comtesse-de-cherisey

## FLIP_TO_MERGE (2)

### core#71929 — Franck Massard vs Epicure (Franck Massard)

- **Original:** PARENT_CHILD (cluster 11.4.p)
- **Final:** MERGE
- **Survivor change:** `None` → `Franck Massard`
- **Parent change:** `Franck Massard` → `None`
- **Source:** subagent_rechrome
- **Reasoning:** Epicure Wines is Franck Massard's distribution company ('empresa distribuidora'), NOT a sub-brand. CellarTracker/Vivino lists 'Alma' Albarino under Franck Massard directly. All 3 side_b wines (Alma, Licis, Audacia) are Massard's Galician projects. Per §11.4.p merchant-prefix merge into actual producer.
- **Chrome evidence:** Franck Massard es el fundador y propietario de Epicure Wines (empresa distribuidora). 2015 Franck Massard Albariño 'Alma' (CellarTracker).
- **Chrome URL:** https://franckmassard.com/equipo/

### mid#10596 — Lombardi vs Tendil & Lombardi

- **Original:** PARENT_CHILD (cluster 11.4.o)
- **Final:** MERGE
- **Survivor change:** `None` → `Tendil & Lombardi`
- **Parent change:** `Lombardi` → `None`
- **Source:** subagent_rechrome
- **Reasoning:** "Lombardi Anthese Brut", "Lombardi Hymenee" appear as retail shorthand for Tendil & Lombardi Champagne (founded 2007 by Laurent Tendil & Stephane Lombardi). Same Hymenee cuvee on both sides. The "Lombardi" entry is a truncated form of the same brand, not a standalone producer. MERGE not PC.
- **Chrome evidence:** Wine-Searcher: "Lombardi Anthese Brut Rose, Champagne, France" — same cuvee structure as Tendil & Lombardi product line.
- **Chrome URL:** https://www.wine-searcher.com/find/lombardy+anthese+brut+rose+champagne+france

## FLIP_TO_PC (1)

### mid#9707 — Tuck Beckstoffer vs The 75 Wine Company (by Tuck Beckstoffer)

- **Original:** MERGE (cluster 11.1)
- **Final:** PARENT_CHILD
- **Survivor change:** `Tuck Beckstoffer` → `None`
- **Parent change:** `None` → `Tuck Beckstoffer`
- **Source:** subagent_rechrome
- **Reasoning:** The 75 Wine Company is a distinct on-label brand made by Tuck Beckstoffer. Per §11.1 brand-on-label rule these are distinct producers — PC with Tuck Beckstoffer as parent is the correct verdict, not MERGE.
- **Chrome evidence:** "The 75 Wine Company (by Tuck Beckstoffer)" appears as label-branded across retailers and auction houses.
- **Chrome URL:** https://wineauction.ai/wine/the-75-wine-company-by-tuck-beckstoffer-sauvignon-blanc-california

## NEEDS_HUMAN_REVIEW (5)

### core#103518 — Clavelier vs Clavelier et Fils

- **Original:** MERGE (cluster 11.4.h)
- **Final:** DEFERRED_SPRINT_7
- **Survivor change:** `Clavelier et Fils` → `None`
- **Source:** subagent_rechrome
- **Reasoning:** Ambiguous: side_a 'Clavelier' Meursault/Aloxe/Santenay and side_b 'Clavelier et Fils' Gevrey grands crus + Beaune. Bruno Clavelier (Vosne-Romanee) doesn't make most of these wines. May be merchant 'Bruno Clavelier Vins & Millesimes' bottlings or a historical Clavelier family label. Keeping MERGE tentatively but flagging for human review.
- **Chrome evidence:** Bruno Clavelier Vins & Millesimes (merchant) sells Clavelier et Fils Beaune Greves 1980. Bruno Clavelier is also the name of a Vosne-Romanee vigneron.
- **Chrome URL:** https://www.wine-searcher.com/find/clavellier+greves+premier+cru+beaune+les+cote+de+burgundy+france

### core#115931 — Starside vs Two Vintners

- **Original:** PARENT_CHILD (cluster 11.4.s)
- **Final:** DEFERRED_SPRINT_7
- **Parent change:** `Two Vintners` → `None`
- **Source:** subagent_rechrome
- **Reasoning:** Starside is made by Morgan Lee (Two Vintners winemaker) but labeled as house wine for Full Pull Wines retailer. Alternative classification: SKIP (retailer house wine, not a Two Vintners sub-brand). Keeping PC tentatively since same winemaker produces it.
- **Chrome evidence:** Starside is our audacious effort to craft... $20 Washington Cab (Full Pull Wines quote, Morgan Lee of Two Vintners makes it). Vinous labels it 'Block Wines - Starside'.
- **Chrome URL:** https://vinous.com/wines/block-wines-starside-cabernet-sauvignon/2023

### core#141176 — Barons de Rothschild (Lafite) vs Barons de Rothschild

- **Original:** MERGE (cluster 11.4.h)
- **Final:** DEFERRED_SPRINT_7
- **Survivor change:** `Barons de Rothschild (Lafite)` → `None`
- **Source:** subagent_rechrome
- **Reasoning:** 'Domaines Barons de Rothschild (Lafite), Malbec' (AR) is likely the Caro JV (DBR + Catena, labeled as Bodegas Caro typically). If wine is actually labeled DBR Lafite alone, MERGE is correct; if JV label says 'Caro' or 'Rothschild+Catena', should be PC under pair 142038's Catena-Rothschild instead. Flag for human review.
- **Chrome evidence:** Caro is the well-known DBR+Catena JV in Mendoza.

### mid#4058 — Minuto Flor vs Minuto

- **Original:** MERGE (cluster 11.4.h)
- **Final:** DEFERRED_SPRINT_7
- **Survivor change:** `Fratelli Minuto` → `None`
- **Source:** subagent_rechrome
- **Reasoning:** Minuto Flor is attested only on 1939/1973/1985 Barbaresco bottles (pre-modern). Fratelli Minuto is a modern Barbaresco/Barolo producer (Moccagatta family, acquired 1913). Plausibly same family with a historic sub-label "Flor", but no direct source connects them. Ambiguous.
- **Chrome evidence:** Fratelli Minuto: "Familie Minuto, die die Leitung des 1913 von Sergio Minuto erworbenen Weinguts bis heute in..." (Bacchus Vinothek). Minuto Flor: only 1939-1985 vintages on CellarTracker.
- **Chrome URL:** https://www.bacchus-vinothek.de/weingut/fratelli-minuto/

### tail#114856 — Austin vs Quest

- **Original:** PARENT_CHILD (cluster 11.4.s)
- **Final:** DEFERRED_SPRINT_7
- **Parent change:** `Austin Hope` → `None`
- **Source:** subagent_rechrome
- **Reasoning:** Austin Hope winery has Quest as a cuvee; but Austin (Barrel No. 19/21) and Quest are both Paso Robles Cab labels with unclear relationship. Could be same producer with two brand lines or two separate producers.
- **Chrome evidence:** Austin Hope Winery makes Quest; Austin by itself is ambiguous.
- **Chrome URL:** https://www.austinhope.com
