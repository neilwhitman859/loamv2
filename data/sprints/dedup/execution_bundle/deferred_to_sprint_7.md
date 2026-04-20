# Deferred to Sprint 7

Pairs explicitly not resolved in this bundle, with rationale.
For systemic issues (schema changes, multi-entity JVs, etc.), see `open_questions.md`.

## Pair-level deferrals: 7

| ledger_key | cluster | verdict | reason |
|---|---|---|---|
| core#57771 | 11.4.o | SKIP | moreau_family_5_row_cleanup |
| core#62908 | 11.4.h | SKIP | beausejour_row_needs_per_wine_split |
| core#103518 | 11.4.h | DEFERRED_SPRINT_7 | needs_human_review |
| core#115931 | 11.4.s | DEFERRED_SPRINT_7 | needs_human_review |
| core#141176 | 11.4.h | DEFERRED_SPRINT_7 | needs_human_review |
| mid#4058 | 11.4.h | DEFERRED_SPRINT_7 | needs_human_review |
| tail#114856 | 11.4.s | DEFERRED_SPRINT_7 | needs_human_review |

## Detail per deferred pair

### core#57771 — Alex Moreau vs Alex et Benoit Moreau

- Cluster: `11.4.o`
- Original verdict: `PARENT_CHILD`
- Final verdict: `SKIP`
- Flag: `moreau_family_5_row_cleanup`
- Reasoning: Chrome re-validation: 'Alex et Benoit Moreau' is a single-wine cuvée of Domaine Bernard Moreau et Fils (father's Chassagne estate, separate 11w DB row), not a collab producer. Alex and Benoît are sibling winemakers with distinct DB rows (§11.4.m). Making the cuvée a child of Alex alone is factually wrong — it's equally Benoît's, and structurally a wine of Bernard Moreau et Fils per §11.4.e.
- Chrome URL: https://www.ploc.co/observintoire/vins/domaine-bernard-moreau-et-fils-fleurie-alex-et-benoit-moreau-2019-rouge-2364d

### core#62908 — Beausejour vs Beau-Sejour Becot

- Cluster: `11.4.h`
- Original verdict: `MERGE`
- Final verdict: `SKIP`
- Flag: `beausejour_row_needs_per_wine_split`
- Reasoning: Chrome re-validation: 'Beausejour' is an extremely common French château name spanning ≥10 distinct estates (Fronsac, Puisseguin-SE ×2, Montagne-SE, Saint-Estèphe, Pomerol, Chinon, Touraine, Crozes-Hermitage). 'Croix de Beausejour' is Duffau-Lagarrosse's second wine, not Bécot's. The losing row wines span 6+ unrelated estates.
- Chrome URL: https://www.wine-searcher.com/find/beausejour

### core#103518 — Clavelier vs Clavelier et Fils

- Cluster: `11.4.h`
- Original verdict: `MERGE`
- Final verdict: `DEFERRED_SPRINT_7`
- Reasoning: Ambiguous: side_a 'Clavelier' Meursault/Aloxe/Santenay and side_b 'Clavelier et Fils' Gevrey grands crus + Beaune. Bruno Clavelier (Vosne-Romanee) doesn't make most of these wines. May be merchant 'Bruno Clavelier Vins & Millesimes' bottlings or a historical Clavelier family label. Keeping MERGE tentatively but flagging for human review.
- Chrome URL: https://www.wine-searcher.com/find/clavellier+greves+premier+cru+beaune+les+cote+de+burgundy+france

### core#115931 — Starside vs Two Vintners

- Cluster: `11.4.s`
- Original verdict: `PARENT_CHILD`
- Final verdict: `DEFERRED_SPRINT_7`
- Reasoning: Starside is made by Morgan Lee (Two Vintners winemaker) but labeled as house wine for Full Pull Wines retailer. Alternative classification: SKIP (retailer house wine, not a Two Vintners sub-brand). Keeping PC tentatively since same winemaker produces it.
- Chrome URL: https://vinous.com/wines/block-wines-starside-cabernet-sauvignon/2023

### core#141176 — Barons de Rothschild (Lafite) vs Barons de Rothschild

- Cluster: `11.4.h`
- Original verdict: `MERGE`
- Final verdict: `DEFERRED_SPRINT_7`
- Reasoning: 'Domaines Barons de Rothschild (Lafite), Malbec' (AR) is likely the Caro JV (DBR + Catena, labeled as Bodegas Caro typically). If wine is actually labeled DBR Lafite alone, MERGE is correct; if JV label says 'Caro' or 'Rothschild+Catena', should be PC under pair 142038's Catena-Rothschild instead. Flag for human review.

### mid#4058 — Minuto Flor vs Minuto

- Cluster: `11.4.h`
- Original verdict: `MERGE`
- Final verdict: `DEFERRED_SPRINT_7`
- Reasoning: Minuto Flor is attested only on 1939/1973/1985 Barbaresco bottles (pre-modern). Fratelli Minuto is a modern Barbaresco/Barolo producer (Moccagatta family, acquired 1913). Plausibly same family with a historic sub-label "Flor", but no direct source connects them. Ambiguous.
- Chrome URL: https://www.bacchus-vinothek.de/weingut/fratelli-minuto/

### tail#114856 — Austin vs Quest

- Cluster: `11.4.s`
- Original verdict: `PARENT_CHILD`
- Final verdict: `DEFERRED_SPRINT_7`
- Reasoning: Austin Hope winery has Quest as a cuvee; but Austin (Barrel No. 19/21) and Quest are both Paso Robles Cab labels with unclear relationship. Could be same producer with two brand lines or two separate producers.
- Chrome URL: https://www.austinhope.com

---

See `open_questions.md` for the systemic Sprint 7 agenda (schema changes, family cleanups, etc.)
