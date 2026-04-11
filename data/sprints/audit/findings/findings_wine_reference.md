# Wine Expert — Reference Content Audit — Findings

**Session:** S2.4
**Date:** 2026-04-11
**Expert:** wine_reference
**Method:** Opus 4.6 inline + WebFetch primary-source verification (per ratified S2.3 pattern). $0 actual project spend.
**Scope:** content correctness of `appellation_rules` (1,165), `appellation_grapes` (10,414), `grape_synonyms` (34,809), `varietal_categories` (161), `grapes` parent/child (9,695), `appellation_soils` (930), `soil_types` (39), `tasting_descriptors` (304), `farming_certifications` (21), `biodiversity_certifications` (7).

## Summary

- **Total findings:** 30
- **P0:** 8 · **P1:** 14 · **P2:** 7 · **P3:** 1
- **Biggest risks:** (F1) varietal_categories has confirmed wrong-grape links on Merlot, Riesling, Verdejo, Greco, and St. Laurent — the 161-row categorization table routes five common varieties to completely wrong canonical grapes. (F2) Root cause of S2.3 F2 (Chardonnay/Pinot Blanc bug) identified: `grape_synonyms` contains `PINOT CHARDONNAY`, `CHARDONNET PINOT BLANC`, and `PINOT BLANC CHARDONNET` as VIVC synonyms of PINOT BLANC, plus `PINOT GRIGIO` — guaranteeing that any name-based resolver will match "Chardonnay" to both CHARDONNAY BLANC (correct) and PINOT BLANC (wrong). (F3) `appellations.name` holds slash-concatenated alias lists for 121 famous appellations ("Hermitage / Ermitage / L'Hermitage / L'Ermitage", "Porto / Port", "Clos de Vougeot / Clos Vougeot"), breaking UI display, grape linkage, and wine matching. (F4) 240 French AOCs and 105 Italian DOCs share a fake `established_year=1973` bulk-load default (Chambertin actual: 1936, Barolo DOC actual: 1966, Champagne actual: 1936). (F5) `appellation_rules.rules` JSONB has no stable schema — Petit Chablis uses `{"Chardonnay": 100}` while Chambertin uses `{"principal": "Pinot Noir"}`, and the same field lives under 3–5 different keys across rows.
- **Biggest wins:** source_url quality is actually good (81% primary-source coverage, only 2% Wikipedia), `farming_certifications` (21) and `biodiversity_certifications` (7) are clean, `soil_types` list (39) is accurate except for one junk row, and the reference layer's **provenance infrastructure** (source_organization, source_url, last_verified_at, source_text_excerpt columns) is well-designed — the problem is uneven content-load quality, not schema.
- **Connection to S2.3:** F2 identifies the grape_synonyms root cause for S2.3 F2. F8 corrects S2.3 F7 (GARRO is a real VIVC grape #7326, not invented — the error is in wine_grapes linkage, not the grapes table). F20 links S2.3 F14 "Hunter Valley volcanic" confabulation to a structured-data reinforcement — `appellation_soils` lists Basalt for Hunter Valley, which is not characteristic.

## Findings

---

## F1 — varietal_categories routes Merlot, Riesling, Verdejo, Greco, St. Laurent to wrong grapes

- **Severity:** P0
- **Evidence:**
  ```sql
  SELECT vc.name, g.name AS linked_grape
  FROM varietal_categories vc
  JOIN grapes g ON g.id = vc.grape_id
  WHERE vc.type = 'single_varietal'
    AND vc.name IN ('Merlot','Riesling','Verdejo','Greco','St. Laurent');
  ```
  Result:
  - `Merlot` → `GROLLEAU NOIR` (Grolleau Noir is a Loire grape, not Merlot)
  - `Merlot Rosé` → `GROLLEAU NOIR` (same error)
  - `Riesling` → `CROUCHEN` (Crouchen was historically called "Cape Riesling" in South Africa/Australia but is a completely different variety)
  - `Verdejo` → `TROUSSEAU NOIR` (Verdejo is the white Rueda grape; Trousseau Noir is a red Jura grape)
  - `Greco` → `ALBANA BIANCA` (Greco and Albana are related but distinct varieties)
  - `St. Laurent` → `MUSCAT ST. LAURENT` (St. Laurent is an Austrian red grape; Muscat St. Laurent is unrelated)
- **Why it matters:** varietal_categories is a 161-row curated table that likely powers UI grape-based filtering and wine categorization. Linking "Merlot" to Grolleau Noir means every Merlot wine categorized through this table gets routed to a Loire red grape canonical entry. A working sommelier reading "Merlot" categorized as Grolleau Noir in the UI would lose trust immediately. Riesling→Crouchen is worse because Crouchen wines are cheap bulk-style whites completely unlike fine Riesling.
- **Proposed fix:** hand-audit all 161 varietal_categories.grape_id entries. Build a primary-source canonical name list (Merlot, Riesling, Verdejo, Greco di Tufo, St. Laurent) and fix the grape_id references. Add a data test that asserts `vc.name LIKE '%' || g.name` (with accent/synonym tolerance).
- **Effort:** small (1–2 hours, 161 rows, most correct)
- **Dependencies:** none
- **Related findings:** F6 (suffix-polluted canonical grape names make this kind of pattern match harder)

---

## F2 — grape_synonyms root cause of S2.3 F2 (Chardonnay/Pinot Blanc bug): PINOT BLANC has "Chardonnay"-containing VIVC synonyms

- **Severity:** P0
- **Evidence:**
  ```sql
  SELECT synonym FROM grape_synonyms
  WHERE grape_id = (SELECT id FROM grapes WHERE name = 'PINOT BLANC')
    AND synonym ILIKE '%chardonnay%' OR synonym ILIKE '%chardonnet%' OR synonym ILIKE '%grigio%';
  ```
  PINOT BLANC's VIVC synonym list contains:
  - `PINOT CHARDONNAY`
  - `CHARDONNET PINOT BLANC`
  - `PINOT BLANC CHARDONNET`
  - `PINOT GRIGIO`
  - `P. GRIGIO`
  - `BURGUNDER WEISSER`, `WEISSBURGUNDER`, `Weißburgunder` (these are actually legitimate Pinot Blanc synonyms)
  - plus `KLEVNER`, `CLEVNER` (historically ambiguous — in modern Alsace "Klevner" refers to Pinot Blanc but in older German usage it was generic for several Burgundian whites)

  Any resolver looking for a grape whose synonyms or name contain "Chardonnay" will match BOTH:
  1. `CHARDONNAY BLANC` via synonym `CHARDONNAY`
  2. `PINOT BLANC` via synonyms `PINOT CHARDONNAY` / `CHARDONNET PINOT BLANC` / `PINOT BLANC CHARDONNET`

  This is the structural root cause of S2.3 F2 (2,743 of 2,809 Chardonnay-named wines have `PINOT BLANC` linked via wine_grapes).
- **Why it matters:** S2.3 F2 is one of the two biggest P0 findings of Sprint 2 (the other being the 286K staging orphans in S2.2 F1). It affects 97.6% of Chardonnay-named wines. Without fixing the synonym pollution, every re-run of the grape resolver will re-create the same bug.
- **Proposed fix:** Two-step.
  1. Delete the historical-misnomer synonyms from PINOT BLANC: `PINOT CHARDONNAY`, `CHARDONNET PINOT BLANC`, `PINOT BLANC CHARDONNET`. Also delete `PINOT GRIGIO` / `P. GRIGIO` (these belong to Pinot Gris, a botanically distinct variety).
  2. Audit the grape resolver in `pipeline/lib/resolve.py` or wherever wine-name → grape matching lives. Ensure it prefers exact primary-name matches over synonym matches, and that ties break toward the most common variety in the wine's country context. (This second step is S2.5 code scope — flag for hand-off.)
- **Effort:** medium (synonym deletes are trivial; resolver audit is half-day)
- **Dependencies:** F11 (primary-name collisions must also be resolved before resolver can be trusted)
- **Related findings:** S2.3 F2, F6 (CHARDONNAY BLANC canonical name), F11 (921 collisions)

---

## F3 — 121 famous appellations stored with slash-concatenated alias names in `appellations.name`

- **Severity:** P0
- **Evidence:**
  ```sql
  SELECT count(*) FROM appellations
  WHERE deleted_at IS NULL AND name LIKE '% / %';
  -- Returns: 121
  ```
  Examples surfaced while searching for famous appellations:
  - `Hermitage / Ermitage / L'Hermitage / L'Ermitage`
  - `Clos de Vougeot / Clos Vougeot`
  - `Porto / Port` (full slug: `porto-port-vinho-do-porto-port-wine-vin-de-porto-oporto-portvin-portwein-portwijn`)
  - `Priorat / Priorato`
  - `Crozes-Hermitage / Crozes-Ermitage`
  - `Moulis / Moulis-en-Médoc`
- **Why it matters:** Multiple downstream systems break on this. UI display shows users "Hermitage / Ermitage / L'Hermitage / L'Ermitage" instead of "Hermitage". Wine records that say "Hermitage" in text cannot exact-match the canonical name. Grape linkage queries for Porto fail because the canonical is "Porto / Port". The `region_aliases` and `appellation_aliases` tables exist specifically to hold alias information — it belongs there, not in the canonical name.
- **Proposed fix:** Sprint 3 script:
  1. For each row with ` / ` in `appellations.name`, split on " / ", take the first token as the new canonical name, move the remaining tokens to `appellation_aliases` with `alias_type='alternate_spelling'`.
  2. Rebuild `appellations.slug` from the cleaned name.
  3. Update search_vector.
  4. Do NOT delete the row — there are likely wine_ids pointing at it.
- **Effort:** small (one migration script, ~121 rows)
- **Dependencies:** none
- **Related findings:** F4 (missing diacritics on the same names)

---

## F4 — French AOC names missing diacritical marks

- **Severity:** P0
- **Evidence:**
  ```sql
  SELECT name FROM appellations
  WHERE deleted_at IS NULL
    AND (name ~ '[Ss]aint-[Ee]milion' AND name !~ 'É|é')
     OR (name ~ 'Echezeaux' AND name !~ '[éè]');
  ```
  Results:
  - `Saint-Emilion`, `Saint-Emilion Grand Cru`, `Lussac Saint-Emilion`, `Montagne-Saint-Emilion`, `Puisseguin Saint-Emilion`, `Saint-Georges-Saint-Emilion` (all should be Saint-Émilion per INAO)
  - `Echezeaux` (should be Échézeaux — Grand Cru Burgundy)
  - `Grands-Echezeaux` (should be Grands-Échézeaux — Grand Cru Burgundy)

  Count queries: 2 Échézeaux variants + 6 Saint-Émilion variants = 8 immediate cases, more exist on broader searches.
- **Why it matters:** French AOC names with diacritics are the official names per INAO. A certified sommelier reading "Echezeaux" on a wine page would notice immediately. This also breaks joins from wine records that do use the correct French accents ("Château Latour à Pomerol" vs "Chateau Latour a Pomerol"). Fuzzy matching partially hides this but failures happen at the margins.
- **Proposed fix:** build a known-accent map (INAO cahier des charges is the primary source) and a migration script that updates `appellations.name` + `slug` + `search_vector` for all French AOCs. Also add to `appellation_aliases` the non-accented forms for fuzzy matching.
- **Effort:** small (one-pass mapping, probably <50 rows)
- **Dependencies:** none
- **Related findings:** F3

---

## F5 — Pauillac 1855 classification summary has 3 of 5 tier counts wrong

- **Severity:** P0
- **Evidence:**
  ```sql
  SELECT rules->>'classification' FROM appellation_rules ar
  JOIN appellations a ON a.id = ar.appellation_id
  WHERE a.name = 'Pauillac';
  ```
  Result:
  > "1855 — includes 18 Classified Growths (1 Premier Cru: Lafite, Latour, Mouton; 2 Deuxième, 1 Troisième, 5 Quatrième, 12 Cinquième)"

  Actual Pauillac 1855 breakdown (verified against widely-known classification tables):
  - **3** Premier Crus: Lafite-Rothschild, Latour, Mouton-Rothschild (Mouton elevated in 1973). The rule JSON names 3 châteaux but calls them "1 Premier Cru" — **internally contradictory**.
  - **2** Deuxième Crus: Pichon-Longueville Baron + Pichon-Longueville Comtesse de Lalande. ✓ correct.
  - **0** Troisième Crus in Pauillac. The rule JSON claims "1 Troisième" — **wrong**.
  - **1** Quatrième Cru: Duhart-Milon. The rule JSON claims "5 Quatrième" — **wrong**.
  - **12** Cinquième Crus. ✓ correct.
  - **Total: 3 + 2 + 0 + 1 + 12 = 18.** Total is right, breakdown is wrong in 3 of 5 tiers.

  Source URL in the row is a real INAO cahier des charges PDF (`https://extranet.inao.gouv.fr/fichier/PNOCDCPauillac.pdf`), so this is a transcription/summary error, not a source quality issue.
- **Why it matters:** A sommelier reading Pauillac's Loam page would see a factually wrong tier count. Premier Cru Classé of Pauillac is a famous wine heritage — saying "1 Premier Cru" when there are 3 is a howler.
- **Proposed fix:** hand-edit the JSONB `classification` field. Better: normalize 1855 classification data into its own structured table (château × tier × appellation) so summaries can be computed from the data instead of hand-written.
- **Effort:** trivial (single row edit) or medium (structured 1855 table if that's the right path)
- **Dependencies:** none (structural fix enables richer Bordeaux content)
- **Related findings:** F7 (thin rule content on other Médoc AOCs)

---

## F6 — `grapes` canonical names use cépage+color suffix form instead of common names

- **Severity:** P0
- **Evidence:**
  ```sql
  SELECT count(*) FILTER (WHERE name ~ ' BLANC$| BLANCA$| BLANCO$') AS blanc_suffix,
         count(*) FILTER (WHERE name ~ ' NOIR$| NEGRO$| NERO$') AS noir_suffix,
         count(*) FILTER (WHERE name ~ ' TINTO$| TINTA$') AS tinto_suffix
  FROM grapes;
  -- Returns: blanc_suffix=118, noir_suffix=147, tinto_suffix=26
  ```
  The `grapes.name` field uses an INAO-style cépage-plus-color-suffix convention: `CHARDONNAY BLANC`, `MERLOT NOIR`, `TEMPRANILLO TINTO`, `PINOT NOIR`, `CARIGNAN NOIR`. This is the VIVC import style.

  Side effects observed:
  - `CHARDONNAY BLANC` is the canonical for Chardonnay — but there is no such grape in common usage as "Chardonnay Blanc." Chardonnay is Chardonnay.
  - `MERLOT NOIR` is used even in English-speaking contexts where it's just "Merlot."
  - `TEMPRANILLO TINTO` — "Tinto" means "red"; Tempranillo is Tempranillo.
  - `MONASTRELL` appears as the canonical name for Mourvèdre and gets linked to Châteauneuf-du-Pape (a French appellation) using the Spanish name.
  - `GARNACHA BLANCA`/`GARNACHA ROJA`/`GARNACHA TINTA` appear linked to Châteauneuf-du-Pape instead of Grenache Blanc/Gris/Noir.
- **Why it matters:** Every wine page showing a grape displays a name that's either ungrammatical ("Chardonnay Blanc") or in the wrong language for the context (Spanish names on French appellations, French "Pinot Noir" notation on US wines). A sommelier would consider this a display-layer failure. It also blocks fuzzy matching against source catalogs that use common-name forms.
- **Proposed fix:** Two options:
  - **(A) Add a `grapes.display_name` column** (already exists based on the GARRO row — `display_name='Garro'`, `name='GARRO'`). Populate display_name for all 9,695 grapes with the common English name. Make it the canonical display form. Keep `name` as the VIVC-style internal key.
  - **(B) Rename `grapes.name`** to common form. Higher risk — breaks downstream joins that might be hardcoded against "CHARDONNAY BLANC".
  - Recommend **A** — use the existing `display_name` column and update frontend to prefer it.
- **Effort:** medium (bulk-populate display_name for 9,695 grapes; hand-review ~200 tricky cases; update frontend)
- **Dependencies:** none
- **Related findings:** F1 (varietal_categories needs display_name too), F14 (Châteauneuf-du-Pape shows Spanish names)

---

## F7 — 921 grape-synonym rows collide with primary names of different grapes (re-confirms S2.1 F7 with higher precision)

- **Severity:** P0
- **Evidence:**
  ```sql
  SELECT count(DISTINCT gs.synonym) AS distinct_colliding_synonyms,
         count(*) AS total_collision_rows,
         count(DISTINCT gs.grape_id) AS grapes_affected
  FROM grape_synonyms gs
  JOIN grapes g2 ON upper(g2.name) = upper(gs.synonym) AND g2.id <> gs.grape_id;
  -- 657 distinct synonyms, 921 collision rows, 551 grapes affected
  ```
  Worst offenders observed in sample:
  - `AGLIANICO` is a synonym of 4 different primary grapes: MAGLIOCCO DOLCE, AGLIANICONE, LAMBRUSCO MAESTRI, NEGRO AMARO — plus it IS the primary name of AGLIANICO. Any wine resolver searching for "Aglianico" has 5 candidate grape_ids.
  - `ALBANA` collides with 3 primary grapes (ALBANA BIANCA, SANTA MAGDALENA, ALBANA itself).
  - `ABRUNHAL` collides across 3 grapes (MARUFO, TROUSSEAU NOIR, ABRUNHAL).
- **Why it matters:** Any name-based grape resolver hits ambiguity on ~10% of grape rows. The Chardonnay/Pinot Blanc bug (S2.3 F2, F2 this session) is one extreme instance. The Rioja "Trousseau Noir" appearing as required is another — Maturana Tinta (Rioja) and Trousseau Noir (Jura) are DNA-confirmed the same variety but got loaded under different primary names, so a loader looking up either hit the collision.
- **Proposed fix:**
  1. For each collision, choose ONE primary grape via DNA/VIVC authority and route the synonym to it.
  2. For genuine DNA-confirmed aliases (Mazuelo=Carignan=Cariñena, Garnacha=Grenache, Monastrell=Mourvèdre), consolidate to a single grape row with all aliases in synonyms.
  3. For accidental collisions (historical confusion where two distinct grapes share a name), keep both grape rows and delete the collision synonym.
- **Effort:** large (921 rows need case-by-case review with VIVC lookup; possibly Sonnet/Opus assisted triage)
- **Dependencies:** F2 resolver fix depends on this
- **Related findings:** F2, S2.1 F7

---

## F8 — S2.3 F7 correction: GARRO is a real VIVC grape, not invented

- **Severity:** P0 (rewords S2.3 F7 — downgrades the "invented grape" framing, keeps the wine_grapes linkage error)
- **Evidence:**
  ```sql
  SELECT name, vivc_number, origin_country_id, species, origin_region, parentage_confirmed
  FROM grapes WHERE name = 'GARRO';
  ```
  Result: `GARRO` (VIVC #7326, species=vinifera, origin_region=SPAIN, parentage_confirmed=true, created 2026-03-14).
  - GARRO is a real but obscure Spanish variety in the VIVC catalog.
  - Messina Hof's Papa Paulo Port (Texas) is linked to GARRO in wine_grapes.
  - Primary source from S2.3 F7 (messinahof.com) confirms Messina Hof's actual grape is **Lenoir / Black Spanish** (Vitis aestivalis hybrid).
  - Lenoir is present in Loam's grapes table as `BALDWIN LENOIR` and `JACQUEZ` (Jacquez is another name for Lenoir).
- **Why it matters:** S2.3 F7 incorrectly framed this as "invented grape." The corrected framing is: **GARRO is a real grape that Messina Hof does not grow** — the error is in wine_grapes linkage (wine matched to wrong grape_id), not in the grapes table. This matters for Sprint 3 scoping: fixing this requires fixing the wine-grape resolver, not deleting grapes rows.
- **Proposed fix:** in Sprint 3, fix the Messina Hof Papa Paulo wine_grapes link to point at BALDWIN LENOIR (or consolidate LENOIR/BALDWIN LENOIR/JACQUEZ into one canonical grape first, then link). As a meta-check, run a pass over wine_grapes where `grape_id` points at a grape from a `origin_country_id` that doesn't match the wine's producer country — surface those for manual review.
- **Effort:** small (Messina Hof specific) / medium (country-mismatch meta-check across all wines)
- **Dependencies:** F7 collision resolution
- **Related findings:** S2.3 F7 (corrected here)

---

## F9 — `established_year` poisoned with fake 1973 default on 240 French AOCs + 105 Italian DOCs

- **Severity:** P1
- **Evidence:**
  ```sql
  SELECT c.iso_code, a.established_year, count(*)
  FROM appellations a LEFT JOIN countries c ON c.id = a.country_id
  WHERE a.deleted_at IS NULL GROUP BY c.iso_code, a.established_year
  HAVING count(*) > 20;
  -- FR 1973 → 240 appellations
  -- IT 1973 → 105 appellations
  ```
  1973 is NOT a real AOC/DOC establishment year for these regions:
  - Chambertin, Montrachet, La Tâche, Musigny, Champagne: 1936 (first INAO decree year)
  - Barolo, Barbaresco: DOC 1966, DOCG 1980
  - Brunello di Montalcino: DOC 1966, DOCG 1980
  - Chianti Classico: DOCG 1984
  
  Meanwhile `appellation_rules.rules->>'historical_references'` and `rules->>'initially_recognized'` contain the correct year data for the same rows. Barolo's rules JSONB says "DPR 23.04.1966 GU 145; DOCG DPR 03.10.1980 GU 242" but the structured `appellations.established_year` column shows 1973.
- **Why it matters:** 345 rows have wrong establishment years. Frontend pages rendering "AOC Chambertin, established 1973" are factually wrong. For French AOCs this is particularly bad because 1936 is a landmark date (first INAO law).
- **Proposed fix:** bulk migration. For each row with `established_year IN (1973, 1976)` where the `rules` JSONB has a real date, extract and overwrite. For rows without a corresponding rules entry, NULL the column (no fake default). Then remove any default-value clause from the loader.
- **Effort:** small–medium (SQL + loader code review)
- **Dependencies:** none
- **Related findings:** F10 (classification_level has same "default or null" issue)

---

## F10 — `classification_level` coverage dominated by German einzellage; no Burgundy grand_cru, no AOC, no DOCG tags

- **Severity:** P1
- **Evidence:**
  ```sql
  SELECT classification_level, count(*) FROM appellations
  WHERE classification_level IS NOT NULL GROUP BY classification_level;
  ```
  1,743 of 3,661 appellations have classification_level populated (47.6%), but the populated values break down as:
  - `einzellage`: 1,179 (67.6% of populated)
  - `ward`: 101 (South Africa)
  - `region`: 90
  - `grosslage`: 83
  - `district`: 60
  - `subregion`: 55
  - ... (long tail) ...
  - `grand_cru`: **2** (should be ~50+ Burgundy Grands Crus + all Alsace Grand Cru)
  - `aoc`: **8** (should be ~300+)
  - `dac`: 15
  - `appellation`: 20
  - NO `docg`, NO `doc`, NO `ava`, NO `cru_classe`, NO `premier_cru`

  Cross-check: Chambertin, La Tâche, Montrachet, Musigny, Chambertin-Clos de Bèze all have `classification_level = NULL`.
- **Why it matters:** the column that should tell us "is this appellation a Grand Cru / DOCG / AVA / Premier Cru / AOC" is functionally useless for classification filtering. Any UI filter like "show me all Burgundy Grand Crus" returns 2 rows. German sub-appellations are over-represented; the rest of the world is absent.
- **Proposed fix:** Sprint 3 or Sprint 4 workstream. Build a canonical classification_level vocabulary (grand_cru, premier_cru, aoc, aop, docg, doc, ava, dac, ...). Map each country's appellation tier system into it. Bulk-populate from primary sources. Consider moving to `appellation_tiers` reference table with ordered tier hierarchy per country.
- **Effort:** medium–large (depends on tier hierarchy normalization)
- **Dependencies:** Sprint 4 reference redesign may subsume this
- **Related findings:** F9, F5 (1855 classification), F18

---

## F11 — `appellation_rules.rules` JSONB has no stable schema; same field lives under 3–5 different keys

- **Severity:** P1
- **Evidence:**
  ```sql
  SELECT key, count(*) FROM appellation_rules, jsonb_object_keys(rules) AS key
  GROUP BY key ORDER BY count(*) DESC LIMIT 40;
  ```
  Same concept lives under multiple keys across rows:
  - Establishment date: `established_date` (438), `established_year` (24), `established` (21), `initially_recognized` (observed in CdP row)
  - Area: `area_acres` (236), `area_ha` (225)
  - Elevation: `elevation_range_ft` (212), `elevation_range_m` (162), `elevation_max_m` (16) + `elevation_min_m` (16)
  - Alcohol min: `min_alcohol_pct` (33), `min_abv` (26)
  - Notes: `notes` (152), `note` (114)
  - Wine type: `wine_type` (160), `wine_types` (70), `wine_types_permitted` (24)
  - Color/flag: `colors` (375), `color_rules` (431 — nested `red_only`/`white_only`), `colors_permitted` (24), `red_only` (33), `white_only` (20)
  - Grape data: `grape_rules` (719), `grape_source_rule` (256), plus inline in `grape_rules.red.grape`, `rules.red.varieties`, etc.

  Extreme case: Petit Chablis uses `{"grape_rules": {"Chardonnay": 100}}` while Chambertin uses `{"grape_rules": {"principal": "Pinot Noir", "accessory_max": 15}}`. Cannot extract "what grape is required" with one query.
- **Why it matters:** the entire `appellation_rules` table is unqueryable as a structured source. Downstream code must try 3–5 keys per field and fall back to text parsing. Any Sprint 3 workstream that wants to extract rules into canonical columns will need to handle every schema variant.
- **Proposed fix:** **Sprint 4 scope.** Build a canonical `rules` schema (one JSONB shape or a structured normalization), write a migration that translates every existing row into the canonical form, and freeze the JSONB shape with a CHECK constraint or validator.
- **Effort:** large (core part of Sprint 4 reference redesign)
- **Dependencies:** Sprint 4
- **Related findings:** F12, F13, F29

---

## F12 — Yield and ABV extraction sparse in `appellation_rules` (7%/5% top-level coverage)

- **Severity:** P1
- **Evidence:**
  ```sql
  SELECT
    count(*) FILTER (WHERE rules ? 'max_yield_hl_ha' OR rules ? 'max_yield_kg_ha' OR rules ? 'max_yield_t_ha') AS with_yield,
    count(*) FILTER (WHERE rules ? 'min_alcohol_pct' OR rules ? 'min_abv') AS with_min_abv,
    count(*) AS total
  FROM appellation_rules;
  -- with_yield=80, with_min_abv=59, total=1165
  ```
  Only 6.9% have max_yield and 5.1% have min_alcohol at the top level. For well-populated rows (Barolo, Barbaresco, Brunello, Chianti Classico) the values exist deeper in nested JSON like `rules.min_alcohol_pct.base` or `rules.max_yield_kg_ha.red`. For Burgundy Grand Crus (Chambertin, Charmes-Chambertin, Bonnes-Mares, Montrachet, Musigny), the values don't exist at all.
- **Why it matters:** the two numbers a user most wants from an appellation page — "what's the max yield" and "what's the min alcohol" — are mostly missing or buried. Frontend pages render blank fields.
- **Proposed fix:** tied to F11. Canonical rules schema should hoist these two fields to top level. Where missing, backfill from primary-source data (already fetched in many rows, just needs extraction).
- **Effort:** medium (extraction + normalization pass)
- **Dependencies:** F11
- **Related findings:** F11, F13

---

## F13 — 434 of 1,165 rules (37%) have ≤3 top-level keys; load quality wildly uneven

- **Severity:** P1
- **Evidence:**
  ```sql
  SELECT count(*) FILTER (WHERE jsonb_typeof(rules)='object'
    AND (SELECT count(*) FROM jsonb_object_keys(rules)) <= 3) AS thin_rules
  FROM appellation_rules;
  -- thin_rules=434
  ```
  Specific thin rules observed (Burgundy Grand Crus):
  - `Chambertin`: `{color_rules, grape_rules}` (2 keys)
  - `Charmes-Chambertin`: `{color_rules, grape_rules}` (2 keys)
  - `Bonnes-Mares`: `{color_rules, grape_rules}` (2 keys) — also missing the accessory grape list that Charmes/Chambertin have
  - `Montrachet`, `Musigny`: similar stubs
  - `Margaux`: `{colors, region, communes, red_only, wine_type, grape_rules, classification}` — richer, but no yield/alcohol/density/aging data

  Meanwhile Italian DOCGs (`Barolo`, `Barbaresco`, `Brunello di Montalcino`, `Chianti Classico`) have deep data with nested tiers, density minimums, historical_references, extraction-ratio caps.
- **Why it matters:** content depth is not uniform. Chambertin is arguably Loam's most important single appellation (grand cru Burgundy) and it has 2 keys; a random Italian DOC has 20. When we ship enrichment, users hitting famous Burgundy names will get thinner data than users hitting obscure Italian DOCs. Reverse of expectation.
- **Proposed fix:** Sprint 3 or Sprint 4. Flag all 434 thin rules for re-fetch against primary sources (INAO cahier des charges are all available at `extranet.inao.gouv.fr`, many already URL-linked in `source_url`). A targeted Opus-inline pass could re-scrape and normalize them.
- **Effort:** medium (300+ rules need structured re-fetch)
- **Dependencies:** F11 (canonical schema first)
- **Related findings:** F11, F12

---

## F14 — `appellation_grapes` materializes Spanish grape names onto French appellations; missing Picardan from Châteauneuf-du-Pape

- **Severity:** P1
- **Evidence:**
  ```sql
  SELECT g.name FROM appellation_grapes ag
  JOIN appellations a ON a.id = ag.appellation_id
  JOIN grapes g ON g.id = ag.grape_id
  WHERE a.name = 'Châteauneuf-du-Pape'
  ORDER BY g.name;
  ```
  Châteauneuf-du-Pape in appellation_grapes (16 varieties linked):
  - BOURBOULENC, BRUN ARGENTE, CINSAUT, CLAIRETTE BLANCHE, CLAIRETTE ROSE, COUNOISE, **GARNACHA BLANCA**, **GARNACHA ROJA**, **GARNACHA TINTA**, **MONASTRELL**, MUSCARDIN, PIQUEPOUL BLANC, PIQUEPOUL NOIR, ROUSSANNE, SYRAH, TERRET NOIR

  - "Garnacha Blanca / Roja / Tinta" should display as "Grenache Blanc / Gris / Noir" for a French appellation
  - "Monastrell" should display as "Mourvèdre" for a French appellation
  - **"Picardan"** (one of the 13/18 authorized varieties per INAO CDC) is missing from the appellation_grapes list despite being named in `appellation_rules.rules.grape_rules.all.varieties`
  - "Brun Argente" is missing the accent (should be Brun Argenté, local name Vaccarèse)
  - "Clairette Rose" is missing accent (should be Clairette Rosé)

  Same pattern on Bordeaux:
  - Pauillac links to `COT` (French name for Malbec) and `MERLOT NOIR`, `VERDOT PETIT` (inverted name), `CARMENERE`
  - Margaux same six varieties
- **Why it matters:** (1) wine pages display grape pills using the wrong language for the appellation context. (2) The rules JSONB → appellation_grapes load pipeline dropped Picardan, so the enrichment layer thinks CdP has 16 varieties, not 17+. (3) Frontend can't easily render "Grenache" for CdP because the canonical links to Garnacha Tinta.
- **Proposed fix:** join F6 (display_name on grapes) + a backfill pass that re-reads `appellation_rules.rules` and syncs appellation_grapes. When the same grape appears under multiple names (Garnacha = Grenache), the appellation context should pick the language-appropriate form for display.
- **Effort:** medium
- **Dependencies:** F6, F11
- **Related findings:** F6, F15

---

## F15 — Grape name inversion: `VERDOT PETIT`, `MESLIER PETIT` appear in appellation_grapes

- **Severity:** P1
- **Evidence:**
  ```sql
  SELECT g.name FROM grapes g WHERE g.name IN ('VERDOT PETIT','MESLIER PETIT','CHATEY PETIT');
  ```
  - `VERDOT PETIT` is linked as "typical" for Margaux and Pauillac. Should be "Petit Verdot".
  - `MESLIER PETIT` is linked as "typical" for Champagne. Should be "Petit Meslier".
  
  S2.3 F9 already flagged `VERDOT PETIT` (284 wine_grapes links) and `MERLOT NOIR` (1,001 wine_grapes links) in the wine-facing grape table. S2.4 confirms the root cause is in `grapes.name` itself — the inverted form is the canonical name, so every downstream table (appellation_grapes, wine_grapes, varietal_categories) inherits it.
- **Why it matters:** a wine labeled "Petit Verdot" cannot find its canonical grape via exact match. Synonym matching must save the day. A sommelier seeing "Verdot Petit 2%" on a Château Margaux blend would notice.
- **Proposed fix:** rename `grapes.name` for the inverted cases. 5-10 known cases. Low-risk single migration. Add `grapes.display_name` values per F6.
- **Effort:** trivial
- **Dependencies:** none
- **Related findings:** F6, F14, S2.3 F9

---

## F16 — Chambertin / Charmes-Chambertin / Bonnes-Mares missing accessory grapes in appellation_grapes (inconsistent with La Tâche)

- **Severity:** P1
- **Evidence:**
  ```sql
  SELECT a.name, g.name AS grape, ag.association_type
  FROM appellation_grapes ag
  JOIN appellations a ON a.id = ag.appellation_id
  JOIN grapes g ON g.id = ag.grape_id
  WHERE a.name IN ('Chambertin','Chambertin-Clos de Bèze','Bonnes-Mares','La Tâche');
  ```
  - Chambertin: PINOT NOIR required (only)
  - Chambertin-Clos de Bèze: PINOT NOIR required (only)
  - La Tâche: PINOT NOIR required + CHARDONNAY BLANC, PINOT BLANC, PINOT GRIS typical (0-15%)
  
  Burgundy Grand Cru rules allow up to 15% combined accessory grapes (Chardonnay, Pinot Blanc, Pinot Gris). La Tâche has the accessories loaded correctly; Chambertin and CCdB do not, despite the same rule applying.
- **Why it matters:** inconsistent loading means appellation pages render different depth for peer Grand Crus. Rarely used in practice but surfaces on high-end wine pages.
- **Proposed fix:** bulk load accessory grapes for all Burgundy Grand Crus from the INAO cahier des charges (one pattern applies to all).
- **Effort:** trivial
- **Dependencies:** F11
- **Related findings:** F13

---

## F17 — `appellation_soils` has zero provenance columns (only 2 columns: appellation_id, soil_type_id)

- **Severity:** P1
- **Evidence:**
  ```sql
  SELECT column_name FROM information_schema.columns
  WHERE table_schema='public' AND table_name='appellation_soils';
  -- Returns: appellation_id, soil_type_id
  ```
  The `appellation_rules` table has 13 columns including `source_organization`, `source_url`, `source_document_title`, `source_text_excerpt`, `source_accessed_date`, `last_verified_at`.
  `appellation_soils` has 2 columns. No way to know where any of 930 links came from or when it was last verified.
- **Why it matters:** impossible to audit correctness. Cannot re-run source verification. Cannot tell if a Hunter Valley → Basalt link is from a TTB AVA doc, a regional tourism website, a producer self-description, or AI inference. S2.3 F14 flagged AI-generated prose confabulating geology ("Hunter Valley volcanic") — structured appellation_soils data that supports or refutes those claims has no provenance to compare against.
- **Proposed fix:** add provenance columns (`source_url`, `source_organization`, `source_text_excerpt`, `last_verified_at`, `source_accessed_date`). Backfill where possible from commit history of the loader; NULL otherwise.
- **Effort:** small (schema change) + medium (backfill)
- **Dependencies:** none
- **Related findings:** F20, S2.1 F16 (provenance gaps on appellation_grapes)

---

## F18 — Hunter Valley linked to `Basalt` in appellation_soils (likely reinforces S2.3 F14 volcanic confabulation)

- **Severity:** P1
- **Evidence:**
  ```sql
  SELECT st.name FROM appellation_soils ags
  JOIN appellations a ON a.id = ags.appellation_id
  JOIN soil_types st ON st.id = ags.soil_type_id
  WHERE a.name = 'Hunter Valley';
  -- Alluvial, Basalt, Clay, Loam, Silt
  ```
  Hunter Valley's characteristic soils per regional consensus are:
  - Alluvial flats (from the Hunter River) ✓
  - Clay loam ✓ ✓
  - Sandstone (from Permian geology)
  - Volcanic soils **only in isolated pockets in the Upper Hunter**, not as a dominant type
  
  Linking `Basalt` as a regional soil for the whole Hunter Valley AVA is overstated. Hunter Valley's reputation is built on weathered sedimentary substrates, not basalt.

  Meanwhile `Santa Ynez Valley` has zero rows in appellation_soils — S2.3 F14's "Franciscan shale" claim was pure AI confabulation with no structured-data anchor.
- **Why it matters:** S2.3 F14 flagged `wine_insights` prose confabulating "Hunter Valley volcanic soils." Some of that confabulation may be reinforced by structured data listing Basalt (a volcanic rock) among Hunter Valley soils. If Sprint 3 regenerates `wine_insights` content using `appellation_soils` as ground truth, the basalt link will keep generating "volcanic" prose.
- **Proposed fix:** delete the Basalt row for Hunter Valley. Audit other non-obvious soil-appellation pairings against primary sources (Wine Australia GI register for Australian AVAs; TTB AVA docs for US AVAs; INAO for French AOCs). This is a F17 provenance prerequisite task.
- **Effort:** small (verify + delete) / medium (full pass)
- **Dependencies:** F17
- **Related findings:** F17, F19, S2.3 F14

---

## F19 — `soil_types` has one junk row: "Ite"

- **Severity:** P2
- **Evidence:**
  ```sql
  SELECT name, description FROM soil_types WHERE name = 'Ite';
  -- Ite | Used broadly to describe metamorphic/clay soils in various regions
  ```
  "Ite" is a **suffix**, not a soil type. The suffix `-ite` appears on many igneous/metamorphic rock names (granite, schist, gneiss, rhyolite). Standing alone, "Ite" is not a soil name. The description text even admits "used broadly to describe" — a giveaway that the row was mis-extracted from a tokenizer.
- **Why it matters:** if any wine or appellation links to `Ite`, the UI displays a nonsense soil name. Also fails the sommelier bar if surfaced.
- **Proposed fix:** investigate whether any `appellation_soils` rows link to Ite; if so, remap them; then delete the row. If nothing links to it, just delete.
- **Effort:** trivial
- **Dependencies:** none
- **Related findings:** F17

---

## F20 — `source_organization` has massive string fragmentation (INAO in 20+ variants, MASAF in 15+, SAWIS in 15+)

- **Severity:** P2
- **Evidence:**
  ```sql
  SELECT source_organization, count(*)
  FROM appellation_rules
  WHERE source_organization ILIKE '%inao%'
  GROUP BY source_organization ORDER BY count(*) DESC;
  ```
  INAO variants found (non-exhaustive):
  - `INAO` (244)
  - `INAO (Institut National de l'Origine et de la Qualité)` (59)
  - `INAO — Institut National de l'Origine et de la Qualité` (10)
  - `INAO (Institut National de l'Origine et de la Qualite)` (8 — no accents)
  - `INAO (Institut national de l'origine et de la qualité)` (2 — lowercase)
  - `INAO (Institut National de l'Appellation d'Origine)` (1 — old name pre-2007)
  - `INAO — Institut National de l'Origine et de la Qualité (France)` (2)
  - `Institut National de l'Origine et de la Qualité (INAO)` (2)
  - `INAO – Comité National des Appellations d'Origine` (1)
  - plus 10+ "INAO / ..." hybrid entries combining INAO with Wikipedia or regional bodies

  Similar for MASAF (15+), SAWIS/WOSA (15+ combinations), MAPA (8+), Wines of Greece (20+).
- **Why it matters:** cannot group by source for coverage reports. Cannot drive source-quality metrics. Cannot run a "re-verify all INAO rows" maintenance pass cleanly. Not a correctness issue per row, but a major data-hygiene issue for the reference layer.
- **Proposed fix:** normalize to canonical source_organization values via a controlled vocabulary (e.g., `INAO`, `MASAF`, `MAPA`, `TTB`, `Wine Australia`, `SAWIS`, `WOSA`, `EU eAmbrosia`, `Wikipedia`). Introduce `source_organization_id` FK to a new `source_organizations` reference table. Deprecate the free-text column.
- **Effort:** small (one normalization pass, ~120 distinct values to canonicalize)
- **Dependencies:** Sprint 4 reference redesign scope
- **Related findings:** F30

---

## F21 — TTB-sourced `appellation_rules` rows are 98% unverified (4 of 188 have last_verified_at)

- **Severity:** P2
- **Evidence:**
  ```sql
  SELECT source_organization, count(*), count(*) FILTER (WHERE last_verified_at IS NOT NULL)
  FROM appellation_rules
  GROUP BY source_organization
  HAVING source_organization LIKE '%TTB%';
  -- TTB (Alcohol and Tobacco Tax and Trade Bureau) | 188 | 4
  -- Alcohol and Tobacco Tax and Trade Bureau (TTB) | 50 | 50
  ```
  TTB-sourced rows split across two string-variant buckets — the second variant is 100% verified (50/50), the first is 2% verified (4/188). Most likely the 188 batch was bulk-loaded without triggering the verification timestamp.
- **Why it matters:** the unverified 184 TTB rows include Napa Valley, Oakville, Rutherford, and other core US AVA rules — rows users will hit frequently.
- **Proposed fix:** single UPDATE: where source_organization IN TTB variants, set `last_verified_at = coalesce(last_verified_at, source_accessed_date, updated_at)`. This is mechanical — the data was sourced from TTB, so "verified" is implicit in the load, just never recorded.
- **Effort:** trivial
- **Dependencies:** F20 normalization
- **Related findings:** F20

---

## F22 — Rioja appellation_grapes lists `TROUSSEAU NOIR` as required; technically correct-by-DNA but misleading canonical form

- **Severity:** P2
- **Evidence:**
  ```sql
  SELECT g.name FROM appellation_grapes ag
  JOIN appellations a ON a.id = ag.appellation_id
  JOIN grapes g ON g.id = ag.grape_id
  WHERE a.name = 'Rioja' AND ag.association_type = 'required';
  -- CARIGNAN NOIR, GARNACHA TINTA, GRACIANO, TEMPRANILLO TINTO, TROUSSEAU NOIR
  ```
  Rioja's 5 authorized red grapes per the Consejo Regulador DOCa Rioja are: Tempranillo, Garnacha Tinta, Graciano, Mazuelo (= Cariñena = Carignan Noir), **Maturana Tinta**. DNA analysis (2008) confirmed Spanish Maturana Tinta = French Trousseau Noir.
  
  So the link is not factually wrong — Maturana Tinta and Trousseau Noir are the same variety. But the canonical form displayed in a Rioja context should be "Maturana Tinta" (Spanish name for Spanish appellation), not "Trousseau Noir" (French name).
- **Why it matters:** a Rioja wine page listing "Trousseau Noir" among its grapes would confuse any reader who knows Rioja — Trousseau is a Jura grape in common usage. A sommelier would flag this as wrong even though it's technically correct.
- **Proposed fix:** tied to F6 (`display_name` context-aware). When a grape is surfaced on a Spanish appellation, prefer the Spanish synonym; on French, the French; on German, the German. This is a display-layer fix, not a data fix.
- **Effort:** small (depends on display_name strategy)
- **Dependencies:** F6
- **Related findings:** F6, F7, F14

---

## F23 — Margaux AOC communes include duplicate "Cantenac"

- **Severity:** P2
- **Evidence:**
  ```sql
  SELECT rules->'communes' FROM appellation_rules ar
  JOIN appellations a ON a.id = ar.appellation_id
  WHERE a.name = 'Margaux';
  -- ["Margaux-Cantenac", "Cantenac", "Soussans", "Arsac", "Labarde"]
  ```
  Margaux-Cantenac is the merged municipality (formed 2017 from Margaux and Cantenac). Listing both "Margaux-Cantenac" and "Cantenac" separately is redundant. The canonical Margaux AOC communes are 4 (or 5 depending on whether you count pre- and post-merger).
- **Why it matters:** minor data hygiene, not a correctness error. A sommelier might notice.
- **Proposed fix:** hand-edit the row. Deduplicate "Cantenac" from the list.
- **Effort:** trivial
- **Dependencies:** none
- **Related findings:** F13

---

## F24 — Napa Valley `grape_source_rule` cites only federal 85% rule, missing California state 100% county requirement

- **Severity:** P2
- **Evidence:**
  ```sql
  SELECT rules->>'grape_source_rule' FROM appellation_rules ar
  JOIN appellations a ON a.id = ar.appellation_id WHERE a.name = 'Napa Valley';
  -- "85% minimum from the named viticultural area per 27 CFR 4.25a"
  ```
  Federal TTB rule (27 CFR 4.25a) requires 85% minimum of grapes from the named AVA. **California state law (CA Business & Professions Code § 25232, the "Napa Valley rule") additionally requires 100% of grapes from Napa County when "Napa Valley" appears on the label.** Loam's rule only cites the federal floor.
- **Why it matters:** incomplete. A sommelier asked "how strict is the Napa Valley labeling rule" would answer "100% Napa county" — Loam says "85%". This applies to all Napa Valley sub-AVAs (Oakville, Rutherford, Stags Leap, Howell Mountain, etc.) as well.
- **Proposed fix:** update Napa Valley + all ~16 Napa sub-AVA rows with the dual-rule text: "85% minimum from the named viticultural area per 27 CFR 4.25a; additionally 100% from Napa County per CA B&P § 25232 when 'Napa Valley' appears on label."
- **Effort:** trivial
- **Dependencies:** none
- **Related findings:** F13

---

## F25 — 6 varietal_categories have correct-but-confusing grape links (synonym-variety routing)

- **Severity:** P2
- **Evidence:**
  ```sql
  SELECT vc.name, g.name FROM varietal_categories vc
  JOIN grapes g ON g.id = vc.grape_id
  WHERE vc.name IN ('Zinfandel','Shiraz','Petite Sirah','Nero d''Avola','Godello','Torrontés');
  ```
  These are all technically correct but the canonical grape picked is not the one users expect:
  - `Zinfandel` → `PRIMITIVO` ✓ (DNA confirmed same)
  - `White Zinfandel` → `PRIMITIVO` ✓
  - `Shiraz` → `SYRAH` ✓
  - `Petite Sirah` → `DURIF` ✓
  - `Nero d'Avola` → `CALABRESE` ✓ (Sicilian local name)
  - `Godello` → `GOUVEIO` ✓ (same grape, Portuguese name used)
  - `Torrontés` → `TORRONTES MENDOCINO` ✓
  
  These are not errors per se. But they reveal an inconsistent canonical-name policy: some grapes consolidate to the historically oldest name (Primitivo over Zinfandel), some to the regional name of origin (Calabrese over Nero d'Avola, Gouveio over Godello), some to the local name where the category lives (Torrontes Mendocino for the Argentine category). No stated rule.
- **Why it matters:** users looking at `Zinfandel` wines see "Primitivo" as the grape. Some will know these are the same; many will not. Same for Shiraz/Syrah — most American users expect "Shiraz" for Australian wines even though the canonical is Syrah.
- **Proposed fix:** formalize a canonical-name policy. Options:
  - **(A)** Most common English name wins (Zinfandel, Shiraz, Petite Sirah, Nero d'Avola, Godello — the display-name forms). Controversial because it privileges English.
  - **(B)** Oldest/original name wins (Primitivo, Syrah, Durif, Calabrese, Gouveio). Current informal policy. Controversial because it's not what users expect.
  - **(C)** Context-dependent via display_name (F6). Show "Shiraz" on Australian wine pages, "Syrah" on Rhône wine pages. Best UX, most engineering.
  - Recommend **C**.
- **Effort:** small policy decision + medium implementation
- **Dependencies:** F6
- **Related findings:** F6, F22

---

## F26 — `tasting_descriptors` mixes structural/palate terms with flavor descriptors

- **Severity:** P2
- **Evidence:**
  ```sql
  SELECT name FROM tasting_descriptors ORDER BY name LIMIT 50;
  ```
  Same 304-row table contains:
  - Flavor descriptors: `Acacia`, `Almond`, `Apricot`, `Basil`, `Blackberry`, `Bonfire / Campfire` (standard WSET aroma/flavor wheel)
  - Structural/palate measurements: `Acidity`, `Alcohol`, `Body / Weight`, `Bone Dry`, `Astringent / Drying`
  - Near-duplicates: `Barnyard / Stable` + `Brett / Barnyard`, `Cedar` + `Cedar (Aged)`, `Chalk` (also a soil type in `soil_types`) + `Chalky`
  - Parent categories at same level as children: `Black Fruit` alongside `Blackberry`, `Black Cherry`, `Black Plum`, `Blackcurrant / Cassis`
- **Why it matters:** categorization issue. A tasting notes UI would want to separate structural (acidity level) from flavor (blackberry) for display. The `parent_descriptor_id` column exists for hierarchy but coverage is unknown.
- **Proposed fix:** add a `descriptor_kind` column (flavor, aroma, structure, palate, texture, flaw, other) and populate. Dedupe the near-duplicates. Audit `parent_descriptor_id` coverage.
- **Effort:** small
- **Dependencies:** none
- **Related findings:** none

---

## F27 — `farming_certifications` (21 rows) and `biodiversity_certifications` (7 rows) are clean — positive finding

- **Severity:** P3 (positive — informational)
- **Evidence:**
  ```sql
  SELECT name, description FROM farming_certifications; -- 21 rows
  SELECT name, description FROM biodiversity_certifications; -- 7 rows
  ```
  All 28 rows are real, correctly-named, well-described certifications: EU Organic, USDA Organic, Demeter Biodynamic, Biodyvin, Fair Trade, HVE, LIVE, SIP, Natural Wine, Salmon-Safe, Terra Vitis, Sustainable Winegrowing NZ, Entwine Australia, Regenerative Organic Certified, Bird Friendly (Smithsonian), etc.
  
  One minor note: `HVE` appears in farming_certifications and also as `Haute Valeur Environnementale (HVE)` in biodiversity_certifications. Likely intentional (HVE covers both farming practices and biodiversity) but creates a visible duplication.
- **Why it matters:** this is the reference layer done right. Small, curated, clean, well-described. Compare to `grape_synonyms` (34,809 rows, 921 collisions) or `appellation_rules` (1,165 rows, 37% thin, schema drift): size and curation effort correlate with quality.
- **Proposed fix:** optional — add an `HVE` alias in one of the two tables pointing at the other, OR split HVE into "HVE (farming)" and "HVE (biodiversity)" if they're considered distinct. Leave as-is works too.
- **Effort:** trivial if taken
- **Dependencies:** none
- **Related findings:** (contrast with F1, F2, F11)

---

## F28 — `grapes.parentage_confirmed = true` on 5,225 of 9,695 (54%) — good coverage but includes unverified GARRO-style mass loads

- **Severity:** P2
- **Evidence:**
  ```sql
  SELECT count(*) FILTER (WHERE parentage_confirmed = true) AS confirmed,
         count(*) AS total,
         count(*) FILTER (WHERE parent1_grape_id IS NOT NULL) AS with_parent1
  FROM grapes;
  -- confirmed=5225, total=9695, with_parent1=4050
  ```
  54% parentage confirmation is actually high for any grape database. However, GARRO (VIVC #7326, Spanish crossing) is marked `parentage_confirmed=true` — a grape that's obscure enough to cause wrong-grape linkage errors (F8) is trusted at the parentage level. No obvious way from DB alone to tell which parentages are VIVC-sourced vs Loam-added.
- **Why it matters:** the `parentage_confirmed` flag is only as good as the source behind it. Without a `parentage_source` column, we cannot audit which parentage claims to trust in enrichment content.
- **Proposed fix:** add `parentage_source` text column to grapes (vivc|dna|literature|inferred). Backfill vivc where `vivc_number IS NOT NULL`. Leave others NULL. Low priority until enrichment uses parentage data.
- **Effort:** small
- **Dependencies:** none
- **Related findings:** F8

---

## F29 — `appellations` structured columns drift from `appellation_rules` JSONB

- **Severity:** P2
- **Evidence:**
  ```sql
  SELECT a.name, a.established_year, a.max_yield_hl_ha, a.min_alcohol_pct, a.min_aging_months
  FROM appellations a WHERE a.name IN ('Chambertin','Barolo','Brunello di Montalcino','Champagne');
  -- All four: established_year=1973 (wrong per F9)
  -- Chambertin: max_yield_hl_ha=49 (appellations) but rules JSONB has no max_yield at all
  -- Barolo: appellations.max_yield_hl_ha=56 but rules JSONB has max_yield_t_ha=8 which is ~64 hl/ha
  -- Champagne: appellations.max_yield_hl_ha=NULL, rules JSONB has max_yield_kg_ha=12400
  -- min_alcohol_pct NULL in appellations for all; rules JSONB has per-tier values
  -- min_aging_months NULL in appellations for all; rules JSONB has per-tier values
  ```
  The `appellations` table has structured columns (min_alcohol_pct, max_yield_hl_ha, min_aging_months, etc.) that duplicate data also held in `appellation_rules.rules` JSONB. The two sources disagree or complement each other inconsistently.
- **Why it matters:** downstream queries don't know which to trust. Frontend might read `appellations.max_yield_hl_ha` and get a stale value, or read the JSONB and get a nested path that doesn't exist for the given row.
- **Proposed fix:** **Sprint 4 scope.** As part of the reference layer redesign, decide on ONE canonical location for each rule field. Recommend: keep structured columns on `appellations`, drop duplicated data from `appellation_rules.rules`, keep only source-text and provenance in `appellation_rules`. OR go the other way and drop the structured columns. Pick one.
- **Effort:** large (reference redesign)
- **Dependencies:** Sprint 4
- **Related findings:** F9, F10, F11, F12

---

## F30 — Positive finding: source_url primary-source coverage is 81%, Wikipedia only 2%

- **Severity:** P3 (positive — informational)
- **Evidence:**
  ```sql
  SELECT
    CASE WHEN source_url ~* 'inao\.gouv' THEN 'inao'
         WHEN source_url ~* 'ttb\.gov|ecfr\.gov' THEN 'ttb'
         WHEN source_url ~* 'politicheagricole|gazzettaufficiale' THEN 'masaf'
         WHEN source_url ~* 'mapa\.gob|boe\.es' THEN 'mapa'
         WHEN source_url ~* 'eambrosia|europa\.eu' THEN 'eu_register'
         WHEN source_url ~* 'wineaustralia' THEN 'wine_australia'
         WHEN source_url ~* 'wosa|sawis' THEN 'sa_primary'
         WHEN source_url ~* 'wikipedia' THEN 'wikipedia'
         ELSE 'other' END AS source_type, count(*)
  FROM appellation_rules GROUP BY source_type;
  -- inao=322, ttb=238, masaf=164, mapa=95, wine_australia=73, sa_primary=53, eu_register=30 → 975 primary (83.7%)
  -- wikipedia=24 (2%), other=166 (14%)
  ```
  Only 24 rows (2%) cite Wikipedia, the rest are primary regulatory bodies or trade registers. This is unusually good source discipline for a curated database — most wine-data references over-rely on Wikipedia summaries.
- **Why it matters:** the provenance layer is one of Loam's bigger assets. It means Sprint 3/4 content fixes can be driven directly from primary-source URLs already stored on each row. Even for thin-rule rows (F13), the source_url usually points at the exact INAO PDF or TTB eCFR section that has the missing data.
- **Proposed fix:** none needed. Replace the 24 Wikipedia-sourced rows with primary sources opportunistically, but not a priority. Most critically, La Tâche's rule currently cites `INAO (via Wikipedia La Tâche AOC article summarizing Cahier des Charges)` — can be replaced with the direct INAO PDF.
- **Effort:** trivial
- **Dependencies:** none
- **Related findings:** F21 (TTB verification backfill is the real source-layer TODO)

---

## Meta-patterns for S2.9 synthesis

1. **The reference layer's provenance infrastructure is good; content loads are uneven.** `appellation_rules` has 13 well-designed columns including provenance (F30 shows 81% primary-source URLs). But the content inside `rules` JSONB has no stable schema (F11), 37% of rows are thin stubs (F13), and the depth is inverted from user expectation — obscure Italian DOCs have deep data while Burgundy Grand Crus have 2 keys each (F13). Sprint 4 reference redesign should keep the provenance infrastructure and fix the loader pattern.
2. **Canonical grape naming is the single biggest cross-cutting issue.** F1 (varietal_categories wrong links), F2 (Chardonnay/Pinot Blanc synonym pollution), F6 (suffix-polluted grape names), F7 (921 primary-name collisions), F8 (GARRO linkage), F14 (Spanish names on French appellations), F15 (name inversion), F22 (Rioja Trousseau Noir) are all facets of the same underlying problem: `grapes.name` uses VIVC internal cépage+suffix form as canonical, downstream tables inherit it, and resolvers have no policy for picking between synonyms. **Fix the grapes table first, then cascade.** This is a pre-Sprint-3 workstream, not a Sprint 4 one.
3. **Appellation naming has parallel issues.** F3 (slash-concatenated alias names), F4 (missing diacritics), F9 (fake 1973 default), F10 (classification_level useless), F29 (structured vs JSONB drift) all come from the same root: loaders were written one per source, each with its own naming convention, and the results landed in shared columns without normalization. Sprint 4 should lock a canonical appellation naming policy and regenerate from it.
4. **S2.3 findings traceable to S2.4 data issues:** S2.3 F2 (Chardonnay/Pinot Blanc) → F2 root cause (synonym pollution). S2.3 F7 (invented grape) → F8 correction (GARRO is real, wine_grapes linkage is the bug). S2.3 F9 (Verdot Petit) → F15 root cause (grapes.name inversion). S2.3 F14 (Hunter Valley volcanic) → F18 structured-data reinforcement (Basalt link). This validates the S2.3→S2.4 ordering — the wine-level audit surfaced symptoms, the reference-level audit surfaced causes.

## Sprint 3 sequence recommendation (updated from S2.3)

The S2.3 recommended sequence was:
1. S2.2 F1 staging relink
2. S2.3 F3 producer seed
3. F2/F7/F8 grape repair
4. F6 color+country repair
5. F10 L3 re-fact-check
6. Content regeneration

S2.4 refines step 3. The "grape repair" workstream should run as:
- **3a** — Clean `grapes.name` canonical form (F6, F15 — rename VERDOT PETIT → Petit Verdot etc., populate display_name for all 9,695 rows)
- **3b** — Resolve the 921 primary-name collisions (F7) and delete the PINOT BLANC "Chardonnay"-containing synonyms (F2)
- **3c** — Fix the 5+ varietal_categories wrong grape_id links (F1)
- **3d** — Re-run the grape resolver against wine_grapes to fix the 2,743 Chardonnay/Pinot Blanc mismatches (S2.3 F2) + all other grape linkage bugs it surfaces
- **3e** — Only then (not before) run F11-F13 JSONB content backfill, F14 appellation_grapes language/Picardan fixes, F17 appellation_soils provenance schema

Total new findings in S2.4 that block Sprint 3: **8 P0 + 14 P1 = 22 items** in the "must fix before enrichment" bucket.

## Scope-breaker check

None of the S2.4 findings require a Sprint 3 rewrite. They slot cleanly into the existing Sprint 3 "execute fixes" envelope, and several become inputs to Sprint 4 reference redesign rather than Sprint 3 execution. Recalibration: Sprint 3 scope grows by ~20 items, Sprint 4 scope grows by the F11/F29 canonical-rules-schema workstream.

## Budget

Opus 4.6 inline + 1 WebFetch call (Wikipedia La Tâche area verification). **Actual spend: $0.00.** Ratified pattern per `docs/DECISIONS.md` 2026-04-11. The $18 S2.3 pre-auth continues to roll forward to Sprint 3 F10 L3 re-fact-check where scale may genuinely warrant API spend.
