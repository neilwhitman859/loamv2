# Path A Rollback Procedures

Per-migration rollback SQL for every Path A change. If a seeded rule turns out
to be wrong or a cascade needs reversing, use the corresponding section below.

**General principle**: Path A migrations are additive (INSERT) or stamping-only
(UPDATE with provenance fields). Cascades fill NULLs, never overwrite. Every
change is traceable via `source_url` (for provenance rows) or `updated_at`
timestamp window (for cascade operations).

---

## Session 1 (prior — 2026-04-05 morning): 30 rules seeded

Rollback scripts exist in `data/session_prompts/seed_appellation_rules*.md` or
via the migrations listed in `list_migrations`. See DECISIONS.md "Path A session
complete" entry.

---

## Session 2 (this turn — 2026-04-05 continuation): 38 new rules added (30 → 68)

### To undo ALL of Session 2 at once

```sql
-- Remove all appellation_rules added in Session 2
DELETE FROM appellation_rules
WHERE source_url IN (
  -- Batch 2a: Jumilla
  'https://eur-lex.europa.eu/legal-content/IT/TXT/PDF/?uri=OJ:C_202501605',
  -- Batch 2b: 11 French AOCs
  'https://extranet.inao.gouv.fr/fichier/PNOCDCPauillac.pdf',
  'https://extranet.inao.gouv.fr/fichier/PNOCDCMargaux.pdf',
  'https://extranet.inao.gouv.fr/fichier/PNOCDCSaint-Julien.pdf',
  'https://extranet.inao.gouv.fr/fichier/CDC---Graves-et-Graves-sup%C3%A9rieures---PNO-2023.pdf',
  'https://extranet.inao.gouv.fr/fichier/CDC---Pessac-L%C3%A9ognan---PNO-2024.pdf',
  'https://extranet.inao.gouv.fr/fichier/PNO2020CDCCrozesHermitage.pdf',
  'https://extranet.inao.gouv.fr/fichier/PNO2023AOPCornas.pdf',
  'https://extranet.inao.gouv.fr/fichier/PNOCDCAOCCondrieu.pdf',
  'https://extranet.inao.gouv.fr/fichier/PNO-CDCMorgon-221130.pdf',
  'https://extranet.inao.gouv.fr/fichier/PNO2022AOPBANDOL.pdf',
  'https://extranet.inao.gouv.fr/fichier/PNO-AOC-MINERVOIS-2019.docx.pdf',
  -- Batch 3a: 11 more French AOCs
  'https://extranet.inao.gouv.fr/fichier/4-CDC-Pomerol-PNO.pdf',
  'https://extranet.inao.gouv.fr/fichier/4-CDC-Sauternes-PNO.pdf',
  'https://extranet.inao.gouv.fr/fichier/4-CDC-Barsac-PNO.pdf',
  'https://extranet.inao.gouv.fr/fichier/CDCSaint-Emilion-PNO2023.pdf',
  'https://extranet.inao.gouv.fr/fichier/CDCSaint-Emilion-Grand-cru-PNO2023.pdf',
  'https://extranet.inao.gouv.fr/fichier/PNOCDCSaint-Estephe.pdf',
  'https://extranet.inao.gouv.fr/fichier/PNO2023SaintJoseph.pdf',
  'https://extranet.inao.gouv.fr/fichier/PNO2023AOPCoteRotie.pdf',
  'https://extranet.inao.gouv.fr/fichier/PNOCDCMoulinaVent.pdf',
  'https://extranet.inao.gouv.fr/fichier/PNOCDCFleurie.pdf',
  'https://extranet.inao.gouv.fr/fichier/pnocdcaoc-vacqueyras.pdf',
  -- Batch 3b: 6 Spanish DOPs
  'https://www.mapa.gob.es/es/alimentacion/temas/calidad-diferenciada/rueda_2023_11_29_tcm30-211417.pdf',
  'https://www.mapa.gob.es/dam/mapa/contenido/alimentacion/temas/calidad-agroalimentaria/2017-calidad-diferenciada/nuevo_denominaciones/pliegos-de-condiciones/pliego-condiciones-vinos/dops/penedes_2022_05_17.pdf',
  'https://www.mapa.gob.es/dam/mapa/contenido/alimentacion/temas/calidad-agroalimentaria/2017-calidad-diferenciada/nuevo_denominaciones/pliegos-de-condiciones/pliego-condiciones-vinos/dops/navarra_2024_10_11.pdf',
  'https://www.mapa.gob.es/dam/mapa/contenido/alimentacion/temas/calidad-agroalimentaria/2017-calidad-diferenciada/nuevo_denominaciones/pliegos-de-condiciones/pliego-condiciones-vinos/dops/toro_2025_01_03.pdf',
  'https://www.mapa.gob.es/dam/mapa/contenido/alimentacion/temas/calidad-agroalimentaria/2017-calidad-diferenciada/nuevo_denominaciones/pliegos-de-condiciones/pliego-condiciones-vinos/dops/bierzo_2021_07_23.pdf',
  'https://www.mapa.gob.es/dam/mapa/contenido/alimentacion/temas/calidad-agroalimentaria/2017-calidad-diferenciada/nuevo_denominaciones/pliegos-de-condiciones/pliego-condiciones-vinos/dops/somontano_2011_01_01.pdf',
  -- Batch 4: 9 more (Italian Veneto + French)
  'https://sharing.regione.veneto.it/index.php/s/yABaGJKHrga9jxQ/download',
  'https://sharing.regione.veneto.it/index.php/s/PxRP64A5pjymRYw/download',
  'https://sharing.regione.veneto.it/index.php/s/3cKJDw6BCWMDQsf/download',
  'https://sharing.regione.veneto.it/index.php/s/2KE5myjcgjbdmyB/download',
  'https://sharing.regione.veneto.it/index.php/s/GMSo6kKiEMDzckA/download',
  'https://extranet.inao.gouv.fr/fichier/CDCCorbieresPNO2019.pdf',
  'https://extranet.inao.gouv.fr/fichier/PNOCdC-Faugeres.pdf',
  'https://extranet.inao.gouv.fr/fichier/PNO2025AOPSaintPeray.pdf',
  'https://extranet.inao.gouv.fr/fichier/PNOCDCAOCCairanne.pdf'
);
```

### To undo appellation_grapes UPDATEs (stamp-only changes)

UPDATEs set source_organization / source_url / source_text_excerpt / notes /
association_type / last_verified_at on rows that previously had NULL provenance.
To revert provenance stamping (keeping the row):

```sql
UPDATE appellation_grapes SET
  source_url = NULL, source_organization = NULL,
  source_document_title = NULL, source_accessed_date = NULL,
  source_text_excerpt = NULL, last_verified_at = NULL,
  notes = NULL, association_type = 'typical'
WHERE source_url IN (<same list of URLs as above>);
```

### To undo appellation_grapes INSERTs

New grape rows have source_organization set. To remove them:

```sql
DELETE FROM appellation_grapes
WHERE source_url IN (<same list of URLs as above>)
  AND (appellation_id, grape_id) NOT IN (
    -- Keep rows that pre-existed (we UPDATEd them, didn't INSERT)
    -- Pre-existing rows are identifiable by the fact they had the appellation
    -- but different/NULL provenance BEFORE this session.
    -- For a clean rollback, delete all then re-run the pre-session seed.
  );
```

**Caveat**: This delete would also remove UPDATEd rows. A safer approach is to
identify session-inserted rows by checking `last_verified_at` against the
migration timestamp (all rows with provenance URL X and last_verified_at within
the migration's execution window).

---

## Cascade rollback — wine.color changes

### Batch 2+3+4 color cascades

Each cascade migration ran with a specific `WHERE color IS NULL AND appellation_id = X`
filter. To rollback, reverse the color assignment ONLY for wines where the
updated_at timestamp matches the cascade execution time and color matches what
was set.

**Per-cascade rollback SQL**:

```sql
-- Template: reverse a color cascade
-- Replace:
--   <appellation_id>     — the target appellation
--   <color_assigned>     — 'red' or 'white'
--   <cascade_start_time> — approximate migration execution timestamp
--   <cascade_end_time>   — slightly later (1 minute window)
UPDATE wines SET color = NULL, updated_at = NOW()
WHERE appellation_id = '<appellation_id>'
  AND color = '<color_assigned>'
  AND updated_at BETWEEN '<cascade_start_time>'::timestamptz
                     AND '<cascade_end_time>'::timestamptz;
```

**Caveat**: Other processes (Riddler nightly agent, Enofile promotion, TTB
reclassification) also update `wines.color` and `wines.updated_at`. If multiple
processes touched the same wine in the same time window, the rollback may
revert work that should be kept. For precision, inspect `wine_change_log` or
similar audit table (not currently present — this is a known gap).

### Batch 2 cascade executions

| Appellation | Color Set | Approx Rows Touched | Filter |
|---|---|---|---|
| Pauillac | red | ~121 | appellation_id='f353a164-...' AND color IS NULL |
| Margaux | red | ~128 | appellation_id='7fa11621-...' AND color IS NULL |
| Saint-Julien | red | ~74 | appellation_id='50c88a88-...' AND color IS NULL |
| Cornas | red | ~106 | appellation_id='dd4f6ed6-...' AND color IS NULL |
| Morgon | red | ~231 | appellation_id='c0773ba2-...' AND color IS NULL |
| Condrieu | white | ~109 | appellation_id='c7b4c983-...' AND color IS NULL |

### Batch 3 cascade executions

| Appellation | Color Set | Filter |
|---|---|---|
| Pomerol | red | color IS NULL |
| Saint-Émilion | red | color IS NULL |
| Saint-Émilion Grand Cru | red | color IS NULL |
| Saint-Estèphe | red | color IS NULL |
| Côte-Rôtie | red | color IS NULL |
| Moulin-à-Vent | red | color IS NULL |
| Fleurie | red | color IS NULL |
| Sauternes | white | color IS NULL |
| Barsac | white | color IS NULL |

### Batch 4 cascade executions

| Appellation | Color Set | Filter |
|---|---|---|
| Valpolicella | red | color IS NULL |
| Bardolino | red | color IS NULL |
| Soave | white | color IS NULL |
| Soave Superiore | white | color IS NULL |
| Saint-Péray | white | color IS NULL |

---

## wine_grapes cascade rollback (Cornas + Condrieu)

Batch 2 also created 617 `wine_grapes` rows at percentage=100 for Cornas
(100% Syrah) and Condrieu (100% Viognier). These are the ONLY wine_grapes
rows created by Path A.

```sql
-- Rollback Cornas 100% Syrah cascade
DELETE FROM wine_grapes
WHERE grape_id = '2af8e266-79be-4aa8-8464-06897ea20924'  -- SYRAH
  AND percentage = 100
  AND wine_id IN (
    SELECT id FROM wines
    WHERE appellation_id = 'dd4f6ed6-b92b-495a-939f-3505badeb5b1'  -- Cornas
      AND (color = 'red' OR color IS NULL)
  );

-- Rollback Condrieu 100% Viognier cascade
DELETE FROM wine_grapes
WHERE grape_id = '72f81853-b586-4173-a922-fca8f75d2029'  -- VIOGNIER
  AND percentage = 100
  AND wine_id IN (
    SELECT id FROM wines
    WHERE appellation_id = 'c7b4c983-b0cb-4951-bccd-0102a826da93'  -- Condrieu
      AND (color = 'white' OR color IS NULL)
  );
```

**Caveat**: This rollback is safe because no other process creates
`wine_grapes` rows with exactly `grape_id = Syrah/Viognier AND percentage = 100`
for wines in these specific appellations at this scale. If a manual override
or external import adds similar rows between now and rollback time, they would
be included. Narrow with `created_at` timestamp if a more surgical rollback
is needed.

---

## varietal_category_id cascade rollback (Cornas + Condrieu)

Batch 2 also set `wines.varietal_category_id` on Cornas (→ Syrah category) and
Condrieu (→ Viognier category).

```sql
-- Rollback Cornas varietal category
UPDATE wines SET varietal_category_id = NULL
WHERE appellation_id = 'dd4f6ed6-b92b-495a-939f-3505badeb5b1'
  AND varietal_category_id = 'ed14113a-9fa2-4093-bd12-9ad217412070';  -- Syrah

-- Rollback Condrieu varietal category
UPDATE wines SET varietal_category_id = NULL
WHERE appellation_id = 'c7b4c983-b0cb-4951-bccd-0102a826da93'
  AND varietal_category_id = 'd1807946-0a6b-4deb-9b05-950b1db177cc';  -- Viognier
```

---

## Batch 11 (2026-04-05 very late): 22 Italian DOC/DOCG via MASAF catalogoviti mass sweep

**Source URL pattern**: `http://catalogoviti.politicheagricole.it/scheda_denom.php?t=dsc&q={id}`

### Appellations seeded (22)

| # | Appellation | q ID | appellation_id | Rule |
|---|---|---|---|---|
| 1 | Aglianico del Vulture | 2002 | b9960dcb-a445-41ad-ade2-76d5f7af3996 | 100% Aglianico, red |
| 2 | Barbera d'Alba | 2020 | 040007ee-88ed-4317-8add-e6927e62f992 | 85-100% Barbera + 0-15% Nebbiolo, red |
| 3 | Gattinara | 1039 | edfd5eaf-c0e3-48cc-9b04-528f4f9bd4ff | 90-100% Nebbiolo (Spanna), red |
| 4 | Primitivo di Manduria | 2236 | 3853e919-65f7-43cf-afe3-effe0802805f | 85%+ Primitivo, red |
| 5 | Montepulciano d'Abruzzo | 2200 | 53863f87-8dc4-4717-8c93-e65a445c0c70 | 85%+ Montepulciano, red |
| 6 | Verdicchio dei Castelli di Jesi | 2320 | 910b1424-3cf7-4466-ac5f-221c43eeae1f | 85%+ Verdicchio, white |
| 7 | Verdicchio di Matelica | 2321 | daedd257-3c5f-479f-b606-d6e471bb7696 | 85%+ Verdicchio, white |
| 8 | Lugana | 2174 | c9b67c82-b71e-4453-b058-8af4cbcf1422 | 90%+ Turbiana (Trebbiano di Soave), white |
| 9 | Cesanese del Piglio / Piglio | 1021 | d6e82f1a-cb2c-4edb-82e7-5024d0483d74 | 90%+ Cesanese Affile/comune, red |
| 10 | Trebbiano d'Abruzzo | 2299 | 84117ba3-2808-4616-9434-19a4e9eac5ae | 85%+ Trebbiano abruzzese/Bombino/Trebbiano toscano, white |
| 11 | Cerasuolo di Vittoria | 1020 | 8baccbec-2b2a-4e20-8562-83101164fa5d | 50-70% Nero d'Avola + 30-50% Frappato, red |
| 12 | Frascati Superiore | 1038 | df2ed21b-521f-4e94-b682-addf99f8ba30 | 70%+ Malvasia Candia/Lazio + 30% complement, white |
| 13 | Offida | 1049 | 15a4e1c5-0f5f-4567-add7-16595c3fe66c | Pecorino 85+ / Passerina 85+ / rosso Montepulciano 85+ |
| 14 | Conegliano Valdobbiadene - Prosecco | 1029 | 42756499-9ef1-4bdf-9500-77554d734324 | 85%+ Glera, sparkling |
| 15 | Montefalco | 2198 | 2c9b00cd-ed65-4547-b24a-60452e75a3f5 | bianco: Grechetto 50%+ + Trebbiano 20-35%; rosso: Sangiovese 60-70% + Sagrantino 10-15% |
| 16 | Trento | 2301 | a5a8277a-8b12-4aae-910a-4aba9a150b7d | Chardonnay/Pinot bianco/Pinot nero/Meunier, spumante |
| 17 | Alta Langa | 1003 | c4695c4e-e3f3-4f79-8fa1-8510c1babda4 | 90-100% Pinot nero/Chardonnay, spumante |
| 18 | Alto Adige (umbrella) | 2010 | b8297e3c-4467-4c2b-8ac3-8952e0cb9891 | umbrella, varietal 85%+ rules |
| 19 | Trentino (umbrella) | 2300 | 19a8b3f5-cacc-4615-a066-27110407ecc7 | umbrella, bianco/rosso/kretzer |
| 20 | Collio Goriziano (umbrella) | 2101 | d3281bfd-a332-44c6-9527-0175866bbdd7 | umbrella, 16+ varieties |
| 21 | Friuli Isonzo (umbrella) | 2136 | b6824ed5-1c28-4a41-980f-ee8947c58dfc | umbrella, 20+ varieties |
| 22 | Colli Tortonesi (umbrella) | 2094 | b4579f42-af01-407a-8d7c-d15c029e8e2a | umbrella, Barbera/Dolcetto/Timorasso/Cortese varietal rules |

### To undo batch 11 rules

```sql
DELETE FROM appellation_rules
WHERE source_url IN (
  'http://catalogoviti.politicheagricole.it/scheda_denom.php?t=dsc&q=2002',
  'http://catalogoviti.politicheagricole.it/scheda_denom.php?t=dsc&q=2020',
  'http://catalogoviti.politicheagricole.it/scheda_denom.php?t=dsc&q=1039',
  'http://catalogoviti.politicheagricole.it/scheda_denom.php?t=dsc&q=2236',
  'http://catalogoviti.politicheagricole.it/scheda_denom.php?t=dsc&q=2200',
  'http://catalogoviti.politicheagricole.it/scheda_denom.php?t=dsc&q=2320',
  'http://catalogoviti.politicheagricole.it/scheda_denom.php?t=dsc&q=2321',
  'http://catalogoviti.politicheagricole.it/scheda_denom.php?t=dsc&q=2174',
  'http://catalogoviti.politicheagricole.it/scheda_denom.php?t=dsc&q=1021',
  'http://catalogoviti.politicheagricole.it/scheda_denom.php?t=dsc&q=2299',
  'http://catalogoviti.politicheagricole.it/scheda_denom.php?t=dsc&q=1020',
  'http://catalogoviti.politicheagricole.it/scheda_denom.php?t=dsc&q=1038',
  'http://catalogoviti.politicheagricole.it/scheda_denom.php?t=dsc&q=1049',
  'http://catalogoviti.politicheagricole.it/scheda_denom.php?t=dsc&q=1029',
  'http://catalogoviti.politicheagricole.it/scheda_denom.php?t=dsc&q=2198',
  'http://catalogoviti.politicheagricole.it/scheda_denom.php?t=dsc&q=2301',
  'http://catalogoviti.politicheagricole.it/scheda_denom.php?t=dsc&q=1003',
  'http://catalogoviti.politicheagricole.it/scheda_denom.php?t=dsc&q=2010',
  'http://catalogoviti.politicheagricole.it/scheda_denom.php?t=dsc&q=2300',
  'http://catalogoviti.politicheagricole.it/scheda_denom.php?t=dsc&q=2101',
  'http://catalogoviti.politicheagricole.it/scheda_denom.php?t=dsc&q=2136',
  'http://catalogoviti.politicheagricole.it/scheda_denom.php?t=dsc&q=2094'
);

-- appellation_grapes rollback via same URL list:
DELETE FROM appellation_grapes
WHERE source_url IN (<same URL list>);
```

### Batch 11 cascades

**Color cascades** (all NULL-only fills):

| Appellation | Color Set | Total Wines |
|---|---|---|
| Aglianico del Vulture | red | 154 |
| Barbera d'Alba | red | 845 (2 pre-existing white left intact) |
| Gattinara | red | 132 (2 pre-existing white left intact) |
| Primitivo di Manduria | red | 136 |
| Montepulciano d'Abruzzo | red | 351 (2 pre-existing white left intact) |
| Cerasuolo di Vittoria | red | 61 |
| Cesanese del Piglio | red | 39 (1 pre-existing white left intact) |
| Verdicchio dei Castelli di Jesi | white | 174 (5 pre-existing red left intact) |
| Verdicchio di Matelica | white | 44 |
| Lugana | white | 81 (3 pre-existing red left intact) |
| Trebbiano d'Abruzzo | white | 153 (7 pre-existing red left intact) |
| Frascati Superiore | white | 20 (4 pre-existing red left intact) |

**Total wines with color in batch 11 appellations: 2,190** (pre-existing wrongs left alone per no-overwrite rule — flagged for future cleanup).

To reverse a specific cascade:

```sql
-- Reverse red cascade for Aglianico del Vulture
UPDATE wines SET color = NULL, updated_at = NOW()
WHERE appellation_id = 'b9960dcb-a445-41ad-ade2-76d5f7af3996'
  AND color = 'red'
  AND updated_at > '2026-04-05 21:30:00+00'::timestamptz;  -- batch 11 window
```

**Varietal_category_id cascades** (NULL-only):

| Appellation | Category |
|---|---|
| Aglianico del Vulture | Aglianico (3a9dcf56-...) |
| Barbera d'Alba | Barbera (1db7eed4-...) |
| Gattinara | Nebbiolo (97a82ffb-...) |
| Primitivo di Manduria | Primitivo (1a54bde6-...) |
| Montepulciano d'Abruzzo | Montepulciano (496537db-...) |
| Verdicchio Castelli di Jesi | Verdicchio (03ce1738-...) |
| Verdicchio di Matelica | Verdicchio (03ce1738-...) |
| Lugana | Trebbiano (6722bd7c-...) |
| Trebbiano d'Abruzzo | Trebbiano (6722bd7c-...) |
| Conegliano Valdobbiadene Prosecco | Prosecco (7b38ca9d-...) |

To reverse: `UPDATE wines SET varietal_category_id = NULL WHERE appellation_id = '...' AND varietal_category_id = '...'` per pair.

**Wine_grapes 100% cascade** (ONE appellation only — Aglianico del Vulture 100% Aglianico):

79 new rows created at `percentage=100, percentage_source='adeb8cb4-6d80-42f1-9962-d13340f82978'` (MASAF source_type).

```sql
-- Rollback
DELETE FROM wine_grapes
WHERE grape_id = '951f1441-a2a0-4b03-98c0-bff93cd6f046'  -- Aglianico
  AND percentage = 100
  AND percentage_source = 'adeb8cb4-6d80-42f1-9962-d13340f82978'  -- MASAF
  AND wine_id IN (SELECT id FROM wines WHERE appellation_id = 'b9960dcb-a445-41ad-ade2-76d5f7af3996');
```

Clean provenance: every batch 11 row is identifiable by (`source_url LIKE '%catalogoviti%'` AND `source_accessed_date='2026-04-05'`) for rules/grapes, and by (`percentage_source='adeb8cb4-6d80-42f1-9962-d13340f82978'`) for wine_grapes.

---

## Batch 12 (2026-04-05 very late 2): 17 more Italian DOCs/DOCGs via MASAF catalogoviti

### Appellations seeded (17)

| # | Appellation | q ID | appellation_id | Rule |
|---|---|---|---|---|
| 1 | Rosso di Montalcino | 2250 | 28973a7d-f586-418f-93e9-877c596da9fd | 100% Sangiovese, red |
| 2 | Nebbiolo d'Alba | 2211 | 4096834f-7de2-49ab-b102-180474d0ab48 | 100% Nebbiolo, red |
| 3 | Dolcetto d'Alba | 2115 | 8385e4da-7703-44ef-8c7e-ee662487eb80 | 100% Dolcetto, red |
| 4 | Cannonau di Sardegna | 2049 | 13539eea-4d25-4da4-a77c-a5f1af45b482 | 85%+ Cannonau (Grenache), red |
| 5 | Romagna Albana | 1058 | 3006ac12-8d74-497f-a7e9-971ad0e8d613 | 95%+ Albana, white |
| 6 | Valpolicella Ripasso | 2314 | a6b38cfe-7cbe-44b5-86e3-ef12fcffcbd2 | Corvina 45-95% + Corvinone (sub) + Rondinella 5-30%, red |
| 7 | Salice Salentino | 2258 | 78bc7c98-a29f-4f1e-a5fa-3e04e3cad941 | 75%+ Negroamaro, red/rosato |
| 8 | Faro | 2128 | 8cb5f577-f930-4654-8ee1-75d3f989db30 | Nerello Mascalese 45-60 + Nocera 5-10 + Nerello Cappuccio 15-30, red |
| 9 | Vin Santo del Chianti Classico | 2331 | a0214fa1-5514-4016-9be8-d10937696593 | Trebbiano+Malvasia 60%+; Occhio di Pernice 80%+ Sangiovese |
| 10 | Gioia del Colle | 2145 | 20a3547f-7fd2-4801-ace7-55a0e29d4265 | multi-subtype (Primitivo 50-60, bianco Trebbiano 50-70, etc.) |
| 11 | Monferrato DOC | 2192 | 641c9dc1-624b-4434-a6f4-de78acbcf732 | umbrella |
| 12 | Sannio | 2266 | ae024b44-4e4e-4c0f-9c2b-226f85125a4c | umbrella |
| 13 | Valle d'Aosta | 2311 | ea1fca59-0f76-436b-bb91-f1196bfbdfd7 | umbrella (19+ varietal specifications) |
| 14 | Abruzzo DOC | 2001 | 97bc797a-f953-4a8b-ad4a-db8d9ed33276 | multi-subtype |
| 15 | Cortona | 2109 | 07b7489d-c8b8-4f2b-b649-830ce8569fb9 | multi-subtype (Syrah/Merlot blend + varietals) |
| 16 | Colline Novaresi | 2099 | f145b094-2eb1-4b2b-a087-0a154a891410 | multi-subtype |
| 17 | Marsala | 2183 | fa643a34-c81a-4aee-bd44-2096d070cadf | fortified, oro/ambra (whites) + rubino (reds) |

### To undo batch 12 rules

```sql
DELETE FROM appellation_rules WHERE source_url IN (
  'http://catalogoviti.politicheagricole.it/scheda_denom.php?t=dsc&q=2250',
  'http://catalogoviti.politicheagricole.it/scheda_denom.php?t=dsc&q=2211',
  'http://catalogoviti.politicheagricole.it/scheda_denom.php?t=dsc&q=2115',
  'http://catalogoviti.politicheagricole.it/scheda_denom.php?t=dsc&q=2049',
  'http://catalogoviti.politicheagricole.it/scheda_denom.php?t=dsc&q=1058',
  'http://catalogoviti.politicheagricole.it/scheda_denom.php?t=dsc&q=2314',
  'http://catalogoviti.politicheagricole.it/scheda_denom.php?t=dsc&q=2258',
  'http://catalogoviti.politicheagricole.it/scheda_denom.php?t=dsc&q=2128',
  'http://catalogoviti.politicheagricole.it/scheda_denom.php?t=dsc&q=2331',
  'http://catalogoviti.politicheagricole.it/scheda_denom.php?t=dsc&q=2145',
  'http://catalogoviti.politicheagricole.it/scheda_denom.php?t=dsc&q=2192',
  'http://catalogoviti.politicheagricole.it/scheda_denom.php?t=dsc&q=2266',
  'http://catalogoviti.politicheagricole.it/scheda_denom.php?t=dsc&q=2311',
  'http://catalogoviti.politicheagricole.it/scheda_denom.php?t=dsc&q=2001',
  'http://catalogoviti.politicheagricole.it/scheda_denom.php?t=dsc&q=2109',
  'http://catalogoviti.politicheagricole.it/scheda_denom.php?t=dsc&q=2099',
  'http://catalogoviti.politicheagricole.it/scheda_denom.php?t=dsc&q=2183'
);
```

### Batch 12 cascades

**Color cascades**: 7 appellations → 1,432 wines. Pre-existing wrongs left intact:
2 white Nebbiolo d'Alba, 1 white Valpolicella Ripasso, 1 white Cannonau di Sardegna, 2 red Romagna Albana.

**varietal_category_id**: Rosso di Montalcino → Sangiovese, Nebbiolo d'Alba → Nebbiolo.

**wine_grapes 100%** (3 true 100% appellations only): Rosso di Montalcino + Nebbiolo d'Alba + Dolcetto d'Alba — all with `percentage_source='adeb8cb4-6d80-42f1-9962-d13340f82978'` (MASAF source_type).

### Batch 12 running totals after application

- appellation_rules: 140 → **157** (+17)
- appellation_grapes with full provenance: 673 → **688** (+15)
- All rules have 100% provenance; 0 duplicates per appellation_id.

---

## Batch 13 (2026-04-05 loop-cycle 9): 24 more Italian DOCs/DOCGs from MASAF

### Appellations seeded (24)

Sicilia DOC (q=2274), Romagna DOC (q=2245), Recioto della Valpolicella DOCG (q=1054), Venezia DOC (q=2319), Cerasuolo d'Abruzzo (q=2063), Colli Piacentini (q=2092), Bianco di Custoza (q=2028), Rosso di Montepulciano (q=2251), Garda DOC (q=2142), Carso (q=2055), Vesuvio DOC (q=2325), Castel del Monte (q=2058), Erbaluce di Caluso DOCG (q=1035), Orvieto DOC (q=2222), Ischia DOC (q=2157), Ghemme DOCG (q=1041), Montecucco DOC (q=2197), Sforzato di Valtellina DOCG (q=1062), Vittoria DOC (q=2335), Lacrima di Morro d'Alba (q=2158), Valtellina Rosso (q=2316), Colli di Luni (q=2081), Lambrusco di Sorbara DOC (q=2161), Lambrusco Grasparossa di Castelvetro (q=2162).

### Batch 13 cascades

- **RED** color cascades: Recioto della Valpolicella, Rosso di Montepulciano, Ghemme, Sforzato di Valtellina, Valtellina Rosso, Lacrima di Morro d'Alba
- **WHITE** color cascades: Bianco di Custoza, Erbaluce di Caluso, Orvieto
- **Cerasuolo d'Abruzzo skipped** — legally rosato, color cascade risky without knowing wines.color enum support for 'rose'/'rosato'
- **varietal_category_id**: Rosso di Montepulciano → Sangiovese, Ghemme/Sforzato/Valtellina Rosso → Nebbiolo, Cerasuolo d'Abruzzo → Montepulciano
- **wine_grapes 100% cascade**: Erbaluce di Caluso (true 100% single-variety)

### To undo batch 13 rules

```sql
DELETE FROM appellation_rules WHERE source_url IN (
  'http://catalogoviti.politicheagricole.it/scheda_denom.php?t=dsc&q=2274',
  'http://catalogoviti.politicheagricole.it/scheda_denom.php?t=dsc&q=2245',
  'http://catalogoviti.politicheagricole.it/scheda_denom.php?t=dsc&q=1054',
  'http://catalogoviti.politicheagricole.it/scheda_denom.php?t=dsc&q=2319',
  'http://catalogoviti.politicheagricole.it/scheda_denom.php?t=dsc&q=2063',
  'http://catalogoviti.politicheagricole.it/scheda_denom.php?t=dsc&q=2092',
  'http://catalogoviti.politicheagricole.it/scheda_denom.php?t=dsc&q=2028',
  'http://catalogoviti.politicheagricole.it/scheda_denom.php?t=dsc&q=2251',
  'http://catalogoviti.politicheagricole.it/scheda_denom.php?t=dsc&q=2142',
  'http://catalogoviti.politicheagricole.it/scheda_denom.php?t=dsc&q=2055',
  'http://catalogoviti.politicheagricole.it/scheda_denom.php?t=dsc&q=2325',
  'http://catalogoviti.politicheagricole.it/scheda_denom.php?t=dsc&q=2058',
  'http://catalogoviti.politicheagricole.it/scheda_denom.php?t=dsc&q=1035',
  'http://catalogoviti.politicheagricole.it/scheda_denom.php?t=dsc&q=2222',
  'http://catalogoviti.politicheagricole.it/scheda_denom.php?t=dsc&q=2157',
  'http://catalogoviti.politicheagricole.it/scheda_denom.php?t=dsc&q=1041',
  'http://catalogoviti.politicheagricole.it/scheda_denom.php?t=dsc&q=2197',
  'http://catalogoviti.politicheagricole.it/scheda_denom.php?t=dsc&q=1062',
  'http://catalogoviti.politicheagricole.it/scheda_denom.php?t=dsc&q=2335',
  'http://catalogoviti.politicheagricole.it/scheda_denom.php?t=dsc&q=2158',
  'http://catalogoviti.politicheagricole.it/scheda_denom.php?t=dsc&q=2316',
  'http://catalogoviti.politicheagricole.it/scheda_denom.php?t=dsc&q=2081',
  'http://catalogoviti.politicheagricole.it/scheda_denom.php?t=dsc&q=2161',
  'http://catalogoviti.politicheagricole.it/scheda_denom.php?t=dsc&q=2162'
);
```

### Batch 13 running totals

- appellation_rules: 157 → **181** (+24)
- appellation_grapes with full provenance: 688 → **717** (+29)
- 0 duplicates, 100% provenance coverage.

---

## Batch 14 (2026-04-05 loop-cycle 10): 21 more Italian DOCs/DOCGs from MASAF

### Appellations seeded (21)

Brachetto d'Acqui DOCG (q=1012), Carmignano DOCG (q=1015), Ruchè di Castagnole Monferrato DOCG (q=1060), Ramandolo DOCG (q=1053), Vernaccia di Serrapetrona DOCG (q=1072), Dolcetto di Diano d'Alba (q=1032), Cinque Terre / Sciacchetrà (q=2068), Colli Bolognesi (q=2075), Rosso Piceno / Piceno (q=2253), Sant'Antimo (q=2267), Breganze (q=2039), Riviera Ligure di Ponente (q=2243), Cirò (q=2070), Grignolino d'Asti (q=2151), Cortese dell'Alto Monferrato (q=2107), Copertino (q=2105), Malvasia delle Lipari (q=2175), Monica di Sardegna (q=2193), Nuragus di Cagliari (q=2215), Freisa d'Asti (q=2130), Aleatico di Puglia (q=2007).

**Skipped** (already seeded in earlier batches): Asti DOCG (already via Valoritalia), Barbera d'Asti DOCG (already in earlier MASAF batch), Brunello di Montalcino (already via Valoritalia).

### Cascades

- **RED**: Carmignano, Ruchè, Dolcetto di Diano d'Alba, Rosso Piceno, Grignolino d'Asti, Monica di Sardegna, Freisa d'Asti, Aleatico di Puglia
- **WHITE**: Ramandolo, Cinque Terre, Cortese dell'Alto Monferrato, Nuragus di Cagliari, Malvasia delle Lipari
- **varietal_category_id**: Carmignano → Sangiovese
- **wine_grapes 100% cascade**: Dolcetto di Diano d'Alba, Ramandolo (Verduzzo Friulano), Freisa d'Asti (true 100% single-variety each)

### To undo batch 14 rules

```sql
DELETE FROM appellation_rules WHERE source_url IN (
  'http://catalogoviti.politicheagricole.it/scheda_denom.php?t=dsc&q=1012',
  'http://catalogoviti.politicheagricole.it/scheda_denom.php?t=dsc&q=1015',
  'http://catalogoviti.politicheagricole.it/scheda_denom.php?t=dsc&q=1060',
  'http://catalogoviti.politicheagricole.it/scheda_denom.php?t=dsc&q=1053',
  'http://catalogoviti.politicheagricole.it/scheda_denom.php?t=dsc&q=1072',
  'http://catalogoviti.politicheagricole.it/scheda_denom.php?t=dsc&q=1032',
  'http://catalogoviti.politicheagricole.it/scheda_denom.php?t=dsc&q=2068',
  'http://catalogoviti.politicheagricole.it/scheda_denom.php?t=dsc&q=2075',
  'http://catalogoviti.politicheagricole.it/scheda_denom.php?t=dsc&q=2253',
  'http://catalogoviti.politicheagricole.it/scheda_denom.php?t=dsc&q=2267',
  'http://catalogoviti.politicheagricole.it/scheda_denom.php?t=dsc&q=2039',
  'http://catalogoviti.politicheagricole.it/scheda_denom.php?t=dsc&q=2243',
  'http://catalogoviti.politicheagricole.it/scheda_denom.php?t=dsc&q=2070',
  'http://catalogoviti.politicheagricole.it/scheda_denom.php?t=dsc&q=2151',
  'http://catalogoviti.politicheagricole.it/scheda_denom.php?t=dsc&q=2107',
  'http://catalogoviti.politicheagricole.it/scheda_denom.php?t=dsc&q=2105',
  'http://catalogoviti.politicheagricole.it/scheda_denom.php?t=dsc&q=2175',
  'http://catalogoviti.politicheagricole.it/scheda_denom.php?t=dsc&q=2193',
  'http://catalogoviti.politicheagricole.it/scheda_denom.php?t=dsc&q=2215',
  'http://catalogoviti.politicheagricole.it/scheda_denom.php?t=dsc&q=2130',
  'http://catalogoviti.politicheagricole.it/scheda_denom.php?t=dsc&q=2007'
);
```

### Batch 14 running totals

- appellation_rules: 181 → **202** (+21)
- appellation_grapes with full provenance: 717 → **739** (+22)
- 0 duplicates, 100% provenance coverage.

---

## Batch 15 (2026-04-05 loop-cycle 11): 17 Burgundy AOCs via INAO CDCs

### Appellations seeded (17)

Beaune, Chambolle-Musigny, Corton, Santenay, Saint-Aubin, Morey-Saint-Denis, Mercurey, Marsannay, Vosne-Romanée, Rully, Pernand-Vergelesses, Clos de Vougeot / Clos Vougeot, Aloxe-Corton, Corton-Charlemagne, Savigny-lès-Beaune, Auxey-Duresses, Echezeaux.

Source URL pattern: `https://extranet.inao.gouv.fr/fichier/{PNOCDC-Name.pdf}` — various naming conventions (PNOCDC{Name}.pdf, PNOCDC-{Name}.pdf, PNOCDC{CompactName}.pdf).

### Cascades

- **Color** (grand cru + single-color village only): Chambolle-Musigny (red), Vosne-Romanée (red), Echezeaux (red grand cru), Corton-Charlemagne (white grand cru). Others have both colors; no cascade.
- **varietal_category_id**: Chambolle-Musigny, Vosne-Romanée, Echezeaux → Pinot Noir; Corton-Charlemagne → Chardonnay.

### To undo batch 15 rules

```sql
DELETE FROM appellation_rules WHERE source_url IN (
  'https://extranet.inao.gouv.fr/fichier/PNOCDC-Beaune.pdf',
  'https://extranet.inao.gouv.fr/fichier/PNOCDC-Chambolle-Musigny.pdf',
  'https://extranet.inao.gouv.fr/fichier/PNOCDCCorton.pdf',
  'https://extranet.inao.gouv.fr/fichier/PNOCDCSantenay.pdf',
  'https://extranet.inao.gouv.fr/fichier/PNOCDCSaint-Aubin.pdf',
  'https://extranet.inao.gouv.fr/fichier/PNOCDC-MoreySaintDenis.pdf',
  'https://extranet.inao.gouv.fr/fichier/PNOCDCMercurey.pdf',
  'https://extranet.inao.gouv.fr/fichier/PNOCDC-Marsannay.pdf',
  'https://extranet.inao.gouv.fr/fichier/PNOCDCVosne-Romanee.pdf',
  'https://extranet.inao.gouv.fr/fichier/PNOCDCRully.pdf',
  'https://extranet.inao.gouv.fr/fichier/PNOCDCPernand-Vergelesses.pdf',
  'https://extranet.inao.gouv.fr/fichier/PNOCDCVougeot.pdf',
  'https://extranet.inao.gouv.fr/fichier/PNOCDCAloxeCorton.pdf',
  'https://extranet.inao.gouv.fr/fichier/PNOCDCCorton-Charlemagne.pdf',
  'https://extranet.inao.gouv.fr/fichier/PNOCDCSavigny-les-Beaune.pdf',
  'https://extranet.inao.gouv.fr/fichier/PNOCDC-Auxey-Duresses.pdf',
  'https://extranet.inao.gouv.fr/fichier/PNOCDCEchezeaux.pdf'
);
```

### Batch 15 running totals

- appellation_rules: 202 → **219** (+17)
- appellation_grapes with full provenance: 739 → **770** (+31)
- 0 duplicates, 100% provenance coverage.

---

## Batch 16 (2026-04-05 loop-cycle 12): 11 major French AOCs via INAO CDCs

### Appellations seeded (11)

Alsace / Vin d'Alsace, Bordeaux, Côtes du Rhône, Arbois (Jura), Chinon, Touraine, Saumur, Languedoc, Beaujolais, Côtes du Rhône Villages, Coteaux Champenois.

Source URL patterns:
- PNOCDCAlsace.pdf, PNOCDCLanguedoc.pdf, PNOCDCBeaujolais.pdf, PNOCDCCotesduRhoneVillages.pdf, PNOCDCCoteauxChampenois.pdf
- PNOCDC-Cotes-du-Rhone.pdf, PNOCDC-Touraine.pdf, PNOCDC-Saumur.pdf
- 3-CDC-Bordeaux.pdf (Bordeaux)
- PNO-CdcArbois-cn220210.pdf (Arbois)
- CPAOV-2017-224-Chinon.pdf (Chinon)

### Cascades

**None** — all 11 are multi-color umbrellas (white + red + rosé subtypes). No strictly single-color cascade possible. Rule text and primary grape rows seeded; no wine_grapes or color changes.

### To undo batch 16 rules

```sql
DELETE FROM appellation_rules WHERE source_url IN (
  'https://extranet.inao.gouv.fr/fichier/PNOCDCAlsace.pdf',
  'https://extranet.inao.gouv.fr/fichier/3-CDC-Bordeaux.pdf',
  'https://extranet.inao.gouv.fr/fichier/PNOCDC-Cotes-du-Rhone.pdf',
  'https://extranet.inao.gouv.fr/fichier/PNO-CdcArbois-cn220210.pdf',
  'https://extranet.inao.gouv.fr/fichier/CPAOV-2017-224-Chinon.pdf',
  'https://extranet.inao.gouv.fr/fichier/PNOCDC-Touraine.pdf',
  'https://extranet.inao.gouv.fr/fichier/PNOCDC-Saumur.pdf',
  'https://extranet.inao.gouv.fr/fichier/PNOCDCLanguedoc.pdf',
  'https://extranet.inao.gouv.fr/fichier/PNOCDCBeaujolais.pdf',
  'https://extranet.inao.gouv.fr/fichier/PNOCDCCotesduRhoneVillages.pdf',
  'https://extranet.inao.gouv.fr/fichier/PNOCDCCoteauxChampenois.pdf'
);
```

### Batch 16 running totals

- appellation_rules: 219 → **230** (+11)
- appellation_grapes with full provenance: 770 → **797** (+27)
- 0 duplicates, 100% provenance coverage.

---

## Audit results (2026-04-05 late)

Clean audit after session 2:

| Check | Result |
|---|---|
| `appellation_rules` total | 68 (0 duplicates per appellation_id) |
| `appellation_rules` with full provenance | 68/68 (100%) |
| `appellation_grapes` with full provenance | 401 (0 missing source_url or source_text_excerpt) |
| Duplicate `appellation_grapes` rows | 0 |
| Wine colors violating appellation rules | ~975 (pre-existing — NOT caused by session cascades) |
| "Red grape in white-only rule" mismatches | 2 (Sauvignon Gris in Sauternes/Barsac — `grapes.color='red'` data quality bug; Sauvignon Gris is a pink-berried white mutation used exclusively for white wines) |
| "White grape in red-only rule" mismatches | 19 (all are legitimate field-blend accessories per legal CDC: Viognier in Côte-Rôtie; Aligoté/Chardonnay/Melon in Beaujolais crus; Pinot Blanc/Pinot Gris/Chardonnay in Burgundy red climats — all explicitly authorized in their respective CDC texts) |

**Illegal color breakdown (975 total, all pre-existing — not touched by session)**:
- Champagne red: 800 (biggest single block)
- Chablis red: 50
- Prosecco red: 25 (newly flagged by batch 4 Prosecco rule)
- Pouilly-Fumé red: 14, Soave red: 13, Pouilly-Fuissé red: 11, Vouvray red: 11
- Barolo rose: 9, Sauternes red: 7, Barolo white: 5
- Smaller counts (1-3 each) across ~15 other appellations
- These are cleanup targets for a future session, NOT bugs caused by Path A.

---

## Exercises 5 + 4 (2026-04-05): region/country grapes cascade + containment expansion

### Exercise 5: region_grapes + country_grapes cascade

```sql
-- Undo country_grapes cascade (+82 rows)
DELETE FROM country_grapes WHERE notes LIKE 'Cascaded from region_grapes%';

-- Undo region_grapes cascade (+223 rows)
DELETE FROM region_grapes WHERE notes LIKE 'Cascaded from appellation_grapes%';

-- Restore 'required' association_type on pre-existing rows (33 region_grapes + 12 country_grapes)
-- NOTE: these were changed to 'typical' per decision that regions/countries don't have legal requirements
-- Reverting this would restore the original (incorrect) values. Only do this if the decision is reversed.
```

### Exercise 4 Batch 1: Bordeaux + Bourgogne + Rhône + Portugal containment (+162 rows)

```sql
-- Undo all Exercise 4 containment (added with source='curated')
-- Bordeaux umbrella: Bordeaux AOC → 38 children
DELETE FROM appellation_containment
WHERE parent_id = 'c22540df-2860-4a43-966a-3439cb6d9e82'
AND source = 'curated'
AND child_id NOT IN (
  -- Preserve any pre-existing curated rows (none expected, but safe)
  SELECT child_id FROM appellation_containment WHERE parent_id = 'c22540df-2860-4a43-966a-3439cb6d9e82' AND source = 'explicit'
);

-- Saint-Emilion → Saint-Emilion Grand Cru
DELETE FROM appellation_containment
WHERE parent_id = '911c087c-fcff-4021-9f8e-0dbfe7ba6dd6'
AND child_id = '751fc99d-73d7-4e85-a72e-dea69dc7205d';

-- Bourgogne umbrella: Bourgogne AOC → 82 children
DELETE FROM appellation_containment
WHERE parent_id = 'cb6e8610-119b-48de-a708-a29f880ac864'
AND source = 'curated';

-- Côtes du Rhône umbrella: → 18 children
DELETE FROM appellation_containment
WHERE parent_id = '945b5857-0f6e-4155-a6e8-d353d28a9209'
AND source = 'curated';

-- CDR Villages → 8 named village crus
DELETE FROM appellation_containment
WHERE parent_id = '5139f8ab-be53-4956-8064-fdef1a280387'
AND source = 'curated';

-- Portuguese containment: Douro→Porto, Algarve→4, Açores→3, Lisboa→7
DELETE FROM appellation_containment
WHERE parent_id IN (
  'd9f7deec-cff6-45fd-8d91-c3be9936bd39',  -- Douro
  'c9c8fcc2-669d-4762-a9b8-b8d63e5a88fc',  -- Algarve
  '8898d517-d603-4bee-b644-6f200ca21874',  -- Açores
  '358fcc6f-4c00-44f5-ab5d-637651a8affc'   -- Lisboa
)
AND source = 'curated';
```
