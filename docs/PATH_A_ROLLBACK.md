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
