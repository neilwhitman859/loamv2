# Loop: US AVA Rules + Non-US Appellation Rules

Recurring loop prompt. Runs every 10 minutes. Processes appellation rules from legal sources.

## SELF-TERMINATION CHECK (run FIRST every cycle)

```sql
-- Check remaining US AVAs without rules
SELECT COUNT(*) as remaining_us_avas
FROM appellations a
LEFT JOIN appellation_rules ar ON ar.appellation_id = a.id
WHERE a.country_id = 'f30a9c62-b8df-41fe-b226-563dbcd56023'
AND ar.id IS NULL
AND EXISTS (SELECT 1 FROM wines w WHERE w.appellation_id = a.id);

-- Check remaining non-US appellations without rules (France, Australia, South Africa, Spain, Greece)
SELECT c.name as country, COUNT(*) as remaining
FROM appellations a
JOIN countries c ON a.country_id = c.id
LEFT JOIN appellation_rules ar ON ar.appellation_id = a.id
WHERE c.name IN ('France', 'Australia', 'South Africa', 'Spain', 'Greece')
AND ar.id IS NULL
AND EXISTS (SELECT 1 FROM wines w WHERE w.appellation_id = a.id)
GROUP BY c.name ORDER BY remaining DESC;
```

If remaining_us_avas = 0 AND all non-US targets are 0, report final summary and STOP.

## Supabase project: vgbppjhmvbggfjztzobl

---

## PHASE 2: US AVA Rules (while remaining_us_avas > 0)

Each cycle: process **20 US AVAs** without rules, ordered by wine count descending.

### Step 1: Get next batch
```sql
SELECT a.id, a.name, COUNT(w.id) as wine_count
FROM appellations a
LEFT JOIN appellation_rules ar ON ar.appellation_id = a.id
JOIN wines w ON w.appellation_id = a.id
WHERE a.country_id = 'f30a9c62-b8df-41fe-b226-563dbcd56023'
AND ar.id IS NULL
GROUP BY a.id, a.name
ORDER BY COUNT(w.id) DESC
LIMIT 20;
```

### Step 2: Search for each AVA

Run WebSearch queries in parallel batches of 5. For each AVA:
- Search: `"{AVA name}" AVA established year area acres CFR section TTB`
- Extract: established year, total area in acres, elevation range if available, state(s), key soils/climate

For the CFR section, search: `"{AVA name}" site:ecfr.gov part 9`

### Step 3: Insert appellation_rules

For each AVA, insert one row:
```sql
INSERT INTO appellation_rules (id, appellation_id, rules, source_url, source_organization, source_document_title, source_accessed_date, source_text_excerpt)
VALUES (
  gen_random_uuid(), '{appellation_id}',
  '{rules_jsonb}'::jsonb,
  'https://www.ecfr.gov/current/title-27/chapter-I/subchapter-A/part-9/subpart-C/section-{cfr_num}',
  'TTB (Alcohol and Tobacco Tax and Trade Bureau)',
  '27 CFR §{cfr_num} — {AVA name}',
  CURRENT_DATE,
  'The {AVA name} viticultural area is located in {state}.'
) ON CONFLICT DO NOTHING;
```

**Rules JSONB structure for US AVAs:**
```json
{
  "designation_type": "AVA",
  "established_date": "YYYY-MM-DD",
  "boundary_summary": "Located in [county/state], [brief description].",
  "geographic_features": "[key climate, topography, distinguishing features]",
  "grape_source_rule": "85% minimum from the named viticultural area per 27 CFR 4.25a",
  "area_acres": 12345,
  "elevation_range_ft": {"min": 100, "max": 2000},
  "key_soils": ["soil type 1", "soil type 2"],
  "cfr_section": "9.XX"
}
```

Note: US AVAs do NOT mandate grape varieties, colors, or wine types. Do not invent varietal rules.
Leave fields NULL if data is not reliably available. NULL > wrong.

### Step 4: Update appellations table

```sql
UPDATE appellations SET
  established_year = {year},
  area_ha = ROUND({acres} / 2.471),
  elevation_min_m = ROUND({min_ft} * 0.3048),
  elevation_max_m = ROUND({max_ft} * 0.3048)
WHERE id = '{appellation_id}'
AND established_year IS NULL;
```

Only update fields you have data for. Leave NULL if not available.

---

## PHASE 3: Non-US Appellation Rules (after US AVAs complete)

Priority order: France → Australia → South Africa → Spain → Greece

Each cycle: process **15 non-US appellations** without rules, ordered by wine count descending.

**Note on French appellations:** ~73% of remaining French appellations are IGPs (permissive umbrellas, no strict grape mandates, no cascades needed). AOCs require the full treatment: appellation_grapes + cascades. Agents should identify IGP vs AOC for each appellation and apply the appropriate depth of work.

### Step 1: Get next batch
```sql
SELECT a.id, a.name, c.name as country, COUNT(w.id) as wine_count
FROM appellations a
JOIN countries c ON a.country_id = c.id
LEFT JOIN appellation_rules ar ON ar.appellation_id = a.id
JOIN wines w ON w.appellation_id = a.id
WHERE c.name IN ('France', 'Australia', 'South Africa', 'Spain', 'Greece')
AND ar.id IS NULL
GROUP BY a.id, a.name, c.name
ORDER BY COUNT(w.id) DESC
LIMIT 15;
```

### Step 2: Fetch rules from legal sources

Launch **3 parallel background agents of 5 appellations each**.

**France (IGP):** INAO extranet CDCs
- WebFetch: `https://extranet.inao.gouv.fr/fichier/PNO*IGP*{name}*.pdf` (try variations)
- Or WebSearch: `"{appellation name}" IGP INAO "cahier des charges" site:extranet.inao.gouv.fr`
- Source: INAO

**France (AOC):** INAO cahier des charges
- Try WebFetch: `https://www.inao.gouv.fr/` for CDC links
- Or WebSearch: `"{appellation name}" INAO cahier des charges grape varieties rules`
- Source: INAO (Institut National de l'Origine et de la Qualité)

**Australia (GI):** Wine Australia register
- WebSearch: `"{appellation name}" "Wine Australia" GI grapes varieties established`
- Source: Wine Australia GI Register

**South Africa (WO):** SAWIS Wine of Origin
- WebSearch: `"{appellation name}" "wine of origin" South Africa SAWIS established`
- Source: SAWIS (South African Wine Industry Information & Systems)

**Spain (DO/DOCa):** MAPA pliegos de condiciones
- WebSearch: `"{appellation name}" MAPA pliego condiciones uvas variedades`
- Source: MAPA (Ministerio de Agricultura, Pesca y Alimentación)

**Greece (PDO):** EU eAmbrosia or OPEKEPE
- WebSearch: `"{appellation name}" Greece PDO wine grapes varieties established`
- Source: EU eAmbrosia PDO register

### Step 3: Insert appellation_rules

For non-US appellations, the rules JSONB can include varietal mandates, color restrictions, etc.:
```json
{
  "designation_type": "AOC/GI/WO/DO/PDO",
  "established_date": "YYYY-MM-DD",
  "boundary_summary": "Located in [region], [country].",
  "geographic_features": "[soils, climate, elevation]",
  "colors": ["red", "white", "rosé"],
  "grape_rules": {
    "red": {"mode": "blend", "primary_grapes": ["Grape A", "Grape B"], "min_pct": 80},
    "white": {"mode": "single_variety", "grape": "Grape C", "min_pct": 100}
  },
  "area_ha": 1234,
  "cfr_section": null
}
```

For non-US rules, if grape varieties are confirmed from official sources:
- Insert `appellation_grapes` rows with provenance
- Run color cascade (only if appellation is strictly single-color AND it's a NULL-fill)
- Run varietal_category cascade if single-variety

### Step 4: Run cascades (non-US only, conservative)

For each non-US appellation where grape rules are unambiguous:
```sql
-- Color cascade (NULL-fill only, single-color appellations)
UPDATE wines SET color = '{color}'
WHERE appellation_id = '{appellation_id}'
AND color IS NULL
AND wine_type != 'sparkling';

-- Varietal category (single variety, exact name match)
UPDATE wines w SET varietal_category_id = vc.id
FROM varietal_categories vc
WHERE w.appellation_id = '{appellation_id}'
AND vc.name = '{primary_grape}'
AND w.varietal_category_id IS NULL;
```

---

## RULES (apply to all phases)

- **NULL > wrong.** Only insert data you can verify from the source.
- **ON CONFLICT DO NOTHING.** Never overwrite existing data.
- **Provenance required.** Every appellation_rules row must have source_url.
- **Conservative on cascades.** Only fill NULLs. Never overwrite existing color/category data.
- **Skip if uncertain.** If you can't find reliable source data for an appellation within 2 searches, skip it and move on.
- **Batch searches in parallel** where possible to reduce cycle time.

## LOGGING

Append to `data/stats/cron_loop_journal.md` at end of each cycle:
```
| N | Phase | [AVA/appellation names] | +X rules, +Y fields | [notes] |
```

## CYCLE-END STATUS MESSAGE

After appending the journal entry, always output a visible status summary:

```
**Cycle [N] complete** — +15 rules (total: [X])
Remaining: FR [n], SA [n], AU [n], ES [n], GR [n] = [total]
Notable: [1-2 sentence highlight of the most interesting appellation this cycle]
```

This ensures the user can see progress without reading tool calls.
