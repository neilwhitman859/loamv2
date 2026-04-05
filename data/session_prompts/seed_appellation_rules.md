# Session: Seed appellation_rules + appellation_grapes from legal sources

Read CLAUDE.md first and give a briefing. Then read:
- docs/DECISIONS.md entry "No probabilistic inference on canonical columns"
- memory/feedback_no_probabilistic_inference.md
- data/session_prompts/next_session.md (this session is Path A from that file)

## Goal

Seed the empty `appellation_rules` and `appellation_grapes` tables with wine law
facts pulled ONLY from official regulatory sources. Once seeded, run definitional
cascades to fill varietal_category_id, wine_grapes, and color on wines whose
appellation_id is a regulated single-varietal or single-color appellation.

This is strictly definitional work — each fact must trace to a legal document.
No Wikipedia, no wine blogs, no marketing sites, no AI hallucination. Every
inserted row carries its source URL and an excerpt of the legal text it cites.

## Reputable sources — approved list

Use ONLY these. If a fact isn't in one of these, don't write it.

**EU (covers France, Italy, Spain, Portugal, Germany, Greece, Austria, Hungary):**
- **eAmbrosia** — the EU's official geographical indications register:
  https://ec.europa.eu/info/food-farming-fisheries/food-safety-and-quality/certification/quality-labels/geographical-indications-register/
  Has the legal "Single Document" and "Product Specification" for every EU
  PDO/PGI. This is the single authoritative source for EU wine appellations.

**France (supplementary to eAmbrosia):**
- **INAO** — Institut National de l'Origine et de la Qualité:
  https://www.inao.gouv.fr/ — publishes cahiers des charges (specification
  documents) for every AOC.

**Italy (supplementary to eAmbrosia):**
- **MASAF** (ex-MiPAAF) — disciplinari di produzione for DOC/DOCG:
  https://www.politicheagricole.it/ — search for "disciplinari".

**Spain (supplementary to eAmbrosia):**
- **Ministerio de Agricultura, Pesca y Alimentación** — pliegos de condiciones
  for DO/DOCa: https://www.mapa.gob.es/

**USA:**
- **TTB** — 27 CFR Part 9 for AVAs: https://www.ttb.gov/wine/ava-map-explorer
  and https://www.ecfr.gov/current/title-27/chapter-I/subchapter-A/part-9
  (AVAs are geographic designations only — they do NOT mandate varietals or
  color. Only 85% grape source rule applies. Do not invent rules for US AVAs.)

**Australia:** Wine Australia GI Register:
https://www.wineaustralia.com/labelling/register-of-protected-gis-and-other-terms

**New Zealand:** IPONZ GI Register:
https://www.iponz.govt.nz/about-ip/geographical-indications/

**South Africa:** SAWIS Wine of Origin scheme:
https://www.sawis.co.za/

**Argentina:** INV — Instituto Nacional de Vitivinicultura:
https://www.argentina.gob.ar/inv

**Chile:** SAG — Servicio Agrícola y Ganadero appellation regulations:
https://www.sag.gob.cl/

## Explicitly NOT approved

- Wikipedia
- Wine-Searcher, Vivino, Decanter, Wine Folly, Wine Enthusiast editorial pages
- Producer/importer marketing pages
- Generic "wine education" blogs
- AI-generated summaries of wine law (from any model, including you)
- "What I remember about Barolo" — even if widely known, cite the legal source

If you can't find the legal source for an appellation within 5-10 minutes of
searching, skip it and move on. Don't guess.

## Schema discovery (first step before any inserts)

Query the actual schemas before writing anything:

```sql
SELECT table_name, column_name, data_type, is_nullable
FROM information_schema.columns
WHERE table_schema='public'
  AND table_name IN ('appellation_rules', 'appellation_grapes')
ORDER BY table_name, ordinal_position;
```

Also inspect the `appellations` table — it already has some columns populated,
check what's there so you don't overwrite: `allowed_grapes_description`,
`max_yield_hl_ha` (930 populated), `min_aging_months`, `min_alcohol_pct`,
`elevation_min_m`, `elevation_max_m`.

Decide per field whether it lives on `appellations` directly or on a link
table. Don't duplicate.

## Provenance is mandatory

This is the hard rule for this session: every inserted row must carry its
citation inside the row. Session transcripts are not provenance.

1. After schema discovery, if `appellation_rules` and `appellation_grapes` do
   NOT already have source-tracking columns, add them via migration BEFORE
   seeding anything:

```sql
ALTER TABLE appellation_rules
  ADD COLUMN source_url text NOT NULL,
  ADD COLUMN source_organization text NOT NULL,
  ADD COLUMN source_document_title text,
  ADD COLUMN source_accessed_date date NOT NULL DEFAULT CURRENT_DATE,
  ADD COLUMN source_text_excerpt text,
  ADD COLUMN last_verified_at timestamptz DEFAULT now();

ALTER TABLE appellation_grapes
  ADD COLUMN source_url text NOT NULL,
  ADD COLUMN source_organization text NOT NULL,
  ADD COLUMN source_document_title text,
  ADD COLUMN source_accessed_date date NOT NULL DEFAULT CURRENT_DATE,
  ADD COLUMN source_text_excerpt text,
  ADD COLUMN last_verified_at timestamptz DEFAULT now();
```

   The `NOT NULL` on `source_url` and `source_organization` is intentional —
   it makes it physically impossible to insert a rule without citing a source.
   Update SCHEMA.md to reflect the new columns.

2. Every INSERT carries full provenance. No exceptions. If you can't cite the
   legal source URL, don't write the row.

3. Provenance field meanings:
   - `source_url` — the exact URL (eAmbrosia single document, INAO cahier des
     charges, TTB eCFR section, etc.). Must be resolvable.
   - `source_organization` — canonical label: "EU eAmbrosia", "INAO", "MASAF",
     "TTB 27 CFR Part 9", "Wine Australia GI Register", etc. Use consistent
     labels so you can filter by authority later.
   - `source_document_title` — e.g., "PDO Barolo — Product Specification"
   - `source_accessed_date` — when you read it. Laws change; freshness matters.
   - `source_text_excerpt` — literal paraphrased sentence from the legal doc
     (~100-300 chars). A reviewer must be able to verify the claim from this
     excerpt alone without re-fetching the URL.
   - `last_verified_at` — set on insert, updated when re-verified later.

4. When cascading from `appellation_rules` → `wine_grapes` or `wines.color`,
   populate the target's source column (e.g. `wine_grapes.percentage_source`)
   pointing back to the `appellation_rules.id` that sourced the cascade.
   Downstream data must be able to trace back to the original legal document.

5. At the end of the session, this query must return complete citations for
   every seeded rule:

```sql
SELECT ar.id, a.name as appellation,
       ar.source_organization, ar.source_url,
       ar.source_text_excerpt
FROM appellation_rules ar
JOIN appellations a ON a.id = ar.appellation_id
ORDER BY a.name;
```

   Run this query before wrap-up and verify every row has all five fields
   populated.

## Prioritization

Order by impact. Query to get top 100 appellations by linked wine count:

```sql
SELECT a.id, a.name, a.country_id, c.name as country, COUNT(w.id) as wine_count
FROM appellations a
LEFT JOIN wines w ON w.appellation_id = a.id
LEFT JOIN countries c ON c.id = a.country_id
GROUP BY a.id, a.name, a.country_id, c.name
ORDER BY wine_count DESC
LIMIT 100;
```

Start with the biggest. Seeding Barolo (many wines) has higher cascade value
than seeding a small Cru Bourgeois. eAmbrosia is the single best starting
point — it has standardized English-language "Single Document" PDFs for every
EU PDO/PGI. Expect 60-80% of high-value cascades to come from eAmbrosia alone.

## Scope per appellation

For each regulated appellation, extract from the legal source:

**To `appellation_grapes` (link table):**
- Each allowed grape, with `required_percentage` (100 for single-variety) or
  `min_percentage` / `max_percentage` where the law specifies a range
- `is_primary` flag for the dominant grape in blends (if the law names one)
- Full provenance on every row

**To `appellation_rules` (link table or structured column):**
- `min_alcohol_pct`
- `max_yield_hl_ha`
- `min_aging_months` (by aging category if law distinguishes, e.g., Rioja
  Crianza vs Reserva vs Gran Reserva)
- `elevation_min_m` / `elevation_max_m` where regulated
- Required color (red/white/rosé) where the appellation is single-color
- Full provenance on every row

## Validation gates

1. **First 5 appellations are a dry run.** Before any bulk insert, pick 5
   high-value appellations (e.g., Barolo, Chablis, Sancerre, Rioja, Brunello
   di Montalcino). For each:
   - Find the legal source URL in one of the approved registers
   - Fetch/read the document in-session
   - Paraphrase the relevant facts
   - Write the row with full provenance
   - Stop and show me the 5 seeded rows before continuing

2. **After each batch of 10 appellations**, run the cascade preview (do NOT
   execute the cascade yet):
   - How many wines would get varietal_category_id set?
   - How many wine_grapes rows would be created?
   - How many wines.color values would be set?
   - Show the top 10 affected wines per category for spot-checking

3. If the cascade preview looks wrong (e.g., wines under a Nebbiolo-only rule
   somehow matching Sangiovese, or a color cascade hitting rosé wines on a
   "red only" rule), STOP and investigate before committing.

## Cascade rules (after seeding)

Only run cascades that are DEFINITIONAL given the law:

- **Single-variety appellation (e.g., Barolo = 100% Nebbiolo by law):** set
  varietal_category_id on all wines with that appellation where currently
  NULL. Create wine_grapes row (grape_id=Nebbiolo, percentage=100,
  percentage_source pointing to appellation_rules row) where missing.
- **Regulated single-color appellation (e.g., Sauternes = white by law):**
  set wines.color where NULL.
- **DO NOT** cascade from blend-allowed appellations. "Bordeaux allows Cab
  Sauv, Merlot, Cab Franc, Petit Verdot, Malbec, Carmenère" is NOT
  definitional for any single wine — the wine could be any subset. Only
  write `appellation_grapes` rows, not `wine_grapes`.
- **DO NOT** overwrite existing non-null values. A wine that already has
  `varietal_category_id` set from round 7 (grape-name match) keeps its
  value. Cascades only fill NULLs.

## Commit cadence

Commit after each meaningful batch:
- After schema migration (if any)
- After the first 5 dry-run appellations
- Every ~20 seeded appellations
- After each cascade run
- Final wrap-up commit with CLAUDE.md updates

Don't batch the whole session into one commit.

## Stop conditions

- Stop and ask if a cascade would update more than 10K wines at once.
- Stop and ask if you find an existing value on `appellations` that conflicts
  with what the legal source says.
- Stop and ask if you can't find the legal source URL for a major appellation
  (Barolo, Chablis, Rioja, Champagne, etc.) — something is wrong with your
  search strategy.
- Hard stop if any fact cannot be traced to the approved-sources list.
- Hard stop if the final provenance query returns any row with NULL
  source_url or source_organization.

## Wrap up

Follow CLAUDE.md "wrap up" keyword: update CLAUDE.md round log with results,
append DECISIONS.md for judgment calls (e.g., conflicts between eAmbrosia
and a national source, ambiguous elevation ranges, a rule you had to split
into sub-categories for aging tiers, etc.), update SCHEMA.md if you added
provenance columns, commit at milestones, push.

In DECISIONS.md, record:
- What source organizations were used, with counts
- Any conflicts between sources and how you resolved them
- Any major appellations you couldn't find legal sources for (and why)
- The cascade impact summary (X wines got varietal_category, Y got color,
  Z wine_grapes rows created)

## Do NOT touch

- The two known canonical bugs (66 producer-as-appellation magnets;
  batch_matcher fuzzy collision) — out of scope for this session.
- Weather / `appellation_vintages` — Path B, separate session.
- AI enrichment / `wine_insights` — Path C, separate session.
- TTB barcode scan — running in a parallel session.
- Any row that conflicts with your legal source WITHOUT first stopping to
  flag the conflict. Existing data is assumed correct until proven
  otherwise by a legal citation.
