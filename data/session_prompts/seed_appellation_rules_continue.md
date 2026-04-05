# Session: Continue seeding appellation_rules from legal sources

Follow-up to `seed_appellation_rules.md`. 19 appellations already seeded in the
first session (2026-04-05) — see `docs/DECISIONS.md` "Path A session complete"
for full provenance audit.

Read CLAUDE.md first, then skim the 2026-04-05 Path A entry in DECISIONS.md
before starting. That entry documents the method, the cascade rules in practice,
and the gotchas.

## Where the previous session left off

**Seeded (19 appellations, 100% provenance, INAO × 18 + MAPA × 1):**
Bourgogne, Chablis, Champagne, Chassagne-Montrachet, Châteauneuf-du-Pape,
Gevrey-Chambertin, Gigondas, Meursault, Muscadet, Nuits-Saint-Georges, Pommard,
Pouilly-Fuissé, Pouilly-Fumé, Pouilly-sur-Loire, Puligny-Montrachet, Rioja,
Sancerre, Volnay, Vouvray.

**Cascade impact from first session:**
- +2,466 wines.color fills (9 single-color appellations)
- +3,610 wines.varietal_category_id fills (strict + effective single-variety)
- +2,869 new wine_grapes rows at percentage=100 (true 100% appellations only)

## Priorities for this session

### High-value targets still unseeded (by wine count)

**French AOCs — reliably fetchable from INAO extranet (the easy path):**
- Bordeaux generic (1,288 wines) — blend appellation, colors + allowed grapes list only
- Alsace / Vin d'Alsace (2,164 wines) — varietal-labeled whites, single-grape rules per label
- Beaune (1,270 wines) — Burgundy village, red/white principal+accessory
- Chambolle-Musigny (1,166 wines) — Burgundy village, red only (check)
- Bordeaux Supérieur, Médoc, Saint-Julien, Pauillac, Margaux, Graves, Pessac-Léognan,
  Saint-Émilion, Pomerol — Bordeaux left/right bank
- Bandol, Tavel, Côtes-du-Rhône, Côtes-du-Rhône Villages — Rhône
- Saumur, Saumur-Champigny, Chinon, Bourgueil, Anjou — Loire

**Italian DOCGs — HARD, need alternative fetch path:**
- Barolo (3,212 wines) — 100% Nebbiolo, huge cascade value
- Chianti Classico (1,290 wines) — min 80% Sangiovese
- Brunello di Montalcino (1,276 wines) — 100% Sangiovese
- Barbaresco (1,226 wines) — 100% Nebbiolo
- Langhe (1,940 wines) — broader Piemonte varietal rules
- Sicilia (1,101 wines) — multi-grape DOC
- Alto Adige (1,261 wines) — varietal-labeled
- Montepulciano/Vino Nobile

For Italian DOCGs, MASAF politicheagricole.it was ECONNREFUSED from the first
session — geoblocking suspected. Retry options:
- VPN to Italy/EU
- EUR-Lex direct CELEX by year (search for "Barolo" by specific OJ C-series
  publications of PDO modifications; first session's searches returned
  misleading results — one CELEX was actually for Nocciola del Piemonte)
- eAmbrosia `/details/EUGI.../sd/EN` endpoint (JS-rendered page didn't work
  from WebFetch in first session — may need browser automation)
- Consorzio websites are NOT on the approved source list per session prompt;
  only MASAF, eAmbrosia, or EUR-Lex Official Journal are acceptable for Italian
  PDO legal text.

**Spanish DO/DOCa — MAPA direct PDFs work (Rioja was seeded this way):**
- Ribera del Duero (1,647 wines)
- Priorat, Rías Baixas, Albariño, Rueda, Cava (note: Cava spans 7+ autonomous
  communities, on Spain catch-all)

**German QbA/Prädikat — eAmbrosia or LWBW:**
- Mosel (2,230 wines)
- Pfalz, Rheinhessen, Baden, Franken
- VDP classifications separately

**Portuguese DOC/IPR — IVV or eAmbrosia:**
- Porto / Port (955 wines) — fortified, multi-grape
- Douro
- Vinho Verde
- Dão, Bairrada, Alentejo

**USA AVAs — TTB 27 CFR Part 9 (geographic designations only, no varietal rules):**
- Napa Valley (9,731 wines)
- Paso Robles (4,581), Columbia Valley (4,495), Russian River Valley (3,973),
  Willamette Valley (3,778), etc.
- IMPORTANT: AVAs are geographic only. TTB does NOT mandate varieties or colors.
  Only the 85% grape source rule applies. Don't invent rules for US AVAs.
  Seeding these mostly just captures the boundary + 85% rule + any
  conjunctive labeling requirements.

### Target for this session

Aim for 20-30 new appellations. Focus on French AOCs first (reliable fetch path),
then take one targeted run at Italian DOCGs via EUR-Lex/VPN.

## Method (unchanged from first session)

1. Search for the legal source URL (INAO extranet, MAPA, eAmbrosia, EUR-Lex)
2. WebFetch the PDF — it gets saved to disk as binary content (see tool
   notification).
3. Extract text via `python -c "import pypdf; reader = pypdf.PdfReader(path); ..."`
   and write to `data/legal_sources/{name}_cdc.txt` (or `_disciplinare.txt`, or
   `_pliego.txt` depending on source organization).
4. grep the extract for Section III (Couleur / Colori / Color) and V
   (Encépagement / Vitigni / Variedades) to get colors and grape rules.
5. Also grep for alcohol minimum (title alcoométrique / grado alcoholico), yield
   (rendement / resa), aging (élevage / affinamento / envejecimiento).
6. Write UPSERT SQL with full provenance (source_url, source_organization,
   source_document_title, source_accessed_date, source_text_excerpt,
   last_verified_at).
7. Run cascade preview per batch. Execute cascades (color, varietal_category,
   wine_grapes) only where strictly definitional.
8. Commit every ~10 appellations.

## Cascade rules (unchanged, but see DECISIONS.md for practical nuances)

- **Color fills** only where the appellation is single-color regulated (e.g., Pommard = red only).
- **Varietal category fills** where grape dominance is ≥85% (Burgundy villages, Muscadet, Vouvray qualify — principal variety defines the category even when 5-15% accessory allowed).
- **wine_grapes at percentage=100** ONLY where the law says 100% strictly (Chablis, Sancerre by color, Pouilly-Fumé, Pouilly-Fuissé). Don't create percentage=100 rows for Muscadet/Vouvray/Burgundy villages — their 85-95% principal rule doesn't equal 100%.
- **Skip cascades entirely** for blend-allowed appellations (Bordeaux, Rioja, Champagne, CDP, Gigondas). Seed the rules for future reference but cascade nothing.
- **NULL fills only, no overwrites.** 850+ wines currently labeled with legally impossible colors have been logged to DECISIONS.md for a separate cleanup session.

## Legacy audit

There are 9,231 pre-existing `appellation_grapes` rows from a prior seed pass
that have NULL structured provenance (they have parenthetical "(INAO)" notes in
the `notes` column but no `source_url`, `source_organization`, etc.). When you
seed a new appellation that already has legacy rows, the UPSERT pattern
backfills provenance on the existing rows. So as you work through the priority
list, the legacy gap shrinks.

When the number of appellation_grapes rows with `source_organization IS NULL`
drops to zero, that's the signal to flip `source_url` + `source_organization`
to NOT NULL at the DB level.

## Known data quality issues (don't fix, just work around)

- `grapes` table has two MALVASIA rows (dupe) → skip Malvasía in Rioja-style
  white blend seeding until dedup.
- Turruntés (Rioja white) has synonyms mapping to both ALBILLO REAL and
  ALBILLO MAYOR → identity ambiguous, skip for now.
- Picardan (Châteauneuf-du-Pape) synonym maps to 4 different grapes → skip.
- `varietal_categories` has no Chasselas entry → Pouilly-sur-Loire's 6 wines
  can't get varietal_category. Add "Chasselas" category in a reference-data
  pass if ever prioritized.

## Provenance is still mandatory

Every row written in this session must carry `source_url`,
`source_organization`, `source_text_excerpt`. Run the provenance audit
query before wrap-up:

```sql
SELECT COUNT(*) FROM appellation_rules
WHERE source_url IS NULL OR source_organization IS NULL OR source_text_excerpt IS NULL;
```

Must return 0.

## Wrap up

Same as first session: update CLAUDE.md round log, append DECISIONS.md with
the session summary + any judgment calls, commit at milestones, push.
