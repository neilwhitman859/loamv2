# Identity Rules

Design spec for wine identity, display names, cuvée extraction, and data quality rules. Created Session 2 (2026-04-08). Every subsequent session follows this document.

---

## 1. Wine Identity Model

A wine is uniquely identified by the **identity tuple**:

```
(Producer, Cuvée, Primary Grape(s), Appellation, Color, Classification)
```

- **Producer** — required. Every wine has a producer.
- **Cuvée** — nullable. Many wines have no cuvée. `NULL` is correct when the wine's identity is fully described by the other components (e.g., Château Margaux is just producer + appellation).
- **Primary Grape(s)** — the grape composition. May be unknown (NULL grapes_confirmed).
- **Appellation** — nullable. Some wines have no appellation (e.g., "California" state-level). `appellation_confirmed = TRUE` with `appellation_id = NULL` means "confirmed no appellation."
- **Color** — red, white, rosé, orange. Required for identity_complete.
- **Classification** — Reserva, Premier Cru, Gran Selezione, Spätlese, etc. Stored via `wine_label_designations`. Part of identity: Muga Reserva ≠ Muga Gran Reserva.

Two wines are **the same wine** if and only if all six tuple components match. Vintages are NOT part of wine identity — 2015 and 2018 of the same wine share one `wines` row.

### Storage

| Field | Column | Table |
|-------|--------|-------|
| Producer | `producer_id` FK | `wines` |
| Cuvée | `name` | `wines` |
| Primary Grape(s) | grape links | `wine_grapes` |
| Appellation | `appellation_id` FK | `wines` |
| Color | `color` | `wines` |
| Classification | designation links | `wine_label_designations` |

`wines.name` = cuvée only. Never the full formatted name. Many wines correctly have `name = NULL`.

`wines.display_name` = computed by pipeline code, stored as regular TEXT. Rebuilt when identity components change.

---

## 2. Display Name Patterns by Country

Display names are the human-readable formatted name shown to users. They follow country-specific conventions that match how wine professionals and enthusiasts actually refer to wines.

### Assembly Rules

1. Display name is computed from: `producer_name`, `cuvee` (wines.name), `primary_grape_name`, `appellation_name`, `region_name`, `country_code`, `classification`, `wine_type`
2. Components in `[brackets]` are optional — included only when non-NULL
3. Comma separates the "what" (left) from the "where" (right)
4. Classification comes from `wine_label_designations` (the highest-ranking one if multiple)
5. If no appellation AND no region, use country name as the geographic component
6. `wine_type = 'sparkling'` or `'fortified'` may trigger special patterns

### 2.1 France

**Pattern:** `Producer, Appellation [Classification] [Cuvée]`

France is appellation-driven. The appellation IS the wine's identity. Grapes are implied by appellation rules and almost never appear in the display name.

| Subtype | Pattern | Example |
|---------|---------|---------|
| Standard AOC | `Producer, Appellation` | Château Margaux, Margaux |
| With classification | `Producer, Appellation Classification` | Domaine Leflaive, Puligny-Montrachet Premier Cru |
| With cuvée/vineyard | `Producer, Appellation Classification Cuvée` | Domaine Leflaive, Puligny-Montrachet Premier Cru Les Folatières |
| Burgundy Grand Cru | `Producer, Vineyard Grand Cru` | Domaine Romanée-Conti, Romanée-Conti Grand Cru |
| Champagne | `Producer [Cuvée], Champagne` | Krug Grande Cuvée, Champagne |
| Champagne vintage | `Producer [Cuvée], Champagne` | Dom Pérignon, Champagne |
| Alsace varietal | `Producer Grape, Alsace [Cuvée]` | Trimbach Riesling, Alsace Clos Sainte Hune |
| Alsace Grand Cru | `Producer Grape, Alsace Grand Cru Vineyard` | Zind-Humbrecht Riesling, Alsace Grand Cru Rangen |
| IGP/VDP | `Producer [Cuvée], Region` | Mas de Daumas Gassac Rouge, Pays d'Hérault |

**Edge cases:**
- Burgundy Grand Cru vineyards are appellations themselves (Romanée-Conti, Chambertin, Montrachet). The vineyard name IS the appellation.
- Champagne: grapes never shown. NV is default (don't show "NV"). Vintage Champagne shows year on the vintage, not in display name.
- Second wines: cuvée IS the identity (e.g., "Le Petit Mouton" is cuvée of Château Mouton Rothschild)
- Négociants: same display pattern. No special treatment.

**When grape appears:** Only for Alsace and some IGP/VDP wines. If `appellation.country_code = 'FR'` AND appellation is Alsace (or variant), include grape. Otherwise, omit grape from display name.

### 2.2 United States

**Pattern:** `Producer [Cuvée] [Grape], Appellation`

USA is varietal-driven. The grape name is central to identity.

| Subtype | Pattern | Example |
|---------|---------|---------|
| Standard AVA | `Producer Grape, AVA` | Caymus Cabernet Sauvignon, Napa Valley |
| With cuvée | `Producer Cuvée Grape, AVA` | Ridge Monte Bello Cabernet Sauvignon, Santa Cruz Mountains |
| Blend (no varietal) | `Producer [Cuvée], AVA` | Opus One, Napa Valley |
| State-level | `Producer [Cuvée] Grape, State` | Barefoot Cabernet Sauvignon, California |
| No geography | `Producer [Cuvée] Grape` | [rare — almost always at least state] |

**Edge cases:**
- Blends with no dominant grape: omit grape. Cuvée IS the identity (Opus One, The Prisoner, Insignia).
- Oregon Pinot: standard pattern. `Domaine Drouhin Pinot Noir, Dundee Hills`
- Washington: standard pattern. `Quilceda Creek Cabernet Sauvignon, Columbia Valley`
- Sparkling: `Producer [Cuvée], Region` — "Schramsberg Blanc de Blancs, North Coast". Include "Blanc de Blancs"/"Brut Rosé" as cuvée if present.
- Grocery/value: same pattern. `Josh Cellars Cabernet Sauvignon, California`

**When appellation vs region vs state:** Use the most specific geographic designation available. AVA > county > region > state.

### 2.3 Italy

**Pattern:** `Producer [Cuvée], Appellation [Classification]`

Italy uses appellation-centric naming like France, but cuvée names (vineyard selections, fantasy names) are more common.

| Subtype | Pattern | Example |
|---------|---------|---------|
| Standard DOCG/DOC | `Producer, Appellation` | Gaja, Barbaresco |
| With cuvée | `Producer Cuvée, Appellation` | Giacomo Conterno Monfortino, Barolo |
| With classification | `Producer [Cuvée], Appellation Classification` | Antinori, Chianti Classico Riserva |
| Super Tuscan IGT | `Producer Cuvée, Appellation` | Tenuta San Guido Sassicaia, Bolgheri |
| Barolo MGA | `Producer Cuvée, Barolo` | Bruno Giacosa Falletto, Barolo |

**Edge cases:**
- Super Tuscans: cuvée IS the wine (Sassicaia, Tignanello, Ornellaia, Masseto). The IGT appellation is secondary.
- Barolo/Barbaresco MGA vineyards: treated as cuvée, not appellation subdivision.
- DOCG vs DOC: not shown in display name (it's a quality level of the appellation, not a separate classification). Riserva/Superiore ARE shown.
- Prosecco: `Producer [Cuvée], Prosecco [di Valdobbiadene]`

**Grape in display name:** Almost never. Italian appellations define the grapes. Exception: some IGT wines label with grape.

### 2.4 Spain

**Pattern:** `Producer [Cuvée] [Classification], Appellation`

Spain's classification system (aging tiers) is central to identity.

| Subtype | Pattern | Example |
|---------|---------|---------|
| Standard DO/DOCa | `Producer Classification, Appellation` | Muga Reserva, Rioja |
| With cuvée | `Producer Cuvée Classification, Appellation` | López de Heredia Viña Tondonia Reserva, Rioja |
| No classification | `Producer [Cuvée], Appellation` | Álvaro Palacios L'Ermita, Priorat |
| Cava | `Producer [Cuvée], Cava` | Codorníu Anna de Codorníu, Cava |
| Sherry | `Producer [Cuvée] Style, Jerez` | Valdespino Inocente Fino, Jerez |

**Classification hierarchy:** Joven < Crianza < Reserva < Gran Reserva. Always include when present — they're different wines.

**Edge cases:**
- Priorat/modern producers often use fantasy cuvée names instead of classification
- Sherry style (Fino, Manzanilla, Amontillado, Oloroso, Palo Cortado, PX) IS the classification
- Vino de Pago: `Producer [Cuvée], Pago Name`

### 2.5 Germany

**Pattern:** `Producer [Vineyard] Grape [Prädikat], Region`

Germany's system is grape + vineyard + quality level. All three matter.

| Subtype | Pattern | Example |
|---------|---------|---------|
| Standard QbA | `Producer Grape, Region` | Dr. Loosen Riesling, Mosel |
| With Prädikat | `Producer Grape Prädikat, Region` | J.J. Prüm Riesling Spätlese, Mosel |
| With vineyard | `Producer Vineyard Grape Prädikat, Region` | J.J. Prüm Wehlener Sonnenuhr Riesling Spätlese, Mosel |
| VDP GG | `Producer Vineyard Grape GG, Region` | Dönnhoff Hermannshöhle Riesling GG, Nahe |
| Sekt | `Producer [Cuvée] Sekt, Region` | — |

**Prädikat hierarchy:** Kabinett < Spätlese < Auslese < Beerenauslese < Trockenbeerenauslese < Eiswein. Always include when present.

**Edge cases:**
- VDP classifications (Gutswein, Ortswein, Erste Lage, Grosse Lage/GG) are private trade classifications, not legal Prädikat. Include GG and Erste Lage if known.
- Trocken (dry): include when explicitly labeled. Marks a stylistic choice.
- Vineyard names: treat as cuvée. "Wehlener Sonnenuhr" is the vineyard, stored as cuvée.

### 2.6 Portugal

**Pattern:** `Producer [Cuvée] [Classification], Appellation`

Portugal has table wines and fortified wines with different patterns.

| Subtype | Pattern | Example |
|---------|---------|---------|
| Table DOC | `Producer [Cuvée], Appellation` | Niepoort Redoma, Douro |
| Port vintage | `Producer [Cuvée] Style, Porto` | Taylor's Vintage Port, Porto |
| Port aged | `Producer Age Style, Porto` | Graham's 20 Year Tawny Port, Porto |
| Port single quinta | `Producer Quinta Vintage Port, Porto` | Dow's Quinta do Bomfim Vintage Port, Porto |
| LBV Port | `Producer Late Bottled Vintage, Porto` | Warre's LBV, Porto |
| Madeira | `Producer [Grape] [Age], Madeira` | Blandy's Malmsey 10 Year, Madeira |
| Vinho Verde | `Producer [Cuvée], Vinho Verde` | Anselmo Mendes Muros Antigos, Vinho Verde |

**Edge cases:**
- Port style IS the classification: Vintage, LBV, Tawny, Colheita, Ruby, White, Rosé
- Madeira grape IS the classification: Sercial, Verdelho, Boal/Bual, Malmsey/Malvasia, Terrantez
- Same producer often makes both Douro table wine and Port — different wines
- Age statements (10/20/30/40 Year) are part of identity for Tawny Port

### 2.7 Australia

**Pattern:** `Producer [Cuvée] Grape, Region`

Varietal-driven like USA but with Australian regional identity.

| Subtype | Pattern | Example |
|---------|---------|---------|
| Standard | `Producer Grape, Region` | Torbreck RunRig Shiraz, Barossa Valley |
| With cuvée | `Producer Cuvée Grape, Region` | Penfolds Grange Shiraz, South Australia |
| Multi-region | `Producer [Cuvée] Grape, State` | Penfolds Bin 389 Cabernet Shiraz, South Australia |
| Blend | `Producer Cuvée, Region` | Henschke Hill of Grace Shiraz, Eden Valley |

**Edge cases:**
- "Shiraz" not "Syrah" — use Australian convention for Australian wines
- Multi-region blends: use the broadest common region (e.g., "South Australia" for Barossa+McLaren Vale)
- Penfolds Bin numbers are cuvées: Bin 389, Bin 407, Bin 28

### 2.8 New Zealand

**Pattern:** `Producer [Cuvée] Grape, Region`

Almost identical to Australian pattern. Strongly varietal-driven.

| Example |
|---------|
| Cloudy Bay Sauvignon Blanc, Marlborough |
| Felton Road Pinot Noir, Central Otago |
| Craggy Range Te Muna Road Vineyard Pinot Noir, Martinborough |

### 2.9 Argentina

**Pattern:** `Producer [Cuvée] Grape, Region`

Malbec-dominated. Tiered product lines are common.

| Example |
|---------|
| Catena Zapata Malbec, Mendoza |
| Catena Alta Malbec, Mendoza |
| Achaval-Ferrer Finca Altamira Malbec, Mendoza |

**Edge cases:**
- Tiered lines: "Catena" vs "Catena Alta" vs "Catena Zapata" — the line name is the cuvée
- Sub-regions of Mendoza (Luján de Cuyo, Uco Valley) are becoming important. Use most specific available.

### 2.10 Chile

**Pattern:** `Producer [Cuvée] Grape, Valley/DO`

Similar to Argentina. Tiered product lines.

| Example |
|---------|
| Concha y Toro Don Melchor Cabernet Sauvignon, Puente Alto |
| Montes Alpha Cabernet Sauvignon, Colchagua Valley |
| Viña Errázuriz Max Reserva Cabernet Sauvignon, Aconcagua Valley |

### 2.11 South Africa

**Pattern:** `Producer [Cuvée] Grape, Region`

| Example |
|---------|
| Kanonkop Pinotage, Stellenbosch |
| Boekenhoutskloof The Chocolate Block, Franschhoek |
| Mullineux Syrah, Swartland |

**Edge cases:**
- Cape blends (Pinotage-based): cuvée IS the identity when no dominant grape
- "Stellenbosch"/"Swartland"/"Franschhoek" are the key regions

### 2.12 Austria

**Pattern:** `Producer Grape [Classification], DAC/Region`

Grape-centric with local quality classifications.

| Example |
|---------|
| F.X. Pichler Riesling Smaragd, Wachau |
| Nikolaihof Grüner Veltliner Federspiel, Wachau |
| Hirsch Riesling, Kamptal |
| Kracher Beerenauslese Cuvée, Neusiedlersee |

**Edge cases:**
- Wachau has its own classification: Steinfeder < Federspiel < Smaragd. Always include.
- DAC system: use DAC name as appellation.

### 2.13 Greece

**Pattern:** `Producer [Cuvée] [Grape], Appellation`

Include grape when it's an indigenous variety unfamiliar to most consumers.

| Example |
|---------|
| Gaia Thalassitis Assyrtiko, Santorini |
| Domaine Sigalas Assyrtiko, Santorini |
| Alpha Estate Xinomavro, Amyndeon |

### 2.14 Fallback (all other countries)

**Pattern:** `Producer [Cuvée], Appellation [Grape]`

Used for: Hungary, Georgia, Slovenia, Romania, Croatia, Lebanon, Israel, Canada, England, China, Japan, Moldova, North Macedonia, Switzerland, and any country not listed above.

| Example |
|---------|
| Royal Tokaji Aszú 5 Puttonyos, Tokaj |
| Château Musar, Bekaa Valley |
| Mission Hill Pinot Noir, Okanagan Valley |
| Nyetimber Classic Cuvée, England |

### 2.15 Display Name Function Signature

```python
def build_display_name(
    producer_name: str,
    cuvee: str | None,           # wines.name
    primary_grape: str | None,    # name of dominant grape for display
    appellation_name: str | None,
    region_name: str | None,
    country_code: str,            # ISO 3166-1 alpha-2
    classification: str | None,   # highest-rank label designation
    wine_type: str | None,        # 'table', 'sparkling', 'fortified'
    vineyard_name: str | None,    # for German/Burgundy vineyard sites
) -> str:
    """
    Compute display name from identity components.
    
    Returns the formatted display name following country-specific patterns.
    The function is deterministic — same inputs always produce same output.
    Called at wine creation/update time, result stored in wines.display_name.
    """
```

**Implementation note:** `display_name` is a stored column (not a generated column) because it references data from multiple tables. Recomputed by pipeline when any identity component changes.

---

## 3. Cuvée Extraction Algorithm

The cuvée is whatever remains after stripping known identity components from a raw wine name. Many wines correctly have `cuvée = NULL`.

### Input

- `raw_name` — wine name from staging data (e.g., LWIN, TTB, importer)
- `producer_name` — already-matched producer
- `appellation_name` — already-matched appellation (may be NULL)
- `grape_names` — list of known grape names (from grape reference table)
- `classification_names` — list of classification keywords
- `country_code` — country of the wine

### Algorithm

```
1. NORMALIZE: lowercase, strip accents for matching (preserve original case for output)
2. STRIP PRODUCER: remove producer_name if it appears at the start of raw_name
3. STRIP APPELLATION: remove appellation_name (and common variants/aliases)
4. STRIP GRAPES: remove grape names from the curated grape list
5. STRIP CLASSIFICATIONS: remove classification keywords (see list below)
6. STRIP COLOR WORDS: remove Rouge, Blanc, Rosé, Rosato, Tinto, Blanco, Bianco, Rosso
7. STRIP VINTAGE: remove 4-digit year patterns
8. STRIP NOISE: remove "Estate", "Vineyards", "Winery", "Cellars", "Wine", "Wines"
9. CLEAN: trim whitespace, collapse multiple spaces, strip leading/trailing punctuation
10. RESULT: if empty string → NULL. Otherwise → cuvée.
```

### Classification Keywords (strip from name, store in wine_label_designations)

**French:** Premier Cru, 1er Cru, Grand Cru, Cru Bourgeois, Cru Classé, Supérieur, Villages

**Italian:** Riserva, Gran Selezione, Superiore, Classico, Passito, Recioto, Ripasso, Sforzato/Sfursat

**Spanish:** Joven, Roble, Crianza, Reserva, Gran Reserva, Vendimia Seleccionada

**German Prädikat:** Kabinett, Spätlese, Auslese, Beerenauslese, Trockenbeerenauslese, Eiswein, Trocken, Halbtrocken, Feinherb

**German VDP:** GG, Grosses Gewächs, Erste Lage, Gutswein, Ortswein

**Austrian:** Smaragd, Federspiel, Steinfeder

**Portuguese:** Reserva, Garrafeira, Grande Reserva, Colheita, Late Bottled Vintage, LBV, Vintage, Tawny, Ruby

**General:** Reserve, Old Vine(s), Vieilles Vignes, Barrel Select, Single Vineyard, Limited Edition, Cuvée Prestige, Brut, Extra Brut, Brut Nature, Extra Dry, Sec, Demi-Sec, Doux, Blanc de Blancs, Blanc de Noirs

### Edge Cases

**Wine name = producer name (brand wines):**
- Opus One, Screaming Eagle, Masseto, Dominus → cuvée = NULL
- Detection: after stripping all components, if remaining text matches producer name (fuzzy), set NULL

**Wine name = appellation:**
- "Puligny-Montrachet" by Domaine Leflaive → cuvée = NULL
- "Barolo" by Gaja → cuvée = NULL
- Detection: after stripping producer, if remaining text matches an appellation name, cuvée = NULL

**Wine name = grape:**
- "Cabernet Sauvignon" by Barefoot → cuvée = NULL
- Detection: after stripping all components, nothing remains

**Vineyard names as cuvée:**
- "Les Folatières" (Burgundy climat) → cuvée = "Les Folatières"
- "Wehlener Sonnenuhr" (Mosel vineyard) → cuvée = "Wehlener Sonnenuhr"
- These are meaningful cuvées — the vineyard selection IS the differentiation

**Second wines / sub-brands:**
- "Le Petit Mouton" (Mouton Rothschild second wine) → cuvée = "Le Petit Mouton"
- "Les Forts de Latour" → cuvée = "Les Forts de Latour"
- These are distinct wines with their own identity

**Tiered product lines:**
- "Catena Alta" (vs base "Catena") → cuvée = "Alta"? No — "Catena Alta" is effectively a sub-brand. Treat as cuvée = "Alta" only if producer is "Catena Zapata". Be careful with producer vs cuvée boundaries.
- Decision: when the line name is well-known, treat it as cuvée. When ambiguous, keep in name.

**TTB concatenated names:**
- TTB brand_name + fanciful_name may produce: "RIDGE VINEYARDS MONTE BELLO CABERNET SAUVIGNON SANTA CRUZ MOUNTAINS"
- This is the hardest case. Apply the full stripping algorithm. After removing "RIDGE VINEYARDS" (producer), "CABERNET SAUVIGNON" (grape), "SANTA CRUZ MOUNTAINS" (appellation), we get "MONTE BELLO" → cuvée.

### Cuvée Validation

After extraction, sanity-check:
- Cuvée should not be a country name
- Cuvée should not be a region name (unless it's genuinely part of the wine name)
- Cuvée should not be a standalone grape name
- Cuvée length > 50 chars is suspicious — probably incomplete stripping
- Cuvée that IS the producer name → set NULL

---

## 4. Confirmed Boolean Patterns

### `color_confirmed`

Set `TRUE` when color is established by a reliable source:

| Source | Confirmed? | Example |
|--------|-----------|---------|
| Appellation rule (single-color only) | YES | Chablis = white only → TRUE |
| Label regulation (varietal implies color) | YES | "Cabernet Sauvignon" → red → TRUE |
| Staging data explicit color field | YES | source_lwin.colour = 'R' → TRUE |
| TTB class_type_desc | YES | "RED WINE" → TRUE |
| Appellation rule (multi-color) | NO | Rioja allows red/white/rosé → leave FALSE |
| AI classification | NO | Haiku says "red" → log to ai_suggestions, don't confirm |

### `grapes_confirmed`

Set `TRUE` when the complete grape composition is known:

| Source | Confirmed? | Example |
|--------|-----------|---------|
| Single-variety appellation rule (100%) | YES | Chablis = 100% Chardonnay → TRUE |
| Label regulation + full blend from staging | YES | "Cabernet Sauvignon" (≥75%) + blend partners from importer → TRUE |
| Label regulation alone (primary only) | NO | "Cabernet Sauvignon" = ≥75%, but remaining 25% unknown → FALSE |
| Staging source with complete blend | YES | Empson lists "Sangiovese 80%, Canaiolo 15%, Colorino 5%" → TRUE |
| AI suggestion | NO | Log to ai_suggestions |

**Note:** `blend_complete` is a stronger signal. `blend_complete = TRUE` means all grapes with percentages summing to ~100%. `grapes_confirmed = TRUE` means we're confident we have the right grapes even if not all percentages are known.

### `appellation_confirmed`

Set `TRUE` when the appellation assignment is reliable:

| State | Meaning |
|-------|---------|
| `appellation_id = UUID, appellation_confirmed = TRUE` | Wine belongs to this appellation, confirmed |
| `appellation_id = UUID, appellation_confirmed = FALSE` | Appellation assigned but not yet verified |
| `appellation_id = NULL, appellation_confirmed = TRUE` | Confirmed: this wine has no appellation |
| `appellation_id = NULL, appellation_confirmed = FALSE` | Unknown — haven't determined appellation yet |

Sources that confirm appellation:
- TTB wine_appellation field (regulatory filing)
- LWIN explicit appellation
- Importer catalog appellation
- Appellation name in wine name + country match

### `identity_complete`

Computed boolean: `TRUE` when producer + color + grapes + (appellation OR region) + country are all confirmed.

```sql
identity_complete = (
    producer_id IS NOT NULL
    AND color IS NOT NULL AND color_confirmed = TRUE
    AND EXISTS (SELECT 1 FROM wine_grapes WHERE wine_id = wines.id) AND grapes_confirmed = TRUE
    AND (appellation_id IS NOT NULL OR region_id IS NOT NULL)
    AND country_id IS NOT NULL
)
```

Recalculated by `grade_calculator.py` after each batch.

---

## 5. Staging → Canonical Matching Spec

### Overview

The pipeline works producer-first: validate a producer, then find all their wines across staging tables, then deduplicate and promote.

### Step 1: Producer Matching

For each target producer:

1. **Search staging tables** by producer name (ILIKE with normalization)
   - `source_lwin.producer` — best structured data
   - `source_ttb_colas.brand_name` — US regulatory filings
   - `source_ttb_colas.applicant_name` — sometimes the producer
   - Importer catalogs: `source_skurnik.producer`, `source_empson.producer`, etc.
   - Retailer catalogs: `source_specs.brand`, `source_flatiron.producer`, etc.
   - Competition data: `source_texsom.producer`, `source_berliner.producer`

2. **Normalize for matching:** Strip "Château", "Domaine", "Bodegas", "Weingut", "Tenuta", "Casa", "Cantina" prefixes. Strip "Winery", "Vineyards", "Estate", "Cellars" suffixes. Lowercase, strip accents, collapse spaces.

3. **Cross-reference:** If the same producer appears in 2+ staging sources, confidence is high. Log to `data_provenance`.

4. **Create canonical producer** with `country_id`, `region_id` from best available source.

### Step 2: Wine Discovery

For each validated producer, pull ALL staging records:

```sql
-- Pseudo-query: find all wines by this producer across sources
SELECT 'lwin' as source, id, wine_name, ...
FROM source_lwin WHERE producer ILIKE '%{name}%'
UNION ALL
SELECT 'ttb' as source, id, fanciful_name, ...
FROM source_ttb_colas WHERE brand_name ILIKE '%{name}%'
UNION ALL
-- ... all other staging tables
```

### Step 3: Wine Clustering

Group staging records that represent the same wine:

1. **Extract identity components** from each staging record (cuvée, grape, appellation, color)
2. **Normalize** all components for comparison
3. **Cluster** by identity tuple similarity
4. **Within each cluster:** pick the "best" record as the canonical source (prefer LWIN > importer > TTB > retailer for data quality)

### Step 4: Canonical Wine Creation

For each unique wine cluster:

1. Create `wines` row with: `producer_id`, `name` (cuvée), `appellation_id`, `color`, `country_id`, `region_id`, `wine_type`
2. Compute `display_name` using country rules
3. Link staging records back: set `canonical_wine_id` on all matched staging rows
4. Log all field sources to `data_provenance`

### Step 5: Depth Promotion

After wines exist, promote depth data from staging:
- Vintages (year, ABV, label image)
- Grapes (from staging or label regulation cascade)
- Prices, scores
- UPC/COLA/LWIN external IDs

### Matching Thresholds

| Match Type | Method | Threshold |
|-----------|--------|-----------|
| Producer name | Normalized exact + trigram | ≥ 0.7 similarity |
| Wine cuvée | Normalized exact after component stripping | exact or NULL=NULL |
| Appellation | FK resolution against `appellations` table | exact match required |
| Grape | FK resolution against `grapes` + `grape_synonyms` | exact match required |

**When in doubt:** Don't match. Create a new canonical record. Duplicates are worse than gaps.

---

## 6. Label Regulation Rule

### 27 CFR 4.23 — US Varietal Labeling

If a US wine label names a grape variety, that grape must constitute **≥75%** of the wine (by volume). Oregon wines: **≥90%** for most varieties (≥75% for Cabernet Sauvignon exempt by state law).

**Application:** When a wine's name contains a recognized grape variety name, we can source-confirm that grape at ≥75% (or ≥90% for Oregon).

```
source_type = 'label_regulation'
confidence = 1.00
```

This is not inference — it's legal fact. The label was approved by TTB.

### EU Regulation (EC) No 607/2009 — Single Varietal Labeling

If an EU wine label names a single grape variety, that grape must constitute **≥85%** of the wine.

**Application:** Same as US rule but at 85% threshold for EU-origin wines.

### Implementation

```python
def grape_from_label_regulation(wine_name: str, country_code: str) -> tuple[str, float] | None:
    """
    If wine name contains a grape varietal, return (grape_name, min_percentage).
    
    Returns None if no varietal found in name.
    Uses 75% for US (90% for Oregon), 85% for EU countries.
    """
```

### What this does NOT cover

- Blend composition beyond the named varietal (the other 25%/15% is unknown)
- Wines without a varietal in the name
- Multi-varietal names ("Cabernet Sauvignon - Merlot") — treated as two grapes, each at label-regulation minimum? No — multi-varietal labels don't have individual percentage guarantees. Skip these.

---

## 7. Junk Producer Criteria

Explicit rules for filtering out garbage producer names. Applied before any canonical insertion.

### Reject if ANY of these match:

```python
JUNK_PRODUCER_RULES = [
    # Too short
    lambda name: len(name.strip()) < 3,
    
    # Bare numbers
    lambda name: name.strip().isdigit(),
    
    # Corporate suffixes as entire name
    lambda name: name.strip().upper() in {
        'LLC', 'INC', 'CORP', 'LTD', 'PTY', 'GMBH', 'SA', 'SL', 'SRL',
        'SPA', 'AG', 'NV', 'BV', 'CO', 'COMPANY',
    },
    
    # Placeholder names
    lambda name: name.strip().upper() in {
        'N/A', 'NA', 'UNKNOWN', 'VARIOUS', 'PRIVATE LABEL', 'NONE',
        'TBD', 'TEST', 'SAMPLE', 'OTHER', 'MISC', 'GENERIC',
    },
    
    # All-caps no vowels (likely abbreviations/codes)
    lambda name: (name == name.upper() 
                  and len(name) > 2 
                  and not any(c in name.upper() for c in 'AEIOU')),
    
    # Known TTB junk patterns
    lambda name: any(p in name.upper() for p in [
        'DBA ', 'DOING BUSINESS AS', 'FORMERLY KNOWN AS',
        'AKA ', 'BRAND REGISTRATION',
    ]),
    
    # Suspiciously long (>80 chars — likely concatenated address or description)
    lambda name: len(name.strip()) > 80,
]
```

### Dedup Rules (same producer, different strings)

Two producer names are duplicates if they match after:
1. Stripping prefixes: Château, Domaine, Bodegas, Weingut, Tenuta, Casa, Cantina, Azienda Agricola, Caves, Adega, Kellerei
2. Stripping suffixes: Winery, Vineyards, Estate, Cellars, Wines, Wine Company, Wine Co, & Sons, & Fils, et Fils, e Figli
3. Normalizing: lowercase, strip accents, collapse whitespace, strip punctuation
4. After all normalization, exact string match

**Important:** This is a detection rule, not an automatic merge. Detected duplicates go to human review before merging.

---

## 8. Batch 0 Producer Roster (verified in staging)

All 48 unique producers verified in staging data. TTB covers all 48, LWIN covers 45.

| # | Category | Producer | Primary Staging Sources |
|---|----------|----------|----------------------|
| 1 | Bordeaux | Château Margaux | LWIN (28), TTB (2,598), Spec's, PRO |
| 2 | Bordeaux | Château Lafite Rothschild | LWIN (28), TTB (2,109), TEXSOM |
| 3 | Bordeaux | Château Mouton Rothschild | LWIN (12), TTB (2,952) |
| 4 | Bordeaux | Château Haut-Brion | LWIN (30), TTB (4,419), Flatiron |
| 5 | Bordeaux | Château Latour | LWIN (282*), TTB (1,976), Spec's |
| 6 | Napa | Ridge Vineyards | LWIN (928*), TTB (25,814*), TEXSOM (603), Berliner |
| 7 | Napa | Caymus Vineyards | LWIN (13), TTB (207), TEXSOM |
| 8 | Napa | Stag's Leap Wine Cellars | LWIN (35), TTB (933), TEXSOM |
| 9 | Napa | Opus One | LWIN (4), TTB (66), Spec's |
| 10 | Napa | Silver Oak Cellars | LWIN (6), TTB (136), TEXSOM |
| 11 | Italian | Giacomo Conterno | LWIN (26), TTB (1,834*), Flatiron |
| 12 | Italian | Marchesi Antinori | LWIN (121), TTB (906), TEXSOM, Berliner |
| 13 | Italian | Gaja | LWIN (52), TTB (1,496), Spec's |
| 14 | Italian | Masseto | LWIN (4), TTB (126), Spec's |
| 15 | Italian | Tenuta San Guido | LWIN (4), TTB (278), TEXSOM |
| 16 | Spanish | López de Heredia | LWIN (11), TTB (412), Spec's |
| 17 | Spanish | La Rioja Alta | LWIN (31), TTB (527), Skurnik (12) |
| 18 | Spanish | CVNE | LWIN (53), TTB (354), Skurnik (12) |
| 19 | Spanish | Bodegas Muga | LWIN (26), TTB (860), Berliner, TEXSOM |
| 20 | Spanish | Marqués de Riscal | LWIN (28), TTB (205), Berliner, TEXSOM |
| 21 | German | J.J. Prüm | LWIN (230), TTB (2,332*), Flatiron |
| 22 | German | Dr. Loosen | LWIN (151), TTB (631), Berliner (37), TEXSOM (42) |
| 23 | German | Dönnhoff | LWIN (157), TTB (596), Skurnik (80) |
| 24 | German | Egon Müller | LWIN (146), TTB (276), Flatiron |
| 25 | German | Fritz Haag | LWIN (90), TTB (233), TEXSOM (14) |
| 26 | Australian | Penfolds | LWIN (127), TTB (1,192), TEXSOM (53), BC Liquor |
| 27 | Australian | Henschke | LWIN (38), TTB (369), TEXSOM, Winebow |
| 28 | Australian | Torbreck | LWIN (28), TTB (310), TEXSOM, Spec's |
| 29 | Australian | Tyrrell's | LWIN (77), TTB (591), TEXSOM |
| 30 | Australian | Yalumba | LWIN (75), TTB (762), TEXSOM (50), Winebow |
| 31 | Grocery | Barefoot | LWIN (13), TTB (1,043), TEXSOM (353), Enofile (672) |
| 32 | Grocery | Josh Cellars | TTB (252), TEXSOM (76), Spec's, Kansas, WV |
| 33 | Grocery | Meiomi | LWIN (4), TTB (131), TEXSOM, Spec's |
| 34 | Grocery | 19 Crimes | LWIN (12), TTB (112), Spec's, Kansas |
| 35 | Grocery | Yellow Tail | LWIN (10), TTB (344), TEXSOM (65), Kansas (72) |
| 36 | Négociant | Louis Jadot | LWIN (372), TTB (2,902), TEXSOM, Systembolaget |
| 37 | Négociant | Louis Latour | LWIN (189), TTB (9,184*), Spec's, BC Liquor |
| 38 | Négociant | Joseph Drouhin | LWIN (192), TTB (2,877), Flatiron |
| 39 | Négociant | Bouchard Père et Fils | LWIN (297), TTB (3,934), Systembolaget |
| 40 | Négociant | Maison Champy | LWIN (105), TTB (2,079), Spec's |
| 41 | Single-wine | Dominus Estate | LWIN (5), TTB (66), Systembolaget |
| 42 | Single-wine | Screaming Eagle | LWIN (8), TTB (38), Flatiron, TABC |
| 43 | Single-wine | Harlan Estate | LWIN (9), TTB (47), Spec's, TABC |
| 44 | Single-wine | Scarecrow | LWIN (6), TTB (15), TABC |
| 45 | Portfolio | Treasury Wine Estates | TTB (55), Berliner (52), WV |
| 46 | Portfolio | Constellation Brands | TTB (372), Berliner (18), WV |
| 47 | Portfolio | E&J Gallo | LWIN (36), TTB (3,332), TEXSOM (142), Berliner (80) |
| 48 | Portfolio | Kendall-Jackson | LWIN (1), TTB (2,716), TEXSOM (238) |
| 49 | Portfolio | Duckhorn Vineyards | LWIN (87), TTB (1,024), TEXSOM, Spec's |

*Counts marked with \* are inflated by substring matching — precise matching needed at promotion time.

**Note on portfolio companies:** Treasury Wine Estates, Constellation Brands, and E&J Gallo are parent companies. Their wines exist in staging under subsidiary brand names (Penfolds, Robert Mondavi, Barefoot). At promotion time, decide whether to create the parent company as a producer or only use the subsidiary brands. Recommendation: **use subsidiary brands as producers** (that's what appears on the label), link to parent via `parent_producer_id` if desired.

---

## 9. LWIN License Status

**License: CC BY 4.0 (Creative Commons Attribution 4.0 International)**

- Commercial use: permitted
- Derivatives: permitted
- Redistribution: permitted
- Only obligation: attribution credit

**Action:** Add attribution to frontend footer or about page: "Wine identification powered in part by LWIN, provided by Liv-ex under CC BY 4.0."

No fallback plan needed. LWIN is clear for use as backbone identifier.

---

## 10. Data Provenance Logging

Every field written to canonical tables gets a `data_provenance` entry.

### Required Fields

```
entity_type: 'wine' | 'producer' | 'wine_vintage' | 'wine_grapes' | ...
entity_id: UUID of the canonical record
field_name: column name (e.g., 'name', 'color', 'appellation_id')
field_value: the value written (as text)
source_type: 'lwin' | 'ttb_cola' | 'cascade' | 'label_regulation' | 'staging_*' | 'manual'
source_id: staging row ID or other reference
session_id: which session wrote this
```

### Source Types

| source_type | Meaning | Confirms? |
|-------------|---------|-----------|
| `lwin` | LWIN trade database | YES |
| `ttb_cola` | TTB COLA registry | YES |
| `staging_skurnik` | Skurnik importer catalog | YES |
| `staging_empson` | Empson importer catalog | YES |
| `staging_*` | Any staging table | YES |
| `cascade` | Derived from appellation rules | YES |
| `label_regulation` | 27 CFR 4.23 / EU 607/2009 | YES |
| `wikidata` | Wikidata CC0 | YES |
| `manual` | Human/session entry | YES |
| `ai_suggestion` | AI classified (logged only) | NO — goes to ai_suggestions table |

---

## 11. Producer Identity Rules

Added Sprint 6 B6.3 (2026-04-16) for producer dedup. Section is embedded verbatim in every L1/L2/L3 LLM prompt during Sprint 6. Use this section as the single source of truth when answering "are these two producers the same?"

### 11.0 Core principle: brand-on-label

**A producer is the brand printed on the label.** Not the corporate parent, not the négociant's client, not the importer, not the retailer. If two wines from different `producers` rows have the same brand identity on the label, they are the same producer and should be MERGEd. If they have distinct brand identities — even under shared ownership — they are distinct producers linked by `parent_producer_id`.

This rule resolves most edge cases directly. The remaining complexity is in recognizing when two names describe the same brand identity.

### 11.1 MERGE — same brand identity on label

Two producer rows MERGE if **a shopper holding both bottles would see the same brand name**, modulo the differences below. Survivor rule: the surviving row's `name` should match the form that appears on most labels today (see §11.6).

**MERGE signals (any one is usually sufficient):**

| Signal | Example |
|---|---|
| Same normalized name, same country | "Ridge" + "Ridge Vineyards" (US) |
| Shared `external_ids.external_id` (LWIN_7, website host) | Both rows map to LWIN 1023456 or to `ridgewine.com` |
| Shared TTB `permit_no` (US) | Two rows both show permit `BW-CA-1234` |
| ≥30% shared named wines in catalog | Both "Louis Latour" rows carry "Corton-Charlemagne", "Corton Grancey", etc. |
| Accent / diacritic variant of the same name | "Lopez de Heredia" ⇔ "López de Heredia" |
| Spelling / transliteration variant | "Trimbach" ⇔ "F.E. Trimbach" ⇔ "Maison Trimbach" |
| Abbreviation vs. full form | "DRC" ⇔ "Domaine de la Romanée-Conti" |
| Country-language translation of the same producer | "Tenuta San Guido" keeps that form; don't translate to "San Guido Estate" |
| Rename (same continuous entity, different legal name) | "Opus One" remains Opus One even through ownership changes |

**When the pair is obvious MERGE, verdict = MERGE, confidence ≥ 0.85.**

### 11.2 PARENT-CHILD — distinct brands with ownership

Two producer rows are PARENT-CHILD when **they print distinct brand identities on their respective bottles but one owns / controls / is legally part of the other**. Represented via `producers.parent_producer_id` on the child row (child points to parent). No row-merging occurs.

**PARENT-CHILD signals:**

| Signal | Example |
|---|---|
| Portfolio / holding company owns a distinct brand | "E. & J. Gallo Winery" (parent) ← "Louis M. Martini" (child) |
| Estate has a second label at a separate price point | "Château Margaux" (parent) ← "Pavillon Rouge" if Pavillon Rouge is treated as its own producer rather than as a wine of Margaux (see §11.4.e for the usual case) |
| Négociant owns an estate that bottles under its own name | "Maison Joseph Drouhin" (parent) ← "Domaine Drouhin-Laroze" if the estate has a distinct identity on its labels |
| Regional winery is one of many under a corporate umbrella | "Constellation Brands" (parent) ← "Robert Mondavi Winery" (child) ← "Woodbridge by Robert Mondavi" (grandchild); **see §11.4.g — in practice we do NOT import corporate holdcos like Constellation as producers.** |
| Joint venture | "Opus One" is its own producer even though historically 50/50 Mondavi/Rothschild |

**Do NOT create PARENT-CHILD for:**
- Same brand, same label identity under a group (that's a MERGE, not parent-child)
- The relationship between a winery and its *wines* (wines go in the `wines` table, not producers)
- Distributor → producer (importers and retailers are never "parents")

**When the pair is obvious PARENT-CHILD, verdict = PARENT_CHILD, confidence ≥ 0.85. Record which row is the parent in `signals.parent_producer_id`.**

### 11.3 SKIP — unrelated producers

SKIP when the pair represents two distinct producers with no meaningful relationship. These are the expected majority verdicts on a blocking candidate list (trigram neighbors will pull in many same-country same-starting-letter producers that aren't actually the same).

**SKIP signals:**

| Signal | Example |
|---|---|
| Different brand identity, no ownership relation | "Dominus Estate" vs. "Dominio de Pingus" (one is US Napa, one is Spain Ribera del Duero; orthographically close, identity different) |
| Same commune, different estate | Two different Gevrey-Chambertin producers named "Domaine X" and "Domaine Y" |
| Trigram neighbors by coincidence | "Bodega La Rioja Alta" vs. "Bodegas Rioja Santiago" |
| Different classification but no shared ownership | Separate Barolo producers both in Monforte d'Alba |

**When the pair is obvious SKIP, verdict = SKIP, confidence ≥ 0.85.**

### 11.4 Edge cases

#### 11.4.a — Renames (legal name changes, ownership changes, rebrands)

If the label identity is **continuous** across the rename — same people, same estate, same wines, brand carried forward — treat as MERGE. Record the prior name in `producer_aliases` with `alias_type='historical_name'`. Example: "Mayacamas Vineyards" renamed ownership but kept the brand; MERGE the old and new rows.

If the rebrand creates a **new brand identity** (e.g., the estate is sold, the old brand is discontinued, and the new owner launches a new label from the same land), treat as SKIP and record the link in `producer_aliases` with `alias_type='succeeded_by'` on the predecessor.

Rule of thumb: **does a bottle from 10 years ago and a bottle from last vintage display the same brand name on the front label?** If yes → MERGE. If no → SKIP.

#### 11.4.b — Dissolved and reopened

A producer that went dormant and was later revived by the original family (or with continuous brand intent) is **one continuous producer**, even if there's a gap in `wine_vintages`. MERGE. Example: post-prohibition revivals in California, post-phylloxera revivals in Europe.

#### 11.4.c — Private labels (Charles Shaw, Kirkland Signature, Cameron Hughes)

Treat as **a producer row with the private-label brand as the name**, not as the actual bottling winery. For each vintage, capture the actual vintner in `wines.metadata.actual_vintner`. Reason: the consumer's bottle says "Charles Shaw" on the label, so the producer identity is Charles Shaw. The actual vintner may rotate year-to-year.

Do NOT MERGE a private label with its bottling winery (e.g., do not MERGE "Charles Shaw" with "Bronco Wine Company"). They are separate producer rows. If there is a known parent-child relationship (retailer owns the label), record as PARENT-CHILD.

#### 11.4.d — Retailers (Trader Joe's, Costco, Total Wine)

**Never a producer.** Retailers carry private-label products, but the producer is the brand on the label (Kirkland Signature, Charles Shaw, Winking Owl), not the retailer. If a staging import created a retailer-as-producer row, soft-delete it and re-link the affected wines to the correct private-label producer.

#### 11.4.e — Second wines (Les Forts de Latour, Pavillon Rouge du Château Margaux, Carruades de Lafite)

**Second wines are wines, not producers.** They are additional wines made by the parent producer from lesser grapes / younger vines. Model them as `wines` rows under the main producer (Château Latour → "Les Forts de Latour" wine). Do NOT create "Les Forts de Latour" as its own `producers` row.

If a staging import created one as a producer, SKIP the candidate MERGE with the main producer and flag the row for soft-delete / re-link downstream.

#### 11.4.f — Négociant + estate (same entity bottles both)

When one legal entity bottles both a négociant line and an estate line under **distinct brand identities** on the label, treat as PARENT-CHILD. Example: "Louis Jadot" négociant releases + "Domaine Gagey" (estate line owned by Jadot) = PARENT-CHILD.

When the négociant line and the estate line share a single brand identity on the label (e.g., "Maison Louis Latour" for both), treat as MERGE.

#### 11.4.g — Corporate holdcos (Constellation, Treasury Wine Estates, LVMH, Pernod Ricard)

**Default: do NOT import corporate holdcos as producers.** The label brand (Penfolds, Robert Mondavi, Barefoot, Krug, Dom Pérignon) is the producer. If a staging import created a pure-holdco-as-producer row (e.g., the B6.2 LWIN import created "LVMH" and "Sotheby's" as producers), soft-delete it in dedup review — flag with verdict=SKIP, flag_reason='corporate_holdco_soft_delete'.

**Carve-out: keep the holdco row when the holdco name ALSO appears on consumer-facing labels as a producer brand in its own right.** Some corporate entities have a dual identity — an umbrella holdco AND a physical winery that bottles under their own name. Examples:

- "E. & J. Gallo Winery" — corporate holdco owning dozens of brands (Barefoot, Apothic, André, Carlo Rossi) AND a Modesto BW facility historically bottling wines under the "Gallo" / "E. & J. Gallo Winery" label. Keep the producer row for the label brand; the umbrella relationship to the subsidiary brands can be recorded via `parent_producer_id` (PARENT-CHILD) rather than a MERGE into the holdco.
- "Antinori" — holdco AND estate label ("Marchesi Antinori" on Solaia, Tignanello). Keep.
- "Frescobaldi" — holdco (Marchesi de' Frescobaldi) AND wines labeled directly "Frescobaldi." Keep.

Rule of thumb: if a bottle walking into a consumer's hand could plausibly show the holdco name as the primary brand on the front label, it's a producer row (even if it's also a holdco). If the holdco only appears in fine-print ownership disclosures or B2B contexts, soft-delete.

When in doubt, search the name on Wine-Searcher or the official producer website — if they market wines under that name, keep.

#### 11.4.h — Accent / diacritic variants

Same producer, different rendering. MERGE. The survivor name should match the **form that appears on most labels today** (see §11.6).

- "Lopez de Heredia" ⇔ "López de Heredia" → survivor "López de Heredia" (official label)
- "Chateau Margaux" ⇔ "Château Margaux" → survivor "Château Margaux"
- "Ramon Bilbao" ⇔ "Ramón Bilbao" → survivor "Ramón Bilbao"

Preserve the stripped-accent form in `producer_aliases` so that search still matches user typing without accents.

#### 11.4.i — Importer prefixes ("a Becky Wasserman Selection", "Skurnik Wines Imports")

Staging imports sometimes prepend an importer selection prefix or suffix to the producer name. When matching, **strip importer prefixes/suffixes** before comparing. Example: `"a Becky Wasserman Selection: Domaine Leflaive"` → compare as `"Domaine Leflaive"`. The producer is the brand on the label, not the importer that selected it.

Record the importer relationship in `producer_importers`, not in the producer name.

#### 11.4.j — US producers sharing a TTB `permit_no` (permittee basic permit)

The TTB `permit_no` (column `permit_no` on `source_ttb_colas`; also seen as `permit_number` in newer rows) is **federally unique per licensed entity**. Two US producer rows with the same `permit_no` are almost always the same brand. Treat shared `permit_no` as a **very strong MERGE signal** (confidence ≥ 0.90).

Edge case: some TTB-licensed entities hold multiple brand names. If the two producer rows share a `permit_no` but have clearly distinct brand identities on their respective labels (e.g., a custom-crush facility that licenses two different brands), it may be PARENT-CHILD rather than MERGE. Use the wine catalog overlap to disambiguate: if the same wines appear under both rows, MERGE; if the wine catalogs are disjoint, PARENT-CHILD with the custom-crush facility as parent.

#### 11.4.k — Commune overlap / first-word collision

Many producers share the same first word with unrelated producers (e.g., "Château Montrose" vs. "Château Montelena"; "Domaine Leflaive" vs. "Domaine Leroy"). These are NOT MERGE candidates — the differentiating tail tokens encode the actual identity. Treat as SKIP unless a stronger signal (TTB permit, LWIN, website, shared wine list) overrides.

#### 11.4.l — Joint ventures and brand partnerships

Treat the JV brand as its own producer row. Example: "Opus One" is its own producer even though it is historically 50/50 Mondavi / Rothschild. If one of the parent brands also appears separately in the database, record PARENT-CHILD as appropriate, but do NOT MERGE the JV into either parent.

### 11.5 UNCERTAIN / FLAGGED verdicts

A method (L1, L2, L3) should return verdict=UNCERTAIN when the signals are genuinely mixed:
- Names suggest MERGE but the wine catalogs don't overlap
- TTB signal points MERGE but the brand names look different
- One side has thin data (no wines, no TTB, no LWIN) so there's nothing to verify against

UNCERTAIN pairs propagate up the ladder — L1 UNCERTAIN → L2; L2 UNCERTAIN or MERGE → L3 with web grounding.

At review time (B6.5), the human reviewer may set `verdict='FLAGGED'` with `flag_reason` text to mean "don't resolve this now — comes back in a later sprint, but don't let it auto-apply." Pattern-level observations go to `data/sprints/dedup/open_questions.md` (upgrade 4 from B6.1).

### 11.6 Survivor-selection rules (when MERGE applies)

When two rows MERGE, one survives. Choose by the following priority:

1. **Label form preference:** the form that appears on the most recent labels wins. "López de Heredia" beats "Lopez de Heredia". "Château Margaux" beats "Chateau Margaux".
2. **Accent preservation:** if both forms appear on labels historically, prefer the accented form.
3. **Metadata completeness:** the row with more non-NULL metadata fields (website, year_established, hectares, etc.) wins — unless it conflicts with the label-form preference above.
4. **Wine count:** tie-breaker — the row with more linked wines wins.
5. **LWIN presence:** tie-breaker beneath wine count — the row with an LWIN external_id wins.
6. **Older row:** last tie-breaker — the earlier `created_at` wins (preserves citation links).

The absorbed row's name is written to `producer_aliases` with `alias_type='merged_from'`. All wines, external_ids, provenance pointers, and staging canonical_producer_id references on the absorbed row are re-pointed to the survivor. The absorbed row itself is soft-deleted (`deleted_at = now()`) rather than hard-deleted, and the full merge event is recorded in `producer_merge_history` with a JSON snapshot for reversibility.

### 11.7 Decision verbatim for LLM prompts

> **A producer is the brand printed on the label.** Two producer rows represent the same producer (MERGE) if a shopper holding bottles from each row would see the same brand identity on the front label — accounting for accent variants, transliterations, abbreviations, and rename-with-continuity. They represent distinct producers (SKIP) if the brand identities differ and there is no ownership relation. They are PARENT-CHILD when the brands differ but one owns or legally contains the other. Retailers are never producers. Second wines are wines, not producers. Private labels are their own producer (with the actual vintner recorded per-wine). Corporate holdcos are not imported as producers; the label brand is.

### 11.8 Application to Sprint 6

Section 11 (all of it) is embedded **verbatim** as the identity-policy preamble in every L1, L2, and L3 prompt in Sprint 6. The preamble is cached (Anthropic prompt caching) so the per-pair marginal cost is only the variable pair context. Any amendment to Section 11 during Sprint 6 must be logged to `docs/DECISIONS.md` before taking effect, because an in-sprint rule change invalidates verdicts issued under the prior version.

---

## Appendix: Open Design Questions for Session 3

1. **Classification in display_name vs separate display:** Currently spec'd as part of display_name. Could also be a separate badge/pill in the UI. Decision deferred to frontend design.

2. **Négociant bottlings vs estate:** Same wine (Gevrey-Chambertin 2019) bottled by two different négociants = two different wines? **Recommendation: YES, different wines.** The producer is part of the identity tuple. Louis Jadot Gevrey-Chambertin ≠ Joseph Drouhin Gevrey-Chambertin.

3. **Multi-vintage wines:** Champagne NV, Solera sherry, multi-vintage Port blends. These have `wine_vintages.vintage_year = 0` (NV convention). Confirmed NV wines only.

4. **TTB inflated counts:** Many TTB matches will be false positives due to substring matching. Session 3 will need precise matching (normalized exact + manual review for the 50 Batch 0 producers).

5. **Portfolio parent companies:** Recommendation is to NOT create Treasury Wine Estates / Constellation / E&J Gallo as producers. Use the label brand (Penfolds, Robert Mondavi, Barefoot) as the producer — that's what the consumer sees. Revisit if user disagrees.
