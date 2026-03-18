# Loam v2 Schema: Deep Expert Assessment

## Context

Loam is about to transition from its geographic/reference foundation phase into populating wines, producers, and vintages at scale. Once significant wine data is in the tables, schema changes become painful — migrations on populated tables with foreign keys, data backfills, and application-level changes compound. This assessment identifies every gap, structural flaw, and missing knowledge domain that a wine professional would expect, so we can address them **before** the wine import phase begins.

The assessment is organized from most critical (would block core functionality) to least critical (nice-to-have enrichments).

## Parts Overview

| Part | Title | What It Covers |
|------|-------|----------------|
| **1** | Schema vs. Reality Discrepancies | soil_types missing documented columns |
| **2** | Wine Classification & Typology | Sweetness, color, wine type, fortification, sparkling method, sweet method, bottle format |
| **3** | Tasting & Sensory Data | WSET SAT gaps, structured flavor descriptors, food pairings, aging/maturity |
| **4** | Structural & Relationship Problems | Certs on wrong entity, closure on wrong table, missing junction tables, drinking window sprawl |
| **5** | Missing Entities | Vineyards, winemakers, producer type/ownership, grape parentage, classification systems |
| **6** | Missing Fields on Existing Tables | Column-by-column additions for wines, wine_vintages, producers, grapes, soil_types, appellations |
| **7** | Missing Junction / Relationship Tables | appellation_grapes, varietal_category_grapes, producer certs, vineyard tables |
| **8** | Missing Knowledge Domains | Viticulture depth, extended winemaking, import chain, vintage quality, chemistry→sensory bridge |
| **9** | *(Superseded by revised Part 9 in addendum)* | Original priority list — see revised version below |
| **10** | Addendum — Second Deep Pass | External ID scaling, NV composition, multi-appellation wines, score normalization, availability status, label images |
| **11** | Flex Field Strategy | Three-tier architecture (typed columns → entity_attributes → metadata JSONB) with promotion path |
| **12** | Grape Data Depth | Expanded grape columns, grape_synonyms table, extended attributes for VIVC/science data |
| **13** | Additional Structural Concerns | Publications scoring metadata, appellation regulatory detail, WSET scale decision |
| **Revised 9** | Prioritized Recommendations (Final) | 36 items across Tier 0 (structural) → Tier 1 (before import) → Tier 2 (soon after) → Tier 3 (defer) |
| **14** | Schema Doc Maintenance | What to update after changes |
| **15** | Migration Strategy | Order of operations, backward compatibility |

**Scope:** 21 new tables, ~45 new columns across 10 existing tables.

**Part B (Implementation Spec)** at the bottom of this document is the actionable blueprint with all user decisions incorporated. Parts 1–15 above are retained as analysis reference.

---

## Part 1: Schema vs. Reality Discrepancies

Things SCHEMA.md documents but the database doesn't actually have.

### 1.1 soil_types missing physical properties
SCHEMA.md documents `drainage_rate (0-1)`, `heat_retention (0-1)`, `water_holding_capacity (0-1)` on `soil_types`. **These columns do not exist in the database.** The table only has: id, slug, name, description, timestamps, deleted_at. The 39 soil type descriptions mention these properties in prose ("excellent drainage, low pH") but there's no structured data.

**Impact:** The soil_type_insights AI table has `ai_drainage_explanation` and `ai_best_grapes` — these were designed to complement structured physical properties. Without the structured data, soil comparison and filtering is impossible.

**Fix:** Add the three numeric columns. These can be populated from geological reference data or AI-estimated from the existing descriptions.

---

## Part 2: Wine Classification & Typology

These are fundamental attributes that define what a wine *is*. A user searching for "dry Riesling" or "vintage Port" or "traditional method sparkling" cannot do so with the current schema.

### 2.1 No sweetness level
**The single biggest classification gap.** Wine has three fundamental axes: color, effervescence, and sweetness. The schema captures two but not the third. A Riesling can be bone dry (Trocken) or dessert sweet (Trockenbeerenauslese). The schema has `rs_g_l` (residual sugar) on `wine_vintages`, but:
- No categorical sweetness on `wines`: dry / off-dry / medium-sweet / sweet / luscious
- No way to search/filter by sweetness
- German Prädikat system (Kabinett through TBA) is a sweetness classification — no home for it
- Alsace Vendange Tardive / Sélection de Grains Nobles — no home
- Loire Sec/Demi-Sec/Moelleux/Liquoreux — no home

**Recommendation:** Add `sweetness_level TEXT` to `wines` (dry/off-dry/medium-sweet/sweet/luscious). This is the wine-level default. Consider also adding `pradikat TEXT` to `wine_vintages` for German/Austrian vintage-specific designations.

### 2.2 No wine color on wines
Wine color lives on `varietal_categories.color` (red/white/rose/orange) but NOT on `wines` directly. This breaks for:
- **Blanc de Noirs** — white wine from red grapes (Champagne). Varietal category would say "red" but the wine is white.
- **Orange wine** — white grapes, red winemaking technique. Category might say "white" but it's orange.
- **Rosé** — many are made from red-grape categories but are a distinct color.
- **Blanc de Blancs** — white wine from white grapes, but the varietal category might be "Champagne Blend" with no color.

**Recommendation:** Add `color TEXT` to `wines` (red/white/rose/orange/amber). This is the actual wine color, independent of grape color.

### 2.3 No wine type / fortification
Port, Sherry, Madeira, Marsala, Vin Doux Naturel, Rutherglen Muscat, Commandaria — these are major wine categories with distinct production methods, service contexts, and aging profiles. The schema has no way to distinguish a table wine from a fortified wine.

Also missing: aromatized wines (Vermouth), wines with specific production distinctions (pétillant naturel vs. traditional method).

**Recommendation:** Add `wine_type TEXT` to `wines`. Values: `table` (default), `fortified`, `aromatized`, `dessert`. This is orthogonal to color, effervescence, and sweetness — a wine can be red + still + sweet + fortified (Port) or white + sparkling + dry + table (Champagne).

### 2.4 No sparkling method
`effervescence` captures still/sparkling/semi_sparkling but the *method* is a fundamental quality and style indicator:
- **Traditional method** (Champagne, Cava, Crémant, Franciacorta) — second fermentation in bottle
- **Charmat / tank method** (Prosecco, Lambrusco) — second fermentation in tank
- **Ancestral method** (Pétillant Naturel) — single fermentation, bottled before complete
- **Transfer method** — hybrid
- **Carbonation** — injected CO₂ (lowest quality)

These define completely different products at different price points with different aging behavior.

**Recommendation:** Add `sparkling_method TEXT` to `wines` (traditional/charmat/ancestral/transfer/carbonation). Only applicable when `effervescence != 'still'`.

### 2.5 No sweet wine production method
How a wine becomes sweet is as important as the fact that it's sweet:
- **Botrytis / Noble rot** (Sauternes, Tokaji Aszú, German BA/TBA)
- **Late harvest** (Vendange Tardive, Spätlese)
- **Ice wine / Eiswein** — grapes frozen on vine
- **Passito / Appassimento** — dried grapes (Amarone, Vin Santo, Recioto)
- **Fortification** — spirit added to arrest fermentation (Port, VDN)
- **Vin de Paille** — straw wine, grapes dried on straw mats
- **Cryoextraction** — artificial freezing (legal in some regions)

**Recommendation:** Add `sweet_method TEXT` to `wines` (botrytis/late_harvest/ice_wine/passito/fortified/vin_de_paille/cryoextraction). Only applicable when `sweetness_level` is not 'dry'.

### 2.6 Bottle format missing
750ml, 375ml (half), 1.5L (magnum), 3L (jeroboam/double magnum), etc. Bottle format affects aging, pricing, and collectibility. A magnum ages differently than a 750ml. Prices per format vary significantly.

**Recommendation:** Add `bottle_format_ml INTEGER DEFAULT 750` to `wine_vintages`. This is vintage-level because a producer may offer different formats for different years. Common values: 187, 375, 500, 750, 1500, 3000, 4500, 6000, 9000, 12000, 15000.

---

## Part 3: Tasting & Sensory Data

This is the largest gap area. For a platform whose tagline could be "the full story of a wine," the sensory dimension is almost entirely missing in structured form.

### 3.1 WSET SAT framework not implemented
The schema has the right idea with `acidity`, `tannin`, `body`, `alcohol_level` (1-5 WSET scales) on `wine_vintages`. But:
- **None of the scrapers populate these fields.** They exist but are always NULL.
- The WSET Systematic Approach to Tasting (SAT) has more dimensions than these four.
- These should probably be on `wine_insights` (AI-assessed) rather than `wine_vintages` (factual data), because WSET scales are subjective assessments, not measured values. A producer reports pH (factual); tannin level 1-5 is an interpretation.

**Missing WSET SAT dimensions that belong in the schema:**

**Appearance:**
- Color intensity (pale/medium/deep)
- Color hue (lemon/gold/amber for whites; purple/ruby/garnet/tawny for reds)
- Viscosity / tears / legs (indicator of alcohol and sugar)
- Clarity (clear/hazy — relevant for natural/unfiltered wines)

**Nose:**
- Aroma intensity (light/medium/pronounced)
- Aroma development (youthful/developing/fully developed/tired)

**Palate:**
- Sweetness (dry/off-dry/medium/sweet/luscious) — overlaps with 2.1 above
- Finish length (short/medium/long)
- Complexity (simple/moderate/complex)
- Quality level (faulty/poor/acceptable/good/very good/outstanding)

**Recommendation:** Create a `wine_vintage_tasting` table (or add to `wine_vintage_insights`) with structured WSET SAT fields. These are AI-assessed from critic notes, not factual data. Keep `ph`, `ta_g_l`, `rs_g_l` etc. on `wine_vintages` as measured chemistry. Move `acidity`, `tannin`, `body`, `alcohol_level` (the subjective 1-5 scales) to the tasting/insights layer.

### 3.2 No structured flavor descriptors
The single most asked question about a wine is "what does it taste like?" The schema has `winemaker_notes` and `vintage_notes` as free text, and `ai_flavor_impact` in insights, but no structured tasting descriptors.

The **Computational Wine Wheel 2.0** (985 descriptor attributes, CC0 license) is already in SOURCES.md as a planned data source. It provides a standardized vocabulary (citrus → lemon → lemon zest; earth → mineral → wet stone).

**Recommendation:** Create a `tasting_descriptors` reference table (id, slug, name, category, parent_descriptor_id for hierarchy, wheel_position). Then a `wine_vintage_descriptors` junction table (wine_id, vintage_year, descriptor_id, frequency — how often critics mention it, source). This enables structured queries like "show me wines with minerality and citrus notes."

### 3.3 No structured food pairing data
`wines.food_pairings` is a free text field. For a platform that follows VOICE.md's detailed food pairing philosophy (name specific cuisines, explain the why, cover Tuesday night AND Saturday dinner), structured pairings would be much more powerful.

**Recommendation:** Consider a `food_categories` reference table and `wine_food_pairings` junction with `pairing_strength` (classic/good/adventurous) and `pairing_logic` (why it works). This might be Phase 2 — the AI insights food pairing text may be sufficient for MVP. Flag for later.

### 3.4 No aging/maturity tracking
The schema has excellent drinking window data (producer, critic, calculated, AI — four sources on wine_vintage_insights). But there's no concept of **current maturity status**. Is a 2015 Bordeaux currently "too young," "approaching maturity," "at peak," "past peak," or "declining"?

**Recommendation:** This can be a computed field in the application layer from drinking_window data + current date. No schema change needed, but worth noting as a gap in the insights — `ai_current_status` or similar on `wine_vintage_insights` could capture "drink now" vs "hold" vs "past peak."

---

## Part 4: Structural & Relationship Problems

Issues where tables are connected incorrectly, relationships are missing, or data is modeled at the wrong level.

### 4.1 Farming/biodiversity certs link to wines, not producers
**Currently:** `wine_farming_certifications` and `wine_biodiversity_certifications` link certs to individual wines.

**Reality:** Certifications are almost always at the **producer or estate level**. Tablas Creek is Regenerative Organic Certified — that applies to all their wines. Ridge is SIP Certified — same. You don't certify individual wines; you certify the farming operation. The scraper even captures certs and stores them in `wines.metadata.certifications` because there's no producer-level table.

**Recommendation:** Add `producer_farming_certifications` (producer_id, farming_certification_id, certified_since, certified_until, certifying_body) and `producer_biodiversity_certifications`. Keep the wine-level tables for exceptions (a producer might have one organic vineyard and one conventional).

### 4.2 Closure belongs on wine_vintages, not wines
**Currently:** `wines.closure` (cork/screwcap/diam/wax/other).

**Reality:** Producers change closures between vintages. Penfolds switched Bin 389 to screwcap. Many New Zealand and Australian producers moved from cork to screwcap over a specific vintage. Same wine, different closure by year.

**Recommendation:** Move `closure` to `wine_vintages`. Keep on `wines` only as a "default/typical" if desired, but the authoritative value should be per-vintage.

### 4.3 Several wine-level fields should be vintage-level
Similarly to closure, these fields on `wines` can change between vintages:
- `fermentation_vessel` — a producer might switch from barrel to concrete egg for a specific vintage
- `oak_origin` — might change supplier
- `yeast_type` — might experiment with native vs commercial
- `fining` / `filtration` — might change approach

These are already partially duplicated: `wine_vintages` has `duration_in_oak_months`, `new_oak_pct`, `mlf`, `carbonic_maceration`, `whole_cluster_pct` (vintage-level winemaking). But `fermentation_vessel`, `oak_origin`, `yeast_type`, `fining`, `filtration` are on `wines` (house-style level).

**Recommendation:** This is a design choice, not necessarily a flaw. The current split is: "house style defaults on wine, vintage-specific numbers on vintage." That's defensible. But `closure` definitely needs to move. For the others, consider making them nullable on `wine_vintages` as overrides, with `wines` as the fallback.

### 4.4 wine_vintage_grapes uses wine_id, not wine_vintage_id
**Currently:** `wine_vintage_grapes` has (wine_id, vintage_year, grape_id) with a UNIQUE constraint. It references `wines` directly, not `wine_vintages`.

**Problem:** This means there's no FK-enforced relationship to a specific `wine_vintages` row. You could have a wine_vintage_grapes entry for a wine/year combination that doesn't exist in wine_vintages. This is a data integrity gap.

**Why it was done this way:** `wine_vintages` uses UUID PK (because vintage_year is nullable for NV wines), so you can't use a composite FK. And requiring the UUID means you'd need to look up the wine_vintage first before inserting grapes.

**Recommendation:** Consider adding `wine_vintage_id UUID FK wine_vintages` as an optional column alongside the current composite. This provides referential integrity when the wine_vintage exists, without breaking the insert flow. Or accept this as a known denormalization with application-level enforcement.

### 4.5 No appellation↔grape junction table
`appellations.allowed_grapes_description` is free text ("Grenache, Syrah, Mourvèdre and up to 13 varieties"). This should be a structured relationship.

**Recommendation:** Create `appellation_grapes` (appellation_id, grape_id, is_required BOOLEAN DEFAULT false, max_percentage DECIMAL, min_percentage DECIMAL, notes TEXT). This enables "which appellations allow Grenache?" and "what grapes are permitted in Châteauneuf-du-Pape?" queries. Keep `allowed_grapes_description` for the regulatory prose version.

### 4.6 varietal_categories → grape mapping only works for single varietals
`varietal_categories.grape_id` links to a single grape for single_varietal categories. But named_blend categories (Bordeaux Blend, Rhône Blend, Champagne Blend) have no structured grape composition. You can't query "which grapes make up a Bordeaux Blend?"

**Recommendation:** Create `varietal_category_grapes` (varietal_category_id, grape_id, is_required BOOLEAN, typical_min_pct DECIMAL, typical_max_pct DECIMAL). This captures that a Bordeaux Blend is typically Cabernet Sauvignon + Merlot + Cabernet Franc + Petit Verdot + Malbec.

### 4.7 No wine hierarchy (second wines)
No way to express that Les Forts de Latour is the second wine of Château Latour, or that Pauillac de Latour is the third wine. This is a well-understood relationship in Bordeaux and increasingly in other regions. It affects how users evaluate quality and value.

**Recommendation:** Add `parent_wine_id UUID FK wines` and `wine_tier TEXT` (grand_vin/second/third) to `wines`. Simple, captures the relationship.

### 4.8 Drinking windows stored in three places
- `wine_vintages`: `producer_drinking_window_start/end`
- `wine_vintage_insights`: `critic_drinking_window_start/end`, `calculated_*`, `ai_*`
- `wine_vintage_scores`: `critic_drink_window_start/end`

The first is producer-stated, the second is AI-synthesized, the third is per-individual-critic. This is defensible (different sources, different purposes) but could confuse: when a user asks "when should I drink this?", which source wins? The priority hierarchy should be documented in the schema.

**Recommendation:** No schema change needed, but document the priority: producer stated > critic consensus > calculated > AI estimated.

---

## Part 5: Missing Entities

Entire concepts that wine professionals expect as first-class entities but don't exist in the schema.

### 5.1 Vineyards
**Currently:** `wines.vineyard_name TEXT` — just a text field.

**For a terroir-focused platform called "Loam," this is a significant gap.** In Burgundy, the vineyard (climat/lieu-dit) IS the classification. Clos de Vougeot, La Tâche, Romanée-Conti — these are vineyard names that define the wine. Same in Germany (Einzellage — already 1,192 in the appellation table). Same increasingly in Napa (To Kalon, Beckstoffer), Barossa (Kalimna), and elsewhere.

A vineyard entity would have:
- Name, slug, appellation_id, producer_id (or multi-producer)
- Latitude, longitude, elevation, aspect, slope (currently on wines — but these are vineyard properties, not wine properties)
- Soil composition (currently modeled at wine/appellation/region level — vineyard is the right level)
- Area (hectares), vine density, training system, rootstock
- Planted year, replanted history

**Recommendation:** This is high-value but high-complexity. Vineyards in Burgundy are shared across multiple producers (multiple domaines own parcels in the same climat). In Napa, a vineyard like To Kalon spans multiple owners. The entity model needs to handle this.

**Proposed structure:**
- `vineyards` (id, slug, name, appellation_id, latitude, longitude, elevation_m, aspect, slope, area_ha, vine_density, established_year)
- `vineyard_producers` (vineyard_id, producer_id, area_ha, planted_year) — for shared vineyards
- `vineyard_soils` (vineyard_id, soil_type_id) — most accurate soil level
- Move `wines.vineyard_name` → `wines.vineyard_id FK vineyards`

**Timing:** This could be Phase 2. For now, `vineyard_name` text works for MVP, but the lat/lng/elevation/aspect/slope on `wines` should eventually migrate to vineyards.

### 5.2 Winemakers
Winemakers move between producers, and consulting winemakers (Michel Rolland, Stéphane Derenoncourt, Philippe Melka, Helen Turley) work across many estates simultaneously. Who made the wine is commercially important.

**Proposed structure:**
- `winemakers` (id, slug, name, role — head_winemaker/consulting/assistant)
- `producer_winemakers` (producer_id, winemaker_id, role, start_year, end_year)

**Timing:** Phase 2/3. Nice-to-have, not blocking.

### 5.3 Producer type / ownership
No way to distinguish: estate/domaine, négociant, cooperative, custom crush, virtual/brand. This fundamentally affects how you interpret the wine. A négociant Burgundy and a domaine Burgundy from the same appellation are very different propositions.

No way to track ownership groups. LVMH owns Moët, Veuve Clicquot, Krug, Dom Pérignon, Château d'Yquem, Château Cheval Blanc. Constellation Brands owns Robert Mondavi, Meiomi, The Prisoner. Jackson Family owns Kendall-Jackson, La Crema, Hartford Family. This matters for understanding the wine market.

**Recommendation:** Add to `producers`:
- `producer_type TEXT` (estate/negociant/cooperative/custom_crush/virtual/corporate)
- `parent_company TEXT` (nullable — "LVMH", "Constellation Brands")
- `hectares_under_vine DECIMAL` (nullable)
- `total_production_cases INTEGER` (nullable — annual production)

### 5.4 Grape parentage
Already discussed in the grape research session. Missing `parent1_grape_id` and `parent2_grape_id` on `grapes`. Cabernet Sauvignon = Cabernet Franc × Sauvignon Blanc. Unlocks grape family tree features.

**Recommendation:** Add two self-referencing FKs to `grapes`.

### 5.5 Classification systems
`appellations.classification_level` is a single text field, but wine classification is multi-layered and operates at different entity levels:
- **Bordeaux 1855:** Producer-level (First through Fifth Growth)
- **Burgundy Grand Cru / Premier Cru:** Vineyard-level
- **St-Émilion GCC:** Producer-level, periodically reclassified
- **Cru Bourgeois / Cru Artisan:** Producer-level
- **Italian DOCG/DOC:** Appellation-level (already modeled)
- **German Prädikat:** Vintage-level (Spätlese, Auslese, etc.)
- **Champagne Grand/Premier Cru:** Village-level

**Recommendation:** This is complex enough to warrant a `classifications` reference table (id, slug, name, system — bordeaux_1855/burgundy/st_emilion_gcc/cru_bourgeois/german_pradikat/champagne_cru) and polymorphic `entity_classifications` (entity_type, entity_id, classification_id, level TEXT, year_classified, year_declassified). Alternatively, simpler: just add `classification TEXT` and `classification_system TEXT` to producers and wines as needed. Defer the full system.

---

## Part 6: Missing Fields on Existing Tables

### 6.1 wines table

| Field | Type | Why |
|-------|------|-----|
| `color` | text | Actual wine color (red/white/rose/orange/amber) — see 2.2 |
| `wine_type` | text | table/fortified/aromatized/dessert — see 2.3 |
| `sweetness_level` | text | dry/off-dry/medium-sweet/sweet/luscious — see 2.1 |
| `sparkling_method` | text | traditional/charmat/ancestral/transfer/carbonation — see 2.4 |
| `sweet_method` | text | botrytis/late_harvest/ice_wine/passito/fortified/vin_de_paille — see 2.5 |
| `parent_wine_id` | uuid FK wines | Second/third wine relationship — see 4.7 |
| `wine_tier` | text | grand_vin/second/third — see 4.7 |
| `vineyard_id` | uuid FK vineyards | When vineyards entity created — see 5.1 |

### 6.2 wine_vintages table

| Field | Type | Why |
|-------|------|-----|
| `bottle_format_ml` | integer | 187/375/500/750/1500/3000/etc — see 2.6 |
| `closure` | text | Move from wines — see 4.2 |
| `fermentation_vessel` | text | Override from wine-level default |
| `oak_origin` | text | Override from wine-level default |
| `yeast_type` | text | Override from wine-level default |
| `fining` | text | Override from wine-level default |
| `filtration` | boolean | Override from wine-level default |
| `pradikat` | text | German/Austrian: kabinett/spatlese/auslese/ba/tba/eiswein |
| `maceration_days` | integer | Cold soak + skin contact duration |
| `lees_aging_months` | integer | Sur lie, important for Champagne, Muscadet |
| `bâtonnage` | boolean | Lees stirring |

### 6.3 producers table

| Field | Type | Why |
|-------|------|-----|
| `producer_type` | text | estate/negociant/cooperative/custom_crush/virtual — see 5.3 |
| `parent_company` | text | Corporate ownership — see 5.3 |
| `hectares_under_vine` | decimal | Vineyard size |
| `total_production_cases` | integer | Annual production volume |

### 6.4 grapes table

| Field | Type | Why |
|-------|------|-----|
| `parent1_grape_id` | uuid FK grapes | Parentage — see 5.4 |
| `parent2_grape_id` | uuid FK grapes | Parentage — see 5.4 |
| `grape_type` | text | wine/table/rootstock/dual — VIVC classifies this |
| `species` | text | vinifera/labrusca/riparia/hybrid — matters for American & hybrid grapes |

### 6.5 soil_types table

| Field | Type | Why |
|-------|------|-----|
| `drainage_rate` | decimal | 0-1 scale — documented but missing from DB |
| `heat_retention` | decimal | 0-1 scale — documented but missing from DB |
| `water_holding_capacity` | decimal | 0-1 scale — documented but missing from DB |
| `geological_origin` | text | igneous/sedimentary/metamorphic/alluvial — useful for grouping |
| `parent_soil_type_id` | uuid FK soil_types | Hierarchy (e.g., Kimmeridgian is a type of Limestone) |

### 6.6 appellations table

| Field | Type | Why |
|-------|------|-----|
| `area_ha` | decimal | Total planted area — important size indicator |
| `elevation_min_m` | integer | Elevation range — terroir data |
| `elevation_max_m` | integer | |

---

## Part 7: Missing Junction / Relationship Tables

| Table | PK | Why |
|-------|-----|-----|
| `appellation_grapes` | (appellation_id, grape_id) | Structured allowed varieties — see 4.5 |
| `varietal_category_grapes` | (varietal_category_id, grape_id) | Blend composition — see 4.6 |
| `producer_farming_certifications` | (producer_id, farming_certification_id) | Certs at right level — see 4.1 |
| `producer_biodiversity_certifications` | (producer_id, biodiversity_certification_id) | Same — see 4.1 |
| `vineyard_soils` | (vineyard_id, soil_type_id) | Most accurate soil level — see 5.1 |
| `vineyard_producers` | (vineyard_id, producer_id) | Shared vineyards — see 5.1 |

---

## Part 8: Entire Knowledge Domains Missing or Thin

### 8.1 Viticulture (thin)
The schema captures some vine data on wines (vine_planted_year, vine_age_description, irrigation_type) but misses key viticulture concepts:
- **Training system** (VSP, gobelet/bush vine, pergola, lyre, guyot, cordon) — fundamental to how grapes grow and ripen
- **Rootstock** — matters for phylloxera resistance, vigor, drought tolerance. Own-rooted vines (pre-phylloxera, Chile, some old Barossa) are notable.
- **Vine density** (vines per hectare) — affects quality vs quantity tradeoff
- **Canopy management** (leaf pulling, shoot thinning, green harvest)
- **Cover crops** (used in organic/biodynamic viticulture)
- **Yield** (hl/ha or tons/acre for a specific vintage) — on wine_vintages

These belong on a vineyard entity or on wines as nullable enrichment fields.

**Recommendation:** Add `training_system TEXT`, `rootstock TEXT`, `vine_density_per_ha INTEGER`, `yield_hl_ha DECIMAL` as nullable fields. Training system and rootstock belong on vineyard or wine level. Yield belongs on wine_vintages.

### 8.2 Extended winemaking (thin)
The schema has oak aging well covered but misses several important techniques:
- **Maceration duration** — cold soak days, total skin contact days. Critical for reds.
- **Lees aging** — months sur lie, bâtonnage (stirring). Critical for Champagne, white Burgundy, Muscadet Sur Lie.
- **Skin contact time** — for orange/amber wines specifically.
- **Co-fermentation** — Viognier co-fermented with Syrah (Côte-Rôtie tradition).
- **Extended maceration** — post-fermentation skin contact (Barolo, some Napa Cabs).
- **Chapitalization** — added sugar before fermentation (legal in some regions, not others). Controversial.
- **Acidification / de-acidification** — adjusting acid levels. Common in warm climates.
- **Micro-oxygenation** — controversial technique, alternative to barrel aging.
- **Aging vessel detail** — the schema has `fermentation_vessel` but no `aging_vessel`. Many wines ferment in stainless and age in barrel, or ferment in barrel and age in concrete.

**Recommendation:** Add to `wine_vintages`: `maceration_days INTEGER`, `lees_aging_months INTEGER`, `batonnage BOOLEAN`, `skin_contact_days INTEGER` (for orange wines), `aging_vessel TEXT` (same enum as fermentation_vessel), `yield_hl_ha DECIMAL`, `co_fermentation_grapes TEXT` (nullable).

### 8.3 Import/distribution chain (not modeled)
For the US market especially, the importer is hugely important as a quality signal. Kermit Lynch, Louis/Dressner, Skurnik, Rosenthal, Vineyard Brands — these importers curate portfolios that act as quality filters. A wine imported by Kermit Lynch carries an implicit endorsement.

**Recommendation:** Consider an `importers` reference table and `producer_importers` (producer_id, importer_id, country_id — which importer in which market). This is Phase 2/3 but worth noting. Already in SOURCES.md as planned scraping targets.

### 8.4 Vintage quality at appellation level
Individual wine vintage scores exist, but there's no concept of **appellation-level vintage quality**. "2010 was a great year in Bordeaux" is fundamental wine knowledge. The `appellation_vintages` table has weather data but no quality assessment.

**Recommendation:** Add to `appellation_vintages`: `vintage_rating TEXT` (poor/below_average/average/good/very_good/excellent/exceptional), `vintage_rating_source UUID FK source_types`, `vintage_summary TEXT`. This can be AI-generated from weather data + aggregate scores.

### 8.5 Wine chemistry → sensory bridge (not modeled)
The schema has excellent chemistry (`ph`, `ta_g_l`, `rs_g_l`, `va_g_l`, `so2_*`, `brix_at_harvest`) and a placeholder for sensory (WSET 1-5 scales, currently unused). But there's no bridge between them. pH + TA predict perceived acidity. RS predicts sweetness perception. Alcohol predicts body. These are well-understood relationships in wine science.

**Recommendation:** This is application-layer logic, not schema. But the schema should ensure the chemistry data is captured consistently (it is) and the sensory assessments have a clean home (currently they're orphaned 1-5 fields that nothing populates). Decision: are WSET 1-5 scales factual data (wine_vintages) or AI assessments (insights)?

---

## Part 9: Prioritized Recommendations

### Do Before Wine Import (Critical)
1. Add `color` to `wines`
2. Add `wine_type` to `wines` (table/fortified/aromatized/dessert)
3. Add `sweetness_level` to `wines`
4. Add `sparkling_method` to `wines`
5. Add `producer_type` to `producers`
6. Add `producer_farming_certifications` and `producer_biodiversity_certifications` tables
7. Create `appellation_grapes` junction table
8. Add `parent1_grape_id`, `parent2_grape_id` to `grapes`
9. Add soil physical properties to `soil_types` (fix schema-vs-reality discrepancy)
10. Add `bottle_format_ml` to `wine_vintages`
11. Move or duplicate `closure` to `wine_vintages`

### Do Soon After (High Value)
12. Add `sweet_method` to `wines`
13. Add `pradikat` to `wine_vintages`
14. Create `varietal_category_grapes` junction table
15. Add `parent_wine_id` and `wine_tier` to `wines`
16. Add `maceration_days`, `lees_aging_months`, `aging_vessel`, `yield_hl_ha` to `wine_vintages`
17. Add `parent_company`, `hectares_under_vine`, `total_production_cases` to `producers`
18. Add `vintage_rating` to `appellation_vintages`
19. Add `grape_type`, `species` to `grapes`
20. Decide where WSET 1-5 sensory scales live (wine_vintages vs insights)

### Defer (Phase 2+)
21. Vineyard entity (complex, high value but can start with vineyard_name text)
22. Winemaker entity
23. Classification system (complex, multi-entity)
24. Structured tasting descriptors (wine wheel)
25. Structured food pairings
26. Importer entity
27. Training system, rootstock, vine density fields

---

## Part 10: Addendum — Second Deep Pass

Items missed in the first assessment, focusing on: structural decisions that become painful to change with data in the tables, grape data depth, and flex field strategy.

### 10.1 External ID pattern doesn't scale (HARD TO CHANGE LATER)

**Currently:** `wine_vintages` has three hardcoded external ID columns: `cellartracker_id`, `wine_searcher_id`, `vivino_id`. Every new data source (Liv-ex LWIN, Wine.com, K&L, Total Wine, Decanter, etc.) requires an ALTER TABLE to add another column. Most rows will have NULLs for most systems.

**This is a table-level structural problem.** Once wine_vintages has 100K+ rows, adding columns is still fast in Postgres, but the *pattern* is wrong — you end up with a table that's 40% external ID columns. And producers would need the same pattern (Vivino winery ID, Wine-Searcher producer ID, etc.).

**Recommendation:** Create `external_ids` table:
```
external_ids (
  id UUID PK,
  entity_type TEXT NOT NULL,        -- 'wine', 'wine_vintage', 'producer', 'grape'
  entity_id UUID NOT NULL,
  vintage_year INTEGER,             -- nullable, for wine_vintage lookups
  system TEXT NOT NULL,             -- 'cellartracker', 'vivino', 'wine_searcher', 'lwin', etc.
  external_id TEXT NOT NULL,
  external_url TEXT,                -- direct link to the entity in that system
  UNIQUE(entity_type, entity_id, vintage_year, system)
)
```

This is polymorphic (like `trends`), handles any entity type, and you never need to ALTER TABLE for a new data source. Keep the existing three columns on wine_vintages for backward compatibility during migration, but new integrations use this table.

**Why this is hard to change later:** Once scrapers are writing to `vivino_id`, `cellartracker_id`, etc., and queries depend on them, migrating to a junction table means updating every scraper, every query, and backfilling. Do it now while the data is small.

### 10.2 NV wine vintage composition (HARD TO CHANGE LATER)

**Currently:** NV wines get a `wine_vintages` row with `vintage_year = NULL`, plus `solera_system BOOLEAN` and `age_statement_years INTEGER`. But NV wines are far more complex:

- **NV Champagne** is typically ~60% base vintage + reserve wines from 3–6 prior years. Krug Grande Cuvée might blend 120+ wines from 10+ vintages.
- **Sherry solera** is a fractional blending system where wine passes through criaderas of increasing age.
- **NV Port (Ruby, Tawny)** blends vintages for consistency.
- **Multi-vintage blends** (increasingly common in Australia, e.g., Penfolds Grange "multi-vintage" experiments).

The current schema can't express "this NV Champagne is 55% 2019, 20% 2018, 15% 2017, 10% 2016." This is high-value data that producers increasingly share on back labels and tech sheets.

**Recommendation:** Create `wine_vintage_components`:
```
wine_vintage_components (
  id UUID PK,
  wine_id UUID FK wines NOT NULL,
  nv_vintage_year INTEGER,          -- the NV "vintage" (NULL or the base year)
  component_vintage_year INTEGER NOT NULL,  -- the contributing vintage
  percentage DECIMAL,               -- nullable if unknown
  component_wine_id UUID FK wines,  -- nullable, if from a different wine
  notes TEXT,                       -- "reserve wine", "criadera 3", etc.
  source UUID FK source_types
)
```

**Why this is hard to change later:** NV wines are already modeled with NULL vintage_year. Adding this is additive, but once NV wines start flowing in, you'll want this data structure in place so scrapers can write component data immediately rather than stashing it in metadata JSONB.

### 10.3 Multi-appellation wines (HARD TO CHANGE LATER)

**Currently:** `wines.appellation_id` is a single FK. But in California and Australia especially, wines can blend grapes from multiple appellations. A wine labeled "Sonoma County" might source from Russian River Valley + Sonoma Coast + Alexander Valley. An Australian wine might be "Barossa Valley / McLaren Vale."

There's `wine_regions` for multi-region wines, but no equivalent `wine_appellations` for multi-appellation wines.

**Recommendation:** Create `wine_appellations` (wine_id, appellation_id, composite PK) mirroring `wine_regions`. Keep `wines.appellation_id` as the primary/label appellation.

**Why this is hard to change later:** The appellation FK is the primary geographic anchor for a wine. If you discover a wine needs multiple appellations after import, you'd need to decide which one stays in the FK and move others to a junction table. Build the junction table now.

### 10.4 Score normalization (HARD TO CHANGE LATER)

**Currently:** `wine_vintage_scores` has `score`, `score_low`, `score_high`, `score_scale`. But there's no normalized score for cross-publication comparison. Comparing a 95/100 from Wine Advocate to a 19/20 from Jancis Robinson to a 4.2/5 from Vivino requires normalization.

**Recommendation:** Add `score_normalized DECIMAL` to `wine_vintage_scores` — a 0-100 scale computed on insert. The normalization formula is simple: (score / max_score) * 100. Also add the formula logic to the `publications` table:
- Add `score_scale_min DECIMAL`, `score_scale_max DECIMAL` to `publications` (e.g., Wine Advocate: 50-100, Jancis: 0-20, Vivino: 1-5).

**Why this is hard to change later:** Once you have 50K+ scores from mixed publications, computing aggregates ("what's the consensus score for this wine?") requires either runtime normalization (slow) or backfilling the normalized column. Build it in from the start.

### 10.5 Wine lifecycle / availability status

**Currently:** No concept of whether a wine-vintage is: current release, sold out, futures/en primeur, library/cellar release, discontinued, or auction only. This affects what you show users and how you present value.

**Recommendation:** Add `availability_status TEXT` to `wine_vintages` (current_release / sold_out / futures / library / discontinued / auction_only). Nullable — omit if unknown.

### 10.6 Label image and visual data

**Currently:** No place for wine label images or bottle shots. Visual identification is one of the first things consumers use. TTB COLA has 4.6M label images (public domain). Vivino is built on label scanning.

**Recommendation:** Add `label_image_url TEXT` and `bottle_image_url TEXT` to `wine_vintages`. These are URLs to stored images, not the images themselves. The SOURCES.md already notes TTB COLA and WineSensed as potential image sources.

---

## Part 11: Flex Field Strategy

The user's goal: have structured ways to capture high-quality data from diverse sources even when that data doesn't fit neatly into fixed columns. This needs to be professional and follow best practices.

### 11.1 The problem with current `metadata JSONB`

`metadata JSONB` exists on wines, wine_vintages, and producers. It's being used as a dumping ground:
- Ridge scraper puts `growing_season_weather`, `barrels`, `aging`, `fermentation`, `selection`, `history` into metadata
- Tablas Creek puts `production_notes`, `blending_date`, `certifications` into metadata
- Stag's Leap puts `winemaking` narratives into metadata

This data is **invisible to SQL queries**, has **no type validation**, and has **no documentation** of what keys exist. As more scrapers run, the metadata blobs become increasingly heterogeneous and unmaintainable.

### 11.2 Recommended three-tier flex architecture

**Tier 1: Typed columns** — For known, important, queryable facts.
This is the standard approach. If a field is well-understood and will exist for many wines, it gets a typed column. Examples: `alcohol_pct`, `ph`, `duration_in_oak_months`, `cases_produced`, all the new fields proposed in this assessment.

**Tier 2: Extended attributes table** — For semi-structured, queryable data that doesn't warrant a column yet.
A typed key-value system that sits between fixed columns and freeform JSON:

```
attribute_definitions (
  id UUID PK,
  slug TEXT UNIQUE NOT NULL,
  name TEXT NOT NULL,
  category TEXT NOT NULL,            -- 'viticulture', 'winemaking', 'sensory', 'market', 'regulatory'
  data_type TEXT NOT NULL,           -- 'text', 'numeric', 'boolean', 'date', 'text[]'
  unit TEXT,                         -- 'months', 'percent', 'hectares', 'vines_per_ha', etc.
  description TEXT,
  applies_to TEXT[] NOT NULL,        -- ['wine', 'wine_vintage', 'producer', 'grape', 'appellation']
  created_at TIMESTAMPTZ
)

entity_attributes (
  id UUID PK,
  entity_type TEXT NOT NULL,         -- 'wine', 'wine_vintage', 'producer', 'grape'
  entity_id UUID NOT NULL,
  vintage_year INTEGER,              -- nullable, for wine_vintage attributes
  attribute_id UUID FK attribute_definitions NOT NULL,
  value_text TEXT,
  value_numeric DECIMAL,
  value_boolean BOOLEAN,
  value_date DATE,
  source_id UUID FK source_types,
  confidence DECIMAL,                -- 0-1, how reliable is this data point
  notes TEXT,                        -- freeform context
  created_at TIMESTAMPTZ,
  updated_at TIMESTAMPTZ,
  UNIQUE(entity_type, entity_id, vintage_year, attribute_id)
)
```

**Examples of what goes here:**
- `training_system` = 'gobelet' (viticulture, text, applies_to wine)
- `rootstock` = '110R' (viticulture, text, applies_to wine)
- `vine_density_per_ha` = 6000 (viticulture, numeric, applies_to wine)
- `chapitalization` = false (winemaking, boolean, applies_to wine_vintage)
- `micro_oxygenation` = true (winemaking, boolean, applies_to wine_vintage)
- `co_fermentation_grape` = 'Viognier' (winemaking, text, applies_to wine_vintage)
- `skin_contact_days` = 21 (winemaking, numeric, applies_to wine_vintage)
- `contains_sulfites` = true (regulatory, boolean, applies_to wine)
- `fining_agent` = 'bentonite' (winemaking, text, applies_to wine_vintage)
- `estimated_production_bottles` = 12000 (market, numeric, applies_to wine_vintage)
- `ssr_marker_vvs2` = '133:151' (genetics, text, applies_to grape)

**Why this works:**
- **Queryable:** `SELECT * FROM entity_attributes ea JOIN attribute_definitions ad ON ea.attribute_id = ad.id WHERE ad.slug = 'training_system' AND ea.value_text = 'gobelet'`
- **Typed:** Each attribute has a declared data_type, and only the matching value column should be populated
- **Source-tracked:** Every attribute has a source FK
- **Discoverable:** `attribute_definitions` serves as a data dictionary — you can query what attributes exist, what categories they belong to, what entities they apply to
- **Promotable:** When an attribute gets enough usage, you "promote" it to a typed column on the entity table and backfill from entity_attributes

**Tier 3: `metadata JSONB`** — For truly unstructured overflow.
Keep metadata JSONB for scraper-specific data that hasn't been categorized yet. But add documentation rules:
- Every key in metadata must be documented in a `docs/METADATA_KEYS.md` file
- Keys should use consistent naming (snake_case)
- When a metadata key appears in 3+ scrapers, it should be promoted to Tier 2 (attribute) or Tier 1 (column)

### 11.3 Promotion path: metadata → attribute → column

This is the lifecycle for a data field:
1. **Scraper discovers new data** (e.g., Tablas Creek provides blending date) → goes into `metadata JSONB`
2. **Second source provides same data** → create an `attribute_definition`, start writing to `entity_attributes` instead
3. **Data proves important and common** → promote to typed column, backfill from entity_attributes, deprecate the attribute

This prevents the "add a column for every possible field" problem while ensuring nothing valuable gets lost.

---

## Part 12: Grape Data Depth

The user specifically wants the schema to handle a wide variety of grape data from sources like VIVC, Wikidata, TTB, OIV, etc.

### 12.1 Current grapes table is too thin

Current: id, slug, name, aliases[], color, origin_country_id, vivc_number, timestamps.

VIVC alone provides 15+ data dimensions per variety. The grapes table needs to be deeper without becoming unwieldy.

### 12.2 Recommended grape columns (add to `grapes` table)

| Field | Type | Source | Why |
|-------|------|--------|-----|
| `parent1_grape_id` | uuid FK grapes | VIVC/Wikidata | Parentage — enables family trees |
| `parent2_grape_id` | uuid FK grapes | VIVC/Wikidata | Parentage |
| `parentage_confirmed` | boolean | VIVC | DNA-confirmed vs bibliographic |
| `species` | text | VIVC | vinifera/labrusca/riparia/rupestris/hybrid/complex_hybrid |
| `grape_type` | text | VIVC | wine/table/rootstock/drying/juice/dual |
| `berry_skin_color` | text | VIVC | blanc/noir/rose/gris/rouge (more specific than current `color`) |
| `origin_region` | text | VIVC/Wikidata | More specific than country (e.g., "Bordeaux" not just "France") |
| `ttb_name` | text | TTB CFR 4.91 | Official US label name if different from canonical |
| `oiv_number` | text | OIV | OIV variety code |
| `wikidata_id` | text | Wikidata | Q-identifier for cross-referencing |

### 12.3 Grape extended attributes (use Tier 2 flex system)

These are valuable but won't exist for most grapes. Perfect for the `entity_attributes` system:

- `ssr_marker_*` (9 standard microsatellite markers from VIVC — VVS2, VVMD5, VVMD7, etc.)
- `breeder` (text — who bred this variety)
- `breeding_institute` (text)
- `crossing_year` (numeric)
- `sex_of_flowers` (text — hermaphrodite/female/male)
- `chlorotype` (text — genetic marker)
- `typical_bud_break` (text — early/mid/late relative to Chasselas)
- `typical_ripening` (text — early/mid/late)
- `disease_susceptibility_powdery_mildew` (text — low/medium/high)
- `disease_susceptibility_downy_mildew` (text)
- `disease_susceptibility_botrytis` (text)
- `vigor` (text — low/medium/high)
- `yield_tendency` (text — low/medium/high)
- `drought_tolerance` (text — low/medium/high)
- `acreage_global_ha` (numeric — total world plantings)
- `acreage_trend` (text — increasing/stable/declining)

This means the grapes table stays clean (core identity + key relationships) while the flex system handles the long tail of viticulture science data that VIVC and other sources provide.

### 12.4 Grape synonym handling

The current `aliases TEXT[]` array works for simple cases (Syrah = Shiraz) but has limitations:
- No source tracking (which authority says this is a synonym?)
- No language/country context (Grenache in France = Garnacha in Spain = Cannonau in Italy)
- No synonym type (synonym vs clone name vs marketing name vs historic name)

**Recommendation:** Consider a `grape_synonyms` table for richer synonym data:
```
grape_synonyms (
  id UUID PK,
  grape_id UUID FK grapes NOT NULL,
  synonym TEXT NOT NULL,
  language TEXT,                      -- 'fr', 'es', 'it', 'de', etc.
  country_id UUID FK countries,       -- where this synonym is used
  synonym_type TEXT,                  -- 'synonym', 'clone', 'marketing', 'historic', 'local'
  source TEXT,                        -- 'vivc', 'wikidata', 'ttb', 'oiv'
  is_primary_in_country BOOLEAN,      -- is this THE name used in that country?
  UNIQUE(grape_id, synonym, country_id)
)
```

Keep `aliases TEXT[]` on grapes for quick lookups, but use `grape_synonyms` as the detailed reference. The array becomes a denormalized cache of synonym names.

**Why this matters for data import:** When matching wine data from different countries, you need to know that "Garnacha" on a Spanish label = "Grenache" in your database. The synonym table with country context makes this matching reliable.

### 12.5 Grape-to-grape relationships beyond parentage

Parentage (parent1/parent2) captures genetic lineage, but there are other grape relationships:
- **Clones** — Pinot Noir has 50+ recognized clones (Dijon 115, 667, 777, Pommard, Wadenswil). These are genetically identical but produce noticeably different wines.
- **Mutations** — Pinot Gris and Pinot Blanc are color mutations of Pinot Noir, not crosses.
- **Sports** — natural mutations selected from existing vines.

**Recommendation:** Add `relationship_type TEXT` context. For clones, these could be tracked as separate grape entries with `parent1_grape_id` pointing to the parent variety and a `is_clone BOOLEAN` flag. Or use the flex attribute system for clone-specific data. This is Phase 2+ but the parentage FK structure should accommodate it.

---

## Part 13: Additional Structural Concerns

### 13.1 publications table needs scoring metadata

`publications` has: id, slug, name, country, url, type. But for score normalization (10.4), it needs:

| Field | Type | Why |
|-------|------|-----|
| `score_scale_min` | decimal | Minimum possible score (e.g., 50 for WA, 0 for JR) |
| `score_scale_max` | decimal | Maximum possible score (e.g., 100 for WA, 20 for JR) |
| `scoring_system` | text | 100-point/20-point/5-star/letter/descriptive |
| `active` | boolean | Is this publication still publishing reviews? |

### 13.2 Appellation regulatory detail is thin

The appellations table has `min_aging_months`, `max_yield_hl_ha`, `min_alcohol_pct`, `allowed_grapes_description`. But major regulatory systems have more requirements:

- **Maximum alcohol** (important for some appellations)
- **Minimum vine age** (some appellations require vines be X years old)
- **Vine density requirements** (minimum vines/ha — Burgundy requires 10,000/ha)
- **Pruning method requirements** (some appellations mandate specific pruning)
- **Permitted winemaking interventions** (chapitalization, acidification, irrigation)

**Recommendation:** Most of these are best handled via the Tier 2 flex attribute system rather than adding columns to appellations. The regulatory detail varies enormously by country and designation type. Add `max_alcohol_pct DECIMAL` and `min_vine_density_per_ha INTEGER` as columns if they're common enough; put the rest in entity_attributes.

### 13.3 The WSET 1-5 scales need a decision

`wine_vintages` currently has `acidity`, `tannin`, `body`, `alcohol_level` as INTEGER (1-5 WSET scales). These are **never populated** and are **subjective assessments, not measured values**. They sit awkwardly next to factual chemistry data (pH, TA, RS).

**Decision options:**
1. **Keep on wine_vintages** — treat as semi-factual (WSET-trained tasters produce consistent assessments). Add `sensory_source UUID FK source_types`.
2. **Move to wine_vintage_insights** — treat as AI-derived from critic notes and chemistry data. Add the four fields plus additional SAT dimensions (finish_length, complexity, aroma_intensity).
3. **Create wine_vintage_tasting** — separate table for structured tasting assessment, keeping wine_vintages purely factual.

**Recommendation:** Option 2 (move to insights). This aligns with Principle #5 (fact and AI stay separate). The chemistry data on wine_vintages is producer-reported fact. The sensory assessments are interpretations. They belong in the insights layer. Add: `sensory_acidity`, `sensory_tannin`, `sensory_body`, `sensory_alcohol`, `sensory_sweetness`, `finish_length`, `aroma_intensity`, `complexity`, `quality_level` to `wine_vintage_insights`.

Then remove or deprecate the four orphaned integer columns from `wine_vintages` during the schema hardening pass.

---

## Revised Part 9: Prioritized Recommendations (Updated)

### Tier 0: Structural Decisions (most painful to change later — do first)
1. Create `external_ids` table (replaces scattered ID columns pattern)
2. Create `wine_appellations` junction table (multi-appellation support)
3. Create `entity_attributes` + `attribute_definitions` tables (flex field system)
4. Add `score_normalized` + scoring metadata to publications/scores
5. Decide WSET 1-5 scale location (recommend move to insights)
6. Create `grape_synonyms` table (richer than aliases array)

### Tier 1: Do Before Wine Import (blocks core data model)
7. Add `color` to `wines`
8. Add `wine_type` to `wines` (table/fortified/aromatized/dessert)
9. Add `sweetness_level` to `wines`
10. Add `sparkling_method` to `wines`
11. Add `producer_type` to `producers`
12. Add `producer_farming_certifications` + `producer_biodiversity_certifications`
13. Create `appellation_grapes` junction table
14. Add `parent1_grape_id`, `parent2_grape_id`, `species`, `grape_type` to `grapes`
15. Fix `soil_types` missing physical properties
16. Add `bottle_format_ml` to `wine_vintages`
17. Move/duplicate `closure` to `wine_vintages`
18. Add `label_image_url` to `wine_vintages`

### Tier 2: Do Soon After (high value enrichment)
19. Add `sweet_method` to `wines`
20. Add `pradikat` to `wine_vintages`
21. Create `varietal_category_grapes` junction table
22. Add `parent_wine_id` + `wine_tier` to `wines`
23. Add `maceration_days`, `lees_aging_months`, `aging_vessel`, `yield_hl_ha` to `wine_vintages`
24. Add `parent_company`, `hectares_under_vine`, `total_production_cases` to `producers`
25. Add `vintage_rating` to `appellation_vintages`
26. Create `wine_vintage_components` (NV composition)
27. Add `availability_status` to `wine_vintages`
28. Add grape extended columns: `parentage_confirmed`, `berry_skin_color`, `origin_region`, `ttb_name`, `oiv_number`, `wikidata_id`
29. Add `score_scale_min`, `score_scale_max` to `publications`

### Tier 3: Defer (Phase 2+)
30. Vineyard entity
31. Winemaker entity
32. Classification system (complex, multi-entity)
33. Structured tasting descriptors (wine wheel reference table + junction)
34. Structured food pairings
35. Importer entity
36. Viticulture fields (training system, rootstock via flex attributes)

---

## Part 14: Schema Doc Maintenance

After implementing changes:
- Update `docs/SCHEMA.md` with all new columns, tables, and relationships
- Log decisions to `docs/DECISIONS.md`
- Fix the soil_types discrepancy in SCHEMA.md (currently documents columns that don't exist)
- Create `docs/METADATA_KEYS.md` documenting all known metadata JSONB keys
- Create `docs/ATTRIBUTE_CATALOG.md` listing all defined attribute_definitions with categories

---

## Part 15: Migration Strategy

### Order of operations
1. Create new tables first (entity_attributes, attribute_definitions, external_ids, appellation_grapes, etc.)
2. ALTER existing tables to add new columns (all nullable — no risk to existing data)
3. Backfill from metadata JSONB where applicable (e.g., move certifications from wine metadata to producer_farming_certifications)
4. Migrate external IDs from wine_vintages columns to external_ids table
5. Deprecate (but don't drop) old columns — mark in SCHEMA.md as deprecated
6. Update scrapers to write to new structure

### Backward compatibility
- All new columns are nullable — existing INSERT statements won't break
- Old external ID columns remain until all scrapers are migrated
- metadata JSONB stays — it's the safety net for the unexpected
- No NOT NULL or CHECK constraints on new columns until data patterns are established

---

## Verification

After schema changes:
- Run `SELECT column_name FROM information_schema.columns WHERE table_name = 'wines'` to confirm new columns
- Run existing scrapers against a test wine to verify backward compatibility
- Query for any NOT NULL constraint violations on existing data
- Run advisory checks via Supabase MCP for security/performance issues
- Verify entity_attributes table can store and retrieve typed data correctly
- Test a sample attribute_definition + entity_attribute insert/query cycle

---

---
---

# PART B: IMPLEMENTATION SPEC

*User decisions from review incorporated. Parts 1–15 above are retained as analysis reference. This section is the actionable blueprint.*

---

## Answers to Open Questions

### Q: 4.4 — Should wine_vintage_grapes FK to wine_vintages? Drop current relationship? NV impact?

**Answer: Keep both FKs.** Add `wine_vintage_id UUID FK wine_vintages` to `wine_vintage_grapes` alongside the existing `wine_id`. NV wines work fine because `wine_vintages` uses a UUID PK — even NV wines (vintage_year = NULL) have a wine_vintages row with a UUID.

- `wine_id` stays for convenience (quick "what grapes does this wine use?" queries)
- `wine_vintage_id` adds referential integrity (can't have blend data for a nonexistent vintage)
- Application code should populate both on insert
- Existing data can be backfilled: match on (wine_id, vintage_year) to find the wine_vintage UUID

### Q: 4.8 — What is "calculated" drinking window and how does it work?

**Answer:** "Calculated" means an **algorithmic formula** applied to structured wine data, distinct from both critic opinions and AI synthesis. The inputs would be:

- **Grape variety aging profile** (Cabernet ages longer than Pinot Gris)
- **Score level** (higher scores correlate with longer aging potential)
- **Oak treatment** (new oak + long barrel time = longer window)
- **Production method** (fortified, traditional method sparkling = longer)
- **Appellation typicity** (Barolo ages longer than Valpolicella)
- **Vintage quality** (great vintages age longer)

Example formula output: "A 94-point Napa Cabernet with 20 months new French oak → drink 2027-2040, peak 2031-2037."

**Priority hierarchy (document in SCHEMA.md):**
1. Producer stated (most authoritative — they made it)
2. Critic consensus (aggregated from wine_vintage_scores drink windows)
3. Calculated (algorithmic from wine attributes)
4. AI estimated (Claude synthesis when other sources unavailable)

### Q: 5.1 — Keep lat/lng/elevation/aspect/slope on wines or move to vineyards?

**Answer: Keep both.** This follows the existing three-tier fallback pattern (wine → appellation → region) already used for soils.

- When `wines.vineyard_id` is set → use vineyard's geography data (authoritative)
- When `wines.vineyard_id` is NULL → use wine-level lat/lng/elevation as fallback
- Many wines don't have a named vineyard (e.g., "Estate Red" blended from multiple blocks)
- Wine-level data stays as nullable fallback; vineyard-level is the preferred source

### Q: 8.1/8.2 — How necessary are viticulture and extended winemaking fields?

**Answer:** The most impactful ones should be typed columns. The rest go in the flex attribute system.

**Worth typed columns** (these meaningfully change how a user understands the wine):
- `maceration_days` — cold soak + skin contact. Critical for reds, orange wines.
- `lees_aging_months` — essential for Champagne, Muscadet Sur Lie, white Burgundy.
- `aging_vessel` — many wines ferment in one vessel, age in another. Missing.
- `yield_hl_ha` — directly correlates with concentration and quality.
- `skin_contact_days` — distinguishes orange wine from white wine.
- `batonnage` — lees stirring, key for Burgundy whites.

**Better as flex attributes** (important for specialists but sparse data):
- Training system (VSP, gobelet, pergola, guyot)
- Rootstock (110R, SO4, 3309C, own-rooted)
- Vine density (vines/ha)
- Chapitalization, acidification, micro-oxygenation
- Cover crops, canopy management
- Co-fermentation details

### Q: 8.3 — Importer schema depth

**Research findings:** US law requires importer name + address on every imported wine label. TTB COLA registry has 2.6M+ label records including importer (applicant) data. COLA Cloud API provides programmatic access (500 free requests/month). Major importers like Kermit Lynch, Skurnik, Wilson Daniels curate quality-filtered portfolios — importer is a strong quality signal.

**Schema:**
```
importers (
  id UUID PK,
  slug TEXT UNIQUE NOT NULL,
  name TEXT NOT NULL,
  country_id UUID FK countries NOT NULL,     -- market they import INTO (e.g., US)
  city TEXT,                                  -- principal place of business
  state TEXT,                                 -- for US importers
  website_url TEXT,
  portfolio_focus TEXT,                       -- 'french', 'italian', 'natural', 'fine_wine', 'broad'
  ttb_permit_id TEXT,                         -- TTB basic permit number (public)
  metadata JSONB,                            -- flexible overflow
  created_at, updated_at, deleted_at
)

producer_importers (
  producer_id UUID FK producers NOT NULL,
  importer_id UUID FK importers NOT NULL,
  is_current BOOLEAN DEFAULT true,
  start_year INTEGER,
  end_year INTEGER,                           -- null = current relationship
  source_id UUID FK source_types,
  PK (producer_id, importer_id)
)
```

**Data population strategy:**
1. Seed major importers manually (~30-50 key US fine wine importers)
2. When scraping producer websites, capture importer if listed
3. Use COLA Cloud API to map wines → importers via TTB label data
4. Source type: 'ttb-cola' for COLA-derived data, 'producer-website' for self-reported

### Q: 8.4 — Vintage quality: AI or direct sources?

**Answer: Both.** Direct sources exist and should be preferred:
- **Wine Spectator** publishes vintage charts (ratings by region by year)
- **Decanter** publishes vintage guides
- **Jancis Robinson** publishes vintage assessments
- **Robert Parker / Wine Advocate** had vintage ratings

Schema supports both: `vintage_rating_source UUID FK source_types` can be 'wine-spectator', 'decanter', 'jancis-robinson', or 'ai-sonnet'. When no published rating exists, AI can synthesize from weather data + aggregate scores.

Add to `appellation_vintages`:
- `vintage_rating TEXT` (poor/below_average/average/good/very_good/excellent/exceptional)
- `vintage_rating_source UUID FK source_types`
- `vintage_summary TEXT`

### Q: 10.1 — External ID strategy / LWIN

**Research findings:** LWIN is the closest thing to an industry-standard wine ID. 200K wines, **free CC-licensed download**, created by Liv-ex. Wine-Searcher integrated it in 2022. Coverage skews toward fine/investment wines. No universal wine ID exists.

**Recommended strategy:**
- Loam's own UUIDs remain the primary identifier (there is no external universal ID)
- LWIN serves as the **preferred external cross-reference** for wines that have one
- Other system IDs (CellarTracker, Vivino, Wine-Searcher) stored in `external_ids` table
- Download the free LWIN database and cross-reference against Loam wines during import
- Add `lwin` on both `wines` (LWIN-7, identifies the wine) and `wine_vintages` (LWIN-11, identifies wine+vintage)

```
wines:         add `lwin TEXT UNIQUE`     — LWIN-7 code (nullable, populated from free Liv-ex DB)
wine_vintages: add `lwin TEXT UNIQUE`     — LWIN-11 code (nullable, wine+vintage identifier)
```

The `external_ids` table handles other systems (CellarTracker, Vivino, etc.). LWIN gets first-class column status on both tables as the industry-standard cross-reference.

### Q: 10.4 — Score normalization revised

**Decision:** Don't normalize individual publication scores. Always show the source publication alongside the score. Normalization only happens at query time when computing aggregate/consensus scores.

**Changes from original recommendation:**
- Do NOT add `score_normalized` to wine_vintage_scores
- DO add `score_scale_min` and `score_scale_max` to `publications` (needed for runtime aggregate normalization)
- Application layer handles normalization formula: `(score - min) / (max - min) * 100`

### Q: 13.3 — WSET sensory scales location

**Decision:** Create `wine_vintage_tasting_insights` as a separate table (from Part 3.1 discussion). Move the four orphaned WSET 1-5 scales from `wine_vintages` to this new insights table. Add expanded SAT dimensions. These are AI-assessed interpretations, not factual data.

---

## Complete New Tables (17 total)

### Tier 0: Structural (hardest to change later)

**1. external_ids**
```sql
CREATE TABLE external_ids (
  id UUID PK DEFAULT gen_random_uuid(),
  entity_type TEXT NOT NULL,
  entity_id UUID NOT NULL,
  vintage_year INTEGER,
  system TEXT NOT NULL,
  external_id TEXT NOT NULL,
  external_url TEXT,
  created_at TIMESTAMPTZ DEFAULT now(),
  UNIQUE(entity_type, entity_id, vintage_year, system)
);
```

**2. wine_appellations**
```sql
CREATE TABLE wine_appellations (
  wine_id UUID NOT NULL REFERENCES wines(id),
  appellation_id UUID NOT NULL REFERENCES appellations(id),
  PRIMARY KEY (wine_id, appellation_id)
);
```

**3. attribute_definitions**
```sql
CREATE TABLE attribute_definitions (
  id UUID PK DEFAULT gen_random_uuid(),
  slug TEXT UNIQUE NOT NULL,
  name TEXT NOT NULL,
  category TEXT NOT NULL,           -- viticulture/winemaking/sensory/market/regulatory/genetics
  data_type TEXT NOT NULL,          -- text/numeric/boolean/date
  unit TEXT,
  description TEXT,
  applies_to TEXT[] NOT NULL,       -- {wine, wine_vintage, producer, grape, appellation}
  created_at TIMESTAMPTZ DEFAULT now()
);
```

**4. entity_attributes**
```sql
CREATE TABLE entity_attributes (
  id UUID PK DEFAULT gen_random_uuid(),
  entity_type TEXT NOT NULL,
  entity_id UUID NOT NULL,
  vintage_year INTEGER,
  attribute_id UUID NOT NULL REFERENCES attribute_definitions(id),
  value_text TEXT,
  value_numeric DECIMAL,
  value_boolean BOOLEAN,
  value_date DATE,
  source_id UUID REFERENCES source_types(id),
  confidence DECIMAL,
  notes TEXT,
  created_at TIMESTAMPTZ DEFAULT now(),
  updated_at TIMESTAMPTZ DEFAULT now(),
  UNIQUE(entity_type, entity_id, vintage_year, attribute_id)
);
```

**5. grape_synonyms**
```sql
CREATE TABLE grape_synonyms (
  id UUID PK DEFAULT gen_random_uuid(),
  grape_id UUID NOT NULL REFERENCES grapes(id),
  synonym TEXT NOT NULL,
  language TEXT,                     -- ISO 639-1: 'fr', 'es', 'it', 'de'
  country_id UUID REFERENCES countries(id),
  synonym_type TEXT,                 -- synonym/clone/marketing/historic/local
  source TEXT,                       -- vivc/wikidata/ttb/oiv
  is_primary_in_country BOOLEAN DEFAULT false,
  created_at TIMESTAMPTZ DEFAULT now(),
  UNIQUE(grape_id, synonym, country_id)
);
```

**6. classifications**
```sql
CREATE TABLE classifications (
  id UUID PK DEFAULT gen_random_uuid(),
  slug TEXT UNIQUE NOT NULL,
  name TEXT NOT NULL,
  system TEXT NOT NULL,              -- bordeaux_1855/burgundy/st_emilion_gcc/cru_bourgeois/champagne_cru
  country_id UUID REFERENCES countries(id),
  description TEXT,
  last_reclassification_year INTEGER,
  created_at TIMESTAMPTZ DEFAULT now(),
  updated_at TIMESTAMPTZ DEFAULT now(),
  deleted_at TIMESTAMPTZ
);
```

**7. classification_levels**
```sql
CREATE TABLE classification_levels (
  id UUID PK DEFAULT gen_random_uuid(),
  classification_id UUID NOT NULL REFERENCES classifications(id),
  level_name TEXT NOT NULL,          -- 'First Growth', 'Premier Cru', etc.
  level_rank INTEGER,                -- 1 = highest
  description TEXT,
  UNIQUE(classification_id, level_name)
);
```

**8. entity_classifications**
```sql
CREATE TABLE entity_classifications (
  id UUID PK DEFAULT gen_random_uuid(),
  classification_level_id UUID NOT NULL REFERENCES classification_levels(id),
  entity_type TEXT NOT NULL,         -- producer/wine/vineyard/appellation
  entity_id UUID NOT NULL,
  year_classified INTEGER,
  year_declassified INTEGER,
  notes TEXT,
  UNIQUE(classification_level_id, entity_type, entity_id)
);
```

### Tier 1: Before Wine Import

**9. appellation_grapes**
```sql
CREATE TABLE appellation_grapes (
  appellation_id UUID NOT NULL REFERENCES appellations(id),
  grape_id UUID NOT NULL REFERENCES grapes(id),
  is_required BOOLEAN DEFAULT false,
  max_percentage DECIMAL,
  min_percentage DECIMAL,
  notes TEXT,
  PRIMARY KEY (appellation_id, grape_id)
);
```

**10. varietal_category_grapes**
```sql
CREATE TABLE varietal_category_grapes (
  varietal_category_id UUID NOT NULL REFERENCES varietal_categories(id),
  grape_id UUID NOT NULL REFERENCES grapes(id),
  is_required BOOLEAN DEFAULT false,
  typical_min_pct DECIMAL,
  typical_max_pct DECIMAL,
  PRIMARY KEY (varietal_category_id, grape_id)
);
```

**11. producer_farming_certifications**
```sql
CREATE TABLE producer_farming_certifications (
  producer_id UUID NOT NULL REFERENCES producers(id),
  farming_certification_id UUID NOT NULL REFERENCES farming_certifications(id),
  certified_since INTEGER,
  certified_until INTEGER,
  certifying_body TEXT,
  source_id UUID REFERENCES source_types(id),
  PRIMARY KEY (producer_id, farming_certification_id)
);
```

**12. producer_biodiversity_certifications**
```sql
CREATE TABLE producer_biodiversity_certifications (
  producer_id UUID NOT NULL REFERENCES producers(id),
  biodiversity_certification_id UUID NOT NULL REFERENCES biodiversity_certifications(id),
  certified_since INTEGER,
  certified_until INTEGER,
  certifying_body TEXT,
  source_id UUID REFERENCES source_types(id),
  PRIMARY KEY (producer_id, biodiversity_certification_id)
);
```

**13. vineyards**
```sql
CREATE TABLE vineyards (
  id UUID PK DEFAULT gen_random_uuid(),
  slug TEXT UNIQUE NOT NULL,
  name TEXT NOT NULL,
  appellation_id UUID REFERENCES appellations(id),
  latitude DECIMAL,
  longitude DECIMAL,
  elevation_m INTEGER,
  aspect TEXT,
  slope TEXT,
  area_ha DECIMAL,
  vine_density_per_ha INTEGER,
  established_year INTEGER,
  metadata JSONB,
  created_at TIMESTAMPTZ DEFAULT now(),
  updated_at TIMESTAMPTZ DEFAULT now(),
  deleted_at TIMESTAMPTZ
);
```

**14. vineyard_producers**
```sql
CREATE TABLE vineyard_producers (
  vineyard_id UUID NOT NULL REFERENCES vineyards(id),
  producer_id UUID NOT NULL REFERENCES producers(id),
  area_ha DECIMAL,
  planted_year INTEGER,
  PRIMARY KEY (vineyard_id, producer_id)
);
```

**15. vineyard_soils**
```sql
CREATE TABLE vineyard_soils (
  vineyard_id UUID NOT NULL REFERENCES vineyards(id),
  soil_type_id UUID NOT NULL REFERENCES soil_types(id),
  PRIMARY KEY (vineyard_id, soil_type_id)
);
```

### Tier 2: Soon After

**16. wine_vintage_tasting_insights**
```sql
CREATE TABLE wine_vintage_tasting_insights (
  id UUID PK DEFAULT gen_random_uuid(),
  wine_id UUID NOT NULL REFERENCES wines(id),
  vintage_year INTEGER,
  -- WSET SAT scales (moved from wine_vintages, 1-5 scale)
  sensory_acidity INTEGER,
  sensory_tannin INTEGER,
  sensory_body INTEGER,
  sensory_alcohol INTEGER,
  sensory_sweetness INTEGER,
  -- Expanded SAT dimensions
  color_intensity TEXT,              -- pale/medium/deep
  color_hue TEXT,                    -- lemon/gold/amber (whites) | purple/ruby/garnet/tawny (reds)
  aroma_intensity TEXT,              -- light/medium/pronounced
  aroma_development TEXT,            -- youthful/developing/fully_developed/tired
  finish_length TEXT,                -- short/medium/long
  complexity TEXT,                   -- simple/moderate/complex
  quality_level TEXT,                -- faulty/poor/acceptable/good/very_good/outstanding
  -- Standard insight fields
  confidence DECIMAL,
  enriched_at TIMESTAMPTZ,
  refresh_after TIMESTAMPTZ,
  created_at TIMESTAMPTZ DEFAULT now(),
  updated_at TIMESTAMPTZ DEFAULT now(),
  UNIQUE(wine_id, vintage_year)
);
```

**17. wine_vintage_components** (NV composition)
```sql
CREATE TABLE wine_vintage_components (
  id UUID PK DEFAULT gen_random_uuid(),
  wine_id UUID NOT NULL REFERENCES wines(id),
  nv_vintage_year INTEGER,
  component_vintage_year INTEGER NOT NULL,
  percentage DECIMAL,
  component_wine_id UUID REFERENCES wines(id),
  notes TEXT,
  source_id UUID REFERENCES source_types(id)
);
```

**18. tasting_descriptors** (reference table)
```sql
CREATE TABLE tasting_descriptors (
  id UUID PK DEFAULT gen_random_uuid(),
  slug TEXT UNIQUE NOT NULL,
  name TEXT NOT NULL,
  category TEXT NOT NULL,            -- fruit/floral/spice/earth/oak/other
  parent_descriptor_id UUID REFERENCES tasting_descriptors(id),
  created_at TIMESTAMPTZ DEFAULT now()
);
```

**19. wine_vintage_descriptors**
```sql
CREATE TABLE wine_vintage_descriptors (
  wine_id UUID NOT NULL REFERENCES wines(id),
  vintage_year INTEGER,
  descriptor_id UUID NOT NULL REFERENCES tasting_descriptors(id),
  frequency INTEGER DEFAULT 1,      -- how many critics mention it
  source_id UUID REFERENCES source_types(id),
  PRIMARY KEY (wine_id, vintage_year, descriptor_id)
);
```

**20. importers**
```sql
CREATE TABLE importers (
  id UUID PK DEFAULT gen_random_uuid(),
  slug TEXT UNIQUE NOT NULL,
  name TEXT NOT NULL,
  country_id UUID NOT NULL REFERENCES countries(id),
  city TEXT,
  state TEXT,
  website_url TEXT,
  portfolio_focus TEXT,              -- french/italian/natural/fine_wine/broad
  ttb_permit_id TEXT,
  metadata JSONB,
  created_at TIMESTAMPTZ DEFAULT now(),
  updated_at TIMESTAMPTZ DEFAULT now(),
  deleted_at TIMESTAMPTZ
);
```

**21. producer_importers**
```sql
CREATE TABLE producer_importers (
  producer_id UUID NOT NULL REFERENCES producers(id),
  importer_id UUID NOT NULL REFERENCES importers(id),
  is_current BOOLEAN DEFAULT true,
  start_year INTEGER,
  end_year INTEGER,
  source_id UUID REFERENCES source_types(id),
  PRIMARY KEY (producer_id, importer_id)
);
```

---

## Column Changes on Existing Tables

### wines — ADD columns
```sql
ALTER TABLE wines ADD COLUMN color TEXT;                    -- red/white/rose/orange/amber
ALTER TABLE wines ADD COLUMN wine_type TEXT DEFAULT 'table'; -- table/fortified/aromatized/dessert
ALTER TABLE wines ADD COLUMN sweetness_level TEXT;          -- dry/off-dry/medium-sweet/sweet/luscious
ALTER TABLE wines ADD COLUMN sparkling_method TEXT;         -- traditional/charmat/ancestral/transfer/carbonation
ALTER TABLE wines ADD COLUMN sweet_method TEXT;             -- botrytis/late_harvest/ice_wine/passito/fortified/vin_de_paille
ALTER TABLE wines ADD COLUMN parent_wine_id UUID REFERENCES wines(id);  -- second wine hierarchy
ALTER TABLE wines ADD COLUMN wine_tier TEXT;                -- grand_vin/second/third
ALTER TABLE wines ADD COLUMN vineyard_id UUID REFERENCES vineyards(id);
ALTER TABLE wines ADD COLUMN lwin TEXT UNIQUE;              -- LWIN-7 code (wine identity, from free Liv-ex DB)
ALTER TABLE wines ADD COLUMN label_image_url TEXT;          -- wine-level default label image
-- Keep lat/lng/elevation/aspect/slope as fallback when vineyard_id is NULL
```

### wine_vintages — ADD columns
```sql
ALTER TABLE wine_vintages ADD COLUMN bottle_format_ml INTEGER DEFAULT 750;
ALTER TABLE wine_vintages ADD COLUMN closure TEXT;          -- cork/screwcap/diam/wax/other (moved from wines)
ALTER TABLE wine_vintages ADD COLUMN fermentation_vessel TEXT;  -- override from wine-level default
ALTER TABLE wine_vintages ADD COLUMN oak_origin TEXT;       -- override
ALTER TABLE wine_vintages ADD COLUMN yeast_type TEXT;       -- override
ALTER TABLE wine_vintages ADD COLUMN fining TEXT;           -- override
ALTER TABLE wine_vintages ADD COLUMN filtration BOOLEAN;    -- override
ALTER TABLE wine_vintages ADD COLUMN pradikat TEXT;         -- kabinett/spatlese/auslese/ba/tba/eiswein
ALTER TABLE wine_vintages ADD COLUMN maceration_days INTEGER;
ALTER TABLE wine_vintages ADD COLUMN lees_aging_months INTEGER;
ALTER TABLE wine_vintages ADD COLUMN batonnage BOOLEAN;
ALTER TABLE wine_vintages ADD COLUMN skin_contact_days INTEGER;  -- for orange/amber wines
ALTER TABLE wine_vintages ADD COLUMN aging_vessel TEXT;     -- barrel/stainless/concrete/amphora/foudre/mixed
ALTER TABLE wine_vintages ADD COLUMN yield_hl_ha DECIMAL;
ALTER TABLE wine_vintages ADD COLUMN availability_status TEXT;  -- current_release/sold_out/futures/library/discontinued
ALTER TABLE wine_vintages ADD COLUMN label_image_url TEXT;  -- vintage-specific label image
ALTER TABLE wine_vintages ADD COLUMN lwin TEXT UNIQUE;      -- LWIN-11 code (wine+vintage, from free Liv-ex DB)
```

### wine_vintage_grapes — ADD column
```sql
ALTER TABLE wine_vintage_grapes ADD COLUMN wine_vintage_id UUID REFERENCES wine_vintages(id);
-- Backfill: UPDATE wine_vintage_grapes wvg SET wine_vintage_id = wv.id
--   FROM wine_vintages wv WHERE wvg.wine_id = wv.wine_id
--   AND (wvg.vintage_year = wv.vintage_year OR (wvg.vintage_year IS NULL AND wv.vintage_year IS NULL));
```

### wine_vintage_insights — ADD column
```sql
ALTER TABLE wine_vintage_insights ADD COLUMN ai_current_drinking_status TEXT;  -- drink_now/hold/approaching_peak/at_peak/past_peak/declining
```

### producers — ADD columns
```sql
ALTER TABLE producers ADD COLUMN producer_type TEXT;        -- estate/negociant/cooperative/custom_crush/virtual/corporate
ALTER TABLE producers ADD COLUMN parent_company TEXT;
ALTER TABLE producers ADD COLUMN hectares_under_vine DECIMAL;
ALTER TABLE producers ADD COLUMN total_production_cases INTEGER;
```

### grapes — ADD columns
```sql
ALTER TABLE grapes ADD COLUMN parent1_grape_id UUID REFERENCES grapes(id);
ALTER TABLE grapes ADD COLUMN parent2_grape_id UUID REFERENCES grapes(id);
ALTER TABLE grapes ADD COLUMN parentage_confirmed BOOLEAN;
ALTER TABLE grapes ADD COLUMN species TEXT;                 -- vinifera/labrusca/riparia/hybrid/complex_hybrid
ALTER TABLE grapes ADD COLUMN grape_type TEXT;              -- wine/table/rootstock/drying/juice/dual
ALTER TABLE grapes ADD COLUMN berry_skin_color TEXT;        -- blanc/noir/rose/gris/rouge
ALTER TABLE grapes ADD COLUMN origin_region TEXT;           -- more specific than country
ALTER TABLE grapes ADD COLUMN ttb_name TEXT;                -- official US label name
ALTER TABLE grapes ADD COLUMN oiv_number TEXT;
ALTER TABLE grapes ADD COLUMN wikidata_id TEXT;
```

### soil_types — ADD columns (fix schema-vs-reality discrepancy)
```sql
ALTER TABLE soil_types ADD COLUMN drainage_rate DECIMAL;    -- 0-1 scale
ALTER TABLE soil_types ADD COLUMN heat_retention DECIMAL;   -- 0-1 scale
ALTER TABLE soil_types ADD COLUMN water_holding_capacity DECIMAL;  -- 0-1 scale
ALTER TABLE soil_types ADD COLUMN geological_origin TEXT;   -- igneous/sedimentary/metamorphic/alluvial
ALTER TABLE soil_types ADD COLUMN parent_soil_type_id UUID REFERENCES soil_types(id);
```

### appellations — ADD columns
```sql
ALTER TABLE appellations ADD COLUMN area_ha DECIMAL;
ALTER TABLE appellations ADD COLUMN elevation_min_m INTEGER;
ALTER TABLE appellations ADD COLUMN elevation_max_m INTEGER;
```

### appellation_vintages — ADD columns
```sql
ALTER TABLE appellation_vintages ADD COLUMN vintage_rating TEXT;  -- poor/below_average/average/good/very_good/excellent/exceptional
ALTER TABLE appellation_vintages ADD COLUMN vintage_rating_source UUID REFERENCES source_types(id);
ALTER TABLE appellation_vintages ADD COLUMN vintage_summary TEXT;
```

### publications — ADD columns
```sql
ALTER TABLE publications ADD COLUMN score_scale_min DECIMAL;  -- e.g., 50 for WA, 0 for JR
ALTER TABLE publications ADD COLUMN score_scale_max DECIMAL;  -- e.g., 100 for WA, 20 for JR
ALTER TABLE publications ADD COLUMN scoring_system TEXT;      -- 100-point/20-point/5-star/letter/descriptive
ALTER TABLE publications ADD COLUMN active BOOLEAN DEFAULT true;
```

---

## Columns to Deprecate (don't drop yet)

```
wine_vintages.acidity          → moves to wine_vintage_tasting_insights.sensory_acidity
wine_vintages.tannin           → moves to wine_vintage_tasting_insights.sensory_tannin
wine_vintages.body             → moves to wine_vintage_tasting_insights.sensory_body
wine_vintages.alcohol_level    → moves to wine_vintage_tasting_insights.sensory_alcohol
wine_vintages.cellartracker_id → moves to external_ids (system='cellartracker')
wine_vintages.wine_searcher_id → moves to external_ids (system='wine_searcher')
wine_vintages.vivino_id        → moves to external_ids (system='vivino')
wines.closure                  → duplicated to wine_vintages.closure (keep on wines as house default)
```

---

## Migration Order

**Phase A: Create new tables (no impact on existing data)**
1. attribute_definitions + entity_attributes (flex system)
2. external_ids
3. grape_synonyms
4. classifications + classification_levels + entity_classifications
5. wine_appellations
6. appellation_grapes + varietal_category_grapes
7. producer_farming_certifications + producer_biodiversity_certifications
8. vineyards + vineyard_producers + vineyard_soils
9. wine_vintage_tasting_insights
10. wine_vintage_components
11. tasting_descriptors + wine_vintage_descriptors
12. importers + producer_importers

**Phase B: ALTER existing tables (all nullable, no risk)**
1. wines: add color, wine_type, sweetness_level, sparkling_method, sweet_method, parent_wine_id, wine_tier, vineyard_id, lwin, label_image_url
2. wine_vintages: add bottle_format_ml, closure, winemaking overrides, pradikat, maceration/lees/aging fields, availability_status, label_image_url
3. wine_vintage_grapes: add wine_vintage_id
4. wine_vintage_insights: add ai_current_drinking_status
5. producers: add producer_type, parent_company, hectares_under_vine, total_production_cases
6. grapes: add parentage FKs, species, grape_type, berry_skin_color, origin_region, ttb_name, oiv_number, wikidata_id
7. soil_types: add physical properties + geological_origin + parent_soil_type_id
8. appellations: add area_ha, elevation_min_m, elevation_max_m
9. appellation_vintages: add vintage_rating, vintage_rating_source, vintage_summary
10. publications: add score_scale_min/max, scoring_system, active

**Phase C: Backfill and migrate**
1. Backfill wine_vintage_grapes.wine_vintage_id from (wine_id, vintage_year) match
2. Migrate external IDs from wine_vintages columns to external_ids table
3. Migrate certifications from wine.metadata to producer_farming_certifications
4. Backfill existing wines' color from varietal_category.color (then manually fix exceptions)
5. Mark deprecated columns in SCHEMA.md
6. Download LWIN database, cross-reference against existing wines

**Phase D: Update scrapers**
1. Update scraper output to write new columns (color, wine_type, sweetness, etc.)
2. Write producer-level certifications instead of wine-level
3. Write to external_ids table instead of vivino_id/cellartracker_id columns
4. Capture importer data when available from producer websites

---

## Post-Migration Documentation

- Update `docs/SCHEMA.md` with all new columns, tables, relationships
- Create `docs/METADATA_KEYS.md` documenting all known metadata JSONB keys across scrapers
- Create `docs/ATTRIBUTE_CATALOG.md` listing all attribute_definitions with categories
- Update `docs/DECISIONS.md` with all decisions made in this assessment
- Fix soil_types discrepancy in SCHEMA.md
- Document drinking window priority hierarchy in SCHEMA.md

---

## Verification Checklist

After schema changes:
- [ ] All new tables created with correct constraints
- [ ] All ALTER TABLE columns added and nullable
- [ ] wine_vintage_grapes.wine_vintage_id backfilled
- [ ] Existing scrapers still insert without errors
- [ ] Query test: insert + retrieve entity_attribute round-trip
- [ ] Query test: grape_synonyms with country context
- [ ] Query test: classification system (insert Bordeaux 1855 example)
- [ ] Run Supabase advisory checks for security/performance
- [ ] SCHEMA.md accurately reflects all changes
- [ ] DECISIONS.md updated with all decisions from this session

---

## Summary Stats (Updated)

- **Total new tables:** 21
- **Total new columns on existing tables:** ~45
- **Migration phases:** 4 (A: new tables → B: ALTER existing → C: backfill → D: update scrapers)
- **Tables with Tier 0 structural changes:** external_ids, wine_appellations, entity_attributes, attribute_definitions, grape_synonyms, classifications system (3 tables)
- **Upcoming work flagged:** Grape table population (VIVC/Wikidata), LWIN cross-reference, appellation_grapes population, importer seeding
