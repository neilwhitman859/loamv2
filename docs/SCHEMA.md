# Loam v2 Schema Reference

All tables in the public schema. Canonical tables hold curated, high-quality data. xwines_* tables hold bulk staging data from the X-Wines dataset (reference only).

When this file is out of date, query the DB directly: `information_schema.columns`.

---

## Conventions

**PKs:** Entity tables: `id UUID PK DEFAULT gen_random_uuid()`. Join tables: composite PK from FKs.
**Slugs:** `slug TEXT UNIQUE NOT NULL` on entity tables (wines, producers, appellations, regions, countries, grapes, varietal_categories, soil_types, water_bodies, farming_certifications, source_types, publications, biodiversity_certifications).
**Soft deletes:** `deleted_at TIMESTAMPTZ DEFAULT NULL` on core entity tables.
**Timestamps:** `created_at` and `updated_at` TIMESTAMPTZ DEFAULT now() on all entity tables.
**`updated_at` triggers:** `set_updated_at()` BEFORE UPDATE trigger on all 36 tables with `updated_at`. Auto-sets timestamp on every UPDATE.
**Source tracking:** `{field}_source UUID FK source_types` companion columns where provenance varies.
**Naming:** snake_case. FK columns: `{table_singular}_id`.
**Polymorphic FK validation:** `validate_polymorphic_fks()` function checks `entity_classifications`, `entity_attributes`, `external_ids`, `enrichment_log` for orphaned rows. Run after bulk operations.

---

## 1. Geography

### countries
| Column | Type | Constraints | Notes |
|---|---|---|---|
| id | uuid | PK | |
| slug | text | UNIQUE NOT NULL | |
| name | text | NOT NULL | |
| iso_code | text | UNIQUE, nullable | ISO 3166-1 alpha-2 |
| created_at / updated_at / deleted_at | timestamptz | standard | |

### regions
Self-referencing hierarchy.
| Column | Type | Constraints | Notes |
|---|---|---|---|
| id | uuid | PK | |
| slug | text | UNIQUE NOT NULL | |
| name | text | NOT NULL | |
| country_id | uuid | FK countries, NOT NULL | |
| parent_id | uuid | FK regions, nullable | null = top-level |
| is_catch_all | boolean | NOT NULL | One catch-all per country for wines without specific region |
| created_at / updated_at / deleted_at | timestamptz | standard | |

### appellations
Legal designations. Weather attaches here.
| Column | Type | Constraints | Notes |
|---|---|---|---|
| id | uuid | PK | |
| slug | text | UNIQUE NOT NULL | |
| name | text | NOT NULL | |
| designation_type | text | nullable | AVA/AOC/DOCa/DOCG/DO/IGT/GI/WO/DAC/PDO/PGI etc. |
| country_id | uuid | FK countries, NOT NULL | |
| region_id | uuid | FK regions, NOT NULL | |
| latitude | decimal | nullable | Weather fetch reference point |
| longitude | decimal | nullable | |
| hemisphere | text | nullable | north/south |
| growing_season_start_month | integer | nullable | 1-12 |
| growing_season_end_month | integer | nullable | 1-12 |
| min_aging_months | integer | nullable | Regulatory |
| max_yield_hl_ha | decimal | nullable | Regulatory |
| min_alcohol_pct | decimal | nullable | Regulatory |
| allowed_grapes_description | text | nullable | Rule as stated |
| classification_level | text | nullable | Grand Cru, Classico, etc. |
| regulatory_body | text | nullable | INAO, TTB, etc. |
| regulatory_url | text | nullable | |
| established_year | integer | nullable | |
| baseline_gdd | decimal | nullable | Long-term avg for comparison |
| baseline_rainfall_mm | decimal | nullable | |
| baseline_harvest_temp_c | decimal | nullable | |
| area_ha | decimal | nullable | Total vineyard area in hectares |
| elevation_min_m | integer | nullable | |
| elevation_max_m | integer | nullable | |
| created_at / updated_at / deleted_at | timestamptz | standard | |

### appellation_containment
Hierarchy of appellations (e.g., Pauillac is within Haut-Médoc).
| Column | Type | Constraints | Notes |
|---|---|---|---|
| parent_id | uuid | NOT NULL, FK appellations | Containing appellation |
| child_id | uuid | NOT NULL, FK appellations | Contained appellation |
| source | text | NOT NULL, default 'explicit' | |

### geographic_boundaries
PostGIS geometry for map display and spatial queries. Links to one of country, region, or appellation.
| Column | Type | Constraints | Notes |
|---|---|---|---|
| id | uuid | PK | |
| country_id | uuid | FK countries, nullable | |
| region_id | uuid | FK regions, nullable | |
| appellation_id | uuid | FK appellations, nullable | |
| boundary | geometry | nullable | Polygon/MultiPolygon |
| centroid | geometry | NOT NULL | Point |
| bounding_box | geometry | nullable | Envelope |
| boundary_confidence | text | NOT NULL | |
| boundary_source | text | NOT NULL | |
| boundary_source_id | text | nullable | |
| boundary_updated_at | timestamptz | NOT NULL, default now() | |
| created_at / updated_at | timestamptz | standard | |

---

## 2. Producers

### producers
| Column | Type | Constraints | Notes |
|---|---|---|---|
| id | uuid | PK | |
| slug | text | UNIQUE NOT NULL | |
| name | text | NOT NULL | |
| name_normalized | text | NOT NULL | For dedup matching |
| country_id | uuid | FK countries, nullable | Nullable — review during next import |
| region_id | uuid | FK regions, nullable | Null if multi-region |
| appellation_id | uuid | FK appellations, nullable | |
| website_url | text | nullable | |
| year_established | integer | nullable | |
| producer_type | text | nullable | estate/negociant/cooperative/custom_crush/virtual/corporate |
| parent_company | text | nullable | |
| hectares_under_vine | decimal | nullable | |
| total_production_cases | integer | nullable | |
| parent_producer_id | uuid | FK producers, nullable | Self-ref for second labels/sub-brands (e.g., Sea Slopes → Fort Ross) |
| philosophy | text | nullable | Producer philosophy/approach statement |
| metadata | jsonb | nullable | Flexible extra data from scraping |
| created_at / updated_at / deleted_at | timestamptz | standard | |

### winemakers
| Column | Type | Constraints | Notes |
|---|---|---|---|
| id | uuid | PK | |
| slug | text | UNIQUE NOT NULL | |
| name | text | NOT NULL | |
| country_id | uuid | FK countries, nullable | |
| metadata | jsonb | nullable | |
| created_at / updated_at | timestamptz | standard | |

### producer_winemakers
Junction: producer ↔ winemaker with role and tenure. Winemakers often consult for multiple producers.
| Column | Type | Constraints | Notes |
|---|---|---|---|
| id | uuid | PK | |
| producer_id | uuid | FK producers, NOT NULL | ON DELETE CASCADE |
| winemaker_id | uuid | FK winemakers, NOT NULL | ON DELETE CASCADE |
| role | text | NOT NULL, CHECK | head/consulting/assistant/founding |
| start_year | smallint | nullable | |
| end_year | smallint | nullable | null = current |
| created_at | timestamptz | default now() | |
UNIQUE(producer_id, winemaker_id, role)

### producer_aliases
| Column | Type | Constraints | Notes |
|---|---|---|---|
| id | uuid | PK | |
| producer_id | uuid | FK producers, NOT NULL | |
| name | text | NOT NULL | Alternate spelling |
| name_normalized | text | NOT NULL | |
| source | text | NOT NULL | e.g. 'xwines_dedup' |
| created_at | timestamptz | default now() | |

### producer_regions
PK: composite (producer_id, region_id). For multi-region producers.

---

## 3. Wines

### wines
| Column | Type | Constraints | Notes |
|---|---|---|---|
| id | uuid | PK | |
| slug | text | UNIQUE NOT NULL | |
| name | text | NOT NULL | Wine name excluding producer |
| name_normalized | text | NOT NULL | For dedup matching |
| producer_id | uuid | FK producers, NOT NULL | |
| country_id | uuid | FK countries, NOT NULL | |
| region_id | uuid | FK regions, nullable | Null when multi-region |
| appellation_id | uuid | FK appellations, nullable | |
| varietal_category_id | uuid | FK varietal_categories, NOT NULL | |
| varietal_category_source | uuid | FK source_types, nullable | |
| label_designation | text | nullable | Raw label text |
| effervescence | text | nullable | still/sparkling/semi_sparkling |
| is_nv | boolean | NOT NULL default false | |
| vineyard_name | text | nullable | Specific vineyard if named |
| food_pairings | text | nullable | From producer/scraping |
| latitude | decimal | nullable | Map/elevation |
| longitude | decimal | nullable | |
| elevation_m | integer | nullable | API-derived |
| aspect | text | nullable | AI-enriched |
| aspect_source | uuid | FK source_types, nullable | |
| slope | text | nullable | AI-enriched |
| slope_source | uuid | FK source_types, nullable | |
| fog_exposure | text | nullable | AI-enriched |
| fog_exposure_source | uuid | FK source_types, nullable | |
| vine_planted_year | integer | nullable | |
| vine_age_description | text | nullable | |
| vine_planted_year_source | uuid | FK source_types, nullable | |
| irrigation_type | text | nullable | dry_farmed/irrigated/deficit_irrigation |
| irrigation_type_source | uuid | FK source_types, nullable | |
| oak_origin | text | nullable | french/american/slavonian/hungarian/mixed/none |
| yeast_type | text | nullable | native/commercial/mixed |
| fining | text | nullable | unfined/fined/partial |
| filtration | boolean | nullable | |
| closure | text | nullable | cork/screwcap/diam/wax/other (house default; vintage-level override on wine_vintages) |
| fermentation_vessel | text | nullable | barrel/stainless/concrete/amphora/foudre/mixed (house default) |
| oak_source | uuid | FK source_types, nullable | Covers house-style fields |
| color | text | nullable | red/white/rose/orange/amber (actual wine color, not grape color) |
| wine_type | text | default 'table' | table/fortified/aromatized/dessert |
| sweetness_level | text | nullable | dry/off-dry/medium-sweet/sweet/luscious |
| sparkling_method | text | nullable | traditional/charmat/ancestral/transfer/carbonation |
| sweet_method | text | nullable | botrytis/late_harvest/ice_wine/passito/fortified/vin_de_paille/cryoextraction |
| parent_wine_id | uuid | FK wines, nullable | Second wine hierarchy |
| wine_tier | text | nullable | grand_vin/second/third |
| vineyard_id | uuid | FK vineyards, nullable | Links to vineyards table |
| lwin | text | UNIQUE, nullable | LWIN-7 code (wine identity) |
| label_image_url | text | nullable | Wine-level default label image |
| duplicate_of | uuid | FK wines, nullable | Canonical pointer |
| first_vintage_year | integer | nullable | Year the wine was first produced |
| style | text | nullable | Wine style description (e.g., "traditional Rioja", "bold red") |
| metadata | jsonb | nullable | Flexible extra data from scraping |
| created_at / updated_at / deleted_at | timestamptz | standard | |

### wine_appellations
Secondary appellations for multi-appellation wines. Primary appellation stays on wines.appellation_id.
PK: composite (wine_id, appellation_id). is_primary boolean default false, notes text nullable.

### wine_regions
PK: composite (wine_id, region_id). For multi-region wines.

### wine_aliases
UUID PK. wine_id FK (CASCADE), name text NOT NULL, alias_type text NOT NULL CHECK ('previous_name'|'alternate_label'|'market_name'), start_year integer, end_year integer, notes text, created_at. Tracks historical and alternate names for wines (renames, market-specific labels). Added 2026-03-15.

---

## 4. Wine Vintages

### wine_vintages
UUID PK + UNIQUE(wine_id, vintage_year). Vintage_year nullable for NV wines.
| Column | Type | Constraints | Notes |
|---|---|---|---|
| id | uuid | PK | |
| wine_id | uuid | FK wines, NOT NULL | |
| vintage_year | integer | nullable | Null for NV |
| acidity | integer | nullable | 1-5 WSET scale |
| tannin | integer | nullable | 1-5 |
| body | integer | nullable | 1-5 |
| alcohol_level | integer | nullable | 1-5 |
| alcohol_pct | decimal | nullable | Precise percentage |
| abv | numeric(4,1) | nullable | Alcohol by volume percentage for the vintage |
| ph | decimal | nullable | |
| ta_g_l | decimal | nullable | Titratable acidity g/L |
| rs_g_l | decimal | nullable | Residual sugar g/L |
| va_g_l | decimal | nullable | Volatile acidity g/L |
| so2_free_mg_l | decimal | nullable | |
| so2_total_mg_l | decimal | nullable | |
| chemical_data_source | uuid | FK source_types, nullable | |
| brix_at_harvest | decimal | nullable | |
| duration_in_oak_months | integer | nullable | |
| new_oak_pct | integer | nullable | |
| neutral_oak_pct | integer | nullable | |
| whole_cluster_pct | integer | nullable | |
| bottle_aging_months | integer | nullable | |
| carbonic_maceration | boolean | nullable | |
| mlf | text | nullable | full/partial/none |
| winemaking_source | uuid | FK source_types, nullable | |
| harvest_start_date | date | nullable | |
| harvest_end_date | date | nullable | |
| harvest_date_source | uuid | FK source_types, nullable | |
| winemaker_notes | text | nullable | From producer/tech sheet |
| vintage_notes | text | nullable | Growing season narrative |
| cases_produced | integer | nullable | |
| bottling_date | date | nullable | |
| producer_drinking_window_start | integer | nullable | Year |
| producer_drinking_window_end | integer | nullable | Year |
| release_price_usd | decimal | nullable | |
| release_price_original | decimal | nullable | |
| release_price_currency | text | nullable | ISO code |
| release_price_source | uuid | FK source_types, nullable | |
| disgorgement_date | date | nullable | NV/sparkling |
| age_statement_years | integer | nullable | NV |
| solera_system | boolean | default false | NV |
| cellartracker_id | text | nullable | External ID |
| wine_searcher_id | text | nullable | |
| vivino_id | text | nullable | |
| bottle_format_ml | integer | default 750 | 187/375/500/750/1500/3000/etc. |
| closure | text | nullable | Per-vintage override of wines.closure |
| fermentation_vessel | text | nullable | Per-vintage override |
| oak_origin | text | nullable | Per-vintage override |
| yeast_type | text | nullable | Per-vintage override |
| fining | text | nullable | Per-vintage override |
| filtration | boolean | nullable | Per-vintage override |
| pradikat | text | nullable | kabinett/spatlese/auslese/ba/tba/eiswein |
| maceration_days | integer | nullable | |
| lees_aging_months | integer | nullable | |
| batonnage | boolean | nullable | |
| skin_contact_days | integer | nullable | For orange/amber wines |
| aging_vessel | text | nullable | barrel/stainless/concrete/amphora/foudre/mixed |
| yield_hl_ha | decimal | nullable | |
| availability_status | text | nullable | current_release/sold_out/futures/library/discontinued |
| release_date | date | nullable | When this vintage was released to market |
| label_image_url | text | nullable | Vintage-specific label image |
| lwin | text | UNIQUE, nullable | LWIN-11 code (wine+vintage) |
| metadata | jsonb | nullable | Flexible extra data |
| created_at / updated_at / deleted_at | timestamptz | standard | |

**Override pattern:** `closure`, `fermentation_vessel`, `oak_origin`, `yeast_type`, `fining`, `filtration` exist on both `wines` (house default) and `wine_vintages` (per-vintage override). When non-null on wine_vintages, it takes precedence.

**Deprecated columns** (still present, moving to wine_vintage_tasting_insights): `acidity`, `tannin`, `body`, `alcohol_level`. Moving to external_ids: `cellartracker_id`, `wine_searcher_id`, `vivino_id`.

---

## 5. Grapes

### grapes
9,690 rows from VIVC (Vitis International Variety Catalogue). `name` stores VIVC prime name (UPPERCASE). `display_name` stores industry-standard name via three-tier strategy: Tier 1 explicit overrides (26 major grapes, e.g., MERLOT NOIR→Merlot, COT→Malbec), Tier 2 family-preserved (Pinot/Cabernet/Sauvignon keep suffixes), Tier 3 auto title-case with color suffix stripping for single-variant grapes.

| Column | Type | Constraints | Notes |
|---|---|---|---|
| id | uuid | PK | |
| slug | text | UNIQUE NOT NULL | |
| name | text | NOT NULL | VIVC prime name (canonical, UPPERCASE) |
| display_name | text | nullable | Industry-standard name for UI display |
| color | text | nullable | red/white (derived from berry skin) |
| origin_country_id | uuid | FK countries, nullable | |
| vivc_number | text | nullable | Vitis International Variety Catalogue |
| parent1_grape_id | uuid | FK grapes, nullable | Parentage |
| parent2_grape_id | uuid | FK grapes, nullable | Parentage |
| parentage_confirmed | boolean | nullable | |
| species | text | nullable | vinifera/labrusca/riparia/hybrid/complex_hybrid |
| grape_type | text | nullable | wine/table/rootstock/drying/juice/dual |
| berry_skin_color | text | nullable | VIVC raw value (black/green/grey/red/rose) |
| origin_region | text | nullable | More specific than country |
| ttb_name | text | nullable | Official US label name |
| oiv_number | text | nullable | OIV reference |
| wikidata_id | text | nullable | |
| aroma_class | text | nullable | |
| crossing_year | integer | nullable | Year of crossing (bred varieties) |
| breeder | text | nullable | |
| breeding_institute | text | nullable | |
| origin_type | text | nullable | cross/selection/wild |
| eu_catalog_countries | text[] | nullable | Countries in EU Vine Catalogue |
| created_at / updated_at / deleted_at | timestamptz | standard | |

### grape_synonyms
Grape name variants by country and language. E.g., Shiraz = Syrah in Australia.
| Column | Type | Constraints | Notes |
|---|---|---|---|
| id | uuid | PK | |
| grape_id | uuid | FK grapes, NOT NULL | |
| synonym | text | NOT NULL | |
| language | text | nullable | ISO 639-1 |
| country_id | uuid | FK countries, nullable | |
| synonym_type | text | nullable | synonym/clone/marketing/historic/local |
| source | text | nullable | vivc/wikidata/ttb/oiv |
| is_primary_in_country | boolean | default false | |
| created_at | timestamptz | default now() | |
UNIQUE(grape_id, synonym, country_id)

### varietal_categories
| Column | Type | Constraints | Notes |
|---|---|---|---|
| id | uuid | PK | |
| slug | text | UNIQUE NOT NULL | |
| name | text | NOT NULL | |
| color | text | nullable | red/white/rose/orange |
| effervescence | text | nullable | still/sparkling/semi_sparkling |
| type | text | nullable | single_varietal/named_blend/generic_blend/regional_designation/proprietary |
| grape_id | uuid | FK grapes, nullable | For single varietals |
| description | text | nullable | |
| created_at / updated_at / deleted_at | timestamptz | standard | |

### wine_grapes
PK: composite (wine_id, grape_id). percentage decimal nullable, percentage_source FK.

### wine_vintage_grapes
UUID PK. UNIQUE (wine_id, vintage_year, grape_id). Per-vintage grape percentages.
| Column | Type | Notes |
|---|---|---|
| id | uuid | PK |
| wine_id | uuid | FK wines, NOT NULL |
| vintage_year | integer | nullable |
| grape_id | uuid | FK grapes, NOT NULL |
| percentage | decimal | nullable |
| percentage_source | uuid | FK source_types, nullable |
| wine_vintage_id | uuid | FK wine_vintages, nullable | Optional FK for referential integrity |

### country_grapes
Grape varieties associated with a country. Typical = known for / commonly planted.
PK: composite (country_id, grape_id). association_type text NOT NULL CHECK ('required','typical') default 'typical', notes text nullable.

### region_grapes
Grape varieties associated with a region. Typical = known for / commonly planted.
PK: composite (region_id, grape_id). association_type text NOT NULL CHECK ('required','typical') default 'typical', notes text nullable.

### appellation_grapes
Structured allowed varieties per appellation. Complements `appellations.allowed_grapes_description` (free text).
PK: composite (appellation_id, grape_id). association_type text NOT NULL CHECK ('required','typical') default 'typical', max_percentage decimal nullable, min_percentage decimal nullable, notes text nullable.

### varietal_category_grapes
Blend composition for varietal categories. E.g., Bordeaux Blend = Cabernet Sauvignon + Merlot + ...
PK: composite (varietal_category_id, grape_id). is_required boolean default false, typical_min_pct / typical_max_pct decimal nullable.

---

## 6. Weather

### appellation_vintages
PK: composite (appellation_id, vintage_year). Weather data per appellation per year.
| Column | Type | Notes |
|---|---|---|
| appellation_id | uuid | FK appellations |
| vintage_year | integer | |
| gdd | decimal | Growing degree days |
| total_rainfall_mm | decimal | |
| harvest_rainfall_mm | decimal | |
| harvest_avg_temp_c | decimal | |
| spring_frost_days | integer | |
| heat_spike_days | integer | |
| avg_diurnal_range_c | decimal | |
| growing_season_start | date | |
| growing_season_end | date | |
| vintage_rating | text | nullable | poor/below_average/average/good/very_good/excellent/exceptional |
| vintage_rating_source | uuid | FK source_types, nullable | |
| vintage_summary | text | nullable | |
| created_at / updated_at | timestamptz | |

Baselines (long-term averages) stored on the appellations table, not here.

---

## 7. Soil

### soil_types
id, slug, name, description, drainage_rate (decimal 0-1), heat_retention (decimal 0-1), water_holding_capacity (decimal 0-1), geological_origin (text: igneous/sedimentary/metamorphic/alluvial), parent_soil_type_id (FK soil_types, self-ref hierarchy), timestamps, deleted_at

### wine_soils / appellation_soils / region_soils
Three-tier fallback (wine → appellation → region). Each: PK composite ({entity}_id, soil_type_id).

---

## 8. Water Bodies

### water_bodies
id, slug, name, type (ocean/sea/river/lake/estuary), description, timestamps, deleted_at

### wine_water_bodies / appellation_water_bodies / region_water_bodies
Three-tier fallback. Each: PK composite ({entity}_id, water_body_id).

---

## 9. Certifications

### farming_certifications
id, slug, name, description, timestamps, deleted_at

### wine_farming_certifications
PK composite (wine_id, farming_certification_id). certified_since (integer, nullable).

### biodiversity_certifications
id, slug, name, description, url, timestamps, deleted_at

### wine_biodiversity_certifications
PK composite (wine_id, biodiversity_certification_id). certified_since (integer, nullable).

### producer_farming_certifications
Producer-level certifications (most certs apply to the farming operation, not individual wines).
PK: composite (producer_id, farming_certification_id). certified_since/until (integer), certifying_body (text), source_id FK.

### producer_biodiversity_certifications
PK: composite (producer_id, biodiversity_certification_id). certified_since/until (integer), certifying_body (text), source_id FK.

---

## 10. Source Tracking

### source_types
| Column | Type | Notes |
|---|---|---|
| id | uuid | PK |
| slug | text | UNIQUE NOT NULL |
| name | text | NOT NULL |
| description | text | nullable |
| reliability_tier | integer | nullable |
| created_at / updated_at / deleted_at | timestamptz | |

---

## 11. Scores

### publications
id, slug, name, country, url, type (critic_publication/community/auction_house), score_scale_min (decimal), score_scale_max (decimal), scoring_system (text: 100-point/20-point/5-star/letter/descriptive), active (boolean default true), timestamps, deleted_at

### wine_vintage_scores
UUID PK. wine_id FK, vintage_year (nullable for NV), score, score_low, score_high, score_scale, publication_id FK, critic, tasting_note, review_text, drinking_status, blind_tasted, critic_drink_window_start/end, review_date, review_type, is_community (default false), rating_count, is_superseded (default false), url, source_id FK, discovered_at, timestamps

---

## 12. Pricing

### wine_vintage_prices
UUID PK. wine_id FK, vintage_year (nullable), price_usd, price_original, currency, price_type (retail/auction/pre_arrival), source_id FK, source_url, merchant_name, price_date, created_at

Release price lives on wine_vintages, not here.

---

## 13. Documents

### wine_vintage_documents
UUID PK. wine_id, vintage_year, url, document_type, title, source_id FK, discovered_at, last_verified_at, timestamps

### producer_documents
UUID PK. producer_id, url, document_type, title, source_id FK, discovered_at, last_verified_at, timestamps

### appellation_documents
UUID PK. appellation_id, url, document_type, title, source_id FK, discovered_at, last_verified_at, timestamps

---

## 14. AI Insights

All insight tables share: confidence (numeric), enriched_at, refresh_after (nullable), created_at, updated_at.

### wine_vintage_insights
UUID PK. UNIQUE (wine_id, vintage_year).
AI: ai_vintage_summary, ai_weather_impact, ai_microclimate_impact, ai_flavor_impact, ai_aging_potential, ai_quality_assessment, ai_comparison_to_normal, ai_harvest_analysis, ai_value_assessment.
Critic window: critic_drinking_window_start/end, critic_peak_start/end, critic_window_source FK.
Calculated window: calculated_drinking_window_start/end, calculated_peak_start/end, calculated_window_explanation.
AI window: ai_drinking_window_start/end, ai_peak_start/end, ai_window_explanation.
ai_current_drinking_status: drink_now/hold/approaching_peak/at_peak/past_peak/declining.

### wine_vintage_tasting_insights
WSET SAT structured tasting data (AI-assessed, not measured chemistry). UNIQUE(wine_id, vintage_year).
Sensory scales (1-5): sensory_acidity, sensory_tannin, sensory_body, sensory_alcohol, sensory_sweetness.
Appearance: color_intensity (pale/medium/deep), color_hue (lemon/gold/amber | purple/ruby/garnet/tawny).
Nose: aroma_intensity (light/medium/pronounced), aroma_development (youthful/developing/fully_developed/tired).
Palate: finish_length (short/medium/long), complexity (simple/moderate/complex), quality_level (faulty/poor/acceptable/good/very_good/outstanding).
Standard insight fields: confidence, enriched_at, refresh_after, timestamps.

### wine_vintage_nv_components
Tracks which vintages compose a non-vintage wine. id UUID PK, wine_id FK, nv_vintage_year (integer), component_vintage_year (integer NOT NULL), percentage (decimal), component_wine_id (FK wines, nullable — for reserve wines from different cuvées), notes, source_id FK.

### wine_insights
PK: wine_id. ai_wine_summary, ai_style_profile, ai_terroir_expression, ai_food_pairing, ai_cellar_recommendation, ai_comparable_wines, ai_vegetation_and_land_use, vegetation_source FK, vegetation_confidence. Typical aging (relative years): typical_drinking_window_years, typical_aging_potential_years, typical_peak_start_years, typical_peak_end_years.

### appellation_insights
PK: appellation_id. ai_overview, ai_climate_profile, ai_soil_profile, ai_signature_style, ai_key_grapes, ai_aging_generalization, ai_notable_producers_summary.

### region_insights
PK: region_id. ai_overview, ai_climate_profile, ai_sub_region_comparison, ai_signature_style, ai_history.

### country_insights
PK: country_id. ai_overview, ai_wine_history, ai_key_regions, ai_signature_styles, ai_regulatory_overview.

### producer_insights
PK: producer_id. ai_overview, ai_winemaking_style, ai_reputation, ai_value_assessment, ai_portfolio_summary.

### grape_insights
PK: grape_id. ai_overview, ai_flavor_profile, ai_growing_conditions, ai_food_pairing, ai_regions_of_note, ai_aging_characteristics.

### soil_type_insights
PK: soil_type_id. ai_overview, ai_wine_impact, ai_notable_regions, ai_drainage_explanation, ai_best_grapes.

### water_body_insights
PK: water_body_id. ai_overview, ai_wine_impact, ai_notable_regions.

---

## 15. Trends

### trends
Single polymorphic table. id UUID PK, entity_type (appellation/region/country/producer/grape/varietal_category), entity_id UUID, trend_type (market_trend/emerging_narrative/buyer_sentiment/price_movement), content, confidence, enriched_at, refresh_after NOT NULL, created_at.

---

## 16. Search & Dedup (canonical)

### wine_candidates
id UUID PK, producer_name, wine_name, wine_type, grapes (text[]), primary_grape, elaborate, abv, country, region_name, vintage_years (int[]), wines_id FK nullable, source_url, created_at. Currently empty — canonical candidates table.

### producer_dedup_staging
id SERIAL PK, producer_name, country, norm, wine_count. Currently empty — canonical dedup staging.

### producer_dedup_pairs
id SERIAL PK, name_a, name_b, country, similarity, wines_a, wines_b, verdict, verdict_source. Currently empty — canonical dedup pairs.

---

## 17. Enrichment

### enrichment_log
Tracks every AI enrichment operation with full cost/model/audit trail. Rebuilt 2026-03-15.
| Column | Type | Constraints | Notes |
|---|---|---|---|
| id | uuid | PK | |
| entity_type | text | NOT NULL | wine/producer/grape/appellation/region/country/wine_vintage |
| entity_id | uuid | NOT NULL | |
| enrichment_type | text | NOT NULL | insight/tasting_descriptors/food_pairings/classification/grape_analysis |
| model | text | NOT NULL | e.g., claude-sonnet-4-20250514 |
| prompt_template | text | nullable | Name/version of prompt template used |
| input_tokens | integer | nullable | |
| output_tokens | integer | nullable | |
| cost_usd | numeric(10,6) | nullable | |
| fields_updated | text[] | nullable | Which columns/fields were set |
| previous_values | jsonb | nullable | Snapshot of old values for rollback |
| status | text | NOT NULL, default 'completed' | completed/failed/needs_review/superseded |
| error_message | text | nullable | |
| reviewed_by | text | nullable | Human reviewer if manually checked |
| reviewed_at | timestamptz | nullable | |
| source_ids | uuid[] | nullable | Sources fed to the model as context |
| attempts | integer | default 1 | |
| stale_reason | text | nullable | |
| created_at | timestamptz | NOT NULL, default now() | |

Indexes: entity (entity_type, entity_id), type (enrichment_type), model (model, created_at), status (partial: status != 'completed').

---

## 18. Vineyards

### vineyards
Named vineyards (Clos de Vougeot, Monte Bello, etc.). CHECK constraint: at least one of appellation_id, region_id, country_id must be non-null.
| Column | Type | Constraints | Notes |
|---|---|---|---|
| id | uuid | PK | |
| slug | text | UNIQUE NOT NULL | |
| name | text | NOT NULL | |
| appellation_id | uuid | FK appellations, nullable | |
| region_id | uuid | FK regions, nullable | |
| country_id | uuid | FK countries, nullable | |
| latitude / longitude | decimal | nullable | |
| elevation_m | integer | nullable | |
| aspect | text | nullable | |
| slope | text | nullable | |
| area_ha | decimal | nullable | |
| vine_density_per_ha | integer | nullable | |
| established_year | integer | nullable | |
| metadata | jsonb | nullable | |
| created_at / updated_at / deleted_at | timestamptz | standard | |

### vineyard_producers
PK: composite (vineyard_id, producer_id). area_ha decimal nullable, planted_year integer nullable.

### vineyard_soils
PK: composite (vineyard_id, soil_type_id).

### wine_vineyards
Wine-level vineyard sourcing (default/typical sources). Many-to-many.
| Column | Type | Constraints | Notes |
|---|---|---|---|
| id | uuid | PK | |
| wine_id | uuid | FK wines, NOT NULL | ON DELETE CASCADE |
| vineyard_id | uuid | FK vineyards, NOT NULL | ON DELETE CASCADE |
| notes | text | nullable | |
| created_at | timestamptz | default now() | |
UNIQUE(wine_id, vineyard_id)

### wine_vintage_vineyards
Per-vintage vineyard sourcing (when sources change year-to-year).
| Column | Type | Constraints | Notes |
|---|---|---|---|
| id | uuid | PK | |
| wine_id | uuid | FK wines, NOT NULL | ON DELETE CASCADE |
| vintage_year | smallint | NOT NULL | |
| vineyard_id | uuid | FK vineyards, NOT NULL | ON DELETE CASCADE |
| percentage | numeric(5,2) | nullable | Percentage of fruit from this vineyard |
| notes | text | nullable | |
| created_at | timestamptz | default now() | |
UNIQUE(wine_id, vintage_year, vineyard_id)

---

## 19. Bottle Formats

### bottle_formats
Reference table for standard wine bottle sizes.
| Column | Type | Constraints | Notes |
|---|---|---|---|
| id | uuid | PK | |
| name | text | UNIQUE NOT NULL | Piccolo, Half Bottle, Standard, Magnum, etc. |
| volume_ml | integer | NOT NULL | 187, 375, 750, 1500, 3000, etc. |
| sort_order | smallint | NOT NULL DEFAULT 0 | |
| created_at | timestamptz | default now() | |

Pre-seeded: Piccolo (187ml), Half Bottle (375ml), Standard (750ml), Magnum (1500ml), Double Magnum (3000ml), Jeroboam (4500ml), Imperial (6000ml), Salmanazar (9000ml), Balthazar (12000ml), Nebuchadnezzar (15000ml).

### wine_vintage_formats
Tracks which bottle formats are available for a given vintage, with optional per-format production and pricing.
| Column | Type | Constraints | Notes |
|---|---|---|---|
| id | uuid | PK | |
| wine_id | uuid | FK wines, NOT NULL | ON DELETE CASCADE |
| vintage_year | smallint | NOT NULL | |
| bottle_format_id | uuid | FK bottle_formats, NOT NULL | ON DELETE CASCADE |
| cases_produced | integer | nullable | Production in this format |
| release_price_usd | numeric(10,2) | nullable | Price for this specific format |
| created_at | timestamptz | default now() | |
UNIQUE(wine_id, vintage_year, bottle_format_id)

---

## 20. Classifications

### classifications
Classification systems (Bordeaux 1855, Burgundy Grand/Premier Cru, St-Émilion GCC, etc.).
id UUID PK, slug UNIQUE NOT NULL, name NOT NULL, system NOT NULL (bordeaux_1855/burgundy/st_emilion_gcc/cru_bourgeois/champagne_cru), country_id FK, description, last_reclassification_year, timestamps, deleted_at.

### classification_levels
Levels within a classification system. E.g., First Growth, Second Growth for Bordeaux 1855.
id UUID PK, classification_id FK NOT NULL, level_name NOT NULL, level_rank (integer, 1 = highest), description. UNIQUE(classification_id, level_name).

### entity_classifications
Links any entity (producer/wine/vineyard/appellation) to a classification level. Polymorphic.
id UUID PK, classification_level_id FK NOT NULL, entity_type NOT NULL, entity_id UUID NOT NULL, year_classified, year_declassified, notes. UNIQUE(classification_level_id, entity_type, entity_id).

---

## 21. Flex Fields

### attribute_definitions
Defines available flex attributes. Pre-existing table.
id UUID PK, slug UNIQUE NOT NULL, name NOT NULL, category NOT NULL, data_type NOT NULL, unit (nullable), description (nullable), applies_to TEXT[] NOT NULL, created_at.

### entity_attributes
Polymorphic flex field values for any entity. UNIQUE(entity_type, entity_id, vintage_year, attribute_id).
id UUID PK, attribute_id FK attribute_definitions NOT NULL, entity_type NOT NULL, entity_id UUID NOT NULL, vintage_year (nullable), value_text, value_numeric (decimal), value_boolean, value_date, source_id FK, confidence (decimal), notes, timestamps.

---

## 22. External IDs

### external_ids
Generic registry for external system IDs (LWIN, CellarTracker, Vivino, Wine-Searcher, etc.) on any entity. Polymorphic.
id UUID PK, entity_type NOT NULL, entity_id UUID NOT NULL, system NOT NULL, external_id NOT NULL, source_id FK, notes, created_at. UNIQUE(entity_type, entity_id, system).

---

## 23. Tasting Descriptors

### tasting_descriptors
Hierarchical flavor vocabulary (e.g., citrus → lemon → lemon zest). Reference table.
id UUID PK, slug UNIQUE NOT NULL, name NOT NULL, category NOT NULL (fruit/floral/spice/earth/oak/other), parent_descriptor_id FK tasting_descriptors (self-ref), created_at.

### wine_vintage_descriptors
PK: composite (wine_id, vintage_year, descriptor_id). frequency integer default 1 (how many critics mention it), source_id FK.

---

## 24. Importers

### importers
Import companies. Country-agnostic but populated US-first (TTB COLA as primary data source).
id UUID PK, slug UNIQUE NOT NULL, name NOT NULL, country_id FK NOT NULL, city, state, website_url, portfolio_focus (french/italian/natural/fine_wine/broad), ttb_permit_id, metadata JSONB, timestamps, deleted_at.

### producer_importers
PK: composite (producer_id, importer_id). is_current boolean default true, start_year, end_year, source_id FK.

---

## 25. Label Designations

### label_designations
Controlled vocabulary for wine label terms (Riserva, Kabinett, Estate Bottled, etc.). Replaces free-text `wines.label_designation`.
| Column | Type | Constraints | Notes |
|---|---|---|---|
| id | uuid | PK | |
| canonical_name | text | NOT NULL | English/standard name |
| local_name | text | nullable | Name in local language |
| slug | text | UNIQUE NOT NULL | |
| country_id | uuid | FK countries, nullable | Null = universal (e.g., sparkling sweetness terms) |
| category | text | NOT NULL | CHECK: aging_tier/pradikat_tier/production_method/estate_bottling/late_harvest/ice_wine/botrytis_sweet/vineyard_designation/vineyard_age/quality_tier/geographic_qualifier/sparkling_type/early_release |
| is_regulated | boolean | NOT NULL default true | |
| regulatory_body | text | nullable | |
| description | text | nullable | What this designation means |
| legal_requirements | text | nullable | General requirements |
| created_at / updated_at / deleted_at | timestamptz | standard | |

### label_designation_rules
Appellation-specific requirements for designations that vary by DOC/DOCG/DO (e.g., Riserva aging differs between Barolo and Chianti).
| Column | Type | Constraints | Notes |
|---|---|---|---|
| id | uuid | PK | |
| label_designation_id | uuid | FK label_designations, NOT NULL | |
| appellation_id | uuid | FK appellations, NOT NULL | |
| min_aging_months | integer | nullable | Total minimum aging |
| min_barrel_months | integer | nullable | |
| min_bottle_months | integer | nullable | |
| min_abv | decimal(4,1) | nullable | Minimum alcohol |
| max_yield_hl_ha | decimal(5,1) | nullable | |
| notes | text | nullable | Additional details (e.g., Oechsle for Prädikats) |
| created_at | timestamptz | default now() | |
UNIQUE(label_designation_id, appellation_id)

### wine_label_designations
Many-to-many join replacing free-text `wines.label_designation`.
PK: composite (wine_id, label_designation_id).

---

## 26. Appellation Rules

### appellation_rules
Flexible JSONB storage for appellation-level winemaking/production rules. Covers ABV minimums, yield limits, oak aging, bottle aging, allowed methods — varies wildly across regulatory frameworks.
| Column | Type | Constraints | Notes |
|---|---|---|---|
| id | uuid | PK | |
| appellation_id | uuid | FK appellations, NOT NULL, UNIQUE | One row per appellation |
| rules | jsonb | NOT NULL, default '{}' | e.g., min_abv_pct, max_yield_hl_ha, min_oak_months, min_bottle_months |
| source | text | nullable | Regulatory document reference |
| notes | text | nullable | |
| created_at / updated_at | timestamptz | standard | |

---

## 27. X-Wines Staging Tables (reference only)

Bulk data from the X-Wines dataset (CC0 public domain). Kept for reference but not actively maintained. Data quality is lower than canonical tables.

| Table | Structure | Notes |
|---|---|---|
| xwines_producers | Same as producers | ~32K bulk-imported producers |
| xwines_producer_aliases | Same as producer_aliases | 266 alias records from dedup |
| xwines_wines | Similar to wines (fewer columns) | ~530K wines |
| xwines_wine_vintages | Similar to wine_vintages (fewer columns) | ~2.2M vintages |
| xwines_wine_grapes | Same as wine_grapes | ~314K grape links |
| xwines_wine_candidates | Same as wine_candidates | 100,646 original imports |
| xwines_wine_vintage_scores | Same as wine_vintage_scores | ~306K scores |
| xwines_wine_vintage_prices | Same as wine_vintage_prices | ~50K prices |
| xwines_producer_dedup_staging | Same as producer_dedup_staging | 30,684 dedup candidates |
| xwines_producer_dedup_pairs | Same as producer_dedup_pairs | 8,208 fuzzy match verdicts |
| xwines_wine_insights | Same as wine_insights | Empty |
| xwines_producer_insights | Same as producer_insights | Empty |
| xwines_region_name_mappings | region_name, country, region_id, appellation_id, match_type | 183 mappings from bulk import pipeline |

---

## Table Count

**Canonical:** 75 tables (Geography 5, Producers 5, Wines 4, Vintages 1, Grapes 6, Weather 1, Soil 4, Water 4, Certifications 6, Sources 1, Scores 2, Pricing 1, Documents 3, Insights 11, Trends 1, Search/Dedup 3, Enrichment 1, Vineyards 3, Classifications 3, Flex Fields 2, External IDs 1, Tasting Descriptors 2, Importers 2, Label Designations 3, Appellation Rules 1)
**xwines_ staging:** 13 tables
**Total:** 88 tables
