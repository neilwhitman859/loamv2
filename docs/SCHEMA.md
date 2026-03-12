# Loam v2 Schema Reference

All tables in the public schema. Canonical tables hold curated, high-quality data. xwines_* tables hold bulk staging data from the X-Wines dataset (reference only).

When this file is out of date, query the DB directly: `information_schema.columns`.

---

## Conventions

**PKs:** Entity tables: `id UUID PK DEFAULT gen_random_uuid()`. Join tables: composite PK from FKs.
**Slugs:** `slug TEXT UNIQUE NOT NULL` on entity tables (wines, producers, appellations, regions, countries, grapes, varietal_categories, soil_types, water_bodies, farming_certifications, source_types, publications, biodiversity_certifications).
**Soft deletes:** `deleted_at TIMESTAMPTZ DEFAULT NULL` on core entity tables.
**Timestamps:** `created_at` and `updated_at` TIMESTAMPTZ DEFAULT now() on all entity tables.
**Source tracking:** `{field}_source UUID FK source_types` companion columns where provenance varies.
**Naming:** snake_case. FK columns: `{table_singular}_id`.

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
| metadata | jsonb | nullable | Flexible extra data from scraping |
| created_at / updated_at / deleted_at | timestamptz | standard | |

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
| closure | text | nullable | cork/screwcap/diam/wax/other |
| fermentation_vessel | text | nullable | barrel/stainless/concrete/amphora/foudre/mixed |
| oak_source | uuid | FK source_types, nullable | Covers house-style fields |
| duplicate_of | uuid | FK wines, nullable | Canonical pointer |
| metadata | jsonb | nullable | Flexible extra data from scraping |
| created_at / updated_at / deleted_at | timestamptz | standard | |

### wine_regions
PK: composite (wine_id, region_id). For multi-region wines.

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
| metadata | jsonb | nullable | Flexible extra data |
| created_at / updated_at / deleted_at | timestamptz | standard | |

---

## 5. Grapes

### grapes
| Column | Type | Constraints | Notes |
|---|---|---|---|
| id | uuid | PK | |
| slug | text | UNIQUE NOT NULL | |
| name | text | NOT NULL | Canonical name |
| aliases | text[] | nullable | Synonym names |
| color | text | nullable | red/white/pink/grey |
| origin_country_id | uuid | FK countries, nullable | |
| vivc_number | text | nullable | Vitis International Variety Catalogue |
| created_at / updated_at / deleted_at | timestamptz | standard | |

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
| created_at / updated_at | timestamptz | |

Baselines (long-term averages) stored on the appellations table, not here.

---

## 7. Soil

### soil_types
id, slug, name, description, drainage_rate (0-1), heat_retention (0-1), water_holding_capacity (0-1), timestamps, deleted_at

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
id, slug, name, country, url, type (critic_publication/community/auction_house), timestamps, deleted_at

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
id UUID PK. entity_type, entity_id, vintage_year (nullable), stage, status, started_at, completed_at, failed_at, error_message, attempts, stale_reason, timestamps. UNIQUE: (entity_type, entity_id, vintage_year, stage). No deleted_at.

---

## 18. X-Wines Staging Tables (reference only)

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

**Canonical:** 50 tables (Geography 5, Producers 3, Wines 2, Vintages 1, Grapes 4, Weather 1, Soil 4, Water 4, Certifications 4, Sources 1, Scores 2, Pricing 1, Documents 3, Insights 9, Trends 1, Search/Dedup 3, Enrichment 1, wine_regions 1)
**xwines_ staging:** 13 tables
**Total:** 63 tables
