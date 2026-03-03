# Loam v2 Schema Summary (Table-by-Table)

*Consolidated from schema-decisions.md s1-s20. Every table, column, FK.*

---

## Conventions

**PKs:** Entity tables: `id UUID PK DEFAULT gen_random_uuid()`. Join tables: composite PK from FKs. (s17)
**Slugs:** `slug TEXT UNIQUE NOT NULL` on: wines, producers, appellations, regions, countries, grapes, varietal_categories, soil_types, water_bodies, farming_certifications, source_types, publications, biodiversity_certifications. (s17)
**Soft deletes:** `deleted_at TIMESTAMPTZ DEFAULT NULL` on core tables. (s18)
**Timestamps:** `created_at` and `updated_at` TIMESTAMPTZ DEFAULT now() on all entity tables.
**Source tracking:** `{field}_source UUID FK source_types` companion columns where provenance varies. (s6)
**Naming:** snake_case. FK columns: `{table_singular}_id`.

---
## 1. Geography

### countries
| Column | Type | Constraints | Notes |
|---|---|---|---|
| id | uuid | PK | |
| slug | text | UNIQUE NOT NULL | |
| name | text | NOT NULL | |
| iso_code | text | UNIQUE, nullable | ISO 3166-1 alpha-2. GAP: not in decisions doc |
| created_at / updated_at / deleted_at | timestamptz | standard | |

### regions
Self-referencing hierarchy. (s2)
| Column | Type | Constraints | Notes |
|---|---|---|---|
| id | uuid | PK | |
| slug | text | UNIQUE NOT NULL | |
| name | text | NOT NULL | |
| country_id | uuid | FK countries, NOT NULL | |
| parent_id | uuid | FK regions, nullable | null = top-level |
| created_at / updated_at / deleted_at | timestamptz | standard | |

GAP: No lat/lng. Useful for map display?

### appellations
Legal designations. Weather attaches here. (s1, s2, s21b, s21c)
| Column | Type | Constraints | Notes |
|---|---|---|---|
| id | uuid | PK | |
| slug | text | UNIQUE NOT NULL | |
| name | text | NOT NULL | |
| designation_type | text | nullable | AVA/AOC/DOCa/DOCG/DO/IGT |
| country_id | uuid | FK countries, NOT NULL | |
| region_id | uuid | FK regions, NOT NULL | |
| latitude | decimal | nullable | Weather fetch reference |
| longitude | decimal | nullable | |
| hemisphere | text | nullable | north/south |
| growing_season_start_month | integer | nullable | 1-12 |
| growing_season_end_month | integer | nullable | 1-12 |
| min_aging_months | integer | nullable | Regulatory (s21b) |
| max_yield_hl_ha | decimal | nullable | Regulatory (s21b) |
| min_alcohol_pct | decimal | nullable | Regulatory (s21b) |
| allowed_grapes_description | text | nullable | Rule as stated (s21b) |
| classification_level | text | nullable | Grand Cru, Classico, etc. (s21b) |
| regulatory_body | text | nullable | INAO, TTB, etc. (s21b) |
| regulatory_url | text | nullable | (s21b) |
| established_year | integer | nullable | (s21b) |
| baseline_gdd | decimal | nullable | Long-term avg (s21c) |
| baseline_rainfall_mm | decimal | nullable | (s21c) |
| baseline_harvest_temp_c | decimal | nullable | (s21c) |
| created_at / updated_at / deleted_at | timestamptz | standard | |

RESOLVED: Fully spec'd in s21b. Regulatory fields and baselines added.

---
## 2. Producers

### producers (s3, s19)
| Column | Type | Constraints | Notes |
|---|---|---|---|
| id | uuid | PK | |
| slug | text | UNIQUE NOT NULL | |
| name | text | NOT NULL | |
| name_normalized | text | NOT NULL | Dedup (s19) |
| country_id | uuid | FK countries, NOT NULL | |
| region_id | uuid | FK regions, nullable | Null if multi-region |
| overview | text | nullable | |
| overview_source | uuid | FK source_types, nullable | (s6) |
| website | text | nullable | |
| established_year | integer | nullable | |
| created_at / updated_at / deleted_at | timestamptz | standard | |

### producer_regions
PK: composite (producer_id, region_id)

---
## 3. Wines

### wines (s2, s4, s5, s8, s10, s13, s19)
| Column | Type | Constraints | Notes |
|---|---|---|---|
| id | uuid | PK |  |
| slug | text | UNIQUE NOT NULL |  |
| name | text | NOT NULL | Wine name excl producer |
| name_normalized | text | NOT NULL | Dedup (s19) |
| producer_id | uuid | FK producers, NOT NULL | s3 |
| country_id | uuid | FK countries, NOT NULL | s2 |
| region_id | uuid | FK regions, nullable | Null when multi-region |
| appellation_id | uuid | FK appellations, nullable | s2 |
| varietal_category_id | uuid | FK varietal_categories, NOT NULL | AI-classified s4 |
| varietal_category_source | uuid | FK source_types, nullable | s6 |
| label_designation | text | nullable | Raw label text s4 |
| effervescence | text | nullable | still/sparkling/semi_sparkling s4 |
| is_nv | boolean | NOT NULL default false | s13 |
| latitude | decimal | nullable | Map/elevation |
| longitude | decimal | nullable |  |
| elevation_m | integer | nullable | API-derived s5 |
| aspect | text | nullable | AI-enriched s5 |
| aspect_source | uuid | FK source_types, nullable | s6 |
| slope | text | nullable | AI-enriched s5 |
| slope_source | uuid | FK source_types, nullable | s6 |
| fog_exposure | text | nullable | AI-enriched s5 |
| fog_exposure_source | uuid | FK source_types, nullable | s6 |
| vine_planted_year | integer | nullable | s8 |
| vine_age_description | text | nullable | s8 |
| vine_planted_year_source | uuid | FK source_types, nullable | s8 |
| irrigation_type | text | nullable | dry_farmed/irrigated/deficit_irrigation s8 |
| irrigation_type_source | uuid | FK source_types, nullable | s8 |
| oak_origin | text | nullable | french/american/slavonian/hungarian/mixed/none s10 |
| yeast_type | text | nullable | native/commercial/mixed s10 |
| fining | text | nullable | unfined/fined/partial s10 |
| filtration | boolean | nullable | s10 |
| closure | text | nullable | cork/screwcap/diam/wax/other s10 |
| fermentation_vessel | text | nullable | barrel/stainless/concrete/amphora/foudre/mixed s10 |
| oak_source | uuid | FK source_types, nullable | Covers house-style fields s10 |
| duplicate_of | uuid | FK wines, nullable | Canonical pointer s19 |
| created_at/updated_at/deleted_at | timestamptz | standard |  |

### wine_regions
PK: composite (wine_id, region_id)

---
## 4. Wine Vintages

### wine_vintages (s5, s8, s10, s13, s14, s19, s21a)
| Column | Type | Constraints | Notes |
|---|---|---|---|
| id | uuid | PK | s21a: UUID PK, not composite |
| wine_id | uuid | FK wines, NOT NULL | |
| vintage_year | integer | nullable | Null for NV. UNIQUE(wine_id, vintage_year) |
| acidity | integer | nullable | 1-5 WSET s10 |
| tannin | integer | nullable | 1-5 s10 |
| body | integer | nullable | 1-5 s10 |
| alcohol_level | integer | nullable | 1-5 s10 |
| alcohol_pct | decimal | nullable | Precise pct s10 |
| ph | decimal | nullable | s10 |
| ta_g_l | decimal | nullable | Titratable acidity g/L s10 |
| rs_g_l | decimal | nullable | Residual sugar g/L s10 |
| va_g_l | decimal | nullable | Volatile acidity g/L s10 |
| so2_free_mg_l | decimal | nullable | s10 |
| so2_total_mg_l | decimal | nullable | s10 |
| chemical_data_source | uuid | FK source_types, nullable | s10 |
| duration_in_oak_months | integer | nullable | s10 |
| new_oak_pct | integer | nullable | s10 |
| neutral_oak_pct | integer | nullable | s10 |
| whole_cluster_pct | integer | nullable | s10 |
| bottle_aging_months | integer | nullable | s10 |
| carbonic_maceration | boolean | nullable | s10 |
| mlf | text | nullable | full/partial/none s10 |
| winemaking_source | uuid | FK source_types, nullable | s10 |
| harvest_start_date | date | nullable | s8 |
| harvest_end_date | date | nullable | s8 |
| harvest_date_source | uuid | FK source_types, nullable | s8 |
| release_price_usd | decimal | nullable | s14 |
| release_price_original | decimal | nullable | s14 |
| release_price_currency | text | nullable | ISO code s14 |
| release_price_source | uuid | FK source_types, nullable | s14 |
| disgorgement_date | date | nullable | NV s13 |
| age_statement_years | integer | nullable | NV s13 |
| solera_system | boolean | default false | NV s13 |
| cellartracker_id | text | nullable | s19 |
| wine_searcher_id | text | nullable | s19 |
| vivino_id | text | nullable | s19 |
| created_at/updated_at/deleted_at | timestamptz | standard |  |

RESOLVED: UUID PK + UNIQUE (wine_id, vintage_year). (s21a)

---
## 5. Grapes

### grapes (s4, s12)
id, slug, name, aliases (text[]), color (red/white/pink/grey), origin_country_id FK, vivc_number, timestamps, deleted_at

### varietal_categories (s4)
id, slug, name, color (red/white/rose/orange), effervescence (still/sparkling/semi_sparkling), type (single_varietal/named_blend/generic_blend/regional_designation/proprietary), grape_id FK nullable, description, timestamps, deleted_at

### wine_grapes (s4, s5)
PK: composite (wine_id, grape_id). percentage decimal nullable, percentage_source FK.

### wine_vintage_grapes (s5, s21a)
UUID PK. UNIQUE (wine_id, vintage_year, grape_id). percentage, percentage_source.

---
## 6. Weather

### appellation_vintages (s1, s5, s21c)
PK: composite (appellation_id, vintage_year)
Weather: gdd, total_rainfall_mm, harvest_rainfall_mm, harvest_avg_temp_c, spring_frost_days, heat_spike_days, avg_diurnal_range_c
Growing season: growing_season_start (date), growing_season_end (date)
RESOLVED: Baselines moved to appellations table (s21c).

---
## 7. Soil

### soil_types (s7)
id, slug, name, description, drainage_rate (0-1), heat_retention (0-1), water_holding_capacity (0-1), timestamps, deleted_at

### wine_soils / appellation_soils / region_soils (s7)
Three-tier fallback. Each: PK composite ({entity}_id, soil_type_id), role (primary/secondary/subsoil/bedrock), source FK.

---
## 8. Water Bodies

### water_bodies (s8)
id, slug, name, type (ocean/sea/river/lake/estuary), description, timestamps, deleted_at

### wine_water_bodies / appellation_water_bodies / region_water_bodies (s8)
Three-tier fallback. Each: PK composite ({entity}_id, water_body_id), distance_km, direction, source FK.

---
## 9. Certifications

### farming_certifications (s8)
id, slug, name, description, timestamps, deleted_at

### wine_farming_certifications: PK composite (wine_id, farming_certification_id), source FK

### biodiversity_certifications (s16)
id, slug, name, description, url, timestamps, deleted_at

### wine_biodiversity_certifications: PK composite (wine_id, biodiversity_certification_id), source FK

---
## 10. Source Tracking

### source_types (s6)
id, slug, name, description, category (first_party/third_party/ai/manual), default_confidence (0-1), timestamps
Seeds: producer_stated, manual, api_derived, ai_scraped, ai_inferred, wine_database, publication, importer_stated

---
## 11. Scores

### publications (s11)
id, slug, name, country, url, type (critic_publication/community/auction_house), timestamps, deleted_at

### wine_vintage_scores (s11)
UUID PK. wine_id FK, vintage_year (nullable for NV), score, score_low, score_high, score_scale, publication_id FK, critic, tasting_note, review_text, drinking_status, blind_tasted, critic_drink_window_start/end, review_date, review_type, is_community, rating_count, is_superseded, url, source FK, discovered_at, timestamps

---
## 12. Pricing

### wine_vintage_prices (s14)
UUID PK. wine_id FK, vintage_year (nullable), price_usd, price_original, currency, price_type (retail/auction/pre_arrival), source FK, source_url, merchant_name, price_date, created_at
Release price lives on wine_vintages, not here.

---
## 13. Documents

### wine_vintage_documents (s9): UUID PK, wine_id, vintage_year, url, document_type, title, source FK, discovered_at, last_verified_at
### producer_documents (s9): UUID PK, producer_id, url, document_type, title, source FK, discovered_at, last_verified_at
### appellation_documents (s9): UUID PK, appellation_id, url, document_type, title, source FK, discovered_at, last_verified_at

---
## 14. AI Insights

All share: confidence, enriched_at, refresh_after (nullable), created_at, updated_at (s6)

### wine_vintage_insights: UUID PK, UNIQUE (wine_id, vintage_year) (s21a)
AI fields: ai_vintage_summary, ai_weather_impact, ai_microclimate_impact, ai_flavor_impact, ai_aging_potential, ai_quality_assessment, ai_comparison_to_normal, ai_harvest_analysis, ai_value_assessment
Critic window: critic_drinking_window_start/end, critic_peak_start/end, critic_window_source FK
Calculated window: calculated_drinking_window_start/end, calculated_peak_start/end, calculated_window_explanation
AI window: ai_drinking_window_start/end, ai_peak_start/end, ai_window_explanation

### wine_insights: PK wine_id
ai_wine_summary, ai_style_profile, ai_terroir_expression, ai_food_pairing, ai_cellar_recommendation, ai_comparable_wines, ai_vegetation_and_land_use, vegetation_source FK, vegetation_confidence
Typical aging (relative years): typical_drinking_window_years, typical_aging_potential_years, typical_peak_start_years, typical_peak_end_years

### appellation_insights: PK appellation_id
ai_overview, ai_climate_profile, ai_soil_profile, ai_signature_style, ai_key_grapes, ai_aging_generalization, ai_notable_producers_summary

### region_insights: PK region_id. ai_overview, ai_climate_profile, ai_sub_region_comparison, ai_signature_style, ai_history
### producer_insights: PK producer_id. ai_overview, ai_winemaking_style, ai_reputation, ai_value_assessment, ai_portfolio_summary
### grape_insights: PK grape_id. ai_overview, ai_flavor_profile, ai_growing_conditions, ai_food_pairing, ai_regions_of_note, ai_aging_characteristics
### soil_type_insights: PK soil_type_id. ai_overview, ai_wine_impact, ai_notable_regions, ai_drainage_explanation, ai_best_grapes
### water_body_insights: PK water_body_id. ai_overview, ai_wine_impact, ai_notable_regions
### country_insights (s21e): PK country_id. ai_overview, ai_wine_history, ai_key_regions, ai_signature_styles, ai_regulatory_overview

---
## 15. Trends (s21d)

### trends
Single polymorphic table. Replaces 6 entity-specific tables.
id UUID PK, entity_type (appellation/region/country/producer/grape/varietal_category), entity_id UUID, trend_type (market_trend/emerging_narrative/buyer_sentiment/price_movement), content, confidence, enriched_at, refresh_after NOT NULL, created_at

---
## 16. Search

### wine_candidates (s15)
id, producer_name, wine_name, primary_grape, vintage_years (int[]), source_url, wines_id FK nullable, created_at
RESOLVED: Handoff uses existing dedup system. No schema changes needed. (s21f)

---
## 17. Enrichment

### enrichment_log (s20)
id UUID PK. entity_type, entity_id, vintage_year (nullable), stage, status, started_at, completed_at, failed_at, error_message, attempts, stale_reason, timestamps
UNIQUE: (entity_type, entity_id, vintage_year, stage). No deleted_at.

---
## Issues — All Resolved (s21a-f)

1. ~~Nullable vintage_year in PKs~~ → UUID PK + unique constraint (s21a)
2. ~~Appellations fields~~ → Fully spec'd with regulatory fields (s21b)
3. ~~Weather field names~~ → Confirmed in summary; baselines moved to appellations (s21c)
4. ~~Baselines storage~~ → On appellations, stored once (s21c)
5. ~~Trends tables~~ → Single polymorphic table (s21d)
6. ~~wine_candidates handoff~~ → Pipeline handles it, no schema changes (s21f)
7. ~~Country insights~~ → Added (s21e)

---
## Table Count: 44

Geography 3, Producers 2, Wines 2, Vintages 1, Grapes 4, Weather 1, Soil 4, Water 4, Certifications 4, Sources 1, Scores 2, Pricing 1, Documents 3, Insights 9, Trends 1, Search 1, Enrichment 1
