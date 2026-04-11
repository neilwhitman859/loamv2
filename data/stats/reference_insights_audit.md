# Reference-layer insights audit

**Generated:** 2026-04-11, Session 14 Phase B W5
**Purpose:** read-only prep for the Reference-First (Sprint 2) enrichment work. Surveys the seven `*_insights` tables that will hold the reference-layer AI content, reports what's in each today, and flags what needs to happen in Sprint 2. No writes.

---

## Summary table

| Table                  | Rows | Coverage                   | Avg length | Avg confidence | Sprint 2 verdict               |
|------------------------|-----:|---------------------------:|-----------:|---------------:|--------------------------------|
| `grape_insights`       |    0 | 0 / 9,694                  |          — |              — | Seed from scratch              |
| `producer_insights`    |    0 | 0 / 10,676                 |          — |              — | Seed from scratch              |
| `soil_type_insights`   |    0 | 0 / 39                     |          — |              — | Seed from scratch (tiny)       |
| `water_body_insights`  |    0 | 0 / 0 (water_bodies empty) |          — |              — | Parent table empty; defer      |
| `region_insights`      |  202 | 202 / 389 (51.9%)          | 457 chars  |           0.75 | Audit quality, re-enrich gaps  |
| `appellation_insights` |   82 | 82 / 3,662 (2.2%)          | 487 chars  |           0.82 | Thin coverage; big expansion   |
| `country_insights`     |   62 | 62 / 68 (91.2%)            | 454 chars  |           0.65 | Near-complete; audit + top up  |

---

## Schema

Every `*_insights` table follows the same shape:

- **PK:** `{entity}_id` (FK to the parent table). Exactly one row per parent entity.
- **Content columns:** `ai_overview` (always present), plus 3–6 topical fields per table.
- **Metadata:** `confidence` (numeric 0–1), `enriched_at`, `refresh_after`, `created_at`, `updated_at`.

Per-table topical columns:

- `grape_insights`: `ai_overview`, `ai_flavor_profile`, `ai_growing_conditions`, `ai_food_pairing`, `ai_regions_of_note`, `ai_aging_characteristics`
- `producer_insights`: `ai_overview`, `ai_winemaking_style`, `ai_reputation`, `ai_value_assessment`, `ai_portfolio_summary`
- `soil_type_insights`: `ai_overview`, `ai_wine_impact`, `ai_notable_regions`, `ai_drainage_explanation`, `ai_best_grapes`
- `water_body_insights`: `ai_overview`, `ai_wine_impact`, `ai_notable_regions`
- `region_insights`: `ai_overview`, `ai_climate_profile`, `ai_sub_region_comparison`, `ai_signature_style`, `ai_history`
- `appellation_insights`: `ai_overview`, `ai_climate_profile`, `ai_soil_profile`, `ai_signature_style`, `ai_key_grapes`, `ai_aging_generalization`, `ai_notable_producers_summary`
- `country_insights`: `ai_overview`, `ai_wine_history`, `ai_key_regions`, `ai_signature_styles`, `ai_regulatory_overview`

No `enrichment_tier` or `fact_check_status` columns on these tables yet — those were added only on `wine_insights` during Session 12's L3 work. If Sprint 2 wants fact-check gating on reference insights, it will need to add parallel columns.

---

## Content quality spot-check (existing rows)

Sampled one row from each non-empty table by most recent `enriched_at`:

**`region_insights` — Australia (confidence 0.80):**
> "Australia as a catch-all designation represents the country's everyday drinking wines, typically blended across multiple regions to achieve consistent, approachable styles at accessible prices. These wines showcase Austr…"

**`appellation_insights` — Yountville (confidence 0.90):**
> "Yountville sits in the heart of Napa Valley's floor, where the valley narrows and maritime fog meets warmer inland air. This small AVA encompasses some of Napa's most sought-after vineyard sites, with deep alluvial soils…"

**`country_insights` — Uruguay (confidence 0.70):**
> "Uruguay has quietly built one of South America's most distinctive wine identities around Tannat, a grape that found its New World home here in ways that rival its French origins. This small country wedged between Brazil…"

**Voice read:** generic but not wrong. Pre-dates the Session 12 voice/fact-check discipline, so some hedging and adjective padding likely present. Content is usable as a prior for Sprint 2 re-enrichment but should **not** be exposed to users as-is until audited.

---

## Sprint 2 implications

### Pipeline shift

Wine-level enrichment has been the target all along; Session 12 showed that strategy fails for thin Grade C wines. **Reference-First flips the target**: enrich the reference layer once, render wine pages as thin synthesis over that layer. The seven tables above are the reference layer's content surface.

### Data we already have for ground-truth grounding

- `grape_insights` → `grapes` (9,694 rows, VIVC-keyed) + `grape_synonyms` (34,820) + `grapes.parent1_grape_id` / `parent2_grape_id` parentage. Strong priors.
- `producer_insights` → `producers` (10,676) + `producer_winemakers` + `producer_farming_certifications` + `producer_biodiversity_certifications` + catalog-sourced descriptions + website URLs. Enough to ground descriptive copy.
- `soil_type_insights` → `soil_types` (39) has `drainage_rate`, `heat_retention`, `water_holding_capacity`, `geological_origin` — enough to ground factual content without any external pull.
- `region_insights` → `regions` + `region_grapes` + `appellation_vintages` weather rollups (134,877 rows, 1981–present).
- `appellation_insights` → **strongest ground-truth set:** `appellation_rules` (1,165 rows with legal provenance), `appellation_grapes` (10,414 rows), `appellation_soils` (930 links), `appellation_vintages` (weather). This is the tier that should ship first in Sprint 2.
- `country_insights` → `countries` + `country_grapes` + all the geographic containment.
- `water_body_insights` → **blocked**: `water_bodies` is 0 rows. Table schema exists but no data source has been wired. Defer.

### Recommended Sprint 2 ordering

1. **`appellation_insights` first.** Strongest ground-truth data (legal rules + grapes + weather). Most product leverage because wine pages will lean on appellation content most heavily.
2. **`grape_insights` second.** Second-strongest ground-truth, high cardinality, big product surface (every varietal page).
3. **`region_insights` audit + fill.** 202 rows already exist; audit them with the Session 12 fact-check pattern, fill the 187 gaps.
4. **`soil_type_insights` batch.** Only 39 rows total, cheap to do as a single batch.
5. **`country_insights` audit.** Mostly filled (62/68); just needs the quality audit + the 6 missing countries.
6. **`producer_insights`.** Highest cardinality (10,676), lowest priority for a first vertical slice — producer pages can render with the structured-fields-only fallback.
7. **`water_body_insights`.** Deferred — parent table empty.

### Things to build before Sprint 2 execution

- Extend the L1+L3 pipeline from `pipeline/enrich/` to read `appellation_rules` / `grape_synonyms` / `appellation_soils` as the ground-truth facts packet for each insight tier.
- Add `enrichment_tier` + `fact_check_status` columns to the reference insight tables (parallel to `wine_insights`) if Sprint 2 wants the same gate.
- Decide how `wine_detail_view` should present reference-layer content (JOIN the appellation/grape insight rows? Compose client-side? Separate RPC?).

---

**Next action:** Sprint 2 planning session (Session 15) will resolve the vertical slice choice and the producer-layer strategy, then use this audit as the starting map for the enrichment plan.
