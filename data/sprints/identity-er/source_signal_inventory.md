# Source and signal inventory for `identity_dossier_select_v1`

This file answers two questions:

1. Which Loam sources can populate `producer_dossier_v1` today?
2. Which of those sources should actually feed shortlist generation in v1?

All counts below are from the live DB on 2026-04-21.

---

## Live coverage snapshot

| Source family | Live rows | Linked rows | Distinct linked producers | Signals that matter for producer identity | Recommended role |
| --- | ---: | ---: | ---: | --- | --- |
| Canonical producers + wines | `33,281` producers, `225,281` wines, `125,934` vintages | n/a | `33,218` producers with wines | canonical name, normalized name, country/region, wine roster, appellations, grapes, color mix | Base dossier and shortlist context |
| LWIN | `189,359` | `189,333` | `33,177` | `producer_name`, `display_name`, `wine_name`, `country`, `region`, `sub_region`, `designation`, `classification` | Primary global identity backbone and shortlist seed |
| TTB COLA | `3,283,319` | `801,258` | `9,138` | `brand_name`, `applicant_name`, `permit_no`, `wine_appellation`, `grape_varietals`, `applicant_state` | Primary US regulatory/market fingerprint and shortlist seed |
| PRO Platform | `346,080` | `79,310` | `5,580` | `brand`, `supplier_name`, `appellation`, `distributors`, `cola_number` | Strong US state-reg corroboration and shortlist seed |
| Texas TABC | `182,933` | `56,219` | `5,073` | `brand_name`, `trade_name`, `permit_license`, `ttb_number` | US state-reg corroboration and shortlist seed |
| Kansas brands | `65,476` | `12,583` | `1,719` | `brand_name`, `appellation`, `distributor1`, `distributor2`, `cola_number` | US state-reg corroboration and shortlist seed |
| Skurnik | `5,541` | `3,151` | `245` | `producer`, `country`, `region`, `appellation`, `grape`, `farming`, `notes` | Importer/portfolio support; cheap dossier enrich |
| Flatiron | `4,130` | `2,104` | `675` | `producer`, `country`, `region`, `grapes`, `vintage` | Retail support; cheap dossier enrich |
| Specs | `21,913` | `9,743` | `1,599` | `wine_origin`, `upc`, `wine_category` | Retail presence only; weak producer signal |
| Wally's | `19,446` | `12,415` | `1,611` | `vendor`, `tags` | Retail presence and distributor/merchant clues only |
| Firstleaf | `1,770` | `355` | `191` | `vendor`, `metadata` | Weak fallback support only |
| Kermit Lynch growers | `193` | `44` | `44` | `website`, `about`, `location`, `founded_year`, `winemaker`, `annual_production` | Escalation-only profile source |
| Winebow | `536` | `204` | `50` | `appellation`, `vineyard`, `soil`, `description`, `scores` | Escalation-only profile source |
| Empson | `279` | `90` | `13` | `vineyard_location`, `soil`, `altitude`, `winemaker`, `first_vintage`, `description` | Escalation-only profile source |
| European Cellars | `443` | `130` | `17` | `appellation`, `certifications`, `farming`, `soil`, `vinification`, `scores` | Escalation-only profile source |

---

## Source tiers for shortlist generation

### Tier A: shortlist seed sources

These sources should actively generate candidates in v1.

| Source | Why it belongs in shortlist generation |
| --- | --- |
| `producers.name_normalized` | Cheapest lexical seed; still needed for exact, trigram, substring, and token-based blocking |
| `source_lwin.producer_name` | Nearly full-corpus global coverage; strongest non-US identity backbone |
| `source_ttb_colas.brand_name` | Best US label-facing producer signal at scale |
| `source_ttb_colas.applicant_name` | Useful legal/permit-name counterweight to brand names |
| `source_ttb_colas.permit_no` | Strong cluster clue for some US producer cases; do not treat as an automatic merge verdict by itself |
| `source_pro_platform.brand` + `supplier_name` | State-market corroboration for brand/legal-name shapes |
| `source_tabc.brand_name` + `trade_name` | More US brand/legal-name corroboration |
| `source_kansas_brands.brand_name` | Additional US market coverage and distributor clues |
| Canonical wine/LWIN overlap | Shared wine names, shared `lwin_7`, and dominant portfolio/appellation overlap can lift or suppress candidates after lexical seeding |

### Tier B: cheap dossier enrichers, but not primary seeds

These are useful once a candidate already exists, but they should not be
responsible for generating the shortlist by themselves.

| Source | Why enrich-only in v1 |
| --- | --- |
| `source_skurnik` | Strong location/portfolio/farming clues, but too small and importer-specific to seed the corpus |
| `source_flatiron` | Helpful producer/region/grape support, but retail naming is noisier than backbone/regulatory data |
| `source_specs` | Excellent UPC and origin data, weak direct producer identity |
| `source_wallys` | Good merchant/vendor presence, weak canonical producer naming |
| `source_firstleaf` | Useful only as weak retail confirmation |

### Tier C: escalation-only sources

These are too sparse, too verbose, or too biased toward a tiny subcorpus for
default shortlist generation.

| Source | Why escalation-only |
| --- | --- |
| `source_kermit_lynch_growers` | Rich profile data, but only `44` linked producers |
| `source_winebow` | Rich terroir/profile data, but only `50` linked producers |
| `source_empson` | Excellent details, but only `13` linked producers |
| `source_european_cellars` | Excellent details, but only `17` linked producers |

---

## Field-to-source matrix

| Dossier field | Best current sources | Notes |
| --- | --- | --- |
| Canonical identity core | `producers`, `wines` | Reliable base row key; `producers.appellation_id` is unusable today (`0` populated) |
| Label-facing name forms | `producers.name`, `source_lwin.producer_name`, `source_ttb_colas.brand_name`, state-reg brand fields | Main alias/abbreviation signal |
| Legal/applicant names | `source_ttb_colas.applicant_name`, `source_pro_platform.supplier_name`, `source_tabc.trade_name` | Needed to separate legal entity names from bottle-facing names |
| Country/region/appellation fingerprint | canonical wine rollups, `source_lwin.country/region/sub_region`, `source_ttb_colas.wine_appellation`, state-reg/appellation fields, importer catalogs | Geography must be aggregated, not taken from one source row |
| Portfolio fingerprint | canonical `wines`, `external_ids` on wines, `source_lwin.display_name/wine_name`, importer catalogs, retail catalogs | Use top-k wine labels and designations, not full rosters |
| Grapes and color/style mix | `wine_grapes`, canonical wine rollups, importer catalogs, `source_ttb_colas.grape_varietals` | Grape data is partial but still useful as portfolio shape |
| US regulatory fingerprint | `source_ttb_colas.permit_no`, `applicant_name`, `applicant_state`; state registration sources | Important for custom-crush, holdco, and merchant-label adjacency |
| Distributor/importer clues | `source_pro_platform.distributors`, Kansas distributors, `source_wallys.vendor`, importer catalogs | Useful for `RELATED_BUT_DISTINCT`, weak for pure `SAME_AS` |
| Website/domain | `producers.website_url`, `source_kermit_lynch_growers.website` | Too sparse for cheap-path dependency |
| People/history | `source_kermit_lynch_growers`, `source_empson`, canonical `producers` | Escalation only |
| Terroir/profile text | `source_winebow`, `source_empson`, `source_european_cellars`, `source_skurnik` | Escalation only |

---

## Signals that are missing or too thin today

These are real gaps, but acceptable for a first proof:

1. Canonical producer aliases are absent.
   - `producer_aliases = 0`
   - Consequence: shortlist v1 must derive name-form diversity from source tables, not from a finished alias registry.
2. Canonical producer relationship tables are absent.
   - `producer_importers = 0`
   - `parent_producer_id = 0`
   - Consequence: `RELATED_BUT_DISTINCT` must be inferred from source-level clues, not a trusted canonical graph.
3. Producer external IDs are absent.
   - `external_ids` where `entity_type='producer' = 0`
   - Consequence: wine-level `lwin_7` and regulatory linkage matter more than producer-level IDs.
4. Canonical websites and history are almost empty.
   - `website_url = 14`
   - `year_established = 14`
   - Consequence: website/history cannot sit on the cheap path.
5. Producer appellation is unusable.
   - `producers.appellation_id = 0`
   - Consequence: appellation must be derived from attached wines and source footprints.

These gaps are acceptable for a bounded proof because the strongest available
signals are still present:

- near-full LWIN producer coverage
- large US regulatory footprint through TTB + state registrations
- large canonical wine portfolio to roll up into place/style fingerprints

---

## Recommended shortlist inputs for Session 10.3

The next session should treat these as the candidate-input stack, in order:

1. Canonical lexical blocking on `producers.name_normalized`
2. LWIN producer-name variants
3. TTB brand-name variants
4. State-registration brand/legal-name variants
5. Shared wine/LWIN and shared regulatory footprint as re-rankers
6. Importer/retailer signals only after a candidate already exists

That ordering keeps v1 honest:

- strong global identity first
- then strong US regulatory identity
- then smaller commercial/profile sources

---

## Bottom line

Loam already has enough data to build a serious v1 identity object, but not
enough canonical producer metadata to pretend this is a website-driven dossier
system yet.

The proof-worthy v1 should therefore be:

- backbone-first
- regulatory-aware
- portfolio-shaped
- relationship-conservative

and only escalate into website/profile/history evidence on the narrow frontier.
