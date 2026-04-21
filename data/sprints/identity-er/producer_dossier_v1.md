# `producer_dossier_v1`

## Purpose

`producer_dossier_v1` is the minimum reusable evidence object for
`identity_dossier_select_v1`.

It is designed to answer one question well:

> Does this producer row represent the same label-facing producer identity as a
> shortlisted candidate, a related-but-distinct entity, or neither?

The dossier is not a producer biography. It is a compact identity fingerprint.

---

## V1 doctrine

1. Prefer structured fingerprints over prose.
2. Optimize for `SAME_AS` vs `RELATED_BUT_DISTINCT`, not for perfect metadata.
3. Keep the cheap dossier buildable from data Loam already has at scale.
4. Reserve sparse and verbose evidence for escalation only.
5. Show conflicts explicitly instead of hiding them inside a score.

---

## Constraints from the live DB snapshot (2026-04-21)

The current database strongly constrains what belongs in the cheap dossier:

- `producers`: `33,281`
- `producers.country_id` populated: `33,274`
- `producers.region_id` populated: `29,703`
- `producers.appellation_id` populated: `0`
- `producers.website_url` populated: `14`
- `producers.year_established` populated: `14`
- `producers.parent_company` populated: `0`
- `producers.parent_producer_id` populated: `0`
- `producer_aliases`: `0`
- `producer_importers`: `0`
- `producer_winemakers`: `0`
- `producer_timeline`: `0`
- `external_ids` where `entity_type='producer'`: `0`

Implication: v1 must lean on names, place, portfolio, and regulatory/market
signals. It must not depend on canonical producer website/history/parent fields
or on alias/importer tables that do not yet exist in practice.

---

## Dossier tiers

### Cheap dossier

Built from existing structured Loam data only. Safe to compute for a large
share of the corpus. This is the default object the shortlist selector sees.

### Escalation dossier

Adds sparse website/profile/history clues and small raw evidence excerpts for
the narrow `UNSURE` frontier. This is opt-in and must stay much smaller than
the cheap path.

---

## Cheap dossier blocks

| Block | Required fields | Why it exists | Primary population today |
| --- | --- | --- | --- |
| `identity_core` | `producer_id`, `canonical_name`, `name_normalized`, `slug`, canonical `country`, canonical `region` | Stable row identity and first-pass country/region anchoring | `producers` |
| `name_fingerprint` | `label_like_names[]`, `legal_or_applicant_names[]`, `short_forms[]` | Most producer errors are name-shape errors: abbreviations, accents, legal names, importer/merchant prefixes, generational forms | `producers`, `source_lwin.producer_name`, `source_ttb_colas.brand_name`, `source_ttb_colas.applicant_name`, `source_pro_platform.brand`, `source_pro_platform.supplier_name`, `source_tabc.brand_name`, `source_tabc.trade_name`, `source_kansas_brands.brand_name`, importer catalogs, retailer vendor fields |
| `source_presence` | canonical `wine_count`, canonical `vintage_count`, `source_families[]` with linked row counts | Distinguishes backbone-only rows from US-market-visible rows and shows where the identity is grounded | `wines`, `wine_vintages`, linked `source_*` tables |
| `place_fingerprint` | `countries[]`, `regions[]`, `appellations[]`, `place_conflicts[]` | Related-but-distinct cases often separate on geography even when names are close | `producers`, `wines`, `source_lwin`, `source_ttb_colas.wine_appellation`, `source_pro_platform.appellation`, `source_kansas_brands.appellation`, importer catalogs |
| `portfolio_fingerprint` | `top_wine_labels[]`, `top_lwin_display_names[]`, `top_grapes[]`, `top_designations[]`, `color_mix[]` | Portfolio shape is a better identity clue than raw string similarity alone | `wines`, `wine_grapes`, `external_ids` on wines, `source_lwin.display_name`, importer/retailer catalogs |
| `regulatory_market_fingerprint` | `ttb_permits[]`, `ttb_applicant_states[]`, `ttb_applicant_names[]`, `supplier_or_distributor_names[]` | Needed for US-market, custom-crush, holdco, importer-prefix, and merchant-label cases | `source_ttb_colas`, `source_pro_platform`, `source_tabc`, `source_kansas_brands`, `source_wallys.vendor` |
| `conflicts` | `name_conflicts[]`, `place_conflicts[]`, `market_conflicts[]`, `sparse_signal_flags[]` | The selector should see what is contradictory or thin, not just what matches | Derived from all cheap-dossier blocks |
| `escalation_available` | `website_domains[]`, `profile_source_families[]`, `people_history_signals[]` | Lets the system decide whether escalation is likely to add value before spending on it | `producers`, `source_kermit_lynch_growers`, `source_empson`, `source_winebow`, `source_european_cellars`, `source_skurnik` |

---

## Escalation-only blocks

These fields are useful, but too sparse or too verbose for the cheap path.

| Block | Escalation fields | Why escalation only | Current source families |
| --- | --- | --- | --- |
| `web_identity` | full `website_urls[]`, normalized domains, source URLs | Canonical website coverage is almost zero today, so this cannot be a cheap-path dependency | `producers.website_url`, `source_kermit_lynch_growers.website` |
| `profile_snippets` | short `about_excerpt`, `description_excerpt`, `viticulture_excerpt` | Valuable for frontier cases, but too verbose for the default packet | `source_kermit_lynch_growers.about`, `source_skurnik.description/notes`, `source_winebow.description/vineyard_description`, `source_empson.description`, `source_european_cellars.vinification` |
| `people_history` | `founded_years[]`, `winemaker_names[]`, `annual_production[]`, `first_vintage[]` | Sparse, source-specific, and best used when the cheap dossier cannot break a tie | `source_kermit_lynch_growers`, `source_empson`, canonical `producers` |
| `vineyard_profile` | `vineyard_locations[]`, `soil_terms[]`, `altitude_terms[]`, `farming_terms[]` | More about profile coherence than core identity; useful after shortlist narrowing | `source_empson`, `source_winebow`, `source_european_cellars`, `source_kermit_lynch`, `source_skurnik` |
| `raw_supporting_rows` | capped source-row excerpts with source identifiers | Needed for auditability on hard cases only | Any source family used in escalation |

---

## Recommended JSON shape

```json
{
  "dossier_version": "producer_dossier_v1",
  "producer_id": "uuid",
  "identity_core": {
    "canonical_name": "string",
    "name_normalized": "string",
    "slug": "string",
    "country": "string|null",
    "region": "string|null"
  },
  "name_fingerprint": {
    "label_like_names": [
      {
        "value": "string",
        "normalized": "string",
        "source_family": "canonical|lwin|ttb|state_reg|importer|retailer",
        "row_count": 0
      }
    ],
    "legal_or_applicant_names": [
      {
        "value": "string",
        "source_family": "ttb|state_reg",
        "row_count": 0
      }
    ],
    "short_forms": ["string"]
  },
  "source_presence": {
    "wine_count": 0,
    "vintage_count": 0,
    "source_families": [
      {
        "source_family": "lwin|ttb|state_reg|importer|retailer",
        "linked_rows": 0
      }
    ]
  },
  "place_fingerprint": {
    "countries": [{"value": "string", "count": 0}],
    "regions": [{"value": "string", "count": 0}],
    "appellations": [{"value": "string", "count": 0}],
    "place_conflicts": ["string"]
  },
  "portfolio_fingerprint": {
    "top_wine_labels": [{"value": "string", "count": 0}],
    "top_lwin_display_names": [{"value": "string", "count": 0}],
    "top_grapes": [{"value": "string", "count": 0}],
    "top_designations": [{"value": "string", "count": 0}],
    "color_mix": [{"value": "string", "count": 0}]
  },
  "regulatory_market_fingerprint": {
    "ttb_permits": [{"value": "string", "count": 0}],
    "ttb_applicant_states": [{"value": "string", "count": 0}],
    "ttb_applicant_names": [{"value": "string", "count": 0}],
    "supplier_or_distributor_names": [{"value": "string", "count": 0}]
  },
  "conflicts": {
    "name_conflicts": ["string"],
    "place_conflicts": ["string"],
    "market_conflicts": ["string"],
    "sparse_signal_flags": ["string"]
  },
  "escalation_available": {
    "website_domains": ["string"],
    "profile_source_families": ["string"],
    "people_history_signals": ["founded_year", "winemaker", "annual_production"]
  }
}
```

---

## Build rules

1. Cap arrays aggressively.
   - `label_like_names`: max `12`
   - `legal_or_applicant_names`: max `6`
   - `countries`, `regions`, `appellations`, `top_wine_labels`, `top_lwin_display_names`: max `8`
   - `ttb_permits`, `supplier_or_distributor_names`: max `10`
2. Deduplicate by normalized value, but preserve the best label-facing form.
3. Prefer counts and top-k rollups over raw row dumps.
4. Keep brand-facing names separate from legal/applicant names.
5. Always surface conflicts and sparsity even when they make the packet look
   worse.
6. The cheap dossier should contain no long prose fields.

---

## Explicit exclusions from v1

Do not put these in the cheap dossier:

- full wine rosters
- raw source-table dumps
- free-form producer biographies
- full TTB application records
- parent/child assertions inferred from one weak source
- website scraping as a prerequisite

Those are either escalation-only or out of scope for `v1`.

---

## What this enables next

With `producer_dossier_v1` fixed, the next session can design shortlist
generation around a stable object:

- which source families seed candidate retrieval
- which signals are cheap enough to use at scale
- how small the shortlist must stay before selector and escalation work begins
