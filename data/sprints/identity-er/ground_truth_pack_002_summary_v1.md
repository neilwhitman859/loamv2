# ground_truth_pack_002_v1 summary

Pack 002 extends the audited Sprint 7 truth base with cross-country SAME_AS continuity,
non-FR RELATED_BUT_DISTINCT families, and non-FR shared-permit NONE traps.

## Live corpus snapshot

- producers: `33281`
- active wines: `224316`
- wine_vintages: `125934`
- producer_dedup_pairs: `600103`
- blank verdict_source rows: `600103`

## Baseline before Pack 002

- scoreable pairs after ground_truth_pack_001_v1: `446`
- `SAME_AS` baseline after ground_truth_pack_001_v1: `105`
- `RELATED_BUT_DISTINCT` baseline after ground_truth_pack_001_v1: `54`
- `NONE` baseline after ground_truth_pack_001_v1: `287`

## Pack inventory

- manifest requests: `21` candidate records
- built records: `21`
- net-new scoreable additions: `21`
- prior-truth overlaps carried forward: `0`
- skipped candidate records: `0`

## Overall label mix

- `NONE`: `5`
- `RELATED_BUT_DISTINCT`: `5`
- `SAME_AS`: `11`

## Net-new scoreable additions by label

- `NONE`: `5`
- `RELATED_BUT_DISTINCT`: `5`
- `SAME_AS`: `11`

## Gap to 1,000 after Pack 002 additions

- total scoreable pairs after Pack 002 additions: `467`
- remaining gap to `1,000`: `+533`
- `SAME_AS`: `116` current after Pack 002, need `+184` to reach `300`
- `RELATED_BUT_DISTINCT`: `59` current after Pack 002, need `+141` to reach `200`
- `NONE`: `292` current after Pack 002, need `+208` to reach `500`

## Family mix

- `cross_border_estate_under_parent_house`: `1`
- `cross_house_collaboration_label`: `2`
- `estate_under_parent_house`: `1`
- `global_brand_cross_country`: `11`
- `joint_venture_label`: `1`
- `ttb_shared_permit_none`: `5`

## Tier mix

- `core`: `20`
- `mid`: `1`

## Country mix

- `US`: `5`
- `IT/US`: `2`
- `AT/SI`: `1`
- `AT/RO`: `1`
- `AU/NULL`: `1`
- `GB`: `1`
- `AR/US`: `1`
- `AU/CN`: `1`
- `AU/FR`: `1`
- `AU/US`: `1`
- `CL/US`: `1`
- `FR/ES`: `1`

## Evidence mix

- `official_web`: `5`
- `official_web_cross_country_same_brand`: `11`
- `ttb_none`: `5`

## Skipped candidates (first 20)

- none

## Notes

- Pair identity stays keyed on producer IDs, not raw `producer_dedup_pairs.id`, because the live table still contains duplicate method-stage rows per producer pair.
- Pack 002 switches the builder to explicit producer IDs because Pack 001's name-keyed lookup would collapse cross-country same-name merchant-brand cases.
- `official_web_cross_country_same_brand` cases require matching normalized producer names across distinct countries.
- `ttb_none` cases require shared official TTB permit evidence plus zero exact wine-name overlap and distinct normalized brand identities.
- Pack 002 keeps Pack 001 labels fixed; this pack only adds new audited cases that still clear the strict evidence bar.
