# ground_truth_pack_001_v1 summary

Pack 001 targets the first audited expansion pass toward Sprint 7's `1,000` scoreable pair goal.
It mixes net-new scoreable pairs with explicit benchmark-truth repair / quarantine records.

## Live corpus snapshot

- producers: `33281`
- active wines: `224316`
- wine_vintages: `125934`
- producer_dedup_pairs: `600103`
- blank verdict_source rows: `600103`

## Pack inventory

- manifest requests: `72` candidate records
- built records: `36`
- net-new scoreable additions: `29`
- seed repair / reaffirm records: `6`
- quarantined disputes: `3`
- skipped candidate records: `36`

## Overall label mix

- `NONE`: `15`
- `QUARANTINED_DISPUTE`: `3`
- `RELATED_BUT_DISTINCT`: `8`
- `SAME_AS`: `10`

## Net-new scoreable additions by label

- `NONE`: `12`
- `RELATED_BUT_DISTINCT`: `8`
- `SAME_AS`: `9`

## Gap to 1,000 after Pack 001 additions

- total scoreable pairs after Pack 001 additions: `446`
- remaining gap to `1,000`: `+554`
- `SAME_AS`: `105` current after Pack 001, need `+195` to reach `300`
- `RELATED_BUT_DISTINCT`: `54` current after Pack 001, need `+146` to reach `200`
- `NONE`: `287` current after Pack 001, need `+213` to reach `500`

## Family mix

- `challenge_fils_person_alias_negative`: `1`
- `challenge_fils_person_alias_positive`: `1`
- `challenge_maison_alias_negative`: `1`
- `challenge_maison_alias_positive`: `1`
- `challenge_place_alias`: `1`
- `challenge_place_alias_negative`: `1`
- `collaboration_label`: `1`
- `foreign_domain_under_parent_house`: `2`
- `subbrand_by_parent`: `3`
- `subbrand_collection`: `2`
- `ttb_custom_crush_none`: `13`
- `ttb_variant_same_as`: `9`

## Tier mix

- `core`: `18`
- `mid`: `11`
- `tail`: `7`

## Country mix

- `US`: `26`
- `FR`: `4`
- `AT`: `2`
- `FR/US`: `2`
- `AU`: `1`
- `IT`: `1`

## Evidence mix

- `brand_literal_ttb`: `1`
- `mixed_web_trade_registry`: `3`
- `official_web`: `10`
- `ttb_none`: `13`
- `ttb_same_as`: `9`

## Quarantined disputes

- `Fery-Meunier` / `Jean Fery & Fils`
- `Protheau & Fils` / `Jean-Francois Protheau`
- `Ardhuy Cabotte` / `de la Cabotte`

## Skipped candidates (first 20)

- `Aaron` / `Aaron Wines`: `ttb_same_as_no_exact_overlap`
- `Bedrock` / `Bedrock Wine Co.`: `ttb_same_as_no_exact_overlap`
- `Boedecker` / `Boedecker Cellars`: `missing_shared_ttb_permit`
- `Cakebread` / `Cakebread Cellars`: `missing_shared_ttb_permit`
- `Carlson` / `Carlson Vineyards`: `ttb_same_as_no_exact_overlap`
- `Caymus` / `Caymus Vineyards`: `missing_shared_ttb_permit`
- `Chandon` / `Domaine Chandon`: `ttb_same_as_no_exact_overlap`
- `Duckhorn` / `Duckhorn Vineyards`: `ttb_same_as_no_exact_overlap`
- `Ecole No 41` / `L'Ecole No 41`: `missing_shared_ttb_permit`
- `Flowers` / `Flowers Vineyards`: `missing_shared_ttb_permit`
- `Hall` / `Hall Wines`: `missing_shared_ttb_permit`
- `Hartwell` / `Hartwell Vineyards`: `ttb_same_as_no_exact_overlap`
- `Justin` / `Justin Vineyard`: `missing_shared_ttb_permit`
- `King Family Vineyard` / `King Family Vineyards`: `ttb_same_as_no_exact_overlap`
- `Marimar` / `Marimar Estate`: `ttb_same_as_no_exact_overlap`
- `Ojai` / `The Ojai Vineyard`: `ttb_same_as_no_exact_overlap`
- `Peay` / `Peay Vineyards`: `ttb_same_as_no_exact_overlap`
- `Ridge` / `Ridge Vineyards`: `ttb_same_as_no_exact_overlap`
- `Rombauer` / `Rombauer Vineyards`: `missing_shared_ttb_permit`
- `Shafer` / `Shafer Vineyards`: `missing_shared_ttb_permit`

## Notes

- Pair identity is keyed on producer IDs, not raw `producer_dedup_pairs.id`, because the live table contains duplicate method-stage rows for the same producer pair.
- `ttb_same_as` records require shared official TTB permit evidence plus exact wine-name overlap and normalized near-identical brand names.
- `ttb_none` records require shared official TTB permit evidence plus zero exact wine-name overlap and distinct normalized brand names.
- `tier_tag` for new Pack 001 records uses current live wine-count exposure buckets: `core` >= 25 wines on either side, `mid` >= 8, otherwise `tail`.
- Pack 001 stays well below the aspirational `150`-record target because disputed-history cases were quarantined and several candidate pairs failed the evidence bar on re-check.
