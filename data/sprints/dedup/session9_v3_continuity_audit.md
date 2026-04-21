# Session 9 - v3 continuity audit and redesign

**Date:** 2026-04-21  
**Goal:** audit the false-merge clusters from `session7_first_real_bakeoff_v2`, identify exactly which continuity / alias heuristics are over-permissive, and lock the minimum v3 redesign needed before any further proof rerun  
**Primary deliverable:** one durable memo that separates packet-side continuity failures from adjudicator overreach and recommends the smallest v3 packet/guardrail change set  
**Non-goals:** changing `benchmark_v1`, softening the frozen Session 4 gates, running another full bakeoff, or building execution queues

---

## 1. Executive summary

The v2 contract fixes worked. The continuity layer did not.

Headline facts from the full `session7_first_real_bakeoff_v2` run:

- `sonnet_guardrailed_v2` still produced **26 false merges**
- `gemini_guardrailed_v2` produced **31 false merges**
- `sonnet_gemini_consensus_v2` reduced that to **18 false merges**, but on the same underlying continuity mistakes
- across the three contenders, there are **39 unique false-merge benchmark cases**
- **37 of those 39** false merges carried some form of `official_continuity_*` ref in the packet
- only **2 of 39** false merges happened without a continuity ref
- the dominant failure bucket is **alias cross-mention continuity**: **29 / 39** unique false-merge cases
- the second bucket is **shared-domain continuity**: **8 / 39** unique false-merge cases
- only **2 / 39** look like pure adjudicator overreach with no continuity ref present

That means the v2 false-merge problem is primarily a **packet-side retrieval/resolution problem**, not a model-family selection problem.

The decisive design bug is:

1. `choose_official_domain()` accepts `serper.organic.domain_match` as if it were authoritative official-domain resolution.
2. `build_continuity_refs()` then mints `official_continuity_alias_*` if **any token** from the other producer name appears in the title/snippet/url of one official hit.
3. `apply_merge_veto()` treats **any** `official_continuity_*` ref as sufficient to bypass the merge veto.

In practice, shared surname tokens on importer pages, merchant pages, regional directories, or umbrella portfolio sites are being upgraded into "official continuity," and the guardrail then stands down exactly where it should be strictest.

---

## 2. What was audited

Reviewed:

- `data/sprints/dedup/bakeoff_v2/packets/benchmark_v1_packets_full_v2.jsonl`
- `data/sprints/dedup/bakeoff_v2/normalized/session7_first_real_bakeoff_v2/*.jsonl`
- `data/sprints/dedup/bakeoff_v2/scored/session7_first_real_bakeoff_v2.{json,md}`
- `data/sprints/dedup/session7_bakeoff_v2_design.md`
- `pipeline/identity/bakeoff_packet_v2.py`
- `pipeline/identity/bakeoff_harness_v2.py`
- `pipeline/identity/bakeoff_run_v2.py`

Focus:

- every v2 false merge from Sonnet, Gemini, and consensus
- packet continuity refs present on those rows
- the specific official-hit domains and resolution sources those continuity refs came from
- whether the current 28-case proof subset actually covers the v2 continuity failures

---

## 3. False-merge buckets

## 3.1 Bucket A - alias cross-mention continuity is far too permissive

**Scope:** 29 / 39 unique false-merge cases  
**Shared across all 3 contenders:** 17 / 29

Representative cases:

- `blind_core_audit_041` `Dancer` vs `Theo Dancer`
- `blind_core_audit_067` `Franck Bonville` vs `Camille Bonville`
- `blind_core_audit_065` `Pierre-Yves Colin-Morey` vs `Clement Colin Morey`
- `known_false_merge_patterns_005` `Bosio` vs `Luca Bosio`
- `tail_random_sample_018` `32° South` vs `South Sea`

What happened:

- `build_continuity_refs()` creates `official_continuity_alias_*` when **any token** from the other producer name appears in the title/snippet/url of one resolved hit.
- In family-split cases, the other-name token is often just the shared surname: `Bonville`, `Guyon`, `Bornard`, `Bosio`, `Bastida`, `Jayer`, `Bouard`.
- That is not alias proof. It is often just family overlap, merchant copy, importer copy, or directory metadata.

Why the packet is at fault:

- the continuity ref is minted **before** the adjudicator reasons
- once that ref exists, the harness treats it as official continuity and the merge veto stands down
- this converts "shared surname, high-risk" into "continuity resolved" with almost no evidentiary bar

Evidence pattern:

- every continuity-support hit in this bucket came from `site_query`
- on the false merges, those "official" domains were frequently not producer-owned domains at all, e.g. `wineinvestment.com`, `thatcherswine.com`, `yelp.com`, `wein.plus`, `frederickwildman.com`, `livingwines.com.au`, `skurnik.com`, `coeurwineco.com`, `vinify.app`, `thewinecellarinsider.com`, `nzwine.com`
- even when the domain is real, the continuity inference is still too weak because the logic keys off any token match, not exact alias proof

Conclusion:

`official_continuity_alias_*` is currently acting like a **surname-cross-mention detector**, not a continuity proof.

## 3.2 Bucket B - shared-domain continuity is being over-read as same-brand identity

**Scope:** 8 / 39 unique false-merge cases  
**Shared across all 3 contenders:** 1 / 8  
**Still dangerous:** yes, because these are high-confidence false merges on umbrella/family domains

Representative cases:

- `blind_core_audit_048` `Pierre Meurgey` vs `Meurgey Croses`
- `blind_core_audit_052` `L'Ostal Cazes` vs `Cazes`
- `blind_core_audit_090` `M. Chapoutier` vs `Mathilde Chapoutier`
- `known_false_merge_patterns_001` `Cordella` vs `Fabio Cordella`

What happened:

- both sides resolved to the same root domain
- `build_continuity_refs()` emitted `official_continuity_shared_domain`
- `apply_merge_veto()` treated that as sufficient official continuity

Why that is wrong:

- one family / portfolio / umbrella domain can legitimately host multiple distinct on-label brands
- the current logic does not distinguish:
  - same domain, same brand
  - same domain, different family member line
  - same domain, different estate under one family company
  - same domain, parent brand vs child product line

The shared-domain ref is especially unsafe in `11.4.g` and `11.4.m` cases because same-domain family portfolio structure is exactly where brand-on-label identity can diverge.

Conclusion:

`official_continuity_shared_domain` should not count as hard continuity by itself. It is at best a weak continuity hint unless page-level brand identity is also aligned.

## 3.3 Bucket C - residual adjudicator overreach where packet continuity was absent

**Scope:** 2 / 39 unique false-merge cases  
**Cases:** `blind_core_audit_076` `Veramar` vs `Bogati`, `blind_core_audit_080` `Bella Union` vs `Provenance Vineyards`

What happened:

- these merges did **not** rely on an `official_continuity_*` ref
- Sonnet still merged by over-reading:
  - operator / founder / family relationship
  - acquisition / ownership / integration language
  - local catalog overlap plus same-region context

What the packet failed to surface:

- no explicit `risk_parent_portfolio_or_acquisition`
- no explicit `risk_operator_not_brand_identity`
- no explicit veto on "same owner / same operator / same estate property" as insufficient for `MERGE`

Conclusion:

There is a real but smaller adjudicator problem. It is not the main blocker, but v3 should add one narrow ownership/product-line risk backstop so Sonnet cannot convert acquisition/operator stories into identity merges.

---

## 4. Packet-side vs adjudicator-side diagnosis

## 4.1 Packet-side continuity failure dominates

The evidence is overwhelming:

- **37 / 39** unique false-merge cases carried `official_continuity_*`
- **29 / 39** were alias-cross-mention continuity
- **8 / 39** were shared-domain continuity
- on the false merges, the continuity-support hit came from `site_query` on domains chosen by `serper.organic.domain_match`
- none of the bad continuity refs needed producer-row `website_url`
- none depended on a knowledge-graph website

This means the packet is manufacturing continuity from a trust tier that is too weak.

## 4.2 Adjudicator overreach is residual, not primary

The adjudicator-only bucket is small:

- only **2 / 39** unique false-merge cases lacked continuity refs entirely
- both are Sonnet-only, not shared across the lineup

This does not mean the model is fine. It means:

- model swaps alone will not solve v2
- consensus cannot solve a poisoned packet; it only damped the count
- the next cycle should focus on continuity trust tier and guardrail semantics first

## 4.3 Consensus is not a separate failure family

`sonnet_gemini_consensus_v2` improved count-level safety, but its false merges largely match the packet-driven continuity buckets above. It is downstream of the same evidence layer, not an independent proof that the reasoning problem is solved.

---

## 5. Minimum v3 redesign

The smallest credible v3 is a **continuity trust-tier split**, not a model change.

## 5.1 Change 1 - split continuity into hard vs soft

Add two distinct continuity classes:

- `hard_official_continuity_*`
- `soft_continuity_hint_*`

Only **hard** continuity may unblock a `MERGE`.

Soft continuity can still be shown to the adjudicator, but it must **not** satisfy the guardrail.

## 5.2 Change 2 - stop treating `serper.organic.domain_match` as official-domain resolution

For v3, the minimum safe rule is:

- producer-row `website_url` root domain: can be official
- knowledge-graph website: can be official if domain looks producer-owned
- plain organic domain match: **not official**; keep as secondary only

That is the cheapest high-leverage change because it removes the main source of bogus continuity refs without redesigning the whole packet schema.

If no producer-owned official domain resolves under that stricter rule:

- emit unresolved official evidence
- keep the site-query material as `secondary_*`
- do not mint `official_continuity_*`

## 5.3 Change 3 - raise the alias bar from token overlap to exact alias proof

`build_continuity_refs()` should not fire alias continuity because one token overlaps.

Minimum safe v3 rule:

- require the **full normalized other-name phrase**, not any single token
- require that phrase on a **hard-official** page, not on a soft domain match
- if the packet only has surname-level overlap or family-member mentions, emit a soft hint or no continuity ref at all

This one change directly targets the `11.4.m` family-split false merges.

## 5.4 Change 4 - downgrade shared-domain continuity unless page-level brand identity matches

`official_continuity_shared_domain` should become hard continuity only if:

- both sides resolve to the same hard-official producer-owned domain, and
- the page titles / summaries indicate the **same brand identity**, not just same family/company umbrella

Otherwise:

- demote it to `soft_continuity_hint_shared_domain`
- add a portfolio-risk ref when the same domain hosts distinct branded project pages

## 5.5 Change 5 - make the merge veto depend on hard continuity, not any continuity

Current behavior:

- any `official_continuity_*` disables the veto

v3 behavior:

- only `hard_official_continuity_*` can disable the veto
- if the packet contains only soft continuity plus any high-risk contradiction, the row must stay `FLAGGED` or `SKIP`

Keep the existing veto families and add one more:

- existing: `risk_shared_surname_split`
- existing: `risk_holdco_or_product_tier`
- existing: `geo_country_conflict`
- new: `risk_owner_or_operator_not_identity`

## 5.6 Change 6 - add one narrow ownership/acquisition risk flag

For `Veramar/Bogati` and `Bella Union/Provenance`, the packet needs one explicit risk ref that says:

- shared ownership
- operator overlap
- estate acquisition
- site integration

do **not** prove same brand-on-label identity

This is the only adjudicator-side backstop needed for the next proof pass.

---

## 6. Recommended v3 scope

Do **not** spend the next session on:

- new models
- new benchmark cases in the frozen main benchmark
- queue-building
- full rerun infrastructure changes unrelated to continuity

Do spend it on:

1. tightening continuity resolution in `bakeoff_packet_v2.py`
2. splitting hard vs soft continuity refs
3. changing `apply_merge_veto()` so soft continuity does not waive the veto
4. adding the narrow ownership/acquisition risk ref
5. rerunning proof only

That is the minimum change set most likely to move the false-merge count materially without reopening Session 4 or exploding scope.

---

## 7. Proof subset recommendation

Do **not** reuse the current 28-case proof slice unchanged.

Why:

- the current proof subset is selected from **v1 Sonnet** false merges + misses
- it covers only **10 of the 39** v2 false-merge cases
- **29 of the 39** v2 false-merge cases are absent from the current proof

So the proof slice should become:

- **reuse the current 28 cases as the base**
- **add a targeted continuity add-on**

Recommended add-on: **8 continuity stress cases**

1. `blind_core_audit_041` `Dancer` vs `Theo Dancer`
2. `blind_core_audit_067` `Franck Bonville` vs `Camille Bonville`
3. `blind_core_audit_048` `Pierre Meurgey` vs `Meurgey Croses`
4. `blind_core_audit_052` `L'Ostal Cazes` vs `Cazes`
5. `known_false_merge_patterns_005` `Bosio` vs `Luca Bosio`
6. `tail_random_sample_008` `La Tour du Pin` vs `Tour du Pin Figeac`
7. `blind_core_audit_076` `Veramar` vs `Bogati`
8. `blind_core_audit_080` `Bella Union` vs `Provenance Vineyards`

Why these 8:

- 4 alias-cross-mention family/label cases
- 2 shared-domain umbrella cases
- 2 no-continuity ownership/operator cases

That gives the proof subset direct coverage of every v2 continuity failure family without inflating it into another full benchmark run.

Recommended proof size:

- current 28 + targeted 8 = **36 cases**

Pass condition for the next proof:

- zero schema issues
- zero hidden-field leaks
- zero false merges on the 8-case continuity add-on
- materially lower false merges on the reused 28-case base

If that proof fails, stop. Do not run the full 152 again.

---

## 8. Recommendation

Lock v3 around a single principle:

**weak continuity hints must stop pretending to be official continuity.**

The next rerun should test:

- stricter official-domain trust
- hard vs soft continuity refs
- exact-phrase alias proof only
- shared-domain downgraded unless brand identity matches
- one ownership/acquisition risk flag
- expanded 36-case proof subset

That is the minimum v3 with a plausible path to cutting the false-merge clusters that survived the v2 contract cleanup.
