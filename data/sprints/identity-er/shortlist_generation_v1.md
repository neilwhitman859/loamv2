# `shortlist_generation_v1`

## Purpose

`shortlist_generation_v1` defines how `identity_dossier_select_v1` turns one
anchor producer into a **small candidate set** for select-best-or-none
adjudication.

This spec answers four questions:

1. Which signals are allowed to create candidate IDs?
2. Which signals may only re-rank or break ties after a candidate already
   exists?
3. How small must the final shortlist stay?
4. Which obvious false candidates should be dropped before selector work?

The goal is not "maximum recall at any cost." The goal is a shortlist that is
small enough to stay trustworthy and cheap while still catching the merge-worthy
cases Loam most needs to recover.

---

## Live evidence that constrains the design

All counts below are from the live DB on 2026-04-21.

- `33,281` total producers
- `33,247` producers have at least one **external Tier A** footprint in LWIN,
  TTB, PRO, TABC, or Kansas
- only `34` producers have no external Tier A footprint at all
- only `4` producers have no external Tier A footprint but do appear in any
  Tier B or Tier C source
- exact normalized-name collisions are limited:
  - `32,153` normalized names are singletons
  - `439` normalized names have exactly `2` producers
  - `54` normalized names have `3-5` producers
  - `8` normalized names have `6-10` producers
  - only `1` normalized name bucket is larger than `10`
  - the largest bucket size is `11`
- only `1,128` producer rows live in any duplicate normalized-name bucket at all

Implications:

1. The shortlist does **not** need to be large. Exact-name ambiguity is
   concentrated in a small minority of the corpus.
2. Importer / retailer / profile sources are not worth promoting to seed
   generators in `v1`. They would broaden the candidate universe for almost the
   whole corpus while buying meaningful extra retrieval for only a tiny fringe.
3. `v1` should bias toward **high-quality candidate generation** over
   "never miss any remotely related row."

---

## V1 doctrine

1. Tier A sources may seed candidates. Tier B may only re-rank. Tier C is
   escalation-only.
2. The final selector shortlist must stay at **12 candidates or fewer**.
3. Empty shortlist is a legal outcome. Safe abstention beats forced guessing.
4. A weak lexical hit should not survive just because the candidate universe is
   cheap to grow.
5. Regulatory and market clues are corroborators, not automatic merge evidence.
6. Suppression should remove known false-candidate families before the selector
   sees them.

---

## Source roles

### Tier A: allowed to seed candidate IDs in `v1`

These are the only sources that may create new candidate IDs for the shortlist.

| Seed family | Allowed seed fields | Why allowed |
| --- | --- | --- |
| Canonical lexical | `producers.name`, `producers.name_normalized`, deterministic stripped/short-form variants from the canonical name | Cheapest first-pass retrieval and universal coverage |
| LWIN lexical | `source_lwin.producer_name` mapped back to `canonical_producer_id` | Near-full global backbone and strong producer-name coverage |
| TTB lexical | `source_ttb_colas.brand_name`, `source_ttb_colas.applicant_name` mapped back to `canonical_producer_id` | Strong US label/legal-name evidence at scale |
| State-registration lexical | `source_pro_platform.brand`, `source_pro_platform.supplier_name`, `source_tabc.brand_name`, `source_tabc.trade_name`, `source_kansas_brands.brand_name` mapped back to `canonical_producer_id` | Additional US brand/legal-name corroboration |

### Tier B: may re-rank or break ties, but may not seed by themselves

| Source family | Allowed use in `v1` | Why not seed |
| --- | --- | --- |
| Shared wine / LWIN overlap | boost or demote candidates already found by Tier A | strong once a candidate exists, but too indirect to create candidates from scratch |
| TTB `permit_no`, applicant state, state-reg permit / cola references | boost or demote candidates already found by Tier A | custom-crush / shared-facility risk is too high for standalone seeding |
| Skurnik, Flatiron, Specs, Wally's, Firstleaf | corroborate portfolio, geography, or merchant presence | noisy commercial context; useful support, weak first-pass identity retrieval |
| Distributor / supplier names | break ties or help surface `RELATED_BUT_DISTINCT` later | often relationship clues, not bottle-facing identity |
| Canonical place / portfolio fingerprints | rerank exact or near-exact candidates | should shape final order, not widen the raw pool |

### Tier C: escalation-only

| Source family | Allowed use |
| --- | --- |
| Kermit Lynch growers, Winebow, Empson, European Cellars | escalation dossier only |
| Website domains / history / people snippets | escalation dossier only |

### Explicit non-seed rule

Importer, retailer, profile, website, terroir, and history sources may **not**
create a candidate ID that Tier A failed to create. If Tier A did not surface a
candidate, Tier B/C may only help later escalation or explain why the shortlist
stayed empty.

---

## Retrieval pipeline

### Stage 0: build the anchor variant pack

For each anchor producer, generate a compact set of normalized name forms:

- canonical name
- canonical normalized name
- deterministic stripped form after removing generic producer words such as
  `domaine`, `chateau`, `tenuta`, `bodega`, `vina`, `weingut`, `cantina`, and
  legal suffix noise
- conservative short form only when it is directly recoverable from the anchor's
  own Tier A names

Do **not** generate open-ended acronyms or free-form nickname expansions in
`v1`.

### Stage 1: Tier A raw candidate generation

Generate candidate IDs independently from each allowed seed family, then union
them.

Per-family raw caps:

| Seed family | Raw cap per anchor |
| --- | ---: |
| Canonical lexical | `8` |
| LWIN lexical | `8` |
| TTB lexical | `6` |
| State-registration lexical | `6` |

Rules:

1. Exact normalized matches outrank containment matches.
2. Containment matches outrank conservative fuzzy/trigram matches.
3. Same candidate found by multiple seed families is kept once with all source
   provenance attached.
4. Raw union is capped at **24 unique candidates** before suppression.

Reason for `24`:

- It is large enough to preserve multi-source ambiguity before pruning.
- It is small enough that the system cannot silently drift back toward a giant
  pair corpus.

### Stage 2: suppression before selector

Apply the suppression rules below. Candidates dropped here do not reach the
selector.

### Stage 3: rerank surviving candidates

Re-rank only the surviving candidates using:

- number of independent Tier A seed families that found the candidate
- exact vs containment vs fuzzy lexical strength
- shared wine / `lwin_7` / display-name overlap
- place coherence: country, region, appellation footprint
- portfolio coherence: top labels, grapes, designations, color mix
- regulatory coherence: same applicant name shape, same state-registration
  supplier/brand alignment
- Tier B corroboration from importer / retailer / merchant presence

### Stage 4: final shortlist trim

Keep the top **12** surviving candidates for selector work.

Reason for `12`:

1. The largest duplicate normalized-name bucket in the live DB is `11`, so a
   `12`-candidate cap still preserves the entire hardest exact-name ambiguity
   set plus one extra slot for a non-exact near-match.
2. The selector needs room for real ambiguity, but not so much room that it
   turns into another noisy corpus review.

If zero candidates survive, the outcome is:

- `shortlist = []`
- `shortlist_status = no_candidate_found`

That is a valid bounded-proof outcome.

---

## Suppression rules

These rules exist to kill obvious false candidates before selector work.

### 1. Self and duplicate collapse

- drop the anchor row itself
- collapse duplicate candidate IDs found by multiple seed families into one
  record
- collapse repeated normalized forms for the same candidate into one best
  label-facing form

### 2. Weak lexical-only suppression

Drop the candidate when all of these are true:

- the match is only a loose fuzzy / trigram hit or a one-token overlap
- the shared token is not an exact normalized name
- no second Tier A family independently found the same candidate

This rule exists to stop the shortlist from filling with vague surname and
substring collisions.

### 3. Shared-surname / first-word collision suppression

Drop the candidate when all of these are true:

- the lexical bridge is only a shared surname, one shared dominant token, or a
  generic first-word overlap
- there is no exact normalized match
- there is no corroborating Tier A portfolio or regulatory alignment

This targets the failure families already documented in `IDENTITY_RULES.md`:

- shared-surname family splits (`11.4.m`)
- commune / first-word collisions (`11.4.k`)

### 4. Permit-only / facility-only suppression

Drop the candidate when all of these are true:

- the connection is driven only by shared `permit_no`, applicant state, or
  other facility-style regulatory metadata
- bottle-facing brand names do not cohere strongly
- there is no supporting portfolio overlap

This is the shortlist-layer version of `IDENTITY_RULES.md` `11.4.j`:
custom-crush and shared-facility adjacency is **not** enough reason to show the
candidate to the selector.

### 5. Legal-name-only suppression

Drop the candidate when all of these are true:

- the only bridge is legal/applicant/supplier naming
- bottle-facing names diverge materially
- there is no second independent Tier A family confirming same-brand identity

This prevents holdcos, bottlers, and administrative entities from crowding the
shortlist when the label brand is actually different.

### 6. Merchant / importer / retailer context-only suppression

Drop the candidate when all of these are true:

- the apparent link comes only from merchant, importer, distributor, or retailer
  context
- Tier A did not produce a real lexical candidate
- there is no exact same-name canonical candidate already present

Reason: these sources are excellent for context and for
`RELATED_BUT_DISTINCT`, but too noisy for first-pass candidate creation.

### 7. Contradiction suppression for weak matches

Drop a weak, non-exact lexical candidate when:

- place fingerprints diverge sharply, and
- portfolio fingerprints diverge sharply, and
- no regulatory or shared-wine evidence counterbalances the divergence

Exact same-name candidates are exempt from this rule in `v1`; they may still
reach the selector even when geography conflicts, because global brands and
country-split duplicate rows are a real pattern.

### 8. Shortlist pressure suppression

When exact and multi-source candidates already fill the cap, drop the weakest
remaining fuzzy candidates first.

Priority order under pressure:

1. exact multi-source candidates
2. exact single-source candidates
3. containment / orthographic variants with second-source corroboration
4. fuzzy single-source candidates

---

## Guardrails against false suppression

The suppression layer should stay conservative in two specific ways:

1. **Do not drop exact same-name candidates only because countries differ.**
   Session 6 and the live DB both show real multi-country global brands
   (`Tussock Jumper`, `90+ Cellars`, `Cupcake Vineyards`) that would be lost by
   that rule.
2. **Do not let permit overlap overrule brand mismatch.**
   Shared TTB or state-reg permits are adjacency clues, not identity proof.

---

## Ranking order after suppression

The final ranking stack for `v1` is:

1. exact normalized candidate found by `2+` Tier A families
2. exact normalized candidate found by `1` Tier A family with strong shared
   wine / place / regulatory coherence
3. strong containment or orthographic variant found by `2+` Tier A families
4. strong containment or orthographic variant found by `1` Tier A family plus
   strong Tier B corroboration
5. remaining canonical lexical fallback candidates

Tier B may move a candidate **up or down** inside those bands, but Tier B may
not introduce a new candidate or rescue a candidate that suppression already
dropped.

---

## Acceptable recall gaps for the first bounded proof

The first proof does **not** need to solve every candidate-retrieval case.
These gaps are acceptable in `v1`:

1. Cases where importer / retailer / profile evidence is the only real bridge.
2. Cases that require website/history/people evidence to discover the candidate
   at all.
3. Cases where the best available bridge is shared-facility or shared-permit
   adjacency.
4. Cases where the candidate can only be justified by weak fuzzy lexical
   similarity with no second-source support.

These misses are **not** acceptable in the first proof:

1. exact same-name duplicates already visible through Tier A sources
2. obvious legal-name vs bottle-name variants already visible in TTB or state
   registration data
3. obvious multi-country same-brand duplicates already visible through exact
   name plus LWIN / regulatory support
4. cases where multiple Tier A families independently point to the same
   candidate and the shortlist still fails to surface it

---

## Bottom line

`shortlist_generation_v1` is intentionally narrow:

- Tier A creates candidates
- Tier B re-ranks candidates
- Tier C handles escalation only
- final shortlist cap = `12`
- empty shortlist is allowed
- known false-candidate families are suppressed before selector work

That keeps Sprint 7 honest. The next session can now design the selector
harness against a fixed candidate universe instead of reopening retrieval scope.
