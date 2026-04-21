# `selector_harness_v1`

## Purpose

`selector_harness_v1` turns the fixed Sprint 7 dossier and shortlist contracts
into the exact select-best-or-none packet for `identity_dossier_select_v1`.

This spec answers five questions:

1. What exact object does the selector model see for the anchor producer?
2. How is each shortlisted candidate represented so the model compares
   identities instead of raw source dumps?
3. What exact JSON must the selector return?
4. How do `none` and `UNSURE` differ?
5. What bounded proof should score selector quality before any implementation
   sprint tries to automate against it?

The selector is a **verdict layer over a fixed shortlist**, not a retrieval
engine and not a merge executor.

---

## V1 doctrine

1. Choose **one candidate or none**, never multiple candidates.
2. Show the model normalized identity cards and explicit anchor-vs-candidate
   comparisons, not raw source rows.
3. Make `none` a first-class option, not an implicit failure case.
4. Keep `UNSURE` narrow: it means escalation may add decisive evidence, not
   "I do not want to decide."
5. Use patterned reason codes and evidence references instead of long prose.
6. Prove the selector in isolation before blaming or praising shortlist
   retrieval.

---

## What the selector is and is not allowed to do

### The selector may do

- choose one shortlisted candidate
- choose `none`
- assign one of the four Sprint 7 labels:
  - `SAME_AS`
  - `RELATED_BUT_DISTINCT`
  - `NONE`
  - `UNSURE`
- request escalation when the cheap dossier cannot safely resolve a frontier

### The selector may not do

- invent a candidate that is not in the shortlist
- merge rows
- write graph edges directly
- use free-form source dumps as justification
- treat shared permits, ownership, or importer context as automatic identity

---

## Packet shape the model sees

The selector sees one JSON object:

```json
{
  "selector_version": "selector_harness_v1",
  "case_id": "selector_case_uuid",
  "dossier_version": "producer_dossier_v1",
  "shortlist_version": "shortlist_generation_v1",
  "anchor": {},
  "shortlist": {},
  "decision_options": {}
}
```

### Packet-level rules

1. `anchor` is always present.
2. `shortlist.candidates` may be empty.
3. `shortlist.candidate_count` must be `<= 12`.
4. The packet must contain **no hidden answer key**, benchmark label, or
   expected candidate id.
5. All evidence the selector cites later must exist in the packet by JSON path.

---

## Anchor card

The selector should not see the full raw `producer_dossier_v1` object.
It should see a selector-specific projection that keeps the important blocks but
compresses them into a stable, auditable identity card.

### Anchor shape

```json
{
  "producer_id": "uuid",
  "canonical_name": "string",
  "name_normalized": "string",
  "slug": "string",
  "country": "string|null",
  "region": "string|null",
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
  "source_presence": {
    "wine_count": 0,
    "vintage_count": 0,
    "source_families": [
      {
        "source_family": "canonical|lwin|ttb|state_reg|importer|retailer",
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

### Anchor caps for selector use

These are intentionally tighter than the full dossier caps.

| Field | Max items |
| --- | ---: |
| `label_like_names` | `8` |
| `legal_or_applicant_names` | `4` |
| `source_presence.source_families` | `6` |
| `countries`, `regions`, `appellations` | `6` each |
| `top_wine_labels` | `6` |
| `top_lwin_display_names` | `4` |
| `top_grapes`, `top_designations`, `color_mix` | `4` each |
| `ttb_permits`, `ttb_applicant_names`, `supplier_or_distributor_names` | `6` each |
| each conflict list | `4` |

Reason: the anchor should feel rich enough to reason over, but not so large
that the model starts skimming instead of comparing.

---

## Candidate mini-cards

Each shortlisted candidate should appear as a compact mini-card with three
parts:

1. **retrieval basis** — why this candidate is even here
2. **candidate identity card** — the candidate's own compressed fingerprint
3. **comparison to anchor** — the normalized overlaps and conflicts the model
   should reason over

This is the key move that keeps the selector comparing identities rather than
mentally diffing two raw dossiers.

### Candidate shape

```json
{
  "candidate_id": "uuid",
  "candidate_rank": 1,
  "retrieval_basis": {
    "seed_families": ["canonical", "lwin"],
    "lexical_strength": "exact_normalized|containment|orthographic_variant|fuzzy",
    "matched_anchor_name_forms": ["string"],
    "shortlist_pressure_band": "exact_multi_source|exact_single_source|variant_multi_source|fallback"
  },
  "candidate_identity": {
    "canonical_name": "string",
    "country": "string|null",
    "region": "string|null",
    "label_like_names": [{"value": "string", "row_count": 0}],
    "legal_or_applicant_names": [{"value": "string", "row_count": 0}],
    "source_families": [{"source_family": "string", "linked_rows": 0}],
    "top_wine_labels": [{"value": "string", "count": 0}],
    "top_lwin_display_names": [{"value": "string", "count": 0}],
    "top_grapes": [{"value": "string", "count": 0}],
    "top_designations": [{"value": "string", "count": 0}],
    "color_mix": [{"value": "string", "count": 0}],
    "ttb_applicant_names": [{"value": "string", "count": 0}],
    "supplier_or_distributor_names": [{"value": "string", "count": 0}],
    "conflicts": ["string"],
    "sparse_signal_flags": ["string"]
  },
  "comparison_to_anchor": {
    "matched_name_forms": ["string"],
    "matched_place_signals": ["string"],
    "matched_portfolio_signals": ["string"],
    "matched_regulatory_signals": ["string"],
    "divergent_name_signals": ["string"],
    "divergent_place_signals": ["string"],
    "divergent_portfolio_signals": ["string"],
    "risk_tags": ["shared_surname", "permit_only", "global_brand_country_split"],
    "why_this_candidate_survived": ["string"]
  }
}
```

### Candidate caps

| Field | Max items |
| --- | ---: |
| `matched_anchor_name_forms` | `4` |
| candidate `label_like_names` | `6` |
| candidate `legal_or_applicant_names` | `4` |
| candidate `source_families` | `5` |
| candidate `top_wine_labels` | `5` |
| candidate `top_lwin_display_names` | `3` |
| candidate `top_grapes`, `top_designations`, `color_mix` | `3` each |
| `ttb_applicant_names`, `supplier_or_distributor_names` | `4` each |
| each comparison list | `4` |
| `risk_tags`, `why_this_candidate_survived` | `4` each |

### Why the comparison block matters

The selector should not have to infer all deltas by itself from two separate
cards. `comparison_to_anchor` precomputes the important identity contrasts:

- exact or near-exact name bridges
- geography coherence or divergence
- portfolio overlap
- regulatory alignment
- known danger tags from `IDENTITY_RULES.md` families

That keeps the model focused on the identity question instead of turning the
prompt into a document-comparison exercise.

---

## Decision options object

The packet should also include a small explicit decision frame so `none` is a
real choice and `UNSURE` is not left vague.

```json
{
  "allowed_choice_types": ["candidate", "none"],
  "none_definition": "Choose NONE when no shortlisted candidate deserves SAME_AS or RELATED_BUT_DISTINCT and escalation is unlikely to change that.",
  "unsure_definition": "Choose UNSURE when evidence is too thin or conflicting for a safe accepted label and escalation has a plausible path to resolve it.",
  "selection_rule": "Choose at most one candidate. Do not emit multiple candidates or an invented candidate."
}
```

---

## Recommended model-visible wrapper

The selector prompt wrapper should be short and operational:

1. Read the anchor card first.
2. Review each candidate mini-card and its comparison block.
3. Choose **one candidate or none**.
4. Apply the label to that choice:
   - `SAME_AS` = same label-facing producer identity
   - `RELATED_BUT_DISTINCT` = meaningful relationship but not the same producer
   - `NONE` = no candidate deserves an accepted relationship
   - `UNSURE` = escalation may resolve a real frontier case
5. Prefer `NONE` or `UNSURE` over a risky `SAME_AS`.
6. Return JSON only.

The wrapper should not ask for essays or force the model to explain every
candidate.

---

## Output contract

The selector must emit exactly one JSON object in this shape:

```json
{
  "selector_version": "selector_harness_v1",
  "anchor_producer_id": "uuid",
  "shortlist_status": "candidates_present|no_candidate_found",
  "choice": {
    "choice_type": "candidate|none",
    "selected_candidate_id": "uuid|null",
    "selected_candidate_rank": "integer|null"
  },
  "label": "SAME_AS|RELATED_BUT_DISTINCT|NONE|UNSURE",
  "primary_reason_code": "string",
  "secondary_reason_codes": ["string"],
  "rule_hypotheses": ["11.4.h"],
  "support_ref_paths": ["/shortlist/candidates/0/comparison_to_anchor/matched_name_forms/0"],
  "conflict_ref_paths": ["/shortlist/candidates/0/comparison_to_anchor/divergent_place_signals/0"],
  "needs_escalation": true,
  "escalation_focus": ["web_identity", "profile_snippets", "people_history", "raw_supporting_rows", "retrieval_gap"]
}
```

### Output rules

1. `choice.choice_type = "candidate"` requires:
   - `selected_candidate_id` present
   - `selected_candidate_rank` present
2. `choice.choice_type = "none"` requires:
   - `selected_candidate_id = null`
   - `selected_candidate_rank = null`
3. `label = SAME_AS` requires `choice_type = candidate`
4. `label = RELATED_BUT_DISTINCT` requires `choice_type = candidate`
5. `label = NONE` requires `choice_type = none`
6. `label = UNSURE` may use either:
   - `choice_type = candidate` when one candidate is the frontier
   - `choice_type = none` when the shortlist itself looks incomplete or too thin
7. `support_ref_paths` must contain:
   - `2-6` valid JSON paths for `SAME_AS`, `RELATED_BUT_DISTINCT`, and `NONE`
   - `1-6` valid JSON paths for `UNSURE`
8. `conflict_ref_paths` may be empty for accepted labels, but must be present
   for `UNSURE`
9. `needs_escalation` must be:
   - `false` for `SAME_AS`, `RELATED_BUT_DISTINCT`, and `NONE`
   - `true` for `UNSURE`
10. `escalation_focus` must be empty unless `needs_escalation = true`

### Why JSON paths

`support_ref_paths` and `conflict_ref_paths` should point back into the packet
so the scorer can verify that the selector actually used visible evidence.
Friendly IDs can be added later, but JSON path validity is the minimum
auditability requirement.

---

## Reason-code families

`primary_reason_code` must come from one of these families.
`secondary_reason_codes` are optional but should stay small (`<= 3`).

### `SAME_AS`

| Code | Meaning | Typical rule crosswalk |
| --- | --- | --- |
| `same_exact_or_orthographic_alias` | exact alias, accent, article, or spelling variant | `11.4.h` |
| `same_historical_name_continuity` | historical form, succession, or generational rename | `11.4.f` |
| `same_legal_vs_label_identity` | legal/applicant naming differs but bottle-facing identity is the same | `11.4.i` / `11.4.h` |
| `same_merchant_or_importer_prefix` | merchant/importer prefix wraps the real producer identity | `11.4.i` / `11.4.p` |
| `same_global_brand_multi_country` | same global brand split across countries or sourcing rows | `11.4.n` |
| `same_sparse_stub_absorption` | one candidate is an obvious empty or near-empty artifact row | `11.4.h` |

### `RELATED_BUT_DISTINCT`

| Code | Meaning | Typical rule crosswalk |
| --- | --- | --- |
| `related_shared_owner_distinct_brand` | same holdco/owner, different label-facing brand | `11.4.g` |
| `related_shared_family_distinct_estates` | same surname/family, distinct estates | `11.4.m` |
| `related_shared_permit_or_facility` | same permit/facility/custom-crush environment, distinct brands | `11.4.j` |
| `related_joint_venture_or_collab` | JV/collab relation to a principal | `11.4.o` |
| `related_auction_or_negociant_bottling` | same auction/negociant context, distinct bottlings | `11.4.q` / `11.4.r` |
| `related_subbrand_or_secondary_label` | related sub-brand or secondary label that should not merge | `11.4.s` |

### `NONE`

| Code | Meaning |
| --- | --- |
| `none_no_candidate_survived` | shortlist is empty and that is the correct cheap-path outcome |
| `none_lexical_collision_only` | name overlap only; no coherent identity signal |
| `none_place_portfolio_conflict` | geography and portfolio diverge strongly enough to rule out relation |
| `none_weak_fuzzy_no_support` | weak fuzzy hit with no second-source support |
| `none_noise_candidate` | candidate is shortlist noise, not a meaningful related entity |

### `UNSURE`

| Code | Meaning |
| --- | --- |
| `unsure_conflicting_signals` | real contradictions remain inside the cheap dossier |
| `unsure_thin_evidence` | anchor or candidate is too sparse for a safe accepted label |
| `unsure_top_candidates_too_close` | two shortlist candidates are plausibly live and cheap evidence cannot separate them |
| `unsure_shortlist_gap_possible` | the shortlist itself may be missing the true candidate |
| `unsure_escalation_only_signal_needed` | website/profile/history/raw supporting rows are likely needed |

---

## `none` vs `UNSURE`

This distinction must stay sharp.

### Choose `NONE` when

- no candidate deserves `SAME_AS`
- no candidate deserves `RELATED_BUT_DISTINCT`
- the shortlist looks ordinary, not suspiciously incomplete
- escalation is unlikely to add a decisive new signal

`NONE` is a confident negative outcome.

### Choose `UNSURE` when

- a specific candidate is a real frontier case, or
- the shortlist itself may be incomplete, or
- the cheap dossier is materially contradictory or too thin, and
- escalation has a plausible path to resolve the case

`UNSURE` is not "negative with low confidence." It is "frontier case worth
extra evidence."

### Practical test

Use this rule:

- if the right next step is **stop** -> `NONE`
- if the right next step is **spend more evidence here** -> `UNSURE`

---

## `RELATED_BUT_DISTINCT` vs `NONE`

Use `RELATED_BUT_DISTINCT` only when the evidence supports a **durable,
non-merge relationship** that is worth storing later.

Examples:

- same holdco, different label brand
- shared family, different estate
- custom-crush/shared permit adjacency
- JV/collab label vs a parent
- auction or negociant bottling relation

If the candidate is just shortlist noise or a lexical collision, use `NONE`
instead.

---

## Proof design for Session 10.6

The proof should be split into two phases so selector quality and retrieval
quality stay separately legible.

### Phase A: selector-only proof

Use **frozen hand-built shortlist packets** so we test:

- packet shape
- prompt wrapper
- output schema
- `none` vs `UNSURE`
- label discipline

without letting shortlist-builder bugs hide inside selector scoring.

### Phase B: shortlist integration smoke

Run the same proof cases through the real shortlist builder and compare:

- whether the expected candidate is present
- whether candidate caps are respected
- whether the selected candidate stays near the top

Phase B is an integration check, not a replacement for Phase A.

---

## Hidden proof-set shape

The frozen answer key for `selector_proof_v1` should look like this:

```json
{
  "proof_version": "selector_proof_v1",
  "case_id": "selector_proof_001",
  "risk_tier": "core|tail",
  "pattern_family": "11.4.h",
  "world_relationship": "SAME_AS|RELATED_BUT_DISTINCT|NONE",
  "expected_selector_label": "SAME_AS|RELATED_BUT_DISTINCT|NONE|UNSURE",
  "acceptable_candidate_ids": ["uuid"],
  "shortlist_expectation": "candidate_present|empty_shortlist_ok|frontier_gap_ok",
  "escalation_expected": true
}
```

### Why both `world_relationship` and `expected_selector_label`

Some cases are genuinely resolvable in the real world but should still be
`UNSURE` on the cheap path. The proof should score the selector against the
**expected cheap-path behavior**, not punish safe abstention just because the
world truth is knowable with richer evidence.

---

## Recommended bounded-proof composition

Freeze a **48-case** proof set:

| Stratum | Count | Purpose |
| --- | ---: | --- |
| `same_as_core` | `16` | alias, orthographic, historical-form, merchant-prefix, and global-brand recoveries already visible in Tier A data |
| `related_controls` | `12` | holdco, shared-surname, permit/facility, JV, sub-brand, and auction/negociant adjacency cases |
| `none_controls` | `12` | lexical collisions, weak fuzzy survivors, and noise cases where no durable relation should be stored |
| `unsure_frontier` | `8` | thin or contradictory cases where the cheap path should abstain and request escalation |

### Why `48`

1. It covers all four labels.
2. It is small enough to hand-audit.
3. It is large enough to include the failure families Loam already learned the
   hard way in Sprint 6.

Use `benchmark_v1` as source material where it fits, but do **not** mutate
`benchmark_v1`. `selector_proof_v1` is a separate frozen artifact because it
needs `RELATED_BUT_DISTINCT` and `UNSURE` cases that the merge-only benchmark
does not contain.

---

## Phase A scoring

### Primary outcome buckets

- `false_same_as`
  - selector emitted `SAME_AS` on a case whose expected selector label is not
    `SAME_AS`, or chose the wrong candidate
- `false_related`
  - selector emitted `RELATED_BUT_DISTINCT` on a case whose expected selector
    label is not `RELATED_BUT_DISTINCT`, or chose the wrong candidate
- `missed_same_as`
  - expected `SAME_AS`, selector emitted anything else
- `missed_related`
  - expected `RELATED_BUT_DISTINCT`, selector emitted anything else
- `false_none`
  - selector emitted `NONE` where the expected label is `SAME_AS` or
    `RELATED_BUT_DISTINCT`
- `over_escalation`
  - expected accepted label or `NONE`, selector emitted `UNSURE`
- `unsafe_frontier_resolution`
  - expected `UNSURE`, selector emitted an accepted label
- `schema_error`
  - invalid JSON, illegal label, illegal candidate choice, or broken field
    requirements

### Integrity metrics

- `schema_valid_rate`
- `choice_valid_rate`
  - selected candidate must actually exist in the shortlist
- `evidence_ref_integrity_rate`
  - all support/conflict paths resolve into the packet

### Phase A gates

The selector harness is not ready to implement unless all of these pass:

1. `false_same_as = 0`
2. `false_related <= 1`
3. `unsafe_frontier_resolution = 0`
4. `missed_same_as <= 4` out of `16`
5. `missed_related <= 4` out of `12`
6. `over_escalation <= 8` out of `40` non-frontier cases
7. `schema_valid_rate = 1.00`
8. `choice_valid_rate = 1.00`
9. `evidence_ref_integrity_rate >= 0.95`

Interpretation:

- zero false `SAME_AS` is the hard safety bar
- `RELATED_BUT_DISTINCT` mistakes are less catastrophic than false merges, but
  they still need to stay rare
- some missed captures are acceptable in the first bounded proof
- format and evidence traceability are non-negotiable

---

## Phase B integration smoke metrics

Phase B should measure shortlist behavior on the same frozen proof cases.

### Metrics

- `gold_candidate_present_rate`
  - among cases whose `acceptable_candidate_ids` should be present
- `gold_candidate_top_3_rate`
- `shortlist_cap_breaches`
- `none_control_median_candidate_count`
  - on `none_controls`
- `empty_shortlist_correct_rate`
  - on cases where `empty_shortlist_ok` is the expected outcome

### Phase B gates

1. `gold_candidate_present_rate >= 0.90`
2. `gold_candidate_top_3_rate >= 0.75`
3. `shortlist_cap_breaches = 0`
4. `none_control_median_candidate_count <= 3`
5. `empty_shortlist_correct_rate >= 0.75`

Reason: selector quality is meaningless if retrieval regularly hides the real
candidate or floods obvious `NONE` cases with noise.

---

## Bottom line

`selector_harness_v1` fixes the missing middle of Sprint 7:

- one anchor selector card
- up to `12` candidate mini-cards
- choose one candidate or `none`
- apply one of four explicit labels
- keep `none` and `UNSURE` separate
- prove the selector in isolation before we conflate it with retrieval or
  escalation

With this spec frozen, the next honest session can design the escalation layer
and accepted-edge rules without reopening what the selector sees or returns.
