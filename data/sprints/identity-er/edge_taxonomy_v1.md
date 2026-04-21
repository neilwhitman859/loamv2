# Edge taxonomy for `identity_dossier_select_v1`

This file defines the selector's exact output labels and what each one means
for graph storage, clustering, and merge eligibility.

---

## Output labels

The selector must emit exactly one of these labels:

- `SAME_AS`
- `RELATED_BUT_DISTINCT`
- `NONE`
- `UNSURE`

No other label is allowed in `v1`.

---

## Label semantics

| Label | Meaning | Store as accepted edge? | Merge-eligible? | Effect on future clustering |
| --- | --- | --- | --- | --- |
| `SAME_AS` | Same label-facing producer identity. These two rows should collapse to one producer identity if later execution gates allow it. | Yes, positive identity edge | Yes, this is the only merge-eligible label | Can contribute to later clustering |
| `RELATED_BUT_DISTINCT` | Not the same producer identity, but there is a meaningful relationship worth preserving explicitly | Yes, related-but-non-identity edge | No | Must block merge-by-transitivity across this edge |
| `NONE` | Not the same producer and no explicit relationship claim is justified from the evidence object | Yes, negative edge | No | Must block merge between the pair |
| `UNSURE` | Evidence is conflicting or too thin to make an accepted claim | No accepted graph edge yet | No | Send to escalation or leave unresolved |

---

## What counts as `SAME_AS`

`SAME_AS` means the candidate is the same producer identity a human should see
on the bottle or in the catalog after cleanup.

Typical patterns:

- accent/no-accent or short/full-form variants
- legal-name vs bottle-facing-name forms for the same producer
- same producer split across countries or source systems when the portfolio and
  identity signals cohere
- clear same-brand duplicates created by import or matching drift

`SAME_AS` is not "shares a permit," "same family," or "same importer." It is a
single producer identity verdict.

---

## What counts as `RELATED_BUT_DISTINCT`

This label is first-class because Sprint 6 repeatedly found adjacent entities
that should not merge but also should not be treated as unrelated.

Typical patterns:

- shared ownership or holdco, different label-facing producer
- custom-crush neighbors sharing a facility or permit
- merchant or importer prefixes attached to a distinct producer
- parent brand vs sub-brand where the brand on label is distinct
- same surname family, same commune, same history, but distinct producer rows
- joint venture or collaboration brand that is not the same identity as either
  parent

The key rule: `RELATED_BUT_DISTINCT` is still a non-merge verdict.

---

## What counts as `NONE`

`NONE` means the shortlisted candidate is simply not the same producer and the
evidence object does not justify storing a durable relationship.

Typical patterns:

- lexical collision only
- same country and same surname, but no coherent portfolio or market linkage
- same importer or merchant environment with no real producer-level connection
- noisy shortlist artifact

`NONE` should be common. It is the default safe outcome for weak candidates.

---

## What counts as `UNSURE`

`UNSURE` is not a soft merge. It means one of these is true:

1. the cheap dossier is missing too much evidence
2. the evidence is materially contradictory
3. the candidate shortlist did not surface a clean best match
4. the selector cannot distinguish `RELATED_BUT_DISTINCT` from `SAME_AS`

`UNSURE` should either:

- trigger the escalation dossier, or
- remain unresolved if escalation is not worth the spend

It should not write an accepted positive or negative edge by itself.

---

## Minimal selector output contract

The selector response should carry enough structure to support storage and
audit, but stay lean:

```json
{
  "anchor_producer_id": "uuid",
  "selected_candidate_id": "uuid|null",
  "label": "SAME_AS|RELATED_BUT_DISTINCT|NONE|UNSURE",
  "reason_code": "string",
  "evidence_for": ["string"],
  "conflicts": ["string"],
  "needs_escalation": true
}
```

Recommended rules:

- `selected_candidate_id` is required for `SAME_AS` and
  `RELATED_BUT_DISTINCT`
- `selected_candidate_id` may be `null` for `NONE` and `UNSURE`
- `reason_code` should be short and patterned, for example:
  - `exact_alias`
  - `shared_wine_identity`
  - `shared_owner_not_same_brand`
  - `custom_crush_shared_permit`
  - `merchant_prefix`
  - `same_family_distinct_estates`
  - `insufficient_evidence`

---

## Storage rules

1. Only accepted `SAME_AS` edges may later feed clustering or merge execution.
2. `RELATED_BUT_DISTINCT` and `NONE` are both non-merge barriers.
3. `RELATED_BUT_DISTINCT` must not be silently downgraded to `NONE`; the
   relationship is useful product data and a safety guard.
4. `UNSURE` should not create a durable accepted edge unless a later
   escalation step resolves it.
5. No optimistic transitivity:
   - `A SAME_AS B`
   - `B RELATED_BUT_DISTINCT C`
   - therefore `A` must not merge with `C`

---

## Why this matters

The old pairwise framing collapsed too much ambiguity into "merge or do not
merge." `identity_dossier_select_v1` only becomes trustworthy if the selector
can distinguish:

- true identity matches
- adjacent-but-separate producer identities
- ordinary shortlist misses
- genuinely unresolved frontier cases

That is what this label set is for.
