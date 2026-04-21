# `escalation_dossier_v1`

## Purpose

`escalation_dossier_v1` is the narrow heavy-path packet for
`identity_dossier_select_v1`.

It exists for one job only:

> take a selector-side `UNSURE` frontier case and decide whether richer,
> still-bounded evidence can safely resolve it to `SAME_AS`,
> `RELATED_BUT_DISTINCT`, or `NONE` without reopening the whole shortlist.

Escalation is not a second wide-open selector. It is a one-pass frontier
resolver.

---

## V1 doctrine

1. Only `UNSURE` cases may escalate.
2. Escalation stays candidate-focused whenever possible.
3. The heavy path may add richer evidence blocks, but it may not widen into a
   new free-form retrieval session.
4. One escalation pass only. If the heavy pass is still unsure, the case stays
   unresolved.
5. The escalation output uses the same four-label contract as the cheap
   selector so accepted-edge logic does not fork by stage.

---

## Which `UNSURE` cases deserve escalation

Escalation is allowed only when **all** of the following are true:

1. the cheap selector returned `label = UNSURE`
2. `needs_escalation = true`
3. the selector packet and output are schema-valid
4. at least one requested escalation block has real source coverage on the
   anchor, the frontier candidate, or both
5. the case fits one of the eligible frontier families below

### Eligible frontier families

| Frontier family | When to escalate | Typical cheap-path code |
| --- | --- | --- |
| Candidate contradiction | one chosen candidate still looks live, but cheap evidence contains real conflicts that richer identity/profile evidence may resolve | `unsure_conflicting_signals` |
| Candidate sparsity | one chosen candidate is plausible, but one or both sides are too thin in the cheap dossier and richer source excerpts actually exist | `unsure_thin_evidence` |
| Top-two ambiguity | the selector cannot safely separate the top candidate from one close rival, and richer evidence may break the tie | `unsure_top_candidates_too_close` |
| Shortlist-gap suspicion | the selector thinks the shortlist may be missing the real candidate, and there is a concrete gap signal rather than mere frustration | `unsure_shortlist_gap_possible` |
| Escalation-only signal needed | the selector can name the exact richer block likely to decide the case | `unsure_escalation_only_signal_needed` |

### Concrete gap signals required for shortlist-gap escalation

`UNSURE` with `choice_type = none` may escalate only when at least one of these
is true:

- the raw Tier A union contained a suppressed exact or near-exact candidate
- the anchor has non-empty `escalation_available.profile_source_families`
- the anchor or a top candidate has a non-empty `website_domains` signal
- the cheap dossier exposes a concrete retrieval blind spot that Session 10.6
  can represent in `retrieval_gap_diagnostics`

If none of those exist, `UNSURE` should close as abstention, not escalate.

---

## Which `UNSURE` cases should stop immediately

Do **not** escalate when any of these are true:

- the selector returned `SAME_AS`, `RELATED_BUT_DISTINCT`, or `NONE`
- the only bridge is weak fuzzy overlap, shared surname, first-word collision,
  permit-only adjacency, facility-only adjacency, merchant context, or importer
  context
- the case has already consumed one escalation pass
- no requested escalation block has non-empty source coverage
- the shortlist is empty and there is no concrete shortlist-gap signal
- the selector's own evidence already supports "stop" and the model merely
  hedged

These cases should end in abstention or a cheap-path `NONE`, not in a more
expensive packet.

---

## Escalation modes

`escalation_dossier_v1` supports exactly two modes.

### 1. `candidate_frontier`

Use when the cheap selector identified one specific frontier candidate.

Rules:

- the packet includes the anchor card, the chosen frontier candidate, and at
  most one optional shadow rival summary
- the heavy pass may resolve to `SAME_AS`, `RELATED_BUT_DISTINCT`, `NONE`, or
  remain `UNSURE`
- the heavy pass may not introduce a new candidate id

### 2. `shortlist_gap_probe`

Use when the cheap selector chose `none` but flagged a plausible missing-best-
candidate problem.

Rules:

- the packet includes the anchor card, a shortlist summary, and
  `retrieval_gap_diagnostics`
- the heavy pass may resolve to `NONE` or remain `UNSURE`
- it may resolve to `SAME_AS` or `RELATED_BUT_DISTINCT` only if the missing
  candidate is already present in the raw suppressed-candidate evidence and is
  promoted into the escalation packet explicitly
- it may not run a fresh open-ended search

Interpretation: `shortlist_gap_probe` is a bounded re-check of a suspicious
miss, not permission to reopen shortlist generation.

---

## Allowed extra evidence blocks

Only the blocks below may be added beyond `selector_harness_v1`.

| Block | Allowed contents | Max size | Why allowed |
| --- | --- | ---: | --- |
| `web_identity` | normalized domains, canonical website URLs, capped source URLs | `3` urls/domains per side | helps separate true producer identity from merchant/importer wrappers |
| `profile_snippets` | short excerpts from grower/importer profile text | `2` snippets per side, `280` chars each | useful when cheap fingerprint is too thin |
| `people_history` | founded year, winemaker names, first vintage, annual production | `4` structured facts per side | can resolve succession and continuity cases |
| `vineyard_profile` | capped location, soil, altitude, farming terms | `4` facts per side | useful only when estate profile coherence matters |
| `raw_supporting_rows` | small source-row excerpts with stable source ids | `6` excerpts total | lets the heavy pass cite grounded rows on hard cases |
| `retrieval_gap_diagnostics` | suppressed-candidate summaries, shortlist suppression reasons, raw-union notes | `4` items total | only for `shortlist_gap_probe` |

### Block limits

1. No long biographies.
2. No full source-table dumps.
3. No more than `3` source families per side in the heavy packet.
4. `raw_supporting_rows` must cite concrete source ids or row identifiers.
5. `retrieval_gap_diagnostics` may describe only already-observed shortlist or
   suppression behavior. It may not invent unseen candidates.

---

## Packet shape

The escalation packet wraps the selector packet rather than replacing it.

```json
{
  "escalation_version": "escalation_dossier_v1",
  "selector_result_ref": {
    "selector_version": "selector_harness_v1",
    "anchor_producer_id": "uuid",
    "prior_label": "UNSURE",
    "prior_choice_type": "candidate|none",
    "prior_selected_candidate_id": "uuid|null",
    "prior_reason_code": "string"
  },
  "escalation_mode": "candidate_frontier|shortlist_gap_probe",
  "anchor": {},
  "frontier_candidate": {},
  "shadow_rival_summary": {},
  "added_evidence": {
    "web_identity": {},
    "profile_snippets": [],
    "people_history": {},
    "vineyard_profile": {},
    "raw_supporting_rows": [],
    "retrieval_gap_diagnostics": []
  },
  "decision_options": {}
}
```

### Packet rules

1. `anchor` is always present.
2. `frontier_candidate` is required for `candidate_frontier`.
3. `shadow_rival_summary` is optional and capped to one rival.
4. `retrieval_gap_diagnostics` is allowed only for `shortlist_gap_probe`.
5. Every cited support or conflict path must resolve into the escalation packet.

---

## How escalation output differs from cheap-selector output

The **label set and choice rules do not change**. That is intentional.

The escalation output keeps the selector JSON contract and adds only the
minimum extra audit fields:

```json
{
  "decision_stage": "escalation",
  "resolved_from_prior_label": "UNSURE",
  "used_escalation_blocks": [
    "web_identity",
    "profile_snippets"
  ]
}
```

Everything else should stay aligned with `selector_harness_v1`:

- same `choice` object
- same four labels
- same `primary_reason_code` / `secondary_reason_codes`
- same support and conflict path behavior
- same `needs_escalation` semantics, except an escalation-stage `UNSURE` must
  set `needs_escalation = false`

Reason: downstream storage and scoring should depend on **what the final label
means**, not on whether it came from the cheap path or the heavy path.

---

## Heavy-pass stop rules

After one escalation pass:

- `SAME_AS` -> resolve and stop
- `RELATED_BUT_DISTINCT` -> resolve and stop
- `NONE` -> resolve and stop
- `UNSURE` -> stop and leave the case unresolved

No second escalation round is allowed in `v1`.

---

## What Session 10.6 should inherit

Session 10.6 should build escalation artifacts under these fixed rules:

1. implement only the two escalation modes above
2. allow only the listed escalation blocks
3. keep the heavy packet candidate-focused
4. do not add a new label or a second heavy pass
5. do not let escalation widen the shortlist beyond one frontier candidate plus
   one optional rival summary
6. treat `UNSURE` after escalation as a durable unresolved frontier case, not a
   soft accepted edge

---

## Bottom line

`escalation_dossier_v1` keeps Sprint 7 honest:

- escalate only real frontier cases
- add richer evidence without reopening retrieval
- resolve what can be resolved
- abstain after one heavy pass if the case is still not trustworthy
