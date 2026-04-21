# Sprint 7: Producer Identity ER — Identity Dossier Select

**Status:** Active. Opened 2026-04-21.
**Sprint number:** 7
**Shape:** Design-first rebuild. No producer merges are applied during the launch phase.

---

## Goal

Replace Sprint 6's pairwise merge-judge framing with a producer-identity system
that can work **without human pair review at scale** while remaining
conservative enough for Loam's trust bar.

This sprint is not trying to rescue the old path with one more prompt. It is
trying to prove whether a materially different method family is viable.

---

## Hard constraints

1. **No human pair review at scale.** Review queues for the user are not a
   viable dependency.
2. **Fixed budget.** Spend is allowed only where it buys a new method, not more
   repetitions of the old one.
3. **Calendar time is available; user time is not.** Slow, careful offline
   passes are acceptable; labor-heavy supervision is not.
4. **Abstention beats a false merge.** The system may leave duplicates
   unresolved if the evidence object is not strong enough.

---

## Method name

- Human-readable: **Identity Dossier Select**
- Machine label: **`identity_dossier_select_v1`**

Why this name:
- **Identity** because the core problem is producer identity, not just fuzzy
  string matching.
- **Dossier** because the model should reason over a structured producer packet,
  not a raw pair in isolation.
- **Select** because the primary decision should be "best candidate or none,"
  not only pairwise yes/no.

---

## Method shape

### 1. Producer dossier builder

Build a reusable structured dossier for each producer candidate:
- names, aliases, normalized forms
- location signals
- website / domain signals
- importer / parent / relationship clues
- recurring wine-line / appellation / style patterns
- provenance and conflicts across sources

### 2. Candidate shortlist generation

Use cheap blocking / retrieval to build a **small shortlist** for each producer
instead of a large free-floating pair universe.

### 3. Select-best-or-none adjudication

The model sees one anchor selector card plus a shortlist of candidate
mini-cards and must first choose **one candidate or `none`**, then apply:
- `SAME_AS`
- `RELATED_BUT_DISTINCT`
- `NONE`
- `UNSURE`

### 4. AI-only escalation

Only the uncertainty frontier gets a heavier pass with richer evidence. If the
heavy pass is still unsure, the system does not merge.

### 5. Accepted-edge graph

Store:
- accepted positive identity edges
- explicit negative edges
- related-but-distinct edges

Clusters come **after** accepted `SAME_AS` edges. No optimistic transitivity.

---

## What Sprint 7 is trying to prove

1. A dossier object is materially more informative than the Sprint 6 pair
   packet.
2. Shortlist selection is safer and more useful than isolated pairwise judging.
3. Explicit `RELATED_BUT_DISTINCT` handling reduces false merges on adjacent
   brand / importer / family / holdco cases.
4. AI-only escalation can stay small enough to fit the fixed budget.

---

## What this sprint does not include

- Human review queues
- Full producer-merge execution
- Full-corpus rollout before a bounded proof
- Wine dedup (deferred to Sprint 8)
- Prompt-v2 / re-enrichment work (deferred to Sprint 9)

---

## Session plan

| Session | Focus | Durable output |
|---|---|---|
| 10.1 | Sprint launch + method naming | Sprint scaffold, roadmap updates, next prompt |
| 10.2 | Producer dossier schema + candidate inventory | `producer_dossier_v1.md` + `source_signal_inventory.md` + `edge_taxonomy_v1.md` |
| 10.3 | Shortlist generation design | `shortlist_generation_v1.md` + blocking / retrieval plan |
| 10.4 | Selector contract + evaluation harness | `selector_harness_v1.md` + prompt / schema / scorer design |
| 10.5 | Control layer design | `escalation_dossier_v1.md` + `accepted_edge_rules_v1.md` + `selector_proof_v1.md` |
| 10.6 | Bounded proof build | frozen proof bundle + scorer + edge-write simulator |
| 10.7 | Bounded proof execution + go / no-go decision | scored proof results + continuation memo |

---

## Success criteria

- [x] `producer_dossier_v1` exists and is small enough to build cheaply
- [x] shortlist design can produce manageable candidate sets
- [x] selector contract cleanly represents `RELATED_BUT_DISTINCT`
- [x] escalation path is bounded and AI-only
- [x] accepted-edge rules prevent blind transitive merges
- [ ] bounded proof shows a credible safety / usefulness improvement over raw
      pairwise judging

## Latest Result

Session 10.7 ran the first real bounded proof on `claude-sonnet-4-6` at
`$2.08` and returned `NO_GO`. Phase A missed `7 / 16` `SAME_AS`, missed all
`12 / 12` `RELATED_BUT_DISTINCT`, and resolved all `8` frontier cheap-path
`UNSURE` cases to non-`UNSURE` labels. Phase B recovered only `4 / 6`
resolvable frontier cases, reopened `1` false `RELATED_BUT_DISTINCT`, and
still resolved the `2` expected-`UNSURE` shortlist-gap probes to `NONE`. Phase
C remains blocked because no honest reusable shortlist-builder implementation
exists yet, and Phase D reopened `4` contradictory overwrite attempts when
escalation tried to recover cases the selector had already written as `NONE`.
Sprint 7 implementation work is therefore blocked pending an explicit user
decision on failure analysis versus freeze.

---

## Budget posture

**Phase 1 ceiling: $20.00**

Interpretation:
- launch + design work should cost `$0`
- first proof work must stay cheap and bounded
- do not expand the sprint budget unless the new method clears an honest proof

---

## Sprint sequence after launch

```
Sprint 6: pairwise producer dedup rebuild (closed, preserved as benchmark)
Sprint 7: producer identity ER (this sprint)
Sprint 8: wine dedup
Sprint 9: prompt v2 + L3 fact-check gate + re-enrichment
```
