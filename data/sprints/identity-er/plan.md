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
| 10.8 | Broad autonomous method bakeoff | ranked method ledger + strongest surviving bounded-proof artifact under the frozen production gate |
| 10.9 | Independent web validation | outside-benchmark validation ledger + recommendation memo for the benchmark-clearing stack |
| 10.10 | Benchmark truth repair | repaired truth ledger for challenged late overlay wins + keep/narrow/remove recommendation |

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

Session 10.8's broad bakeoff found a benchmark-clearing survivor:
`hybrid_guarded_fils_person_alias_v1` reached `0` false merges / `0` hard
misses / `0` soft misses on the frozen benchmark after a guarded base plus
three zero-cost overlays. Session 10.9 then pressure-tested that stack against
fresh web evidence and live read-only canonical snapshots and withdrew it as
the trusted recommendation. `place_alias` held up, but the `maison_alias`
benchmark win around `Ardhuy Cabotte` / `de la Cabotte` is now challenged by
outside evidence, and `fils_person_alias` remains unresolved rather than
independently validated. Sprint 7 is therefore blocked on truth repair, not on
finding yet another benchmark survivor.

## Current Direction

The broad bakeoff itself is complete. The next honest move is a focused
benchmark-truth and entity-history repair pass on the challenged late overlay
wins that turned the stack from "good guarded survivor" into "perfect on the
frozen benchmark." Keep `benchmark_v1`, the Session 4 production/fallback
gates, `no DB writes`, `no human pair review at scale`, and the rule that
abstention beats a false merge fixed. Do not reopen method search until the
challenged truth cases are either repaired or formally demoted.

---

## Budget posture

**Phase 1 ceiling: $30.00**

Interpretation:
- launch + design work should cost `$0`
- first proof work must stay cheap and bounded
- the user-authorized broad bakeoff + validation work now sits within a hard
  `$30` ceiling
- do not expand the sprint budget unless the post-repair path earns it

---

## Sprint sequence after launch

```
Sprint 6: pairwise producer dedup rebuild (closed, preserved as benchmark)
Sprint 7: producer identity ER (this sprint)
Sprint 8: wine dedup
Sprint 9: prompt v2 + L3 fact-check gate + re-enrichment
```
