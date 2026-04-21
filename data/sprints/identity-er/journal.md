# Sprint 7: Producer Identity ER — journal

**Opened:** 2026-04-21
**Status:** Active
**Current block:** Session 10.3 resolved shortlist-generation scope. The next
honest step is Session 10.4: design the selector harness against the now-fixed
`producer_dossier_v1` + `shortlist_generation_v1` contracts.

---

## Block log

- **Session 10.1 (2026-04-21):** launched Sprint 7 as the new active producer
  identity effort.

  **Scope held:** no implementation of dossier builders, no benchmark reruns,
  no queue-building, no merge execution, and no human-review workflow.

  **Decisions locked at launch:**
  - no human pair review at scale
  - fixed budget
  - AI-only escalation allowed only on a narrow frontier
  - abstention is better than a false merge
  - the new live method name is `identity_dossier_select_v1` ("Identity
    Dossier Select")

  **Why the sprint exists:** Sprint 6 proved the pairwise merge-only framing is
  useful as benchmark context but not trustworthy enough as the live path.
  Sprint 7 therefore starts from reusable producer dossiers, shortlist
  selection, explicit relationship typing, and accepted-edge-first merging.

- **Session 10.2 (2026-04-21):** turned Sprint 7's method name into a usable
  spec package.

  **Scope held:** no dossier-builder implementation, no shortlist code, no
  benchmark reruns, no queue-building, and no merge execution.

  **Artifacts produced:**
  - `producer_dossier_v1.md`
  - `source_signal_inventory.md`
  - `edge_taxonomy_v1.md`

  **Decisions locked in the spec:**
  - the cheap dossier is backbone-first and regulatory-aware, not website-first
  - the cheap path should rely on names, place, portfolio shape, and US
    regulatory/market clues because canonical producer relationship/history
    tables are still effectively empty
  - website/profile/history evidence is reserved for escalation only
  - the selector label set is fixed at `SAME_AS`, `RELATED_BUT_DISTINCT`,
    `NONE`, `UNSURE`
  - only accepted `SAME_AS` edges are merge-eligible; `RELATED_BUT_DISTINCT`
    is a first-class stored non-merge edge

  **Why this matters:** Sprint 7 can now design shortlist generation against a
  stable evidence object instead of re-litigating what the model should see on
  every later session.

- **Session 10.2a (2026-04-21):** tightened the handoff contract for any
  autonomous continuation.

  **Scope held:** no shortlist design, no selector work, no implementation,
  no benchmark reruns, and no merge execution.

  **Artifacts produced:**
  - explicit `Autonomous continuation: yes/no` protocol in `data/sessions.md`
  - matching repo-wide rule in `AGENTS.md`
  - heartbeat prompt hardened to require the explicit field

  **Decisions locked in the protocol:**
  - automation should advance only when the latest **Done** session explicitly
    says `Autonomous continuation: yes`
  - missing or ambiguous autonomy language counts as blocked
  - the current Sprint 7 handoff remains blocked until the shortlist
    seed-source decision is explicitly cleared

  **Why this matters:** the heartbeat can now follow a narrow machine-readable
  gate instead of trying to infer permission from narrative wrap-up prose.

- **Session 10.3 (2026-04-21):** turned Sprint 7's shortlist idea into a fixed
  retrieval contract.

  **Scope held:** no shortlist-builder implementation, no selector prompt
  design, no benchmark proof, no queue-building, and no merge execution.

  **Artifacts produced:**
  - `shortlist_generation_v1.md`
  - `s10_4_selector_harness_design.md`

  **Decisions locked in the spec:**
  - only Tier A sources may seed candidate IDs in `v1`: canonical lexical,
    LWIN producer names, TTB brand/applicant names, and state-registration
    brand/legal names
  - importer, retailer, merchant, profile, website, and history sources are
    re-rank-only or escalation-only, not seed generators
  - the raw candidate union is capped at `24` unique candidates and the final
    selector shortlist is capped at `12`
  - the suppression layer must proactively drop weak fuzzy hits, shared-surname
    / first-word collisions, permit-only adjacency, and merchant/importer
    context-only candidates before selector work
  - empty shortlist is a valid `v1` outcome; safe abstention beats a forced
    noisy candidate list

  **Why this matters:** Session 10.2a's autonomy blocker is now cleared by an
  explicit shortlist-source decision, and Session 10.4 can design the selector
  against a stable candidate universe instead of reopening retrieval scope.
