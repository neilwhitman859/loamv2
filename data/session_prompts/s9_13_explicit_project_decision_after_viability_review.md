# Session 9.13 - explicit project decision after viability review

Goal:
Make the founder-level decision that Session 9.12 surfaced: after freezing the
current producer-dedup line, should Loam continue under a narrower honest
scope, or should the project be shelved?

Primary deliverable:
- one short decision memo that:
  - records the chosen path
  - states why that path won
  - updates Sprint 6 / project posture accordingly
  - names the exact next roadmap step that is now allowed

In scope:
- `data/sprints/dedup/session9_12_high_level_next_steps_viability_review.md`
- `data/dashboard.html`
- `AGENTS.md`
- `docs/DECISIONS.md` if the user locks the decision
- `data/sprints/dedup/journal.md`
- `data/sessions.md`

Out of scope:
- new producer-dedup design work
- new reruns or proof subsets
- queue-building
- implementation planning for multiple branches at once

Budget:
- target actual spend: `$0`
- hard cap: `$0`

Decision options:
1. Freeze producer dedup and keep Loam alive only under a narrower, honest
   product scope review.
2. Shelve Loam and stop active work.
3. Reopen producer dedup only as a clearly new strategy with a new success
   argument and a fresh approval gate.

Recommendation:
- default to Option 1 unless the user now believes trustworthy producer dedup
  is so central that failing it means Loam should stop entirely.

Stop rule:
- log the project-level decision and stop
- do not drift from decision-making into new design or execution work
