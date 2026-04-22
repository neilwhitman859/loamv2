# Session 10.11 - ground truth pack 001

Use the new Sprint 7 ground-truth frame, not the old "find another promising method" frame.

Read first:
- `data/sprints/identity-er/ground_truth_program_v1.md`
- `data/sprints/identity-er/ground_truth_seed_summary_v1.md`
- `data/sprints/identity-er/ground_truth_seed_pairs_v1.jsonl`
- `data/sprints/identity-er/method_bakeoff/session10_9_web_validation_ledger_v1.md`

Goal:
- build the first audited expansion pack toward the 1,000-pair scoreable truth target

Hard constraints:
- no DB writes
- no benchmark reruns
- no new method search
- use primary sources where possible
- quarantine disputed cases instead of forcing labels

Required output:
- `data/sprints/identity-er/ground_truth_pack_001_v1.jsonl`
- `data/sprints/identity-er/ground_truth_pack_001_summary_v1.md`
- update `ground_truth_program_v1.md` only if the pack reveals a real target-shape mistake

Pack 001 target:
- 150 audited pair records if possible
- prioritize:
  - challenged benchmark-truth families and close analogues
  - underrepresented `RELATED_BUT_DISTINCT` families:
    - `11.4.g`
    - `11.4.j`
    - `11.4.m`
    - `11.4.o`
    - `11.4.s`
  - non-`FR` high-risk precision traps

For every record include:
- pair ids or producer ids
- normalized truth label (`SAME_AS`, `RELATED_BUT_DISTINCT`, `NONE`, or quarantined dispute)
- source evidence pointer(s)
- short rationale
- family/tier/country tags

Success condition:
- the sprint ends with one audited, reusable expansion pack that can be added to the seed manifest without reopening policy or pretending disputed history is settled.
