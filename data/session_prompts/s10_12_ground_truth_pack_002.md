# Session 10.12 - ground truth pack 002

Read first:
- `data/sprints/identity-er/ground_truth_program_v1.md`
- `data/sprints/identity-er/ground_truth_pack_001_v1.jsonl`
- `data/sprints/identity-er/ground_truth_pack_001_summary_v1.md`
- `data/sprints/identity-er/ground_truth_seed_pairs_v1.jsonl`

Goal:
- build the second audited expansion pack toward the `1,000`-pair scoreable truth target

Hard constraints:
- no DB writes
- no benchmark reruns
- no new method search
- keep Pack 001 labels fixed unless a direct evidence contradiction is discovered
- use primary sources where possible
- quarantine disputed cases instead of forcing labels

Focus:
- additional `SAME_AS` recall families that still survive a strict evidence bar
- more non-`FR` `RELATED_BUT_DISTINCT` families
- more non-`FR` high-risk `NONE` precision traps
- avoid over-concentrating on one permit family or one country

Required output:
- `data/sprints/identity-er/ground_truth_pack_002_v1.jsonl`
- `data/sprints/identity-er/ground_truth_pack_002_summary_v1.md`
- update `ground_truth_program_v1.md` only if Pack 002 reveals a real target-shape mistake

Success condition:
- the sprint ends with a second audited, reusable expansion pack that clearly moves the truth base beyond Pack 001 without weakening the evidence bar or pretending unresolved history is settled.
