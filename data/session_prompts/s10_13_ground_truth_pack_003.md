# Session 10.13 - ground truth pack 003

Read first:
- `data/sprints/identity-er/ground_truth_program_v1.md`
- `data/sprints/identity-er/ground_truth_pack_001_v1.jsonl`
- `data/sprints/identity-er/ground_truth_pack_001_summary_v1.md`
- `data/sprints/identity-er/ground_truth_pack_002_v1.jsonl`
- `data/sprints/identity-er/ground_truth_pack_002_summary_v1.md`
- `data/sprints/identity-er/ground_truth_seed_pairs_v1.jsonl`

Goal:
- build the third audited expansion pack toward the `1,000`-pair scoreable truth target

Hard constraints:
- no DB writes
- no benchmark reruns
- no new method search
- keep Pack 001 and Pack 002 labels fixed unless a direct evidence contradiction is discovered
- use primary sources where possible
- quarantine disputed cases instead of forcing labels

Focus:
- non-`FR` high-similarity `NONE` traps from `US`, `IT`, `ES`, `PT`, `AU`, and cross-country rows
- better mid / tail balance; avoid another pack dominated by easy core merchant-house continuity cases
- additional `RELATED_BUT_DISTINCT` only when first-party lineage, estate, or collaboration evidence is explicit
- prefer candidate families not already overrepresented in Pack 001 / Pack 002

Required output:
- `data/sprints/identity-er/ground_truth_pack_003_v1.jsonl`
- `data/sprints/identity-er/ground_truth_pack_003_summary_v1.md`
- a reproducible Pack 003 builder (new or cleanly extended) that preserves the current strict evidence bar

Success condition:
- the truth corpus grows again without weakening the evidence bar, while improving country and tier balance instead of just repeating the easiest core cases.
