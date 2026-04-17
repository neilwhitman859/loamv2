# Production Routing Agreement Matrix

Calibration pairs: 600

## Per-bucket counts + accuracy vs gold

| Bucket | Count | %   | Gold dist | Accuracy |
|---|---|---|---|---|
| auto_skip_4way | 235 | 39.2% | {'MERGE': 5, 'SKIP': 151} | 151/156 = 96.8% |
| user_review_pc | 167 | 27.8% | {'MERGE': 19, 'SKIP': 89, 'PARENT_CHILD': 9} | - |
| auto_apply_merge_4way | 97 | 16.2% | {'MERGE': 63} | 63/63 = 100.0% |
| auto_apply_merge_cross_family | 25 | 4.2% | {'MERGE': 12, 'SKIP': 1} | 12/13 = 92.3% |
| user_review_uncertain | 23 | 3.8% | {} | - |
| auto_skip_4way_lowconf | 21 | 3.5% | {'MERGE': 2, 'SKIP': 6} | 6/8 = 75.0% |
| auto_skip_cross_family | 14 | 2.3% | {'SKIP': 6, 'MERGE': 1} | 6/7 = 85.7% |
| l3_rigor_needed_lowconf | 10 | 1.7% | {'MERGE': 4} | - |
| auto_apply_merge_4way_lowconf | 5 | 0.8% | {'MERGE': 5} | 5/5 = 100.0% |
| l3_rigor_needed_disagreement | 3 | 0.5% | {'MERGE': 2} | - |

**Auto-handled (auto-merge + auto-skip at stage 1 or 2): 397/600 = 66.2%**
**User review required: 190/600 = 31.7%**

## Deep dive: auto-apply bucket accuracy (critical — zero-tolerance for FPs)

## User review breakdown

- **user_review_pc**: 167 pairs (gold dist: {'MERGE': 19, 'SKIP': 89, 'PARENT_CHILD': 9})
- **user_review_uncertain**: 23 pairs (gold dist: {})

## Scale projection to full 151K production corpus

Calibration is stratified + biased toward MERGE/PC. Extrapolation to the full
L1 corpus requires re-running L1.5 Gemini basic on all 151K pairs first.
Based on L1 Haiku batched distribution on all 151,120 pairs:
- MERGE: 2,606 (1.72%)
- PARENT_CHILD: 2,121 (1.40%)
- SKIP: 145,310 (96.16%)
- UNCERTAIN: 1,083 (0.72%)

Estimated production routing (ballpark pending L1.5 full run):
- Auto-apply MERGE: ~1,500-2,000 pairs (subset of L1 MERGE where Gemini basic also MERGE)
- Auto-skip: ~29,000 pairs (L1 SKIP >=0.97 intersected with Gemini SKIP >=0.97)
- Escalate to L2: ~20,000-40,000 pairs
- User review: 50-200 pairs (target)
