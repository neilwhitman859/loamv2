"""
B6.4 Phase H — Final agreement matrix producing production routing rules.

Combines all tier verdicts into ACTION buckets based on committed thresholds:
- auto_apply_merge: L1 Haiku MERGE >=0.80 AND L1.5 Gemini basic MERGE >=0.80
- auto_skip:       L1 Haiku SKIP >=0.97 AND L1.5 Gemini basic SKIP >=0.97
- escalate_l2:     anything else (PC, UNCERTAIN, low conf, disagreement)
- auto_apply_l2_merge: L2 Haiku rich MERGE >=0.90 AND L2.5 Gemini rich MERGE >=0.90
- escalate_l3:     L2/L2.5 PC, UNCERTAIN, disagreement
- auto_apply_l3_merge: L3 Sonnet+web MERGE >=0.92
- user_review:     L3 PC, UNCERTAIN, MERGE <0.92

Reports accuracy against gold per bucket.

Output: data/sprints/dedup/agreement_matrix.md
"""

from __future__ import annotations

import json
from collections import Counter, defaultdict
from pathlib import Path

CALIBRATION_PATH = Path(__file__).resolve().parents[2] / "data" / "sprints" / "dedup" / "calibration_set.json"
OUT_PATH = Path(__file__).resolve().parents[2] / "data" / "sprints" / "dedup" / "agreement_matrix.md"


def resolve_gold(p):
    return p.get("gold_verdict") or p.get("proxy_gold")


def classify_pair(p):
    """Apply the committed ladder logic; return (bucket, reason)."""
    l1_v, l1_c = p.get("l1_verdict"), p.get("l1_confidence") or 0
    g_v, g_c = p.get("l1_gemini_basic_verdict"), p.get("l1_gemini_basic_confidence") or 0
    l2_v, l2_c = p.get("l2_haiku_rich_verdict"), p.get("l2_haiku_rich_confidence") or 0
    l2g_v, l2g_c = p.get("l2_gemini_rich_verdict"), p.get("l2_gemini_rich_confidence") or 0

    # 4-way unanimous (strongest signal)
    all_verdicts = [l1_v, g_v, l2_v, l2g_v]
    if all(v == "MERGE" for v in all_verdicts):
        min_conf = min(l1_c, g_c, l2_c, l2g_c)
        if min_conf >= 0.85:
            return "auto_apply_merge_4way", "All 4 tiers MERGE >=0.85"
        return "auto_apply_merge_4way_lowconf", "All 4 tiers MERGE but some <0.85"
    if all(v == "SKIP" for v in all_verdicts):
        min_conf = min(l1_c, g_c, l2_c, l2g_c)
        if min_conf >= 0.90:
            return "auto_skip_4way", "All 4 tiers SKIP >=0.90"
        return "auto_skip_4way_lowconf", "All 4 tiers SKIP but some <0.90"

    # Cross-family consensus (Haiku + Gemini) — MORE reliable than same-family
    # MERGE: any Haiku-tier MERGE + any Gemini-tier MERGE at >=0.85 each
    haiku_says_merge = (l1_v == "MERGE" and l1_c >= 0.85) or (l2_v == "MERGE" and l2_c >= 0.85)
    gemini_says_merge = (g_v == "MERGE" and g_c >= 0.85) or (l2g_v == "MERGE" and l2g_c >= 0.85)
    if haiku_says_merge and gemini_says_merge:
        return "auto_apply_merge_cross_family", "Haiku+Gemini cross-family MERGE consensus >=0.85"

    # SKIP: both families say SKIP at >=0.95
    haiku_says_skip = (l1_v == "SKIP" and l1_c >= 0.95) or (l2_v == "SKIP" and l2_c >= 0.95)
    gemini_says_skip = (g_v == "SKIP" and g_c >= 0.95) or (l2g_v == "SKIP" and l2g_c >= 0.95)
    if haiku_says_skip and gemini_says_skip:
        return "auto_skip_cross_family", "Haiku+Gemini cross-family SKIP consensus >=0.95"

    # PC presence at any tier → user review (PC precision ~10% across all tiers)
    if "PARENT_CHILD" in all_verdicts:
        return "user_review_pc", "At least one tier said PARENT_CHILD (PC unreliable ~10% prec)"

    # UNCERTAIN at any tier
    if "UNCERTAIN" in all_verdicts:
        return "user_review_uncertain", "At least one tier UNCERTAIN"

    # Any cross-family disagreement
    if haiku_says_merge and gemini_says_skip:
        return "l3_rigor_needed_disagreement", "Haiku MERGE vs Gemini SKIP (FNR candidate)"
    if gemini_says_merge and haiku_says_skip:
        return "l3_rigor_needed_disagreement", "Gemini MERGE vs Haiku SKIP (FNR candidate)"

    # Low-confidence agreement — escalate to L3
    return "l3_rigor_needed_lowconf", "Agreed verdict but confidence below auto-accept thresholds"


def main():
    doc = json.loads(CALIBRATION_PATH.read_text(encoding="utf-8"))
    pairs = [p for tier, pp in doc["tiers"].items() for p in pp]

    buckets = defaultdict(list)
    for p in pairs:
        bucket, reason = classify_pair(p)
        buckets[bucket].append(p)

    lines = ["# Production Routing Agreement Matrix", ""]
    lines.append(f"Calibration pairs: {len(pairs)}")
    lines.append("")
    lines.append("## Per-bucket counts + accuracy vs gold")
    lines.append("")
    lines.append("| Bucket | Count | %   | Gold dist | Accuracy |")
    lines.append("|---|---|---|---|---|")
    total_auto = 0
    total_review = 0
    for bucket, items in sorted(buckets.items(), key=lambda x: -len(x[1])):
        n = len(items)
        with_gold = [p for p in items if resolve_gold(p)]
        gold_dist = Counter(resolve_gold(p) for p in with_gold)
        # Accuracy = how often the bucket-implied action matches gold
        if bucket.startswith("auto_apply_merge"):
            correct = sum(1 for p in with_gold if resolve_gold(p) == "MERGE")
            total_auto += n
        elif bucket.startswith("auto_skip"):
            correct = sum(1 for p in with_gold if resolve_gold(p) == "SKIP")
            total_auto += n
        elif bucket.startswith("user_review"):
            correct = "-"  # user decides, not auto
            total_review += n
        else:
            correct = "-"
        acc = f"{correct}/{len(with_gold)} = {100*correct/len(with_gold):.1f}%" if isinstance(correct, int) and with_gold else "-"
        pct = f"{100*n/len(pairs):.1f}%"
        lines.append(f"| {bucket} | {n} | {pct} | {dict(gold_dist)} | {acc} |")
    lines.append("")

    # Summary: what fraction of calibration set can be auto-handled with current rules?
    lines.append(f"**Auto-handled (auto-merge + auto-skip at stage 1 or 2): {total_auto}/{len(pairs)} = {100*total_auto/len(pairs):.1f}%**")
    lines.append(f"**User review required: {total_review}/{len(pairs)} = {100*total_review/len(pairs):.1f}%**")
    lines.append("")

    # Deep dive on auto-apply buckets
    lines.append("## Deep dive: auto-apply bucket accuracy (critical — zero-tolerance for FPs)")
    lines.append("")
    for bucket_name in ["auto_apply_merge_L1_x_L1_5", "auto_apply_merge_L2_x_L2_5",
                        "auto_skip_L1_x_L1_5", "auto_skip_L2_x_L2_5"]:
        items = buckets.get(bucket_name, [])
        with_gold = [p for p in items if resolve_gold(p)]
        if not with_gold:
            continue
        expected = "MERGE" if "merge" in bucket_name else "SKIP"
        correct = sum(1 for p in with_gold if resolve_gold(p) == expected)
        wrong = [p for p in with_gold if resolve_gold(p) != expected]
        lines.append(f"### {bucket_name}")
        lines.append(f"- Total: {len(items)} (gold-labeled: {len(with_gold)})")
        lines.append(f"- Expected action: {expected}")
        lines.append(f"- Correct: {correct}/{len(with_gold)} = {100*correct/len(with_gold):.1f}%")
        if wrong:
            lines.append(f"- **Wrong: {len(wrong)}** — these would be {'false merges' if expected=='MERGE' else 'false skips (missed MERGEs)'}")
            for p in wrong[:5]:
                lines.append(f"  - [{p['pair_id']}] {p['name_a']!r}/{p['name_b']!r} gold={resolve_gold(p)}")
        lines.append("")

    # Deep dive on user_review categories
    lines.append("## User review breakdown")
    lines.append("")
    for bucket_name in ["user_review_pc", "user_review_uncertain", "user_review_l1_l1_5_disagree",
                        "user_review_l2_l2_5_disagree", "needs_l3_or_review"]:
        items = buckets.get(bucket_name, [])
        if not items:
            continue
        with_gold = [p for p in items if resolve_gold(p)]
        gold_dist = Counter(resolve_gold(p) for p in with_gold)
        lines.append(f"- **{bucket_name}**: {len(items)} pairs (gold dist: {dict(gold_dist)})")
    lines.append("")

    # L1 verdict distribution at scale projection
    lines.append("## Scale projection to full 151K production corpus")
    lines.append("")
    lines.append("Calibration is stratified + biased toward MERGE/PC. Extrapolation to the full")
    lines.append("L1 corpus requires re-running L1.5 Gemini basic on all 151K pairs first.")
    lines.append("Based on L1 Haiku batched distribution on all 151,120 pairs:")
    lines.append("- MERGE: 2,606 (1.72%)")
    lines.append("- PARENT_CHILD: 2,121 (1.40%)")
    lines.append("- SKIP: 145,310 (96.16%)")
    lines.append("- UNCERTAIN: 1,083 (0.72%)")
    lines.append("")
    lines.append("Estimated production routing (ballpark pending L1.5 full run):")
    lines.append("- Auto-apply MERGE: ~1,500-2,000 pairs (subset of L1 MERGE where Gemini basic also MERGE)")
    lines.append("- Auto-skip: ~29,000 pairs (L1 SKIP >=0.97 intersected with Gemini SKIP >=0.97)")
    lines.append("- Escalate to L2: ~20,000-40,000 pairs")
    lines.append("- User review: 50-200 pairs (target)")
    lines.append("")

    OUT_PATH.write_text("\n".join(lines), encoding="utf-8")
    print(f"Wrote {OUT_PATH}")


if __name__ == "__main__":
    import sys
    sys.exit(main())
