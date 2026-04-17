"""
B6.4 cross-model agreement analysis.

For the calibration set, compute how often each pair of tiers agree, and
how precise the agreement is against gold. This is the core signal for
cross-model verification ladders (L1+L1.5, L2+L2.5, L1+L1.5+L2+L2.5+L3 unanimous).

Output: data/sprints/dedup/crossmodel_agreement.md
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import Counter, defaultdict
from itertools import combinations
from pathlib import Path

CALIBRATION_PATH = Path(__file__).resolve().parents[2] / "data" / "sprints" / "dedup" / "calibration_set.json"
OUT_PATH = Path(__file__).resolve().parents[2] / "data" / "sprints" / "dedup" / "crossmodel_agreement.md"


TIER_PREFIXES = [
    ("l1",               "L1 Haiku"),
    ("l1_gemini_basic",  "L1.5 Gemini basic"),
    ("l2_haiku_rich",    "L2 Haiku rich"),
    ("l2_gemini_rich",   "L2.5 Gemini rich"),
    ("l3_sonnet_web",    "L3 Sonnet+web"),
]


def resolve_gold(p):
    return p.get("gold_verdict") or p.get("proxy_gold")


def main():
    doc = json.loads(CALIBRATION_PATH.read_text(encoding="utf-8"))
    all_pairs = [p for tier, pairs in doc["tiers"].items() for p in pairs]

    # Determine which tiers have any data
    active = []
    for prefix, label in TIER_PREFIXES:
        n = sum(1 for p in all_pairs if p.get(f"{prefix}_verdict"))
        if n:
            active.append((prefix, label))
            print(f"  {label}: {n} pairs")

    lines = ["# Cross-model agreement matrix", ""]
    lines.append(f"Total calibration pairs: {len(all_pairs)}")
    lines.append(f"Tiers with data: {len(active)}")
    lines.append("")

    # Pair-wise agreement
    lines.append("## Pairwise verdict agreement")
    lines.append("")
    for (p1, l1), (p2, l2) in combinations(active, 2):
        with_both = [p for p in all_pairs if p.get(f"{p1}_verdict") and p.get(f"{p2}_verdict")]
        if not with_both:
            continue
        same = sum(1 for p in with_both if p[f"{p1}_verdict"] == p[f"{p2}_verdict"])
        total = len(with_both)
        lines.append(f"### {l1}  vs  {l2}")
        lines.append(f"- Overall agreement: {same}/{total} = {100*same/total:.1f}%")
        # Per-verdict agreement
        verdicts = ["MERGE", "PARENT_CHILD", "SKIP", "UNCERTAIN"]
        lines.append("")
        lines.append(f"| {l1} \\ {l2} |" + "|".join(f" {v} " for v in verdicts) + "| TOTAL |")
        lines.append("|---" * (len(verdicts) + 2) + "|")
        for v1 in verdicts:
            row = {v2: 0 for v2 in verdicts}
            for p in with_both:
                a = p[f"{p1}_verdict"]
                b = p[f"{p2}_verdict"]
                if a == v1:
                    row[b] += 1
            tot = sum(row.values())
            if tot:
                lines.append(f"| **{v1}** |" + "|".join(f" {row[v2]} " for v2 in verdicts) + f"| {tot} |")
        lines.append("")

        # Precision against gold for INTERSECTION verdicts where both agree
        with_gold = [p for p in with_both if resolve_gold(p)]
        if with_gold:
            lines.append("")
            lines.append("**Precision/recall of AGREEMENT against gold (both methods same verdict):**")
            lines.append("")
            lines.append(f"| joint verdict | N | accuracy vs gold | gold dist |")
            lines.append("|---|---|---|---|")
            for v in verdicts:
                agreed = [p for p in with_gold if p[f"{p1}_verdict"] == v and p[f"{p2}_verdict"] == v]
                if not agreed:
                    continue
                tp = sum(1 for p in agreed if resolve_gold(p) == v)
                dist = Counter(resolve_gold(p) for p in agreed)
                lines.append(f"| both={v} | {len(agreed)} | {tp}/{len(agreed)} = {100*tp/len(agreed):.1f}% | {dict(dist)} |")
            lines.append("")

        # Cases where methods disagree - who's right?
        lines.append("")
        lines.append("**When methods DISAGREE, who wins? (gold-labeled pairs only):**")
        lines.append("")
        disagreements = [p for p in with_gold if p[f"{p1}_verdict"] != p[f"{p2}_verdict"]]
        if disagreements:
            p1_wins = sum(1 for p in disagreements if resolve_gold(p) == p[f"{p1}_verdict"])
            p2_wins = sum(1 for p in disagreements if resolve_gold(p) == p[f"{p2}_verdict"])
            neither = len(disagreements) - p1_wins - p2_wins
            lines.append(f"- Total disagreements: {len(disagreements)}")
            lines.append(f"- {l1} matches gold: {p1_wins}/{len(disagreements)} = {100*p1_wins/len(disagreements):.1f}%")
            lines.append(f"- {l2} matches gold: {p2_wins}/{len(disagreements)} = {100*p2_wins/len(disagreements):.1f}%")
            lines.append(f"- Neither matches gold: {neither}/{len(disagreements)}")
        lines.append("")
        lines.append("---")
        lines.append("")

    # N-way unanimous agreement stats
    if len(active) >= 3:
        lines.append("## Unanimous agreement among N tiers (on pairs with data from all tiers)")
        lines.append("")
        with_all = [p for p in all_pairs if all(p.get(f"{pfx}_verdict") for pfx, _ in active)]
        with_gold = [p for p in with_all if resolve_gold(p)]
        lines.append(f"Pairs with data from all {len(active)} tiers: {len(with_all)}")
        lines.append(f"  of which gold-labeled: {len(with_gold)}")
        lines.append("")

        if with_all:
            unanimous = [p for p in with_all
                         if len(set(p.get(f"{pfx}_verdict") for pfx, _ in active)) == 1]
            lines.append(f"All {len(active)} tiers unanimous: {len(unanimous)} pairs ({100*len(unanimous)/len(with_all):.1f}%)")
            if unanimous:
                verdict_counts = Counter(p[f"{active[0][0]}_verdict"] for p in unanimous)
                lines.append(f"  Distribution: {dict(verdict_counts)}")

            # Precision of unanimous against gold
            u_with_gold = [p for p in unanimous if resolve_gold(p)]
            if u_with_gold:
                correct = sum(1 for p in u_with_gold if resolve_gold(p) == p[f"{active[0][0]}_verdict"])
                lines.append(f"  Unanimous precision vs gold: {correct}/{len(u_with_gold)} = {100*correct/len(u_with_gold):.1f}%")
        lines.append("")

    OUT_PATH.write_text("\n".join(lines), encoding="utf-8")
    print(f"Wrote {OUT_PATH}")


if __name__ == "__main__":
    sys.exit(main())
