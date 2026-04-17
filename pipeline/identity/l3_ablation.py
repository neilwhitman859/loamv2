"""
B6.4 Phase G — L3 web vs no-web ablation comparison.

For the subset of pairs that have BOTH an L3-web label (from oracle, stored as
gold_verdict with gold_source='sonnet_4_6_web_search_20250305') AND an L3-no-web
label (from producer_dedup_pairs with method_name='l3_sonnet_noweb_ablation_calibration'),
compare verdicts and report web's marginal contribution.

Output: data/sprints/dedup/l3_ablation.md
"""

import json
from collections import Counter, defaultdict
from pathlib import Path

from pipeline.lib.db import get_conn

CALIBRATION_PATH = Path(__file__).resolve().parents[2] / "data" / "sprints" / "dedup" / "calibration_set.json"
OUT_PATH = Path(__file__).resolve().parents[2] / "data" / "sprints" / "dedup" / "l3_ablation.md"


def main():
    doc = json.loads(CALIBRATION_PATH.read_text(encoding="utf-8"))

    conn = get_conn()
    cur = conn.cursor()

    # Pull no-web verdicts
    cur.execute("""
        SELECT producer_id_a, producer_id_b, verdict, confidence, reasoning
        FROM producer_dedup_pairs
        WHERE method_name = 'l3_sonnet_noweb_ablation_calibration'
    """)
    noweb = {}
    for pa, pb, v, c, r in cur.fetchall():
        noweb[(str(pa), str(pb))] = (v, float(c) if c else 0, r)

    # Match to calibration set pairs with oracle gold
    pairs_with_both = []
    for tier, pairs in doc["tiers"].items():
        for p in pairs:
            key = (p["producer_id_a"], p["producer_id_b"])
            if not p.get("gold_verdict"):
                continue
            if p.get("gold_source") != "sonnet_4_6_web_search_20250305":
                continue
            if key not in noweb:
                continue
            pairs_with_both.append((p, noweb[key]))

    lines = ["# L3 Sonnet 4.6: web vs no-web ablation", ""]
    lines.append(f"Pairs with both labels: {len(pairs_with_both)}")
    lines.append("")

    if not pairs_with_both:
        lines.append("No pairs have both labels. Skip.")
        OUT_PATH.write_text("\n".join(lines), encoding="utf-8")
        print(f"Wrote {OUT_PATH}")
        return 0

    # Agreement
    agree = sum(1 for p, (nv, nc, nr) in pairs_with_both if p["gold_verdict"] == nv)
    total = len(pairs_with_both)
    lines.append(f"## Verdict agreement (web vs no-web)")
    lines.append("")
    lines.append(f"Agreement: {agree}/{total} = {100*agree/total:.1f}%")
    lines.append("")

    # Confusion matrix
    cm = defaultdict(lambda: defaultdict(int))
    for p, (nv, nc, nr) in pairs_with_both:
        cm[p["gold_verdict"]][nv] += 1
    verdicts = ["MERGE", "PARENT_CHILD", "SKIP", "UNCERTAIN"]
    lines.append("| web \\ no-web |" + "|".join(f" {v} " for v in verdicts) + "|")
    lines.append("|---" * (len(verdicts) + 1) + "|")
    for v in verdicts:
        if sum(cm[v].values()) == 0:
            continue
        lines.append(f"| **{v}** |" + "|".join(f" {cm[v][v2]} " for v2 in verdicts) + "|")
    lines.append("")

    # Cases where web ≠ no-web: cost of dropping web
    disagreements = [(p, n) for p, n in pairs_with_both if p["gold_verdict"] != n[0]]
    lines.append(f"## Disagreements ({len(disagreements)} pairs)")
    lines.append("")
    if disagreements:
        lines.append("| name_a | name_b | web verdict (gold) | no-web verdict | no-web conf |")
        lines.append("|---|---|---|---|---|")
        for p, (nv, nc, nr) in disagreements[:20]:
            lines.append(f"| {p['name_a']!r} | {p['name_b']!r} | {p['gold_verdict']} | {nv} | {nc:.2f} |")
        if len(disagreements) > 20:
            lines.append(f"*{len(disagreements) - 20} more not shown*")
        lines.append("")

        # Verdict-pattern analysis
        lines.append("### Disagreement patterns")
        lines.append("")
        patterns = Counter()
        for p, (nv, nc, nr) in disagreements:
            patterns[f"web={p['gold_verdict']} → no-web={nv}"] += 1
        for pattern, n in patterns.most_common():
            lines.append(f"- {pattern}: {n} pairs")
        lines.append("")

    # Confidence distributions for both
    lines.append("## Confidence distribution")
    lines.append("")
    lines.append("Web:")
    web_conf = Counter()
    for p, _ in pairs_with_both:
        c = p.get("gold_confidence") or 0
        bucket = ">=0.90" if c >= 0.90 else "0.70-0.90" if c >= 0.70 else "<0.70"
        web_conf[bucket] += 1
    for b, n in web_conf.most_common():
        lines.append(f"- {b}: {n}")
    lines.append("")
    lines.append("No-web:")
    noweb_conf = Counter()
    for _, (nv, nc, _) in pairs_with_both:
        bucket = ">=0.90" if nc >= 0.90 else "0.70-0.90" if nc >= 0.70 else "<0.70"
        noweb_conf[bucket] += 1
    for b, n in noweb_conf.most_common():
        lines.append(f"- {b}: {n}")
    lines.append("")

    # Web's marginal value
    lines.append("## Interpretation")
    lines.append("")
    delta = 100 * (total - agree) / total
    lines.append(f"**Web search changed the verdict on {total-agree}/{total} ({delta:.1f}%) of ablation pairs.**")
    lines.append("")
    if delta >= 10:
        lines.append("→ **Web adds meaningful signal.** Keep web at L3 rigor tier.")
    elif delta >= 5:
        lines.append("→ Web adds marginal signal. Cost-benefit analysis needed. Keep web at L3 but consider dropping at L2.")
    else:
        lines.append("→ Web adds minimal signal (<5%). Consider dropping web at L3 (saves ~$0.14/pair).")
    lines.append("")
    lines.append("Cost per pair:")
    lines.append("- L3 with web (oracle): ~$0.147 / pair average")
    lines.append("- L3 no-web: ~$0.012 / pair average (~92% cheaper)")

    OUT_PATH.write_text("\n".join(lines), encoding="utf-8")
    print(f"Wrote {OUT_PATH}")

    cur.close(); conn.close()
    return 0


if __name__ == "__main__":
    import sys
    sys.exit(main())
