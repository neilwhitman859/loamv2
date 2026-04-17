"""
B6.4 calibration analysis — compute accuracy per confidence bucket.

Post-processes data/sprints/dedup/calibration_set.json to answer:
- For a given tier (l1, l1_gemini, l2, l2_gemini), how accurate is each confidence
  bucket when measured against the gold label?
- What threshold maximizes MERGE precision AND PARENT_CHILD precision jointly?
- What's the ROC-style precision-recall trade-off?

The "gold" column is the merger of:
- Tier proxy gold (T1=MERGE, T2=SKIP)
- L3 oracle gold for pairs whose gold_verdict is populated.

Output: markdown report to data/sprints/dedup/<tier>_calibration.md

Run:
    python -m pipeline.identity.calibration_analysis --tier l1
    python -m pipeline.identity.calibration_analysis --tier l1_gemini_basic
    python -m pipeline.identity.calibration_analysis --tier l2_haiku_rich
    python -m pipeline.identity.calibration_analysis --tier l2_gemini_rich
    python -m pipeline.identity.calibration_analysis --all  # one report per tier
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import Counter, defaultdict
from pathlib import Path

CALIBRATION_PATH = Path(__file__).resolve().parents[2] / "data" / "sprints" / "dedup" / "calibration_set.json"
OUT_DIR = Path(__file__).resolve().parents[2] / "data" / "sprints" / "dedup"


def resolve_gold(pair: dict) -> str | None:
    """Merge proxy-gold (T1/T2) with oracle-gold (T3/T4/T5)."""
    if pair.get("gold_verdict"):
        return pair["gold_verdict"]
    return pair.get("proxy_gold")


def tier_key_for(tier_name: str) -> tuple[str, str, str]:
    """For a tier name like 'l1', return the keys used in calibration pairs.

    Returns (verdict_key, confidence_key, display_name).
    """
    mapping = {
        "l1":               ("l1_verdict",               "l1_confidence",               "L1 Haiku batched"),
        "l1_gemini_basic":  ("l1_gemini_basic_verdict",  "l1_gemini_basic_confidence",  "L1.5 Gemini basic"),
        "l2_haiku_rich":    ("l2_haiku_rich_verdict",    "l2_haiku_rich_confidence",    "L2 Haiku rich"),
        "l2_gemini_rich":   ("l2_gemini_rich_verdict",   "l2_gemini_rich_confidence",   "L2.5 Gemini rich"),
        "l3_sonnet_web":    ("l3_sonnet_web_verdict",    "l3_sonnet_web_confidence",    "L3 Sonnet+web"),
    }
    if tier_name not in mapping:
        raise ValueError(f"Unknown tier {tier_name!r}; known: {sorted(mapping)}")
    return mapping[tier_name]


def analyze(doc: dict, tier_name: str) -> str:
    """Return markdown report comparing tier predictions against gold."""
    vkey, ckey, display = tier_key_for(tier_name)

    rows = []
    for tier, pairs in doc["tiers"].items():
        for p in pairs:
            gold = resolve_gold(p)
            if not gold:
                continue
            verdict = p.get(vkey)
            conf = p.get(ckey)
            if not verdict or conf is None:
                continue
            rows.append({
                "pair_id": p["pair_id"],
                "tier": tier,
                "name_a": p["name_a"],
                "name_b": p["name_b"],
                "gold": gold,
                "pred": verdict,
                "conf": float(conf),
                "gold_source": "oracle" if p.get("gold_verdict") else "proxy",
            })

    n_total = len(rows)
    if n_total == 0:
        return f"# {display} calibration\n\nNo rows with both gold and {tier_name} predictions.\n"

    # Confusion matrix
    confusion = defaultdict(lambda: defaultdict(int))
    for r in rows:
        confusion[r["gold"]][r["pred"]] += 1

    verdicts = ["MERGE", "PARENT_CHILD", "SKIP", "UNCERTAIN"]

    lines = [f"# {display} calibration against gold labels", ""]
    lines.append(f"Total labeled pairs: {n_total}")
    lines.append(f"Gold-label source: {Counter(r['gold_source'] for r in rows).most_common()}")
    lines.append("")

    # Confusion matrix block
    lines.append("## Confusion matrix (rows=gold, cols=pred)")
    lines.append("")
    header = "| gold \\ pred |" + "|".join(f" {v} " for v in verdicts) + "| TOTAL |"
    sep = "|---" + "|---" * len(verdicts) + "|---|"
    lines.append(header)
    lines.append(sep)
    for gv in verdicts:
        row_total = sum(confusion[gv].values())
        line = f"| **{gv}** |" + "|".join(f" {confusion[gv][pv]} " for pv in verdicts) + f"| {row_total} |"
        lines.append(line)
    lines.append("")

    # Overall accuracy
    correct = sum(1 for r in rows if r["gold"] == r["pred"])
    lines.append(f"**Overall accuracy**: {correct}/{n_total} = {100*correct/n_total:.1f}%")
    lines.append("")

    # Per-verdict precision/recall
    lines.append("## Per-verdict precision + recall")
    lines.append("")
    lines.append("| verdict | N-gold | N-pred | TP | precision | recall | F1 |")
    lines.append("|---|---|---|---|---|---|---|")
    for v in verdicts:
        n_gold = sum(confusion[gv].get(v, 0) for gv in verdicts)  # count of pred=v
        n_pred = n_gold  # same summary
        n_gold_actual = sum(confusion[v].values())  # rows where gold=v
        n_pred_actual = sum(confusion[gv].get(v, 0) for gv in verdicts)  # rows where pred=v
        tp = confusion[v].get(v, 0)
        prec = tp / max(1, n_pred_actual)
        rec = tp / max(1, n_gold_actual)
        f1 = 2 * prec * rec / max(1e-9, prec + rec)
        lines.append(f"| {v} | {n_gold_actual} | {n_pred_actual} | {tp} | {prec:.3f} | {rec:.3f} | {f1:.3f} |")
    lines.append("")

    # Confidence bucket analysis — for auto-accept threshold
    lines.append("## Accuracy by predicted-verdict and confidence bucket")
    lines.append("")
    lines.append("For each (predicted verdict × confidence bucket), how often does pred == gold?")
    lines.append("This drives auto-accept thresholds: if MERGE@[0.95+] is 98%+ accurate, it's safe to auto-apply.")
    lines.append("")
    buckets = [
        (0.97, 1.01, ">=0.97"),
        (0.92, 0.97, "0.92-0.97"),
        (0.85, 0.92, "0.85-0.92"),
        (0.75, 0.85, "0.75-0.85"),
        (0.00, 0.75, "<0.75"),
    ]
    for pred in verdicts:
        lines.append(f"### Predicted = **{pred}**")
        lines.append("")
        lines.append("| bucket | N | accuracy | agree vs gold |")
        lines.append("|---|---|---|---|")
        for lo, hi, label in buckets:
            subset = [r for r in rows if r["pred"] == pred and lo <= r["conf"] < hi]
            if not subset:
                continue
            tp = sum(1 for r in subset if r["gold"] == r["pred"])
            gold_dist = Counter(r["gold"] for r in subset)
            lines.append(f"| {label} | {len(subset)} | {tp}/{len(subset)} = {100*tp/len(subset):.1f}% | {dict(gold_dist)} |")
        lines.append("")

    # Sweeping MERGE threshold to find auto-accept cutoff
    lines.append("## MERGE auto-accept threshold sweep")
    lines.append("")
    lines.append("If we auto-accept pred=MERGE at confidence >= T, what's the precision and recall?")
    lines.append("")
    lines.append("| threshold | N pred-MERGE | precision | recall vs gold-MERGE |")
    lines.append("|---|---|---|---|")
    gold_merge = sum(1 for r in rows if r["gold"] == "MERGE")
    for T in [0.97, 0.95, 0.92, 0.90, 0.87, 0.85, 0.80]:
        merge_preds = [r for r in rows if r["pred"] == "MERGE" and r["conf"] >= T]
        if not merge_preds:
            continue
        tp = sum(1 for r in merge_preds if r["gold"] == "MERGE")
        prec = tp / max(1, len(merge_preds))
        rec = tp / max(1, gold_merge)
        lines.append(f"| >= {T:.2f} | {len(merge_preds)} | {tp}/{len(merge_preds)} = {100*prec:.1f}% | {tp}/{gold_merge} = {100*rec:.1f}% |")
    lines.append("")

    # PARENT_CHILD threshold sweep
    lines.append("## PARENT_CHILD auto-accept threshold sweep")
    lines.append("")
    lines.append("| threshold | N pred-PC | precision | recall vs gold-PC |")
    lines.append("|---|---|---|---|")
    gold_pc = sum(1 for r in rows if r["gold"] == "PARENT_CHILD")
    for T in [0.97, 0.95, 0.92, 0.90, 0.87, 0.85, 0.80]:
        preds = [r for r in rows if r["pred"] == "PARENT_CHILD" and r["conf"] >= T]
        if not preds:
            continue
        tp = sum(1 for r in preds if r["gold"] == "PARENT_CHILD")
        prec = tp / max(1, len(preds))
        rec = tp / max(1, gold_pc) if gold_pc else 0.0
        lines.append(f"| >= {T:.2f} | {len(preds)} | {tp}/{len(preds)} = {100*prec:.1f}% | {tp}/{gold_pc} = {100*rec:.1f}% |")
    lines.append("")

    # SKIP threshold sweep (precision of "I'm sure it's NOT a merge")
    lines.append("## SKIP auto-skip threshold sweep")
    lines.append("")
    lines.append("Purpose: when the tier says SKIP, how often is it actually SKIP? High precision here means we can drop these pairs without further review.")
    lines.append("")
    lines.append("| threshold | N pred-SKIP | precision | recall vs gold-SKIP |")
    lines.append("|---|---|---|---|")
    gold_skip = sum(1 for r in rows if r["gold"] == "SKIP")
    for T in [0.97, 0.95, 0.92, 0.90, 0.87, 0.85]:
        preds = [r for r in rows if r["pred"] == "SKIP" and r["conf"] >= T]
        if not preds:
            continue
        tp = sum(1 for r in preds if r["gold"] == "SKIP")
        prec = tp / max(1, len(preds))
        rec = tp / max(1, gold_skip) if gold_skip else 0.0
        lines.append(f"| >= {T:.2f} | {len(preds)} | {tp}/{len(preds)} = {100*prec:.1f}% | {tp}/{gold_skip} = {100*rec:.1f}% |")
    lines.append("")

    # False negatives (tier says SKIP, gold says MERGE/PC) — these are the costly errors
    fns = [r for r in rows if r["pred"] == "SKIP" and r["gold"] in ("MERGE", "PARENT_CHILD")]
    lines.append(f"## False negatives (tier SKIP but gold MERGE/PC): {len(fns)}")
    lines.append("")
    if fns:
        # Show up to 15
        lines.append("| pair_id | tier | name_a | name_b | gold | conf | gold_src |")
        lines.append("|---|---|---|---|---|---|---|")
        for r in sorted(fns, key=lambda x: -x["conf"])[:15]:
            lines.append(f"| {r['pair_id']} | {r['tier']} | {r['name_a']!r} | {r['name_b']!r} | {r['gold']} | {r['conf']:.2f} | {r['gold_source']} |")
        if len(fns) > 15:
            lines.append(f"| ... | | | | | | |")
            lines.append(f"*{len(fns) - 15} more not shown*")
        lines.append("")

    # False positives (tier says MERGE, gold says SKIP) — auto-accept risk
    fps = [r for r in rows if r["pred"] == "MERGE" and r["gold"] == "SKIP"]
    lines.append(f"## False positives (tier MERGE but gold SKIP): {len(fps)}")
    lines.append("")
    if fps:
        lines.append("| pair_id | tier | name_a | name_b | conf | gold_src |")
        lines.append("|---|---|---|---|---|---|")
        for r in sorted(fps, key=lambda x: -x["conf"])[:15]:
            lines.append(f"| {r['pair_id']} | {r['tier']} | {r['name_a']!r} | {r['name_b']!r} | {r['conf']:.2f} | {r['gold_source']} |")
        if len(fps) > 15:
            lines.append(f"*{len(fps) - 15} more not shown*")
        lines.append("")

    # Proxy-only tier breakdown
    lines.append("## Proxy-only segment (T1=MERGE, T2=SKIP): sanity check")
    lines.append("")
    proxy_rows = [r for r in rows if r["gold_source"] == "proxy"]
    proxy_correct = sum(1 for r in proxy_rows if r["gold"] == r["pred"])
    if proxy_rows:
        lines.append(f"Proxy accuracy: {proxy_correct}/{len(proxy_rows)} = {100*proxy_correct/len(proxy_rows):.1f}%")
        lines.append("")
        lines.append("(If proxy accuracy is very high, proxies are reliable. If low, proxies misclassify edges — check blocking rules.)")
    lines.append("")

    return "\n".join(lines)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--tier", default="l1", help="which tier to analyze")
    ap.add_argument("--all", action="store_true", help="run for all tiers with data")
    args = ap.parse_args()

    if not CALIBRATION_PATH.exists():
        print("Missing calibration_set.json", file=sys.stderr)
        return 1

    doc = json.loads(CALIBRATION_PATH.read_text(encoding="utf-8"))
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    tiers_to_run = [args.tier]
    if args.all:
        # Probe which tiers have data
        tiers_to_run = []
        for tname in ["l1", "l1_gemini_basic", "l2_haiku_rich", "l2_gemini_rich", "l3_sonnet_web"]:
            vkey, _, _ = tier_key_for(tname)
            has_data = any(
                p.get(vkey)
                for tier, pairs in doc["tiers"].items()
                for p in pairs
            )
            if has_data:
                tiers_to_run.append(tname)

    for tname in tiers_to_run:
        try:
            report = analyze(doc, tname)
        except Exception as e:
            print(f"ERROR analyzing {tname}: {e}", file=sys.stderr)
            continue
        out_path = OUT_DIR / f"{tname}_calibration.md"
        out_path.write_text(report, encoding="utf-8")
        print(f"Wrote {out_path}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
