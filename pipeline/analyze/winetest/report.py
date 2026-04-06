"""
WineTest report formatting, blind spot analysis, and trend tracking.
"""

import json
from datetime import datetime
from pathlib import Path

RESULTS_DIR = Path(__file__).resolve().parents[3] / "data" / "stats" / "winetest"


def generate_report(
    benchmark_meta: dict,
    findability_results: list[dict],
    depth_results: list[dict],
    blind_spots: list[dict],
    accuracy: dict | None = None,
    story: dict | None = None,
) -> str:
    """Generate a human-readable WineTest report."""

    total = len(findability_results)
    found = len([r for r in findability_results if r["status"] == "found"])
    producer_only = len([r for r in findability_results if r["status"] == "producer_only"])
    not_found = len([r for r in findability_results if r["status"] == "not_found"])
    find_pct = found / total * 100 if total > 0 else 0

    depth_avg = (
        sum(r["overall"] for r in depth_results) / len(depth_results) * 100
        if depth_results
        else 0
    )

    # Category-level depth averages
    depth_cats = {}
    if depth_results:
        for cat in depth_results[0]["categories"]:
            vals = [r["categories"][cat] for r in depth_results]
            depth_cats[cat] = sum(vals) / len(vals) * 100

    acc_pct = accuracy.get("accuracy_pct") if accuracy else None
    story_avg = story.get("avg_score") if story else None
    story_norm = story.get("avg_score_normalized") if story else None

    # Overall score (4 dimensions, equal weight)
    dims = [find_pct, depth_avg]
    if acc_pct is not None:
        dims.append(acc_pct)
    if story_norm is not None:
        dims.append(story_norm)
    overall = sum(dims) / len(dims)

    # --- Build report ---
    lines = []
    ts = datetime.now().strftime("%Y-%m-%d %H:%M")
    lines.append(f"WINETEST -- {ts}")
    lines.append("=" * 56)
    lines.append("")
    lines.append(f"Overall: {overall:.0f}/100")
    lines.append("")

    # Sources
    lines.append(f"Benchmark: {total} wines from {len(benchmark_meta.get('sources', []))} sources")
    lines.append(f"  Seed: {benchmark_meta.get('seed', 'N/A')}")
    cats = benchmark_meta.get("categories_sampled", [])
    if cats:
        lines.append(f"  Categories: {', '.join(cats)}")
    lines.append("")

    # Findability
    lines.append(f"FINDABILITY    {find_pct:.0f}% ({found}/{total} found)")
    lines.append(f"  Producer only (wine not matched): {producer_only}")
    lines.append(f"  Not found at all: {not_found}")
    lines.append("")

    # Depth
    lines.append(f"DEPTH          {depth_avg:.0f}% (weighted avg across {len(depth_results)} found wines)")
    if depth_cats:
        max_label = max(len(k) for k in depth_cats)
        for cat, pct in sorted(depth_cats.items(), key=lambda x: -x[1]):
            bar = _bar(pct)
            lines.append(f"  {cat:<{max_label}}  {pct:5.0f}%  {bar}")
    lines.append("")

    # Accuracy
    if accuracy and acc_pct is not None:
        errs = accuracy.get("total_errors", 0)
        lines.append(
            f"ACCURACY       {acc_pct:.0f}% "
            f"({accuracy['wines_checked']}/{accuracy['sample_size']} wines checked, "
            f"{errs} field error{'s' if errs != 1 else ''})"
        )
        if accuracy.get("errors"):
            for e in accuracy["errors"][:5]:
                lines.append(f"  X {e['field']}: {e['note']}")
            if len(accuracy["errors"]) > 5:
                lines.append(f"  ... and {len(accuracy['errors']) - 5} more")
        lines.append("")

    # Story
    if story and story_avg is not None:
        lines.append(f"STORY          {story_avg:.1f}/5.0 ({story_norm:.0f}/100 normalized)")
        dist = story.get("distribution", {})
        if dist:
            dist_str = "  ".join(f"{k}*={v}" for k, v in sorted(dist.items()))
            lines.append(f"  Distribution: {dist_str}")
        # Show worst-rated wines
        worst = sorted(
            [r for r in story.get("results", []) if r.get("score")],
            key=lambda x: x["score"],
        )[:3]
        if worst and worst[0]["score"] <= 2:
            lines.append("  Weakest:")
            for w in worst:
                lines.append(
                    f"    {w['benchmark_producer']} {w['benchmark_name']}: "
                    f"{w['score']}/5 -- {w.get('reasoning', '')}"
                )
        lines.append("")

    # Blind spots
    if blind_spots:
        lines.append("BLIND SPOTS (>50% miss rate)")
        for bs in blind_spots[:10]:
            lines.append(
                f"  {bs['dimension']:<12} {bs['value']:<30} "
                f"{bs['miss_rate']:.0%} missed ({bs['missed']}/{bs['total']})"
            )
        lines.append("")

    # Notable misses
    high_value_misses = [
        r for r in findability_results
        if r["status"] == "not_found"
        and r["benchmark"].get("price_tier") in ("premium", "luxury")
    ]
    if high_value_misses:
        lines.append("NOTABLE MISSES (premium/luxury wines not found)")
        for m in high_value_misses[:10]:
            bm = m["benchmark"]
            lines.append(f"  {bm['producer']} — {bm['name']} ({bm.get('country', '?')})")
        if len(high_value_misses) > 10:
            lines.append(f"  ... and {len(high_value_misses) - 10} more")
        lines.append("")

    # Trend
    prev = _load_previous_result()
    if prev:
        lines.append("TREND (vs previous run)")
        prev_overall = prev.get("overall_score", 0)
        delta = overall - prev_overall
        arrow = "UP" if delta > 0 else "DOWN" if delta < 0 else "FLAT"
        lines.append(f"  Overall: {prev_overall:.0f} → {overall:.0f} ({arrow} {abs(delta):.1f})")
        prev_ts = prev.get("timestamp", "unknown")
        lines.append(f"  Previous run: {prev_ts}")
        lines.append("")

    return "\n".join(lines)


def save_results(
    benchmark_meta: dict,
    findability_results: list[dict],
    depth_results: list[dict],
    blind_spots: list[dict],
    accuracy: dict | None,
    story: dict | None,
    report_text: str,
) -> Path:
    """Save timestamped results JSON and text report."""
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)

    ts = datetime.now().strftime("%Y-%m-%dT%H%M%S")

    total = len(findability_results)
    found = len([r for r in findability_results if r["status"] == "found"])
    find_pct = found / total * 100 if total > 0 else 0
    depth_avg = (
        sum(r["overall"] for r in depth_results) / len(depth_results) * 100
        if depth_results
        else 0
    )

    dims = [find_pct, depth_avg]
    acc_pct = accuracy.get("accuracy_pct") if accuracy else None
    story_norm = story.get("avg_score_normalized") if story else None
    if acc_pct is not None:
        dims.append(acc_pct)
    if story_norm is not None:
        dims.append(story_norm)
    overall = sum(dims) / len(dims)

    result_data = {
        "timestamp": datetime.now().isoformat(),
        "overall_score": round(overall, 1),
        "findability": {
            "pct": round(find_pct, 1),
            "found": found,
            "total": total,
        },
        "depth": {
            "pct": round(depth_avg, 1),
        },
        "accuracy": {
            "pct": acc_pct,
            "errors": accuracy.get("total_errors") if accuracy else None,
        } if accuracy else None,
        "story": {
            "avg_score": story.get("avg_score") if story else None,
            "normalized": story_norm,
        } if story else None,
        "blind_spots": blind_spots,
        "benchmark_metadata": benchmark_meta,
        "benchmark_wines": [r["benchmark"] for r in findability_results],
        "findability_detail": [
            {
                "producer": r["benchmark"]["producer"],
                "name": r["benchmark"]["name"],
                "status": r["status"],
                "match_wine": r.get("match_wine_name"),
                "match_producer": r.get("match_producer_name"),
                "confidence": r["confidence"],
            }
            for r in findability_results
        ],
        "depth_detail": [
            {
                "wine_id": r["wine_id"],
                "wine": r.get("match_wine_name"),
                "categories": r["categories"],
                "overall": round(r["overall"], 3),
            }
            for r in depth_results
        ],
        "accuracy_detail": accuracy.get("results") if accuracy else None,
        "story_detail": story.get("results") if story else None,
    }

    json_path = RESULTS_DIR / f"{ts}.json"
    txt_path = RESULTS_DIR / f"{ts}.txt"

    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(result_data, f, indent=2, default=str)
    with open(txt_path, "w", encoding="utf-8") as f:
        f.write(report_text)

    return json_path


def _load_previous_result() -> dict | None:
    """Load the most recent previous result for trend comparison."""
    if not RESULTS_DIR.exists():
        return None
    jsons = sorted(RESULTS_DIR.glob("*.json"), reverse=True)
    # Skip the current run (will be saved after this)
    for p in jsons:
        try:
            with open(p, encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            continue
    return None


def _bar(pct: float, width: int = 20) -> str:
    """Render a simple bar chart."""
    filled = int(pct / 100 * width)
    return "#" * filled + "." * (width - filled)
