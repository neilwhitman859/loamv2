"""
WineTest — External benchmark assessment for the Loam wine database.

Measures how well our DB covers wines Americans actually encounter in stores,
restaurants, and friends' houses. Generates a fresh benchmark from external
sources each run, then scores findability, depth, accuracy, and story quality.

Usage:
    python -m pipeline.analyze.winetest
    python -m pipeline.analyze.winetest --size 100 --no-accuracy
    python -m pipeline.analyze.winetest --categories 6 --seed 42
    python -m pipeline.analyze.winetest --accuracy-sample 50

Flags:
    --size N              Benchmark size (default 200)
    --categories N        Number of random categories to sample (default 4)
    --accuracy-sample N   Wines to check for accuracy+story (default 30)
    --no-accuracy         Skip accuracy and story tests (saves ~$0.60)
    --seed N              Random seed for reproducibility
"""

import argparse
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from pipeline.lib.db import get_conn


def main():
    parser = argparse.ArgumentParser(description="WineTest benchmark assessment")
    parser.add_argument("--size", type=int, default=200, help="Benchmark size")
    parser.add_argument("--categories", type=int, default=4, help="Random categories to sample")
    parser.add_argument("--accuracy-sample", type=int, default=30, help="Accuracy sample size")
    parser.add_argument("--no-accuracy", action="store_true", help="Skip accuracy+story tests")
    parser.add_argument("--seed", type=int, default=None, help="Random seed")
    args = parser.parse_args()

    # Force UTF-8 on Windows
    import io, os
    if os.name == "nt":
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

    print()
    print("+======================================+")
    print("|          WINETEST v1.0               |")
    print("|  External Benchmark Assessment       |")
    print("+======================================+")
    print()

    t0 = time.time()

    # --- Phase 1: Generate benchmark ---
    print("Phase 1: Generating benchmark...")
    from pipeline.analyze.winetest.benchmark import generate_benchmark

    benchmark = generate_benchmark(
        size=args.size,
        num_categories=args.categories,
        seed=args.seed,
    )
    wines = benchmark["wines"]
    meta = benchmark["metadata"]
    print(f"  Generated {len(wines)} benchmark wines (seed={meta['seed']})")
    print(f"  Categories: {', '.join(meta['categories_sampled'])}")
    print()

    if not wines:
        print("ERROR: No benchmark wines generated. Check API key.")
        sys.exit(1)

    # --- Phase 2: Score findability ---
    print("Phase 2: Scoring findability...")
    conn = get_conn()

    from pipeline.analyze.winetest.scorer import score_findability, score_depth, detect_blind_spots

    findability = score_findability(wines, conn)
    found_count = len([r for r in findability if r["status"] == "found"])
    producer_only = len([r for r in findability if r["status"] == "producer_only"])
    not_found_count = len([r for r in findability if r["status"] == "not_found"])
    print(f"  Found: {found_count}/{len(wines)} ({found_count/len(wines)*100:.0f}%)")
    print(f"  Producer only: {producer_only}")
    print(f"  Not found: {not_found_count}")
    print()

    # --- Phase 3: Score depth ---
    print("Phase 3: Scoring depth...")
    depth = score_depth(findability, conn)
    if depth:
        avg_depth = sum(r["overall"] for r in depth) / len(depth) * 100
        print(f"  Average depth: {avg_depth:.0f}%")
    else:
        avg_depth = 0
        print("  No wines found — depth score N/A")
    print()

    # --- Phase 4: Blind spot detection ---
    print("Phase 4: Detecting blind spots...")
    blind_spots = detect_blind_spots(findability)
    if blind_spots:
        for bs in blind_spots[:5]:
            print(f"  {bs['dimension']}: {bs['value']} — {bs['miss_rate']:.0%} missed")
    else:
        print("  No significant blind spots detected")
    print()

    # --- Phase 5: Accuracy + Story (optional) ---
    accuracy = None
    story = None
    if not args.no_accuracy:
        # Use same seed for accuracy and story so they sample the same wines
        acc_seed = (meta["seed"] + 7) if meta.get("seed") else None

        print(f"Phase 5a: Accuracy verification ({args.accuracy_sample} wine sample)...")
        from pipeline.analyze.winetest.accuracy import score_accuracy, score_story

        accuracy = score_accuracy(
            findability, conn,
            sample_size=args.accuracy_sample,
            seed=acc_seed,
        )
        if accuracy.get("accuracy_pct") is not None:
            print(f"  Accuracy: {accuracy['accuracy_pct']:.0f}% ({accuracy['total_errors']} errors)")
        else:
            print("  Accuracy: N/A (no wines to check)")
        print()

        print(f"Phase 5b: Story quality ({args.accuracy_sample} wine sample)...")
        story = score_story(
            findability, conn,
            sample_size=args.accuracy_sample,
            seed=acc_seed,
        )
        if story.get("avg_score") is not None:
            print(f"  Story score: {story['avg_score']:.1f}/5.0")
        else:
            print("  Story score: N/A")
        print()
    else:
        print("Phase 5: Skipped (--no-accuracy)")
        print()

    conn.close()

    # --- Phase 6: Generate report ---
    print("Phase 6: Generating report...")
    from pipeline.analyze.winetest.report import generate_report, save_results

    report = generate_report(
        meta, findability, depth, blind_spots, accuracy, story,
    )

    json_path = save_results(
        meta, findability, depth, blind_spots, accuracy, story, report,
    )

    elapsed = time.time() - t0
    print(f"  Saved to {json_path}")
    print(f"  Elapsed: {elapsed:.0f}s")
    print()

    # --- Print full report ---
    print("─" * 56)
    print()
    print(report)


if __name__ == "__main__":
    main()
