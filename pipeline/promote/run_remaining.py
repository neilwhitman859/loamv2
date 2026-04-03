"""
Run remaining pipeline steps after TTB wine linking completes:
1. COLA pass 1d (vintages + COLA external IDs)
2. Grape promotion with encoding fixes
3. Batch matcher (retailers)
4. Retail promote (UPCs/prices)

Usage:
    python -m pipeline.promote.run_remaining [--dry-run]
"""

import subprocess
import sys
import time


def run_step(name, cmd):
    print(f"\n{'='*60}", flush=True)
    print(f"  STEP: {name}", flush=True)
    print(f"{'='*60}", flush=True)
    t0 = time.time()
    result = subprocess.run(cmd, shell=True)
    elapsed = time.time() - t0
    status = "OK" if result.returncode == 0 else f"FAILED (exit {result.returncode})"
    print(f"  {status} in {elapsed:.0f}s", flush=True)
    return result.returncode == 0


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    dry = " --dry-run" if args.dry_run else ""

    steps = [
        ("COLA Pass 1d: Vintages + External IDs",
         f"python -m pipeline.promote.cola_pass1 --step 1d{dry}"),
        ("Grape promotion (with encoding fixes)",
         f"python -m pipeline.promote.grape_from_helper{dry}"),
        ("Batch matcher: Spec's + Wally's + Flatiron + LCBO + Systembolaget + BC Liquor",
         f"python -m pipeline.promote.batch_matcher --source specs,wallys,flatiron,lcbo,systembolaget,bc_liquor{dry}"),
        ("Retail promote: UPCs + Prices",
         f"python -m pipeline.promote.retail_promote --source flatiron,lcbo,systembolaget,bc_liquor{dry}"),
    ]

    results = []
    for name, cmd in steps:
        ok = run_step(name, cmd)
        results.append((name, ok))
        if not ok:
            print(f"\n  WARNING: {name} failed, continuing with next step...", flush=True)

    print(f"\n{'='*60}", flush=True)
    print("  SUMMARY", flush=True)
    print(f"{'='*60}", flush=True)
    for name, ok in results:
        status = "OK" if ok else "FAILED"
        print(f"  [{status}] {name}", flush=True)


if __name__ == "__main__":
    main()
