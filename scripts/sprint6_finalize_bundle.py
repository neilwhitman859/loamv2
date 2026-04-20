"""Orchestrator: build the full execution bundle from source verdicts + overrides.

Runs in order:
1. sprint6_build_final_ledger      → verdict_ledger.jsonl + verdict_ledger_summary.md
2. sprint6_build_scorecard         → scorecard.md
3. sprint6_build_flips_report      → rechrome_flips.md
4. sprint6_build_execution_plan    → execution_plan.md
5. sprint6_build_deferred_list     → deferred_to_sprint_7.md

Run this after both re-Chrome subagent passes are complete (Core + Mid/Tail/Yellow).
"""
from __future__ import annotations

import subprocess
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]


def run(module: str) -> bool:
    print(f"\n=== Running {module} ===")
    r = subprocess.run([sys.executable, "-m", f"scripts.{module}"], cwd=REPO)
    if r.returncode != 0:
        print(f"FAILED: {module} returned {r.returncode}")
        return False
    return True


def main():
    steps = [
        "sprint6_build_final_ledger",
        "sprint6_build_scorecard",
        "sprint6_build_flips_report",
        "sprint6_build_execution_plan",
        "sprint6_build_deferred_list",
    ]
    for s in steps:
        if not run(s):
            print(f"\nStopped at {s}")
            return 1
    print("\nAll bundle artifacts built:")
    for p in (REPO / "data/sprints/dedup/execution_bundle").glob("*"):
        print(f"  {p.name}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
