"""Build rechrome_flips.md — every B6.6 override with before/after evidence.

Reads: data/sprints/dedup/execution_bundle/verdict_ledger.jsonl
Writes: data/sprints/dedup/execution_bundle/rechrome_flips.md
"""
from __future__ import annotations

import json
from collections import Counter
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
LEDGER = REPO / "data/sprints/dedup/execution_bundle/verdict_ledger.jsonl"
OUT = REPO / "data/sprints/dedup/execution_bundle/rechrome_flips.md"


def main():
    with LEDGER.open(encoding="utf-8") as f:
        ledger = [json.loads(line) for line in f if line.strip()]

    overrides = [e for e in ledger if e.get("override_source")
                 and e.get("override_action") not in (None, "KEEP")]

    # Group by action + source
    by_action = Counter(e.get("override_action") for e in overrides)
    by_source = Counter(e.get("override_source") for e in overrides)

    L = []
    L.append("# B6.6 Re-Chrome Flips")
    L.append("")
    L.append("Every verdict override applied during Sprint 6 B6.6, with evidence.")
    L.append("")
    L.append("## Summary")
    L.append("")
    L.append(f"Total overrides: **{len(overrides)}** (out of {len(ledger)} ledger entries)")
    L.append("")
    L.append("### By action")
    L.append("")
    L.append("| Action | Count |")
    L.append("|---|---|")
    for k, v in by_action.most_common():
        L.append(f"| {k} | {v} |")
    L.append("")
    L.append("### By source")
    L.append("")
    L.append("| Source | Count |")
    L.append("|---|---|")
    for k, v in by_source.most_common():
        L.append(f"| {k} | {v} |")
    L.append("")

    # Group by action type for readability
    actions_to_show = ["FLIP_TO_SKIP", "FLIP_DIRECTION", "FLIP_TO_MERGE", "FLIP_TO_PC", "NEEDS_HUMAN_REVIEW"]
    for action in actions_to_show:
        matching = [e for e in overrides if e.get("override_action") == action]
        if not matching:
            continue
        L.append(f"## {action} ({len(matching)})")
        L.append("")
        for e in matching:
            L.append(f"### {e.get('ledger_key')} — {e.get('name_a')} vs {e.get('name_b')}")
            L.append("")
            L.append(f"- **Original:** {e.get('original_verdict')} (cluster {e.get('pattern_cluster')})")
            L.append(f"- **Final:** {e.get('final_verdict')}")
            if e.get("final_survivor_name") != e.get("original_survivor_name"):
                L.append(f"- **Survivor change:** `{e.get('original_survivor_name')}` → `{e.get('final_survivor_name')}`")
            if e.get("final_parent_name") != e.get("original_parent_name"):
                L.append(f"- **Parent change:** `{e.get('original_parent_name')}` → `{e.get('final_parent_name')}`")
            L.append(f"- **Source:** {e.get('override_source')}")
            L.append(f"- **Reasoning:** {e.get('override_reasoning', '').strip()}")
            if e.get("override_chrome_evidence"):
                L.append(f"- **Chrome evidence:** {e['override_chrome_evidence']}")
            if e.get("override_chrome_url"):
                L.append(f"- **Chrome URL:** {e['override_chrome_url']}")
            if e.get("sprint7_flag"):
                L.append(f"- **Sprint 7 flag:** `{e['sprint7_flag']}`")
            L.append("")

    OUT.write_text("\n".join(L), encoding="utf-8")
    print(f"Flips report written: {OUT}")
    print(f"  {len(overrides)} overrides across {len(actions_to_show)} action types")


if __name__ == "__main__":
    main()
