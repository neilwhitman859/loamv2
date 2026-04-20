"""Build deferred_to_sprint_7.md from the ledger.

Reads: data/sprints/dedup/execution_bundle/verdict_ledger.jsonl
Writes: data/sprints/dedup/execution_bundle/deferred_to_sprint_7.md

Lists every ledger entry with sprint7_flag or final_verdict='DEFERRED_SPRINT_7',
plus a pointer to open_questions.md for the systemic items.
"""
from __future__ import annotations

import json
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
LEDGER = REPO / "data/sprints/dedup/execution_bundle/verdict_ledger.jsonl"
OUT = REPO / "data/sprints/dedup/execution_bundle/deferred_to_sprint_7.md"


def main():
    with LEDGER.open(encoding="utf-8") as f:
        ledger = [json.loads(line) for line in f if line.strip()]

    deferred_entries = [e for e in ledger
                         if e.get("sprint7_flag") or e.get("final_verdict") == "DEFERRED_SPRINT_7"]

    L = []
    L.append("# Deferred to Sprint 7")
    L.append("")
    L.append("Pairs explicitly not resolved in this bundle, with rationale.")
    L.append(f"For systemic issues (schema changes, multi-entity JVs, etc.), see `open_questions.md`.")
    L.append("")
    L.append(f"## Pair-level deferrals: {len(deferred_entries)}")
    L.append("")
    if deferred_entries:
        L.append("| ledger_key | cluster | verdict | reason |")
        L.append("|---|---|---|---|")
        for e in deferred_entries:
            flag = e.get("sprint7_flag") or "needs_human_review"
            L.append(f"| {e.get('ledger_key')} | {e.get('pattern_cluster')} | "
                     f"{e.get('final_verdict')} | {flag} |")
        L.append("")
        L.append("## Detail per deferred pair")
        L.append("")
        for e in deferred_entries:
            L.append(f"### {e.get('ledger_key')} — {e.get('name_a')} vs {e.get('name_b')}")
            L.append("")
            L.append(f"- Cluster: `{e.get('pattern_cluster')}`")
            L.append(f"- Original verdict: `{e.get('original_verdict')}`")
            L.append(f"- Final verdict: `{e.get('final_verdict')}`")
            if e.get("sprint7_flag"):
                L.append(f"- Flag: `{e.get('sprint7_flag')}`")
            if e.get("override_reasoning"):
                L.append(f"- Reasoning: {e['override_reasoning']}")
            if e.get("override_chrome_url"):
                L.append(f"- Chrome URL: {e['override_chrome_url']}")
            L.append("")

    L.append("---")
    L.append("")
    L.append("See `open_questions.md` for the systemic Sprint 7 agenda (schema changes, family cleanups, etc.)")
    L.append("")

    OUT.write_text("\n".join(L), encoding="utf-8")
    print(f"Deferred list written: {OUT}")
    print(f"  {len(deferred_entries)} pair-level deferrals")


if __name__ == "__main__":
    main()
