"""Session 10.8 - build offline-only holdout packets for guarded-frontier review.

This script does not touch the DB and does not call any models. It joins the
fresh 24-case holdout manifest with frozen local Chrome-validation artifacts so
we can answer two narrower questions honestly:

1. Is a zero-cost offline audit runnable from existing frozen files?
2. Is a faithful fresh-holdout rerun of `hybrid_guarded_frontier_v1` runnable?

The answer is intentionally split because the available frozen artifacts are
asymmetric:
- all 24 holdout cases retain verdict-level evidence snippets
- only the 10 positive holdout cases retain structured side context with wine
  lists in the local rechrome context files

That means we can build a blinded offline audit packet set, but we should not
pretend it is equivalent to the fully structured visible packets used in the
benchmark reruns.
"""

from __future__ import annotations

import json
from collections import Counter
from datetime import date
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
METHOD_BAKEOFF_DIR = REPO_ROOT / "data" / "sprints" / "identity-er" / "method_bakeoff"
MANIFEST_PATH = METHOD_BAKEOFF_DIR / "session10_8_guarded_frontier_holdout_manifest_v1.json"
OUTPUT_VISIBLE_JSONL = (
    METHOD_BAKEOFF_DIR
    / "session10_8_guarded_frontier_holdout_offline_visible_packets_v1.jsonl"
)
OUTPUT_BUNDLE_JSON = (
    METHOD_BAKEOFF_DIR / "session10_8_guarded_frontier_holdout_offline_bundle_v1.json"
)
OUTPUT_RUNNABILITY_MD = (
    METHOD_BAKEOFF_DIR / "session10_8_guarded_frontier_holdout_runnability.md"
)

VERDICT_SOURCES = {
    "core": REPO_ROOT / "data" / "sprints" / "dedup" / "chrome_validation" / "core_verdicts.jsonl",
    "mid": REPO_ROOT / "data" / "sprints" / "dedup" / "chrome_validation" / "mid_verdicts.jsonl",
    "tail": REPO_ROOT / "data" / "sprints" / "dedup" / "chrome_validation" / "tail_verdicts.jsonl",
}
CONTEXT_SOURCES = [
    REPO_ROOT / "data" / "sprints" / "dedup" / "chrome_validation" / "_rechrome_core_context.jsonl",
    REPO_ROOT / "data" / "sprints" / "dedup" / "chrome_validation" / "_rechrome_rest_context.jsonl",
]


def canonical_json_dumps(value: object) -> str:
    return json.dumps(value, ensure_ascii=True, indent=2, sort_keys=False) + "\n"


def write_jsonl(path: Path, rows: list[dict]) -> None:
    payload = "".join(json.dumps(row, ensure_ascii=True, sort_keys=False) + "\n" for row in rows)
    path.write_text(payload, encoding="utf-8")


def load_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def load_jsonl(path: Path) -> list[dict]:
    return [
        json.loads(line)
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]


def build_verdict_rows() -> dict[int, dict]:
    rows: dict[int, dict] = {}
    for tier, path in VERDICT_SOURCES.items():
        for row in load_jsonl(path):
            pair_id = int(row["pair_id"])
            tagged = dict(row)
            tagged["source_tier"] = tier
            tagged["source_artifact"] = str(path.relative_to(REPO_ROOT)).replace("\\", "/")
            rows[pair_id] = tagged
    return rows


def build_context_rows() -> dict[int, dict]:
    rows: dict[int, dict] = {}
    for path in CONTEXT_SOURCES:
        for row in load_jsonl(path):
            pair_id = row.get("pair_id")
            if pair_id is None:
                continue
            tagged = dict(row)
            tagged["source_artifact"] = str(path.relative_to(REPO_ROOT)).replace("\\", "/")
            rows[int(pair_id)] = tagged
    return rows


def visible_packet(case: dict, verdict_row: dict, context_row: dict | None) -> dict:
    evidence_refs = []
    if verdict_row.get("evidence_a"):
        evidence_refs.append(
            {
                "ref_id": "evidence_a",
                "subject": "side_a",
                "summary": verdict_row["evidence_a"],
                "url": verdict_row.get("evidence_url_a"),
            }
        )
    if verdict_row.get("evidence_b"):
        evidence_refs.append(
            {
                "ref_id": "evidence_b",
                "subject": "side_b",
                "summary": verdict_row["evidence_b"],
                "url": verdict_row.get("evidence_url_b"),
            }
        )

    side_a_context = None
    side_b_context = None
    if context_row:
        side_a_context = context_row.get("side_a")
        side_b_context = context_row.get("side_b")

    return {
        "packet_id": f"guarded_frontier_holdout_pair_{case['pair_id']}_offline_v1",
        "case_id": case["case_id"],
        "pair_id": case["pair_id"],
        "producer_name_a": case["producer_name_a"],
        "producer_name_b": case["producer_name_b"],
        "country_a": case["country_a"],
        "country_b": case["country_b"],
        "side_a_context": side_a_context,
        "side_b_context": side_b_context,
        "evidence_refs": evidence_refs,
        "packet_notes": {
            "structured_side_context_available": bool(context_row),
            "verdict_evidence_only": not bool(context_row),
            "source_artifacts": [
                verdict_row["source_artifact"],
                *([context_row["source_artifact"]] if context_row else []),
            ],
        },
    }


def coverage_row(case: dict, verdict_row: dict, context_row: dict | None) -> dict:
    evidence_ref_count = sum(
        1
        for key in ("evidence_a", "evidence_b")
        if str(verdict_row.get(key) or "").strip()
    )
    return {
        "case_id": case["case_id"],
        "pair_id": case["pair_id"],
        "expected_verdict": case["expected_verdict"],
        "source_pair_tier": case["source_pair_tier"],
        "pattern_cluster": case["pattern_cluster"],
        "has_verdict_evidence": evidence_ref_count >= 1,
        "evidence_ref_count": evidence_ref_count,
        "has_structured_side_context": bool(context_row),
        "context_source_artifact": context_row["source_artifact"] if context_row else None,
        "coverage_class": "rich" if context_row else "thin",
    }


def summarize_coverage(rows: list[dict]) -> dict:
    total = len(rows)
    expected_counts = Counter(row["expected_verdict"] for row in rows)
    rich_counts = Counter(
        row["expected_verdict"] for row in rows if row["has_structured_side_context"]
    )
    thin_case_ids = [row["case_id"] for row in rows if not row["has_structured_side_context"]]

    return {
        "case_count": total,
        "expected_verdict_counts": dict(expected_counts),
        "verdict_evidence_cases": sum(1 for row in rows if row["has_verdict_evidence"]),
        "structured_context_cases": sum(
            1 for row in rows if row["has_structured_side_context"]
        ),
        "merge_cases_with_structured_context": rich_counts.get("MERGE", 0),
        "skip_cases_with_structured_context": rich_counts.get("SKIP", 0),
        "thin_case_ids": thin_case_ids,
        "offline_evidence_is_post_adjudicated": True,
        "offline_audit_runnable": total > 0
        and all(row["has_verdict_evidence"] for row in rows),
        "independent_fresh_confirmation_runnable": False,
        "faithful_fresh_confirmation_runnable": total > 0
        and all(row["has_structured_side_context"] for row in rows),
    }


def render_markdown(summary: dict) -> str:
    thin_cases = ", ".join(f"`{case_id}`" for case_id in summary["thin_case_ids"]) or "none"
    lines = [
        "# Session 10.8 - guarded frontier holdout runnability",
        "",
        f"- Date: {date.today().isoformat()}",
        "- Candidate under confirmation: `hybrid_guarded_frontier_v1`",
        "- Fresh confirmation slice: `session10_8_guarded_frontier_holdout_manifest_v1.json`",
        "- Offline packet bundle: `session10_8_guarded_frontier_holdout_offline_bundle_v1.json`",
        "",
        "## What Was Checked",
        "",
        "The fresh 24-case holdout was re-checked against frozen local artifacts only.",
        "No DB access, no network calls, and no new model calls were used.",
        "",
        "The join looked for two levels of frozen evidence:",
        "",
        "- verdict-level evidence snippets from `core_verdicts.jsonl`, `mid_verdicts.jsonl`, and `tail_verdicts.jsonl`",
        "- structured side context (producer ids, wine counts, wine lists) from the local rechrome context files",
        "",
        "## Coverage Result",
        "",
        f"- Holdout cases: `{summary['case_count']}`",
        f"- Cases with at least one frozen verdict-evidence snippet: `{summary['verdict_evidence_cases']} / {summary['case_count']}`",
        f"- Cases with structured side context: `{summary['structured_context_cases']} / {summary['case_count']}`",
        f"- MERGE cases with structured side context: `{summary['merge_cases_with_structured_context']} / {summary['expected_verdict_counts'].get('MERGE', 0)}`",
        f"- SKIP cases with structured side context: `{summary['skip_cases_with_structured_context']} / {summary['expected_verdict_counts'].get('SKIP', 0)}`",
        "",
        "## Honest Runnability Split",
        "",
        f"- Zero-cost offline audit runnable: `{str(summary['offline_audit_runnable']).lower()}`",
        f"- Independent fresh confirmation runnable: `{str(summary['independent_fresh_confirmation_runnable']).lower()}`",
        f"- Faithful fresh-holdout method rerun runnable: `{str(summary['faithful_fresh_confirmation_runnable']).lower()}`",
        "",
        "## Why The Split Matters",
        "",
        "All 24 cases preserve enough frozen evidence to build blinded offline packets for a manual or inline audit.",
        "That is useful, but it is not the same thing as an independent or faithful rerun of the guarded-frontier method.",
        "",
        "The frozen evidence snippets are already post-adjudicated summaries taken from the Chrome-validation verdict files rather than raw packet-build retrieval traces.",
        "So an offline audit can still test whether the current candidate remains logically aligned with the frozen source-of-truth, but it cannot honestly promote the method to \"fresh-holdout independently confirmed.\"",
        "",
        "The blocker is asymmetry in the preserved packet structure:",
        "",
        "- all 10 positive holdout cases still have structured side context",
        "- none of the 14 skip holdout cases still have structured side context",
        "",
        "Because the current live risk is false merges on adjacent or shared-surname skip traps, losing the structured side context exactly on the negative set means a fully comparable rerun would over-credit the candidate if we treated this offline bundle as equivalent to the benchmark packets.",
        "",
        "## Thin-Coverage Cases",
        "",
        thin_cases,
        "",
        "## Recommendation",
        "",
        "Recommended wording now:",
        "",
        "> `hybrid_guarded_frontier_v1` is benchmark-pass with repeated rerun confirmation; a zero-cost offline holdout audit is now runnable from frozen local artifacts, but that audit would be consistency-only, not independent confirmation, and a faithful fresh-holdout rerun is still blocked because the preserved offline packet structure is incomplete on all 14 negative holdout cases.",
        "",
        "Next bounded step:",
        "",
        "- if we want maximum rigor without unfreezing infrastructure, run a clearly labeled zero-cost offline audit on the blinded packets",
        "- if we want a true fresh-holdout rerun claim, restore a working local packet-build/runtime path first",
    ]
    return "\n".join(lines).rstrip() + "\n"


def main() -> int:
    manifest = load_json(MANIFEST_PATH)
    verdict_rows = build_verdict_rows()
    context_rows = build_context_rows()

    visible_packets: list[dict] = []
    coverage_rows: list[dict] = []

    for case in manifest["cases"]:
        pair_id = int(case["pair_id"])
        verdict_row = verdict_rows.get(pair_id)
        if verdict_row is None:
            raise RuntimeError(f"Missing frozen verdict evidence for pair_id={pair_id}")
        context_row = context_rows.get(pair_id)
        visible_packets.append(visible_packet(case, verdict_row, context_row))
        coverage_rows.append(coverage_row(case, verdict_row, context_row))

    summary = summarize_coverage(coverage_rows)
    bundle = {
        "bundle_id": "guarded_frontier_holdout_offline_bundle_v1",
        "created_on": date.today().isoformat(),
        "source_manifest": str(MANIFEST_PATH.relative_to(REPO_ROOT)).replace("\\", "/"),
        "visible_packet_artifact": str(OUTPUT_VISIBLE_JSONL.relative_to(REPO_ROOT)).replace("\\", "/"),
        "coverage_summary": summary,
        "cases": coverage_rows,
    }

    OUTPUT_VISIBLE_JSONL.parent.mkdir(parents=True, exist_ok=True)
    write_jsonl(OUTPUT_VISIBLE_JSONL, visible_packets)
    OUTPUT_BUNDLE_JSON.write_text(canonical_json_dumps(bundle), encoding="utf-8")
    OUTPUT_RUNNABILITY_MD.write_text(render_markdown(summary), encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
