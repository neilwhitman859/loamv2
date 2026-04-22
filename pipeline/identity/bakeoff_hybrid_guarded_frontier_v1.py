"""Session 10.8 - guarded follow-on for the hybrid frontier method.

This method is intentionally conservative. It starts from an already-scored
`hybrid_signature_plus_judge_v1` run and adds three post-judge safeguards:

1. If a frontier model output fails contract validation, reuse the frozen base
   control row instead of scoring a schema-invalid child output.
2. If the frozen base was already `FLAGGED`, do not let the frontier harden it
   to `SKIP`.
3. Apply two narrow ambiguity guards on top of the frontier:
   - veto a shared-surname `MERGE` when the only visible bridge is duplicated
     cross-side secondary retrieval on a flagged base row
   - coerce the generic short-name stub pattern from `SKIP` to `FLAGGED`

The point is not to widen policy. The point is to stop the frontier layer from
pretending more certainty than the visible evidence deserves.
"""

from __future__ import annotations

import argparse
import json
from copy import deepcopy
from datetime import datetime
from pathlib import Path

from pipeline.identity.bakeoff_harness_v1 import load_benchmark_payload, score_run
from pipeline.identity.bakeoff_harness_v2 import (
    DEFAULT_BENCHMARK,
    DEFAULT_OUTPUT_DIR,
    ensure_visible_packets,
)
from pipeline.identity.bakeoff_method_bakeoff_proof import (
    CONTROL_NORMALIZED,
    load_rows_by_case,
    render_case_id_list,
    summarize_rows,
    write_json,
)
from pipeline.identity.bakeoff_packet_v2 import canonical_json_dumps, write_jsonl
from pipeline.identity.bakeoff_hybrid_signature_plus_judge_v1 import (
    CONTENDER_ID as SOURCE_CONTENDER_ID,
    copy_control_row,
)
from pipeline.identity.bakeoff_method_bakeoff_proof import build_features


REPO_ROOT = Path(__file__).resolve().parents[2]
RUN_NAME_DEFAULT = "session10_8_hybrid_guarded_frontier_v1"
DEFAULT_OUTPUT_ROOT = REPO_ROOT / "data" / "sprints" / "identity-er" / "method_bakeoff"
DEFAULT_REPORT_JSON = DEFAULT_OUTPUT_ROOT / f"{RUN_NAME_DEFAULT}.json"
DEFAULT_REPORT_MD = DEFAULT_OUTPUT_ROOT / f"{RUN_NAME_DEFAULT}.md"
CONTENDER_ID = "hybrid_guarded_frontier_v1"
METHOD_VERSION = "hybrid_guarded_frontier_v1"


def now_iso() -> str:
    return datetime.now().astimezone().isoformat(timespec="seconds")


def load_jsonl(path: Path) -> list[dict]:
    with path.open(encoding="utf-8") as handle:
        return [json.loads(line) for line in handle if line.strip()]


def accumulate_usage(rows: list[dict]) -> dict:
    totals = {
        "prompt_tokens": 0,
        "completion_tokens": 0,
        "search_calls": 0,
        "cost_usd": 0.0,
    }
    for row in rows:
        usage = row.get("usage") or {}
        totals["prompt_tokens"] += int(usage.get("prompt_tokens", 0) or 0)
        totals["completion_tokens"] += int(usage.get("completion_tokens", 0) or 0)
        totals["search_calls"] += int(usage.get("search_calls", 0) or 0)
        totals["cost_usd"] += float(usage.get("cost_usd", 0.0) or 0.0)
    totals["cost_usd"] = round(totals["cost_usd"], 6)
    return totals


def duplicate_secondary_across_sides(packet: dict) -> bool:
    side_a: list[str] = []
    side_b: list[str] = []
    for ref in packet.get("evidence_refs", []):
        ref_id = str(ref.get("ref_id") or "")
        summary = str(ref.get("summary") or "").strip()
        if not summary or not ref_id.startswith("secondary_"):
            continue
        if ref_id.startswith("secondary_a_"):
            side_a.append(summary)
        elif ref_id.startswith("secondary_b_"):
            side_b.append(summary)
    return bool(set(side_a) & set(side_b))


def apply_guards(
    *,
    source_row: dict,
    control_row: dict,
    case: dict,
    packet: dict,
) -> tuple[dict, str | None]:
    row = deepcopy(source_row)
    row["contender_id"] = CONTENDER_ID
    features = build_features(case, packet)
    control_verdict = control_row["normalized_output"]["verdict"]

    if not row.get("schema_valid", True):
        base = copy_control_row(control_row)
        base["contender_id"] = CONTENDER_ID
        base["usage"] = row.get("usage") or base.get("usage", {})
        base["timing_ms"] = row.get("timing_ms", 0)
        base["guardrail_applied"] = "invalid_output_reuse_control"
        return base, "invalid_output_reuse_control"

    verdict = row["normalized_output"]["verdict"]

    if control_verdict == "FLAGGED" and verdict == "SKIP":
        row["normalized_output"]["verdict"] = "FLAGGED"
        row["normalized_output"]["confidence"] = min(
            float(row["normalized_output"].get("confidence", 0.0) or 0.0),
            0.82,
        )
        row["normalized_output"]["follow_up"] = "keep_flagged_not_skip"
        row["guardrail_applied"] = "keep_flagged_not_skip"
        return row, "keep_flagged_not_skip"

    if (
        verdict == "MERGE"
        and control_verdict == "FLAGGED"
        and features.shared_surname_split
        and duplicate_secondary_across_sides(packet)
    ):
        row["normalized_output"]["verdict"] = "FLAGGED"
        row["normalized_output"]["confidence"] = min(
            float(row["normalized_output"].get("confidence", 0.0) or 0.0),
            0.82,
        )
        row["normalized_output"]["follow_up"] = (
            "merge_veto_duplicate_secondary_on_shared_surname"
        )
        row["guardrail_applied"] = (
            "merge_veto_duplicate_secondary_on_shared_surname"
        )
        return row, "merge_veto_duplicate_secondary_on_shared_surname"

    if (
        verdict == "SKIP"
        and control_verdict == "SKIP"
        and features.shared_surname_split
        and features.same_country
        and features.same_region
        and features.containment == "b_in_a"
        and features.has_subset_match
        and features.exact_overlap_count == 0
        and features.anchor_overlap_count >= 1
        and features.wine_count_large <= 12
        and features.shared_core_token_count == 1
        and not features.country_conflict
    ):
        row["normalized_output"]["verdict"] = "FLAGGED"
        row["normalized_output"]["confidence"] = min(
            float(row["normalized_output"].get("confidence", 0.0) or 0.0),
            0.82,
        )
        row["normalized_output"]["follow_up"] = "generic_stub_ambiguity_flag"
        row["guardrail_applied"] = "generic_stub_ambiguity_flag"
        return row, "generic_stub_ambiguity_flag"

    return row, None


def render_case_matrix(
    *,
    case_ids: list[str],
    cases_by_id: dict[str, dict],
    control_rows_by_case: dict[str, dict],
    source_rows_by_case: dict[str, dict],
    contender_rows_by_case: dict[str, dict],
) -> list[str]:
    lines = [
        "| Case | Expected | Control | Source Hybrid | Guarded Contender |",
        "|---|---|---|---|---|",
    ]
    for case_id in case_ids:
        expected = cases_by_id[case_id]["expected_verdict"]
        control_verdict = control_rows_by_case[case_id]["normalized_output"]["verdict"]
        source_verdict = source_rows_by_case[case_id]["normalized_output"]["verdict"]
        contender_verdict = contender_rows_by_case[case_id]["normalized_output"]["verdict"]
        lines.append(
            f"| `{case_id}` | {expected} | {control_verdict} | {source_verdict} | {contender_verdict} |"
        )
    return lines


def render_markdown(report: dict) -> str:
    score = report["score"]
    summary = report["summary"]
    lines: list[str] = []
    lines.append("# Session 10.8 - hybrid guarded frontier")
    lines.append("")
    lines.append(f"- Generated: {report['generated_at']}")
    lines.append(f"- Run name: `{report['run_name']}`")
    lines.append(f"- Source run: `{report['source_run_name']}`")
    lines.append(f"- Source model: `{report['source_model']}`")
    lines.append(f"- Method version: `{METHOD_VERSION}`")
    lines.append(f"- Estimated inherited model spend: `${report['usage']['cost_usd']:.4f}`")
    lines.append("")
    lines.append("## Goal")
    lines.append("")
    lines.append(
        "Test whether a conservative post-judge ambiguity guard can turn the promising hybrid contender into a real production-gate survivor without widening policy or adding another heavy pass."
    )
    lines.append("")
    lines.append("## Guard Changes")
    lines.append("")
    for item in report["guard_changes"]:
        lines.append(
            f"- `{item['case_id']}`: `{item['source_verdict']}` -> `{item['guarded_verdict']}` via `{item['guardrail']}`"
        )
    lines.append("")
    lines.append("## Scorecard")
    lines.append("")
    lines.append(
        f"- Counts: false merge `{score['counts']['false_merge']}`, hard missed `{score['counts']['hard_missed_merge']}`, soft missed `{score['counts']['soft_missed_merge']}`, safe flag `{score['counts']['safe_flag']}`."
    )
    lines.append(
        f"- Rates: exact acc `{score['rates']['exact_verdict_accuracy']:.4f}`, merge capture `{score['rates']['merge_capture_rate']:.4f}`, survivor acc `{score['rates']['survivor_accuracy']:.4f}`, flag rate `{score['rates']['flag_rate_total']:.4f}`."
    )
    lines.append(
        f"- Gates: production `{score['gates']['production']['status']}`, fallback `{score['gates']['fallback']['status']}`."
    )
    lines.append("")
    lines.append("## Delta Vs Control")
    lines.append("")
    lines.append(
        f"- Recoveries vs control: {render_case_id_list(summary['recovered_case_ids'])}"
    )
    lines.append(
        f"- Blind-core recoveries vs control: {render_case_id_list(summary['blind_core_recovered_case_ids'])}"
    )
    lines.append(
        f"- New false merges vs control: {render_case_id_list(summary['false_merge_case_ids'])}"
    )
    lines.append("")
    lines.append("## Touched Cases")
    lines.append("")
    lines.extend(report["touched_case_matrix"])
    lines.append("")
    lines.append("## Recommendation")
    lines.append("")
    lines.append(f"- Status: `{report['recommendation']['status']}`")
    lines.append(f"- Reason: {report['recommendation']['reason']}")
    return "\n".join(lines).rstrip() + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(description="Apply guarded-follow-on rules to a hybrid frontier run")
    parser.add_argument("--benchmark", type=Path, default=DEFAULT_BENCHMARK)
    parser.add_argument("--packet-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--source-report", type=Path, required=True)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--run-name", default=RUN_NAME_DEFAULT)
    parser.add_argument("--report-json", type=Path, default=DEFAULT_REPORT_JSON)
    parser.add_argument("--report-md", type=Path, default=DEFAULT_REPORT_MD)
    args = parser.parse_args()

    source_report_path = args.source_report.resolve()
    source_report = json.loads(source_report_path.read_text(encoding="utf-8"))
    source_normalized = REPO_ROOT / source_report["normalized_path"]
    source_rows = load_jsonl(source_normalized)
    source_rows_by_case = {row["case_id"]: row for row in source_rows}

    benchmark_payload = load_benchmark_payload(args.benchmark)
    cases_by_id = {case["case_id"]: case for case in benchmark_payload["cases"]}
    full_case_ids = [case["case_id"] for case in benchmark_payload["cases"]]
    full_packets, _, packet_validation = ensure_visible_packets(
        args.packet_dir,
        args.benchmark,
        force_rebuild=False,
    )
    packet_lookup = {packet["packet_id"]: packet for packet in full_packets}
    control_rows_by_case = load_rows_by_case(CONTROL_NORMALIZED)

    contender_rows_by_case: dict[str, dict] = {}
    guard_changes: list[dict] = []

    for case_id in full_case_ids:
        case = cases_by_id[case_id]
        packet = packet_lookup[f"producer_pair_{case['pair_id']}_v2"]
        source_row = source_rows_by_case[case_id]
        control_row = control_rows_by_case[case_id]
        guarded_row, guardrail = apply_guards(
            source_row=source_row,
            control_row=control_row,
            case=case,
            packet=packet,
        )
        guarded_row["contender_id"] = CONTENDER_ID
        contender_rows_by_case[case_id] = guarded_row
        if guardrail:
            guard_changes.append(
                {
                    "case_id": case_id,
                    "source_verdict": source_row["normalized_output"]["verdict"],
                    "guarded_verdict": guarded_row["normalized_output"]["verdict"],
                    "guardrail": guardrail,
                }
            )

    ordered_rows = [contender_rows_by_case[case_id] for case_id in full_case_ids]
    run_root = args.output_root / args.run_name
    run_root.mkdir(parents=True, exist_ok=True)
    normalized_path = run_root / f"{CONTENDER_ID}.jsonl"
    score_json_path = run_root / f"{CONTENDER_ID}.score.json"
    score_md_path = run_root / f"{CONTENDER_ID}.score.md"
    write_jsonl(normalized_path, ordered_rows)

    scored = score_run(
        benchmark_payload=benchmark_payload,
        normalized_paths=[normalized_path],
        full_packets=full_packets,
        output_json=score_json_path,
        output_md=score_md_path,
        run_name=args.run_name,
    )
    contender_score = scored["contenders"][0]
    summary = summarize_rows(
        proof_case_ids=full_case_ids,
        cases_by_id=cases_by_id,
        rows_by_case=contender_rows_by_case,
        control_rows_by_case=control_rows_by_case,
    )

    touched_case_ids = [item["case_id"] for item in guard_changes]
    touched_case_matrix = render_case_matrix(
        case_ids=touched_case_ids,
        cases_by_id=cases_by_id,
        control_rows_by_case=control_rows_by_case,
        source_rows_by_case=source_rows_by_case,
        contender_rows_by_case=contender_rows_by_case,
    )

    if contender_score["gates"]["production"]["status"] == "pass":
        recommendation = {
            "status": "candidate_clears_frozen_production_gate",
            "reason": (
                "The guarded frontier follow-on cleared the frozen production gate by preventing the frontier layer from hardening or merging beyond what the visible evidence can safely support."
            ),
        }
    elif contender_score["gates"]["fallback"]["status"] == "pass":
        recommendation = {
            "status": "candidate_improves_to_fallback_only",
            "reason": (
                "The guarded frontier follow-on preserved fallback safety but still missed the frozen production gate."
            ),
        }
    else:
        recommendation = {
            "status": "candidate_failed_full_gate",
            "reason": (
                "The guarded frontier follow-on did not clear the frozen production or fallback gate."
            ),
        }

    report = {
        "generated_at": now_iso(),
        "run_name": args.run_name,
        "source_run_name": source_report["run_name"],
        "source_model": source_report["model"],
        "method_version": METHOD_VERSION,
        "source_report": str(source_report_path.relative_to(REPO_ROOT)),
        "packet_validation": {
            "packet_version": packet_validation["packet_version"],
            "packet_count": packet_validation["packet_count"],
            "hidden_field_leaks": packet_validation["hidden_field_leaks"],
        },
        "normalized_path": str(normalized_path.relative_to(REPO_ROOT)),
        "score_json_path": str(score_json_path.relative_to(REPO_ROOT)),
        "score_md_path": str(score_md_path.relative_to(REPO_ROOT)),
        "guard_changes": guard_changes,
        "usage": accumulate_usage(ordered_rows),
        "summary": summary,
        "score": contender_score,
        "touched_case_matrix": touched_case_matrix,
        "recommendation": recommendation,
    }
    write_json(args.report_json, report)
    args.report_md.write_text(render_markdown(report), encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
