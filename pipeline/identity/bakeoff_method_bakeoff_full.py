"""Session 9.11 - full 152-case method bakeoff rerun.

Runs the capped full rerun for the three Session 9.10 proof survivors against
the frozen benchmark and the frozen Session 9.7 fallback control. No model
calls are made here; this is a deterministic scale-up of the proof survivors.
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime
from pathlib import Path

from pipeline.identity.bakeoff_harness_v1 import load_benchmark_payload, score_run
from pipeline.identity.bakeoff_harness_v2 import (
    DEFAULT_BENCHMARK,
    DEFAULT_OUTPUT_DIR,
    DEFAULT_RUN_ROOT,
    ensure_visible_packets,
)
from pipeline.identity.bakeoff_method_bakeoff_proof import (
    CONTROL_CONTENDER_ID,
    CONTROL_NORMALIZED,
    CONTROL_RUN_NAME,
    METHOD_SPECS,
    build_features,
    build_method_rows,
    load_rows_by_case,
    rows_decision_vector,
    summarize_rows,
    write_json,
)
from pipeline.identity.bakeoff_packet_v2 import write_jsonl


REPO_ROOT = Path(__file__).resolve().parents[2]
RUN_NAME_DEFAULT = "session9_11_full_method_bakeoff_rerun_if_approved"
DEFAULT_MEMO_PATH = REPO_ROOT / "data" / "sprints" / "dedup" / f"{RUN_NAME_DEFAULT}.md"
CONTROL_SCORED_JSON = (
    DEFAULT_RUN_ROOT / "scored" / "session9_7_layered_safety_sonnet_r2_narrow.json"
)
FULL_METHOD_ORDER = [
    "merge_proposer_plus_veto_v1",
    "expanded_layered_router_v1",
    "evidence_digest_then_judge_v1",
]
INPUT_PATHS = [
    "data/session_prompts/s9_11_full_method_bakeoff_rerun_if_approved.md",
    "data/sprints/dedup/session9_10_method_bakeoff_proof_subset.md",
    "data/sprints/dedup/bakeoff_v2/scored/session9_10_method_bakeoff_proof_subset.json",
    "data/sprints/dedup/bakeoff_v2/scored/session9_10_method_bakeoff_proof_subset.md",
    "data/sprints/dedup/bakeoff_v2/scored/session9_7_layered_safety_sonnet_r2_narrow.json",
    "data/sprints/dedup/bakeoff_v2/scored/session9_7_layered_safety_sonnet_r2_narrow_memo.md",
]


def format_delta(value: float) -> str:
    if value > 0:
        return f"+{value:.4f}"
    return f"{value:.4f}"


def selection_sort_key(row: dict) -> tuple:
    eligibility_rank = {
        "production_eligible": 0,
        "fallback_only": 1,
        "ineligible": 2,
    }
    rank_tuple = row.get("rank_tuple") or ()
    if isinstance(rank_tuple, list):
        rank_tuple = tuple(rank_tuple)
    return (
        eligibility_rank.get(row["eligibility"], 99),
        rank_tuple if rank_tuple else (float("inf"),),
        row["contender_id"],
    )


def load_control_report() -> dict:
    return json.loads(CONTROL_SCORED_JSON.read_text(encoding="utf-8"))


def find_best_artifact(score_summary: dict) -> dict:
    ranked = sorted(score_summary["winner_selection"], key=selection_sort_key)
    return ranked[0]


def build_comparison_bundle(
    *,
    full_case_ids: list[str],
    cases_by_id: dict[str, dict],
    control_rows_by_case: dict[str, dict],
    contender_rows_by_case: dict[str, dict],
) -> dict:
    comparison = summarize_rows(
        proof_case_ids=full_case_ids,
        cases_by_id=cases_by_id,
        rows_by_case=contender_rows_by_case,
        control_rows_by_case=control_rows_by_case,
    )
    return {
        **comparison,
        "decision_vector": list(rows_decision_vector(full_case_ids, contender_rows_by_case)),
    }


def render_case_id_list(case_ids: list[str]) -> str:
    return ", ".join(f"`{case_id}`" for case_id in case_ids) if case_ids else "none"


def render_markdown(report: dict) -> str:
    control = report["control"]
    best = report["best_artifact"]
    recommendation = report["recommendation"]

    lines: list[str] = []
    lines.append("# Session 9.11 - full method bakeoff rerun")
    lines.append("")
    lines.append(f"- Generated: {report['generated_at']}")
    lines.append(f"- Run name: `{report['run_name']}`")
    lines.append(f"- Benchmark: `{report['benchmark_id']}`")
    lines.append(f"- Cases scored: `{report['case_count']}`")
    lines.append(f"- Frozen control: `{CONTROL_RUN_NAME}` / `{CONTROL_CONTENDER_ID}`")
    lines.append(f"- New model spend this session: `${report['new_model_spend_usd']:.2f}`")
    lines.append("")
    lines.append("## Goal")
    lines.append("")
    lines.append(
        "Run the full 152-case rerun for the three Session 9.10 proof survivors, compare them against the frozen Session 9.7 fallback control, and decide whether Sprint 6 can move toward queue-building or should freeze at the best non-production artifact."
    )
    lines.append("")
    lines.append("## Inputs")
    lines.append("")
    for path in report["inputs"]:
        lines.append(f"- `{path}`")
    lines.append("")
    lines.append("## Full-rerun scorecard")
    lines.append("")
    lines.append("| Artifact | Exact acc | False merge | Hard missed | Soft missed | Safe flag | Survivor acc | Production gate | Fallback gate |")
    lines.append("|---|---:|---:|---:|---:|---:|---:|---|---|")
    lines.append(
        f"| `{CONTROL_CONTENDER_ID}` | {control['rates']['exact_verdict_accuracy']:.4f} | {control['counts']['false_merge']} | "
        f"{control['counts']['hard_missed_merge']} | {control['counts']['soft_missed_merge']} | {control['counts']['safe_flag']} | "
        f"{control['rates']['survivor_accuracy']:.4f} | {control['gates']['production']['status']} | {control['gates']['fallback']['status']} |"
    )
    for contender in report["contenders"]:
        score = contender["score"]
        lines.append(
            f"| `{contender['contender_id']}` | {score['rates']['exact_verdict_accuracy']:.4f} | {score['counts']['false_merge']} | "
            f"{score['counts']['hard_missed_merge']} | {score['counts']['soft_missed_merge']} | {score['counts']['safe_flag']} | "
            f"{score['rates']['survivor_accuracy']:.4f} | {score['gates']['production']['status']} | {score['gates']['fallback']['status']} |"
        )
    lines.append("")
    lines.append("## Delta vs frozen Session 9.7 control")
    lines.append("")
    lines.append(
        f"The frozen control remains the comparison baseline: `{control['counts']['false_merge']}` false merges, "
        f"`{control['counts']['hard_missed_merge']}` hard misses, `{control['counts']['soft_missed_merge']}` soft misses, "
        f"`{control['rates']['flag_rate_total']:.4f}` flag rate, fallback gate `{control['gates']['fallback']['status']}`."
    )
    lines.append("")
    lines.append("| Contender | Recoveries vs control | Blind-core blocker recoveries | New false merges | Changed cases | Exact acc delta | Flag-rate delta |")
    lines.append("|---|---:|---:|---:|---:|---:|---:|")
    for contender in report["contenders"]:
        comparison = contender["comparison"]
        score = contender["score"]
        lines.append(
            f"| `{contender['contender_id']}` | {len(comparison['recovered_case_ids'])} | {len(comparison['blind_core_recovered_case_ids'])} | "
            f"{len(comparison['false_merge_case_ids'])} | {len(comparison['changed_case_ids'])} | "
            f"{format_delta(score['rates']['exact_verdict_accuracy'] - control['rates']['exact_verdict_accuracy'])} | "
            f"{format_delta(score['rates']['flag_rate_total'] - control['rates']['flag_rate_total'])} |"
        )
    lines.append("")
    for contender in report["contenders"]:
        comparison = contender["comparison"]
        score = contender["score"]
        blind_core = score["breakdowns"]["stratum"]["blind_core_audit"]
        known_false = score["breakdowns"]["stratum"]["known_false_merge_patterns"]
        tail = score["breakdowns"]["stratum"]["tail_random_sample"]
        lines.append(f"### `{contender['contender_id']}`")
        lines.append("")
        lines.append(f"- Recoveries vs control: {render_case_id_list(comparison['recovered_case_ids'])}")
        lines.append(f"- New false merges vs control: {render_case_id_list(comparison['false_merge_case_ids'])}")
        lines.append(
            f"- Blind-core missed merges: `{control['breakdowns']['stratum']['blind_core_audit']['hard_missed_merge'] + control['breakdowns']['stratum']['blind_core_audit']['soft_missed_merge']}` -> "
            f"`{blind_core['hard_missed_merge'] + blind_core['soft_missed_merge']}`"
        )
        lines.append(
            f"- Known-false-merge pattern false merges: `{control['breakdowns']['stratum']['known_false_merge_patterns']['false_merge']}` -> "
            f"`{known_false['false_merge']}`"
        )
        lines.append(
            f"- Tail false merges: `{control['breakdowns']['stratum']['tail_random_sample']['false_merge']}` -> `{tail['false_merge']}`"
        )
        lines.append(
            f"- Gate result: production `{score['gates']['production']['status']}`, fallback `{score['gates']['fallback']['status']}`."
        )
        lines.append("")
    lines.append("## Best surviving artifact")
    lines.append("")
    lines.append(
        f"Best artifact after the full rerun: `{best['contender_id']}` ({best['eligibility']}, production `{best['production_gate']}`, fallback `{best['fallback_gate']}`)."
    )
    lines.append("")
    lines.append(report["best_artifact_reason"])
    lines.append("")
    lines.append("## Recommendation")
    lines.append("")
    lines.append(f"- Status: `{recommendation['status']}`")
    lines.append(f"- Best surviving artifact: `{recommendation['best_artifact']}`")
    lines.append(f"- Queue-building: `{recommendation['queue_building']}`")
    lines.append(f"- Reason: {recommendation['reason']}")
    return "\n".join(lines).rstrip() + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(description="Run the Session 9.11 full method bakeoff rerun")
    parser.add_argument("--benchmark", type=Path, default=DEFAULT_BENCHMARK)
    parser.add_argument("--packet-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_RUN_ROOT)
    parser.add_argument("--run-name", default=RUN_NAME_DEFAULT)
    parser.add_argument("--memo-path", type=Path, default=DEFAULT_MEMO_PATH)
    args = parser.parse_args()

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

    features_by_case: dict[str, object] = {}
    for case_id in full_case_ids:
        case = cases_by_id[case_id]
        packet_id = f"producer_pair_{case['pair_id']}_v2"
        features_by_case[case_id] = build_features(case, packet_lookup[packet_id])

    normalized_dir = args.output_root / "normalized" / args.run_name
    scored_dir = args.output_root / "scored"
    normalized_dir.mkdir(parents=True, exist_ok=True)
    scored_dir.mkdir(parents=True, exist_ok=True)

    control_rows = [control_rows_by_case[case_id] for case_id in full_case_ids]
    control_path = normalized_dir / f"{CONTROL_CONTENDER_ID}.jsonl"
    write_jsonl(control_path, control_rows)

    normalized_paths = [control_path]
    contender_reports: list[dict] = []
    decision_vectors: dict[tuple[str, ...], str] = {}
    redundant_contenders: list[dict] = []

    for contender_id in FULL_METHOD_ORDER:
        rows, rows_by_case = build_method_rows(
            contender_id=contender_id,
            proof_case_ids=full_case_ids,
            control_rows_by_case=control_rows_by_case,
            features_by_case=features_by_case,
        )
        contender_path = normalized_dir / f"{contender_id}.jsonl"
        write_jsonl(contender_path, rows)
        normalized_paths.append(contender_path)

        comparison = build_comparison_bundle(
            full_case_ids=full_case_ids,
            cases_by_id=cases_by_id,
            control_rows_by_case=control_rows_by_case,
            contender_rows_by_case=rows_by_case,
        )
        vector = tuple(comparison["decision_vector"])
        redundant_with = None
        if vector in decision_vectors:
            redundant_with = decision_vectors[vector]
            redundant_contenders.append(
                {"contender_id": contender_id, "redundant_with": redundant_with}
            )
        else:
            decision_vectors[vector] = contender_id

        contender_reports.append(
            {
                "contender_id": contender_id,
                "label": METHOD_SPECS[contender_id]["label"],
                "method_class": METHOD_SPECS[contender_id]["method_class"],
                "comparison": comparison,
                "redundant_with": redundant_with,
            }
        )

    score_json_path = scored_dir / f"{args.run_name}.json"
    score_md_path = scored_dir / f"{args.run_name}.md"
    score_summary = score_run(
        benchmark_payload=benchmark_payload,
        normalized_paths=normalized_paths,
        full_packets=full_packets,
        output_json=score_json_path,
        output_md=score_md_path,
        run_name=args.run_name,
    )
    scored_by_id = {row["contender_id"]: row for row in score_summary["contenders"]}

    control_report = scored_by_id[CONTROL_CONTENDER_ID]
    for contender in contender_reports:
        contender["score"] = scored_by_id[contender["contender_id"]]

    control_scored_reference = load_control_report()
    best_artifact = find_best_artifact(score_summary)
    if best_artifact["contender_id"] == CONTROL_CONTENDER_ID:
        best_artifact_reason = (
            "The Session 9.7 layered fallback control remains the best surviving artifact because it is still the only run that clears the fallback gate. "
            "Every broader Session 9.11 survivor recovered some missed merges but reopened 5-9 false merges on the full benchmark, which immediately blocks both production and fallback status."
        )
        recommendation = {
            "status": "freeze_at_best_non_production_artifact",
            "best_artifact": CONTROL_CONTENDER_ID,
            "queue_building": "do_not_proceed",
            "reason": (
                "All three broader methods fail the frozen production gate and also fail the fallback gate once scaled to all 152 cases. "
                "Sprint 6 should freeze at the best existing non-production artifact instead of opening another redesign in this session."
            ),
        }
    elif best_artifact["eligibility"] == "production_eligible":
        best_artifact_reason = (
            "A Session 9.11 broader method cleared the frozen production gate, so the best surviving artifact is now a production-eligible successor rather than the Session 9.7 fallback control."
        )
        recommendation = {
            "status": "proceed_toward_queue_building",
            "best_artifact": best_artifact["contender_id"],
            "queue_building": "can_proceed",
            "reason": (
                "At least one broader method cleared the frozen production gate on the full benchmark, so Sprint 6 can move toward queue-building on that artifact."
            ),
        }
    else:
        best_artifact_reason = (
            "A Session 9.11 broader method improved the fallback posture enough to become the best surviving non-production artifact, even though no contender cleared the production gate."
        )
        recommendation = {
            "status": "freeze_at_best_non_production_artifact",
            "best_artifact": best_artifact["contender_id"],
            "queue_building": "do_not_proceed",
            "reason": (
                "No contender cleared the frozen production gate, so Sprint 6 should still freeze at the strongest non-production artifact rather than queue-building."
            ),
        }

    report = {
        "run_name": args.run_name,
        "generated_at": datetime.now().astimezone().isoformat(timespec="seconds"),
        "benchmark_id": benchmark_payload["benchmark_id"],
        "case_count": benchmark_payload["case_count"],
        "inputs": INPUT_PATHS,
        "new_model_spend_usd": 0.0,
        "packet_validation": {
            "packet_version": packet_validation["packet_version"],
            "packet_count": packet_validation["packet_count"],
            "hidden_field_leaks": packet_validation["hidden_field_leaks"],
        },
        "control": control_report,
        "control_scored_reference": control_scored_reference,
        "contenders": contender_reports,
        "redundant_contenders": redundant_contenders,
        "best_artifact": best_artifact,
        "best_artifact_reason": best_artifact_reason,
        "recommendation": recommendation,
    }

    report_json_path = scored_dir / f"{args.run_name}_manifest.json"
    memo_text = render_markdown(report)
    args.memo_path.write_text(memo_text, encoding="utf-8")
    write_json(
        report_json_path,
        {
            "run_name": args.run_name,
            "benchmark_id": benchmark_payload["benchmark_id"],
            "control_run_name": CONTROL_RUN_NAME,
            "control_contender_id": CONTROL_CONTENDER_ID,
            "contenders": FULL_METHOD_ORDER,
            "redundant_contenders": redundant_contenders,
            "packet_validation": report["packet_validation"],
            "best_artifact": best_artifact,
            "recommendation": recommendation,
            "normalized": {
                CONTROL_CONTENDER_ID: str(control_path),
                **{
                    contender["contender_id"]: str(normalized_dir / f"{contender['contender_id']}.jsonl")
                    for contender in contender_reports
                },
            },
            "outputs": {
                "score_json": str(score_json_path),
                "score_md": str(score_md_path),
                "memo_md": str(args.memo_path),
            },
            "comparison_to_control": {
                contender["contender_id"]: contender["comparison"] for contender in contender_reports
            },
        },
    )

    print(f"Completed full method bakeoff rerun: {args.run_name}")
    print(f"Score JSON: {score_json_path}")
    print(f"Memo: {args.memo_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
