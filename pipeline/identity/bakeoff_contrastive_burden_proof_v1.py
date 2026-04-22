"""Session 10.8 - contrastive burden adjudicator proof subset.

This script runs one new model-based method on the frozen Session 9.10 proof
subset before any full rerun:

- `contrastive_burden_adjudicator_v1`

The method uses the existing visible v2 packets, but changes the reasoning
contract:

1. Build the strongest merge story.
2. Build the strongest non-merge story.
3. Merge only if the merge story clearly beats the strongest alternative.

No benchmark mutation, DB writes, or second heavy pass are allowed here.
"""

from __future__ import annotations

import argparse
import json
import re
import time
from collections import Counter
from copy import deepcopy
from datetime import datetime
from pathlib import Path

import anthropic

from pipeline.identity.bakeoff_harness_v1 import load_benchmark_payload, valid_rule_ids
from pipeline.identity.bakeoff_harness_v2 import DEFAULT_BENCHMARK, DEFAULT_OUTPUT_DIR, ensure_visible_packets
from pipeline.identity.bakeoff_method_bakeoff_proof import (
    BLIND_CORE_BLOCKER_CASE_IDS,
    CONTROL_CONTENDER_ID,
    CONTROL_NORMALIZED,
    CONTROL_RUN_NAME,
    INPUT_PATHS,
    METHOD_ORDER,
    PROOF_GROUPS,
    build_case_table,
    load_rows_by_case,
    render_case_id_list,
    summarize_rows,
    validate_groups,
    write_json,
)
from pipeline.identity.bakeoff_packet_v2 import canonical_json_dumps, write_jsonl
from pipeline.identity.bakeoff_run_v2 import extract_json_object
from pipeline.identity.selector_proof_v1 import anthropic_text, anthropic_usage_to_dict, load_section_11
from pipeline.lib.db import get_env
from pipeline.lib.models import SONNET_MODEL


REPO_ROOT = Path(__file__).resolve().parents[2]
RUN_NAME_DEFAULT = "session10_8_contrastive_burden_proof_v1"
DEFAULT_OUTPUT_ROOT = REPO_ROOT / "data" / "sprints" / "identity-er" / "method_bakeoff"
DEFAULT_REPORT_JSON = DEFAULT_OUTPUT_ROOT / f"{RUN_NAME_DEFAULT}.json"
DEFAULT_REPORT_MD = DEFAULT_OUTPUT_ROOT / f"{RUN_NAME_DEFAULT}.md"
CONTENDER_ID = "contrastive_burden_adjudicator_v1"


def now_iso() -> str:
    return datetime.now().astimezone().isoformat(timespec="seconds")


def zero_usage() -> dict:
    return {
        "prompt_tokens": 0,
        "completion_tokens": 0,
        "search_calls": 0,
        "cost_usd": 0.0,
    }


def sanitize_error(text: str) -> str:
    cleaned = re.sub(r"\s+", "_", text.strip().lower())
    cleaned = re.sub(r"[^a-z0-9_:/.-]+", "", cleaned)
    return cleaned[:120] or "unknown_error"


def build_runtime_error_row(wrapper: dict, message: str, timing_ms: int) -> dict:
    return {
        "benchmark_id": wrapper["benchmark_id"],
        "case_id": wrapper["case_id"],
        "packet_id": wrapper["packet_visible"]["packet_id"],
        "contender_id": CONTENDER_ID,
        "raw_output": None,
        "runtime_error": sanitize_error(message),
        "timing_ms": timing_ms,
        "usage": zero_usage(),
    }


def build_system_prompt(section_11: str) -> str:
    return f"""You are `contrastive_burden_adjudicator_v1`, a merge-only producer-dedup adjudicator.

You will receive exactly one JSON request wrapper containing:
- `packet_visible`
- `allowed_ref_ids`

Use ONLY the evidence inside that wrapper. Do not use outside knowledge, live
search, or hidden benchmark speculation.

Apply the producer identity rules strictly:

{section_11}

Contrastive burden method:
- First build the strongest `MERGE` story the visible packet can support.
- Then build the strongest competing non-merge story from these families:
  - family or generational split
  - umbrella brand, reserve line, or product-tier confusion
  - owner, operator, importer, or merchant relationship without identity
  - cross-country same-name rows without continuity
- Return `MERGE` only if the merge story is stronger than every competing
  non-merge story and actually rebuts the main contradiction evidence.

Decision burden:
- Do not merge from geography plus catalog overlap alone.
- Do not merge from shared surname alone.
- Do not merge from unresolved official evidence unless the packet still shows a
  decisive same-identity bridge such as:
  - near-exact name plus exact overlap plus same country
  - containment plus a thin subset footprint without a stronger split story
  - repeated secondary evidence on both sides that clearly points to one
    producer identity rather than a family or portfolio relationship
- If the strongest non-merge story is still plausible and unrebutted, do not
  return `MERGE`.
- Prefer `FLAGGED` when same identity remains plausible but not proven.
- Prefer `SKIP` when the packet more strongly supports distinct-but-related or
  clearly distinct identities.

Output contract requirements:
- Return exactly one JSON object and nothing else.
- Allowed `verdict` values: `MERGE`, `SKIP`, `FLAGGED`.
- `confidence` must be a number from 0 to 1.
- `rule_ids` must cite real Section 11 rule ids such as `11.1`, `11.4.h`,
  `11.4.m`, `11.6`.
- Every output must include at least one `key_support_refs` item chosen only
  from `allowed_ref_ids`.
- If `verdict` is `MERGE` and the packet includes a recommended survivor, set
  `survivor_producer_id` to one of the listed candidate ids.
- If `verdict` is `SKIP` or `FLAGGED`, set `survivor_producer_id` to null.
- `reason` should name the winning story and the strongest losing story in one
  short paragraph.
- `follow_up` should be null unless the row truly needs more evidence.

Return this exact shape:
{{
  "packet_id": "<packet id>",
  "verdict": "MERGE | SKIP | FLAGGED",
  "confidence": 0.0,
  "rule_ids": ["11.x"],
  "reason": "short evidence-grounded explanation",
  "key_support_refs": ["ref_id"],
  "key_contradiction_refs": ["ref_id"],
  "survivor_producer_id": "uuid_or_null",
  "follow_up": null
}}
"""


def build_wrapper(benchmark_id: str, case: dict, visible_packet: dict) -> dict:
    return {
        "benchmark_id": benchmark_id,
        "case_id": case["case_id"],
        "contender_id": CONTENDER_ID,
        "instructions_version": "contrastive_burden_adjudicator_v1",
        "packet_visible": visible_packet,
        "allowed_ref_ids": visible_packet["envelope"]["allowed_ref_ids"],
        "output_contract_version": "adjudication_output_v2",
        "allow_tools": False,
        "temperature": 0,
    }


def fail_closed_row(raw_row: dict, error_code: str, usage: dict) -> dict:
    return {
        "benchmark_id": raw_row["benchmark_id"],
        "case_id": raw_row["case_id"],
        "packet_id": raw_row["packet_id"],
        "contender_id": raw_row["contender_id"],
        "normalized_output": {
            "packet_id": raw_row["packet_id"],
            "verdict": "FLAGGED",
            "confidence": 0.0,
            "rule_ids": [],
            "reason": "Output failed contract validation and was coerced to FLAGGED.",
            "key_support_refs": [],
            "key_contradiction_refs": [],
            "survivor_producer_id": None,
            "follow_up": error_code,
        },
        "schema_valid": False,
        "citation_integrity": False,
        "rule_trace_valid": False,
        "timing_ms": raw_row.get("timing_ms", 0),
        "usage": usage,
        "normalization_error": error_code,
    }


def normalize_one(raw_row: dict, packet: dict, allowed_rule_ids: set[str]) -> dict:
    usage = raw_row.get("usage") or zero_usage()
    runtime_error = raw_row.get("runtime_error")
    if runtime_error:
        return fail_closed_row(raw_row, f"runtime_error:{runtime_error}", usage)
    output = raw_row.get("raw_output")
    if isinstance(output, str):
        try:
            output = json.loads(output)
        except json.JSONDecodeError:
            return fail_closed_row(raw_row, "invalid_json", usage)
    if not isinstance(output, dict):
        return fail_closed_row(raw_row, "invalid_payload", usage)

    required = {
        "packet_id",
        "verdict",
        "confidence",
        "rule_ids",
        "reason",
        "key_support_refs",
        "key_contradiction_refs",
        "survivor_producer_id",
        "follow_up",
    }
    missing = sorted(required - set(output))
    if missing:
        return fail_closed_row(raw_row, f"missing_fields:{','.join(missing)}", usage)
    if output["packet_id"] != raw_row["packet_id"]:
        return fail_closed_row(raw_row, "packet_id_mismatch", usage)
    if output["verdict"] not in {"MERGE", "SKIP", "FLAGGED"}:
        return fail_closed_row(raw_row, "illegal_verdict", usage)
    if not isinstance(output["confidence"], (int, float)) or not (0 <= float(output["confidence"]) <= 1):
        return fail_closed_row(raw_row, "invalid_confidence", usage)
    if not isinstance(output["rule_ids"], list) or not all(isinstance(item, str) for item in output["rule_ids"]):
        return fail_closed_row(raw_row, "invalid_rule_ids", usage)
    if not isinstance(output["key_support_refs"], list) or not all(isinstance(item, str) for item in output["key_support_refs"]):
        return fail_closed_row(raw_row, "invalid_support_refs", usage)
    if not isinstance(output["key_contradiction_refs"], list) or not all(isinstance(item, str) for item in output["key_contradiction_refs"]):
        return fail_closed_row(raw_row, "invalid_contradiction_refs", usage)
    if not isinstance(output["reason"], str) or not output["reason"].strip():
        return fail_closed_row(raw_row, "missing_reason", usage)
    if output["follow_up"] is not None and not isinstance(output["follow_up"], str):
        return fail_closed_row(raw_row, "invalid_follow_up", usage)

    candidate_ids = {
        item["producer_id"]
        for item in packet["evidence"]["survivor_if_merge"].get("candidate_order", [])
        if item.get("producer_id")
    }
    required_survivor = packet["evidence"]["survivor_if_merge"].get("recommended_survivor_producer_id")
    if output["verdict"] == "MERGE":
        if required_survivor and not output["survivor_producer_id"]:
            return fail_closed_row(raw_row, "missing_required_survivor", usage)
        if output["survivor_producer_id"] and output["survivor_producer_id"] not in candidate_ids:
            return fail_closed_row(raw_row, "invalid_survivor_candidate", usage)
    if output["verdict"] in {"SKIP", "FLAGGED"} and output["survivor_producer_id"] is not None:
        return fail_closed_row(raw_row, "survivor_present_without_merge", usage)

    refs = {
        entry["ref_id"]
        for entry in packet.get("evidence_refs", [])
        if isinstance(entry, dict) and entry.get("ref_id")
    }
    all_refs = set(output["key_support_refs"]) | set(output["key_contradiction_refs"])
    if output["key_support_refs"] and not set(output["key_support_refs"]).issubset(refs):
        return fail_closed_row(raw_row, "broken_support_refs", usage)
    if output["key_contradiction_refs"] and not set(output["key_contradiction_refs"]).issubset(refs):
        return fail_closed_row(raw_row, "broken_contradiction_refs", usage)
    if not output["key_support_refs"]:
        return fail_closed_row(raw_row, "missing_support_refs", usage)
    if not set(output["rule_ids"]).issubset(allowed_rule_ids):
        return fail_closed_row(raw_row, "invalid_rule_trace", usage)

    return {
        "benchmark_id": raw_row["benchmark_id"],
        "case_id": raw_row["case_id"],
        "packet_id": raw_row["packet_id"],
        "contender_id": raw_row["contender_id"],
        "normalized_output": {
            "packet_id": output["packet_id"],
            "verdict": output["verdict"],
            "confidence": round(float(output["confidence"]), 4),
            "rule_ids": output["rule_ids"],
            "reason": output["reason"].strip(),
            "key_support_refs": output["key_support_refs"],
            "key_contradiction_refs": output["key_contradiction_refs"],
            "survivor_producer_id": output["survivor_producer_id"],
            "follow_up": output["follow_up"],
        },
        "schema_valid": True,
        "citation_integrity": bool(all_refs.issubset(refs)),
        "rule_trace_valid": True,
        "timing_ms": raw_row.get("timing_ms", 0),
        "usage": usage,
    }


def normalize_rows(raw_rows: list[dict], packet_lookup: dict[str, dict]) -> list[dict]:
    allowed_rules = valid_rule_ids()
    return [
        normalize_one(raw_row, packet_lookup[raw_row["packet_id"]], allowed_rules)
        for raw_row in raw_rows
    ]


def rows_decision_vector(proof_case_ids: list[str], rows_by_case: dict[str, dict]) -> tuple[str, ...]:
    return tuple(rows_by_case[case_id]["normalized_output"]["verdict"] for case_id in proof_case_ids)


def accumulate_usage(rows: list[dict]) -> dict:
    totals = zero_usage()
    for row in rows:
        usage = row.get("usage") or {}
        totals["prompt_tokens"] += int(usage.get("prompt_tokens", 0) or 0)
        totals["completion_tokens"] += int(usage.get("completion_tokens", 0) or 0)
        totals["search_calls"] += int(usage.get("search_calls", 0) or 0)
        totals["cost_usd"] += float(usage.get("cost_usd", 0.0) or 0.0)
    totals["cost_usd"] = round(totals["cost_usd"], 6)
    return totals


def render_markdown(report: dict) -> str:
    lines: list[str] = []
    lines.append("# Session 10.8 - contrastive burden proof subset")
    lines.append("")
    lines.append(f"- Generated: {report['generated_at']}")
    lines.append(f"- Run name: `{report['run_name']}`")
    lines.append(f"- Model: `{report['model']}`")
    lines.append(f"- Frozen control: `{CONTROL_RUN_NAME}` / `{CONTROL_CONTENDER_ID}`")
    lines.append(f"- Proof subset size: `{report['subset']['case_count']}`")
    lines.append(f"- Estimated spend: `${report['usage']['cost_usd']:.4f}`")
    lines.append("")
    lines.append("## Goal")
    lines.append("")
    lines.append(
        "Test whether a stronger contrastive reasoning contract can recover the actual blocker cases without reopening the trap-heavy proof subset."
    )
    lines.append("")
    lines.append("## Proof result")
    lines.append("")
    lines.append(
        f"- Recoveries vs control: {render_case_id_list(report['summary']['recovered_case_ids'])}"
    )
    lines.append(
        f"- Blind-core blocker recoveries: {render_case_id_list(report['summary']['blind_core_recovered_case_ids'])}"
    )
    lines.append(
        f"- False merges on proof subset: {render_case_id_list(report['summary']['false_merge_case_ids'])}"
    )
    lines.append(
        f"- Lost current wins: {render_case_id_list(report['summary']['lost_control_win_case_ids'])}"
    )
    lines.append(
        f"- Kill criteria: false merges = {'pass' if report['criteria']['no_false_merges'] else 'fail'}; "
        f"blind-core recoveries >= 2 = {'pass' if report['criteria']['blind_core_recoveries_gte_2'] else 'fail'}; "
        f"no hold-set regressions = {'pass' if report['criteria']['no_lost_control_wins'] else 'fail'}."
    )
    lines.append("")
    lines.append("## Case matrix")
    lines.append("")
    lines.extend(report["case_table"])
    lines.append("")
    lines.append("## Recommendation")
    lines.append("")
    lines.append(f"- Status: `{report['recommendation']['status']}`")
    lines.append(f"- Reason: {report['recommendation']['reason']}")
    return "\n".join(lines).rstrip() + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(description="Run the Session 10.8 contrastive burden proof subset")
    parser.add_argument("--benchmark", type=Path, default=DEFAULT_BENCHMARK)
    parser.add_argument("--packet-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--run-name", default=RUN_NAME_DEFAULT)
    parser.add_argument("--report-json", type=Path, default=DEFAULT_REPORT_JSON)
    parser.add_argument("--report-md", type=Path, default=DEFAULT_REPORT_MD)
    parser.add_argument("--model", default=SONNET_MODEL)
    args = parser.parse_args()

    benchmark_payload = load_benchmark_payload(args.benchmark)
    cases_by_id = {case["case_id"]: case for case in benchmark_payload["cases"]}
    proof_case_ids = validate_groups(cases_by_id)
    full_packets, visible_packets, packet_validation = ensure_visible_packets(
        args.packet_dir,
        args.benchmark,
        force_rebuild=False,
    )
    packet_lookup = {packet["packet_id"]: packet for packet in full_packets}
    visible_lookup = {packet["packet_id"]: packet for packet in visible_packets}
    control_rows_by_case = load_rows_by_case(CONTROL_NORMALIZED)
    control_subset_rows_by_case = {
        case_id: deepcopy(control_rows_by_case[case_id]) for case_id in proof_case_ids
    }

    run_root = args.output_root / args.run_name
    raw_path = run_root / f"{CONTENDER_ID}.raw.jsonl"
    normalized_path = run_root / f"{CONTENDER_ID}.jsonl"
    run_root.mkdir(parents=True, exist_ok=True)

    section_11 = load_section_11()
    system_prompt = build_system_prompt(section_11)
    client = anthropic.Anthropic(api_key=get_env("ANTHROPIC_API_KEY"))

    raw_rows: list[dict] = []
    for case_id in proof_case_ids:
        case = cases_by_id[case_id]
        packet_id = f"producer_pair_{case['pair_id']}_v2"
        wrapper = build_wrapper(
            benchmark_payload["benchmark_id"],
            case,
            deepcopy(visible_lookup[packet_id]),
        )
        started = time.perf_counter()
        try:
            response = client.messages.create(
                model=args.model,
                max_tokens=900,
                temperature=0,
                system=[{
                    "type": "text",
                    "text": system_prompt,
                    "cache_control": {"type": "ephemeral"},
                }],
                messages=[{
                    "role": "user",
                    "content": [{
                        "type": "text",
                        "text": json.dumps(wrapper, ensure_ascii=True, sort_keys=True),
                    }],
                }],
            )
            timing_ms = int((time.perf_counter() - started) * 1000)
            raw_text = anthropic_text(response)
            parsed = extract_json_object(raw_text)
            raw_rows.append(
                {
                    "benchmark_id": wrapper["benchmark_id"],
                    "case_id": wrapper["case_id"],
                    "packet_id": wrapper["packet_visible"]["packet_id"],
                    "contender_id": CONTENDER_ID,
                    "raw_output": parsed if parsed is not None else raw_text,
                    "raw_text": raw_text,
                    "timing_ms": timing_ms,
                    "usage": anthropic_usage_to_dict(response.usage, args.model),
                }
            )
        except Exception as exc:
            raw_rows.append(
                build_runtime_error_row(
                    wrapper,
                    str(exc),
                    int((time.perf_counter() - started) * 1000),
                )
            )

    write_jsonl(raw_path, raw_rows)
    normalized_rows = normalize_rows(raw_rows, packet_lookup)
    write_jsonl(normalized_path, normalized_rows)
    normalized_rows_by_case = {row["case_id"]: row for row in normalized_rows}
    summary = summarize_rows(
        proof_case_ids=proof_case_ids,
        cases_by_id=cases_by_id,
        rows_by_case=normalized_rows_by_case,
        control_rows_by_case=control_subset_rows_by_case,
    )
    criteria = {
        "no_false_merges": len(summary["false_merge_case_ids"]) == 0,
        "blind_core_recoveries_gte_2": len(summary["blind_core_recovered_case_ids"]) >= 2,
        "no_lost_control_wins": len(summary["lost_control_win_case_ids"]) == 0,
    }
    survives_proof = all(criteria.values())
    if survives_proof:
        recommendation = {
            "status": "proceed_to_full_rerun",
            "reason": (
                "The contrastive burden adjudicator survived the trap-heavy proof subset with zero false merges, at least two blind-core recoveries, and no hold-set regressions."
            ),
        }
    else:
        recommendation = {
            "status": "eliminated_on_proof_subset",
            "reason": (
                "The contrastive burden adjudicator did not survive the trap-heavy proof subset cleanly enough to justify a full 152-case rerun."
            ),
        }

    report = {
        "generated_at": now_iso(),
        "run_name": args.run_name,
        "model": args.model,
        "inputs": INPUT_PATHS + [
            "data/sprints/identity-er/method_bakeoff/session10_8_broad_method_bakeoff_design.md",
        ],
        "packet_validation": {
            "packet_version": packet_validation["packet_version"],
            "packet_count": packet_validation["packet_count"],
            "hidden_field_leaks": packet_validation["hidden_field_leaks"],
        },
        "subset": {
            "case_count": len(proof_case_ids),
            "case_ids": proof_case_ids,
            "groups": PROOF_GROUPS,
            "blind_core_blocker_case_ids": BLIND_CORE_BLOCKER_CASE_IDS,
        },
        "usage": accumulate_usage(raw_rows),
        "raw_path": str(raw_path.relative_to(REPO_ROOT)),
        "normalized_path": str(normalized_path.relative_to(REPO_ROOT)),
        "summary": summary,
        "criteria": criteria,
        "survives_proof": survives_proof,
        "decision_vector": list(rows_decision_vector(proof_case_ids, normalized_rows_by_case)),
        "case_table": build_case_table(
            proof_case_ids=proof_case_ids,
            cases_by_id=cases_by_id,
            control_rows_by_case=control_subset_rows_by_case,
            contender_rows_by_case=normalized_rows_by_case,
        ),
        "recommendation": recommendation,
    }
    write_json(args.report_json, report)
    args.report_md.write_text(render_markdown(report), encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
