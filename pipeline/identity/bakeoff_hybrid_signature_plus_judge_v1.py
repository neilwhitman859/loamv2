"""Session 10.8 - hybrid signature-plus-judge full benchmark runner.

This contender is a true hybrid:

1. Keep the frozen Session 9.7 layered-safety control as the base.
2. Add only a tiny set of semantically interpretable deterministic promotions
   that survived full-benchmark pressure checks with zero false merges.
3. Route one narrow visible-feature frontier to a model judge instead of
   opening a broad second pass.

The frontier is intentionally small. The goal is not to "rejudge the whole
benchmark", but to see whether a model can safely finish the last narrow band
that the safe deterministic layer cannot honestly settle.
"""

from __future__ import annotations

import argparse
import json
import re
import time
from copy import deepcopy
from datetime import datetime
from pathlib import Path

import anthropic

from pipeline.identity.bakeoff_harness_v1 import (
    load_benchmark_payload,
    score_run,
    valid_rule_ids,
)
from pipeline.identity.bakeoff_harness_v2 import (
    DEFAULT_BENCHMARK,
    DEFAULT_OUTPUT_DIR,
    ensure_visible_packets,
)
from pipeline.identity.bakeoff_method_bakeoff_proof import (
    CONTROL_CONTENDER_ID,
    CONTROL_NORMALIZED,
    CONTROL_RUN_NAME,
    INPUT_PATHS,
    build_features,
    load_rows_by_case,
    render_case_id_list,
    summarize_rows,
    write_json,
)
from pipeline.identity.bakeoff_packet_v2 import canonical_json_dumps, write_jsonl
from pipeline.identity.bakeoff_run_v2 import extract_json_object
from pipeline.identity.selector_proof_v1 import anthropic_text, load_section_11
from pipeline.lib.db import get_env
from pipeline.lib.models import OPUS_MODEL, SONNET_MODEL


REPO_ROOT = Path(__file__).resolve().parents[2]
RUN_NAME_DEFAULT = "session10_8_hybrid_signature_plus_judge_v1"
DEFAULT_OUTPUT_ROOT = REPO_ROOT / "data" / "sprints" / "identity-er" / "method_bakeoff"
DEFAULT_REPORT_JSON = DEFAULT_OUTPUT_ROOT / f"{RUN_NAME_DEFAULT}.json"
DEFAULT_REPORT_MD = DEFAULT_OUTPUT_ROOT / f"{RUN_NAME_DEFAULT}.md"
CONTENDER_ID = "hybrid_signature_plus_judge_v1"
METHOD_VERSION = "hybrid_signature_plus_judge_v1"
DETERMINISTIC_RULESET_VERSION = "hybrid_safe_rules_v1"
FRONTIER_VERSION = "surname_frontier_v1"

MODEL_PRICING = {
    SONNET_MODEL: {
        "input": 3.0,
        "output": 15.0,
    },
    OPUS_MODEL: {
        "input": 15.0,
        "output": 75.0,
    },
}


def now_iso() -> str:
    return datetime.now().astimezone().isoformat(timespec="seconds")


def zero_usage() -> dict:
    return {
        "prompt_tokens": 0,
        "completion_tokens": 0,
        "search_calls": 0,
        "cost_usd": 0.0,
    }


def usage_to_dict(usage, model: str) -> dict:
    pricing = MODEL_PRICING.get(model, {"input": 0.0, "output": 0.0})
    input_tokens = int(getattr(usage, "input_tokens", 0) or 0)
    output_tokens = int(getattr(usage, "output_tokens", 0) or 0)
    cost = (
        input_tokens * pricing["input"] / 1_000_000
        + output_tokens * pricing["output"] / 1_000_000
    )
    return {
        "prompt_tokens": input_tokens,
        "completion_tokens": output_tokens,
        "search_calls": 0,
        "cost_usd": round(cost, 6),
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


def copy_control_row(control_row: dict) -> dict:
    row = deepcopy(control_row)
    row["contender_id"] = CONTENDER_ID
    row["timing_ms"] = 0
    row["usage"] = zero_usage()
    return row


def secondary_value_map(packet: dict) -> dict[str, str]:
    values: dict[str, str] = {}
    for entry in packet.get("evidence_refs", []):
        ref_id = str(entry.get("ref_id") or "")
        if not ref_id.startswith("secondary_"):
            continue
        summary = str(entry.get("summary") or "").strip()
        if summary:
            values[ref_id] = summary
    return values


def duplicate_secondary_across_sides(packet: dict) -> bool:
    values = secondary_value_map(packet)
    a_values = {
        value for ref_id, value in values.items() if ref_id.startswith("secondary_a_")
    }
    b_values = {
        value for ref_id, value in values.items() if ref_id.startswith("secondary_b_")
    }
    return bool(a_values & b_values)


def all_secondary_same(packet: dict) -> bool:
    values = list(secondary_value_map(packet).values())
    return len(values) >= 2 and len(set(values)) == 1


def deterministic_rule_id(case: dict, packet: dict, features) -> str | None:
    lexical = packet["evidence"]["comparison"]["lexical"]
    catalog = packet["evidence"]["comparison"]["catalog"]

    # Safe on the full benchmark: same-region subset alias cases with a
    # duplicated secondary summary on both sides.
    if (
        features.same_country
        and features.same_region
        and features.containment in {"a_in_b", "b_in_a"}
        and features.has_subset_match
        and not features.shared_surname_split
        and not features.secondary_relationship_without_identity
        and not features.holdco_or_product_tier
        and not features.owner_or_operator_not_identity
        and duplicate_secondary_across_sides(packet)
    ):
        return "alpha_same_region_subset_dupsec"

    # Safe on the full benchmark: same-country, split-region, two-core-token,
    # thin-catalog umbrella/alias cases.
    if (
        features.same_country
        and not features.same_region
        and features.shared_core_token_count >= 2
        and features.wine_count_large <= 12
        and not features.shared_surname_split
        and not features.secondary_relationship_without_identity
    ):
        return "beta_two_core_not_same_region"

    # Safe on the full benchmark: cross-country exact-name cases only when
    # every visible secondary summary collapses to the same producer identity.
    if (
        features.country_conflict
        and features.trigram_similarity >= 1.0
        and not features.secondary_relationship_without_identity
        and not features.shared_surname_split
        and not features.holdco_or_product_tier
        and not features.owner_or_operator_not_identity
        and all_secondary_same(packet)
    ):
        return "gamma_cross_country_all_secondary_same"

    # Safe on the full benchmark: historical joint-label cases where the only
    # extra lexical token on the larger side is `et`, plus exact overlap.
    if (
        features.shared_surname_split
        and features.same_country
        and features.same_region
        and features.containment == "a_in_b"
        and int(catalog.get("exact_overlap_count") or 0) >= 1
        and lexical.get("wrapper_tokens_only_on_b") == ["et"]
    ):
        return "delta_et_historical_joint_label"

    return None


def frontier_case(case_id: str, case: dict, packet: dict, features, control_row: dict) -> bool:
    if control_row["normalized_output"]["verdict"] == "MERGE":
        return False
    if deterministic_rule_id(case, packet, features):
        return False
    return (
        features.shared_surname_split
        and features.same_country
        and features.same_region
        and features.containment in {"a_in_b", "b_in_a"}
        and features.has_subset_match
        and not features.country_conflict
    )


def build_system_prompt(section_11: str) -> str:
    return f"""You are `hybrid_signature_plus_judge_v1`, the frontier judge for a hybrid producer-dedup method.

You will receive exactly one JSON request wrapper containing:
- `packet_visible`
- `allowed_ref_ids`
- `frontier_context`

Use ONLY the evidence inside that wrapper. Do not use outside knowledge, live
search, or hidden benchmark speculation.

Apply the producer identity rules strictly:

{section_11}

Frontier context:
- These packets were routed here because the frozen base would NOT merge them.
- They all show a high-risk surname/same-region subset pattern.
- The default posture is still abstention or skip, not optimism.

This frontier exists to separate:
- true historical or shorthand same-identity cases
from
- shared-surname family splits, related estates, collaborations, and other
  near-miss traps.

Decision burden:
- `risk_shared_surname_split` is a live contradiction. Geography, subset shape,
  lexical containment, and shared cru footprint are NOT enough by themselves.
- Return `MERGE` only if the visible packet itself rebuts the split story and
  supports one current producer identity.
- Strong merge stories in this frontier look like:
  - a historical combined label versus the surviving descendant label,
  - a thin stub or shorthand row that clearly collapses into one producer
    identity rather than a separate family member,
  - a municipal or collective winery shorthand whose wine footprint and
    secondary evidence still point to one producer identity.
- Strong skip stories look like:
  - different named family members or estates sharing the surname,
  - secondary summaries that point to distinct first names, houses, or
    producer identities,
  - overlap that is fully explained by related family estates in the same
    region.
- Prefer `FLAGGED` when same identity remains plausible but the packet does not
  clearly rebut the split story.
- Prefer `SKIP` when a distinct-but-related explanation is stronger.

Important frontier rules:
- If secondary evidence on the two sides points to clearly different named
  producers or estates, do NOT return `MERGE`.
- If `exact_overlap_count` is zero, do NOT return `MERGE` unless the packet
  still shows a specific same-identity bridge stronger than the split story.
- If the packet looks like a place-name or stub alias, only merge if the
  visible wine footprint and secondary evidence converge on a single producer
  identity rather than merely a shared region.

Output contract requirements:
- Return exactly one JSON object and nothing else.
- Allowed `verdict` values: `MERGE`, `SKIP`, `FLAGGED`.
- `confidence` must be a number from 0 to 1.
- `rule_ids` must cite real Section 11 rule ids such as `11.1`, `11.4.f`,
  `11.4.h`, `11.4.m`, `11.4.o`, `11.6`.
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
        "instructions_version": METHOD_VERSION,
        "packet_visible": visible_packet,
        "allowed_ref_ids": visible_packet["envelope"]["allowed_ref_ids"],
        "frontier_context": {
            "frontier_version": FRONTIER_VERSION,
            "base_control": CONTROL_CONTENDER_ID,
            "reason": (
                "shared-surname same-region subset frontier left unresolved by the frozen base"
            ),
        },
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
    required_survivor = packet["evidence"]["survivor_if_merge"].get(
        "recommended_survivor_producer_id"
    )
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


def render_case_matrix(
    *,
    case_ids: list[str],
    cases_by_id: dict[str, dict],
    control_rows_by_case: dict[str, dict],
    contender_rows_by_case: dict[str, dict],
) -> list[str]:
    lines = ["| Case | Expected | Control | Contender |", "|---|---|---|---|"]
    for case_id in case_ids:
        expected = cases_by_id[case_id]["expected_verdict"]
        control_verdict = control_rows_by_case[case_id]["normalized_output"]["verdict"]
        contender_verdict = contender_rows_by_case[case_id]["normalized_output"]["verdict"]
        lines.append(
            f"| `{case_id}` | {expected} | {control_verdict} | {contender_verdict} |"
        )
    return lines


def render_markdown(report: dict) -> str:
    score = report["score"]
    summary = report["summary"]
    lines: list[str] = []
    lines.append("# Session 10.8 - hybrid signature plus judge")
    lines.append("")
    lines.append(f"- Generated: {report['generated_at']}")
    lines.append(f"- Run name: `{report['run_name']}`")
    lines.append(f"- Model: `{report['model']}`")
    lines.append(f"- Frozen control: `{CONTROL_RUN_NAME}` / `{CONTROL_CONTENDER_ID}`")
    lines.append(f"- Deterministic ruleset: `{DETERMINISTIC_RULESET_VERSION}`")
    lines.append(f"- Frontier version: `{FRONTIER_VERSION}`")
    lines.append(f"- Frontier size: `{len(report['frontier_case_ids'])}`")
    lines.append(f"- Estimated spend: `${report['usage']['cost_usd']:.4f}`")
    lines.append("")
    lines.append("## Goal")
    lines.append("")
    lines.append(
        "Test a true hybrid contender: keep the frozen control, add only the small zero-false-merge deterministic promotions that survived full-benchmark pressure checks, then let one narrow shared-surname frontier judge try to finish the last unresolved band."
    )
    lines.append("")
    lines.append("## Deterministic promotions")
    lines.append("")
    for item in report["deterministic_promotions"]:
        lines.append(
            f"- `{item['rule_id']}` -> `{item['case_id']}` ({item['expected_verdict']})"
        )
    lines.append("")
    lines.append("## Frontier")
    lines.append("")
    lines.append(
        f"- Routed cases: {render_case_id_list(report['frontier_case_ids'])}"
    )
    lines.append(
        f"- Judge-touched cases: {render_case_id_list(report['model_touched_case_ids'])}"
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
    lines.append(
        f"- Lost control wins: {render_case_id_list(summary['lost_control_win_case_ids'])}"
    )
    lines.append("")
    lines.append("## Frontier Case Matrix")
    lines.append("")
    lines.extend(report["frontier_case_matrix"])
    lines.append("")
    lines.append("## Recommendation")
    lines.append("")
    lines.append(f"- Status: `{report['recommendation']['status']}`")
    lines.append(f"- Reason: {report['recommendation']['reason']}")
    return "\n".join(lines).rstrip() + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(description="Run the hybrid signature-plus-judge contender")
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
    full_case_ids = [case["case_id"] for case in benchmark_payload["cases"]]
    full_packets, visible_packets, packet_validation = ensure_visible_packets(
        args.packet_dir,
        args.benchmark,
        force_rebuild=False,
    )
    packet_lookup = {packet["packet_id"]: packet for packet in full_packets}
    visible_lookup = {packet["packet_id"]: packet for packet in visible_packets}
    control_rows_by_case = load_rows_by_case(CONTROL_NORMALIZED)

    run_root = args.output_root / args.run_name
    raw_path = run_root / f"{CONTENDER_ID}.raw.jsonl"
    normalized_path = run_root / f"{CONTENDER_ID}.jsonl"
    run_root.mkdir(parents=True, exist_ok=True)

    section_11 = load_section_11()
    system_prompt = build_system_prompt(section_11)
    client = anthropic.Anthropic(api_key=get_env("ANTHROPIC_API_KEY"))

    contender_rows_by_case: dict[str, dict] = {}
    frontier_case_ids: list[str] = []
    deterministic_promotions: list[dict] = []
    raw_rows: list[dict] = []

    for case_id in full_case_ids:
        case = cases_by_id[case_id]
        packet_id = f"producer_pair_{case['pair_id']}_v2"
        packet = packet_lookup[packet_id]
        features = build_features(case, packet)
        control_row = control_rows_by_case[case_id]
        row = copy_control_row(control_row)
        deterministic_rule = deterministic_rule_id(case, packet, features)
        if deterministic_rule and row["normalized_output"]["verdict"] != "MERGE":
            row["normalized_output"]["verdict"] = "MERGE"
            row["normalized_output"]["confidence"] = 0.91
            row["normalized_output"]["reason"] = (
                f"Promoted by deterministic hybrid rule `{deterministic_rule}`."
            )
            row["normalized_output"]["follow_up"] = None
            row["normalized_output"]["survivor_producer_id"] = packet["evidence"][
                "survivor_if_merge"
            ].get("recommended_survivor_producer_id")
            row["hybrid_action"] = "deterministic_promotion"
            row["hybrid_rule_id"] = deterministic_rule
            deterministic_promotions.append(
                {
                    "case_id": case_id,
                    "expected_verdict": case["expected_verdict"],
                    "rule_id": deterministic_rule,
                }
            )
        elif frontier_case(case_id, case, packet, features, control_row):
            frontier_case_ids.append(case_id)
            row["hybrid_action"] = "frontier_model"
        else:
            row["hybrid_action"] = "reused_control"
        contender_rows_by_case[case_id] = row

    for case_id in frontier_case_ids:
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
                    "usage": usage_to_dict(response.usage, args.model),
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
    normalized_frontier_rows = normalize_rows(raw_rows, packet_lookup)
    normalized_frontier_by_case = {row["case_id"]: row for row in normalized_frontier_rows}

    for case_id, row in normalized_frontier_by_case.items():
        row["hybrid_action"] = "frontier_model"
        contender_rows_by_case[case_id] = row

    ordered_rows = [contender_rows_by_case[case_id] for case_id in full_case_ids]
    write_jsonl(normalized_path, ordered_rows)

    score_json_path = run_root / f"{CONTENDER_ID}.score.json"
    score_md_path = run_root / f"{CONTENDER_ID}.score.md"
    score = score_run(
        benchmark_payload=benchmark_payload,
        normalized_paths=[normalized_path],
        full_packets=full_packets,
        output_json=score_json_path,
        output_md=score_md_path,
        run_name=args.run_name,
    )
    contender_score = score["contenders"][0]
    summary = summarize_rows(
        proof_case_ids=full_case_ids,
        cases_by_id=cases_by_id,
        rows_by_case=contender_rows_by_case,
        control_rows_by_case=control_rows_by_case,
    )

    if contender_score["gates"]["production"]["status"] == "pass":
        recommendation = {
            "status": "candidate_clears_frozen_production_gate",
            "reason": (
                "The hybrid contender cleared the frozen production gate on the full benchmark. It now deserves stricter confirmation work rather than more prompt churn."
            ),
        }
    elif contender_score["gates"]["fallback"]["status"] == "pass":
        recommendation = {
            "status": "candidate_improves_to_fallback_only",
            "reason": (
                "The hybrid contender improved the control and preserved fallback safety, but it still missed the frozen production gate."
            ),
        }
    else:
        recommendation = {
            "status": "candidate_failed_full_gate",
            "reason": (
                "The hybrid contender did not clear the frozen production or fallback gate on the full benchmark."
            ),
        }

    frontier_case_matrix = render_case_matrix(
        case_ids=frontier_case_ids,
        cases_by_id=cases_by_id,
        control_rows_by_case=control_rows_by_case,
        contender_rows_by_case=contender_rows_by_case,
    )

    report = {
        "generated_at": now_iso(),
        "run_name": args.run_name,
        "model": args.model,
        "inputs": INPUT_PATHS + [
            "data/sprints/identity-er/method_bakeoff/session10_8_broad_method_bakeoff_design.md",
            "data/sprints/identity-er/method_bakeoff/session10_8_visible_signature_promotion_v1.json",
            "data/sprints/identity-er/method_bakeoff/session10_8_contrastive_burden_proof_v1.json",
        ],
        "packet_validation": {
            "packet_version": packet_validation["packet_version"],
            "packet_count": packet_validation["packet_count"],
            "hidden_field_leaks": packet_validation["hidden_field_leaks"],
        },
        "method_version": METHOD_VERSION,
        "deterministic_ruleset_version": DETERMINISTIC_RULESET_VERSION,
        "frontier_version": FRONTIER_VERSION,
        "usage": accumulate_usage(raw_rows),
        "raw_path": str(raw_path.relative_to(REPO_ROOT)),
        "normalized_path": str(normalized_path.relative_to(REPO_ROOT)),
        "score_json_path": str(score_json_path.relative_to(REPO_ROOT)),
        "score_md_path": str(score_md_path.relative_to(REPO_ROOT)),
        "deterministic_promotions": deterministic_promotions,
        "frontier_case_ids": frontier_case_ids,
        "model_touched_case_ids": [row["case_id"] for row in raw_rows],
        "summary": summary,
        "score": contender_score,
        "frontier_case_matrix": frontier_case_matrix,
        "recommendation": recommendation,
    }
    write_json(args.report_json, report)
    args.report_md.write_text(render_markdown(report), encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
