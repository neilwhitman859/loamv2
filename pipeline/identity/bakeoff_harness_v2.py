"""
Session 8 - Bakeoff harness for evidence_packet_v2.

This module handles:
1. Request-wrapper preparation from visible v2 packets.
2. Proof-subset selection from the canonical v1 failure clusters.
3. Fail-closed normalization plus v2 merge-veto guardrails.
4. Consensus construction from normalized child rows, not raw child outputs.
5. Frozen Session 4 scoring via the existing v1 scorer.
"""

from __future__ import annotations

import argparse
import json
from copy import deepcopy
from pathlib import Path

from pipeline.identity.bakeoff_harness_v1 import (
    load_benchmark_payload,
    score_run,
    valid_rule_ids,
)
from pipeline.identity.bakeoff_packet_v2 import (
    DEFAULT_BENCHMARK,
    DEFAULT_OUTPUT_DIR,
    build_packets,
    canonical_json_dumps,
    write_jsonl,
)


REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_RUN_ROOT = REPO_ROOT / "data" / "sprints" / "dedup" / "bakeoff_v2"
V1_RUN_ROOT = REPO_ROOT / "data" / "sprints" / "dedup" / "bakeoff_v1"
VALID_CONTENDERS = {
    "deterministic_control_v1",
    "sonnet_guardrailed_v2",
    "gemini_guardrailed_v2",
    "gpt5mini_guardrailed_v2",
    "sonnet_gemini_consensus_v2",
}
PROOF_CONTINUITY_CASE_IDS = [
    "blind_core_audit_041",
    "blind_core_audit_067",
    "blind_core_audit_048",
    "blind_core_audit_052",
    "known_false_merge_patterns_005",
    "tail_random_sample_008",
    "blind_core_audit_076",
    "blind_core_audit_080",
]
# `blind_core_audit_067` is already in the legacy 28-case base proof slice, so
# add one more alias-cross-mention stress case to preserve the 36-case target.
PROOF_CONTINUITY_FILL_CASE_IDS = [
    "blind_core_audit_065",
]


def load_jsonl(path: Path) -> list[dict]:
    with path.open(encoding="utf-8") as handle:
        return [json.loads(line) for line in handle if line.strip()]


def packets_by_id(packets: list[dict]) -> dict[str, dict]:
    return {packet["packet_id"]: packet for packet in packets}


def benchmark_cases_by_id(benchmark_payload: dict) -> dict[str, dict]:
    return {case["case_id"]: case for case in benchmark_payload["cases"]}


def ensure_visible_packets(packet_dir: Path, benchmark_path: Path) -> tuple[list[dict], list[dict], dict]:
    full_path = packet_dir / "benchmark_v1_packets_full_v2.jsonl"
    visible_path = packet_dir / "benchmark_v1_packets_visible_v2.jsonl"
    validation_path = packet_dir / "benchmark_v1_packet_validation_v2.json"
    benchmark_payload = load_benchmark_payload(benchmark_path)
    rebuild = not full_path.exists() or not visible_path.exists() or not validation_path.exists()
    if not rebuild:
        existing_full = load_jsonl(full_path)
        existing_visible = load_jsonl(visible_path)
        rebuild = (
            len(existing_full) != benchmark_payload["case_count"]
            or len(existing_visible) != benchmark_payload["case_count"]
        )
        if not rebuild:
            return existing_full, existing_visible, json.loads(validation_path.read_text(encoding="utf-8"))

    benchmark_id, full_packets, visible_packets, validations = build_packets(benchmark_path=benchmark_path)
    packet_dir.mkdir(parents=True, exist_ok=True)
    write_jsonl(full_path, full_packets)
    write_jsonl(visible_path, visible_packets)
    validation = {
        "benchmark_id": benchmark_id,
        "packet_count": len(full_packets),
        "visible_packet_count": len(visible_packets),
        "hidden_field_leaks": sum(item["hidden_field_leaks"] for item in validations),
        "retrieval_search_calls": sum(item["retrieval_search_calls"] for item in validations),
        "cases": validations,
    }
    validation_path.write_text(canonical_json_dumps(validation), encoding="utf-8")
    return full_packets, visible_packets, validation


def load_v1_sonnet_rows() -> list[dict]:
    return load_jsonl(
        V1_RUN_ROOT / "normalized" / "session6_first_real_bakeoff_v1" / "sonnet_single_v1.jsonl"
    )


def select_proof_cases(benchmark_payload: dict) -> tuple[list[dict], dict]:
    cases_by_id = benchmark_cases_by_id(benchmark_payload)
    v1_rows = load_v1_sonnet_rows()

    false_merges = sorted(
        row["case_id"]
        for row in v1_rows
        if cases_by_id[row["case_id"]]["expected_verdict"] == "SKIP"
        and row["normalized_output"]["verdict"] == "MERGE"
    )
    hard_misses = sorted(
        row["case_id"]
        for row in v1_rows
        if cases_by_id[row["case_id"]]["expected_verdict"] == "MERGE"
        and row["normalized_output"]["verdict"] == "SKIP"
    )
    soft_misses = sorted(
        row["case_id"]
        for row in v1_rows
        if cases_by_id[row["case_id"]]["expected_verdict"] == "MERGE"
        and row["normalized_output"]["verdict"] == "FLAGGED"
    )
    clean_merge_controls = sorted(
        row["case_id"]
        for row in v1_rows
        if cases_by_id[row["case_id"]]["expected_verdict"] == "MERGE"
        and row["normalized_output"]["verdict"] == "MERGE"
    )[:2]
    clean_skip_controls = sorted(
        row["case_id"]
        for row in v1_rows
        if cases_by_id[row["case_id"]]["expected_verdict"] == "SKIP"
        and row["normalized_output"]["verdict"] == "SKIP"
    )[:2]

    base_case_ids = false_merges + hard_misses + soft_misses[:4] + clean_merge_controls + clean_skip_controls
    chosen_ids = list(base_case_ids)
    continuity_add_on_case_ids: list[str] = []
    seen = set(base_case_ids)
    for case_id in PROOF_CONTINUITY_CASE_IDS + PROOF_CONTINUITY_FILL_CASE_IDS:
        if case_id in seen:
            continue
        chosen_ids.append(case_id)
        continuity_add_on_case_ids.append(case_id)
        seen.add(case_id)
        if len(continuity_add_on_case_ids) == 8:
            break
    if len(continuity_add_on_case_ids) != 8:
        raise RuntimeError("Expanded proof subset could not reach the required 8 continuity add-on cases.")
    chosen = [cases_by_id[case_id] for case_id in chosen_ids]
    return chosen, {
        "base_case_ids": base_case_ids,
        "continuity_add_on_case_ids": continuity_add_on_case_ids,
    }


def build_request_wrapper(benchmark_id: str, case: dict, visible_packet: dict, contender_id: str) -> dict:
    return {
        "benchmark_id": benchmark_id,
        "case_id": case["case_id"],
        "contender_id": contender_id,
        "instructions_version": "bakeoff_adjudicator_v2",
        "packet_visible": visible_packet,
        "allowed_ref_ids": visible_packet["envelope"]["allowed_ref_ids"],
        "output_contract_version": "adjudication_output_v2",
        "allow_tools": False,
        "temperature": 0,
    }


def prepare_request_wrappers(
    benchmark_payload: dict,
    visible_packets: list[dict],
    contenders: list[str],
    run_name: str,
    output_root: Path,
    proof_sample: bool = False,
) -> tuple[list[dict], dict[str, Path], dict]:
    benchmark_id = benchmark_payload["benchmark_id"]
    proof_meta = {"base_case_ids": [], "continuity_add_on_case_ids": []}
    if proof_sample:
        cases, proof_meta = select_proof_cases(benchmark_payload)
    else:
        cases = benchmark_payload["cases"]
    visible_by_id = packets_by_id(visible_packets)

    wrappers: list[dict] = []
    output_paths: dict[str, Path] = {}
    hidden_key_violations = 0
    for contender_id in contenders:
        contender_rows: list[dict] = []
        for case in cases:
            packet_id = f"producer_pair_{case['pair_id']}_v2"
            visible_packet = deepcopy(visible_by_id[packet_id])
            if "benchmark_overlay" in visible_packet.get("envelope", {}):
                hidden_key_violations += 1
            wrapper = build_request_wrapper(benchmark_id, case, visible_packet, contender_id)
            contender_rows.append(wrapper)
            wrappers.append(wrapper)
        output_path = output_root / "requests" / run_name / f"{contender_id}.jsonl"
        write_jsonl(output_path, contender_rows)
        output_paths[contender_id] = output_path

    validation = {
        "benchmark_id": benchmark_id,
        "run_name": run_name,
        "request_count": len(wrappers),
        "case_count": len(cases),
        "case_ids": [case["case_id"] for case in cases],
        "base_case_ids": proof_meta["base_case_ids"],
        "continuity_add_on_case_ids": proof_meta["continuity_add_on_case_ids"],
        "contenders": contenders,
        "hidden_key_violations": hidden_key_violations,
    }
    validation_path = output_root / "requests" / run_name / "request_validation.json"
    validation_path.write_text(canonical_json_dumps(validation), encoding="utf-8")
    return wrappers, output_paths, validation


def packet_ref_ids(packet: dict) -> set[str]:
    return {
        entry["ref_id"]
        for entry in packet.get("evidence_refs", [])
        if isinstance(entry, dict) and entry.get("ref_id")
    }


def packet_risk_refs(packet: dict) -> set[str]:
    return {
        entry["ref_id"]
        for entry in packet.get("evidence_refs", [])
        if isinstance(entry, dict) and entry.get("stance") == "risk"
    }


def has_hard_official_continuity(packet: dict) -> bool:
    return any(
        entry["ref_id"].startswith("hard_official_continuity_")
        for entry in packet.get("evidence_refs", [])
        if isinstance(entry, dict) and entry.get("ref_id")
    )


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
        "guardrail_applied": False,
    }


def parse_output(raw_row: dict, packet: dict, allowed_rule_ids: set[str]) -> tuple[dict | None, dict | None]:
    usage = raw_row.get("usage") or {
        "prompt_tokens": 0,
        "completion_tokens": 0,
        "search_calls": 0,
        "cost_usd": 0.0,
    }
    runtime_error = raw_row.get("runtime_error")
    if runtime_error:
        return None, fail_closed_row(raw_row, f"runtime_error:{runtime_error}", usage)

    output = raw_row.get("raw_output")
    if isinstance(output, str):
        try:
            output = json.loads(output)
        except json.JSONDecodeError:
            return None, fail_closed_row(raw_row, "invalid_json", usage)
    if not isinstance(output, dict):
        return None, fail_closed_row(raw_row, "invalid_payload", usage)

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
        return None, fail_closed_row(raw_row, f"missing_fields:{','.join(missing)}", usage)
    if output["packet_id"] != raw_row["packet_id"]:
        return None, fail_closed_row(raw_row, "packet_id_mismatch", usage)
    if output["verdict"] not in {"MERGE", "SKIP", "FLAGGED"}:
        return None, fail_closed_row(raw_row, "illegal_verdict", usage)
    if not isinstance(output["confidence"], (int, float)) or not (0 <= float(output["confidence"]) <= 1):
        return None, fail_closed_row(raw_row, "invalid_confidence", usage)
    if not isinstance(output["rule_ids"], list) or not all(isinstance(item, str) for item in output["rule_ids"]):
        return None, fail_closed_row(raw_row, "invalid_rule_ids", usage)
    if not isinstance(output["key_support_refs"], list) or not all(isinstance(item, str) for item in output["key_support_refs"]):
        return None, fail_closed_row(raw_row, "invalid_support_refs", usage)
    if not isinstance(output["key_contradiction_refs"], list) or not all(isinstance(item, str) for item in output["key_contradiction_refs"]):
        return None, fail_closed_row(raw_row, "invalid_contradiction_refs", usage)
    if not isinstance(output["reason"], str) or not output["reason"].strip():
        return None, fail_closed_row(raw_row, "missing_reason", usage)
    if output["follow_up"] is not None and not isinstance(output["follow_up"], str):
        return None, fail_closed_row(raw_row, "invalid_follow_up", usage)

    candidate_ids = {
        item["producer_id"]
        for item in packet["evidence"]["survivor_if_merge"].get("candidate_order", [])
        if item.get("producer_id")
    }
    required_survivor = packet["evidence"]["survivor_if_merge"].get("recommended_survivor_producer_id")
    if output["verdict"] == "MERGE":
        if required_survivor and not output["survivor_producer_id"]:
            return None, fail_closed_row(raw_row, "missing_required_survivor", usage)
        if output["survivor_producer_id"] and output["survivor_producer_id"] not in candidate_ids:
            return None, fail_closed_row(raw_row, "invalid_survivor_candidate", usage)
    if output["verdict"] in {"SKIP", "FLAGGED"} and output["survivor_producer_id"] is not None:
        return None, fail_closed_row(raw_row, "survivor_present_without_merge", usage)

    refs = packet_ref_ids(packet)
    all_refs = set(output["key_support_refs"]) | set(output["key_contradiction_refs"])
    if output["key_support_refs"] and not set(output["key_support_refs"]).issubset(refs):
        return None, fail_closed_row(raw_row, "broken_support_refs", usage)
    if output["key_contradiction_refs"] and not set(output["key_contradiction_refs"]).issubset(refs):
        return None, fail_closed_row(raw_row, "broken_contradiction_refs", usage)
    if not output["key_support_refs"]:
        return None, fail_closed_row(raw_row, "missing_support_refs", usage)
    if not set(output["rule_ids"]).issubset(allowed_rule_ids):
        return None, fail_closed_row(raw_row, "invalid_rule_trace", usage)

    normalized = {
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
        "guardrail_applied": False,
    }
    return normalized, None


def apply_merge_veto(normalized_row: dict, packet: dict) -> dict:
    verdict = normalized_row["normalized_output"]["verdict"]
    if verdict != "MERGE":
        return normalized_row
    if has_hard_official_continuity(packet):
        return normalized_row

    risk_refs = packet_risk_refs(packet)
    veto_refs = sorted(
        risk_refs
        & {
            "risk_shared_surname_split",
            "risk_holdco_or_product_tier",
            "risk_owner_or_operator_not_identity",
            "geo_country_conflict",
        }
    )
    if not veto_refs:
        return normalized_row

    support_refs = list(dict.fromkeys(veto_refs + normalized_row["normalized_output"]["key_support_refs"]))
    contradiction_refs = [
        ref
        for ref in normalized_row["normalized_output"]["key_contradiction_refs"]
        if ref not in veto_refs
    ]
    normalized_row["normalized_output"].update(
        {
            "verdict": "FLAGGED",
            "reason": (
                "Merge veto applied because the packet carries a high-risk contradiction pattern "
                f"({', '.join(veto_refs)}) without explicit official continuity evidence."
            ),
            "key_support_refs": support_refs[:3],
            "key_contradiction_refs": contradiction_refs[:2],
            "survivor_producer_id": None,
            "follow_up": "guardrail_merge_veto",
        }
    )
    normalized_row["guardrail_applied"] = True
    return normalized_row


def normalize_one(raw_row: dict, packet: dict, allowed_rule_ids: set[str]) -> dict:
    normalized, error_row = parse_output(raw_row, packet, allowed_rule_ids)
    if error_row:
        return error_row
    return apply_merge_veto(normalized, packet)


def normalize_file(raw_path: Path, packet_lookup: dict[str, dict], output_path: Path) -> Path:
    allowed_rules = valid_rule_ids()
    normalized_rows: list[dict] = []
    for raw_row in load_jsonl(raw_path):
        packet = packet_lookup[raw_row["packet_id"]]
        normalized_rows.append(normalize_one(raw_row, packet, allowed_rules))
    write_jsonl(output_path, normalized_rows)
    return output_path


def contract_valid_flagged_row(
    *,
    benchmark_id: str,
    case_id: str,
    packet: dict,
    contender_id: str,
    support_refs: list[str],
    contradiction_refs: list[str],
    reason: str,
    follow_up: str,
    usage: dict,
    timing_ms: int,
) -> dict:
    return {
        "benchmark_id": benchmark_id,
        "case_id": case_id,
        "packet_id": packet["packet_id"],
        "contender_id": contender_id,
        "normalized_output": {
            "packet_id": packet["packet_id"],
            "verdict": "FLAGGED",
            "confidence": 0.0,
            "rule_ids": ["11.1"],
            "reason": reason,
            "key_support_refs": support_refs[:3],
            "key_contradiction_refs": contradiction_refs[:2],
            "survivor_producer_id": None,
            "follow_up": follow_up,
        },
        "schema_valid": True,
        "citation_integrity": True,
        "rule_trace_valid": True,
        "timing_ms": timing_ms,
        "usage": usage,
        "guardrail_applied": False,
    }


def combine_usage(*rows: dict) -> dict:
    combined = {
        "prompt_tokens": 0,
        "completion_tokens": 0,
        "search_calls": 0,
        "cost_usd": 0.0,
    }
    for row in rows:
        usage = row.get("usage") or {}
        combined["prompt_tokens"] += int(usage.get("prompt_tokens", 0) or 0)
        combined["completion_tokens"] += int(usage.get("completion_tokens", 0) or 0)
        combined["search_calls"] += int(usage.get("search_calls", 0) or 0)
        combined["cost_usd"] += float(usage.get("cost_usd", 0.0) or 0.0)
    combined["cost_usd"] = round(combined["cost_usd"], 6)
    return combined


def build_consensus_normalized_file(
    sonnet_normalized_path: Path,
    cheap_normalized_path: Path,
    packet_lookup: dict[str, dict],
    output_path: Path,
    cheap_contender_id: str,
) -> Path:
    sonnet_rows = {row["case_id"]: row for row in load_jsonl(sonnet_normalized_path)}
    cheap_rows = {row["case_id"]: row for row in load_jsonl(cheap_normalized_path)}
    combined_rows: list[dict] = []

    for case_id in sorted(sonnet_rows):
        sonnet_row = sonnet_rows[case_id]
        cheap_row = cheap_rows[case_id]
        packet = packet_lookup[sonnet_row["packet_id"]]
        usage = combine_usage(sonnet_row, cheap_row)
        timing_ms = int(sonnet_row.get("timing_ms", 0) or 0) + int(cheap_row.get("timing_ms", 0) or 0)
        support_union = list(
            dict.fromkeys(
                sonnet_row["normalized_output"]["key_support_refs"]
                + cheap_row["normalized_output"]["key_support_refs"]
            )
        )
        contradiction_union = list(
            dict.fromkeys(
                sonnet_row["normalized_output"]["key_contradiction_refs"]
                + cheap_row["normalized_output"]["key_contradiction_refs"]
            )
        )

        if not sonnet_row["schema_valid"] or not cheap_row["schema_valid"]:
            combined_rows.append(
                contract_valid_flagged_row(
                    benchmark_id=sonnet_row["benchmark_id"],
                    case_id=case_id,
                    packet=packet,
                    contender_id="sonnet_gemini_consensus_v2",
                    support_refs=support_union or packet["envelope"]["allowed_ref_ids"][:1],
                    contradiction_refs=contradiction_union,
                    reason=(
                        "Consensus escalated because at least one child row failed schema validation; "
                        "invalid child rows are treated as child failures, not consensus evidence."
                    ),
                    follow_up="consensus_child_schema_invalid",
                    usage=usage,
                    timing_ms=timing_ms,
                )
            )
            continue

        sonnet_output = sonnet_row["normalized_output"]
        cheap_output = cheap_row["normalized_output"]
        sonnet_verdict = sonnet_output["verdict"]
        cheap_verdict = cheap_output["verdict"]
        shared_rules = list(dict.fromkeys(sonnet_output["rule_ids"] + cheap_output["rule_ids"]))[:3]

        if (
            sonnet_verdict == "MERGE"
            and cheap_verdict == "MERGE"
            and sonnet_output["survivor_producer_id"] == cheap_output["survivor_producer_id"]
        ):
            combined_rows.append(
                {
                    "benchmark_id": sonnet_row["benchmark_id"],
                    "case_id": case_id,
                    "packet_id": packet["packet_id"],
                    "contender_id": "sonnet_gemini_consensus_v2",
                    "normalized_output": {
                        "packet_id": packet["packet_id"],
                        "verdict": "MERGE",
                        "confidence": min(sonnet_output["confidence"], cheap_output["confidence"]),
                        "rule_ids": shared_rules,
                        "reason": "Normalized-child consensus: Sonnet and the cheaper path both returned contract-valid MERGE with the same survivor.",
                        "key_support_refs": support_union[:3],
                        "key_contradiction_refs": contradiction_union[:2],
                        "survivor_producer_id": sonnet_output["survivor_producer_id"],
                        "follow_up": None,
                    },
                    "schema_valid": True,
                    "citation_integrity": True,
                    "rule_trace_valid": True,
                    "timing_ms": timing_ms,
                    "usage": usage,
                    "guardrail_applied": False,
                }
            )
            continue

        if sonnet_verdict == "SKIP" and cheap_verdict == "SKIP":
            combined_rows.append(
                {
                    "benchmark_id": sonnet_row["benchmark_id"],
                    "case_id": case_id,
                    "packet_id": packet["packet_id"],
                    "contender_id": "sonnet_gemini_consensus_v2",
                    "normalized_output": {
                        "packet_id": packet["packet_id"],
                        "verdict": "SKIP",
                        "confidence": min(sonnet_output["confidence"], cheap_output["confidence"]),
                        "rule_ids": shared_rules,
                        "reason": "Normalized-child consensus: Sonnet and the cheaper path both returned contract-valid SKIP.",
                        "key_support_refs": support_union[:3],
                        "key_contradiction_refs": contradiction_union[:2],
                        "survivor_producer_id": None,
                        "follow_up": None,
                    },
                    "schema_valid": True,
                    "citation_integrity": True,
                    "rule_trace_valid": True,
                    "timing_ms": timing_ms,
                    "usage": usage,
                    "guardrail_applied": False,
                }
            )
            continue

        combined_rows.append(
            contract_valid_flagged_row(
                benchmark_id=sonnet_row["benchmark_id"],
                case_id=case_id,
                packet=packet,
                contender_id="sonnet_gemini_consensus_v2",
                support_refs=support_union or packet["envelope"]["allowed_ref_ids"][:1],
                contradiction_refs=contradiction_union,
                reason=(
                    f"Normalized-child consensus escalated because Sonnet returned {sonnet_verdict} "
                    f"while {cheap_contender_id} returned {cheap_verdict}, or the two MERGE rows "
                    "did not share the same survivor."
                ),
                follow_up="consensus_disagreement",
                usage=usage,
                timing_ms=timing_ms,
            )
        )

    write_jsonl(output_path, combined_rows)
    return output_path


def validate_proof_summary(summary: dict, request_validation: dict, consensus_rows: list[dict]) -> list[str]:
    failures: list[str] = []
    if request_validation["hidden_key_violations"] > 0:
        failures.append("hidden-field leak detected in proof request wrappers")
    for contender in summary["contenders"]:
        schema_valid_rate = contender["auditability"]["schema_valid_rate"] or 0.0
        if schema_valid_rate < 1.0:
            failures.append(
                f"{contender['contender_id']} schema_valid_rate {schema_valid_rate:.4f} < 1.0 on proof subset"
            )
    for row in consensus_rows:
        error_code = row.get("normalization_error") or ""
        if error_code.startswith("broken_"):
            failures.append("consensus still inherited child ref breakage")
            break
    return failures
