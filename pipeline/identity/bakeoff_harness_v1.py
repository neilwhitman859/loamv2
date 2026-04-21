"""
Session 5 - Bakeoff harness for evidence_packet_v1.

This module handles:
1. Request-wrapper preparation from visible packets.
2. A local deterministic control proof runner.
3. Fail-closed normalization of raw contender outputs.
4. Frozen Session 4 scoring helpers and gate evaluation.

The real model bakeoff can plug in later by writing raw output JSONL rows in the
same shape used here for deterministic_control_v1.

Run a proof sample:
    python -m pipeline.identity.bakeoff_harness_v1 proof-run
"""

from __future__ import annotations

import argparse
import json
import math
import statistics
import time
from collections import Counter, defaultdict
from copy import deepcopy
from pathlib import Path

from pipeline.identity.bakeoff_packet_v1 import (
    DEFAULT_BENCHMARK,
    DEFAULT_OUTPUT_DIR,
    build_packets,
    canonical_json_dumps,
    validate_visible_packet,
    write_jsonl,
)


REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_RUN_ROOT = REPO_ROOT / "data" / "sprints" / "dedup" / "bakeoff_v1"
VALID_CONTENDERS = {
    "deterministic_control_v1",
    "haiku_single_v1",
    "gemini_single_v1",
    "gpt5mini_single_v1",
    "haiku_gemini_consensus_v1",
    "sonnet_single_v1",
}


def load_jsonl(path: Path) -> list[dict]:
    with path.open(encoding="utf-8") as handle:
        return [json.loads(line) for line in handle if line.strip()]


def load_benchmark_payload(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def ensure_visible_packets(packet_dir: Path, benchmark_path: Path) -> tuple[list[dict], list[dict]]:
    full_path = packet_dir / "benchmark_v1_packets_full.jsonl"
    visible_path = packet_dir / "benchmark_v1_packets_visible.jsonl"
    benchmark_payload = load_benchmark_payload(benchmark_path)
    rebuild = not full_path.exists() or not visible_path.exists()
    if not rebuild:
        existing_full = load_jsonl(full_path)
        existing_visible = load_jsonl(visible_path)
        rebuild = (
            len(existing_full) != benchmark_payload["case_count"]
            or len(existing_visible) != benchmark_payload["case_count"]
        )
        if not rebuild:
            return existing_full, existing_visible
    if rebuild:
        _, full_packets, visible_packets, validations = build_packets(benchmark_path=benchmark_path)
        packet_dir.mkdir(parents=True, exist_ok=True)
        write_jsonl(full_path, full_packets)
        write_jsonl(visible_path, visible_packets)
        (packet_dir / "benchmark_v1_packet_validation.json").write_text(
            canonical_json_dumps(
                {
                    "benchmark_id": benchmark_payload["benchmark_id"],
                    "packet_count": len(full_packets),
                    "visible_packet_count": len(visible_packets),
                    "hidden_field_leaks": sum(item["hidden_field_leaks"] for item in validations),
                    "cases": validations,
                }
            ),
            encoding="utf-8",
        )
    return load_jsonl(full_path), load_jsonl(visible_path)


def packets_by_id(packets: list[dict]) -> dict[str, dict]:
    return {packet["packet_id"]: packet for packet in packets}


def benchmark_cases_by_id(benchmark_payload: dict) -> dict[str, dict]:
    return {case["case_id"]: case for case in benchmark_payload["cases"]}


def select_proof_cases(cases: list[dict], per_stratum: int = 2) -> list[dict]:
    picked: list[dict] = []
    grouped: dict[str, list[dict]] = defaultdict(list)
    for case in cases:
        grouped[case["stratum"]].append(case)
    for stratum in (
        "blind_core_audit",
        "known_false_merge_patterns",
        "known_missed_merge_patterns",
        "tail_random_sample",
    ):
        picked.extend(grouped.get(stratum, [])[:per_stratum])
    return picked


def build_request_wrapper(benchmark_id: str, case: dict, visible_packet: dict, contender_id: str) -> dict:
    return {
        "benchmark_id": benchmark_id,
        "case_id": case["case_id"],
        "contender_id": contender_id,
        "instructions_version": "bakeoff_adjudicator_v1",
        "packet_visible": visible_packet,
        "output_contract_version": "adjudication_output_v1",
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
    cases = benchmark_payload["cases"]
    if proof_sample:
        cases = select_proof_cases(cases)
    visible_by_id = packets_by_id(visible_packets)

    wrappers: list[dict] = []
    output_paths: dict[str, Path] = {}
    hidden_key_violations = 0
    for contender_id in contenders:
        contender_rows: list[dict] = []
        for case in cases:
            packet_id = f"producer_pair_{case['pair_id']}_v1"
            visible_packet = deepcopy(visible_by_id[packet_id])
            hidden_key_violations += len(validate_visible_packet(visible_packet))
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
        "contenders": contenders,
        "hidden_key_violations": hidden_key_violations,
    }
    validation_path = output_root / "requests" / run_name / "request_validation.json"
    validation_path.write_text(canonical_json_dumps(validation), encoding="utf-8")
    return wrappers, output_paths, validation


def packet_ref_ids(packet: dict) -> set[str]:
    refs: set[str] = set()
    external = packet["evidence"]["external_evidence"]
    for item in external.get("official_domain_hits", []):
        if item.get("ref_id"):
            refs.add(item["ref_id"])
    for item in external.get("secondary_hits", []):
        if item.get("ref_id"):
            refs.add(item["ref_id"])
    comparison = packet["evidence"]["comparison"]
    for item in comparison.get("support_signals", []):
        if item.get("code"):
            refs.add(item["code"])
    for item in comparison.get("contradiction_flags", []):
        if item.get("code"):
            refs.add(item["code"])
    return refs


def valid_rule_ids() -> set[str]:
    text = (REPO_ROOT / "docs" / "IDENTITY_RULES.md").read_text(encoding="utf-8")
    matches = []
    for line in text.splitlines():
        line = line.strip()
        if not line.startswith("#"):
            continue
        parts = line.split()
        if len(parts) < 2:
            continue
        candidate = parts[1]
        if candidate.startswith("11.") and candidate[-1].isalnum():
            matches.append(candidate)
    return set(matches)


def deterministic_control(wrapper: dict) -> dict:
    packet = wrapper["packet_visible"]
    comparison = packet["evidence"]["comparison"]
    survivor = packet["evidence"]["survivor_if_merge"]["recommended_survivor_producer_id"]
    support_codes = {item["code"] for item in comparison.get("support_signals", [])}
    contradiction_codes = {item["code"] for item in comparison.get("contradiction_flags", [])}
    trigram = comparison["lexical"]["trigram_similarity"]
    same_country = comparison["geography"]["same_country"]
    same_region = comparison["geography"]["same_region"]
    exact_overlap_count = comparison["catalog"]["exact_overlap_count"]
    anchor_examples = comparison["catalog"]["anchor_overlap_examples"]
    rule_hint = packet["evidence"]["pair"]["rule_paths_to_check"][0]

    verdict = "FLAGGED"
    confidence = 0.55
    follow_up = "needs_human_review"
    reason = "Deterministic control could not reach a safe merge or skip from packet evidence alone."
    key_support_refs = sorted(list(support_codes))[:2]
    key_contradiction_refs = sorted(list(contradiction_codes))[:2]
    survivor_producer_id = None

    if "country_conflict" in contradiction_codes and "cross_country_name_match" not in support_codes:
        verdict = "SKIP"
        confidence = 0.92
        reason = "Country conflict without a same-brand bridge keeps this pair safely separate."
        follow_up = None
        rule_hint = "11.3"
    elif "shared_surname_split_risk" in contradiction_codes:
        verdict = "SKIP"
        confidence = 0.94
        reason = "Shared-surname split risk is a known false-merge pattern, so the safe deterministic call is SKIP."
        follow_up = None
        rule_hint = "11.4.m"
    elif (
        "lexical_short_full_form" in support_codes
        and same_country
        and (same_region or anchor_examples)
    ):
        verdict = "MERGE"
        confidence = 0.91
        reason = "Short/full-form lexical evidence plus same-country catalog coherence points to one producer identity."
        survivor_producer_id = survivor
        follow_up = None
    elif trigram >= 0.97 and same_country and exact_overlap_count > 0:
        verdict = "MERGE"
        confidence = 0.96
        reason = "Near-exact same-country names with direct catalog overlap make this a deterministic merge."
        survivor_producer_id = survivor
        follow_up = None
        rule_hint = "11.4.h"
    elif "catalog_subset_match" in support_codes and same_country and "country_conflict" not in contradiction_codes:
        verdict = "MERGE"
        confidence = 0.87
        reason = "Same-country catalog subset evidence is strong enough for the deterministic control to merge."
        survivor_producer_id = survivor
        follow_up = None
    elif "country_conflict" in contradiction_codes or "catalog_asymmetry" in contradiction_codes:
        verdict = "SKIP"
        confidence = 0.78
        reason = "The packet shows more contradiction than deterministic merge support, so the control stays conservative."
        follow_up = None
        rule_hint = "11.3"

    return {
        "packet_id": packet["packet_id"],
        "verdict": verdict,
        "confidence": confidence,
        "rule_ids": [rule_hint],
        "reason": reason,
        "key_support_refs": key_support_refs,
        "key_contradiction_refs": key_contradiction_refs,
        "survivor_producer_id": survivor_producer_id if verdict == "MERGE" else None,
        "follow_up": follow_up,
    }


def run_deterministic_request_file(request_file: Path, output_path: Path) -> Path:
    rows = load_jsonl(request_file)
    raw_rows: list[dict] = []
    for wrapper in rows:
        start = time.perf_counter()
        raw_output = deterministic_control(wrapper)
        timing_ms = int((time.perf_counter() - start) * 1000)
        raw_rows.append(
            {
                "benchmark_id": wrapper["benchmark_id"],
                "case_id": wrapper["case_id"],
                "packet_id": wrapper["packet_visible"]["packet_id"],
                "contender_id": wrapper["contender_id"],
                "raw_output": raw_output,
                "timing_ms": timing_ms,
                "usage": {
                    "prompt_tokens": 0,
                    "completion_tokens": 0,
                    "search_calls": 0,
                    "cost_usd": 0.0,
                },
            }
        )
    write_jsonl(output_path, raw_rows)
    return output_path


def fail_closed_row(raw_row: dict, packet: dict, error_code: str, usage: dict) -> dict:
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
    usage = raw_row.get("usage") or {
        "prompt_tokens": 0,
        "completion_tokens": 0,
        "search_calls": 0,
        "cost_usd": 0.0,
    }
    output = raw_row.get("raw_output")
    if isinstance(output, str):
        try:
            output = json.loads(output)
        except json.JSONDecodeError:
            return fail_closed_row(raw_row, packet, "invalid_json", usage)
    if not isinstance(output, dict):
        return fail_closed_row(raw_row, packet, "invalid_payload", usage)

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
        return fail_closed_row(raw_row, packet, f"missing_fields:{','.join(missing)}", usage)
    if output["packet_id"] != raw_row["packet_id"]:
        return fail_closed_row(raw_row, packet, "packet_id_mismatch", usage)
    if output["verdict"] not in {"MERGE", "SKIP", "FLAGGED"}:
        return fail_closed_row(raw_row, packet, "illegal_verdict", usage)
    if not isinstance(output["confidence"], (int, float)) or not (0 <= float(output["confidence"]) <= 1):
        return fail_closed_row(raw_row, packet, "invalid_confidence", usage)
    if not isinstance(output["rule_ids"], list) or not all(isinstance(item, str) for item in output["rule_ids"]):
        return fail_closed_row(raw_row, packet, "invalid_rule_ids", usage)
    if not isinstance(output["key_support_refs"], list) or not all(isinstance(item, str) for item in output["key_support_refs"]):
        return fail_closed_row(raw_row, packet, "invalid_support_refs", usage)
    if not isinstance(output["key_contradiction_refs"], list) or not all(isinstance(item, str) for item in output["key_contradiction_refs"]):
        return fail_closed_row(raw_row, packet, "invalid_contradiction_refs", usage)
    if not isinstance(output["reason"], str) or not output["reason"].strip():
        return fail_closed_row(raw_row, packet, "missing_reason", usage)
    if output["follow_up"] is not None and not isinstance(output["follow_up"], str):
        return fail_closed_row(raw_row, packet, "invalid_follow_up", usage)

    candidate_ids = {
        item["producer_id"]
        for item in packet["evidence"]["survivor_if_merge"].get("candidate_order", [])
        if item.get("producer_id")
    }
    required_survivor = packet["evidence"]["survivor_if_merge"].get("recommended_survivor_producer_id")
    if output["verdict"] == "MERGE":
        if required_survivor and not output["survivor_producer_id"]:
            return fail_closed_row(raw_row, packet, "missing_required_survivor", usage)
        if output["survivor_producer_id"] and output["survivor_producer_id"] not in candidate_ids:
            return fail_closed_row(raw_row, packet, "invalid_survivor_candidate", usage)
    if output["verdict"] == "SKIP" and output["survivor_producer_id"] is not None:
        return fail_closed_row(raw_row, packet, "skip_with_survivor", usage)

    refs = packet_ref_ids(packet)
    all_refs = set(output["key_support_refs"]) | set(output["key_contradiction_refs"])
    if output["key_support_refs"] and not set(output["key_support_refs"]).issubset(refs):
        return fail_closed_row(raw_row, packet, "broken_support_refs", usage)
    if output["key_contradiction_refs"] and not set(output["key_contradiction_refs"]).issubset(refs):
        return fail_closed_row(raw_row, packet, "broken_contradiction_refs", usage)
    if not output["key_support_refs"]:
        return fail_closed_row(raw_row, packet, "missing_support_refs", usage)

    trace_ok = bool(output["rule_ids"]) and set(output["rule_ids"]).issubset(allowed_rule_ids)
    if not trace_ok:
        return fail_closed_row(raw_row, packet, "invalid_rule_trace", usage)

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
        "rule_trace_valid": trace_ok,
        "timing_ms": raw_row.get("timing_ms", 0),
        "usage": usage,
    }


def normalize_file(raw_path: Path, packet_lookup: dict[str, dict], output_path: Path) -> Path:
    allowed_rules = valid_rule_ids()
    normalized_rows: list[dict] = []
    for raw_row in load_jsonl(raw_path):
        packet = packet_lookup[raw_row["packet_id"]]
        normalized_rows.append(normalize_one(raw_row, packet, allowed_rules))
    write_jsonl(output_path, normalized_rows)
    return output_path


def empty_counts() -> dict[str, int]:
    return {
        "true_merge": 0,
        "true_skip": 0,
        "false_merge": 0,
        "hard_missed_merge": 0,
        "soft_missed_merge": 0,
        "safe_flag": 0,
        "survivor_error": 0,
    }


def rate(numerator: int, denominator: int) -> float | None:
    if denominator == 0:
        return None
    return round(numerator / denominator, 4)


def score_contender(
    contender_id: str,
    normalized_rows: list[dict],
    benchmark_payload: dict,
    full_packets: dict[str, dict],
) -> dict:
    cases_by_id = benchmark_cases_by_id(benchmark_payload)
    counts = empty_counts()
    tier_counts: dict[str, dict[str, int]] = defaultdict(empty_counts)
    stratum_counts: dict[str, dict[str, int]] = defaultdict(empty_counts)
    error_ledger: list[dict] = []
    latencies: list[int] = []
    total_cost = 0.0
    total_tokens = 0
    valid_schema = 0
    valid_citations = 0
    valid_rule_trace = 0
    survivor_scorable = 0
    survivor_errors = 0

    for row in normalized_rows:
        case = cases_by_id[row["case_id"]]
        packet = full_packets[row["packet_id"]]
        verdict = row["normalized_output"]["verdict"]
        expected = case["expected_verdict"]
        tier = case["source_pair_tier"]
        stratum = case["stratum"]
        packet_counts = [counts, tier_counts[tier], stratum_counts[stratum]]

        if row["schema_valid"]:
            valid_schema += 1
        if row["citation_integrity"]:
            valid_citations += 1
        if row["rule_trace_valid"]:
            valid_rule_trace += 1
        latencies.append(row.get("timing_ms", 0))
        usage = row.get("usage") or {}
        total_cost += float(usage.get("cost_usd", 0.0) or 0.0)
        total_tokens += int(usage.get("prompt_tokens", 0) or 0) + int(usage.get("completion_tokens", 0) or 0)

        if expected == "MERGE" and verdict == "MERGE":
            for bucket in packet_counts:
                bucket["true_merge"] += 1
        elif expected == "SKIP" and verdict == "SKIP":
            for bucket in packet_counts:
                bucket["true_skip"] += 1
        elif expected == "SKIP" and verdict == "MERGE":
            for bucket in packet_counts:
                bucket["false_merge"] += 1
        elif expected == "MERGE" and verdict == "SKIP":
            for bucket in packet_counts:
                bucket["hard_missed_merge"] += 1
        elif expected == "MERGE" and verdict == "FLAGGED":
            for bucket in packet_counts:
                bucket["soft_missed_merge"] += 1
        elif expected == "SKIP" and verdict == "FLAGGED":
            for bucket in packet_counts:
                bucket["safe_flag"] += 1

        if expected == "MERGE":
            recommended_survivor = packet["evidence"]["survivor_if_merge"].get("recommended_survivor_producer_id")
            survivor_complete = packet["envelope"]["completeness"]["survivor_calc"] == "complete"
            row_survivor_error = False
            if recommended_survivor and survivor_complete:
                survivor_scorable += 1
                if verdict == "MERGE" and row["normalized_output"].get("survivor_producer_id") != recommended_survivor:
                    counts["survivor_error"] += 1
                    tier_counts[tier]["survivor_error"] += 1
                    stratum_counts[stratum]["survivor_error"] += 1
                    survivor_errors += 1
                    row_survivor_error = True
        else:
            row_survivor_error = False

        if (
            expected == "SKIP" and verdict == "MERGE"
            or expected == "MERGE" and verdict in {"SKIP", "FLAGGED"}
            or row_survivor_error
        ):
            error_ledger.append(
                {
                    "case_id": row["case_id"],
                    "pair_id": case["pair_id"],
                    "contender_id": contender_id,
                    "expected_verdict": expected,
                    "predicted_verdict": verdict,
                    "packet_refs_used": row["normalized_output"]["key_support_refs"] + row["normalized_output"]["key_contradiction_refs"],
                }
            )

    merge_cases = sum(1 for row in normalized_rows if cases_by_id[row["case_id"]]["expected_verdict"] == "MERGE")
    skip_cases = sum(1 for row in normalized_rows if cases_by_id[row["case_id"]]["expected_verdict"] == "SKIP")
    total_cases = len(normalized_rows)
    auditability = {
        "schema_valid_rate": rate(valid_schema, total_cases),
        "citation_integrity_rate": rate(valid_citations, total_cases),
        "rule_trace_rate": rate(valid_rule_trace, total_cases),
    }
    auditability_score = None
    if total_cases:
        auditability_score = round(
            0.40 * (auditability["schema_valid_rate"] or 0.0)
            + 0.40 * (auditability["citation_integrity_rate"] or 0.0)
            + 0.20 * (auditability["rule_trace_rate"] or 0.0),
            4,
        )

    rates = {
        "false_merge_rate": rate(counts["false_merge"], skip_cases),
        "hard_missed_merge_rate": rate(counts["hard_missed_merge"], merge_cases),
        "soft_missed_merge_rate": rate(counts["soft_missed_merge"], merge_cases),
        "merge_capture_rate": rate(counts["true_merge"], merge_cases),
        "safe_flag_rate": rate(counts["safe_flag"], skip_cases),
        "flag_rate_total": rate(counts["soft_missed_merge"] + counts["safe_flag"], total_cases),
        "survivor_accuracy": rate(survivor_scorable - survivor_errors, survivor_scorable),
        "exact_verdict_accuracy": rate(counts["true_merge"] + counts["true_skip"], total_cases),
    }
    cost = {
        "cost_per_pair": round(total_cost / total_cases, 6) if total_cases else 0.0,
        "median_latency_ms": int(statistics.median(latencies)) if latencies else 0,
        "tokens_per_pair": round(total_tokens / total_cases, 2) if total_cases else 0.0,
        "total_cost_usd": round(total_cost, 6),
    }

    expected_case_count = benchmark_payload["case_count"]
    full_benchmark_run = total_cases == expected_case_count
    production_gate = evaluate_production_gates(
        counts=counts,
        rates=rates,
        auditability_score=auditability_score,
        auditability=auditability,
        survivor_accuracy=rates["survivor_accuracy"],
        stratum_counts=stratum_counts,
        full_benchmark_run=full_benchmark_run,
    )
    fallback_gate = evaluate_fallback_gates(
        counts=counts,
        rates=rates,
        auditability_score=auditability_score,
        auditability=auditability,
        survivor_accuracy=rates["survivor_accuracy"],
        stratum_counts=stratum_counts,
        full_benchmark_run=full_benchmark_run,
    )

    return {
        "contender_id": contender_id,
        "case_count_seen": total_cases,
        "case_count_expected": expected_case_count,
        "full_benchmark_run": full_benchmark_run,
        "counts": counts,
        "rates": rates,
        "cost": cost,
        "auditability": {**auditability, "auditability_score": auditability_score},
        "gates": {
            "production": production_gate,
            "fallback": fallback_gate,
        },
        "breakdowns": {
            "source_pair_tier": tier_counts,
            "stratum": stratum_counts,
        },
        "error_ledger": error_ledger,
    }


def evaluate_production_gates(
    counts: dict,
    rates: dict,
    auditability_score: float | None,
    auditability: dict,
    survivor_accuracy: float | None,
    stratum_counts: dict,
    full_benchmark_run: bool,
) -> dict:
    if not full_benchmark_run:
        return {"status": "not_applicable_incomplete_benchmark"}
    checks = {
        "false_merge_zero": counts["false_merge"] == 0,
        "blind_core_false_merge_zero": stratum_counts["blind_core_audit"]["false_merge"] == 0,
        "blind_core_hard_missed_zero": stratum_counts["blind_core_audit"]["hard_missed_merge"] == 0,
        "blind_core_soft_missed_lte_1": stratum_counts["blind_core_audit"]["soft_missed_merge"] <= 1,
        "known_false_merge_false_merge_zero": stratum_counts["known_false_merge_patterns"]["false_merge"] == 0,
        "known_false_merge_safe_flag_lte_4": stratum_counts["known_false_merge_patterns"]["safe_flag"] <= 4,
        "known_missed_hard_lte_2": stratum_counts["known_missed_merge_patterns"]["hard_missed_merge"] <= 2,
        "known_missed_soft_lte_4": stratum_counts["known_missed_merge_patterns"]["soft_missed_merge"] <= 4,
        "tail_false_merge_zero": stratum_counts["tail_random_sample"]["false_merge"] == 0,
        "tail_hard_missed_lte_2": stratum_counts["tail_random_sample"]["hard_missed_merge"] <= 2,
        "tail_flag_rate_lte_0_25": ((stratum_counts["tail_random_sample"]["soft_missed_merge"] + stratum_counts["tail_random_sample"]["safe_flag"]) / 20) <= 0.25,
        "survivor_accuracy_gte_0_95": (survivor_accuracy or 0.0) >= 0.95,
        "auditability_score_gte_0_95": (auditability_score or 0.0) >= 0.95,
        "schema_valid_rate_1_00": (auditability["schema_valid_rate"] or 0.0) == 1.0,
    }
    return {
        "status": "pass" if all(checks.values()) else "fail",
        "checks": checks,
    }


def evaluate_fallback_gates(
    counts: dict,
    rates: dict,
    auditability_score: float | None,
    auditability: dict,
    survivor_accuracy: float | None,
    stratum_counts: dict,
    full_benchmark_run: bool,
) -> dict:
    if not full_benchmark_run:
        return {"status": "not_applicable_incomplete_benchmark"}
    checks = {
        "false_merge_zero": counts["false_merge"] == 0,
        "blind_core_false_merge_zero": stratum_counts["blind_core_audit"]["false_merge"] == 0,
        "survivor_accuracy_gte_0_90": (survivor_accuracy or 0.0) >= 0.90,
        "auditability_score_gte_0_95": (auditability_score or 0.0) >= 0.95,
        "flag_rate_total_lte_0_35": (rates["flag_rate_total"] or 0.0) <= 0.35,
    }
    return {
        "status": "pass" if all(checks.values()) else "fail",
        "checks": checks,
    }


def rank_winners(contender_scores: list[dict]) -> list[dict]:
    ranked: list[dict] = []
    for score in contender_scores:
        production_status = score["gates"]["production"]["status"]
        fallback_status = score["gates"]["fallback"]["status"]
        if production_status == "pass":
            eligibility = "production_eligible"
            rank_tuple = (
                score["breakdowns"]["stratum"]["blind_core_audit"]["soft_missed_merge"],
                score["counts"]["hard_missed_merge"],
                -(score["rates"]["survivor_accuracy"] or 0.0),
                -(score["auditability"]["auditability_score"] or 0.0),
                score["rates"]["flag_rate_total"] or math.inf,
                score["cost"]["cost_per_pair"],
            )
        elif fallback_status == "pass":
            eligibility = "fallback_only"
            rank_tuple = (
                score["rates"]["flag_rate_total"] or math.inf,
                score["cost"]["cost_per_pair"],
            )
        else:
            eligibility = "ineligible"
            rank_tuple = ()
        ranked.append(
            {
                "contender_id": score["contender_id"],
                "eligibility": eligibility,
                "production_gate": production_status,
                "fallback_gate": fallback_status,
                "rank_tuple": rank_tuple,
            }
        )
    return ranked


def render_score_markdown(summary: dict) -> str:
    lines: list[str] = []
    lines.append(f"# {summary['run_name']} - Session 4 bakeoff proof scorecard")
    lines.append("")
    lines.append(f"- Benchmark: `{summary['benchmark_id']}`")
    lines.append(f"- Cases scored: {summary['case_count_seen']} / {summary['case_count_expected']}")
    lines.append(f"- Full benchmark run: {'yes' if summary['full_benchmark_run'] else 'no'}")
    lines.append("")
    lines.append("| Contender | Exact acc | False merge | Hard missed merge | Soft missed merge | Safe flag | Auditability | Gate status |")
    lines.append("|---|---:|---:|---:|---:|---:|---:|---|")
    for contender in summary["contenders"]:
        rates = contender["rates"]
        counts = contender["counts"]
        gate = contender["gates"]["production"]["status"]
        lines.append(
            f"| {contender['contender_id']} | "
            f"{(rates['exact_verdict_accuracy'] or 0):.4f} | "
            f"{counts['false_merge']} | {counts['hard_missed_merge']} | "
            f"{counts['soft_missed_merge']} | {counts['safe_flag']} | "
            f"{(contender['auditability']['auditability_score'] or 0):.4f} | {gate} |"
        )
    lines.append("")
    lines.append("## Winner selection table")
    lines.append("")
    lines.append("| Contender | Eligibility | Production gate | Fallback gate |")
    lines.append("|---|---|---|---|")
    for winner_row in summary["winner_selection"]:
        lines.append(
            f"| {winner_row['contender_id']} | {winner_row['eligibility']} | "
            f"{winner_row['production_gate']} | {winner_row['fallback_gate']} |"
        )
    return "\n".join(lines)


def score_run(
    benchmark_payload: dict,
    normalized_paths: list[Path],
    full_packets: list[dict],
    output_json: Path,
    output_md: Path,
    run_name: str,
) -> dict:
    full_packet_lookup = packets_by_id(full_packets)
    contender_scores: list[dict] = []
    seen_case_count = 0
    full_benchmark_run = False
    for path in normalized_paths:
        rows = load_jsonl(path)
        if not rows:
            continue
        contender_scores.append(
            score_contender(
                contender_id=rows[0]["contender_id"],
                normalized_rows=rows,
                benchmark_payload=benchmark_payload,
                full_packets=full_packet_lookup,
            )
        )
        seen_case_count = max(seen_case_count, contender_scores[-1]["case_count_seen"])
        full_benchmark_run = full_benchmark_run or contender_scores[-1]["full_benchmark_run"]

    winner_selection = rank_winners(contender_scores)
    summary = {
        "run_name": run_name,
        "benchmark_id": benchmark_payload["benchmark_id"],
        "case_count_seen": seen_case_count,
        "case_count_expected": benchmark_payload["case_count"],
        "full_benchmark_run": full_benchmark_run,
        "contenders": contender_scores,
        "winner_selection": winner_selection,
    }
    output_json.parent.mkdir(parents=True, exist_ok=True)
    output_json.write_text(canonical_json_dumps(summary), encoding="utf-8")
    output_md.write_text(render_score_markdown(summary), encoding="utf-8")
    return summary


def cmd_prepare(args) -> int:
    benchmark_payload = load_benchmark_payload(args.benchmark)
    _, visible_packets = ensure_visible_packets(args.packet_dir, args.benchmark)
    _, output_paths, validation = prepare_request_wrappers(
        benchmark_payload=benchmark_payload,
        visible_packets=visible_packets,
        contenders=args.contenders,
        run_name=args.run_name,
        output_root=args.output_root,
        proof_sample=args.proof_sample,
    )
    print(f"Prepared request wrappers for {validation['case_count']} cases.")
    for contender_id, path in output_paths.items():
        print(f"  {contender_id}: {path}")
    print(f"  hidden key violations: {validation['hidden_key_violations']}")
    return 0


def cmd_run_deterministic(args) -> int:
    output = run_deterministic_request_file(args.request_file, args.output)
    print(f"Wrote raw deterministic outputs -> {output}")
    return 0


def cmd_normalize(args) -> int:
    full_packets = load_jsonl(args.packet_dir / "benchmark_v1_packets_full.jsonl")
    packet_lookup = packets_by_id(full_packets)
    output = normalize_file(args.raw_file, packet_lookup, args.output)
    print(f"Wrote normalized outputs -> {output}")
    return 0


def cmd_score(args) -> int:
    benchmark_payload = load_benchmark_payload(args.benchmark)
    full_packets = load_jsonl(args.packet_dir / "benchmark_v1_packets_full.jsonl")
    summary = score_run(
        benchmark_payload=benchmark_payload,
        normalized_paths=args.normalized_files,
        full_packets=full_packets,
        output_json=args.output_json,
        output_md=args.output_md,
        run_name=args.run_name,
    )
    print(
        f"Scored {summary['case_count_seen']} / {summary['case_count_expected']} cases "
        f"for {len(summary['contenders'])} contender(s)."
    )
    return 0


def cmd_proof_run(args) -> int:
    benchmark_payload = load_benchmark_payload(args.benchmark)
    full_packets, visible_packets = ensure_visible_packets(args.packet_dir, args.benchmark)
    _, request_paths, validation = prepare_request_wrappers(
        benchmark_payload=benchmark_payload,
        visible_packets=visible_packets,
        contenders=["deterministic_control_v1"],
        run_name=args.run_name,
        output_root=args.output_root,
        proof_sample=True,
    )
    if validation["hidden_key_violations"]:
        raise RuntimeError("Visible request wrappers leaked hidden benchmark fields.")

    request_file = request_paths["deterministic_control_v1"]
    raw_path = args.output_root / "raw" / args.run_name / "deterministic_control_v1.jsonl"
    normalized_path = args.output_root / "normalized" / args.run_name / "deterministic_control_v1.jsonl"
    score_json = args.output_root / "scored" / f"{args.run_name}.json"
    score_md = args.output_root / "scored" / f"{args.run_name}.md"

    run_deterministic_request_file(request_file, raw_path)
    normalize_file(raw_path, packets_by_id(full_packets), normalized_path)
    summary = score_run(
        benchmark_payload=benchmark_payload,
        normalized_paths=[normalized_path],
        full_packets=full_packets,
        output_json=score_json,
        output_md=score_md,
        run_name=args.run_name,
    )

    print(f"Proof run complete for {summary['case_count_seen']} cases.")
    print(f"Request file: {request_file}")
    print(f"Raw outputs: {raw_path}")
    print(f"Normalized outputs: {normalized_path}")
    print(f"Scored summary: {score_json}")
    print(f"Markdown scorecard: {score_md}")
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Session 4 bakeoff harness")
    sub = parser.add_subparsers(dest="command", required=True)

    prepare = sub.add_parser("prepare", help="Build request wrappers for one or more contenders")
    prepare.add_argument("--benchmark", type=Path, default=DEFAULT_BENCHMARK)
    prepare.add_argument("--packet-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    prepare.add_argument("--output-root", type=Path, default=DEFAULT_RUN_ROOT)
    prepare.add_argument("--run-name", default="benchmark_v1_prepare")
    prepare.add_argument("--proof-sample", action="store_true")
    prepare.add_argument("--contenders", nargs="+", default=["deterministic_control_v1"])
    prepare.set_defaults(func=cmd_prepare)

    run_det = sub.add_parser("run-deterministic", help="Run deterministic_control_v1 against prepared request wrappers")
    run_det.add_argument("--request-file", type=Path, required=True)
    run_det.add_argument("--output", type=Path, required=True)
    run_det.set_defaults(func=cmd_run_deterministic)

    normalize = sub.add_parser("normalize", help="Normalize raw contender outputs into Session 4 result rows")
    normalize.add_argument("--packet-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    normalize.add_argument("--raw-file", type=Path, required=True)
    normalize.add_argument("--output", type=Path, required=True)
    normalize.set_defaults(func=cmd_normalize)

    score = sub.add_parser("score", help="Score normalized result rows with frozen Session 4 metrics and gates")
    score.add_argument("--benchmark", type=Path, default=DEFAULT_BENCHMARK)
    score.add_argument("--packet-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    score.add_argument("--normalized-files", type=Path, nargs="+", required=True)
    score.add_argument("--output-json", type=Path, required=True)
    score.add_argument("--output-md", type=Path, required=True)
    score.add_argument("--run-name", default="benchmark_v1_score")
    score.set_defaults(func=cmd_score)

    proof = sub.add_parser("proof-run", help="Run an end-to-end proof sample using deterministic_control_v1")
    proof.add_argument("--benchmark", type=Path, default=DEFAULT_BENCHMARK)
    proof.add_argument("--packet-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    proof.add_argument("--output-root", type=Path, default=DEFAULT_RUN_ROOT)
    proof.add_argument("--run-name", default="proof_run_v1")
    proof.set_defaults(func=cmd_proof_run)

    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    invalid = [contender for contender in getattr(args, "contenders", []) if contender not in VALID_CONTENDERS]
    if invalid:
        raise RuntimeError(f"Unknown contender ids: {invalid}")
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
