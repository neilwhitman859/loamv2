"""Session 9.7 - layered safety-gate proof on top of Session 9.6 specialists.

This script tests a broader multi-stage redesign:

1. Start from the Session 9.6 routed-specialist outputs.
2. Apply deterministic anti-trap vetoes for the most obvious false-merge shapes.
3. Optionally run a skeptical safety reviewer on remaining 11.4.f specialist
   `MERGE` proposals.
4. Revert vetoed proposals to the safe Session 9.3 Gemini base verdict.
5. Score the composite result on the frozen 152-case benchmark.
"""

from __future__ import annotations

import argparse
import json
import time
from copy import deepcopy
from datetime import datetime
from pathlib import Path

import anthropic
import requests

from pipeline.identity.bakeoff_harness_v1 import load_benchmark_payload, score_run, valid_rule_ids
from pipeline.identity.bakeoff_harness_v2 import (
    DEFAULT_BENCHMARK,
    DEFAULT_OUTPUT_DIR,
    DEFAULT_RUN_ROOT,
    ensure_visible_packets,
    load_jsonl,
    parse_output,
)
from pipeline.identity.bakeoff_packet_v2 import canonical_json_dumps, write_jsonl
from pipeline.identity.bakeoff_run_v2 import (
    CONTENDER_META,
    OPENROUTER_URL,
    anthropic_text,
    anthropic_usage_to_dict,
    extract_json_object,
    openrouter_usage_to_dict,
)
from pipeline.lib.db import get_env


REPO_ROOT = Path(__file__).resolve().parents[2]
IDENTITY_RULES_PATH = REPO_ROOT / "docs" / "IDENTITY_RULES.md"
BASE_RUN_NAME = "session9_3_full_rerun_if_approved"
BASE_CONTENDER_ID = "gemini_guardrailed_v2"
SPECIALIST_RUN_NAME = "session9_6_pattern_specialist_proof_if_approved"
SPECIALIST_CONTENDER_ID = "gemini_routed_pattern_specialist_v1"
DEFAULT_BASE_NORMALIZED = DEFAULT_RUN_ROOT / "normalized" / BASE_RUN_NAME / f"{BASE_CONTENDER_ID}.jsonl"
DEFAULT_SPECIALIST_ROOT = DEFAULT_RUN_ROOT / "normalized" / SPECIALIST_RUN_NAME
SESSION9_6_SCORE_JSON = DEFAULT_RUN_ROOT / "scored" / f"{SPECIALIST_RUN_NAME}.json"


def write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(canonical_json_dumps(payload), encoding="utf-8")


def load_section_11() -> str:
    text = IDENTITY_RULES_PATH.read_text(encoding="utf-8")
    start = text.find("## 11. Producer Identity Rules")
    if start == -1:
        raise RuntimeError("Could not locate Section 11 in docs/IDENTITY_RULES.md")
    end = text.find("\n## ", start + 1)
    return text[start:end if end != -1 else None].strip()


def load_base_rows(path: Path) -> dict[str, dict]:
    return {row["case_id"]: row for row in load_jsonl(path)}


def load_specialist_rows(root: Path) -> dict[str, dict]:
    rows: dict[str, dict] = {}
    for path in sorted(root.glob("11_4_*.jsonl")):
        for row in load_jsonl(path):
            rows[row["case_id"]] = row
    if not rows:
        raise RuntimeError(f"No specialist rows found under {root}")
    return rows


def packet_ref_ids(packet: dict) -> set[str]:
    return {
        entry["ref_id"]
        for entry in packet.get("evidence_refs", [])
        if isinstance(entry, dict) and entry.get("ref_id")
    }


def benchmark_cases_by_id(benchmark_payload: dict) -> dict[str, dict]:
    return {case["case_id"]: case for case in benchmark_payload["cases"]}


def exact_overlap_count(packet: dict) -> int:
    return int(packet["evidence"]["comparison"]["catalog"].get("exact_overlap_count", 0) or 0)


def deterministic_veto_reason(case: dict, packet: dict) -> str | None:
    refs = packet_ref_ids(packet)
    cluster = case["pattern_cluster"]
    if (
        cluster == "11.4.h"
        and "risk_shared_surname_split" in refs
        and "catalog_exact_overlap" not in refs
        and "catalog_subset_match" not in refs
        and "geo_same_region" not in refs
    ):
        return "shared_surname_without_catalog_or_region_bridge"
    if (
        cluster == "11.4.h"
        and "risk_secondary_relationship_without_identity" in refs
        and "lex_near_exact" not in refs
        and "lex_contains" not in refs
    ):
        return "secondary_relationship_without_name_bridge"
    return None


def should_review_specialist_merge(case: dict, packet: dict, review_scope: str) -> bool:
    if case["pattern_cluster"] != "11.4.f":
        return False
    if review_scope == "all_11_4_f":
        return True
    refs = packet_ref_ids(packet)
    if review_scope == "narrow_11_4_f_traps":
        return (
            "catalog_exact_overlap" in refs
            and "catalog_subset_match" in refs
            and exact_overlap_count(packet) == 1
            and "lex_contains" not in refs
            and "risk_secondary_relationship_without_identity" not in refs
        )
    raise RuntimeError(f"Unsupported review scope: {review_scope}")


def build_safety_system_prompt(section_11: str) -> str:
    return f"""You are `layered_safety_gate_11_4_f_v1`, a skeptical merge-safety reviewer.

You will receive exactly one JSON wrapper containing:
- `packet_visible`
- `specialist_proposed_output`
- `allowed_ref_ids`

Use ONLY the evidence inside that wrapper. Do not use outside knowledge, live
web search, or hidden benchmark speculation.

Apply the product's producer identity rules:

{section_11}

Your job is NOT to rediscover easy merge reasons. Your job is to decide whether
the proposed `MERGE` is safe enough to keep. The safe default is `FLAGGED`.

Return `MERGE` only when the packet clearly proves one continuous current
producer identity. Return `FLAGGED` when the proposal relies on weak continuity
inference or when real ambiguity remains.

Safety doctrine for 11.4.f review:
- Shared surname, same village, same region, or one shared cuvee are not enough.
- Founder history, family lineage, or "this person leads production at that
  estate" do not by themselves prove that a person-name row and an estate-name
  row are the same current producer identity.
- Importer, retailer, and portfolio blurbs can blur estates and people. Treat
  them as weak unless they explicitly describe a rename, now-known-as
  continuity, or one current label identity.
- Strong keep signals are things like: explicit rename language, "the domaine's
  name changed to...", "formerly labeled as...", or dense multi-anchor portfolio
  continuity that points to one compact estate identity rather than two related
  family branches.
- When torn between "same family / same patrimony" and "same current producer
  identity", prefer `FLAGGED`.

Output contract requirements:
- Return exactly one JSON object and nothing else.
- Allowed `verdict` values in this safety gate: `MERGE` or `FLAGGED`.
- `confidence` must be a JSON number from 0 to 1.
- `rule_ids` must cite real Section 11 rule ids.
- `key_support_refs` and `key_contradiction_refs` must use ONLY ids from
  `allowed_ref_ids`.
- Every output must include at least one `key_support_refs` item.
- If `verdict` is `MERGE`, set `survivor_producer_id` to exactly the
  `specialist_proposed_output.survivor_producer_id` value from the wrapper.
- If `verdict` is `FLAGGED`, set `survivor_producer_id` to null.
- `reason` should be one short evidence-grounded paragraph.
- `follow_up` should be null unless the proposal should be vetoed back to the
  base path, in which case set `follow_up` to `safety_gate_veto`.

Return this exact shape:
{{
  "packet_id": "<packet id>",
  "verdict": "MERGE | FLAGGED",
  "confidence": 0.0,
  "rule_ids": ["11.x"],
  "reason": "short evidence-grounded explanation",
  "key_support_refs": ["ref_id"],
  "key_contradiction_refs": ["ref_id"],
  "survivor_producer_id": "uuid_or_null",
  "follow_up": null
}}
"""


def build_review_wrapper(case: dict, visible_packet: dict, specialist_row: dict) -> dict:
    return {
        "benchmark_id": case["benchmark_id"],
        "case_id": case["case_id"],
        "contender_id": specialist_row["contender_id"],
        "review_scope": "11.4.f_merge_keep_or_veto",
        "packet_visible": visible_packet,
        "allowed_ref_ids": visible_packet["envelope"]["allowed_ref_ids"],
        "specialist_proposed_output": specialist_row["normalized_output"],
        "temperature": 0,
        "allow_tools": False,
    }


def review_runtime_error_row(wrapper: dict, contender_id: str, message: str, timing_ms: int, usage: dict | None = None) -> dict:
    return {
        "benchmark_id": wrapper["benchmark_id"],
        "case_id": wrapper["case_id"],
        "packet_id": wrapper["packet_visible"]["packet_id"],
        "contender_id": contender_id,
        "raw_output": None,
        "runtime_error": message[:160],
        "timing_ms": timing_ms,
        "usage": usage
        or {
            "prompt_tokens": 0,
            "completion_tokens": 0,
            "search_calls": 0,
            "cost_usd": 0.0,
        },
    }


def run_openrouter_reviews(
    wrappers: list[dict],
    output_path: Path,
    contender_ref: str,
    contender_id: str,
    system_prompt: str,
) -> Path:
    meta = CONTENDER_META[contender_ref]
    headers = {
        "Authorization": f"Bearer {get_env('OPENROUTER_API_KEY')}",
        "Content-Type": "application/json",
        "HTTP-Referer": "https://loam.onrender.com",
        "X-Title": "Loam Session 9.7 Layered Safety Gate",
    }
    raw_rows: list[dict] = []
    for wrapper in wrappers:
        body = {
            "model": meta["model_id"],
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": json.dumps(wrapper, ensure_ascii=True, sort_keys=True)},
            ],
            "temperature": 0,
            "max_tokens": 700,
            "provider": {"require_parameters": True},
            "reasoning": {"effort": "minimal"},
        }
        started = time.perf_counter()
        try:
            response = requests.post(OPENROUTER_URL, headers=headers, json=body, timeout=120)
            timing_ms = int((time.perf_counter() - started) * 1000)
            if response.status_code != 200:
                raw_rows.append(
                    review_runtime_error_row(
                        wrapper,
                        contender_id,
                        f"http_{response.status_code}:{response.text[:120]}",
                        timing_ms,
                    )
                )
                continue
            payload = response.json()
            raw_text = payload.get("choices", [{}])[0].get("message", {}).get("content", "")
            parsed = extract_json_object(raw_text)
            raw_rows.append(
                {
                    "benchmark_id": wrapper["benchmark_id"],
                    "case_id": wrapper["case_id"],
                    "packet_id": wrapper["packet_visible"]["packet_id"],
                    "contender_id": contender_id,
                    "raw_output": parsed if parsed is not None else raw_text,
                    "raw_text": raw_text,
                    "timing_ms": timing_ms,
                    "usage": openrouter_usage_to_dict(payload.get("usage"), meta["pricing"]),
                }
            )
        except Exception as exc:  # pragma: no cover - transport guard
            raw_rows.append(
                review_runtime_error_row(
                    wrapper,
                    contender_id,
                    str(exc),
                    int((time.perf_counter() - started) * 1000),
                )
            )
    write_jsonl(output_path, raw_rows)
    return output_path


def run_anthropic_reviews(
    wrappers: list[dict],
    output_path: Path,
    contender_ref: str,
    contender_id: str,
    system_prompt: str,
) -> Path:
    meta = CONTENDER_META[contender_ref]
    client = anthropic.Anthropic(api_key=get_env("ANTHROPIC_API_KEY"))
    raw_rows: list[dict] = []
    for wrapper in wrappers:
        started = time.perf_counter()
        try:
            response = client.messages.create(
                model=meta["model_id"],
                max_tokens=700,
                temperature=0,
                system=[{
                    "type": "text",
                    "text": system_prompt,
                }],
                messages=[{
                    "role": "user",
                    "content": [{"type": "text", "text": json.dumps(wrapper, ensure_ascii=True, sort_keys=True)}],
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
                    "contender_id": contender_id,
                    "raw_output": parsed if parsed is not None else raw_text,
                    "raw_text": raw_text,
                    "timing_ms": timing_ms,
                    "usage": anthropic_usage_to_dict(response.usage, meta["pricing"]),
                }
            )
        except Exception as exc:  # pragma: no cover - transport guard
            raw_rows.append(
                review_runtime_error_row(
                    wrapper,
                    contender_id,
                    str(exc),
                    int((time.perf_counter() - started) * 1000),
                )
            )
    write_jsonl(output_path, raw_rows)
    return output_path


def normalize_review_file(raw_path: Path, packet_lookup: dict[str, dict], output_path: Path) -> tuple[Path, dict[str, dict]]:
    allowed_rules = valid_rule_ids()
    normalized_rows: list[dict] = []
    rows_by_case: dict[str, dict] = {}
    for raw_row in load_jsonl(raw_path):
        packet = packet_lookup[raw_row["packet_id"]]
        normalized, error_row = parse_output(raw_row, packet, allowed_rules)
        row = error_row or normalized
        normalized_rows.append(row)
        rows_by_case[row["case_id"]] = row
    write_jsonl(output_path, normalized_rows)
    return output_path, rows_by_case


def combine_usage(*rows: dict) -> dict:
    combined = {
        "prompt_tokens": 0,
        "completion_tokens": 0,
        "search_calls": 0,
        "cost_usd": 0.0,
    }
    for row in rows:
        if not row:
            continue
        usage = row.get("usage") or {}
        combined["prompt_tokens"] += int(usage.get("prompt_tokens", 0) or 0)
        combined["completion_tokens"] += int(usage.get("completion_tokens", 0) or 0)
        combined["search_calls"] += int(usage.get("search_calls", 0) or 0)
        combined["cost_usd"] += float(usage.get("cost_usd", 0.0) or 0.0)
    combined["cost_usd"] = round(combined["cost_usd"], 6)
    return combined


def copy_with_zero_usage(row: dict, contender_id: str) -> dict:
    copied = deepcopy(row)
    copied["contender_id"] = contender_id
    copied["timing_ms"] = 0
    copied["usage"] = {
        "prompt_tokens": 0,
        "completion_tokens": 0,
        "search_calls": 0,
        "cost_usd": 0.0,
    }
    copied["layered_gate_action"] = "reused_base"
    return copied


def copy_with_usage(
    row: dict,
    contender_id: str,
    *,
    usage_rows: list[dict],
    action: str,
    note: str | None = None,
) -> dict:
    copied = deepcopy(row)
    copied["contender_id"] = contender_id
    copied["timing_ms"] = sum(int((item.get("timing_ms", 0) or 0)) for item in usage_rows if item)
    copied["usage"] = combine_usage(*usage_rows)
    copied["layered_gate_action"] = action
    if note:
        copied["layered_gate_note"] = note
    return copied


def load_session9_6_metrics() -> dict:
    payload = json.loads(SESSION9_6_SCORE_JSON.read_text(encoding="utf-8"))
    return payload["contenders"][0]


def routed_recovered_case_ids(benchmark_payload: dict, rows_by_case: dict[str, dict]) -> list[str]:
    recovered: list[str] = []
    for case in benchmark_payload["cases"]:
        if case["pattern_cluster"] not in {"11.4.h", "11.4.f", "11.4.n", "11.4.p"}:
            continue
        if case["expected_verdict"] != "MERGE":
            continue
        if rows_by_case[case["case_id"]]["normalized_output"]["verdict"] == "MERGE":
            recovered.append(case["case_id"])
    return recovered


def render_memo(
    *,
    run_name: str,
    contender_id: str,
    review_contender_ref: str | None,
    review_case_ids: list[str],
    deterministic_vetoes: list[dict],
    review_decisions: list[dict],
    summary: dict,
    incremental_review_spend: float,
    composite_rows_by_case: dict[str, dict],
    benchmark_payload: dict,
) -> str:
    current = summary["contenders"][0]
    prior = load_session9_6_metrics()
    prior_blind_core = prior["breakdowns"]["stratum"]["blind_core_audit"]
    current_blind_core = current["breakdowns"]["stratum"]["blind_core_audit"]
    prior_missed = prior_blind_core["hard_missed_merge"] + prior_blind_core["soft_missed_merge"]
    current_missed = current_blind_core["hard_missed_merge"] + current_blind_core["soft_missed_merge"]
    recovered = routed_recovered_case_ids(benchmark_payload, composite_rows_by_case)
    session96_bar = {
        "false_merge_zero": current["counts"]["false_merge"] == 0,
        "blind_core_false_merge_zero": current_blind_core["false_merge"] == 0,
        "blind_core_missed_lte_5": current_missed <= 5,
        "targeted_merge_recovered_gte_30": len(recovered) >= 30,
        "flag_rate_total_lte_0_25": (current["rates"]["flag_rate_total"] or 0.0) <= 0.25,
    }

    lines: list[str] = []
    lines.append("# Session 9.7 - layered safety gate memo")
    lines.append("")
    lines.append(f"- Generated: {datetime.now().astimezone().isoformat(timespec='seconds')}")
    lines.append(f"- Run name: `{run_name}`")
    lines.append(f"- Contender id: `{contender_id}`")
    lines.append(f"- Starting point: `{SPECIALIST_RUN_NAME}` layered on `{BASE_RUN_NAME}`")
    if review_contender_ref:
        lines.append(f"- Safety reviewer: `{review_contender_ref}`")
    else:
        lines.append("- Safety reviewer: none (`deterministic_veto_only`)")
    lines.append(f"- Review scope after deterministic vetoes: `{len(review_case_ids)}` specialist merge proposals in `11.4.f`")
    lines.append("")
    lines.append("## Composite scorecard")
    lines.append("")
    lines.append("| Metric | Session 9.6 specialist composite | This run |")
    lines.append("|---|---:|---:|")
    lines.append(f"| False merges overall | {prior['counts']['false_merge']} | {current['counts']['false_merge']} |")
    lines.append(f"| Blind-core false merges | {prior_blind_core['false_merge']} | {current_blind_core['false_merge']} |")
    lines.append(f"| Blind-core missed merges (hard + soft) | {prior_missed} | {current_missed} |")
    lines.append(f"| Routed merge recoveries | 42 / 47 | {len(recovered)} / 47 |")
    lines.append(f"| Full-benchmark flag rate | {(prior['rates']['flag_rate_total'] or 0):.4f} | {(current['rates']['flag_rate_total'] or 0):.4f} |")
    lines.append(f"| Exact verdict accuracy | {(prior['rates']['exact_verdict_accuracy'] or 0):.4f} | {(current['rates']['exact_verdict_accuracy'] or 0):.4f} |")
    lines.append(f"| Production gate | {prior['gates']['production']['status']} | {current['gates']['production']['status']} |")
    lines.append(f"| Fallback gate | {prior['gates']['fallback']['status']} | {current['gates']['fallback']['status']} |")
    lines.append("")
    lines.append("## Session 9.6 Continuation Bar")
    lines.append("")
    for label, ok in (
        ("0 false merges overall", session96_bar["false_merge_zero"]),
        ("0 blind-core false merges", session96_bar["blind_core_false_merge_zero"]),
        ("blind-core missed merges <= 5", session96_bar["blind_core_missed_lte_5"]),
        ("at least 30 / 47 routed merges recovered", session96_bar["targeted_merge_recovered_gte_30"]),
        ("full-benchmark flag rate <= 0.25", session96_bar["flag_rate_total_lte_0_25"]),
    ):
        lines.append(f"- {'PASS' if ok else 'FAIL'}: {label}")
    lines.append("")
    lines.append("## Deterministic Vetoes")
    lines.append("")
    if deterministic_vetoes:
        for item in deterministic_vetoes:
            lines.append(
                f"- `{item['case_id']}`: `{item['reason']}` "
                f"({item['producer_name_a']} <-> {item['producer_name_b']})"
            )
    else:
        lines.append("- none")
    lines.append("")
    lines.append("## Safety Review Decisions")
    lines.append("")
    if review_decisions:
        for item in review_decisions:
            lines.append(
                f"- `{item['case_id']}`: `{item['action']}` "
                f"({item['producer_name_a']} <-> {item['producer_name_b']})"
            )
    else:
        lines.append("- no model review decisions recorded")
    lines.append("")
    lines.append("## Readout")
    lines.append("")
    if current["counts"]["false_merge"] == 0:
        lines.append(
            "The layered redesign eliminated the Session 9.6 false merges on the frozen benchmark."
        )
    else:
        lines.append(
            "The layered redesign improved safety versus Session 9.6, but it still left false merges on the frozen benchmark."
        )
    lines.append(
        f"Incremental reviewer spend in this round was `${incremental_review_spend:.2f}`. "
        f"Composite contender cost reported in the scorecard includes carried specialist spend on routed cases and zero additional cost for reused base rows."
    )
    lines.append("")
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(description="Run the Session 9.7 layered safety-gate proof")
    parser.add_argument("--benchmark", type=Path, default=DEFAULT_BENCHMARK)
    parser.add_argument("--packet-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_RUN_ROOT)
    parser.add_argument("--run-name", required=True)
    parser.add_argument("--contender-id", required=True)
    parser.add_argument("--base-normalized", type=Path, default=DEFAULT_BASE_NORMALIZED)
    parser.add_argument("--specialist-root", type=Path, default=DEFAULT_SPECIALIST_ROOT)
    parser.add_argument(
        "--review-contender-ref",
        choices=["none", "gpt5mini_guardrailed_v2", "sonnet_guardrailed_v2", "gemini_guardrailed_v2"],
        default="none",
    )
    parser.add_argument(
        "--review-scope",
        choices=["all_11_4_f", "narrow_11_4_f_traps"],
        default="all_11_4_f",
    )
    args = parser.parse_args()

    benchmark_payload = load_benchmark_payload(args.benchmark)
    cases_by_id = benchmark_cases_by_id(benchmark_payload)
    full_packets, visible_packets, packet_validation = ensure_visible_packets(
        args.packet_dir,
        args.benchmark,
        force_rebuild=False,
    )
    full_packet_lookup = {packet["packet_id"]: packet for packet in full_packets}
    visible_packet_lookup = {packet["packet_id"]: packet for packet in visible_packets}

    base_rows = load_base_rows(args.base_normalized)
    specialist_rows = load_specialist_rows(args.specialist_root)

    deterministic_vetoes: list[dict] = []
    review_wrappers: list[dict] = []
    review_case_ids: list[str] = []

    for case_id, specialist_row in specialist_rows.items():
        if specialist_row["normalized_output"]["verdict"] != "MERGE":
            continue
        case = cases_by_id[case_id]
        packet_id = specialist_row["packet_id"]
        packet = full_packet_lookup[packet_id]
        veto_reason = deterministic_veto_reason(case, packet)
        if veto_reason:
            overlay = packet["envelope"]["benchmark_overlay"]
            deterministic_vetoes.append(
                {
                    "case_id": case_id,
                    "packet_id": packet_id,
                    "reason": veto_reason,
                    "producer_name_a": overlay["producer_name_a"],
                    "producer_name_b": overlay["producer_name_b"],
                }
            )
            continue
        if should_review_specialist_merge(case, packet, args.review_scope):
            review_case_ids.append(case_id)
            review_wrappers.append(
                build_review_wrapper(
                    case={"benchmark_id": benchmark_payload["benchmark_id"], **case},
                    visible_packet=deepcopy(visible_packet_lookup[packet_id]),
                    specialist_row=deepcopy(specialist_row),
                )
            )

    request_dir = args.output_root / "requests" / args.run_name
    raw_dir = args.output_root / "raw" / args.run_name
    normalized_dir = args.output_root / "normalized" / args.run_name
    review_request_path = request_dir / "safety_review_11_4_f.jsonl"
    write_jsonl(review_request_path, review_wrappers)
    write_json(
        request_dir / "request_validation.json",
        {
            "run_name": args.run_name,
            "review_contender_ref": args.review_contender_ref,
            "review_scope": args.review_scope,
            "review_case_ids": review_case_ids,
            "deterministic_vetoes": deterministic_vetoes,
            "packet_validation": packet_validation,
        },
    )

    review_rows_by_case: dict[str, dict] = {}
    incremental_review_spend = 0.0
    review_contender_id = f"{args.contender_id}_review"
    review_raw_path = raw_dir / "safety_review_11_4_f.jsonl"
    review_normalized_path = normalized_dir / "safety_review_11_4_f.jsonl"
    if args.review_contender_ref != "none" and review_wrappers:
        section_11 = load_section_11()
        system_prompt = build_safety_system_prompt(section_11)
        if CONTENDER_META[args.review_contender_ref]["provider"] == "anthropic":
            run_anthropic_reviews(
                review_wrappers,
                review_raw_path,
                args.review_contender_ref,
                review_contender_id,
                system_prompt,
            )
        else:
            run_openrouter_reviews(
                review_wrappers,
                review_raw_path,
                args.review_contender_ref,
                review_contender_id,
                system_prompt,
            )
        _, review_rows_by_case = normalize_review_file(review_raw_path, full_packet_lookup, review_normalized_path)
        incremental_review_spend = round(
            sum(float((row.get("usage") or {}).get("cost_usd", 0.0) or 0.0) for row in review_rows_by_case.values()),
            6,
        )

    deterministic_veto_lookup = {item["case_id"]: item for item in deterministic_vetoes}
    review_decisions: list[dict] = []
    composite_rows: list[dict] = []
    composite_rows_by_case: dict[str, dict] = {}
    for case in benchmark_payload["cases"]:
        case_id = case["case_id"]
        base_row = base_rows[case_id]
        specialist_row = specialist_rows.get(case_id)
        final_row: dict
        if not specialist_row:
            final_row = copy_with_zero_usage(base_row, args.contender_id)
        elif case_id in deterministic_veto_lookup:
            note = deterministic_veto_lookup[case_id]["reason"]
            final_row = copy_with_usage(
                base_row,
                args.contender_id,
                usage_rows=[specialist_row],
                action="deterministic_veto_to_base",
                note=note,
            )
        elif case_id in review_rows_by_case:
            review_row = review_rows_by_case[case_id]
            packet = full_packet_lookup[specialist_row["packet_id"]]
            overlay = packet["envelope"]["benchmark_overlay"]
            if review_row["normalized_output"]["verdict"] == "MERGE":
                final_row = copy_with_usage(
                    specialist_row,
                    args.contender_id,
                    usage_rows=[specialist_row, review_row],
                    action="review_kept_merge",
                )
                review_decisions.append(
                    {
                        "case_id": case_id,
                        "action": "keep_merge",
                        "producer_name_a": overlay["producer_name_a"],
                        "producer_name_b": overlay["producer_name_b"],
                    }
                )
            else:
                final_row = copy_with_usage(
                    base_row,
                    args.contender_id,
                    usage_rows=[specialist_row, review_row],
                    action="review_veto_to_base",
                )
                review_decisions.append(
                    {
                        "case_id": case_id,
                        "action": "veto_to_base",
                        "producer_name_a": overlay["producer_name_a"],
                        "producer_name_b": overlay["producer_name_b"],
                    }
                )
        else:
            final_row = copy_with_usage(
                specialist_row,
                args.contender_id,
                usage_rows=[specialist_row],
                action="specialist_kept",
            )
        composite_rows.append(final_row)
        composite_rows_by_case[case_id] = final_row

    composite_path = normalized_dir / f"{args.contender_id}.jsonl"
    write_jsonl(composite_path, composite_rows)

    summary = score_run(
        benchmark_payload=benchmark_payload,
        normalized_paths=[composite_path],
        full_packets=full_packets,
        output_json=args.output_root / "scored" / f"{args.run_name}.json",
        output_md=args.output_root / "scored" / f"{args.run_name}.md",
        run_name=args.run_name,
    )
    memo = render_memo(
        run_name=args.run_name,
        contender_id=args.contender_id,
        review_contender_ref=None if args.review_contender_ref == "none" else args.review_contender_ref,
        review_case_ids=review_case_ids,
        deterministic_vetoes=deterministic_vetoes,
        review_decisions=review_decisions,
        summary=summary,
        incremental_review_spend=incremental_review_spend,
        composite_rows_by_case=composite_rows_by_case,
        benchmark_payload=benchmark_payload,
    )
    memo_path = args.output_root / "scored" / f"{args.run_name}_memo.md"
    memo_path.write_text(memo, encoding="utf-8")
    write_json(
        args.output_root / "scored" / f"{args.run_name}_manifest.json",
        {
            "run_name": args.run_name,
            "contender_id": args.contender_id,
            "review_contender_ref": args.review_contender_ref,
            "review_scope": args.review_scope,
            "requests": {"safety_review_11_4_f": str(review_request_path)},
            "raw": {"safety_review_11_4_f": str(review_raw_path) if review_raw_path.exists() else None},
            "normalized": {
                "safety_review_11_4_f": str(review_normalized_path) if review_normalized_path.exists() else None,
                "composite": str(composite_path),
            },
            "deterministic_vetoes": deterministic_vetoes,
            "review_decisions": review_decisions,
            "incremental_review_spend_usd": incremental_review_spend,
            "outputs": {
                "score_json": str(args.output_root / "scored" / f"{args.run_name}.json"),
                "score_md": str(args.output_root / "scored" / f"{args.run_name}.md"),
                "memo_md": str(memo_path),
            },
        },
    )
    print(f"Completed layered safety-gate run: {args.run_name}")
    print(f"Score JSON: {args.output_root / 'scored' / f'{args.run_name}.json'}")
    print(f"Memo: {memo_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
