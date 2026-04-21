"""Session 9.6 - routed pattern-family specialist proof on the frozen benchmark.

Builds one bounded proof layer on top of the safe `gemini_guardrailed_v2` base:

- Route only the four approved families (`11.4.h`, `11.4.f`, `11.4.n`, `11.4.p`)
  through family-specific Gemini specialist prompts.
- Reuse the frozen Session 9.3 Gemini normalized rows for the other 79 cases.
- Score the composite result back against the full 152-case benchmark.

Artifacts land under:
    data/sprints/dedup/bakeoff_v2/
"""

from __future__ import annotations

import argparse
import json
import time
from copy import deepcopy
from datetime import datetime
from pathlib import Path

import requests

from pipeline.identity.bakeoff_harness_v1 import (
    load_benchmark_payload,
    score_run,
    valid_rule_ids,
)
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
    extract_json_object,
)
from pipeline.lib.db import get_env


REPO_ROOT = Path(__file__).resolve().parents[2]
RUN_NAME_DEFAULT = "session9_6_pattern_specialist_proof_if_approved"
BASE_RUN_NAME = "session9_3_full_rerun_if_approved"
BASE_CONTENDER_ID = "gemini_guardrailed_v2"
SPECIALIST_CONTENDER_ID = "gemini_routed_pattern_specialist_v1"
DEFAULT_BASE_NORMALIZED = DEFAULT_RUN_ROOT / "normalized" / BASE_RUN_NAME / f"{BASE_CONTENDER_ID}.jsonl"
DEFAULT_BASE_SCORE_JSON = DEFAULT_RUN_ROOT / "scored" / f"{BASE_RUN_NAME}.json"
IDENTITY_RULES_PATH = REPO_ROOT / "docs" / "IDENTITY_RULES.md"
SPECIALIST_PRICING = CONTENDER_META[BASE_CONTENDER_ID]["pricing"]
SPECIALIST_MODEL_ID = CONTENDER_META[BASE_CONTENDER_ID]["model_id"]

ROUTED_FAMILIES = {
    "11.4.h": {
        "slug": "11_4_h",
        "label": "orthographic / short-full-form",
        "specialist_doctrine": [
            "Sparse official-domain retrieval is common here and does not automatically block MERGE.",
            "Strong local catalog coherence matters: exact overlap, clear subset shape, same-country or same-region footprint, and shared distinctive cuvee/place anchors can prove one producer identity.",
            "Shared-surname or family-split risk is the main trap. Do not merge merely because one name contains the other.",
            "SKIP when the packet suggests coexisting family branches, separate estates, or commune/place ambiguity.",
            "Use FLAGGED only when the evidence stays genuinely mixed after applying the family-specific doctrine.",
        ],
    },
    "11.4.f": {
        "slug": "11_4_f",
        "label": "generational / historical-form",
        "specialist_doctrine": [
            "MERGE when the packet supports one estate or producer continuing through a generational or historical label change, especially with shared appellation footprint, exact overlap, or clear subset shape.",
            "Shared surname alone is not enough; many family branches must remain separate.",
            "Ownership, operator, or acquisition language is weak unless it still points to one continuous on-label estate identity.",
            "SKIP when the packet more plausibly shows parallel family branches or separate current producer identities.",
            "Use FLAGGED only when continuity is plausible but the packet still leaves real ambiguity.",
        ],
    },
    "11.4.n": {
        "slug": "11_4_n",
        "label": "global multi-country brand",
        "specialist_doctrine": [
            "Country conflict is expected in this family and is not a contradiction by itself.",
            "Exact same on-label brand name across multiple countries often indicates one global brand sourcing wine from different countries.",
            "MERGE when the packet shows the same brand identity across countries and there is no evidence of two unrelated local producers.",
            "SKIP when the same words are generic or place-based, or when the packet points to distinct local producers rather than one global brand.",
            "Use FLAGGED only when the packet is truly unresolved after applying the family-specific doctrine.",
        ],
    },
    "11.4.p": {
        "slug": "11_4_p",
        "label": "merchant / curation prefix",
        "specialist_doctrine": [
            "MERGE only when the merchant or prefixed row clearly resolves to the same underlying producer identity through exact overlap, subset shape, or explicit embedded producer naming.",
            "SKIP when the same merchant or curation umbrella fronts distinct source estates or bottlers.",
            "Merchant or private-label umbrellas are not global-brand merges by default.",
            "Use FLAGGED only when the packet points toward a single underlying producer but does not cleanly prove it.",
        ],
    },
}


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


def build_specialist_system_prompt(family_id: str, section_11: str) -> str:
    spec = ROUTED_FAMILIES[family_id]
    doctrine = "\n".join(f"- {line}" for line in spec["specialist_doctrine"])
    return f"""You are `pattern_specialist_{spec['slug']}_v1`, a routed producer-dedup specialist.

This run routes only family `{family_id}` ({spec['label']}) through you.
Use ONLY the JSON wrapper you receive. Do not use outside knowledge, live web
search, or hidden benchmark speculation.

Apply the product's producer identity rules:

{section_11}

Family-specific doctrine for `{family_id}`:
{doctrine}

Important override versus the general v2 adjudicator:
- Do NOT automatically veto `MERGE` just because official-domain evidence is sparse.
- Instead, decide using the family-specific doctrine above plus the packet's
  local catalog coherence, geography, and citeable evidence refs.

Output contract requirements:
- Return exactly one JSON object and nothing else.
- Allowed `verdict` values: `MERGE`, `SKIP`, `FLAGGED`.
- `confidence` must be a JSON number from 0 to 1, never a quoted string.
- `rule_ids` must cite real Section 11 rule ids such as `11.1`, `11.4.h`,
  `11.4.f`, `11.4.n`, `11.4.p`, `11.5`, `11.6`.
- `key_support_refs` and `key_contradiction_refs` must use ONLY ids from
  `allowed_ref_ids`.
- Every output must include at least one `key_support_refs` item.
- If `verdict` is `MERGE` and `recommended_survivor_producer_id` is non-null in
  the packet, set `survivor_producer_id` to that exact recommended survivor id.
- If `verdict` is `SKIP` or `FLAGGED`, set `survivor_producer_id` to null.
- Use `FLAGGED` only when the family-specific evidence is genuinely unresolved.
- `reason` should be one short evidence-grounded paragraph.
- `follow_up` should be null unless more evidence or human review is needed.

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


def build_request_wrapper(benchmark_id: str, case: dict, visible_packet: dict, family_id: str) -> dict:
    return {
        "benchmark_id": benchmark_id,
        "case_id": case["case_id"],
        "contender_id": SPECIALIST_CONTENDER_ID,
        "specialist_family": family_id,
        "instructions_version": f"pattern_specialist_{ROUTED_FAMILIES[family_id]['slug']}_v1",
        "packet_visible": visible_packet,
        "allowed_ref_ids": visible_packet["envelope"]["allowed_ref_ids"],
        "output_contract_version": "adjudication_output_v2",
        "allow_tools": False,
        "temperature": 0,
    }


def collect_routed_cases(benchmark_payload: dict) -> dict[str, list[dict]]:
    family_cases = {family_id: [] for family_id in ROUTED_FAMILIES}
    for case in benchmark_payload["cases"]:
        family_id = case["pattern_cluster"]
        if family_id in family_cases:
            family_cases[family_id].append(case)
    return family_cases


def prepare_family_request_files(
    benchmark_payload: dict,
    visible_packets: list[dict],
    output_root: Path,
    run_name: str,
) -> tuple[dict[str, list[dict]], dict[str, Path], dict]:
    visible_by_id = {packet["packet_id"]: packet for packet in visible_packets}
    request_rows: dict[str, list[dict]] = {}
    request_paths: dict[str, Path] = {}
    family_case_ids: dict[str, list[str]] = {}
    benchmark_id = benchmark_payload["benchmark_id"]

    for family_id, cases in collect_routed_cases(benchmark_payload).items():
        rows: list[dict] = []
        family_case_ids[family_id] = [case["case_id"] for case in cases]
        for case in cases:
            packet_id = f"producer_pair_{case['pair_id']}_v2"
            rows.append(build_request_wrapper(benchmark_id, case, deepcopy(visible_by_id[packet_id]), family_id))
        path = output_root / "requests" / run_name / f"{ROUTED_FAMILIES[family_id]['slug']}.jsonl"
        write_jsonl(path, rows)
        request_rows[family_id] = rows
        request_paths[family_id] = path

    validation = {
        "benchmark_id": benchmark_payload["benchmark_id"],
        "run_name": run_name,
        "families": {
            family_id: {
                "label": ROUTED_FAMILIES[family_id]["label"],
                "case_count": len(case_ids),
                "case_ids": case_ids,
            }
            for family_id, case_ids in family_case_ids.items()
        },
        "total_case_count": sum(len(case_ids) for case_ids in family_case_ids.values()),
    }
    write_json(output_root / "requests" / run_name / "request_validation.json", validation)
    return request_rows, request_paths, validation


def openrouter_usage_to_dict(usage: dict | None) -> dict:
    usage = usage or {}
    prompt_tokens = int(usage.get("prompt_tokens", 0) or 0)
    completion_tokens = int(usage.get("completion_tokens", 0) or 0)
    cost = (
        prompt_tokens * SPECIALIST_PRICING["input"] / 1_000_000
        + completion_tokens * SPECIALIST_PRICING["output"] / 1_000_000
    )
    return {
        "prompt_tokens": prompt_tokens,
        "completion_tokens": completion_tokens,
        "search_calls": 0,
        "cost_usd": round(cost, 6),
    }


def sanitize_error(text: str) -> str:
    cleaned = "".join(ch if ch.isalnum() or ch in "_:/.-" else "_" for ch in text.strip().lower())
    return cleaned[:120] or "unknown_error"


def run_family_requests(
    family_id: str,
    wrappers: list[dict],
    output_path: Path,
    system_prompt: str,
) -> Path:
    headers = {
        "Authorization": f"Bearer {get_env('OPENROUTER_API_KEY')}",
        "Content-Type": "application/json",
        "HTTP-Referer": "https://loam.onrender.com",
        "X-Title": "Loam Session 9.6 Pattern Specialist Proof",
    }
    raw_rows: list[dict] = []
    for wrapper in wrappers:
        body = {
            "model": SPECIALIST_MODEL_ID,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": json.dumps(wrapper, ensure_ascii=True, sort_keys=True)},
            ],
            "temperature": 0,
            "max_tokens": 900,
            "provider": {"require_parameters": True},
            "reasoning": {"effort": "none"},
        }
        started = time.perf_counter()
        try:
            response = requests.post(OPENROUTER_URL, headers=headers, json=body, timeout=120)
            timing_ms = int((time.perf_counter() - started) * 1000)
            if response.status_code != 200:
                raw_rows.append(
                    {
                        "benchmark_id": wrapper["benchmark_id"],
                        "case_id": wrapper["case_id"],
                        "packet_id": wrapper["packet_visible"]["packet_id"],
                        "contender_id": SPECIALIST_CONTENDER_ID,
                        "raw_output": None,
                        "runtime_error": sanitize_error(f"HTTP_{response.status_code}:{response.text[:200]}"),
                        "timing_ms": timing_ms,
                        "usage": {
                            "prompt_tokens": 0,
                            "completion_tokens": 0,
                            "search_calls": 0,
                            "cost_usd": 0.0,
                        },
                    }
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
                    "contender_id": SPECIALIST_CONTENDER_ID,
                    "specialist_family": family_id,
                    "raw_output": parsed if parsed is not None else raw_text,
                    "raw_text": raw_text,
                    "timing_ms": timing_ms,
                    "usage": openrouter_usage_to_dict(payload.get("usage")),
                }
            )
        except Exception as exc:  # pragma: no cover - network transport safety
            raw_rows.append(
                {
                    "benchmark_id": wrapper["benchmark_id"],
                    "case_id": wrapper["case_id"],
                    "packet_id": wrapper["packet_visible"]["packet_id"],
                    "contender_id": SPECIALIST_CONTENDER_ID,
                    "specialist_family": family_id,
                    "raw_output": None,
                    "runtime_error": sanitize_error(str(exc)),
                    "timing_ms": int((time.perf_counter() - started) * 1000),
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


def normalize_specialist_file(raw_path: Path, packet_lookup: dict[str, dict], output_path: Path) -> Path:
    allowed_rules = valid_rule_ids()
    normalized_rows: list[dict] = []
    for raw_row in load_jsonl(raw_path):
        packet = packet_lookup[raw_row["packet_id"]]
        normalized, error_row = parse_output(raw_row, packet, allowed_rules)
        row = error_row or normalized
        row["contender_id"] = SPECIALIST_CONTENDER_ID
        normalized_rows.append(row)
    write_jsonl(output_path, normalized_rows)
    return output_path


def zero_usage_copy(row: dict) -> dict:
    copied = deepcopy(row)
    copied["contender_id"] = SPECIALIST_CONTENDER_ID
    copied["timing_ms"] = 0
    copied["usage"] = {
        "prompt_tokens": 0,
        "completion_tokens": 0,
        "search_calls": 0,
        "cost_usd": 0.0,
    }
    return copied


def build_composite_rows(
    benchmark_payload: dict,
    base_rows_by_case: dict[str, dict],
    routed_rows_by_case: dict[str, dict],
) -> list[dict]:
    rows: list[dict] = []
    for case in benchmark_payload["cases"]:
        case_id = case["case_id"]
        if case_id in routed_rows_by_case:
            rows.append(routed_rows_by_case[case_id])
        else:
            rows.append(zero_usage_copy(base_rows_by_case[case_id]))
    return rows


def load_base_rows(path: Path) -> dict[str, dict]:
    return {row["case_id"]: row for row in load_jsonl(path)}


def routed_family_breakdown(benchmark_payload: dict, composite_rows_by_case: dict[str, dict]) -> dict[str, dict]:
    results: dict[str, dict] = {}
    for family_id in ROUTED_FAMILIES:
        family_cases = [case for case in benchmark_payload["cases"] if case["pattern_cluster"] == family_id]
        stats = {
            "case_count": len(family_cases),
            "expected_merge": 0,
            "expected_skip": 0,
            "recovered_merge": 0,
            "false_merge": 0,
            "hard_missed": 0,
            "soft_missed": 0,
            "safe_flag": 0,
        }
        for case in family_cases:
            verdict = composite_rows_by_case[case["case_id"]]["normalized_output"]["verdict"]
            expected = case["expected_verdict"]
            if expected == "MERGE":
                stats["expected_merge"] += 1
                if verdict == "MERGE":
                    stats["recovered_merge"] += 1
                elif verdict == "SKIP":
                    stats["hard_missed"] += 1
                elif verdict == "FLAGGED":
                    stats["soft_missed"] += 1
            else:
                stats["expected_skip"] += 1
                if verdict == "MERGE":
                    stats["false_merge"] += 1
                elif verdict == "FLAGGED":
                    stats["safe_flag"] += 1
        results[family_id] = stats
    return results


def find_recovered_case_ids(benchmark_payload: dict, composite_rows_by_case: dict[str, dict]) -> list[str]:
    recovered: list[str] = []
    for case in benchmark_payload["cases"]:
        if case["pattern_cluster"] not in ROUTED_FAMILIES or case["expected_verdict"] != "MERGE":
            continue
        if composite_rows_by_case[case["case_id"]]["normalized_output"]["verdict"] == "MERGE":
            recovered.append(case["case_id"])
    return recovered


def load_base_metrics() -> dict:
    payload = json.loads(DEFAULT_BASE_SCORE_JSON.read_text(encoding="utf-8"))
    for contender in payload["contenders"]:
        if contender["contender_id"] == BASE_CONTENDER_ID:
            return contender
    raise RuntimeError(f"Could not find {BASE_CONTENDER_ID} in {DEFAULT_BASE_SCORE_JSON}")


def render_memo(
    benchmark_payload: dict,
    summary: dict,
    family_breakdown: dict[str, dict],
    recovered_case_ids: list[str],
    session_spend_usd: float,
    normalization_failures: int,
) -> str:
    contender = summary["contenders"][0]
    base = load_base_metrics()
    blind_core_counts = contender["breakdowns"]["stratum"]["blind_core_audit"]
    blind_core_missed = blind_core_counts["hard_missed_merge"] + blind_core_counts["soft_missed_merge"]
    checks = {
        "false_merge_zero": contender["counts"]["false_merge"] == 0,
        "blind_core_false_merge_zero": blind_core_counts["false_merge"] == 0,
        "blind_core_missed_lte_5": blind_core_missed <= 5,
        "targeted_merge_recovered_gte_30": len(recovered_case_ids) >= 30,
        "flag_rate_total_lte_0_25": (contender["rates"]["flag_rate_total"] or 0.0) <= 0.25,
    }
    passed = all(checks.values())

    lines: list[str] = []
    lines.append("# Session 9.6 - routed pattern specialist proof memo")
    lines.append("")
    lines.append(f"- Generated: {datetime.now().astimezone().isoformat(timespec='seconds')}")
    lines.append(f"- Run name: `{summary['run_name']}`")
    lines.append(f"- Base path reused outside routed bundle: `{BASE_CONTENDER_ID}` from `{BASE_RUN_NAME}`")
    lines.append(f"- Routed families: {', '.join(f'`{family_id}`' for family_id in ROUTED_FAMILIES)}")
    lines.append(f"- Routed specialist model: `{SPECIALIST_MODEL_ID}`")
    lines.append(f"- Proof verdict: {'PASS' if passed else 'FAIL'}")
    lines.append("")
    lines.append("## Composite scorecard")
    lines.append("")
    lines.append("| Metric | Base Gemini | Routed specialist composite |")
    lines.append("|---|---:|---:|")
    lines.append(f"| False merges overall | {base['counts']['false_merge']} | {contender['counts']['false_merge']} |")
    lines.append(f"| Blind-core false merges | {base['breakdowns']['stratum']['blind_core_audit']['false_merge']} | {blind_core_counts['false_merge']} |")
    lines.append(
        f"| Blind-core missed merges (hard + soft) | "
        f"{base['breakdowns']['stratum']['blind_core_audit']['hard_missed_merge'] + base['breakdowns']['stratum']['blind_core_audit']['soft_missed_merge']} | "
        f"{blind_core_missed} |"
    )
    lines.append(f"| Routed merge recoveries | 0 / 47 | {len(recovered_case_ids)} / 47 |")
    lines.append(f"| Full-benchmark flag rate | {(base['rates']['flag_rate_total'] or 0):.4f} | {(contender['rates']['flag_rate_total'] or 0):.4f} |")
    lines.append(f"| Exact verdict accuracy | {(base['rates']['exact_verdict_accuracy'] or 0):.4f} | {(contender['rates']['exact_verdict_accuracy'] or 0):.4f} |")
    lines.append("")
    lines.append("## Routed family breakdown")
    lines.append("")
    lines.append("| Family | Cases | Expected merges | Recovered merges | False merges | Hard missed | Soft missed | Safe flag |")
    lines.append("|---|---:|---:|---:|---:|---:|---:|---:|")
    for family_id in ROUTED_FAMILIES:
        stats = family_breakdown[family_id]
        lines.append(
            f"| {family_id} | {stats['case_count']} | {stats['expected_merge']} | {stats['recovered_merge']} | "
            f"{stats['false_merge']} | {stats['hard_missed']} | {stats['soft_missed']} | {stats['safe_flag']} |"
        )
    lines.append("")
    lines.append("## Success bar check")
    lines.append("")
    for label, ok in (
        ("0 false merges overall", checks["false_merge_zero"]),
        ("0 blind-core false merges", checks["blind_core_false_merge_zero"]),
        ("blind-core missed merges <= 5", checks["blind_core_missed_lte_5"]),
        ("at least 30 / 47 targeted merges recovered", checks["targeted_merge_recovered_gte_30"]),
        ("full-benchmark flag_rate_total <= 0.25", checks["flag_rate_total_lte_0_25"]),
    ):
        lines.append(f"- {'PASS' if ok else 'FAIL'}: {label}")
    lines.append("")
    lines.append("## Readout")
    lines.append("")
    if passed:
        lines.append(
            "The routed-specialist proof cleared the Session 9.6 bar. This is strong enough to justify a deliberate next-build discussion, "
            "but it is still not approval for queue-building or all-pairs scale-up."
        )
    else:
        lines.append(
            "The routed-specialist proof did not clear the Session 9.6 bar. The family-routed redesign improved recall, but not enough to justify "
            "further build without first accepting a lower quality bar or a broader redesign."
        )
    if normalization_failures:
        lines.append(f"Normalization note: {normalization_failures} routed specialist rows failed contract validation and were fail-closed to `FLAGGED`.")
    lines.append(
        f"Session 9.6 incremental spend was `${session_spend_usd:.2f}`. Reused base Gemini rows outside the routed bundle were carried forward at zero additional cost for this proof."
    )
    lines.append("")
    lines.append("Recovered merge cases:")
    if recovered_case_ids:
        for case_id in recovered_case_ids:
            lines.append(f"- `{case_id}`")
    else:
        lines.append("- none")
    lines.append("")
    return "\n".join(lines)


def build_manifest(
    *,
    run_name: str,
    request_validation: dict,
    request_paths: dict[str, Path],
    raw_paths: dict[str, Path],
    normalized_paths: dict[str, Path],
    composite_path: Path,
    score_json: Path,
    score_md: Path,
    memo_path: Path,
    session_spend_usd: float,
) -> dict:
    return {
        "run_name": run_name,
        "base_run_name": BASE_RUN_NAME,
        "base_contender_id": BASE_CONTENDER_ID,
        "specialist_contender_id": SPECIALIST_CONTENDER_ID,
        "specialist_model_id": SPECIALIST_MODEL_ID,
        "request_validation": request_validation,
        "requests": {family_id: str(path) for family_id, path in request_paths.items()},
        "raw": {family_id: str(path) for family_id, path in raw_paths.items()},
        "normalized": {
            **{family_id: str(path) for family_id, path in normalized_paths.items()},
            "composite": str(composite_path),
        },
        "outputs": {
            "score_json": str(score_json),
            "score_md": str(score_md),
            "memo_md": str(memo_path),
        },
        "session_spend_usd": round(session_spend_usd, 6),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Run the Session 9.6 routed pattern-specialist proof")
    parser.add_argument("--benchmark", type=Path, default=DEFAULT_BENCHMARK)
    parser.add_argument("--packet-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_RUN_ROOT)
    parser.add_argument("--base-normalized", type=Path, default=DEFAULT_BASE_NORMALIZED)
    parser.add_argument("--run-name", default=RUN_NAME_DEFAULT)
    parser.add_argument("--force-rebuild-packets", action="store_true")
    args = parser.parse_args()

    benchmark_payload = load_benchmark_payload(args.benchmark)
    full_packets, visible_packets, packet_validation = ensure_visible_packets(
        args.packet_dir,
        args.benchmark,
        force_rebuild=args.force_rebuild_packets,
    )
    packet_lookup = {packet["packet_id"]: packet for packet in full_packets}
    base_rows_by_case = load_base_rows(args.base_normalized)

    request_rows, request_paths, request_validation = prepare_family_request_files(
        benchmark_payload=benchmark_payload,
        visible_packets=visible_packets,
        output_root=args.output_root,
        run_name=args.run_name,
    )

    section_11 = load_section_11()
    raw_paths: dict[str, Path] = {}
    normalized_paths: dict[str, Path] = {}
    routed_rows_by_case: dict[str, dict] = {}

    for family_id, wrappers in request_rows.items():
        system_prompt = build_specialist_system_prompt(family_id, section_11)
        raw_path = args.output_root / "raw" / args.run_name / f"{ROUTED_FAMILIES[family_id]['slug']}.jsonl"
        raw_paths[family_id] = run_family_requests(family_id, wrappers, raw_path, system_prompt)
        normalized_path = args.output_root / "normalized" / args.run_name / f"{ROUTED_FAMILIES[family_id]['slug']}.jsonl"
        normalized_paths[family_id] = normalize_specialist_file(raw_paths[family_id], packet_lookup, normalized_path)
        for row in load_jsonl(normalized_paths[family_id]):
            routed_rows_by_case[row["case_id"]] = row

    composite_rows = build_composite_rows(benchmark_payload, base_rows_by_case, routed_rows_by_case)
    composite_path = args.output_root / "normalized" / args.run_name / f"{SPECIALIST_CONTENDER_ID}.jsonl"
    write_jsonl(composite_path, composite_rows)

    score_json = args.output_root / "scored" / f"{args.run_name}.json"
    score_md = args.output_root / "scored" / f"{args.run_name}.md"
    summary = score_run(
        benchmark_payload=benchmark_payload,
        normalized_paths=[composite_path],
        full_packets=full_packets,
        output_json=score_json,
        output_md=score_md,
        run_name=args.run_name,
    )

    composite_rows_by_case = {row["case_id"]: row for row in composite_rows}
    family_breakdown = routed_family_breakdown(benchmark_payload, composite_rows_by_case)
    recovered_case_ids = find_recovered_case_ids(benchmark_payload, composite_rows_by_case)
    session_spend_usd = round(sum(float(row["usage"].get("cost_usd", 0.0) or 0.0) for row in routed_rows_by_case.values()), 6)
    normalization_failures = sum(1 for row in routed_rows_by_case.values() if not row["schema_valid"])

    memo_path = args.output_root / "scored" / f"{args.run_name}_memo.md"
    memo_path.write_text(
        render_memo(
            benchmark_payload=benchmark_payload,
            summary=summary,
            family_breakdown=family_breakdown,
            recovered_case_ids=recovered_case_ids,
            session_spend_usd=session_spend_usd,
            normalization_failures=normalization_failures,
        ),
        encoding="utf-8",
    )

    manifest = build_manifest(
        run_name=args.run_name,
        request_validation={
            **request_validation,
            "packet_validation": packet_validation,
        },
        request_paths=request_paths,
        raw_paths=raw_paths,
        normalized_paths=normalized_paths,
        composite_path=composite_path,
        score_json=score_json,
        score_md=score_md,
        memo_path=memo_path,
        session_spend_usd=session_spend_usd,
    )
    write_json(args.output_root / "scored" / f"{args.run_name}_manifest.json", manifest)

    print(f"Completed routed specialist proof: {args.run_name}")
    print(f"Composite normalized rows: {composite_path}")
    print(f"Score JSON: {score_json}")
    print(f"Memo: {memo_path}")
    print(f"Session spend USD: {session_spend_usd:.6f}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
