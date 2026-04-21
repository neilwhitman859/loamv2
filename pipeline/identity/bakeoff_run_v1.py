"""Session 6 - first real producer-dedup adjudication bakeoff.

Runs the frozen Session 4 lineup through the existing Session 5 packet /
request / normalize / score path without changing the contract.

Artifacts land under:
    data/sprints/dedup/bakeoff_v1/

Default canonical run name:
    session6_first_real_bakeoff_v1

Usage:
    python -m pipeline.identity.bakeoff_run_v1
    python -m pipeline.identity.bakeoff_run_v1 --run-name my_run
"""

from __future__ import annotations

import argparse
import json
import re
import time
from collections import Counter
from datetime import datetime
from pathlib import Path

import anthropic
import requests

from pipeline.identity.bakeoff_harness_v1 import (
    DEFAULT_RUN_ROOT,
    load_benchmark_payload,
    load_jsonl,
    normalize_file,
    packet_ref_ids,
    packets_by_id,
    prepare_request_wrappers,
    run_deterministic_request_file,
    score_run,
)
from pipeline.identity.bakeoff_packet_v1 import (
    DEFAULT_BENCHMARK,
    DEFAULT_OUTPUT_DIR,
    build_packets,
    canonical_json_dumps,
    write_jsonl,
)
from pipeline.lib.db import get_env


REPO_ROOT = Path(__file__).resolve().parents[2]
IDENTITY_RULES_PATH = REPO_ROOT / "docs" / "IDENTITY_RULES.md"
RUN_NAME_DEFAULT = "session6_first_real_bakeoff_v1"
OPENROUTER_URL = "https://openrouter.ai/api/v1/chat/completions"
VALID_RULE_FALLBACK = "11.1"

CONTENDER_ORDER = [
    "deterministic_control_v1",
    "haiku_single_v1",
    "gemini_single_v1",
    "gpt5mini_single_v1",
    "haiku_gemini_consensus_v1",
    "sonnet_single_v1",
]

CONTENDER_META = {
    "deterministic_control_v1": {
        "method_class": "deterministic control",
        "models": "none",
        "production_eligible": False,
    },
    "haiku_single_v1": {
        "method_class": "single-model adjudicator",
        "models": "Claude Haiku 4.5 (`claude-haiku-4-5-20251001`)",
        "provider": "anthropic",
        "model_id": "claude-haiku-4-5-20251001",
        "pricing": {
            "input": 0.80,
            "output": 4.00,
            "cache_read": 0.08,
            "cache_write_1h": 1.60,
        },
        "production_eligible": True,
    },
    "gemini_single_v1": {
        "method_class": "single-model adjudicator",
        "models": "Gemini 3 Flash Preview (`google/gemini-3-flash-preview`)",
        "provider": "openrouter",
        "model_id": "google/gemini-3-flash-preview",
        "pricing": {
            "input": 0.15,
            "output": 0.60,
        },
        "production_eligible": True,
    },
    "gpt5mini_single_v1": {
        "method_class": "single-model adjudicator",
        "models": "GPT-5.4-mini (`openai/gpt-5.4-mini`)",
        "provider": "openrouter",
        "model_id": "openai/gpt-5.4-mini",
        "pricing": {
            "input": 0.40,
            "output": 1.60,
        },
        "production_eligible": True,
    },
    "haiku_gemini_consensus_v1": {
        "method_class": "consensus adjudicator",
        "models": "Claude Haiku 4.5 + Gemini 3 Flash Preview",
        "production_eligible": True,
    },
    "sonnet_single_v1": {
        "method_class": "single-model adjudicator",
        "models": "Claude Sonnet 4.5 (`claude-sonnet-4-5-20250929`)",
        "provider": "anthropic",
        "model_id": "claude-sonnet-4-5-20250929",
        "pricing": {
            "input": 3.00,
            "output": 15.00,
            "cache_read": 0.30,
            "cache_write_1h": 6.00,
        },
        "production_eligible": True,
    },
}


def load_section_11() -> str:
    text = IDENTITY_RULES_PATH.read_text(encoding="utf-8")
    match = re.search(
        r"(## 11\. Producer Identity Rules.*?)(?=\n## Appendix|\n---\s*\n## )",
        text,
        re.DOTALL,
    )
    if not match:
        raise RuntimeError("Could not locate Section 11 in docs/IDENTITY_RULES.md")
    return match.group(1).strip()


def build_system_prompt(section_11: str) -> str:
    return f"""You are `bakeoff_adjudicator_v1`, a merge-only producer-dedup adjudicator.

You will receive exactly one JSON request wrapper containing a `packet_visible`
object. Use ONLY the evidence inside that packet. Do not use outside knowledge,
live web search, or hidden benchmark speculation.

Apply the product's producer identity rules strictly:

{section_11}

Output contract requirements:
- Return exactly one JSON object and nothing else.
- Allowed `verdict` values: `MERGE`, `SKIP`, `FLAGGED`.
- `PARENT_CHILD` is invalid in v1.
- `confidence` must be a number from 0 to 1.
- `rule_ids` must cite real Section 11 rule ids such as `11.1`, `11.4.h`, `11.4.m`, `11.6`.
- `key_support_refs` and `key_contradiction_refs` must cite only refs present in the packet:
  - `external_evidence.official_domain_hits[].ref_id`
  - `external_evidence.secondary_hits[].ref_id`
  - `comparison.support_signals[].code`
  - `comparison.contradiction_flags[].code`
- Every output must include at least one `key_support_refs` item. For `SKIP`, decisive contradiction flags like
  `country_conflict` or `shared_surname_split_risk` may appear in `key_support_refs` if they are the main support
  for the skip verdict. Use `key_contradiction_refs` for evidence you considered but did not follow.
- If `verdict` is `MERGE` and `survivor_if_merge.recommended_survivor_producer_id` is non-null, set
  `survivor_producer_id` to one of the listed `candidate_order` producer ids.
- If `verdict` is `SKIP` or `FLAGGED`, set `survivor_producer_id` to null.
- Use `FLAGGED` when evidence is sparse, contradictory, or not strong enough for a safe merge/skip call.
- `reason` should be 1 short paragraph, concrete and citation-driven.
- `follow_up` should be null unless more evidence or human review is needed.

Return this exact shape:
{{
  "packet_id": "<packet id>",
  "verdict": "MERGE | SKIP | FLAGGED",
  "confidence": 0.0,
  "rule_ids": ["11.x"],
  "reason": "short evidence-grounded explanation",
  "key_support_refs": ["ref_or_code"],
  "key_contradiction_refs": ["ref_or_code"],
  "survivor_producer_id": "uuid_or_null",
  "follow_up": null
}}
"""


def write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(canonical_json_dumps(payload), encoding="utf-8")


def rebuild_packets(benchmark_path: Path, packet_dir: Path) -> tuple[list[dict], list[dict], dict]:
    benchmark_id, full_packets, visible_packets, validations = build_packets(benchmark_path=benchmark_path)
    full_path = packet_dir / "benchmark_v1_packets_full.jsonl"
    visible_path = packet_dir / "benchmark_v1_packets_visible.jsonl"
    validation_path = packet_dir / "benchmark_v1_packet_validation.json"
    write_jsonl(full_path, full_packets)
    write_jsonl(visible_path, visible_packets)
    validation_payload = {
        "benchmark_id": benchmark_id,
        "packet_count": len(full_packets),
        "visible_packet_count": len(visible_packets),
        "hidden_field_leaks": sum(item["hidden_field_leaks"] for item in validations),
        "cases": validations,
    }
    write_json(validation_path, validation_payload)
    return full_packets, visible_packets, validation_payload


def extract_json_object(text: str) -> dict | None:
    if not text:
        return None
    stripped = text.strip()
    try:
        parsed = json.loads(stripped)
        return parsed if isinstance(parsed, dict) else None
    except json.JSONDecodeError:
        pass

    candidates = list(re.finditer(r"\{(?:[^{}]|\{[^{}]*\})*\}", stripped, re.DOTALL))
    for match in reversed(candidates):
        try:
            parsed = json.loads(match.group(0))
        except json.JSONDecodeError:
            continue
        if isinstance(parsed, dict):
            return parsed
    return None


def sanitize_error(text: str) -> str:
    cleaned = re.sub(r"\s+", "_", text.strip().lower())
    cleaned = re.sub(r"[^a-z0-9_:/.-]+", "", cleaned)
    return cleaned[:120] or "unknown_error"


def anthropic_text(response) -> str:
    chunks: list[str] = []
    for block in response.content:
        if getattr(block, "type", None) == "text":
            chunks.append(block.text)
    return "".join(chunks).strip()


def anthropic_usage_to_dict(usage, pricing: dict) -> dict:
    input_tokens = int(getattr(usage, "input_tokens", 0) or 0)
    output_tokens = int(getattr(usage, "output_tokens", 0) or 0)
    cache_read = int(getattr(usage, "cache_read_input_tokens", 0) or 0)
    cache_write = int(getattr(usage, "cache_creation_input_tokens", 0) or 0)
    regular_input = max(0, input_tokens - cache_read - cache_write)
    cost = (
        regular_input * pricing["input"] / 1_000_000
        + output_tokens * pricing["output"] / 1_000_000
        + cache_read * pricing.get("cache_read", 0.0) / 1_000_000
        + cache_write * pricing.get("cache_write_1h", 0.0) / 1_000_000
    )
    return {
        "prompt_tokens": input_tokens,
        "completion_tokens": output_tokens,
        "search_calls": 0,
        "cost_usd": round(cost, 6),
        "cache_read_input_tokens": cache_read,
        "cache_creation_input_tokens": cache_write,
    }


def openrouter_usage_to_dict(usage: dict | None, pricing: dict) -> dict:
    usage = usage or {}
    prompt_tokens = int(usage.get("prompt_tokens", 0) or 0)
    completion_tokens = int(usage.get("completion_tokens", 0) or 0)
    cost = (
        prompt_tokens * pricing["input"] / 1_000_000
        + completion_tokens * pricing["output"] / 1_000_000
    )
    return {
        "prompt_tokens": prompt_tokens,
        "completion_tokens": completion_tokens,
        "search_calls": 0,
        "cost_usd": round(cost, 6),
    }


def build_runtime_error_row(wrapper: dict, contender_id: str, message: str, timing_ms: int, usage: dict | None = None) -> dict:
    return {
        "benchmark_id": wrapper["benchmark_id"],
        "case_id": wrapper["case_id"],
        "packet_id": wrapper["packet_visible"]["packet_id"],
        "contender_id": contender_id,
        "raw_output": None,
        "runtime_error": sanitize_error(message),
        "timing_ms": timing_ms,
        "usage": usage or {
            "prompt_tokens": 0,
            "completion_tokens": 0,
            "search_calls": 0,
            "cost_usd": 0.0,
        },
    }


def probe_anthropic_model(model_id: str, system_prompt: str) -> dict:
    client = anthropic.Anthropic(api_key=get_env("ANTHROPIC_API_KEY"))
    started = time.perf_counter()
    try:
        response = client.messages.create(
            model=model_id,
            max_tokens=32,
            temperature=0,
            system=[{
                "type": "text",
                "text": system_prompt,
                "cache_control": {"type": "ephemeral"},
            }],
            messages=[{
                "role": "user",
                "content": [{"type": "text", "text": 'Return exactly {"ok":true}'}],
            }],
        )
        text = anthropic_text(response)
        return {
            "ok": True,
            "model_id": model_id,
            "elapsed_ms": int((time.perf_counter() - started) * 1000),
            "sample_text": text[:120],
        }
    except Exception as exc:
        return {
            "ok": False,
            "model_id": model_id,
            "elapsed_ms": int((time.perf_counter() - started) * 1000),
            "error": str(exc),
        }


def probe_openrouter_model(model_id: str, system_prompt: str) -> dict:
    headers = {
        "Authorization": f"Bearer {get_env('OPENROUTER_API_KEY')}",
        "Content-Type": "application/json",
        "HTTP-Referer": "https://loam.onrender.com",
        "X-Title": "Loam Session 6 Bakeoff",
    }
    body = {
        "model": model_id,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": 'Return exactly {"ok":true}'},
        ],
        "temperature": 0,
        "max_tokens": 32,
    }
    if "gemini" in model_id:
        body["provider"] = {"require_parameters": True}
        body["reasoning"] = {"effort": "none"}

    started = time.perf_counter()
    try:
        response = requests.post(OPENROUTER_URL, headers=headers, json=body, timeout=60)
    except Exception as exc:
        return {
            "ok": False,
            "model_id": model_id,
            "elapsed_ms": int((time.perf_counter() - started) * 1000),
            "error": str(exc),
        }
    if response.status_code != 200:
        return {
            "ok": False,
            "model_id": model_id,
            "elapsed_ms": int((time.perf_counter() - started) * 1000),
            "error": f"HTTP_{response.status_code}:{response.text[:200]}",
        }
    payload = response.json()
    text = payload.get("choices", [{}])[0].get("message", {}).get("content", "")
    return {
        "ok": True,
        "model_id": model_id,
        "elapsed_ms": int((time.perf_counter() - started) * 1000),
        "sample_text": text[:120],
    }


def run_anthropic_request_file(request_file: Path, output_path: Path, contender_id: str, system_prompt: str) -> Path:
    spec = CONTENDER_META[contender_id]
    client = anthropic.Anthropic(api_key=get_env("ANTHROPIC_API_KEY"))
    rows = load_jsonl(request_file)
    raw_rows: list[dict] = []
    for wrapper in rows:
        started = time.perf_counter()
        try:
            response = client.messages.create(
                model=spec["model_id"],
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
                    "contender_id": contender_id,
                    "raw_output": parsed if parsed is not None else raw_text,
                    "raw_text": raw_text,
                    "timing_ms": timing_ms,
                    "usage": anthropic_usage_to_dict(response.usage, spec["pricing"]),
                }
            )
        except Exception as exc:
            raw_rows.append(build_runtime_error_row(wrapper, contender_id, str(exc), int((time.perf_counter() - started) * 1000)))
    write_jsonl(output_path, raw_rows)
    return output_path


def run_openrouter_request_file(request_file: Path, output_path: Path, contender_id: str, system_prompt: str) -> Path:
    spec = CONTENDER_META[contender_id]
    headers = {
        "Authorization": f"Bearer {get_env('OPENROUTER_API_KEY')}",
        "Content-Type": "application/json",
        "HTTP-Referer": "https://loam.onrender.com",
        "X-Title": "Loam Session 6 Bakeoff",
    }
    rows = load_jsonl(request_file)
    raw_rows: list[dict] = []
    for wrapper in rows:
        body = {
            "model": spec["model_id"],
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": json.dumps(wrapper, ensure_ascii=True, sort_keys=True)},
            ],
            "temperature": 0,
            "max_tokens": 900,
        }
        if "gemini" in spec["model_id"]:
            body["provider"] = {"require_parameters": True}
            body["reasoning"] = {"effort": "none"}

        started = time.perf_counter()
        try:
            response = requests.post(OPENROUTER_URL, headers=headers, json=body, timeout=120)
            timing_ms = int((time.perf_counter() - started) * 1000)
            if response.status_code != 200:
                raw_rows.append(
                    build_runtime_error_row(
                        wrapper,
                        contender_id,
                        f"HTTP_{response.status_code}:{response.text[:200]}",
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
                    "usage": openrouter_usage_to_dict(payload.get("usage"), spec["pricing"]),
                }
            )
        except Exception as exc:
            raw_rows.append(build_runtime_error_row(wrapper, contender_id, str(exc), int((time.perf_counter() - started) * 1000)))
    write_jsonl(output_path, raw_rows)
    return output_path


def parse_child_output(row: dict) -> dict | None:
    output = row.get("raw_output")
    if isinstance(output, dict):
        return output
    if isinstance(output, str):
        return extract_json_object(output)
    return None


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


def fallback_support_refs(packet: dict) -> list[str]:
    refs = sorted(packet_ref_ids(packet))
    return refs[:1]


def build_consensus_output(haiku_row: dict, gemini_row: dict, packet: dict) -> dict:
    fallback_refs = fallback_support_refs(packet)
    haiku_output = parse_child_output(haiku_row)
    gemini_output = parse_child_output(gemini_row)
    if haiku_row.get("runtime_error") or gemini_row.get("runtime_error"):
        return {
            "packet_id": packet["packet_id"],
            "verdict": "FLAGGED",
            "confidence": 0.0,
            "rule_ids": [VALID_RULE_FALLBACK],
            "reason": "Consensus could not be formed because one or both child contender calls failed at runtime.",
            "key_support_refs": fallback_refs,
            "key_contradiction_refs": [],
            "survivor_producer_id": None,
            "follow_up": "consensus_child_runtime_error",
        }
    if not haiku_output or not gemini_output:
        return {
            "packet_id": packet["packet_id"],
            "verdict": "FLAGGED",
            "confidence": 0.0,
            "rule_ids": [VALID_RULE_FALLBACK],
            "reason": "Consensus could not be formed because one or both child outputs were not valid JSON objects.",
            "key_support_refs": fallback_refs,
            "key_contradiction_refs": [],
            "survivor_producer_id": None,
            "follow_up": "consensus_child_invalid_output",
        }

    haiku_verdict = haiku_output.get("verdict")
    gemini_verdict = gemini_output.get("verdict")
    shared_support = [
        ref for ref in haiku_output.get("key_support_refs", [])
        if ref in gemini_output.get("key_support_refs", [])
    ]
    merged_support = list(dict.fromkeys(
        shared_support
        + haiku_output.get("key_support_refs", [])
        + gemini_output.get("key_support_refs", [])
        + fallback_refs
    ))
    merged_contradictions = list(dict.fromkeys(
        haiku_output.get("key_contradiction_refs", [])
        + gemini_output.get("key_contradiction_refs", [])
    ))
    merged_rules = list(dict.fromkeys(
        [rule for rule in haiku_output.get("rule_ids", []) if rule in gemini_output.get("rule_ids", [])]
        + haiku_output.get("rule_ids", [])
        + gemini_output.get("rule_ids", [])
        + [VALID_RULE_FALLBACK]
    ))

    if haiku_verdict == gemini_verdict:
        survivor_id = None
        if haiku_verdict == "MERGE":
            haiku_survivor = haiku_output.get("survivor_producer_id")
            gemini_survivor = gemini_output.get("survivor_producer_id")
            if haiku_survivor and gemini_survivor and haiku_survivor != gemini_survivor:
                return {
                    "packet_id": packet["packet_id"],
                    "verdict": "FLAGGED",
                    "confidence": round(min(float(haiku_output.get("confidence", 0.0)), float(gemini_output.get("confidence", 0.0))), 4),
                    "rule_ids": merged_rules[:3],
                    "reason": "Haiku and Gemini agreed on MERGE but disagreed on the survivor row, so the consensus path escalates this pair.",
                    "key_support_refs": merged_support[:3],
                    "key_contradiction_refs": merged_contradictions[:2],
                    "survivor_producer_id": None,
                    "follow_up": "consensus_survivor_disagreement",
                }
            survivor_id = haiku_survivor or gemini_survivor
        follow_up = "consensus_agreement_flagged" if haiku_verdict == "FLAGGED" else None
        return {
            "packet_id": packet["packet_id"],
            "verdict": haiku_verdict,
            "confidence": round(min(float(haiku_output.get("confidence", 0.0)), float(gemini_output.get("confidence", 0.0))), 4),
            "rule_ids": merged_rules[:3],
            "reason": (
                f"Cross-family consensus: Haiku and Gemini both returned {haiku_verdict}. "
                f"Haiku: {haiku_output.get('reason', '').strip()} Gemini: {gemini_output.get('reason', '').strip()}"
            ).strip(),
            "key_support_refs": merged_support[:3],
            "key_contradiction_refs": merged_contradictions[:2],
            "survivor_producer_id": survivor_id if haiku_verdict == "MERGE" else None,
            "follow_up": follow_up,
        }

    return {
        "packet_id": packet["packet_id"],
        "verdict": "FLAGGED",
        "confidence": round(min(float(haiku_output.get("confidence", 0.0)), float(gemini_output.get("confidence", 0.0))), 4),
        "rule_ids": merged_rules[:3],
        "reason": (
            f"Cross-family disagreement: Haiku returned {haiku_verdict} and Gemini returned {gemini_verdict}, "
            "so the consensus contender escalates this pair."
        ),
        "key_support_refs": merged_support[:3],
        "key_contradiction_refs": merged_contradictions[:2],
        "survivor_producer_id": None,
        "follow_up": "consensus_disagreement",
    }


def build_consensus_raw_file(
    haiku_raw_path: Path,
    gemini_raw_path: Path,
    packet_lookup: dict[str, dict],
    output_path: Path,
) -> Path:
    by_case = {}
    for row in load_jsonl(haiku_raw_path):
        by_case.setdefault(row["case_id"], {})["haiku"] = row
    for row in load_jsonl(gemini_raw_path):
        by_case.setdefault(row["case_id"], {})["gemini"] = row

    raw_rows: list[dict] = []
    for case_id in sorted(by_case):
        pair = by_case[case_id]
        haiku_row = pair["haiku"]
        gemini_row = pair["gemini"]
        packet = packet_lookup[haiku_row["packet_id"]]
        raw_rows.append(
            {
                "benchmark_id": haiku_row["benchmark_id"],
                "case_id": case_id,
                "packet_id": haiku_row["packet_id"],
                "contender_id": "haiku_gemini_consensus_v1",
                "raw_output": build_consensus_output(haiku_row, gemini_row, packet),
                "timing_ms": int(haiku_row.get("timing_ms", 0) or 0) + int(gemini_row.get("timing_ms", 0) or 0),
                "usage": combine_usage(haiku_row, gemini_row),
            }
        )
    write_jsonl(output_path, raw_rows)
    return output_path


def count_denominators(cases: list[dict], key: str) -> dict[str, dict[str, int]]:
    totals: dict[str, dict[str, int]] = {}
    for case in cases:
        bucket = case[key]
        info = totals.setdefault(bucket, {"total": 0, "merge": 0, "skip": 0})
        info["total"] += 1
        if case["expected_verdict"] == "MERGE":
            info["merge"] += 1
        elif case["expected_verdict"] == "SKIP":
            info["skip"] += 1
    return totals


def fmt_rate(value: float | None) -> str:
    if value is None:
        return "n/a"
    return f"{value:.4f}"


def bucket_exact_accuracy(bucket_counts: dict, denominators: dict[str, int]) -> float | None:
    total = denominators["total"]
    if total == 0:
        return None
    return round((bucket_counts["true_merge"] + bucket_counts["true_skip"]) / total, 4)


def bucket_flag_rate(bucket_counts: dict, denominators: dict[str, int]) -> float | None:
    total = denominators["total"]
    if total == 0:
        return None
    return round((bucket_counts["soft_missed_merge"] + bucket_counts["safe_flag"]) / total, 4)


def gate_failures(contender: dict, gate_name: str) -> list[str]:
    gate = contender["gates"][gate_name]
    checks = gate.get("checks") or {}
    if not checks:
        return [gate["status"]]
    return [name for name, passed in checks.items() if not passed]


def collect_error_ledger(summary: dict) -> list[dict]:
    ledger: list[dict] = []
    for contender in summary["contenders"]:
        for row in contender["error_ledger"]:
            ledger.append(row)
    ledger.sort(key=lambda row: (row["contender_id"], row["case_id"]))
    return ledger


def render_full_scorecard(summary: dict, benchmark_payload: dict) -> str:
    tier_denoms = count_denominators(benchmark_payload["cases"], "source_pair_tier")
    stratum_denoms = count_denominators(benchmark_payload["cases"], "stratum")
    lines: list[str] = []
    lines.append(f"# {summary['run_name']} - first real adjudication bakeoff")
    lines.append("")
    lines.append(f"- Generated: {datetime.now().astimezone().isoformat(timespec='seconds')}")
    lines.append(f"- Benchmark: `{summary['benchmark_id']}`")
    lines.append(f"- Cases scored: {summary['case_count_seen']} / {summary['case_count_expected']}")
    lines.append(f"- Full benchmark run: {'yes' if summary['full_benchmark_run'] else 'no'}")
    lines.append("")
    lines.append("## Overall summary")
    lines.append("")
    lines.append("| Contender | Method class | Model(s) | Exact acc | False merge | Hard missed | Soft missed | Safe flag | Survivor acc | Auditability | Cost/pair | Total cost | Production gate | Fallback gate |")
    lines.append("|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|---|")
    for contender in summary["contenders"]:
        meta = CONTENDER_META[contender["contender_id"]]
        lines.append(
            f"| {contender['contender_id']} | {meta['method_class']} | {meta['models']} | "
            f"{fmt_rate(contender['rates']['exact_verdict_accuracy'])} | "
            f"{contender['counts']['false_merge']} | "
            f"{contender['counts']['hard_missed_merge']} | "
            f"{contender['counts']['soft_missed_merge']} | "
            f"{contender['counts']['safe_flag']} | "
            f"{fmt_rate(contender['rates']['survivor_accuracy'])} | "
            f"{fmt_rate(contender['auditability']['auditability_score'])} | "
            f"{contender['cost']['cost_per_pair']:.6f} | "
            f"{contender['cost']['total_cost_usd']:.4f} | "
            f"{contender['gates']['production']['status']} | "
            f"{contender['gates']['fallback']['status']} |"
        )

    lines.append("")
    lines.append("## Core/tail breakdown")
    lines.append("")
    lines.append("| Contender | Tier | Cases | False merge | Hard missed | Soft missed | Safe flag | Exact acc | Flag rate |")
    lines.append("|---|---|---:|---:|---:|---:|---:|---:|---:|")
    for contender in summary["contenders"]:
        buckets = contender["breakdowns"]["source_pair_tier"]
        for tier in ("core", "tail"):
            counts = buckets.get(tier, Counter())
            denoms = tier_denoms[tier]
            lines.append(
                f"| {contender['contender_id']} | {tier} | {denoms['total']} | "
                f"{counts.get('false_merge', 0)} | {counts.get('hard_missed_merge', 0)} | "
                f"{counts.get('soft_missed_merge', 0)} | {counts.get('safe_flag', 0)} | "
                f"{fmt_rate(bucket_exact_accuracy(counts, denoms))} | "
                f"{fmt_rate(bucket_flag_rate(counts, denoms))} |"
            )

    lines.append("")
    lines.append("## Stratum breakdown")
    lines.append("")
    lines.append("| Contender | Stratum | Cases | False merge | Hard missed | Soft missed | Safe flag | Exact acc | Flag rate |")
    lines.append("|---|---|---:|---:|---:|---:|---:|---:|---:|")
    for contender in summary["contenders"]:
        buckets = contender["breakdowns"]["stratum"]
        for stratum in (
            "blind_core_audit",
            "known_false_merge_patterns",
            "known_missed_merge_patterns",
            "tail_random_sample",
        ):
            counts = buckets.get(stratum, Counter())
            denoms = stratum_denoms[stratum]
            lines.append(
                f"| {contender['contender_id']} | {stratum} | {denoms['total']} | "
                f"{counts.get('false_merge', 0)} | {counts.get('hard_missed_merge', 0)} | "
                f"{counts.get('soft_missed_merge', 0)} | {counts.get('safe_flag', 0)} | "
                f"{fmt_rate(bucket_exact_accuracy(counts, denoms))} | "
                f"{fmt_rate(bucket_flag_rate(counts, denoms))} |"
            )

    lines.append("")
    lines.append("## Winner-selection table")
    lines.append("")
    lines.append("| Contender | Eligibility | Production gate | Fallback gate | Production gate failures | Fallback gate failures |")
    lines.append("|---|---|---|---|---|---|")
    by_id = {contender["contender_id"]: contender for contender in summary["contenders"]}
    for row in summary["winner_selection"]:
        contender = by_id[row["contender_id"]]
        lines.append(
            f"| {row['contender_id']} | {row['eligibility']} | {row['production_gate']} | "
            f"{row['fallback_gate']} | "
            f"{', '.join(gate_failures(contender, 'production')) or 'none'} | "
            f"{', '.join(gate_failures(contender, 'fallback')) or 'none'} |"
        )

    lines.append("")
    lines.append("## Error ledger")
    lines.append("")
    lines.append("| Contender | Case | Pair | Expected | Predicted | Packet refs used |")
    lines.append("|---|---|---:|---|---|---|")
    for row in collect_error_ledger(summary):
        refs = ", ".join(row["packet_refs_used"][:6]) if row["packet_refs_used"] else "-"
        lines.append(
            f"| {row['contender_id']} | {row['case_id']} | {row['pair_id']} | "
            f"{row['expected_verdict']} | {row['predicted_verdict']} | {refs} |"
        )
    return "\n".join(lines) + "\n"


def run_full_bakeoff(args) -> dict:
    benchmark_payload = load_benchmark_payload(args.benchmark)
    full_packets, visible_packets, packet_validation = rebuild_packets(args.benchmark, args.packet_dir)
    packet_lookup = packets_by_id(full_packets)

    _, request_paths, request_validation = prepare_request_wrappers(
        benchmark_payload=benchmark_payload,
        visible_packets=visible_packets,
        contenders=CONTENDER_ORDER,
        run_name=args.run_name,
        output_root=args.output_root,
        proof_sample=False,
    )

    section_11 = load_section_11()
    system_prompt = build_system_prompt(section_11)
    preflight = {
        "run_name": args.run_name,
        "checked_at": datetime.now().astimezone().isoformat(timespec="seconds"),
        "results": [],
    }

    for contender_id in ("haiku_single_v1", "gemini_single_v1", "gpt5mini_single_v1", "sonnet_single_v1"):
        spec = CONTENDER_META[contender_id]
        if spec["provider"] == "anthropic":
            result = probe_anthropic_model(spec["model_id"], system_prompt)
        else:
            result = probe_openrouter_model(spec["model_id"], system_prompt)
        result["contender_id"] = contender_id
        preflight["results"].append(result)
        if not result["ok"]:
            raise RuntimeError(
                f"Frozen contender unavailable: {contender_id} / {spec['model_id']} / {result['error']}"
            )

    raw_root = args.output_root / "raw" / args.run_name
    normalized_root = args.output_root / "normalized" / args.run_name
    scored_json = args.output_root / "scored" / f"{args.run_name}.json"
    scored_md = args.output_root / "scored" / f"{args.run_name}.md"
    manifest_path = args.output_root / "scored" / f"{args.run_name}_manifest.json"
    error_ledger_path = args.output_root / "scored" / f"{args.run_name}_error_ledger.jsonl"
    preflight_path = raw_root / "preflight.json"

    run_deterministic_request_file(
        request_paths["deterministic_control_v1"],
        raw_root / "deterministic_control_v1.jsonl",
    )
    run_anthropic_request_file(
        request_paths["haiku_single_v1"],
        raw_root / "haiku_single_v1.jsonl",
        "haiku_single_v1",
        system_prompt,
    )
    run_openrouter_request_file(
        request_paths["gemini_single_v1"],
        raw_root / "gemini_single_v1.jsonl",
        "gemini_single_v1",
        system_prompt,
    )
    run_openrouter_request_file(
        request_paths["gpt5mini_single_v1"],
        raw_root / "gpt5mini_single_v1.jsonl",
        "gpt5mini_single_v1",
        system_prompt,
    )
    run_anthropic_request_file(
        request_paths["sonnet_single_v1"],
        raw_root / "sonnet_single_v1.jsonl",
        "sonnet_single_v1",
        system_prompt,
    )
    build_consensus_raw_file(
        raw_root / "haiku_single_v1.jsonl",
        raw_root / "gemini_single_v1.jsonl",
        packet_lookup,
        raw_root / "haiku_gemini_consensus_v1.jsonl",
    )
    write_json(preflight_path, preflight)

    normalized_paths: list[Path] = []
    for contender_id in CONTENDER_ORDER:
        raw_path = raw_root / f"{contender_id}.jsonl"
        normalized_path = normalized_root / f"{contender_id}.jsonl"
        normalize_file(raw_path, packet_lookup, normalized_path)
        normalized_paths.append(normalized_path)

    summary = score_run(
        benchmark_payload=benchmark_payload,
        normalized_paths=normalized_paths,
        full_packets=full_packets,
        output_json=scored_json,
        output_md=scored_md,
        run_name=args.run_name,
    )

    detailed_markdown = render_full_scorecard(summary, benchmark_payload)
    scored_md.write_text(detailed_markdown, encoding="utf-8")
    write_jsonl(error_ledger_path, collect_error_ledger(summary))

    manifest = {
        "run_name": args.run_name,
        "benchmark_id": benchmark_payload["benchmark_id"],
        "packet_validation": packet_validation,
        "request_validation": request_validation,
        "preflight": preflight,
        "requests": {key: str(path) for key, path in request_paths.items()},
        "raw": {contender_id: str(raw_root / f"{contender_id}.jsonl") for contender_id in CONTENDER_ORDER},
        "normalized": {contender_id: str(normalized_root / f"{contender_id}.jsonl") for contender_id in CONTENDER_ORDER},
        "scored_json": str(scored_json),
        "scored_md": str(scored_md),
        "error_ledger_jsonl": str(error_ledger_path),
    }
    write_json(manifest_path, manifest)
    return {
        "summary": summary,
        "manifest_path": manifest_path,
        "score_json": scored_json,
        "score_md": scored_md,
        "error_ledger": error_ledger_path,
        "preflight": preflight_path,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Run the first real adjudication bakeoff")
    parser.add_argument("--benchmark", type=Path, default=DEFAULT_BENCHMARK)
    parser.add_argument("--packet-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_RUN_ROOT)
    parser.add_argument("--run-name", default=RUN_NAME_DEFAULT)
    args = parser.parse_args()

    if args.run_name != RUN_NAME_DEFAULT:
        print(f"Using custom run name: {args.run_name}")
    result = run_full_bakeoff(args)
    print(f"Completed bakeoff run: {args.run_name}")
    print(f"Manifest: {result['manifest_path']}")
    print(f"Scored JSON: {result['score_json']}")
    print(f"Scored Markdown: {result['score_md']}")
    print(f"Error ledger: {result['error_ledger']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
