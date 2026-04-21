"""Session 8 - real producer-dedup adjudication bakeoff v2.

Implements the locked Session 7 redesign without changing `benchmark_v1` or the
frozen Session 4 score math / hard gates.

Artifacts land under:
    data/sprints/dedup/bakeoff_v2/

Default canonical run name:
    session7_first_real_bakeoff_v2
"""

from __future__ import annotations

import argparse
import json
import re
import time
from datetime import datetime
from pathlib import Path

import anthropic
import requests

from pipeline.identity.bakeoff_harness_v1 import score_run
from pipeline.identity.bakeoff_harness_v2 import (
    DEFAULT_BENCHMARK,
    DEFAULT_OUTPUT_DIR,
    DEFAULT_RUN_ROOT,
    VALID_CONTENDERS,
    benchmark_cases_by_id,
    build_consensus_normalized_file,
    ensure_visible_packets,
    load_jsonl,
    load_benchmark_payload,
    normalize_file,
    packet_ref_ids,
    packets_by_id,
    prepare_request_wrappers,
    validate_proof_summary,
)
from pipeline.identity.bakeoff_packet_v2 import canonical_json_dumps, write_jsonl
from pipeline.lib.db import get_env


REPO_ROOT = Path(__file__).resolve().parents[2]
IDENTITY_RULES_PATH = REPO_ROOT / "docs" / "IDENTITY_RULES.md"
RUN_NAME_DEFAULT = "session7_first_real_bakeoff_v2"
OPENROUTER_URL = "https://openrouter.ai/api/v1/chat/completions"
V1_SCORE_JSON = REPO_ROOT / "data" / "sprints" / "dedup" / "bakeoff_v1" / "scored" / "session6_first_real_bakeoff_v1.json"
V1_NORMALIZED_ROOT = REPO_ROOT / "data" / "sprints" / "dedup" / "bakeoff_v1" / "normalized" / "session6_first_real_bakeoff_v1"

CONTENDER_META = {
    "deterministic_control_v1": {
        "method_class": "deterministic control",
        "models": "none",
        "baseline_contender_id": "deterministic_control_v1",
        "production_eligible": False,
    },
    "sonnet_guardrailed_v2": {
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
        "baseline_contender_id": "sonnet_single_v1",
        "production_eligible": True,
    },
    "gemini_guardrailed_v2": {
        "method_class": "single-model adjudicator",
        "models": "Gemini 3 Flash Preview (`google/gemini-3-flash-preview`)",
        "provider": "openrouter",
        "model_id": "google/gemini-3-flash-preview",
        "pricing": {
            "input": 0.15,
            "output": 0.60,
        },
        "baseline_contender_id": "gemini_single_v1",
        "production_eligible": True,
    },
    "gpt5mini_guardrailed_v2": {
        "method_class": "single-model adjudicator",
        "models": "GPT-5.4-mini (`openai/gpt-5.4-mini`)",
        "provider": "openrouter",
        "model_id": "openai/gpt-5.4-mini",
        "pricing": {
            "input": 0.40,
            "output": 1.60,
        },
        "baseline_contender_id": "gpt5mini_single_v1",
        "production_eligible": True,
    },
    "sonnet_gemini_consensus_v2": {
        "method_class": "consensus adjudicator",
        "models": "Claude Sonnet 4.5 + Gemini 3 Flash Preview",
        "baseline_contender_id": "haiku_gemini_consensus_v1",
        "production_eligible": True,
    },
}


def write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(canonical_json_dumps(payload), encoding="utf-8")


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
    return f"""You are `bakeoff_adjudicator_v2`, a merge-only producer-dedup adjudicator.

You will receive exactly one JSON request wrapper containing:
- `packet_visible`
- `allowed_ref_ids`

Use ONLY the evidence inside that wrapper. Do not use outside knowledge, live
web search, or hidden benchmark speculation.

Apply the product's producer identity rules strictly:

{section_11}

Additional v2 rules:
- Treat `packet_visible.evidence_refs[]` as the citeable ledger.
- `key_support_refs` and `key_contradiction_refs` must use ONLY ids from
  `allowed_ref_ids`.
- Do not cite free-text facts like `same_country` or rule ids like `11.4.m`
  as support refs. Rule ids belong only in `rule_ids`.
- If `allowed_ref_ids` includes `risk_shared_surname_split`,
  `risk_holdco_or_product_tier`, or `geo_country_conflict`, do NOT return
  `MERGE` unless an `official_continuity_*` ref is also present.
- Search is retrieval, not truth by itself: prefer official continuity plus
  local catalog coherence plus deterministic identity rules.

Bad citation example:
{{
  "key_support_refs": ["same_country", "11.4.m"]
}}

Good citation example:
{{
  "key_support_refs": ["geo_same_country", "risk_shared_surname_split"],
  "key_contradiction_refs": ["risk_sparse_official_evidence"]
}}

Output contract requirements:
- Return exactly one JSON object and nothing else.
- Allowed `verdict` values: `MERGE`, `SKIP`, `FLAGGED`.
- `PARENT_CHILD` is invalid in v2.
- `confidence` must be a number from 0 to 1.
- `rule_ids` must cite real Section 11 rule ids such as `11.1`, `11.4.h`,
  `11.4.m`, `11.6`.
- Every output must include at least one `key_support_refs` item.
- If `verdict` is `MERGE` and `survivor_if_merge.recommended_survivor_producer_id`
  is non-null, set `survivor_producer_id` to one of the listed candidate ids.
- If `verdict` is `SKIP` or `FLAGGED`, set `survivor_producer_id` to null.
- Use `FLAGGED` when evidence is sparse, contradictory, or not strong enough
  for a safe merge/skip call.
- `reason` should be 1 short paragraph, concrete and citation-driven.
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
        "X-Title": "Loam Session 8 Bakeoff",
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


def deterministic_control(wrapper: dict) -> dict:
    packet = wrapper["packet_visible"]
    refs = {entry["ref_id"] for entry in packet.get("evidence_refs", [])}
    survivor = packet["evidence"]["survivor_if_merge"]["recommended_survivor_producer_id"]

    if "geo_country_conflict" in refs and not any(ref.startswith("official_continuity_") for ref in refs):
        return {
            "packet_id": packet["packet_id"],
            "verdict": "SKIP",
            "confidence": 0.93,
            "rule_ids": ["11.3"],
            "reason": "Deterministic control skips country-conflict pairs unless official continuity is explicit in the packet.",
            "key_support_refs": ["geo_country_conflict"],
            "key_contradiction_refs": ["risk_sparse_official_evidence"] if "risk_sparse_official_evidence" in refs else [],
            "survivor_producer_id": None,
            "follow_up": None,
        }
    if "risk_shared_surname_split" in refs and not any(ref.startswith("official_continuity_") for ref in refs):
        return {
            "packet_id": packet["packet_id"],
            "verdict": "SKIP",
            "confidence": 0.92,
            "rule_ids": ["11.4.m"],
            "reason": "Deterministic control treats shared-surname split as a skip unless official continuity is explicit.",
            "key_support_refs": ["risk_shared_surname_split"],
            "key_contradiction_refs": ["risk_sparse_official_evidence"] if "risk_sparse_official_evidence" in refs else [],
            "survivor_producer_id": None,
            "follow_up": None,
        }
    if "risk_holdco_or_product_tier" in refs and not any(ref.startswith("official_continuity_") for ref in refs):
        return {
            "packet_id": packet["packet_id"],
            "verdict": "FLAGGED",
            "confidence": 0.72,
            "rule_ids": ["11.1"],
            "reason": "Deterministic control escalates holdco/product-tier patterns unless official continuity is explicit.",
            "key_support_refs": ["risk_holdco_or_product_tier"],
            "key_contradiction_refs": ["risk_sparse_official_evidence"] if "risk_sparse_official_evidence" in refs else [],
            "survivor_producer_id": None,
            "follow_up": "needs_human_review",
        }
    if "lex_near_exact" in refs and "geo_same_country" in refs and ("catalog_exact_overlap" in refs or "catalog_subset_match" in refs):
        return {
            "packet_id": packet["packet_id"],
            "verdict": "MERGE",
            "confidence": 0.95,
            "rule_ids": ["11.1", "11.4.h", "11.6"],
            "reason": "Deterministic control merges near-exact same-country pairs when the packet also shows catalog coherence.",
            "key_support_refs": [ref for ref in ("lex_near_exact", "geo_same_country", "catalog_exact_overlap", "catalog_subset_match") if ref in refs][:3],
            "key_contradiction_refs": [],
            "survivor_producer_id": survivor,
            "follow_up": None,
        }
    if "lex_contains" in refs and "geo_same_country" in refs and ("catalog_subset_match" in refs or any(ref.startswith("official_continuity_") for ref in refs)):
        return {
            "packet_id": packet["packet_id"],
            "verdict": "MERGE",
            "confidence": 0.9,
            "rule_ids": ["11.1", "11.4.h", "11.6"],
            "reason": "Deterministic control merges short/full-form same-country pairs when the packet also shows catalog or official continuity support.",
            "key_support_refs": [ref for ref in ("lex_contains", "geo_same_country", "catalog_subset_match", "official_continuity_shared_domain") if ref in refs][:3],
            "key_contradiction_refs": [],
            "survivor_producer_id": survivor,
            "follow_up": None,
        }
    return {
        "packet_id": packet["packet_id"],
        "verdict": "FLAGGED",
        "confidence": 0.55,
        "rule_ids": ["11.1"],
        "reason": "Deterministic control could not reach a safe merge or skip from the v2 packet alone.",
        "key_support_refs": packet["envelope"]["allowed_ref_ids"][:1],
        "key_contradiction_refs": [ref for ref in ("risk_sparse_official_evidence", "catalog_asymmetry") if ref in refs][:2],
        "survivor_producer_id": None,
        "follow_up": "needs_human_review",
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
        "X-Title": "Loam Session 8 Bakeoff",
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


def build_consensus_raw_file(
    sonnet_normalized_path: Path,
    gemini_normalized_path: Path,
    output_path: Path,
) -> Path:
    sonnet_rows = {row["case_id"]: row for row in load_jsonl(sonnet_normalized_path)}
    gemini_rows = {row["case_id"]: row for row in load_jsonl(gemini_normalized_path)}
    raw_rows: list[dict] = []
    for case_id in sorted(sonnet_rows):
        sonnet_row = sonnet_rows[case_id]
        gemini_row = gemini_rows[case_id]
        if not sonnet_row["schema_valid"] or not gemini_row["schema_valid"]:
            raw_output = {
                "packet_id": sonnet_row["packet_id"],
                "verdict": "FLAGGED",
                "confidence": 0.0,
                "rule_ids": ["11.1"],
                "reason": "Consensus escalated because at least one normalized child row was schema-invalid.",
                "key_support_refs": sonnet_row["normalized_output"]["key_support_refs"][:1] or gemini_row["normalized_output"]["key_support_refs"][:1],
                "key_contradiction_refs": [],
                "survivor_producer_id": None,
                "follow_up": "consensus_child_schema_invalid",
            }
        elif (
            sonnet_row["normalized_output"]["verdict"] == "MERGE"
            and gemini_row["normalized_output"]["verdict"] == "MERGE"
            and sonnet_row["normalized_output"]["survivor_producer_id"] == gemini_row["normalized_output"]["survivor_producer_id"]
        ):
            raw_output = {
                "packet_id": sonnet_row["packet_id"],
                "verdict": "MERGE",
                "confidence": min(
                    sonnet_row["normalized_output"]["confidence"],
                    gemini_row["normalized_output"]["confidence"],
                ),
                "rule_ids": list(dict.fromkeys(sonnet_row["normalized_output"]["rule_ids"] + gemini_row["normalized_output"]["rule_ids"]))[:3],
                "reason": "Normalized-child consensus: Sonnet and Gemini both returned contract-valid MERGE with the same survivor.",
                "key_support_refs": list(dict.fromkeys(sonnet_row["normalized_output"]["key_support_refs"] + gemini_row["normalized_output"]["key_support_refs"]))[:3],
                "key_contradiction_refs": list(dict.fromkeys(sonnet_row["normalized_output"]["key_contradiction_refs"] + gemini_row["normalized_output"]["key_contradiction_refs"]))[:2],
                "survivor_producer_id": sonnet_row["normalized_output"]["survivor_producer_id"],
                "follow_up": None,
            }
        elif (
            sonnet_row["normalized_output"]["verdict"] == "SKIP"
            and gemini_row["normalized_output"]["verdict"] == "SKIP"
        ):
            raw_output = {
                "packet_id": sonnet_row["packet_id"],
                "verdict": "SKIP",
                "confidence": min(
                    sonnet_row["normalized_output"]["confidence"],
                    gemini_row["normalized_output"]["confidence"],
                ),
                "rule_ids": list(dict.fromkeys(sonnet_row["normalized_output"]["rule_ids"] + gemini_row["normalized_output"]["rule_ids"]))[:3],
                "reason": "Normalized-child consensus: Sonnet and Gemini both returned contract-valid SKIP.",
                "key_support_refs": list(dict.fromkeys(sonnet_row["normalized_output"]["key_support_refs"] + gemini_row["normalized_output"]["key_support_refs"]))[:3],
                "key_contradiction_refs": list(dict.fromkeys(sonnet_row["normalized_output"]["key_contradiction_refs"] + gemini_row["normalized_output"]["key_contradiction_refs"]))[:2],
                "survivor_producer_id": None,
                "follow_up": None,
            }
        else:
            raw_output = {
                "packet_id": sonnet_row["packet_id"],
                "verdict": "FLAGGED",
                "confidence": 0.0,
                "rule_ids": ["11.1"],
                "reason": "Normalized-child consensus escalated because the two child rows did not support the same executable verdict.",
                "key_support_refs": list(dict.fromkeys(sonnet_row["normalized_output"]["key_support_refs"] + gemini_row["normalized_output"]["key_support_refs"]))[:3],
                "key_contradiction_refs": list(dict.fromkeys(sonnet_row["normalized_output"]["key_contradiction_refs"] + gemini_row["normalized_output"]["key_contradiction_refs"]))[:2],
                "survivor_producer_id": None,
                "follow_up": "consensus_disagreement",
            }
        raw_rows.append(
            {
                "benchmark_id": sonnet_row["benchmark_id"],
                "case_id": case_id,
                "packet_id": sonnet_row["packet_id"],
                "contender_id": "sonnet_gemini_consensus_v2",
                "raw_output": raw_output,
                "timing_ms": int(sonnet_row.get("timing_ms", 0) or 0) + int(gemini_row.get("timing_ms", 0) or 0),
                "usage": {
                    "prompt_tokens": int(sonnet_row["usage"].get("prompt_tokens", 0) or 0) + int(gemini_row["usage"].get("prompt_tokens", 0) or 0),
                    "completion_tokens": int(sonnet_row["usage"].get("completion_tokens", 0) or 0) + int(gemini_row["usage"].get("completion_tokens", 0) or 0),
                    "search_calls": int(sonnet_row["usage"].get("search_calls", 0) or 0) + int(gemini_row["usage"].get("search_calls", 0) or 0),
                    "cost_usd": round(float(sonnet_row["usage"].get("cost_usd", 0.0) or 0.0) + float(gemini_row["usage"].get("cost_usd", 0.0) or 0.0), 6),
                },
            }
        )
    write_jsonl(output_path, raw_rows)
    return output_path


def render_full_scorecard(summary: dict) -> str:
    lines = []
    lines.append(f"# {summary['run_name']} - adjudication bakeoff v2")
    lines.append("")
    lines.append(f"- Generated: {datetime.now().astimezone().isoformat(timespec='seconds')}")
    lines.append(f"- Benchmark: `{summary['benchmark_id']}`")
    lines.append(f"- Cases scored: {summary['case_count_seen']} / {summary['case_count_expected']}")
    lines.append(f"- Full benchmark run: {'yes' if summary['full_benchmark_run'] else 'no'}")
    lines.append("")
    lines.append("| Contender | Exact acc | False merge | Hard missed | Soft missed | Safe flag | Auditability | Flag rate | Production gate | Fallback gate |")
    lines.append("|---|---:|---:|---:|---:|---:|---:|---:|---|---|")
    for contender in summary["contenders"]:
        rates = contender["rates"]
        counts = contender["counts"]
        lines.append(
            f"| {contender['contender_id']} | "
            f"{(rates['exact_verdict_accuracy'] or 0):.4f} | "
            f"{counts['false_merge']} | {counts['hard_missed_merge']} | "
            f"{counts['soft_missed_merge']} | {counts['safe_flag']} | "
            f"{(contender['auditability']['auditability_score'] or 0):.4f} | "
            f"{(rates['flag_rate_total'] or 0):.4f} | "
            f"{contender['gates']['production']['status']} | "
            f"{contender['gates']['fallback']['status']} |"
        )
    return "\n".join(lines) + "\n"


def build_diff_payload(summary: dict, run_name: str, normalized_root: Path) -> dict:
    v1_summary = json.loads(V1_SCORE_JSON.read_text(encoding="utf-8"))
    v1_by_id = {row["contender_id"]: row for row in v1_summary["contenders"]}
    diff_rows: list[dict] = []
    for contender in summary["contenders"]:
        baseline_id = CONTENDER_META[contender["contender_id"]]["baseline_contender_id"]
        baseline = v1_by_id[baseline_id]
        diff_rows.append(
            {
                "contender_id": contender["contender_id"],
                "baseline_contender_id": baseline_id,
                "delta_exact_accuracy": round((contender["rates"]["exact_verdict_accuracy"] or 0.0) - (baseline["rates"]["exact_verdict_accuracy"] or 0.0), 4),
                "delta_false_merge": contender["counts"]["false_merge"] - baseline["counts"]["false_merge"],
                "delta_hard_missed_merge": contender["counts"]["hard_missed_merge"] - baseline["counts"]["hard_missed_merge"],
                "delta_soft_missed_merge": contender["counts"]["soft_missed_merge"] - baseline["counts"]["soft_missed_merge"],
                "delta_safe_flag": contender["counts"]["safe_flag"] - baseline["counts"]["safe_flag"],
                "delta_schema_valid_rate": round((contender["auditability"]["schema_valid_rate"] or 0.0) - (baseline["auditability"]["schema_valid_rate"] or 0.0), 4),
                "delta_auditability_score": round((contender["auditability"]["auditability_score"] or 0.0) - (baseline["auditability"]["auditability_score"] or 0.0), 4),
                "delta_flag_rate_total": round((contender["rates"]["flag_rate_total"] or 0.0) - (baseline["rates"]["flag_rate_total"] or 0.0), 4),
                "production_gate_before": baseline["gates"]["production"]["status"],
                "production_gate_after": contender["gates"]["production"]["status"],
                "fallback_gate_before": baseline["gates"]["fallback"]["status"],
                "fallback_gate_after": contender["gates"]["fallback"]["status"],
            }
        )
    return {
        "run_name": run_name,
        "baseline_run_name": v1_summary["run_name"],
        "rows": diff_rows,
        "normalized_root": str(normalized_root),
    }


def render_diff_markdown(payload: dict) -> str:
    lines = []
    lines.append(f"# {payload['run_name']} - v1 vs v2 diff")
    lines.append("")
    lines.append(f"- Baseline: `{payload['baseline_run_name']}`")
    lines.append("")
    lines.append("| Contender | Baseline | dExact acc | dFalse merge | dHard missed | dSoft missed | dSafe flag | dSchema valid | dAuditability | dFlag rate | Prod gate | Fallback gate |")
    lines.append("|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---|---|")
    for row in payload["rows"]:
        lines.append(
            f"| {row['contender_id']} | {row['baseline_contender_id']} | "
            f"{row['delta_exact_accuracy']:+.4f} | "
            f"{row['delta_false_merge']:+d} | "
            f"{row['delta_hard_missed_merge']:+d} | "
            f"{row['delta_soft_missed_merge']:+d} | "
            f"{row['delta_safe_flag']:+d} | "
            f"{row['delta_schema_valid_rate']:+.4f} | "
            f"{row['delta_auditability_score']:+.4f} | "
            f"{row['delta_flag_rate_total']:+.4f} | "
            f"{row['production_gate_before']} -> {row['production_gate_after']} | "
            f"{row['fallback_gate_before']} -> {row['fallback_gate_after']} |"
        )
    return "\n".join(lines) + "\n"


def preflight_models(system_prompt: str, contenders: list[str]) -> dict:
    payload = {
        "checked_at": datetime.now().astimezone().isoformat(timespec="seconds"),
        "results": [],
    }
    for contender_id in contenders:
        spec = CONTENDER_META[contender_id]
        if spec["provider"] == "anthropic":
            result = probe_anthropic_model(spec["model_id"], system_prompt)
        else:
            result = probe_openrouter_model(spec["model_id"], system_prompt)
        result["contender_id"] = contender_id
        payload["results"].append(result)
        if not result["ok"]:
            raise RuntimeError(f"Contender unavailable: {contender_id} / {spec['model_id']} / {result['error']}")
    return payload


def run_lineup(request_paths: dict[str, Path], raw_root: Path, system_prompt: str) -> dict[str, Path]:
    raw_paths: dict[str, Path] = {}
    raw_paths["deterministic_control_v1"] = run_deterministic_request_file(
        request_paths["deterministic_control_v1"],
        raw_root / "deterministic_control_v1.jsonl",
    )
    raw_paths["sonnet_guardrailed_v2"] = run_anthropic_request_file(
        request_paths["sonnet_guardrailed_v2"],
        raw_root / "sonnet_guardrailed_v2.jsonl",
        "sonnet_guardrailed_v2",
        system_prompt,
    )
    raw_paths["gemini_guardrailed_v2"] = run_openrouter_request_file(
        request_paths["gemini_guardrailed_v2"],
        raw_root / "gemini_guardrailed_v2.jsonl",
        "gemini_guardrailed_v2",
        system_prompt,
    )
    return raw_paths


def normalize_and_score(
    benchmark_payload: dict,
    full_packets: list[dict],
    raw_paths: dict[str, Path],
    normalized_root: Path,
    run_name: str,
    output_root: Path,
) -> tuple[dict, dict[str, Path], Path]:
    packet_lookup = packets_by_id(full_packets)
    normalized_paths: dict[str, Path] = {}
    for contender_id, raw_path in raw_paths.items():
        normalized_paths[contender_id] = normalize_file(raw_path, packet_lookup, normalized_root / f"{contender_id}.jsonl")

    consensus_raw_path = build_consensus_raw_file(
        normalized_paths["sonnet_guardrailed_v2"],
        normalized_paths["gemini_guardrailed_v2"],
        output_root / "raw" / run_name / "sonnet_gemini_consensus_v2.jsonl",
    )
    normalized_paths["sonnet_gemini_consensus_v2"] = build_consensus_normalized_file(
        normalized_paths["sonnet_guardrailed_v2"],
        normalized_paths["gemini_guardrailed_v2"],
        packet_lookup,
        normalized_root / "sonnet_gemini_consensus_v2.jsonl",
        "gemini_guardrailed_v2",
    )

    summary = score_run(
        benchmark_payload=benchmark_payload,
        normalized_paths=[normalized_paths[key] for key in ("deterministic_control_v1", "sonnet_guardrailed_v2", "gemini_guardrailed_v2", "sonnet_gemini_consensus_v2")],
        full_packets=full_packets,
        output_json=output_root / "scored" / f"{run_name}.json",
        output_md=output_root / "scored" / f"{run_name}.md",
        run_name=run_name,
    )
    return summary, normalized_paths, consensus_raw_path


def run_once(args, *, proof_sample: bool, run_name: str) -> dict:
    benchmark_payload = load_benchmark_payload(args.benchmark)
    full_packets, visible_packets, packet_validation = ensure_visible_packets(args.packet_dir, args.benchmark)
    _, request_paths, request_validation = prepare_request_wrappers(
        benchmark_payload=benchmark_payload,
        visible_packets=visible_packets,
        contenders=["deterministic_control_v1", "sonnet_guardrailed_v2", "gemini_guardrailed_v2"],
        run_name=run_name,
        output_root=args.output_root,
        proof_sample=proof_sample,
    )

    section_11 = load_section_11()
    system_prompt = build_system_prompt(section_11)
    preflight = preflight_models(system_prompt, ["sonnet_guardrailed_v2", "gemini_guardrailed_v2"])
    write_json(args.output_root / "raw" / run_name / "preflight.json", preflight)

    raw_paths = run_lineup(request_paths, args.output_root / "raw" / run_name, system_prompt)
    summary, normalized_paths, consensus_raw_path = normalize_and_score(
        benchmark_payload=benchmark_payload,
        full_packets=full_packets,
        raw_paths=raw_paths,
        normalized_root=args.output_root / "normalized" / run_name,
        run_name=run_name,
        output_root=args.output_root,
    )
    (args.output_root / "scored" / f"{run_name}.md").write_text(render_full_scorecard(summary), encoding="utf-8")
    write_json(
        args.output_root / "scored" / f"{run_name}_manifest.json",
        {
            "run_name": run_name,
            "proof_sample": proof_sample,
            "packet_validation": packet_validation,
            "request_validation": request_validation,
            "preflight": preflight,
            "requests": {key: str(path) for key, path in request_paths.items()},
            "raw": {
                **{key: str(path) for key, path in raw_paths.items()},
                "sonnet_gemini_consensus_v2": str(consensus_raw_path),
            },
            "normalized": {key: str(path) for key, path in normalized_paths.items()},
        },
    )
    return {
        "summary": summary,
        "packet_validation": packet_validation,
        "request_validation": request_validation,
        "normalized_paths": normalized_paths,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Run the Session 8 adjudication bakeoff v2")
    parser.add_argument("--benchmark", type=Path, default=DEFAULT_BENCHMARK)
    parser.add_argument("--packet-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_RUN_ROOT)
    parser.add_argument("--run-name", default=RUN_NAME_DEFAULT)
    args = parser.parse_args()

    proof_name = f"{args.run_name}_proof_subset"
    proof_result = run_once(args, proof_sample=True, run_name=proof_name)
    consensus_rows = load_jsonl(proof_result["normalized_paths"]["sonnet_gemini_consensus_v2"])
    proof_failures = validate_proof_summary(
        proof_result["summary"],
        proof_result["request_validation"],
        consensus_rows,
    )
    write_json(
        args.output_root / "scored" / f"{proof_name}_gate_check.json",
        {
            "run_name": proof_name,
            "failures": proof_failures,
            "passed": not proof_failures,
        },
    )
    if proof_failures:
        raise RuntimeError("Proof subset failed stop criteria: " + "; ".join(proof_failures))

    full_result = run_once(args, proof_sample=False, run_name=args.run_name)
    diff_payload = build_diff_payload(
        full_result["summary"],
        args.run_name,
        args.output_root / "normalized" / args.run_name,
    )
    write_json(args.output_root / "scored" / f"{args.run_name}_v1_vs_v2_diff.json", diff_payload)
    (args.output_root / "scored" / f"{args.run_name}_v1_vs_v2_diff.md").write_text(
        render_diff_markdown(diff_payload),
        encoding="utf-8",
    )

    print(f"Completed proof subset: {proof_name}")
    print(f"Completed full bakeoff run: {args.run_name}")
    print(f"Score JSON: {args.output_root / 'scored' / f'{args.run_name}.json'}")
    print(f"Diff Markdown: {args.output_root / 'scored' / f'{args.run_name}_v1_vs_v2_diff.md'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
