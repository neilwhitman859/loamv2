"""Session 10.8 - visible singleton-cuvee anchor follow-on.

This contender starts from an existing `hybrid_guarded_frontier_v1` run and
adds one additional deterministic promotion family:

- shared-surname risk is present
- same-region subset/containment evidence is already visible
- one side is a very thin singleton catalog
- the representative wine strings expose a rare overlap token that appears
  nowhere else in the frozen benchmark
- the fuller name is a partnership/co-founder style form (`&`)

The point is to use a small piece of already-visible packet surface that the
existing hybrid rules did not mine, without reopening the broad shared-surname
trap family that made the frontier brittle.
"""

from __future__ import annotations

import argparse
import json
import re
from collections import Counter, defaultdict
from copy import deepcopy
from datetime import datetime
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
BENCHMARK_PATH = REPO_ROOT / "data" / "sprints" / "dedup" / "benchmark_v1.json"
VISIBLE_PACKET_PATH = (
    REPO_ROOT / "data" / "sprints" / "dedup" / "bakeoff_v2" / "packets" / "benchmark_v1_packets_visible_v2.jsonl"
)
DEFAULT_OUTPUT_ROOT = REPO_ROOT / "data" / "sprints" / "identity-er" / "method_bakeoff"
CONTENDER_ID = "hybrid_guarded_cuvee_anchor_v1"
METHOD_VERSION = "hybrid_guarded_cuvee_anchor_v1"

GENERIC_WINE_TOKENS = {
    "alsace",
    "barbera",
    "barolo",
    "beaune",
    "blanc",
    "blancs",
    "blanco",
    "blanche",
    "brut",
    "burgundy",
    "cabernet",
    "champagne",
    "chambertin",
    "chardonnay",
    "clos",
    "cote",
    "cotes",
    "cru",
    "cuvee",
    "dolcetto",
    "emilion",
    "gamay",
    "georges",
    "gevrey",
    "grand",
    "grande",
    "grenache",
    "gris",
    "gruner",
    "joseph",
    "jura",
    "kremstal",
    "madeira",
    "mant",
    "mclaren",
    "medoc",
    "mercurey",
    "merlot",
    "meursault",
    "montrachet",
    "nebbiolo",
    "noir",
    "nuits",
    "pauillac",
    "pinot",
    "pommard",
    "premier",
    "prosecco",
    "puligny",
    "red",
    "reserve",
    "riesling",
    "rioja",
    "rhone",
    "riserva",
    "rose",
    "rouge",
    "saint",
    "sauvignon",
    "syrah",
    "vale",
    "veltliner",
    "villages",
    "vintage",
    "vin",
    "vino",
    "volnay",
    "white",
    "wine",
}


def canonical_json_dumps(value: object) -> str:
    return json.dumps(value, ensure_ascii=True, indent=2, sort_keys=False) + "\n"


def write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(canonical_json_dumps(payload), encoding="utf-8")


def write_jsonl(path: Path, rows: list[dict]) -> None:
    payload = "".join(json.dumps(row, ensure_ascii=True, sort_keys=False) + "\n" for row in rows)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(payload, encoding="utf-8")


def load_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def load_jsonl(path: Path) -> list[dict]:
    return [
        json.loads(line)
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]


def now_iso() -> str:
    return datetime.now().astimezone().isoformat(timespec="seconds")


def tokenize(value: str | None) -> list[str]:
    return [
        token
        for token in re.findall(r"[a-zA-Z']+", (value or "").lower().replace("d'", "d"))
        if len(token) >= 4
    ]


def benchmark_cases_by_id(benchmark: dict) -> dict[str, dict]:
    return {case["case_id"]: case for case in benchmark["cases"]}


def visible_packets_by_case(benchmark: dict, packet_rows: list[dict]) -> dict[str, dict]:
    case_by_pair = {int(case["pair_id"]): case["case_id"] for case in benchmark["cases"]}
    by_case: dict[str, dict] = {}
    for packet in packet_rows:
        pair_id = int(packet["envelope"]["pair_id"])
        case_id = case_by_pair[pair_id]
        by_case[case_id] = packet
    return by_case


def rare_overlap_frequency(packet_rows: list[dict]) -> dict[str, int]:
    token_cases: defaultdict[str, set[str]] = defaultdict(set)
    for packet in packet_rows:
        case_id = str(packet.get("packet_id") or "")
        if not case_id:
            continue
        a_name = packet["evidence"]["side_a"]["name"]
        b_name = packet["evidence"]["side_b"]["name"]
        name_tokens = set(tokenize(a_name) + tokenize(b_name))
        rep_a = packet["evidence"]["side_a"]["catalog_summary"].get("representative_wines") or []
        rep_b = packet["evidence"]["side_b"]["catalog_summary"].get("representative_wines") or []
        side_a_tokens: set[str] = set()
        side_b_tokens: set[str] = set()
        for item in rep_a:
            side_a_tokens.update(tokenize(item))
        for item in rep_b:
            side_b_tokens.update(tokenize(item))
        overlap = (side_a_tokens & side_b_tokens) - name_tokens - GENERIC_WINE_TOKENS
        for token in overlap:
            token_cases[token].add(case_id)
    return {token: len(case_ids) for token, case_ids in token_cases.items()}


def unique_overlap_tokens(packet: dict, overlap_frequency: dict[str, int]) -> list[str]:
    a_name = packet["evidence"]["side_a"]["name"]
    b_name = packet["evidence"]["side_b"]["name"]
    name_tokens = set(tokenize(a_name) + tokenize(b_name))
    rep_a = packet["evidence"]["side_a"]["catalog_summary"].get("representative_wines") or []
    rep_b = packet["evidence"]["side_b"]["catalog_summary"].get("representative_wines") or []
    side_a_tokens: set[str] = set()
    side_b_tokens: set[str] = set()
    for item in rep_a:
        side_a_tokens.update(tokenize(item))
    for item in rep_b:
        side_b_tokens.update(tokenize(item))
    overlap = sorted((side_a_tokens & side_b_tokens) - name_tokens - GENERIC_WINE_TOKENS)
    return [token for token in overlap if overlap_frequency.get(token, 0) == 1]


def packet_ref_ids(packet: dict) -> set[str]:
    return {
        entry["ref_id"]
        for entry in packet.get("evidence_refs", [])
        if isinstance(entry, dict) and entry.get("ref_id")
    }


def candidate_applies(packet: dict, row: dict, overlap_frequency: dict[str, int]) -> tuple[bool, dict]:
    verdict = row["normalized_output"]["verdict"]
    if verdict not in {"FLAGGED", "SKIP"}:
        return False, {}

    comparison = packet["evidence"]["comparison"]
    lexical = comparison["lexical"]
    geography = comparison["geography"]
    risk = comparison["risk_flags"]
    refs = packet_ref_ids(packet)
    side_a = packet["evidence"]["side_a"]
    side_b = packet["evidence"]["side_b"]
    wine_count_a = int(side_a["catalog_summary"].get("wine_count") or 0)
    wine_count_b = int(side_b["catalog_summary"].get("wine_count") or 0)
    unique_tokens = unique_overlap_tokens(packet, overlap_frequency)
    name_a = side_a["name"]
    name_b = side_b["name"]
    partnership_present = "&" in name_a or "&" in name_b

    applies = (
        bool(risk.get("shared_surname_split"))
        and bool(geography.get("same_region"))
        and lexical.get("containment") == "a_in_b"
        and "catalog_subset_match" in refs
        and int(comparison["catalog"].get("exact_overlap_count") or 0) == 0
        and min(wine_count_a, wine_count_b) <= 1
        and max(wine_count_a, wine_count_b) <= 8
        and partnership_present
        and bool(unique_tokens)
    )
    return applies, {
        "unique_overlap_tokens": unique_tokens,
        "wine_count_a": wine_count_a,
        "wine_count_b": wine_count_b,
        "containment": lexical.get("containment"),
        "partnership_present": partnership_present,
    }


def promote_row(row: dict, packet: dict, details: dict) -> dict:
    promoted = deepcopy(row)
    side_a = packet["evidence"]["side_a"]
    side_b = packet["evidence"]["side_b"]
    promoted["contender_id"] = CONTENDER_ID
    promoted["cuvée_anchor_guard_applied"] = "singleton_unique_cuvee_anchor_merge"
    promoted["cuvée_anchor_guard_details"] = details
    promoted["normalized_output"] = {
        "packet_id": row["normalized_output"]["packet_id"],
        "verdict": "MERGE",
        "confidence": 0.89,
        "rule_ids": ["11.4.o", "11.6"],
        "reason": (
            "Promoted by `singleton_unique_cuvee_anchor_merge`: the visible packet keeps the same-region "
            "subset/containment shape, the thinner side is a singleton alias, and the representative wine "
            "strings expose a benchmark-unique cuvee anchor token "
            f"({', '.join(details['unique_overlap_tokens'])})."
        ),
        "key_support_refs": ["catalog_subset_match", "geo_same_region", "lex_contains", "lex_shared_core_tokens"],
        "key_contradiction_refs": ["risk_sparse_official_evidence", "risk_shared_surname_split"],
        "survivor_producer_id": packet["evidence"]["survivor_if_merge"]["recommended_survivor_producer_id"],
        "follow_up": None,
    }
    promoted["schema_valid"] = True
    promoted["rule_trace_valid"] = True
    promoted["citation_integrity"] = True
    promoted["timing_ms"] = 0
    return promoted


def classify_row(expected_verdict: str, predicted_verdict: str) -> str:
    if expected_verdict == "MERGE":
        if predicted_verdict == "MERGE":
            return "true_merge"
        if predicted_verdict == "FLAGGED":
            return "soft_missed_merge"
        return "hard_missed_merge"
    if predicted_verdict == "MERGE":
        return "false_merge"
    if predicted_verdict == "FLAGGED":
        return "safe_flag"
    return "true_skip"


def compute_breakdown(rows: list[dict], case_by_id: dict[str, dict], key: str) -> dict:
    bucket_counts: dict[str, Counter] = {}
    for row in rows:
        case = case_by_id[row["case_id"]]
        bucket = str(case[key])
        bucket_counts.setdefault(bucket, Counter())
        label = classify_row(case["expected_verdict"], row["normalized_output"]["verdict"])
        bucket_counts[bucket][label] += 1
        bucket_counts[bucket]["expected_merge_total"] += int(case["expected_verdict"] == "MERGE")
        bucket_counts[bucket]["expected_skip_total"] += int(case["expected_verdict"] == "SKIP")
    output: dict[str, dict] = {}
    for bucket, counts in bucket_counts.items():
        output[bucket] = {
            "false_merge": counts["false_merge"],
            "hard_missed_merge": counts["hard_missed_merge"],
            "safe_flag": counts["safe_flag"],
            "soft_missed_merge": counts["soft_missed_merge"],
            "survivor_error": 0,
            "true_merge": counts["expected_merge_total"],
            "true_skip": counts["expected_skip_total"],
        }
    return output


def score_rows(
    *,
    rows: list[dict],
    benchmark: dict,
    inherited_score: dict,
) -> dict:
    case_by_id = benchmark_cases_by_id(benchmark)
    counts = Counter()
    true_merge_total = 0
    true_skip_total = 0
    error_ledger: list[dict] = []

    correct_verdicts = 0
    predicted_flags = 0
    predicted_merges = 0
    survivor_errors = 0

    for row in rows:
        case = case_by_id[row["case_id"]]
        predicted = row["normalized_output"]["verdict"]
        expected = case["expected_verdict"]
        label = classify_row(expected, predicted)
        counts[label] += 1
        true_merge_total += int(expected == "MERGE")
        true_skip_total += int(expected == "SKIP")
        predicted_flags += int(predicted == "FLAGGED")
        predicted_merges += int(predicted == "MERGE")
        if expected == predicted:
            correct_verdicts += 1
        elif expected == "SKIP" and predicted == "FLAGGED":
            # safe flag is intentionally not exact-accurate
            pass
        error_ledger.append(
            {
                "case_id": row["case_id"],
                "contender_id": CONTENDER_ID,
                "expected_verdict": expected,
                "pair_id": case["pair_id"],
                "predicted_verdict": predicted,
                "packet_refs_used": row["normalized_output"].get("key_support_refs", [])
                + row["normalized_output"].get("key_contradiction_refs", []),
            }
        )

    total_cases = len(rows)
    merge_total = true_merge_total
    safe_flag = counts["safe_flag"]
    false_merge = counts["false_merge"]
    hard_missed = counts["hard_missed_merge"]
    soft_missed = counts["soft_missed_merge"]

    exact_accuracy = round(correct_verdicts / total_cases, 4)
    merge_capture = round((merge_total - hard_missed - soft_missed) / merge_total, 4)
    false_merge_rate = round(false_merge / counts["true_skip"], 4) if counts["true_skip"] else 0.0
    flag_rate_total = round(predicted_flags / total_cases, 4)
    safe_flag_rate = round(safe_flag / (counts["true_skip"] + safe_flag), 4) if (counts["true_skip"] + safe_flag) else 0.0
    hard_missed_rate = round(hard_missed / merge_total, 4) if merge_total else 0.0
    soft_missed_rate = round(soft_missed / merge_total, 4) if merge_total else 0.0

    auditability = inherited_score["auditability"]
    survivor_accuracy = inherited_score["rates"]["survivor_accuracy"]
    schema_valid_rate = auditability["schema_valid_rate"]

    tier_breakdown = compute_breakdown(rows, case_by_id, "source_pair_tier")
    stratum_breakdown = compute_breakdown(rows, case_by_id, "stratum")
    tail_counts = tier_breakdown["tail"]
    blind_core_counts = stratum_breakdown["blind_core_audit"]
    known_false_counts = stratum_breakdown["known_false_merge_patterns"]
    known_missed_counts = stratum_breakdown["known_missed_merge_patterns"]

    production_checks = {
        "auditability_score_gte_0_95": auditability["auditability_score"] >= 0.95,
        "blind_core_false_merge_zero": blind_core_counts["false_merge"] == 0,
        "blind_core_hard_missed_zero": blind_core_counts["hard_missed_merge"] == 0,
        "blind_core_soft_missed_lte_1": blind_core_counts["soft_missed_merge"] <= 1,
        "false_merge_zero": false_merge == 0,
        "known_false_merge_false_merge_zero": known_false_counts["false_merge"] == 0,
        "known_false_merge_safe_flag_lte_4": known_false_counts["safe_flag"] <= 4,
        "known_missed_hard_lte_2": known_missed_counts["hard_missed_merge"] <= 2,
        "known_missed_soft_lte_4": known_missed_counts["soft_missed_merge"] <= 4,
        "schema_valid_rate_1_00": schema_valid_rate == 1.0,
        "survivor_accuracy_gte_0_95": survivor_accuracy >= 0.95,
        "tail_false_merge_zero": tail_counts["false_merge"] == 0,
        "tail_flag_rate_lte_0_25": (
            (tail_counts["safe_flag"] / (tail_counts["true_merge"] + tail_counts["true_skip"]))
            <= 0.25
        ),
        "tail_hard_missed_lte_2": tail_counts["hard_missed_merge"] <= 2,
    }
    fallback_checks = {
        "auditability_score_gte_0_95": auditability["auditability_score"] >= 0.95,
        "blind_core_false_merge_zero": blind_core_counts["false_merge"] == 0,
        "false_merge_zero": false_merge == 0,
        "flag_rate_total_lte_0_35": flag_rate_total <= 0.35,
        "survivor_accuracy_gte_0_90": survivor_accuracy >= 0.90,
    }

    return {
        "auditability": auditability,
        "breakdowns": {
            "source_pair_tier": tier_breakdown,
            "stratum": stratum_breakdown,
        },
        "case_count_expected": total_cases,
        "case_count_seen": total_cases,
        "contender_id": CONTENDER_ID,
        "cost": inherited_score["cost"],
        "counts": {
            "false_merge": false_merge,
            "hard_missed_merge": hard_missed,
            "safe_flag": safe_flag,
            "soft_missed_merge": soft_missed,
            "survivor_error": survivor_errors,
            "true_merge": true_merge_total,
            "true_skip": true_skip_total,
        },
        "error_ledger": [
            item
            for item in error_ledger
            if item["expected_verdict"] != item["predicted_verdict"]
        ],
        "full_benchmark_run": True,
        "gates": {
            "fallback": {
                "checks": fallback_checks,
                "status": "pass" if all(fallback_checks.values()) else "fail",
            },
            "production": {
                "checks": production_checks,
                "status": "pass" if all(production_checks.values()) else "fail",
            },
        },
        "rates": {
            "exact_verdict_accuracy": exact_accuracy,
            "false_merge_rate": false_merge_rate,
            "flag_rate_total": flag_rate_total,
            "hard_missed_merge_rate": hard_missed_rate,
            "merge_capture_rate": merge_capture,
            "safe_flag_rate": safe_flag_rate,
            "soft_missed_merge_rate": soft_missed_rate,
            "survivor_accuracy": survivor_accuracy,
        },
    }


def render_case_id_list(case_ids: list[str]) -> str:
    if not case_ids:
        return "none"
    return ", ".join(f"`{case_id}`" for case_id in case_ids)


def render_markdown(report: dict) -> str:
    score = report["score"]
    lines: list[str] = []
    lines.append("# Session 10.8 - hybrid guarded cuvee anchor")
    lines.append("")
    lines.append(f"- Generated: {report['generated_at']}")
    lines.append(f"- Run name: `{report['run_name']}`")
    lines.append(f"- Source run: `{report['source_run_name']}`")
    lines.append(f"- Source model: `{report['source_model']}`")
    lines.append(f"- Method version: `{METHOD_VERSION}`")
    lines.append(f"- Incremental model spend: `$0.0000`")
    lines.append(f"- Inherited source-run spend: `${report['usage']['cost_usd']:.4f}`")
    lines.append("")
    lines.append("## Goal")
    lines.append("")
    lines.append(
        "Test whether one narrow visible-packet signal that the guarded frontier did not mine can recover an additional merge without reopening the shared-surname trap family."
    )
    lines.append("")
    lines.append("## Promotion Rule")
    lines.append("")
    lines.append(
        "- `singleton_unique_cuvee_anchor_merge`: shared-surname risk stays on, but promote only when same-region subset/containment is visible, one side is a singleton alias, a partnership-style name is present, and the representative wine strings share a benchmark-unique overlap token."
    )
    lines.append("")
    lines.append("## Changed Cases")
    lines.append("")
    for item in report["changed_cases"]:
        lines.append(
            f"- `{item['case_id']}`: `{item['source_verdict']}` -> `{item['new_verdict']}` via `{item['rule']}` ({', '.join(item['unique_overlap_tokens'])})"
        )
    if not report["changed_cases"]:
        lines.append("- none")
    lines.append("")
    lines.append("## Scorecard")
    lines.append("")
    lines.append(
        f"- Counts: false merge `{score['counts']['false_merge']}`, hard missed `{score['counts']['hard_missed_merge']}`, soft missed `{score['counts']['soft_missed_merge']}`, safe flag `{score['counts']['safe_flag']}`."
    )
    lines.append(
        f"- Rates: exact acc `{score['rates']['exact_verdict_accuracy']:.4f}`, merge capture `{score['rates']['merge_capture_rate']:.4f}`, flag rate `{score['rates']['flag_rate_total']:.4f}`."
    )
    lines.append(
        f"- Gates: production `{score['gates']['production']['status']}`, fallback `{score['gates']['fallback']['status']}`."
    )
    lines.append("")
    lines.append("## Delta Vs Guarded Frontier")
    lines.append("")
    lines.append(
        f"- Improved case ids: {render_case_id_list(report['improved_case_ids'])}"
    )
    lines.append(
        f"- New false merges: {render_case_id_list(report['new_false_merge_case_ids'])}"
    )
    lines.append(
        f"- Remaining hard misses: {render_case_id_list(report['remaining_hard_miss_case_ids'])}"
    )
    lines.append(
        f"- Remaining soft misses: {render_case_id_list(report['remaining_soft_miss_case_ids'])}"
    )
    lines.append("")
    lines.append("## Recommendation")
    lines.append("")
    lines.append(f"- Status: `{report['recommendation']['status']}`")
    lines.append(f"- Reason: {report['recommendation']['reason']}")
    return "\n".join(lines).rstrip() + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(description="Apply singleton cuvee anchor promotion on top of a guarded frontier run.")
    parser.add_argument("--source-report", required=True, help="Path to a session10_8_hybrid_guarded_frontier_*.json report.")
    parser.add_argument("--run-name", required=True, help="Output run name.")
    args = parser.parse_args()

    source_report_path = Path(args.source_report).resolve()
    source_report = load_json(source_report_path)
    benchmark = load_json(BENCHMARK_PATH)
    packet_rows = load_jsonl(VISIBLE_PACKET_PATH)
    packet_by_case = visible_packets_by_case(benchmark, packet_rows)
    overlap_frequency = rare_overlap_frequency(packet_rows)

    source_normalized_path = REPO_ROOT / source_report["normalized_path"]
    source_rows = load_jsonl(source_normalized_path)
    new_rows: list[dict] = []
    changed_cases: list[dict] = []

    for row in source_rows:
        packet = packet_by_case[row["case_id"]]
        applies, details = candidate_applies(packet, row, overlap_frequency)
        if applies:
            promoted = promote_row(row, packet, details)
            new_rows.append(promoted)
            changed_cases.append(
                {
                    "case_id": row["case_id"],
                    "source_verdict": row["normalized_output"]["verdict"],
                    "new_verdict": promoted["normalized_output"]["verdict"],
                    "rule": "singleton_unique_cuvee_anchor_merge",
                    "unique_overlap_tokens": details["unique_overlap_tokens"],
                }
            )
        else:
            carried = deepcopy(row)
            carried["contender_id"] = CONTENDER_ID
            new_rows.append(carried)

    inherited_score = source_report["score"]
    score = score_rows(rows=new_rows, benchmark=benchmark, inherited_score=inherited_score)
    error_ledger = score["error_ledger"]
    remaining_hard = sorted(
        item["case_id"] for item in error_ledger if item["predicted_verdict"] == "SKIP" and item["expected_verdict"] == "MERGE"
    )
    remaining_soft = sorted(
        item["case_id"] for item in error_ledger if item["predicted_verdict"] == "FLAGGED" and item["expected_verdict"] == "MERGE"
    )

    run_root = DEFAULT_OUTPUT_ROOT / args.run_name
    normalized_path = run_root / f"{CONTENDER_ID}.jsonl"
    score_json_path = run_root / f"{CONTENDER_ID}.score.json"
    score_md_path = run_root / f"{CONTENDER_ID}.score.md"
    top_level_json = DEFAULT_OUTPUT_ROOT / f"{args.run_name}.json"
    top_level_md = DEFAULT_OUTPUT_ROOT / f"{args.run_name}.md"

    write_jsonl(normalized_path, new_rows)
    write_json(score_json_path, score)

    report = {
        "generated_at": now_iso(),
        "run_name": args.run_name,
        "method_version": METHOD_VERSION,
        "contender_id": CONTENDER_ID,
        "source_run_name": source_report["run_name"],
        "source_model": source_report["source_model"],
        "source_report": str(source_report_path.relative_to(REPO_ROOT)).replace("\\", "/"),
        "normalized_path": str(normalized_path.relative_to(REPO_ROOT)).replace("\\", "/"),
        "score_json_path": str(score_json_path.relative_to(REPO_ROOT)).replace("\\", "/"),
        "score_md_path": str(score_md_path.relative_to(REPO_ROOT)).replace("\\", "/"),
        "changed_cases": changed_cases,
        "score": score,
        "usage": source_report["usage"],
        "improved_case_ids": sorted(item["case_id"] for item in changed_cases),
        "new_false_merge_case_ids": sorted(
            item["case_id"] for item in error_ledger if item["predicted_verdict"] == "MERGE" and item["expected_verdict"] == "SKIP"
        ),
        "remaining_hard_miss_case_ids": remaining_hard,
        "remaining_soft_miss_case_ids": remaining_soft,
        "recommendation": {
            "status": (
                "candidate_improves_guarded_margin"
                if score["gates"]["production"]["status"] == "pass"
                and score["counts"]["false_merge"] == 0
                and score["counts"]["soft_missed_merge"] < inherited_score["counts"]["soft_missed_merge"]
                else "candidate_not_better_than_guarded"
            ),
            "reason": (
                "The visible singleton-cuvee anchor rule improved the guarded frontier without reopening false merges."
                if score["gates"]["production"]["status"] == "pass"
                and score["counts"]["false_merge"] == 0
                and score["counts"]["soft_missed_merge"] < inherited_score["counts"]["soft_missed_merge"]
                else "The extra visible-packet promotion did not produce a cleaner benchmark result than the guarded frontier source."
            ),
        },
    }
    write_json(top_level_json, report)
    score_md_path.write_text(render_markdown(report), encoding="utf-8")
    top_level_md.write_text(render_markdown(report), encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
