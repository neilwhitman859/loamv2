"""Session 10.8 - visible-signature promotion bakeoff.

This script tests a new method family on top of the frozen
`session9_7_layered_safety_sonnet_r2_narrow` control:

1. Keep the zero-false-merge fallback control fixed.
2. Extract only visible packet features and citeable refs.
3. Learn a tiny set of positive promotion clauses that recover control misses
   while allowing zero skip hits on the training slice.
4. Promote only control non-merge rows that match one of those clauses.

The script writes two evaluations:
- full-fit: an upper-bound benchmark fit, useful only for candidate discovery
- out-of-fold: each case is predicted by clauses learned without that case

The out-of-fold result is the honest confirmation check. A full-fit production
pass is not enough by itself because the clauses are learned from the benchmark.
"""

from __future__ import annotations

import argparse
import json
from collections import Counter, defaultdict
from copy import deepcopy
from datetime import datetime
from itertools import combinations
from pathlib import Path

from pipeline.identity.bakeoff_harness_v1 import load_benchmark_payload, score_run
from pipeline.identity.bakeoff_harness_v2 import (
    DEFAULT_BENCHMARK,
    DEFAULT_OUTPUT_DIR,
    ensure_visible_packets,
)
from pipeline.identity.bakeoff_method_bakeoff_proof import (
    CONTROL_CONTENDER_ID,
    CONTROL_NORMALIZED,
    CONTROL_RUN_NAME,
    PacketFeatures,
    build_features,
    load_rows_by_case,
)
from pipeline.identity.bakeoff_packet_v2 import canonical_json_dumps, write_jsonl


REPO_ROOT = Path(__file__).resolve().parents[2]
RUN_NAME_DEFAULT = "session10_8_visible_signature_promotion_v1"
DEFAULT_OUTPUT_ROOT = REPO_ROOT / "data" / "sprints" / "identity-er" / "method_bakeoff"
DEFAULT_REPORT_JSON = DEFAULT_OUTPUT_ROOT / f"{RUN_NAME_DEFAULT}.json"
DEFAULT_REPORT_MD = DEFAULT_OUTPUT_ROOT / f"{RUN_NAME_DEFAULT}.md"
TERM_VERSION = "visible_signature_terms_v1"
KEY_REF_IDS = (
    "secondary_a_1",
    "secondary_b_1",
    "secondary_b_2",
    "risk_sparse_official_evidence",
    "risk_shared_surname_split",
    "geo_country_conflict",
    "geo_same_country",
    "geo_same_region",
    "catalog_portfolio_shape",
    "catalog_subset_match",
    "catalog_exact_overlap",
    "catalog_asymmetry",
    "lex_contains",
    "lex_near_exact",
    "lex_shared_core_tokens",
)


def now_iso() -> str:
    return datetime.now().astimezone().isoformat(timespec="seconds")


def write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(canonical_json_dumps(payload), encoding="utf-8")


def case_sort_key(case_id: str, cases_by_id: dict[str, dict]) -> tuple:
    case = cases_by_id[case_id]
    return (
        case["stratum"],
        case["expected_verdict"],
        case["case_id"],
    )


def visible_terms(*, features: PacketFeatures, refs: set[str]) -> tuple[str, ...]:
    terms: set[str] = set()
    terms.add(f"family={features.candidate_family or 'unknown'}")
    terms.add(f"containment={features.containment}")
    terms.add(f"shared_core={features.shared_core_token_count}")
    terms.add("same_country" if features.same_country else "not_same_country")
    terms.add("same_region" if features.same_region else "not_same_region")
    terms.add("country_conflict" if features.country_conflict else "no_country_conflict")
    terms.add("shared_surname_split" if features.shared_surname_split else "no_shared_surname_split")
    terms.add("secondary_rel" if features.secondary_relationship_without_identity else "no_secondary_rel")
    terms.add("holdco" if features.holdco_or_product_tier else "no_holdco")
    terms.add("owner_operator" if features.owner_or_operator_not_identity else "no_owner_operator")
    terms.add("subset" if features.has_subset_match else "no_subset")
    terms.add("portfolio" if features.has_portfolio_shape else "no_portfolio")
    terms.add("overlap0" if features.exact_overlap_count == 0 else "overlap1plus")
    if features.exact_overlap_count >= 1:
        terms.add("overlap1")
    if features.exact_overlap_count >= 2:
        terms.add("overlap2")
    terms.add("anchor0" if features.anchor_overlap_count == 0 else "anchor1plus")
    if features.anchor_overlap_count >= 1:
        terms.add("anchor1")
    if features.anchor_overlap_count >= 2:
        terms.add("anchor2")
    if features.rare_anchor_count >= 1:
        terms.add("rare1")
    for threshold in (1.0, 0.7, 0.6, 0.5, 0.4):
        if features.trigram_similarity >= threshold:
            terms.add(f"trigram>={threshold}")
    if features.wine_count_small <= 1:
        terms.add("small_le_1")
    if features.wine_count_small <= 2:
        terms.add("small_le_2")
    if features.wine_count_small <= 4:
        terms.add("small_le_4")
    if features.wine_count_large <= 12:
        terms.add("large_le_12")
    terms.add("has_secondary_a1" if "secondary_a_1" in refs else "no_secondary_a1")
    terms.add("has_secondary_b1" if "secondary_b_1" in refs else "no_secondary_b1")
    terms.add("has_secondary_b2" if "secondary_b_2" in refs else "no_secondary_b2")
    for ref_id in KEY_REF_IDS:
        if ref_id in refs:
            terms.add(f"ref={ref_id}")
    return tuple(sorted(terms))


def build_term_rows(
    *,
    case_ids: list[str],
    cases_by_id: dict[str, dict],
    control_rows_by_case: dict[str, dict],
    features_by_case: dict[str, PacketFeatures],
    refs_by_case: dict[str, set[str]],
) -> dict[str, dict]:
    rows: dict[str, dict] = {}
    for case_id in case_ids:
        rows[case_id] = {
            "case_id": case_id,
            "expected_verdict": cases_by_id[case_id]["expected_verdict"],
            "control_verdict": control_rows_by_case[case_id]["normalized_output"]["verdict"],
            "terms": visible_terms(features=features_by_case[case_id], refs=refs_by_case[case_id]),
        }
    return rows


def prune_dominated_rules(rules: list[dict]) -> list[dict]:
    pruned: list[dict] = []
    ordered = sorted(
        rules,
        key=lambda item: (
            -len(item["train_miss_hits"]),
            len(item["terms"]),
            item["terms"],
        ),
    )
    for candidate in ordered:
        dominated = False
        for kept in pruned:
            if (
                candidate["train_miss_hits"].issubset(kept["train_miss_hits"])
                and len(kept["terms"]) <= len(candidate["terms"])
            ):
                dominated = True
                break
        if not dominated:
            pruned.append(candidate)
    return pruned


def select_rule_set(candidate_rules: list[dict], max_rules: int) -> list[dict]:
    selected: list[dict] = []
    covered: set[str] = set()
    remaining = list(candidate_rules)
    while remaining and len(selected) < max_rules:
        ranked = sorted(
            remaining,
            key=lambda rule: (
                -len(rule["train_miss_hits"] - covered),
                len(rule["terms"]),
                rule["terms"],
            ),
        )
        best = ranked[0]
        if not (best["train_miss_hits"] - covered):
            break
        selected.append(best)
        covered |= best["train_miss_hits"]
        remaining = [rule for rule in remaining if rule["terms"] != best["terms"]]
    return selected


def discover_rules(
    *,
    train_case_ids: list[str],
    term_rows: dict[str, dict],
    max_rule_size: int,
    max_rules: int,
) -> dict:
    miss_rows = [
        term_rows[case_id]
        for case_id in train_case_ids
        if term_rows[case_id]["expected_verdict"] == "MERGE"
        and term_rows[case_id]["control_verdict"] != "MERGE"
    ]
    skip_rows = [
        term_rows[case_id]
        for case_id in train_case_ids
        if term_rows[case_id]["expected_verdict"] == "SKIP"
        and term_rows[case_id]["control_verdict"] != "MERGE"
    ]
    if not miss_rows:
        return {
            "rules": [],
            "train_miss_case_ids": [],
            "train_skip_case_ids": [row["case_id"] for row in skip_rows],
            "covered_train_miss_case_ids": [],
        }

    candidate_rules_by_hitset: dict[tuple[str, ...], dict] = {}
    for miss_row in miss_rows:
        terms = miss_row["terms"]
        found_for_miss = False
        for size in range(1, max_rule_size + 1):
            size_candidates: list[dict] = []
            for combo in combinations(terms, size):
                combo_key = tuple(combo)
                train_miss_hits = {
                    row["case_id"]
                    for row in miss_rows
                    if all(term in row["terms"] for term in combo)
                }
                if not train_miss_hits:
                    continue
                train_skip_hits = {
                    row["case_id"]
                    for row in skip_rows
                    if all(term in row["terms"] for term in combo)
                }
                if train_skip_hits:
                    continue
                hitset_key = tuple(sorted(train_miss_hits))
                candidate = {
                    "terms": combo_key,
                    "train_miss_hits": train_miss_hits,
                    "train_skip_hits": train_skip_hits,
                }
                size_candidates.append(candidate)
                existing = candidate_rules_by_hitset.get(hitset_key)
                if existing is None or len(combo_key) < len(existing["terms"]) or (
                    len(combo_key) == len(existing["terms"]) and combo_key < existing["terms"]
                ):
                    candidate_rules_by_hitset[hitset_key] = candidate
            if size_candidates:
                found_for_miss = True
                break
        if found_for_miss:
            continue

    candidate_rules = prune_dominated_rules(list(candidate_rules_by_hitset.values()))
    selected_rules = select_rule_set(candidate_rules, max_rules=max_rules)
    covered_train_miss_case_ids = sorted(
        {
            case_id
            for rule in selected_rules
            for case_id in rule["train_miss_hits"]
        }
    )
    return {
        "rules": selected_rules,
        "train_miss_case_ids": sorted(row["case_id"] for row in miss_rows),
        "train_skip_case_ids": sorted(row["case_id"] for row in skip_rows),
        "covered_train_miss_case_ids": covered_train_miss_case_ids,
    }


def support_refs_for_rule(rule_terms: tuple[str, ...], refs: set[str]) -> list[str]:
    ordered: list[str] = []
    if "ref=lex_near_exact" in rule_terms and "lex_near_exact" in refs:
        ordered.append("lex_near_exact")
    if "ref=lex_contains" in rule_terms and "lex_contains" in refs:
        ordered.append("lex_contains")
    if "ref=lex_shared_core_tokens" in rule_terms and "lex_shared_core_tokens" in refs:
        ordered.append("lex_shared_core_tokens")
    if "same_country" in rule_terms and "geo_same_country" in refs:
        ordered.append("geo_same_country")
    if "same_region" in rule_terms and "geo_same_region" in refs:
        ordered.append("geo_same_region")
    if "country_conflict" in rule_terms and "geo_country_conflict" in refs:
        ordered.append("geo_country_conflict")
    if "subset" in rule_terms and "catalog_subset_match" in refs:
        ordered.append("catalog_subset_match")
    if "overlap1" in rule_terms and "catalog_exact_overlap" in refs:
        ordered.append("catalog_exact_overlap")
    if "portfolio" in rule_terms and "catalog_portfolio_shape" in refs:
        ordered.append("catalog_portfolio_shape")
    for ref_id in ("secondary_a_1", "secondary_b_1", "secondary_b_2"):
        if f"ref={ref_id}" in rule_terms and ref_id in refs:
            ordered.append(ref_id)
    if not ordered:
        for ref_id in (
            "lex_shared_core_tokens",
            "lex_contains",
            "lex_near_exact",
            "catalog_subset_match",
            "catalog_portfolio_shape",
            "secondary_a_1",
            "secondary_b_1",
            "secondary_b_2",
        ):
            if ref_id in refs:
                ordered.append(ref_id)
                if len(ordered) == 2:
                    break
    return list(dict.fromkeys(ordered))[:3]


def contradiction_refs_for_rule(rule_terms: tuple[str, ...], refs: set[str]) -> list[str]:
    ordered: list[str] = []
    for ref_id in (
        "risk_shared_surname_split",
        "risk_sparse_official_evidence",
        "geo_country_conflict",
    ):
        if ref_id in refs and (
            f"ref={ref_id}" in rule_terms
            or ref_id == "risk_sparse_official_evidence"
        ):
            ordered.append(ref_id)
    return list(dict.fromkeys(ordered))[:2]


def rule_ids_for_terms(rule_terms: tuple[str, ...]) -> list[str]:
    if "country_conflict" in rule_terms:
        return ["11.1", "11.4.b"]
    if "shared_surname_split" in rule_terms and "containment=b_in_a" in rule_terms:
        return ["11.1", "11.4.f"]
    if "family=same_country_lexical_alias" in rule_terms:
        return ["11.1", "11.4.h"]
    return ["11.1"]


def apply_rules(
    *,
    target_case_ids: list[str],
    rules: list[dict],
    cases_by_id: dict[str, dict],
    control_rows_by_case: dict[str, dict],
    term_rows: dict[str, dict],
    packet_lookup: dict[str, dict],
    contender_id: str,
) -> tuple[list[dict], dict[str, dict], list[dict]]:
    rows: list[dict] = []
    rows_by_case: dict[str, dict] = {}
    promotions: list[dict] = []
    for case_id in target_case_ids:
        row = deepcopy(control_rows_by_case[case_id])
        row["contender_id"] = contender_id
        matched_rules: list[tuple[str, ...]] = []
        if row["normalized_output"]["verdict"] != "MERGE":
            case_terms = set(term_rows[case_id]["terms"])
            for rule in rules:
                if all(term in case_terms for term in rule["terms"]):
                    matched_rules.append(rule["terms"])
            if matched_rules:
                packet_id = f"producer_pair_{cases_by_id[case_id]['pair_id']}_v2"
                packet = packet_lookup[packet_id]
                refs = {
                    entry["ref_id"]
                    for entry in packet.get("evidence_refs", [])
                    if isinstance(entry, dict) and entry.get("ref_id")
                }
                primary_rule = matched_rules[0]
                row["normalized_output"] = {
                    "packet_id": packet_id,
                    "verdict": "MERGE",
                    "confidence": 0.91,
                    "rule_ids": rule_ids_for_terms(primary_rule),
                    "reason": (
                        "Visible-signature promotion layer promoted this control non-merge because the packet matches "
                        "a training-vetted zero-skip merge signature."
                    ),
                    "key_support_refs": support_refs_for_rule(primary_rule, refs),
                    "key_contradiction_refs": contradiction_refs_for_rule(primary_rule, refs),
                    "survivor_producer_id": packet["evidence"]["survivor_if_merge"].get(
                        "recommended_survivor_producer_id"
                    ),
                    "follow_up": None,
                }
                row["promotion_rules"] = [list(rule) for rule in matched_rules]
                promotions.append(
                    {
                        "case_id": case_id,
                        "matched_rules": [list(rule) for rule in matched_rules],
                    }
                )
        rows.append(row)
        rows_by_case[case_id] = row
    return rows, rows_by_case, promotions


def build_folds(case_ids: list[str], cases_by_id: dict[str, dict], fold_count: int) -> list[list[str]]:
    groups: dict[tuple[str, str], list[str]] = defaultdict(list)
    for case_id in case_ids:
        case = cases_by_id[case_id]
        groups[(case["stratum"], case["expected_verdict"])].append(case_id)
    folds: list[list[str]] = [[] for _ in range(fold_count)]
    for group_key in sorted(groups):
        group_case_ids = sorted(groups[group_key], key=lambda cid: cases_by_id[cid]["case_id"])
        for index, case_id in enumerate(group_case_ids):
            folds[index % fold_count].append(case_id)
    return [sorted(fold, key=lambda cid: case_sort_key(cid, cases_by_id)) for fold in folds]


def score_rows(
    *,
    benchmark_payload: dict,
    full_packets: list[dict],
    rows: list[dict],
    output_dir: Path,
    run_name: str,
    contender_id: str,
) -> dict:
    normalized_path = output_dir / f"{contender_id}.jsonl"
    score_json = output_dir / f"{contender_id}.score.json"
    score_md = output_dir / f"{contender_id}.score.md"
    write_jsonl(normalized_path, rows)
    report = score_run(
        benchmark_payload,
        [normalized_path],
        full_packets,
        score_json,
        score_md,
        run_name,
    )
    return {
        "normalized_path": str(normalized_path.relative_to(REPO_ROOT)),
        "score_json_path": str(score_json.relative_to(REPO_ROOT)),
        "score_md_path": str(score_md.relative_to(REPO_ROOT)),
        "score": report["contenders"][0],
        "winner_selection": report["winner_selection"],
    }


def render_rule(rule: dict) -> str:
    terms = ", ".join(f"`{term}`" for term in rule["terms"])
    hits = ", ".join(f"`{case_id}`" for case_id in sorted(rule["train_miss_hits"]))
    return f"- {terms} -> {hits or 'none'}"


def render_markdown(report: dict) -> str:
    lines: list[str] = []
    lines.append("# Session 10.8 - visible-signature promotion bakeoff")
    lines.append("")
    lines.append(f"- Generated: {report['generated_at']}")
    lines.append(f"- Run name: `{report['run_name']}`")
    lines.append(f"- Frozen control: `{CONTROL_RUN_NAME}` / `{CONTROL_CONTENDER_ID}`")
    lines.append(f"- Term set: `{report['term_version']}`")
    lines.append(f"- New model spend: `$0.00`")
    lines.append("")
    lines.append("## Goal")
    lines.append("")
    lines.append(
        "Test whether the frozen Session 9.7 fallback control can be lifted to production quality by a tiny visible-signature promotion layer learned from the existing packet surface, without changing the scorer, the benchmark, or the negative-control base."
    )
    lines.append("")
    lines.append("## Method")
    lines.append("")
    lines.append("1. Keep the Session 9.7 control fixed as the base decision layer.")
    lines.append("2. Extract only visible packet features and citeable refs.")
    lines.append("3. Mine conjunction rules from control misses, but reject any rule that hits a skip row on the training slice.")
    lines.append("4. Select a small rule set that covers as many training misses as possible.")
    lines.append("5. Promote only control non-merge rows that match one of those clauses.")
    lines.append("")
    lines.append("## Full-Fit Result")
    lines.append("")
    lines.append(
        f"Full-fit status: production `{report['full_fit']['score']['gates']['production']['status']}`, "
        f"fallback `{report['full_fit']['score']['gates']['fallback']['status']}`."
    )
    lines.append("")
    lines.append(
        f"- Counts: false merge `{report['full_fit']['score']['counts']['false_merge']}`, hard missed `{report['full_fit']['score']['counts']['hard_missed_merge']}`, soft missed `{report['full_fit']['score']['counts']['soft_missed_merge']}`, safe flag `{report['full_fit']['score']['counts']['safe_flag']}`."
    )
    lines.append(
        f"- Rates: exact acc `{report['full_fit']['score']['rates']['exact_verdict_accuracy']:.4f}`, merge capture `{report['full_fit']['score']['rates']['merge_capture_rate']:.4f}`, flag rate `{report['full_fit']['score']['rates']['flag_rate_total']:.4f}`."
    )
    lines.append(
        f"- Promoted cases: {', '.join(f'`{item['case_id']}`' for item in report['full_fit']['promotions']) or 'none'}"
    )
    lines.append("")
    lines.append("Selected full-fit rules:")
    for rule in report["full_fit"]["rules"]:
        lines.append(render_rule(rule))
    lines.append("")
    lines.append("## Out-Of-Fold Confirmation")
    lines.append("")
    lines.append(
        f"OOF status: production `{report['out_of_fold']['score']['gates']['production']['status']}`, "
        f"fallback `{report['out_of_fold']['score']['gates']['fallback']['status']}`."
    )
    lines.append("")
    lines.append(
        f"- Counts: false merge `{report['out_of_fold']['score']['counts']['false_merge']}`, hard missed `{report['out_of_fold']['score']['counts']['hard_missed_merge']}`, soft missed `{report['out_of_fold']['score']['counts']['soft_missed_merge']}`, safe flag `{report['out_of_fold']['score']['counts']['safe_flag']}`."
    )
    lines.append(
        f"- Rates: exact acc `{report['out_of_fold']['score']['rates']['exact_verdict_accuracy']:.4f}`, merge capture `{report['out_of_fold']['score']['rates']['merge_capture_rate']:.4f}`, flag rate `{report['out_of_fold']['score']['rates']['flag_rate_total']:.4f}`."
    )
    lines.append(
        f"- OOF promoted cases: {', '.join(f'`{item['case_id']}`' for item in report['out_of_fold']['promotions']) or 'none'}"
    )
    lines.append("")
    lines.append("Fold summaries:")
    for fold in report["out_of_fold"]["folds"]:
        lines.append(
            f"- Fold `{fold['fold_index']}`: train misses `{len(fold['train_miss_case_ids'])}`, covered train misses `{len(fold['covered_train_miss_case_ids'])}`, test promotions `{len(fold['test_promotions'])}`."
        )
    lines.append("")
    lines.append("Most frequent OOF rules:")
    for item in report["out_of_fold"]["rule_frequency"]:
        terms = ", ".join(f"`{term}`" for term in item["terms"])
        lines.append(f"- `{item['count']}` folds: {terms}")
    lines.append("")
    lines.append("## Recommendation")
    lines.append("")
    lines.append(f"- Status: `{report['recommendation']['status']}`")
    lines.append(f"- Reason: {report['recommendation']['reason']}")
    return "\n".join(lines).rstrip() + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(description="Run the Session 10.8 visible-signature promotion bakeoff")
    parser.add_argument("--benchmark", type=Path, default=DEFAULT_BENCHMARK)
    parser.add_argument("--packet-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--run-name", default=RUN_NAME_DEFAULT)
    parser.add_argument("--report-json", type=Path, default=DEFAULT_REPORT_JSON)
    parser.add_argument("--report-md", type=Path, default=DEFAULT_REPORT_MD)
    parser.add_argument("--max-rule-size", type=int, default=5)
    parser.add_argument("--max-rules", type=int, default=6)
    parser.add_argument("--fold-count", type=int, default=5)
    args = parser.parse_args()

    benchmark_payload = load_benchmark_payload(args.benchmark)
    case_ids = [case["case_id"] for case in benchmark_payload["cases"]]
    cases_by_id = {case["case_id"]: case for case in benchmark_payload["cases"]}
    full_packets, _, packet_validation = ensure_visible_packets(
        args.packet_dir,
        args.benchmark,
        force_rebuild=False,
    )
    packet_lookup = {packet["packet_id"]: packet for packet in full_packets}
    control_rows_by_case = load_rows_by_case(CONTROL_NORMALIZED)

    features_by_case: dict[str, PacketFeatures] = {}
    refs_by_case: dict[str, set[str]] = {}
    for case_id in case_ids:
        case = cases_by_id[case_id]
        packet_id = f"producer_pair_{case['pair_id']}_v2"
        packet = packet_lookup[packet_id]
        features_by_case[case_id] = build_features(case, packet)
        refs_by_case[case_id] = {
            entry["ref_id"]
            for entry in packet.get("evidence_refs", [])
            if isinstance(entry, dict) and entry.get("ref_id")
        }

    term_rows = build_term_rows(
        case_ids=case_ids,
        cases_by_id=cases_by_id,
        control_rows_by_case=control_rows_by_case,
        features_by_case=features_by_case,
        refs_by_case=refs_by_case,
    )

    run_root = args.output_root / args.run_name
    run_root.mkdir(parents=True, exist_ok=True)

    full_fit_discovery = discover_rules(
        train_case_ids=case_ids,
        term_rows=term_rows,
        max_rule_size=args.max_rule_size,
        max_rules=args.max_rules,
    )
    full_fit_rows, _, full_fit_promotions = apply_rules(
        target_case_ids=case_ids,
        rules=full_fit_discovery["rules"],
        cases_by_id=cases_by_id,
        control_rows_by_case=control_rows_by_case,
        term_rows=term_rows,
        packet_lookup=packet_lookup,
        contender_id="visible_signature_promotion_full_fit_v1",
    )
    full_fit_result = score_rows(
        benchmark_payload=benchmark_payload,
        full_packets=full_packets,
        rows=full_fit_rows,
        output_dir=run_root / "full_fit",
        run_name=args.run_name,
        contender_id="visible_signature_promotion_full_fit_v1",
    )

    folds = build_folds(case_ids, cases_by_id, args.fold_count)
    oof_rows_by_case: dict[str, dict] = {
        case_id: deepcopy(control_rows_by_case[case_id]) for case_id in case_ids
    }
    fold_reports: list[dict] = []
    rule_frequency: Counter[tuple[str, ...]] = Counter()

    for fold_index, test_case_ids in enumerate(folds, start=1):
        train_case_ids = [case_id for case_id in case_ids if case_id not in set(test_case_ids)]
        discovery = discover_rules(
            train_case_ids=train_case_ids,
            term_rows=term_rows,
            max_rule_size=args.max_rule_size,
            max_rules=args.max_rules,
        )
        for rule in discovery["rules"]:
            rule_frequency[rule["terms"]] += 1
        test_rows, test_rows_by_case, test_promotions = apply_rules(
            target_case_ids=test_case_ids,
            rules=discovery["rules"],
            cases_by_id=cases_by_id,
            control_rows_by_case=control_rows_by_case,
            term_rows=term_rows,
            packet_lookup=packet_lookup,
            contender_id="visible_signature_promotion_oof_v1",
        )
        for row in test_rows:
            oof_rows_by_case[row["case_id"]] = row
        fold_reports.append(
            {
                "fold_index": fold_index,
                "test_case_ids": test_case_ids,
                "train_miss_case_ids": discovery["train_miss_case_ids"],
                "covered_train_miss_case_ids": discovery["covered_train_miss_case_ids"],
                "rules": [
                    {
                        "terms": list(rule["terms"]),
                        "train_miss_hits": sorted(rule["train_miss_hits"]),
                    }
                    for rule in discovery["rules"]
                ],
                "test_promotions": test_promotions,
                "test_case_count": len(test_rows),
            }
        )

    oof_rows = [
        deepcopy(oof_rows_by_case[case_id]) for case_id in sorted(case_ids, key=lambda cid: case_sort_key(cid, cases_by_id))
    ]
    for row in oof_rows:
        row["contender_id"] = "visible_signature_promotion_oof_v1"
    oof_promotions = [
        {
            "case_id": row["case_id"],
            "matched_rules": row.get("promotion_rules", []),
        }
        for row in oof_rows
        if row.get("promotion_rules")
    ]
    oof_result = score_rows(
        benchmark_payload=benchmark_payload,
        full_packets=full_packets,
        rows=oof_rows,
        output_dir=run_root / "out_of_fold",
        run_name=args.run_name,
        contender_id="visible_signature_promotion_oof_v1",
    )

    if (
        full_fit_result["score"]["gates"]["production"]["status"] == "pass"
        and oof_result["score"]["gates"]["production"]["status"] == "pass"
    ):
        recommendation = {
            "status": "survives_confirmation",
            "reason": (
                "The visible-signature promotion layer clears the frozen production gate both as a full-fit upper bound and as an out-of-fold confirmation check, so it deserves promotion into the next rigorous method cycle."
            ),
        }
    elif full_fit_result["score"]["gates"]["production"]["status"] == "pass":
        recommendation = {
            "status": "promising_but_unconfirmed",
            "reason": (
                "The full-fit result clears the frozen production gate, but the out-of-fold confirmation did not. Treat this as a promising method family, not as a production-ready win."
            ),
        }
    else:
        recommendation = {
            "status": "fails_confirmation",
            "reason": (
                "The visible-signature promotion layer does not clear the frozen production gate once the result is measured honestly enough to trust."
            ),
        }

    report = {
        "generated_at": now_iso(),
        "run_name": args.run_name,
        "benchmark_id": benchmark_payload["benchmark_id"],
        "term_version": TERM_VERSION,
        "packet_validation": {
            "packet_version": packet_validation["packet_version"],
            "packet_count": packet_validation["packet_count"],
            "hidden_field_leaks": packet_validation["hidden_field_leaks"],
        },
        "control": {
            "run_name": CONTROL_RUN_NAME,
            "contender_id": CONTROL_CONTENDER_ID,
            "normalized_path": str(CONTROL_NORMALIZED.relative_to(REPO_ROOT)),
        },
        "config": {
            "max_rule_size": args.max_rule_size,
            "max_rules": args.max_rules,
            "fold_count": args.fold_count,
        },
        "full_fit": {
            "rules": [
                {
                    "terms": list(rule["terms"]),
                    "train_miss_hits": sorted(rule["train_miss_hits"]),
                }
                for rule in full_fit_discovery["rules"]
            ],
            "train_miss_case_ids": full_fit_discovery["train_miss_case_ids"],
            "covered_train_miss_case_ids": full_fit_discovery["covered_train_miss_case_ids"],
            "promotions": full_fit_promotions,
            **full_fit_result,
        },
        "out_of_fold": {
            "folds": fold_reports,
            "promotions": oof_promotions,
            "rule_frequency": [
                {"terms": list(terms), "count": count}
                for terms, count in sorted(
                    rule_frequency.items(),
                    key=lambda item: (-item[1], len(item[0]), item[0]),
                )
            ],
            **oof_result,
        },
        "recommendation": recommendation,
    }

    write_json(args.report_json, report)
    args.report_md.write_text(render_markdown(report), encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
