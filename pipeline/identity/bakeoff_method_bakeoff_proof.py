"""Session 9.10 - proof-first method bakeoff on a bounded subset.

This script implements four lightweight method-class contenders on top of the
frozen Session 9.7 layered-safety control:

1. expanded_layered_router_v1
2. signature_router_v1
3. merge_proposer_plus_veto_v1
4. evidence_digest_then_judge_v1

The proof subset is intentionally constructed from:
- all Session 9.7 residual misses
- all Session 9.6 false merges
- Session 9.8 named adjacent skip controls
- extra negatives needed by the newly widened method classes
- a small hold set of Session 9.7 wins

No model calls are made here. The point is to compare architecture shapes first
and cheaply before spending on any later full-rerun.
"""

from __future__ import annotations

import argparse
import json
from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from pipeline.identity.bakeoff_harness_v2 import (
    DEFAULT_BENCHMARK,
    DEFAULT_OUTPUT_DIR,
    DEFAULT_RUN_ROOT,
    ensure_visible_packets,
    load_jsonl,
)
from pipeline.identity.bakeoff_packet_v2 import canonical_json_dumps, write_jsonl


REPO_ROOT = Path(__file__).resolve().parents[2]
CONTROL_RUN_NAME = "session9_7_layered_safety_sonnet_r2_narrow"
CONTROL_CONTENDER_ID = "layered_safety_sonnet_r2_narrow_v1"
CONTROL_NORMALIZED = (
    DEFAULT_RUN_ROOT / "normalized" / CONTROL_RUN_NAME / f"{CONTROL_CONTENDER_ID}.jsonl"
)
RUN_NAME_DEFAULT = "session9_10_method_bakeoff_proof_subset"
DEFAULT_MEMO_PATH = REPO_ROOT / "data" / "sprints" / "dedup" / f"{RUN_NAME_DEFAULT}.md"

INPUT_PATHS = [
    "data/sprints/dedup/session9_9_method_bakeoff_design.md",
    "data/sprints/dedup/session9_8_recover_production_from_layered_fallback.md",
    "data/sprints/dedup/session9_7_layered_safety_redesign.md",
    "data/sprints/dedup/bakeoff_v2/scored/session9_7_layered_safety_sonnet_r2_narrow.json",
    "data/sprints/dedup/bakeoff_v2/scored/session9_7_layered_safety_sonnet_r2_narrow.md",
]

PROOF_GROUPS = [
    {
        "id": "session97_residual_misses",
        "label": "Session 9.7 residual misses",
        "why": "All nine remaining misses from the frozen layered-fallback control.",
        "case_ids": [
            "blind_core_audit_001",
            "blind_core_audit_012",
            "blind_core_audit_016",
            "blind_core_audit_019",
            "blind_core_audit_024",
            "known_missed_merge_patterns_001",
            "known_missed_merge_patterns_002",
            "known_missed_merge_patterns_008",
            "known_missed_merge_patterns_011",
        ],
    },
    {
        "id": "session96_false_merges",
        "label": "Session 9.6 false merges",
        "why": "The five concrete trap cases that the Session 9.7 safety layer had to remove.",
        "case_ids": [
            "known_false_merge_patterns_005",
            "known_false_merge_patterns_007",
            "known_false_merge_patterns_011",
            "known_false_merge_patterns_012",
            "tail_random_sample_008",
        ],
    },
    {
        "id": "session98_named_adjacent_skips",
        "label": "Session 9.8 named adjacent skip controls",
        "why": "The exact nearby skip controls named in Session 9.8 when it argued the remaining signatures were entangled with trap zones.",
        "case_ids": [
            "known_false_merge_patterns_009",
            "blind_core_audit_032",
            "blind_core_audit_041",
            "blind_core_audit_062",
            "blind_core_audit_069",
        ],
    },
    {
        "id": "expanded_family_negatives",
        "label": "Expanded-family negatives",
        "why": "Additional negatives required because Session 9.9 widened the contender set into 11.1 / 11.4.b / 11.4.o territory.",
        "case_ids": [
            "blind_core_audit_057",
            "blind_core_audit_091",
            "blind_core_audit_092",
            "blind_core_audit_093",
            "tail_random_sample_020",
        ],
    },
    {
        "id": "hold_set_current_wins",
        "label": "Hold set of current wins",
        "why": "A small preservation set of Session 9.7 wins so the proof catches regressions immediately.",
        "case_ids": [
            "blind_core_audit_002",
            "blind_core_audit_005",
            "blind_core_audit_007",
            "blind_core_audit_023",
            "blind_core_audit_026",
        ],
    },
]

BLIND_CORE_BLOCKER_CASE_IDS = [
    "blind_core_audit_001",
    "blind_core_audit_012",
    "blind_core_audit_016",
    "blind_core_audit_019",
    "blind_core_audit_024",
]

METHOD_ORDER = [
    "expanded_layered_router_v1",
    "signature_router_v1",
    "merge_proposer_plus_veto_v1",
    "evidence_digest_then_judge_v1",
]


@dataclass(frozen=True)
class Proposal:
    signature: str
    reason: str
    rule_ids: tuple[str, ...]
    support_refs: tuple[str, ...]
    contradiction_refs: tuple[str, ...]


@dataclass(frozen=True)
class PacketFeatures:
    case_id: str
    cluster: str
    expected_verdict: str
    packet_id: str
    refs: frozenset[str]
    containment: str
    shared_core_token_count: int
    trigram_similarity: float
    same_country: bool
    same_region: bool
    country_conflict: bool
    shared_surname_split: bool
    secondary_relationship_without_identity: bool
    holdco_or_product_tier: bool
    owner_or_operator_not_identity: bool
    exact_overlap_count: int
    has_subset_match: bool
    has_portfolio_shape: bool
    anchor_overlap_count: int
    rare_anchor_count: int
    wine_count_small: int
    wine_count_large: int
    candidate_family: str | None


def write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(canonical_json_dumps(payload), encoding="utf-8")


def load_benchmark_payload(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def load_rows_by_case(path: Path) -> dict[str, dict]:
    return {row["case_id"]: row for row in load_jsonl(path)}


def validate_groups(cases_by_id: dict[str, dict]) -> list[str]:
    ordered: list[str] = []
    seen: set[str] = set()
    for group in PROOF_GROUPS:
        for case_id in group["case_ids"]:
            if case_id in seen:
                raise RuntimeError(f"Duplicate proof case id: {case_id}")
            if case_id not in cases_by_id:
                raise RuntimeError(f"Unknown proof case id: {case_id}")
            seen.add(case_id)
            ordered.append(case_id)
    return ordered


def extract_candidate_family(pair_payload: dict) -> str | None:
    for item in pair_payload.get("why_this_pair_exists", []):
        if isinstance(item, str) and item.startswith("candidate_family="):
            return item.split("=", 1)[1]
    return None


def build_features(case: dict, packet: dict) -> PacketFeatures:
    lexical = packet["evidence"]["comparison"]["lexical"]
    geography = packet["evidence"]["comparison"]["geography"]
    risk = packet["evidence"]["comparison"]["risk_flags"]
    catalog = packet["evidence"]["comparison"]["catalog"]
    refs = frozenset(
        entry["ref_id"]
        for entry in packet.get("evidence_refs", [])
        if isinstance(entry, dict) and entry.get("ref_id")
    )
    wine_count_a = int(packet["evidence"]["side_a"]["catalog_summary"]["wine_count"] or 0)
    wine_count_b = int(packet["evidence"]["side_b"]["catalog_summary"]["wine_count"] or 0)
    return PacketFeatures(
        case_id=case["case_id"],
        cluster=case["pattern_cluster"],
        expected_verdict=case["expected_verdict"],
        packet_id=packet["packet_id"],
        refs=refs,
        containment=str(lexical.get("containment") or "none"),
        shared_core_token_count=len(lexical.get("shared_core_tokens") or []),
        trigram_similarity=float(lexical.get("trigram_similarity") or 0.0),
        same_country=bool(geography.get("same_country")),
        same_region=bool(geography.get("same_region")),
        country_conflict=bool(risk.get("country_conflict")),
        shared_surname_split=bool(risk.get("shared_surname_split")),
        secondary_relationship_without_identity=bool(risk.get("secondary_relationship_without_identity")),
        holdco_or_product_tier=bool(risk.get("holdco_or_product_tier")),
        owner_or_operator_not_identity=bool(risk.get("owner_or_operator_not_identity")),
        exact_overlap_count=int(catalog.get("exact_overlap_count") or 0),
        has_subset_match="catalog_subset_match" in refs,
        has_portfolio_shape="catalog_portfolio_shape" in refs,
        anchor_overlap_count=len(catalog.get("anchor_overlap_examples") or []),
        rare_anchor_count=len(catalog.get("rare_anchor_wines") or []),
        wine_count_small=min(wine_count_a, wine_count_b),
        wine_count_large=max(wine_count_a, wine_count_b),
        candidate_family=extract_candidate_family(packet["evidence"]["pair"]),
    )


def zero_usage() -> dict:
    return {
        "prompt_tokens": 0,
        "completion_tokens": 0,
        "search_calls": 0,
        "cost_usd": 0.0,
    }


def build_merge_output(packet_id: str, proposal: Proposal) -> dict:
    return {
        "packet_id": packet_id,
        "verdict": "MERGE",
        "confidence": 0.71,
        "rule_ids": list(proposal.rule_ids),
        "reason": proposal.reason,
        "key_support_refs": list(proposal.support_refs),
        "key_contradiction_refs": list(proposal.contradiction_refs),
        "survivor_producer_id": None,
        "follow_up": None,
    }


def copy_control_row(control_row: dict, contender_id: str) -> dict:
    row = deepcopy(control_row)
    row["contender_id"] = contender_id
    row["timing_ms"] = 0
    row["usage"] = zero_usage()
    row["proof_action"] = "reused_control"
    return row


def fixed_safety_block_reason(features: PacketFeatures) -> str | None:
    refs = features.refs
    if (
        features.cluster == "11.4.h"
        and features.shared_surname_split
        and "catalog_exact_overlap" not in refs
        and "catalog_subset_match" not in refs
        and not features.same_region
    ):
        return "fixed_safety_base:shared_surname_without_catalog_or_region_bridge"
    if (
        features.cluster == "11.4.h"
        and features.secondary_relationship_without_identity
        and features.containment == "none"
    ):
        return "fixed_safety_base:secondary_relationship_without_name_bridge"
    if (
        features.cluster == "11.4.f"
        and features.exact_overlap_count == 1
        and "catalog_subset_match" in refs
        and features.containment == "none"
        and not features.secondary_relationship_without_identity
    ):
        return "fixed_safety_base:narrow_11_4_f_one_anchor_trap"
    return None


def candidate_cross_country_exact_name(features: PacketFeatures) -> bool:
    return (
        features.country_conflict
        and features.trigram_similarity == 1.0
        and not features.shared_surname_split
        and not features.secondary_relationship_without_identity
        and not features.holdco_or_product_tier
        and not features.owner_or_operator_not_identity
        and features.anchor_overlap_count == 0
        and features.rare_anchor_count == 0
        and features.wine_count_small <= 2
    )


def candidate_same_country_containment(features: PacketFeatures) -> bool:
    return (
        features.same_country
        and features.containment != "none"
        and features.has_subset_match
        and not features.shared_surname_split
        and not features.secondary_relationship_without_identity
        and not features.holdco_or_product_tier
        and not features.owner_or_operator_not_identity
    )


def candidate_shared_house(features: PacketFeatures) -> bool:
    return (
        features.same_country
        and features.shared_core_token_count == 2
        and features.has_portfolio_shape
        and features.containment == "none"
        and not features.shared_surname_split
        and not features.secondary_relationship_without_identity
        and not features.holdco_or_product_tier
        and not features.owner_or_operator_not_identity
    )


def candidate_generational_contained_overlap(features: PacketFeatures) -> bool:
    return (
        features.same_country
        and features.shared_surname_split
        and features.exact_overlap_count == 1
        and features.has_subset_match
        and features.containment != "none"
        and not features.secondary_relationship_without_identity
    )


def expanded_layered_router_proposal(features: PacketFeatures) -> Proposal | None:
    if candidate_cross_country_exact_name(features):
        return Proposal(
            signature="cross_country_exact_name_small_side",
            reason="Expanded family routing proposes MERGE because the packet shows an exact-name cross-country brand shape with a tiny secondary side and no extra ownership or surname-risk flags.",
            rule_ids=("11.1", "11.4.n"),
            support_refs=("geo_country_conflict", "lex_near_exact", "catalog_portfolio_shape"),
            contradiction_refs=("risk_sparse_official_evidence",),
        )
    if candidate_same_country_containment(features):
        return Proposal(
            signature="same_country_containment_subset_no_surname",
            reason="Expanded family routing proposes MERGE because the packet shows same-country containment plus a subset-style local catalog shape without the shared-surname or relationship traps that the fixed safety base treats as dangerous.",
            rule_ids=("11.1", "11.4.h"),
            support_refs=("geo_same_country", "lex_contains", "catalog_subset_match"),
            contradiction_refs=("risk_sparse_official_evidence",),
        )
    if candidate_shared_house(features):
        return Proposal(
            signature="same_country_shared_house_portfolio_shape",
            reason="Expanded family routing proposes MERGE because the pair shares a compact same-country house-name signature with portfolio-shape coherence and no extra ownership, product-tier, or shared-surname trap flags.",
            rule_ids=("11.1", "11.4.g"),
            support_refs=("geo_same_country", "lex_shared_core_tokens", "catalog_portfolio_shape"),
            contradiction_refs=("risk_sparse_official_evidence",),
        )
    return None


def signature_router_proposal(features: PacketFeatures) -> Proposal | None:
    if candidate_same_country_containment(features):
        return Proposal(
            signature="sig_country_contains_subset",
            reason="Signature routing proposes MERGE because the packet matches the same-country containment-plus-subset signature rather than the older rule-family routing frame.",
            rule_ids=("11.1", "11.4.h"),
            support_refs=("geo_same_country", "lex_contains", "catalog_subset_match"),
            contradiction_refs=("risk_sparse_official_evidence",),
        )
    if (
        features.same_country
        and features.shared_core_token_count == 2
        and features.has_portfolio_shape
        and features.containment == "none"
        and not features.same_region
        and not features.shared_surname_split
        and not features.secondary_relationship_without_identity
        and 0.4 <= features.trigram_similarity <= 0.7
    ):
        return Proposal(
            signature="sig_sparse_portfolio_shared_core",
            reason="Signature routing proposes MERGE because the pair falls into the sparse-official, shared-house, same-country portfolio signature that Session 9.8 identified as a repeated recovery shape.",
            rule_ids=("11.1", "11.4.g"),
            support_refs=("geo_same_country", "lex_shared_core_tokens", "catalog_portfolio_shape"),
            contradiction_refs=("risk_sparse_official_evidence",),
        )
    if candidate_cross_country_exact_name(features):
        return Proposal(
            signature="sig_cross_country_clean_exact_name",
            reason="Signature routing proposes MERGE because the pair matches the clean cross-country exact-name signature without the anchor-heavy or relationship-heavy variants that the proof subset treats as traps.",
            rule_ids=("11.1", "11.4.n"),
            support_refs=("geo_country_conflict", "lex_near_exact", "catalog_portfolio_shape"),
            contradiction_refs=("risk_sparse_official_evidence",),
        )
    return None


def merge_proposer_plus_veto_proposal(features: PacketFeatures) -> Proposal | None:
    base_proposal = expanded_layered_router_proposal(features)
    if base_proposal is not None:
        return base_proposal
    if candidate_generational_contained_overlap(features):
        return Proposal(
            signature="generational_contained_exact_overlap",
            reason="Merge-proposer stage proposes MERGE because the packet shows a same-country generational form with one exact overlap anchor plus explicit containment and subset structure, and the fixed safety base does not classify it as a one-anchor trap.",
            rule_ids=("11.1", "11.4.f"),
            support_refs=("geo_same_country", "catalog_exact_overlap", "catalog_subset_match"),
            contradiction_refs=("risk_shared_surname_split", "risk_sparse_official_evidence"),
        )
    return None


def evidence_digest_then_judge_proposal(features: PacketFeatures) -> Proposal | None:
    positive_signals = int(features.same_country) + int(features.same_region) + int(features.containment != "none")
    positive_signals += int(features.shared_core_token_count >= 2) + int(features.has_subset_match)
    positive_signals += int(features.exact_overlap_count > 0) + int(features.has_portfolio_shape)
    major_risks = int(features.shared_surname_split) + int(features.secondary_relationship_without_identity)
    major_risks += int(features.holdco_or_product_tier) + int(features.owner_or_operator_not_identity)

    if (
        candidate_cross_country_exact_name(features)
        and features.wine_count_small == 1
    ):
        return Proposal(
            signature="digest_cross_country_exact_clean",
            reason="Digest-then-judge proposes MERGE because the digest collapses to exact-name cross-country brand continuity with no extra risk flags and only a one-bottle secondary side.",
            rule_ids=("11.1", "11.4.n"),
            support_refs=("geo_country_conflict", "lex_near_exact", "catalog_portfolio_shape"),
            contradiction_refs=("risk_sparse_official_evidence",),
        )
    if major_risks == 0 and positive_signals >= 5 and features.shared_core_token_count <= 2:
        if features.containment != "none" and features.has_subset_match:
            return Proposal(
                signature="digest_clean_same_country_containment",
                reason="Digest-then-judge proposes MERGE because the digest shows dense same-country containment plus subset coherence without any of the major identity-risk flags.",
                rule_ids=("11.1", "11.4.h"),
                support_refs=("geo_same_country", "lex_contains", "catalog_subset_match"),
                contradiction_refs=("risk_sparse_official_evidence",),
            )
        if features.shared_core_token_count == 2 and features.has_portfolio_shape and not features.same_region:
            return Proposal(
                signature="digest_clean_same_country_shared_house",
                reason="Digest-then-judge proposes MERGE because the digest shows the compact shared-house same-country pattern without the trap signatures that Session 9.8 called out.",
                rule_ids=("11.1", "11.4.g"),
                support_refs=("geo_same_country", "lex_shared_core_tokens", "catalog_portfolio_shape"),
                contradiction_refs=("risk_sparse_official_evidence",),
            )
    if candidate_generational_contained_overlap(features):
        return Proposal(
            signature="digest_generational_contained_overlap",
            reason="Digest-then-judge proposes MERGE because the digest shows a contained generational same-country pattern with a real overlap anchor and no secondary-relationship contradiction.",
            rule_ids=("11.1", "11.4.f"),
            support_refs=("geo_same_country", "catalog_exact_overlap", "catalog_subset_match"),
            contradiction_refs=("risk_shared_surname_split", "risk_sparse_official_evidence"),
        )
    return None


METHOD_SPECS = {
    "expanded_layered_router_v1": {
        "label": "Expanded layered router",
        "method_class": "family expansion",
        "what_changes": "Keeps the Session 9.7 control fixed but adds broader positive-family routes for 11.1 / 11.4.g / cross-country exact-name shapes.",
        "why_credible": "Directly tests the Session 9.9 thesis that the blocker is broader positive control, not one more narrow routed-family tweak.",
        "main_safety_risk": "Could reopen sparse-official shared-house traps if the wider family routing is too permissive.",
        "proposal_fn": expanded_layered_router_proposal,
    },
    "signature_router_v1": {
        "label": "Signature router",
        "method_class": "signature routing",
        "what_changes": "Routes by visible packet signature shape instead of the old rule-family label.",
        "why_credible": "Session 9.8 argued that the remaining misses repeat as packet signatures at least as much as they repeat as rule families.",
        "main_safety_risk": "Loose signature buckets could still collapse distinct skip controls into one optimistic merge signature.",
        "proposal_fn": signature_router_proposal,
    },
    "merge_proposer_plus_veto_v1": {
        "label": "Merge proposer + fixed veto",
        "method_class": "optimistic proposer with fixed backstop",
        "what_changes": "Adds a broader positive proposer on top of the Session 9.7 control, then lets the fixed safety base block only the known trap signatures.",
        "why_credible": "This is the most direct implementation of the Session 9.9 separation between optimism and the frozen safety backstop.",
        "main_safety_risk": "If the proposer overgeneralizes, the fixed safety base might not catch every new trap outside the original 9.7 envelope.",
        "proposal_fn": merge_proposer_plus_veto_proposal,
    },
    "evidence_digest_then_judge_v1": {
        "label": "Evidence digest then judge",
        "method_class": "digest-based positive control",
        "what_changes": "Summarizes the visible evidence into a tighter digest before allowing any control override.",
        "why_credible": "Tests whether some of the remaining misses are evidence-presentation failures rather than missing policy routes.",
        "main_safety_risk": "Digest simplification may still collapse meaningfully different signatures into the same optimistic bucket.",
        "proposal_fn": evidence_digest_then_judge_proposal,
    },
}


def build_case_result(case: dict, predicted_verdict: str) -> str:
    expected = case["expected_verdict"]
    if expected == "MERGE" and predicted_verdict == "MERGE":
        return "true_merge"
    if expected == "MERGE" and predicted_verdict == "SKIP":
        return "hard_missed_merge"
    if expected == "MERGE" and predicted_verdict == "FLAGGED":
        return "soft_missed_merge"
    if expected == "SKIP" and predicted_verdict == "MERGE":
        return "false_merge"
    if expected == "SKIP" and predicted_verdict == "SKIP":
        return "true_skip"
    return "safe_flag"


def summarize_rows(
    *,
    proof_case_ids: list[str],
    cases_by_id: dict[str, dict],
    rows_by_case: dict[str, dict],
    control_rows_by_case: dict[str, dict],
) -> dict:
    counts = {
        "true_merge": 0,
        "hard_missed_merge": 0,
        "soft_missed_merge": 0,
        "false_merge": 0,
        "true_skip": 0,
        "safe_flag": 0,
    }
    recovered_case_ids: list[str] = []
    false_merge_case_ids: list[str] = []
    lost_control_win_case_ids: list[str] = []
    changed_case_ids: list[str] = []
    blind_core_recovered_case_ids: list[str] = []

    for case_id in proof_case_ids:
        predicted = rows_by_case[case_id]["normalized_output"]["verdict"]
        case = cases_by_id[case_id]
        counts[build_case_result(case, predicted)] += 1
        control_verdict = control_rows_by_case[case_id]["normalized_output"]["verdict"]
        if predicted != control_verdict:
            changed_case_ids.append(case_id)
        if case["expected_verdict"] == "MERGE" and control_verdict != "MERGE" and predicted == "MERGE":
            recovered_case_ids.append(case_id)
            if case_id in BLIND_CORE_BLOCKER_CASE_IDS:
                blind_core_recovered_case_ids.append(case_id)
        if case["expected_verdict"] == "SKIP" and predicted == "MERGE":
            false_merge_case_ids.append(case_id)
        if case["expected_verdict"] == "MERGE" and control_verdict == "MERGE" and predicted != "MERGE":
            lost_control_win_case_ids.append(case_id)

    total_cases = len(proof_case_ids)
    exact_matches = counts["true_merge"] + counts["true_skip"]
    expected_merges = sum(1 for case_id in proof_case_ids if cases_by_id[case_id]["expected_verdict"] == "MERGE")
    expected_skips = total_cases - expected_merges

    return {
        "counts": counts,
        "rates": {
            "exact_verdict_accuracy": round(exact_matches / total_cases, 4) if total_cases else 0.0,
            "merge_capture_rate": round(counts["true_merge"] / expected_merges, 4) if expected_merges else 0.0,
            "false_merge_rate": round(counts["false_merge"] / expected_skips, 4) if expected_skips else 0.0,
            "flag_rate_total": round((counts["soft_missed_merge"] + counts["safe_flag"]) / total_cases, 4)
            if total_cases
            else 0.0,
        },
        "recovered_case_ids": recovered_case_ids,
        "blind_core_recovered_case_ids": blind_core_recovered_case_ids,
        "false_merge_case_ids": false_merge_case_ids,
        "lost_control_win_case_ids": lost_control_win_case_ids,
        "changed_case_ids": changed_case_ids,
    }


def rows_decision_vector(proof_case_ids: list[str], rows_by_case: dict[str, dict]) -> tuple[str, ...]:
    return tuple(rows_by_case[case_id]["normalized_output"]["verdict"] for case_id in proof_case_ids)


def build_method_rows(
    *,
    contender_id: str,
    proof_case_ids: list[str],
    control_rows_by_case: dict[str, dict],
    features_by_case: dict[str, PacketFeatures],
) -> tuple[list[dict], dict[str, dict]]:
    rows: list[dict] = []
    rows_by_case: dict[str, dict] = {}
    proposal_fn = METHOD_SPECS[contender_id]["proposal_fn"]

    for case_id in proof_case_ids:
        control_row = control_rows_by_case[case_id]
        row = copy_control_row(control_row, contender_id)
        features = features_by_case[case_id]
        proposal = None
        safety_block = None
        if control_row["normalized_output"]["verdict"] != "MERGE":
            proposal = proposal_fn(features)
            if proposal is not None:
                safety_block = fixed_safety_block_reason(features)
                if safety_block is None:
                    row["normalized_output"] = build_merge_output(features.packet_id, proposal)
                    row["proof_action"] = "proposal_promoted_to_merge"
                    row["proof_signature"] = proposal.signature
                else:
                    row["proof_action"] = "proposal_blocked_by_fixed_safety_base"
                    row["proof_signature"] = proposal.signature
                    row["proof_block_reason"] = safety_block
        rows.append(row)
        rows_by_case[case_id] = row
    return rows, rows_by_case


def render_case_id_list(case_ids: list[str]) -> str:
    return ", ".join(f"`{case_id}`" for case_id in case_ids) if case_ids else "none"


def build_case_table(
    *,
    proof_case_ids: list[str],
    cases_by_id: dict[str, dict],
    control_rows_by_case: dict[str, dict],
    contender_rows_by_case: dict[str, dict],
) -> list[str]:
    lines = []
    lines.append("| Case | Group | Expected | Control | Contender |")
    lines.append("|---|---|---|---|---|")
    group_by_case = {
        case_id: group["label"]
        for group in PROOF_GROUPS
        for case_id in group["case_ids"]
    }
    for case_id in proof_case_ids:
        control_verdict = control_rows_by_case[case_id]["normalized_output"]["verdict"]
        contender_verdict = contender_rows_by_case[case_id]["normalized_output"]["verdict"]
        case = cases_by_id[case_id]
        lines.append(
            f"| `{case_id}` | {group_by_case[case_id]} | {case['expected_verdict']} | {control_verdict} | {contender_verdict} |"
        )
    return lines


def render_markdown(report: dict) -> str:
    lines: list[str] = []
    lines.append("# Session 9.10 - method bakeoff proof subset")
    lines.append("")
    lines.append(f"- Generated: {report['generated_at']}")
    lines.append(f"- Run name: `{report['run_name']}`")
    lines.append(f"- Frozen control: `{CONTROL_RUN_NAME}` / `{CONTROL_CONTENDER_ID}`")
    lines.append(f"- Proof subset size: `{report['subset']['case_count']}`")
    lines.append(f"- New model spend: `${report['new_model_spend_usd']:.2f}`")
    lines.append("")
    lines.append("## Goal")
    lines.append("")
    lines.append(
        "Test the broader Session 9.9 method classes cheaply first, on a bounded subset built from the actual blocker and trap zones, before any later decision about a full 152-case rerun."
    )
    lines.append("")
    lines.append("## Inputs")
    lines.append("")
    for path in report["inputs"]:
        lines.append(f"- `{path}`")
    lines.append("")
    lines.append("## Implemented contenders")
    lines.append("")
    lines.append("| Contender | Method class | What changes | Why it is credible | Main safety risk |")
    lines.append("|---|---|---|---|---|")
    for contender_id in METHOD_ORDER:
        meta = METHOD_SPECS[contender_id]
        lines.append(
            f"| `{contender_id}` | {meta['method_class']} | {meta['what_changes']} | {meta['why_credible']} | {meta['main_safety_risk']} |"
        )
    lines.append("")
    lines.append("## Proof subset composition")
    lines.append("")
    for group in report["subset"]["groups"]:
        lines.append(f"- **{group['label']} ({len(group['case_ids'])})**: {group['why']}")
        lines.append(f"  {render_case_id_list(group['case_ids'])}")
    lines.append("")
    lines.append(
        f"- **Blind-core production blockers (5)**: {render_case_id_list(report['subset']['blind_core_blocker_case_ids'])}"
    )
    lines.append("")
    lines.append("## Frozen control on the proof subset")
    lines.append("")
    control = report["control"]
    lines.append(
        f"The frozen Session 9.7 control carries `{control['summary']['counts']['false_merge']}` false merges, "
        f"`{len(control['summary']['recovered_case_ids'])}` recoveries relative to itself by definition, and "
        f"`{control['summary']['counts']['hard_missed_merge'] + control['summary']['counts']['soft_missed_merge']}` missed merges across this proof slice."
    )
    lines.append("")
    lines.append("## Proof results vs Session 9.7 control")
    lines.append("")
    lines.append("| Contender | Total recoveries vs control | Blind-core blocker recoveries | False merges on proof subset | Lost current wins | Verdict |")
    lines.append("|---|---:|---:|---:|---:|---|")
    for contender in report["contenders"]:
        summary = contender["summary"]
        verdict = "survivor"
        if contender.get("redundant_with"):
            verdict = f"redundant with `{contender['redundant_with']}`"
        elif not contender["survives_proof"]:
            verdict = "eliminated"
        lines.append(
            f"| `{contender['contender_id']}` | {len(summary['recovered_case_ids'])} | {len(summary['blind_core_recovered_case_ids'])} | "
            f"{len(summary['false_merge_case_ids'])} | {len(summary['lost_control_win_case_ids'])} | {verdict} |"
        )
    lines.append("")
    for contender in report["contenders"]:
        summary = contender["summary"]
        lines.append(f"### `{contender['contender_id']}`")
        lines.append("")
        lines.append(f"- Recoveries vs control: {render_case_id_list(summary['recovered_case_ids'])}")
        lines.append(f"- Blind-core blocker recoveries: {render_case_id_list(summary['blind_core_recovered_case_ids'])}")
        lines.append(f"- False merges on proof subset: {render_case_id_list(summary['false_merge_case_ids'])}")
        lines.append(f"- Lost current wins: {render_case_id_list(summary['lost_control_win_case_ids'])}")
        if contender.get("redundant_with"):
            lines.append(f"- Redundancy note: decision vector matched `{contender['redundant_with']}` exactly on this proof subset.")
        lines.append(
            f"- Kill criteria: false merges = {'pass' if contender['criteria']['no_false_merges'] else 'fail'}; "
            f"blind-core recoveries >= 2 = {'pass' if contender['criteria']['blind_core_recoveries_gte_2'] else 'fail'}; "
            f"no hold-set regressions = {'pass' if contender['criteria']['no_lost_control_wins'] else 'fail'}."
        )
        lines.append("")
    lines.append("## Downselect")
    lines.append("")
    if report["downselected_contenders"]:
        lines.append(
            "Recommended contenders for any later full rerun: "
            + ", ".join(f"`{contender_id}`" for contender_id in report["downselected_contenders"])
            + "."
        )
        if report["redundant_contenders"]:
            lines.append(
                "Dropped as redundant on this proof slice: "
                + ", ".join(f"`{item['contender_id']}` -> `{item['redundant_with']}`" for item in report["redundant_contenders"])
                + "."
            )
    else:
        lines.append("No contender survived cleanly, so there is nothing honest to downselect for a full rerun.")
    lines.append("")
    lines.append("## Recommendation")
    lines.append("")
    lines.append(f"- Status: `{report['recommendation']['status']}`")
    lines.append(f"- Reason: {report['recommendation']['reason']}")
    lines.append("")
    lines.append("## Case matrix")
    lines.append("")
    for contender in report["contenders"]:
        lines.append(f"### `{contender['contender_id']}` matrix")
        lines.append("")
        lines.extend(contender["case_table"])
        lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(description="Run the Session 9.10 method bakeoff proof subset")
    parser.add_argument("--benchmark", type=Path, default=DEFAULT_BENCHMARK)
    parser.add_argument("--packet-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_RUN_ROOT)
    parser.add_argument("--run-name", default=RUN_NAME_DEFAULT)
    parser.add_argument("--memo-path", type=Path, default=DEFAULT_MEMO_PATH)
    args = parser.parse_args()

    benchmark_payload = load_benchmark_payload(args.benchmark)
    cases_by_id = {case["case_id"]: case for case in benchmark_payload["cases"]}
    proof_case_ids = validate_groups(cases_by_id)

    full_packets, _, packet_validation = ensure_visible_packets(
        args.packet_dir,
        args.benchmark,
        force_rebuild=False,
    )
    packet_lookup = {packet["packet_id"]: packet for packet in full_packets}
    control_rows_by_case = load_rows_by_case(CONTROL_NORMALIZED)

    features_by_case: dict[str, PacketFeatures] = {}
    for case_id in proof_case_ids:
        case = cases_by_id[case_id]
        packet_id = f"producer_pair_{case['pair_id']}_v2"
        features_by_case[case_id] = build_features(case, packet_lookup[packet_id])

    normalized_dir = args.output_root / "normalized" / args.run_name
    scored_dir = args.output_root / "scored"
    normalized_dir.mkdir(parents=True, exist_ok=True)
    scored_dir.mkdir(parents=True, exist_ok=True)

    control_rows: list[dict] = []
    control_subset_rows_by_case: dict[str, dict] = {}
    for case_id in proof_case_ids:
        row = copy_control_row(control_rows_by_case[case_id], "session9_7_control")
        control_rows.append(row)
        control_subset_rows_by_case[case_id] = row
    write_jsonl(normalized_dir / "session9_7_control.jsonl", control_rows)

    control_summary = summarize_rows(
        proof_case_ids=proof_case_ids,
        cases_by_id=cases_by_id,
        rows_by_case=control_subset_rows_by_case,
        control_rows_by_case=control_subset_rows_by_case,
    )

    contenders: list[dict] = []
    decision_vectors: dict[tuple[str, ...], str] = {}
    redundant_contenders: list[dict] = []

    for contender_id in METHOD_ORDER:
        rows, rows_by_case = build_method_rows(
            contender_id=contender_id,
            proof_case_ids=proof_case_ids,
            control_rows_by_case=control_rows_by_case,
            features_by_case=features_by_case,
        )
        write_jsonl(normalized_dir / f"{contender_id}.jsonl", rows)
        summary = summarize_rows(
            proof_case_ids=proof_case_ids,
            cases_by_id=cases_by_id,
            rows_by_case=rows_by_case,
            control_rows_by_case=control_subset_rows_by_case,
        )
        criteria = {
            "no_false_merges": len(summary["false_merge_case_ids"]) == 0,
            "blind_core_recoveries_gte_2": len(summary["blind_core_recovered_case_ids"]) >= 2,
            "no_lost_control_wins": len(summary["lost_control_win_case_ids"]) == 0,
        }
        survives_proof = all(criteria.values())
        vector = rows_decision_vector(proof_case_ids, rows_by_case)
        redundant_with = None
        if survives_proof and vector in decision_vectors:
            redundant_with = decision_vectors[vector]
            redundant_contenders.append(
                {"contender_id": contender_id, "redundant_with": redundant_with}
            )
        elif survives_proof:
            decision_vectors[vector] = contender_id

        contender_report = {
            "contender_id": contender_id,
            "label": METHOD_SPECS[contender_id]["label"],
            "method_class": METHOD_SPECS[contender_id]["method_class"],
            "summary": summary,
            "criteria": criteria,
            "survives_proof": survives_proof,
            "redundant_with": redundant_with,
            "case_table": build_case_table(
                proof_case_ids=proof_case_ids,
                cases_by_id=cases_by_id,
                control_rows_by_case=control_subset_rows_by_case,
                contender_rows_by_case=rows_by_case,
            ),
        }
        contenders.append(contender_report)

    ranked_survivors = [
        contender
        for contender in contenders
        if contender["survives_proof"] and not contender.get("redundant_with")
    ]
    ranked_survivors.sort(
        key=lambda item: (
            -len(item["summary"]["blind_core_recovered_case_ids"]),
            -len(item["summary"]["recovered_case_ids"]),
            len(item["summary"]["lost_control_win_case_ids"]),
            METHOD_ORDER.index(item["contender_id"]),
        )
    )
    downselected_contenders = [item["contender_id"] for item in ranked_survivors[:3]]

    if downselected_contenders:
        recommendation = {
            "status": "proceed_to_full_method_bakeoff",
            "reason": (
                "At least one broader method class survived the trap-heavy proof subset with zero false merges, "
                "no hold-set regressions, and at least two blind-core blocker recoveries. The next honest step is "
                "a capped full 152-case rerun using only the downselected survivors."
            ),
        }
    else:
        recommendation = {
            "status": "freeze_at_session9_7",
            "reason": (
                "No contender survived the proof subset cleanly enough to justify a full rerun, so Sprint 6 should "
                "freeze at the stronger Session 9.7 fallback artifact."
            ),
        }

    report = {
        "run_name": args.run_name,
        "generated_at": datetime.now().astimezone().isoformat(timespec="seconds"),
        "inputs": INPUT_PATHS,
        "new_model_spend_usd": 0.0,
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
        "control": {
            "contender_id": "session9_7_control",
            "summary": control_summary,
        },
        "contenders": contenders,
        "redundant_contenders": redundant_contenders,
        "downselected_contenders": downselected_contenders,
        "recommendation": recommendation,
    }

    report_json_path = scored_dir / f"{args.run_name}.json"
    report_md_path = scored_dir / f"{args.run_name}.md"
    report_manifest_path = scored_dir / f"{args.run_name}_manifest.json"
    write_json(report_json_path, report)
    memo_text = render_markdown(report)
    report_md_path.write_text(memo_text, encoding="utf-8")
    args.memo_path.write_text(memo_text, encoding="utf-8")
    write_json(
        report_manifest_path,
        {
            "run_name": args.run_name,
            "control_run_name": CONTROL_RUN_NAME,
            "control_contender_id": CONTROL_CONTENDER_ID,
            "proof_case_ids": proof_case_ids,
            "normalized": {
                "session9_7_control": str(normalized_dir / "session9_7_control.jsonl"),
                **{
                    contender_id: str(normalized_dir / f"{contender_id}.jsonl")
                    for contender_id in METHOD_ORDER
                },
            },
            "outputs": {
                "report_json": str(report_json_path),
                "report_md": str(report_md_path),
                "memo_md": str(args.memo_path),
            },
        },
    )

    print(f"Completed method bakeoff proof subset: {args.run_name}")
    print(f"Report JSON: {report_json_path}")
    print(f"Memo: {args.memo_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
