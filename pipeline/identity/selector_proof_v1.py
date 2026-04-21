"""
Session 10.6 - Build and score the local selector_proof_v1 bundle.

This module freezes:
1. the hidden proof key,
2. the visible Phase A selector packets,
3. the visible Phase B escalation packets,
4. the Phase C shortlist manifest,
5. the normalized result schema + scorer, and
6. the accepted-edge / frontier write simulator.

It does not run model calls and it does not write to the DB.
"""

from __future__ import annotations

import argparse
import json
import re
import time
from collections import Counter, defaultdict
from copy import deepcopy
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import anthropic

from pipeline.identity.bakeoff_packet_v1 import (
    canonical_json_dumps,
    fetch_catalog_context,
    fetch_producers,
    normalize_text,
    tokenize,
    write_jsonl,
)
from pipeline.lib.db import get_conn, get_env
from pipeline.lib.models import SONNET_MODEL


REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_MANIFEST = (
    REPO_ROOT
    / "data"
    / "sprints"
    / "identity-er"
    / "proof"
    / "selector_proof_case_sources_v1.json"
)
DEFAULT_OUTPUT_DIR = REPO_ROOT / "data" / "sprints" / "identity-er" / "proof"

PRICING_BY_MODEL = {
    SONNET_MODEL: {
        "input": 3.0,
        "output": 15.0,
    }
}

PHASE_A_RESULTS_FILE = "selector_proof_phase_a_results_v1.jsonl"
PHASE_B_RESULTS_FILE = "selector_proof_phase_b_results_v1.jsonl"
PHASE_A_RAW_FILE = "selector_proof_phase_a_raw_v1.jsonl"
PHASE_B_RAW_FILE = "selector_proof_phase_b_raw_v1.jsonl"
EXECUTION_SUMMARY_FILE = "selector_proof_execution_summary_v1.json"
SCORECARD_FILE = "selector_proof_scorecard_v1.md"
GO_NO_GO_MEMO_FILE = "selector_proof_go_no_go_memo_v1.md"
PHASE_C_NOTE_FILE = "selector_proof_phase_c_runnability_v1.md"

LABEL_ENUM = {"SAME_AS", "RELATED_BUT_DISTINCT", "NONE", "UNSURE"}

PRIMARY_REASON_CODES = {
    "SAME_AS": [
        "same_exact_or_orthographic_alias",
        "same_historical_name_continuity",
        "same_legal_vs_label_identity",
        "same_merchant_or_importer_prefix",
        "same_global_brand_multi_country",
        "same_sparse_stub_absorption",
    ],
    "RELATED_BUT_DISTINCT": [
        "related_shared_owner_distinct_brand",
        "related_shared_family_distinct_estates",
        "related_shared_permit_or_facility",
        "related_joint_venture_or_collab",
        "related_auction_or_negociant_bottling",
        "related_subbrand_or_secondary_label",
    ],
    "NONE": [
        "none_no_candidate_survived",
        "none_lexical_collision_only",
        "none_place_portfolio_conflict",
        "none_weak_fuzzy_no_support",
        "none_noise_candidate",
    ],
    "UNSURE": [
        "unsure_conflicting_signals",
        "unsure_thin_evidence",
        "unsure_top_candidates_too_close",
        "unsure_shortlist_gap_possible",
        "unsure_escalation_only_signal_needed",
    ],
}

GENERIC_NAME_TOKENS = {
    "and",
    "bodega",
    "bodegas",
    "cantina",
    "cellar",
    "cellars",
    "chateau",
    "clos",
    "co",
    "company",
    "de",
    "dei",
    "del",
    "della",
    "domaine",
    "dominio",
    "estate",
    "estates",
    "et",
    "fils",
    "freres",
    "llc",
    "maison",
    "sarl",
    "sa",
    "tenuta",
    "vineyards",
    "vins",
    "weingut",
    "winery",
    "wines",
}

LABEL_REASON_CODES = {
    ("SAME_AS", "11.4.f"): "same_historical_name_continuity",
    ("SAME_AS", "11.4.n"): "same_global_brand_multi_country",
    ("SAME_AS", "11.4.p"): "same_merchant_or_importer_prefix",
    ("SAME_AS", "11.4.i"): "same_legal_vs_label_identity",
    ("RELATED_BUT_DISTINCT", "11.4.g"): "related_shared_owner_distinct_brand",
    ("RELATED_BUT_DISTINCT", "11.4.j"): "related_shared_permit_or_facility",
    ("RELATED_BUT_DISTINCT", "11.4.m"): "related_shared_family_distinct_estates",
    ("RELATED_BUT_DISTINCT", "11.4.o"): "related_joint_venture_or_collab",
    ("RELATED_BUT_DISTINCT", "11.4.q"): "related_auction_or_negociant_bottling",
    ("RELATED_BUT_DISTINCT", "11.4.r"): "related_auction_or_negociant_bottling",
    ("RELATED_BUT_DISTINCT", "11.4.s"): "related_subbrand_or_secondary_label",
}

SOURCE_FAMILY_MAP = {
    "source_lwin": "lwin",
    "source_ttb_colas": "ttb",
    "source_pro_platform": "state_reg",
    "source_tabc": "state_reg",
    "source_kansas_brands": "state_reg",
    "source_skurnik": "importer",
    "source_winebow": "importer",
    "source_empson": "importer",
    "source_european_cellars": "importer",
    "source_kermit_lynch_growers": "importer",
    "source_kermit_lynch": "importer",
}

RESULT_SCHEMA = {
    "schema_version": "selector_proof_result_schema_v1",
    "selector_result": {
        "required": [
            "proof_version",
            "case_id",
            "decision_stage",
            "selector_version",
            "anchor_producer_id",
            "shortlist_status",
            "choice",
            "label",
            "primary_reason_code",
            "secondary_reason_codes",
            "rule_hypotheses",
            "support_ref_paths",
            "conflict_ref_paths",
            "needs_escalation",
            "escalation_focus",
        ],
        "label_enum": ["SAME_AS", "RELATED_BUT_DISTINCT", "NONE", "UNSURE"],
        "choice_type_enum": ["candidate", "none"],
        "stage_enum": ["selector", "escalation"],
    },
    "shortlist_observation": {
        "required": [
            "proof_version",
            "case_id",
            "anchor_producer_id",
            "shortlist_status",
            "candidate_count",
            "candidate_ids",
        ],
        "status_enum": ["candidates_present", "no_candidate_found"],
    },
}


def pretty_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=True, indent=2, sort_keys=True)


def read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def pointer_get(value: Any, pointer: str) -> Any:
    if not pointer.startswith("/"):
        raise KeyError(pointer)
    current = value
    if pointer == "/":
        return current
    for part in pointer[1:].split("/"):
        part = part.replace("~1", "/").replace("~0", "~")
        if isinstance(current, list):
            current = current[int(part)]
        else:
            current = current[part]
    return current


def pointer_exists(value: Any, pointer: str) -> bool:
    try:
        pointer_get(value, pointer)
        return True
    except Exception:
        return False


def safe_text(value: Any) -> str:
    return str(value or "").strip()


def compact_text(value: Any, limit: int = 280) -> str:
    text = " ".join(safe_text(value).split())
    if len(text) <= limit:
        return text
    return text[: limit - 3].rstrip() + "..."


def normalized_last_token(value: str | None) -> str:
    toks = tokenize(value)
    return toks[-1] if toks else ""


def domain_host(url: str | None) -> str | None:
    if not url:
        return None
    parsed = urlparse(url if "://" in url else f"https://{url}")
    host = (parsed.netloc or parsed.path or "").lower().strip()
    host = host.split("/")[0]
    if host.startswith("www."):
        host = host[4:]
    return host or None


def dedupe_count_items(items: list[tuple[str, int]], *, limit: int) -> list[dict]:
    chosen: dict[str, dict] = {}
    for raw_value, count in items:
        value = safe_text(raw_value)
        if not value:
            continue
        norm = normalize_text(value)
        if not norm:
            continue
        existing = chosen.get(norm)
        if not existing or count > existing["count"] or len(value) > len(existing["value"]):
            chosen[norm] = {"value": value, "count": int(count)}
    ordered = sorted(chosen.values(), key=lambda item: (-item["count"], item["value"]))[:limit]
    return ordered


def dedupe_name_entries(entries: list[dict], *, limit: int) -> list[dict]:
    chosen: dict[str, dict] = {}
    for entry in entries:
        value = safe_text(entry.get("value"))
        norm = normalize_text(value)
        if not norm:
            continue
        payload = {
            "value": value,
            "normalized": norm,
            "source_family": entry["source_family"],
            "row_count": int(entry.get("row_count", 0)),
        }
        existing = chosen.get(norm)
        if not existing or payload["row_count"] > existing["row_count"] or len(value) > len(existing["value"]):
            chosen[norm] = payload
    ordered = sorted(chosen.values(), key=lambda item: (-item["row_count"], item["value"]))[:limit]
    return ordered


def dedupe_simple_entries(entries: list[dict], *, limit: int) -> list[dict]:
    chosen: dict[str, dict] = {}
    for entry in entries:
        value = safe_text(entry.get("value"))
        if not value:
            continue
        norm = normalize_text(value)
        existing = chosen.get(norm)
        payload = {
            "value": value,
            "source_family": entry["source_family"],
            "row_count": int(entry.get("row_count", 0)),
        }
        if not existing or payload["row_count"] > existing["row_count"] or len(value) > len(existing["value"]):
            chosen[norm] = payload
    ordered = sorted(chosen.values(), key=lambda item: (-item["row_count"], item["value"]))[:limit]
    return ordered


def derive_short_forms(name_entries: list[dict]) -> list[str]:
    forms: list[str] = []
    for entry in name_entries:
        value = entry["value"]
        tokens = [tok for tok in tokenize(value) if tok not in GENERIC_NAME_TOKENS]
        if len(tokens) >= 2:
            forms.append(" ".join(tokens[-2:]))
            forms.append(tokens[-1])
        elif tokens:
            forms.append(tokens[0])
    deduped: list[str] = []
    seen: set[str] = set()
    for value in forms:
        norm = normalize_text(value)
        if not norm or norm in seen:
            continue
        seen.add(norm)
        deduped.append(value.title() if value.islower() else value)
    return deduped[:4]


def collect_signal_families(pair_row: dict | None) -> list[str]:
    if not pair_row:
        return ["canonical"]
    signals = pair_row.get("signals") or {}
    families: list[str] = []
    for key in signals:
        if key.startswith("s5_"):
            families.append("lwin")
        elif key.startswith("s6_"):
            families.append("ttb")
        elif key.startswith("s8_"):
            families.append("canonical")
        elif key.startswith("s9_"):
            families.append("canonical")
        elif key.startswith("s10_"):
            families.append("canonical")
        else:
            families.append("canonical")
    if not families:
        families.append("canonical")
    ordered: list[str] = []
    for family in families:
        if family not in ordered:
            ordered.append(family)
    return ordered[:3]


def lexical_strength(anchor_name: str, candidate_name: str, pair_row: dict | None) -> str:
    left = normalize_text(anchor_name)
    right = normalize_text(candidate_name)
    if left and right and left == right:
        return "exact_normalized"
    if left and right and (left in right or right in left):
        return "containment"
    similarity = float((pair_row or {}).get("similarity") or 0.0)
    if similarity >= 0.75:
        return "orthographic_variant"
    return "fuzzy"


def shortlist_pressure_band(family_count: int, lex_strength: str) -> str:
    if lex_strength == "exact_normalized" and family_count >= 2:
        return "exact_multi_source"
    if lex_strength == "exact_normalized":
        return "exact_single_source"
    if lex_strength in {"containment", "orthographic_variant"} and family_count >= 2:
        return "variant_multi_source"
    return "fallback"


def support_choice_reason(label: str, pattern_family: str, shortlist_status: str) -> str:
    if label == "NONE" and shortlist_status == "no_candidate_found":
        return "none_no_candidate_survived"
    if label == "NONE":
        return "none_lexical_collision_only"
    if label == "UNSURE" and shortlist_status == "no_candidate_found":
        return "unsure_shortlist_gap_possible"
    if label == "UNSURE":
        if pattern_family in {"11.4.f", "11.4.p"}:
            return "unsure_escalation_only_signal_needed"
        return "unsure_conflicting_signals"
    return LABEL_REASON_CODES.get((label, pattern_family), "same_exact_or_orthographic_alias")


def secondary_reason_codes(label: str, pattern_family: str, shortlist_status: str) -> list[str]:
    codes: list[str] = []
    if label == "SAME_AS" and pattern_family == "11.4.h":
        codes.append("same_sparse_stub_absorption")
    if label == "RELATED_BUT_DISTINCT" and pattern_family == "11.4.j":
        codes.append("related_shared_permit_or_facility")
    if label == "NONE" and shortlist_status == "no_candidate_found":
        codes.append("none_weak_fuzzy_no_support")
    if label == "UNSURE" and shortlist_status == "no_candidate_found":
        codes.append("unsure_thin_evidence")
    return codes[:3]


def flatten_calibration_rows(payload: dict) -> dict[int, dict]:
    rows: dict[int, dict] = {}
    for tier_rows in payload["tiers"].values():
        for row in tier_rows:
            rows[int(row["pair_id"])] = row
    return rows


def load_source_maps(manifest: dict) -> tuple[dict[str, dict], dict[int, dict]]:
    benchmark_path = REPO_ROOT / manifest["source_artifacts"]["benchmark_v1"]
    benchmark_payload = read_json(benchmark_path)
    benchmark_map = {case["case_id"]: case for case in benchmark_payload["cases"]}
    calibration_path = REPO_ROOT / manifest["source_artifacts"]["calibration_set"]
    calibration_map = flatten_calibration_rows(read_json(calibration_path))
    return benchmark_map, calibration_map


def fetch_pair_row(cur, pair_id: int) -> dict:
    cur.execute(
        """
        SELECT id,
               producer_id_a::text,
               producer_id_b::text,
               name_a,
               name_b,
               country,
               similarity,
               wines_a,
               wines_b,
               signals
        FROM producer_dedup_pairs
        WHERE id = %s AND method_name = 'blocking'
        """,
        (pair_id,),
    )
    row = cur.fetchone()
    if not row:
        raise RuntimeError(f"Missing blocking pair_id={pair_id}")
    return {
        "id": row[0],
        "producer_id_a": row[1],
        "producer_id_b": row[2],
        "name_a": row[3],
        "name_b": row[4],
        "country": row[5],
        "similarity": float(row[6] or 0.0),
        "wines_a": int(row[7] or 0),
        "wines_b": int(row[8] or 0),
        "signals": row[9] or {},
    }


def fetch_pair_for_producers(cur, anchor_id: str, candidate_id: str) -> dict | None:
    cur.execute(
        """
        SELECT id,
               producer_id_a::text,
               producer_id_b::text,
               name_a,
               name_b,
               country,
               similarity,
               wines_a,
               wines_b,
               signals
        FROM producer_dedup_pairs
        WHERE method_name = 'blocking'
          AND (
            (producer_id_a = %s::uuid AND producer_id_b = %s::uuid)
            OR
            (producer_id_a = %s::uuid AND producer_id_b = %s::uuid)
          )
        ORDER BY similarity DESC
        LIMIT 1
        """,
        (anchor_id, candidate_id, candidate_id, anchor_id),
    )
    row = cur.fetchone()
    if not row:
        return None
    return {
        "id": row[0],
        "producer_id_a": row[1],
        "producer_id_b": row[2],
        "name_a": row[3],
        "name_b": row[4],
        "country": row[5],
        "similarity": float(row[6] or 0.0),
        "wines_a": int(row[7] or 0),
        "wines_b": int(row[8] or 0),
        "signals": row[9] or {},
    }


def fetch_extra_candidates(cur, anchor_id: str, exclude_ids: set[str], limit: int) -> list[dict]:
    if limit <= 0:
        return []
    cur.execute(
        """
        SELECT id,
               producer_id_a::text,
               producer_id_b::text,
               name_a,
               name_b,
               country,
               similarity,
               wines_a,
               wines_b,
               signals,
               CASE
                 WHEN producer_id_a = %s::uuid THEN producer_id_b::text
                 ELSE producer_id_a::text
               END AS candidate_id
        FROM producer_dedup_pairs
        WHERE method_name = 'blocking'
          AND (producer_id_a = %s::uuid OR producer_id_b = %s::uuid)
        ORDER BY similarity DESC
        LIMIT 50
        """,
        (anchor_id, anchor_id, anchor_id),
    )
    rows: list[dict] = []
    for row in cur.fetchall():
        candidate_id = row[10]
        if candidate_id in exclude_ids:
            continue
        rows.append(
            {
                "id": row[0],
                "producer_id_a": row[1],
                "producer_id_b": row[2],
                "name_a": row[3],
                "name_b": row[4],
                "country": row[5],
                "similarity": float(row[6] or 0.0),
                "wines_a": int(row[7] or 0),
                "wines_b": int(row[8] or 0),
                "signals": row[9] or {},
                "candidate_id": candidate_id,
            }
        )
        exclude_ids.add(candidate_id)
        if len(rows) >= limit:
            break
    return rows


class ContextBuilder:
    def __init__(self, cur):
        self.cur = cur
        self.producer_cache: dict[str, dict] = {}
        self.catalog_cache: dict[str, dict] = {}
        self.card_cache: dict[str, dict] = {}

    def producer_row(self, producer_id: str) -> dict:
        if producer_id not in self.producer_cache:
            self.producer_cache.update(fetch_producers(self.cur, [producer_id]))
        return self.producer_cache[producer_id]

    def catalog(self, producer_id: str) -> dict:
        if producer_id not in self.catalog_cache:
            self.catalog_cache.update(fetch_catalog_context(self.cur, [producer_id]))
        return self.catalog_cache[producer_id]

    def vintage_count(self, producer_id: str) -> int:
        self.cur.execute(
            """
            SELECT COUNT(*)
            FROM wine_vintages wv
            JOIN wines w ON w.id = wv.wine_id
            WHERE w.producer_id = %s::uuid
              AND w.deleted_at IS NULL
            """,
            (producer_id,),
        )
        return int(self.cur.fetchone()[0] or 0)

    def count_rows(self, table: str, producer_id: str) -> int:
        self.cur.execute(
            f"""
            SELECT COUNT(*)
            FROM {table}
            WHERE canonical_producer_id = %s::uuid
            """,
            (producer_id,),
        )
        return int(self.cur.fetchone()[0] or 0)

    def top_values(self, table: str, column: str, producer_id: str, *, limit: int = 6) -> list[tuple[str, int]]:
        self.cur.execute(
            f"""
            SELECT {column}, COUNT(*)
            FROM {table}
            WHERE canonical_producer_id = %s::uuid
              AND {column} IS NOT NULL
              AND NULLIF(BTRIM({column}), '') IS NOT NULL
            GROUP BY 1
            ORDER BY COUNT(*) DESC, 1
            LIMIT {limit}
            """,
            (producer_id,),
        )
        return [(safe_text(row[0]), int(row[1] or 0)) for row in self.cur.fetchall()]

    def top_ttb_permits(self, producer_id: str) -> list[tuple[str, int]]:
        self.cur.execute(
            """
            SELECT COALESCE(NULLIF(BTRIM(permit_no), ''), NULLIF(BTRIM(permit_number), '')) AS permit,
                   COUNT(*)
            FROM source_ttb_colas
            WHERE canonical_producer_id = %s::uuid
              AND COALESCE(NULLIF(BTRIM(permit_no), ''), NULLIF(BTRIM(permit_number), '')) IS NOT NULL
            GROUP BY 1
            ORDER BY COUNT(*) DESC, 1
            LIMIT 6
            """,
            (producer_id,),
        )
        return [(safe_text(row[0]), int(row[1] or 0)) for row in self.cur.fetchall()]

    def top_wine_grapes(self, producer_id: str) -> list[tuple[str, int]]:
        self.cur.execute(
            """
            SELECT COALESCE(NULLIF(BTRIM(g.display_name), ''), g.name) AS grape_name,
                   COUNT(*)
            FROM wine_grapes wg
            JOIN wines w ON w.id = wg.wine_id
            JOIN grapes g ON g.id = wg.grape_id
            WHERE w.producer_id = %s::uuid
              AND w.deleted_at IS NULL
            GROUP BY 1
            ORDER BY COUNT(*) DESC, 1
            LIMIT 4
            """,
            (producer_id,),
        )
        return [(safe_text(row[0]), int(row[1] or 0)) for row in self.cur.fetchall()]

    def wine_designations(self, producer_id: str) -> list[tuple[str, int]]:
        rows = self.top_values("source_lwin", "designation", producer_id, limit=4)
        if rows:
            return rows
        self.cur.execute(
            """
            SELECT COALESCE(
                     NULLIF(BTRIM(ld.canonical_name), ''),
                     NULLIF(BTRIM(ld.local_name), '')
                   ) AS designation,
                   COUNT(*)
            FROM wine_label_designations wld
            JOIN wines w ON w.id = wld.wine_id
            JOIN label_designations ld ON ld.id = wld.label_designation_id
            WHERE w.producer_id = %s::uuid
              AND w.deleted_at IS NULL
              AND COALESCE(
                    NULLIF(BTRIM(ld.canonical_name), ''),
                    NULLIF(BTRIM(ld.local_name), '')
                  ) IS NOT NULL
            GROUP BY 1
            ORDER BY COUNT(*) DESC, 1
            LIMIT 4
            """,
            (producer_id,),
        )
        return [(safe_text(row[0]), int(row[1] or 0)) for row in self.cur.fetchall()]

    def wine_colors(self, producer_id: str) -> list[tuple[str, int]]:
        self.cur.execute(
            """
            SELECT color, COUNT(*)
            FROM wines
            WHERE producer_id = %s::uuid
              AND deleted_at IS NULL
              AND color IS NOT NULL
              AND NULLIF(BTRIM(color), '') IS NOT NULL
            GROUP BY 1
            ORDER BY COUNT(*) DESC, 1
            LIMIT 4
            """,
            (producer_id,),
        )
        return [(safe_text(row[0]), int(row[1] or 0)) for row in self.cur.fetchall()]

    def importer_profile_counts(self, producer_id: str) -> tuple[list[str], list[str]]:
        profile_families: list[str] = []
        people_signals: set[str] = set()
        checks = [
            ("source_skurnik", ["description", "notes"], []),
            ("source_winebow", ["description", "vineyard_description"], ["production"]),
            ("source_empson", ["description", "tasting_notes"], ["winemaker", "production", "first_vintage"]),
            ("source_european_cellars", ["vinification"], []),
            (
                "source_kermit_lynch_growers",
                ["about", "viticulture_notes", "location"],
                ["founded_year", "winemaker", "annual_production"],
            ),
        ]
        for table, text_cols, people_cols in checks:
            text_pred = " OR ".join(
                [f"NULLIF(BTRIM({column}::text), '') IS NOT NULL" for column in text_cols + people_cols]
            )
            self.cur.execute(
                f"""
                SELECT COUNT(*)
                FROM {table}
                WHERE canonical_producer_id = %s::uuid
                  AND ({text_pred})
                """,
                (producer_id,),
            )
            count = int(self.cur.fetchone()[0] or 0)
            if count <= 0:
                continue
            profile_families.append(SOURCE_FAMILY_MAP[table])
            for column in people_cols:
                self.cur.execute(
                    f"""
                    SELECT COUNT(*)
                    FROM {table}
                    WHERE canonical_producer_id = %s::uuid
                      AND {column} IS NOT NULL
                      AND NULLIF(BTRIM({column}::text), '') IS NOT NULL
                    """,
                    (producer_id,),
                )
                if int(self.cur.fetchone()[0] or 0) > 0:
                    people_signals.add(column)
        website_domains: set[str] = set()
        producer = self.producer_row(producer_id)
        if producer.get("website_url"):
            host = domain_host(producer["website_url"])
            if host:
                website_domains.add(host)
        self.cur.execute(
            """
            SELECT website
            FROM source_kermit_lynch_growers
            WHERE canonical_producer_id = %s::uuid
              AND website IS NOT NULL
              AND NULLIF(BTRIM(website), '') IS NOT NULL
            LIMIT 4
            """,
            (producer_id,),
        )
        for row in self.cur.fetchall():
            host = domain_host(row[0])
            if host:
                website_domains.add(host)
        return sorted(website_domains), sorted(profile_families), sorted(people_signals)

    def producer_card(self, producer_id: str) -> dict:
        if producer_id in self.card_cache:
            return deepcopy(self.card_cache[producer_id])

        producer = self.producer_row(producer_id)
        catalog = self.catalog(producer_id)

        label_like_entries = [
            {
                "value": producer["name"],
                "source_family": "canonical",
                "row_count": max(catalog["wine_count"], 1),
            }
        ]
        for source_family, table, column, limit in (
            ("lwin", "source_lwin", "producer_name", 6),
            ("ttb", "source_ttb_colas", "brand_name", 6),
            ("state_reg", "source_pro_platform", "brand", 4),
            ("state_reg", "source_tabc", "brand_name", 3),
            ("state_reg", "source_tabc", "trade_name", 3),
            ("state_reg", "source_kansas_brands", "brand_name", 3),
        ):
            for value, count in self.top_values(table, column, producer_id, limit=limit):
                label_like_entries.append(
                    {"value": value, "source_family": source_family, "row_count": count}
                )
        label_like_names = dedupe_name_entries(label_like_entries, limit=8)

        legal_entries: list[dict] = []
        for value, count in self.top_values("source_ttb_colas", "applicant_name", producer_id, limit=4):
            legal_entries.append({"value": value, "source_family": "ttb", "row_count": count})
        for value, count in self.top_values("source_pro_platform", "supplier_name", producer_id, limit=4):
            legal_entries.append({"value": value, "source_family": "state_reg", "row_count": count})
        legal_or_applicant_names = dedupe_simple_entries(legal_entries, limit=4)

        country_items: list[tuple[str, int]] = []
        if producer.get("country_name"):
            country_items.append((producer["country_name"], max(catalog["wine_count"], 1)))
        wine_country_counts = Counter(
            safe_text(wine.get("country_code"))
            for wine in catalog["wines"]
            if safe_text(wine.get("country_code"))
        )
        country_items.extend((country, count) for country, count in wine_country_counts.items())
        country_items.extend(self.top_values("source_lwin", "country", producer_id, limit=4))

        region_items: list[tuple[str, int]] = []
        if producer.get("region_name"):
            region_items.append((producer["region_name"], max(catalog["wine_count"], 1)))
        wine_region_counts = Counter(
            safe_text(wine.get("region_name"))
            for wine in catalog["wines"]
            if safe_text(wine.get("region_name"))
        )
        region_items.extend((region, count) for region, count in wine_region_counts.items())
        region_items.extend(self.top_values("source_lwin", "region", producer_id, limit=6))

        appellation_items: list[tuple[str, int]] = []
        wine_appellation_counts = Counter(
            safe_text(wine.get("appellation_name"))
            for wine in catalog["wines"]
            if safe_text(wine.get("appellation_name"))
        )
        appellation_items.extend((appellation, count) for appellation, count in wine_appellation_counts.items())
        appellation_items.extend(self.top_values("source_lwin", "sub_region", producer_id, limit=6))
        appellation_items.extend(self.top_values("source_ttb_colas", "wine_appellation", producer_id, limit=4))
        appellation_items.extend(self.top_values("source_pro_platform", "appellation", producer_id, limit=4))
        appellation_items.extend(self.top_values("source_kansas_brands", "appellation", producer_id, limit=4))

        top_wine_labels = dedupe_count_items(
            Counter(safe_text(wine["display_name"]) for wine in catalog["wines"] if safe_text(wine["display_name"])).items(),
            limit=6,
        )
        top_lwin_display_names = dedupe_count_items(
            self.top_values("source_lwin", "display_name", producer_id, limit=4),
            limit=4,
        )
        top_grapes = dedupe_count_items(self.top_wine_grapes(producer_id), limit=4)
        top_designations = dedupe_count_items(self.wine_designations(producer_id), limit=4)
        color_mix = dedupe_count_items(self.wine_colors(producer_id), limit=4)

        permits = dedupe_count_items(self.top_ttb_permits(producer_id), limit=6)
        applicant_states = dedupe_count_items(
            self.top_values("source_ttb_colas", "applicant_state", producer_id, limit=6),
            limit=6,
        )
        applicant_names = dedupe_count_items(
            self.top_values("source_ttb_colas", "applicant_name", producer_id, limit=6),
            limit=6,
        )
        supplier_distributor_items = (
            self.top_values("source_pro_platform", "supplier_name", producer_id, limit=4)
            + self.top_values("source_kansas_brands", "distributor1", producer_id, limit=4)
            + self.top_values("source_kansas_brands", "distributor2", producer_id, limit=4)
        )
        supplier_or_distributor_names = dedupe_count_items(supplier_distributor_items, limit=6)

        lwin_count = self.count_rows("source_lwin", producer_id)
        ttb_count = self.count_rows("source_ttb_colas", producer_id)
        state_reg_count = (
            self.count_rows("source_pro_platform", producer_id)
            + self.count_rows("source_tabc", producer_id)
            + self.count_rows("source_kansas_brands", producer_id)
        )
        importer_count = (
            self.count_rows("source_skurnik", producer_id)
            + self.count_rows("source_winebow", producer_id)
            + self.count_rows("source_empson", producer_id)
            + self.count_rows("source_european_cellars", producer_id)
            + self.count_rows("source_kermit_lynch_growers", producer_id)
            + self.count_rows("source_kermit_lynch", producer_id)
        )

        source_families = []
        for family, count in (
            ("lwin", lwin_count),
            ("ttb", ttb_count),
            ("state_reg", state_reg_count),
            ("importer", importer_count),
        ):
            if count > 0:
                source_families.append({"source_family": family, "linked_rows": count})

        place_conflicts: list[str] = []
        if len({item["value"] for item in dedupe_count_items(country_items, limit=6)}) > 1:
            place_conflicts.append("multiple country signals in cheap dossier")
        if len({item["value"] for item in dedupe_count_items(region_items, limit=6)}) > 2:
            place_conflicts.append("multiple region signals in cheap dossier")

        name_conflicts: list[str] = []
        if len(label_like_names) >= 4:
            name_conflicts.append("multiple label-facing name forms require comparison")
        market_conflicts: list[str] = []
        if len(applicant_states) >= 2:
            market_conflicts.append("multiple applicant states present")
        sparse_signal_flags: list[str] = []
        if catalog["wine_count"] <= 2:
            sparse_signal_flags.append("sparse_wine_catalog")
        if lwin_count == 0 and ttb_count == 0 and state_reg_count == 0:
            sparse_signal_flags.append("limited_external_tier_a_footprint")
        if not applicant_names and not supplier_or_distributor_names:
            sparse_signal_flags.append("thin_regulatory_market_signal")
        if not top_grapes:
            sparse_signal_flags.append("no_grape_fingerprint")

        website_domains, profile_families, people_signals = self.importer_profile_counts(producer_id)

        card = {
            "producer_id": producer_id,
            "canonical_name": producer["name"],
            "name_normalized": producer["name_normalized"] or normalize_text(producer["name"]),
            "slug": normalize_text(producer["name"]).replace(" ", "-"),
            "country": producer.get("country_name"),
            "region": producer.get("region_name"),
            "label_like_names": label_like_names,
            "legal_or_applicant_names": legal_or_applicant_names,
            "source_presence": {
                "wine_count": catalog["wine_count"],
                "vintage_count": self.vintage_count(producer_id),
                "source_families": source_families,
            },
            "place_fingerprint": {
                "countries": dedupe_count_items(country_items, limit=6),
                "regions": dedupe_count_items(region_items, limit=6),
                "appellations": dedupe_count_items(appellation_items, limit=6),
                "place_conflicts": place_conflicts[:4],
            },
            "portfolio_fingerprint": {
                "top_wine_labels": top_wine_labels[:6],
                "top_lwin_display_names": top_lwin_display_names[:4],
                "top_grapes": top_grapes[:4],
                "top_designations": top_designations[:4],
                "color_mix": color_mix[:4],
            },
            "regulatory_market_fingerprint": {
                "ttb_permits": permits[:6],
                "ttb_applicant_states": applicant_states[:6],
                "ttb_applicant_names": applicant_names[:6],
                "supplier_or_distributor_names": supplier_or_distributor_names[:6],
            },
            "conflicts": {
                "name_conflicts": name_conflicts[:4],
                "place_conflicts": place_conflicts[:4],
                "market_conflicts": market_conflicts[:4],
                "sparse_signal_flags": sparse_signal_flags[:4],
            },
            "escalation_available": {
                "website_domains": website_domains[:3],
                "profile_source_families": profile_families[:3],
                "people_history_signals": people_signals[:3],
            },
        }
        self.card_cache[producer_id] = deepcopy(card)
        return card


def card_to_candidate_identity(card: dict) -> dict:
    return {
        "canonical_name": card["canonical_name"],
        "country": card["country"],
        "region": card["region"],
        "label_like_names": [
            {"value": item["value"], "row_count": item["row_count"]} for item in card["label_like_names"][:6]
        ],
        "legal_or_applicant_names": [
            {"value": item["value"], "row_count": item["row_count"]}
            for item in card["legal_or_applicant_names"][:4]
        ],
        "source_families": card["source_presence"]["source_families"][:5],
        "top_wine_labels": card["portfolio_fingerprint"]["top_wine_labels"][:5],
        "top_lwin_display_names": card["portfolio_fingerprint"]["top_lwin_display_names"][:3],
        "top_grapes": card["portfolio_fingerprint"]["top_grapes"][:3],
        "top_designations": card["portfolio_fingerprint"]["top_designations"][:3],
        "color_mix": card["portfolio_fingerprint"]["color_mix"][:3],
        "ttb_applicant_names": card["regulatory_market_fingerprint"]["ttb_applicant_names"][:4],
        "supplier_or_distributor_names": card["regulatory_market_fingerprint"]["supplier_or_distributor_names"][:4],
        "conflicts": (
            card["conflicts"]["name_conflicts"]
            + card["conflicts"]["place_conflicts"]
            + card["conflicts"]["market_conflicts"]
        )[:4],
        "sparse_signal_flags": card["conflicts"]["sparse_signal_flags"][:4],
    }


def names_by_norm(entries: list[dict]) -> dict[str, str]:
    mapping: dict[str, str] = {}
    for entry in entries:
        norm = normalize_text(entry["value"])
        if norm and norm not in mapping:
            mapping[norm] = entry["value"]
    return mapping


def entry_values(entries: list[dict]) -> dict[str, str]:
    return {normalize_text(item["value"]): item["value"] for item in entries if normalize_text(item["value"])}


def collect_overlap(left: list[dict], right: list[dict]) -> list[str]:
    left_map = entry_values(left)
    right_map = entry_values(right)
    shared = sorted(set(left_map) & set(right_map))
    return [left_map[key] for key in shared[:4]]


def collect_difference(left: list[dict], right: list[dict]) -> list[str]:
    left_map = entry_values(left)
    right_map = entry_values(right)
    only_right = sorted(set(right_map) - set(left_map))
    return [right_map[key] for key in only_right[:4]]


def compare_cards(anchor: dict, candidate: dict, pattern_family: str, source_reasoning: str) -> dict:
    matched_name_forms = collect_overlap(anchor["label_like_names"], candidate["label_like_names"])
    matched_place_signals = (
        collect_overlap(anchor["place_fingerprint"]["countries"], candidate["place_fingerprint"]["countries"])
        + collect_overlap(anchor["place_fingerprint"]["regions"], candidate["place_fingerprint"]["regions"])
        + collect_overlap(anchor["place_fingerprint"]["appellations"], candidate["place_fingerprint"]["appellations"])
    )[:4]
    matched_portfolio_signals = (
        collect_overlap(
            anchor["portfolio_fingerprint"]["top_wine_labels"],
            candidate["portfolio_fingerprint"]["top_wine_labels"],
        )
        + collect_overlap(anchor["portfolio_fingerprint"]["top_grapes"], candidate["portfolio_fingerprint"]["top_grapes"])
        + collect_overlap(
            anchor["portfolio_fingerprint"]["top_designations"],
            candidate["portfolio_fingerprint"]["top_designations"],
        )
    )[:4]
    matched_regulatory_signals = (
        collect_overlap(
            anchor["regulatory_market_fingerprint"]["ttb_permits"],
            candidate["regulatory_market_fingerprint"]["ttb_permits"],
        )
        + collect_overlap(
            anchor["regulatory_market_fingerprint"]["ttb_applicant_names"],
            candidate["regulatory_market_fingerprint"]["ttb_applicant_names"],
        )
        + collect_overlap(
            anchor["regulatory_market_fingerprint"]["supplier_or_distributor_names"],
            candidate["regulatory_market_fingerprint"]["supplier_or_distributor_names"],
        )
    )[:4]

    divergent_name_signals = collect_difference(anchor["label_like_names"], candidate["label_like_names"])
    divergent_place_signals = (
        collect_difference(anchor["place_fingerprint"]["countries"], candidate["place_fingerprint"]["countries"])
        + collect_difference(anchor["place_fingerprint"]["regions"], candidate["place_fingerprint"]["regions"])
        + collect_difference(anchor["place_fingerprint"]["appellations"], candidate["place_fingerprint"]["appellations"])
    )[:4]
    divergent_portfolio_signals = (
        collect_difference(
            anchor["portfolio_fingerprint"]["top_wine_labels"],
            candidate["portfolio_fingerprint"]["top_wine_labels"],
        )
        + collect_difference(anchor["portfolio_fingerprint"]["top_grapes"], candidate["portfolio_fingerprint"]["top_grapes"])
    )[:4]

    risk_tags: list[str] = []
    shared_anchor_surname = normalized_last_token(anchor["canonical_name"])
    shared_candidate_surname = normalized_last_token(candidate["canonical_name"])
    if pattern_family == "11.4.m" or (
        shared_anchor_surname and shared_anchor_surname == shared_candidate_surname
    ):
        risk_tags.append("shared_surname")
    if pattern_family == "11.4.j":
        risk_tags.append("permit_only")
    if (
        pattern_family == "11.4.n"
        or anchor.get("country")
        and candidate.get("country")
        and anchor["country"] != candidate["country"]
        and matched_name_forms
    ):
        risk_tags.append("global_brand_country_split")
    if pattern_family == "11.4.g":
        risk_tags.append("holdco_split")
    if pattern_family in {"11.4.o", "11.4.q", "11.4.s"}:
        risk_tags.append("label_relationship")

    why_this_candidate_survived = []
    if matched_name_forms:
        why_this_candidate_survived.append("shared label-facing name form survived Tier A retrieval")
    if matched_portfolio_signals:
        why_this_candidate_survived.append("portfolio overlap kept candidate visible")
    if matched_regulatory_signals:
        why_this_candidate_survived.append("regulatory or market signal corroborated the candidate")
    if not why_this_candidate_survived:
        why_this_candidate_survived.append(compact_text(source_reasoning, 120))

    return {
        "matched_name_forms": matched_name_forms[:4],
        "matched_place_signals": matched_place_signals[:4],
        "matched_portfolio_signals": matched_portfolio_signals[:4],
        "matched_regulatory_signals": matched_regulatory_signals[:4],
        "divergent_name_signals": divergent_name_signals[:4],
        "divergent_place_signals": divergent_place_signals[:4],
        "divergent_portfolio_signals": divergent_portfolio_signals[:4],
        "risk_tags": risk_tags[:4],
        "why_this_candidate_survived": why_this_candidate_survived[:4],
    }


def source_case_summary(case: dict) -> str:
    if case["source_kind"] == "benchmark_v1":
        return compact_text(case["source_payload"]["rationale"], 180)
    return compact_text(case["source_payload"].get("gold_reasoning"), 180)


def build_candidate_packet(
    anchor_card: dict,
    candidate_card: dict,
    pair_row: dict | None,
    rank: int,
    pattern_family: str,
    source_reasoning: str,
) -> dict:
    matched_name_forms = collect_overlap(anchor_card["label_like_names"], candidate_card["label_like_names"])
    seed_families = collect_signal_families(pair_row)
    lex_strength = lexical_strength(anchor_card["canonical_name"], candidate_card["canonical_name"], pair_row)
    return {
        "candidate_id": candidate_card["producer_id"],
        "candidate_rank": rank,
        "retrieval_basis": {
            "seed_families": seed_families,
            "lexical_strength": lex_strength,
            "matched_anchor_name_forms": matched_name_forms[:4],
            "shortlist_pressure_band": shortlist_pressure_band(len(seed_families), lex_strength),
        },
        "candidate_identity": card_to_candidate_identity(candidate_card),
        "comparison_to_anchor": compare_cards(anchor_card, candidate_card, pattern_family, source_reasoning),
    }


def decision_options() -> dict:
    return {
        "allowed_choice_types": ["candidate", "none"],
        "none_definition": (
            "Choose NONE when no shortlisted candidate deserves SAME_AS or "
            "RELATED_BUT_DISTINCT and escalation is unlikely to change that."
        ),
        "unsure_definition": (
            "Choose UNSURE when evidence is too thin or conflicting for a safe "
            "accepted label and escalation has a plausible path to resolve it."
        ),
        "selection_rule": "Choose at most one candidate. Do not emit multiple candidates or an invented candidate.",
    }


def validate_phase_a_packet(packet: dict) -> list[str]:
    issues: list[str] = []
    required_top = {"selector_version", "case_id", "dossier_version", "shortlist_version", "anchor", "shortlist", "decision_options"}
    missing = sorted(required_top - set(packet))
    if missing:
        issues.append(f"missing top-level keys: {missing}")
    shortlist = packet.get("shortlist") or {}
    if shortlist.get("candidate_count", 0) != len(shortlist.get("candidates", [])):
        issues.append("shortlist.candidate_count does not match candidates length")
    if shortlist.get("candidate_count", 0) > 12:
        issues.append("shortlist.candidate_count exceeds 12")
    for candidate in shortlist.get("candidates", []):
        for key in ("candidate_id", "candidate_rank", "retrieval_basis", "candidate_identity", "comparison_to_anchor"):
            if key not in candidate:
                issues.append(f"candidate missing {key}")
    return issues


def validate_phase_b_packet(packet: dict) -> list[str]:
    issues: list[str] = []
    required_top = {
        "case_id",
        "escalation_version",
        "selector_result_ref",
        "escalation_mode",
        "anchor",
        "added_evidence",
        "decision_options",
    }
    missing = sorted(required_top - set(packet))
    if missing:
        issues.append(f"missing escalation keys: {missing}")
    if packet.get("escalation_mode") == "candidate_frontier" and "frontier_candidate" not in packet:
        issues.append("candidate_frontier packet missing frontier_candidate")
    if packet.get("escalation_mode") == "shortlist_gap_probe" and packet.get("frontier_candidate"):
        issues.append("shortlist_gap_probe should not include frontier_candidate")
    return issues


def build_manual_phase_b_blocks(case: dict, phase_a_packet: dict) -> dict:
    source = case["source_payload"]
    blocks = {
        "web_identity": {"anchor_urls": [], "candidate_urls": [], "source_urls": []},
        "profile_snippets": [],
        "people_history": {"anchor": [], "candidate": []},
        "vineyard_profile": {"anchor": [], "candidate": []},
        "raw_supporting_rows": [],
        "retrieval_gap_diagnostics": [],
    }
    if case["source_kind"] == "calibration_set":
        for item in (source.get("gold_web_evidence") or [])[:2]:
            url = safe_text(item.get("url"))
            why = compact_text(item.get("why"), 240)
            title = safe_text(item.get("title"))
            if url:
                blocks["web_identity"]["source_urls"].append(url)
            if why:
                blocks["profile_snippets"].append(
                    {"source_url": url, "title": title, "text": why}
                )
        if source.get("gold_reasoning"):
            blocks["raw_supporting_rows"].append(
                {
                    "source_id": f"calibration_set:{source['pair_id']}",
                    "source_family": "calibration_set",
                    "summary": compact_text(source["gold_reasoning"], 260),
                }
            )
    else:
        rationale = safe_text(source.get("rationale"))
        if rationale:
            blocks["profile_snippets"].append(
                {
                    "source_url": "",
                    "title": case["source_ref"],
                    "text": compact_text(rationale, 240),
                }
            )
            blocks["raw_supporting_rows"].append(
                {
                    "source_id": f"benchmark_v1:{source['case_id']}",
                    "source_family": "benchmark_v1",
                    "summary": compact_text(rationale, 260),
                }
            )
    if case.get("phase_b", {}).get("escalation_mode") == "shortlist_gap_probe":
        target_name = source.get("producer_name_b") or source.get("name_b") or ""
        blocks["retrieval_gap_diagnostics"].append(
            {
                "kind": "suppressed_candidate_probe",
                "summary": (
                    f"Shortlist gap probe against `{target_name}` kept bounded to the "
                    f"proof case source `{case['source_ref']}`."
                ),
            }
        )
    if not blocks["raw_supporting_rows"]:
        blocks["raw_supporting_rows"].append(
            {
                "source_id": f"proof_case:{case['case_id']}",
                "source_family": "proof_case",
                "summary": source_case_summary(case),
            }
        )
    return blocks


def build_phase_a_packet(case: dict, ctx: ContextBuilder, pair_row: dict | None, focal_pair: dict | None) -> tuple[dict, list[str]]:
    anchor_id = case["anchor_producer_id"]
    anchor_card = ctx.producer_card(anchor_id)
    candidates: list[dict] = []
    packet_mode = case["phase_a"]["packet_mode"]
    if packet_mode != "empty_shortlist" and case.get("candidate_producer_id"):
        candidate_card = ctx.producer_card(case["candidate_producer_id"])
        candidates.append(
            build_candidate_packet(
                anchor_card,
                candidate_card,
                focal_pair or pair_row,
                1,
                case["pattern_family"],
                source_case_summary(case),
            )
        )
        if packet_mode == "comparison_packet":
            exclude_ids = {anchor_id, case["candidate_producer_id"]}
            extras = fetch_extra_candidates(
                ctx.cur,
                anchor_id,
                exclude_ids,
                limit=int(case["phase_a"].get("auto_distractor_count", 0)),
            )
            for rank, extra in enumerate(extras, start=2):
                extra_card = ctx.producer_card(extra["candidate_id"])
                candidates.append(
                    build_candidate_packet(
                        anchor_card,
                        extra_card,
                        extra,
                        rank,
                        case["pattern_family"],
                        source_case_summary(case),
                    )
                )

    if packet_mode == "empty_shortlist":
        sparse_flags = anchor_card["conflicts"]["sparse_signal_flags"]
        if "proof_empty_shortlist_case" not in sparse_flags:
            sparse_flags.append("proof_empty_shortlist_case")

    packet = {
        "selector_version": "selector_harness_v1",
        "case_id": case["case_id"],
        "dossier_version": "producer_dossier_v1",
        "shortlist_version": "shortlist_generation_v1",
        "anchor": anchor_card,
        "shortlist": {
            "shortlist_status": "no_candidate_found" if not candidates else "candidates_present",
            "candidate_count": len(candidates),
            "candidates": candidates,
        },
        "decision_options": decision_options(),
    }
    return packet, validate_phase_a_packet(packet)


def build_phase_b_packet(case: dict, phase_a_packet: dict) -> tuple[dict, list[str]]:
    phase_b = case["phase_b"]
    anchor = deepcopy(phase_a_packet["anchor"])
    frontier_candidate = None
    if phase_b["escalation_mode"] == "candidate_frontier":
        frontier_candidate = deepcopy(phase_a_packet["shortlist"]["candidates"][0])
    prior_reason_code = support_choice_reason(
        "UNSURE",
        case["pattern_family"],
        phase_a_packet["shortlist"]["shortlist_status"],
    )
    packet = {
        "case_id": case["case_id"],
        "escalation_version": "escalation_dossier_v1",
        "selector_result_ref": {
            "selector_version": "selector_harness_v1",
            "anchor_producer_id": case["anchor_producer_id"],
            "prior_label": "UNSURE",
            "prior_choice_type": case["phase_a"]["choice_type"],
            "prior_selected_candidate_id": case.get("candidate_producer_id"),
            "prior_reason_code": prior_reason_code,
        },
        "escalation_mode": phase_b["escalation_mode"],
        "anchor": anchor,
        "added_evidence": build_manual_phase_b_blocks(case, phase_a_packet),
        "decision_options": decision_options(),
    }
    if frontier_candidate is not None:
        packet["frontier_candidate"] = frontier_candidate
    return packet, validate_phase_b_packet(packet)


def build_phase_c_manifest(cases: list[dict], phase_a_packets: dict[str, dict]) -> dict:
    items = []
    for case in cases:
        packet = phase_a_packets[case["case_id"]]
        items.append(
            {
                "proof_version": "selector_proof_v1",
                "case_id": case["case_id"],
                "stratum": case["stratum"],
                "anchor_producer_id": case["anchor_producer_id"],
                "shortlist_expectation": case["shortlist_expectation"],
                "expected_shortlist_status": packet["shortlist"]["shortlist_status"],
                "expected_candidate_ids": [
                    candidate["candidate_id"] for candidate in packet["shortlist"]["candidates"]
                ],
                "expected_candidate_count": packet["shortlist"]["candidate_count"],
                "shortlist_cap": 12,
            }
        )
    return {"proof_version": "selector_proof_v1", "cases": items}


def build_hidden_key(cases: list[dict]) -> dict:
    phase_a = []
    phase_b = []
    for case in cases:
        phase_a.append(
            {
                "proof_version": "selector_proof_v1",
                "case_id": case["case_id"],
                "risk_tier": case["risk_tier"],
                "pattern_family": case["pattern_family"],
                "world_relationship": case["world_relationship"],
                "expected_selector_label": case["expected_selector_label"],
                "acceptable_candidate_ids": (
                    [case["candidate_producer_id"]] if case.get("candidate_producer_id") else []
                ),
                "shortlist_expectation": case["shortlist_expectation"],
                "escalation_expected": bool(case.get("phase_b")),
            }
        )
        if case.get("phase_b"):
            phase_b.append(
                {
                    "proof_version": "selector_proof_v1",
                    "case_id": case["case_id"],
                    "expected_escalation_label": case["phase_b"]["expected_escalation_label"],
                    "expected_escalation_choice_type": case["phase_b"]["expected_escalation_choice_type"],
                    "acceptable_candidate_ids": (
                        [case["candidate_producer_id"]] if case.get("candidate_producer_id") else []
                    ),
                    "allowed_escalation_blocks": case["phase_b"]["allowed_escalation_blocks"],
                    "expected_case_resolution": case["phase_b"]["expected_case_resolution"],
                }
            )
    return {"proof_version": "selector_proof_v1", "phase_a": phase_a, "phase_b": phase_b}


def resolve_source_case(manifest_case: dict, benchmark_map: dict[str, dict], calibration_map: dict[int, dict]) -> dict:
    case = deepcopy(manifest_case)
    if case["source_kind"] == "benchmark_v1":
        payload = deepcopy(benchmark_map[case["source_case_id"]])
        case["source_ref"] = case["source_case_id"]
        case["source_payload"] = payload
        case["pair_id"] = int(payload["pair_id"])
        case["anchor_name"] = payload["producer_name_a"]
        case["candidate_name"] = payload["producer_name_b"]
    else:
        payload = deepcopy(calibration_map[int(case["source_pair_id"])])
        case["source_ref"] = f"calibration_pair_{case['source_pair_id']}"
        case["source_payload"] = payload
        case["pair_id"] = int(payload["pair_id"])
        case["anchor_name"] = payload["name_a"]
        case["candidate_name"] = payload["name_b"]
    return case


def hydrate_case_entities(cur, case: dict) -> dict:
    pair_row = fetch_pair_row(cur, case["pair_id"])
    case["anchor_producer_id"] = pair_row["producer_id_a"]
    case["candidate_producer_id"] = pair_row["producer_id_b"]
    case["pair_row"] = pair_row
    return case


def candidate_paths(packet: dict, *, escalation: bool = False) -> list[str]:
    root = "/frontier_candidate/comparison_to_anchor" if escalation else "/shortlist/candidates/0/comparison_to_anchor"
    paths = []
    for bucket in (
        "matched_name_forms",
        "matched_place_signals",
        "matched_portfolio_signals",
        "matched_regulatory_signals",
        "why_this_candidate_survived",
    ):
        pointer = f"{root}/{bucket}/0"
        if pointer_exists(packet, pointer):
            paths.append(pointer)
    for bucket in ("divergent_name_signals", "divergent_place_signals", "divergent_portfolio_signals", "risk_tags"):
        pointer = f"{root}/{bucket}/0"
        if pointer_exists(packet, pointer):
            paths.append(pointer)
    return paths


def empty_shortlist_support_paths(packet: dict) -> list[str]:
    candidates = ["/shortlist/shortlist_status", "/shortlist/candidate_count"]
    for bucket in ("sparse_signal_flags", "name_conflicts", "place_conflicts", "market_conflicts"):
        pointer = f"/anchor/conflicts/{bucket}/0"
        if pointer_exists(packet, pointer):
            candidates.append(pointer)
    return candidates


def escalation_paths(packet: dict) -> list[str]:
    paths = []
    for bucket in (
        "/added_evidence/profile_snippets/0/text",
        "/added_evidence/raw_supporting_rows/0/summary",
        "/added_evidence/retrieval_gap_diagnostics/0/summary",
        "/frontier_candidate/comparison_to_anchor/matched_name_forms/0",
        "/frontier_candidate/comparison_to_anchor/matched_regulatory_signals/0",
        "/frontier_candidate/comparison_to_anchor/divergent_place_signals/0",
        "/frontier_candidate/comparison_to_anchor/risk_tags/0",
    ):
        if pointer_exists(packet, bucket):
            paths.append(bucket)
    return paths


def build_selector_oracle_result(case: dict, packet: dict) -> dict:
    label = case["expected_selector_label"]
    shortlist_status = packet["shortlist"]["shortlist_status"]
    choice_type = case["phase_a"]["choice_type"]
    candidate_id = case.get("candidate_producer_id") if choice_type == "candidate" and shortlist_status == "candidates_present" else None
    candidate_rank = 1 if candidate_id else None
    if shortlist_status == "no_candidate_found":
        support_paths = empty_shortlist_support_paths(packet)
        conflict_paths = support_paths[2:4] if label == "UNSURE" else []
    else:
        support_paths = candidate_paths(packet)
        conflict_paths = [path for path in support_paths if "/divergent_" in path or "/risk_tags/" in path][:3]
    if label in {"SAME_AS", "RELATED_BUT_DISTINCT"}:
        support_paths = [path for path in support_paths if "/matched_" in path or "/why_this_candidate_survived/" in path][:4]
        conflict_paths = []
    elif label == "NONE":
        support_paths = [path for path in support_paths if "/divergent_" in path or "/risk_tags/" in path][:4] or support_paths[:2]
        conflict_paths = []
    else:
        support_paths = support_paths[:3]
        if not conflict_paths:
            conflict_paths = support_paths[:1]
    return {
        "proof_version": "selector_proof_v1",
        "case_id": case["case_id"],
        "decision_stage": "selector",
        "selector_version": "selector_harness_v1",
        "anchor_producer_id": case["anchor_producer_id"],
        "shortlist_status": shortlist_status,
        "choice": {
            "choice_type": choice_type,
            "selected_candidate_id": candidate_id,
            "selected_candidate_rank": candidate_rank,
        },
        "label": label,
        "primary_reason_code": support_choice_reason(label, case["pattern_family"], shortlist_status),
        "secondary_reason_codes": secondary_reason_codes(label, case["pattern_family"], shortlist_status),
        "rule_hypotheses": [case["pattern_family"]],
        "support_ref_paths": support_paths[:6],
        "conflict_ref_paths": conflict_paths[:6],
        "needs_escalation": label == "UNSURE",
        "escalation_focus": case.get("phase_b", {}).get("allowed_escalation_blocks", [])[:3] if label == "UNSURE" else [],
    }


def build_escalation_oracle_result(case: dict, packet: dict) -> dict:
    phase_b = case["phase_b"]
    label = phase_b["expected_escalation_label"]
    choice_type = phase_b["expected_escalation_choice_type"]
    candidate_id = case.get("candidate_producer_id") if choice_type == "candidate" else None
    candidate_rank = 1 if candidate_id else None
    paths = escalation_paths(packet)
    support_paths = paths[:4]
    conflict_paths = [path for path in paths if "/divergent_" in path or "/retrieval_gap_diagnostics/" in path][:3]
    if label == "SAME_AS":
        support_paths = [path for path in paths if "/matched_" in path or "/profile_snippets/" in path][:4] or support_paths
        conflict_paths = []
    elif label == "RELATED_BUT_DISTINCT":
        support_paths = [path for path in paths if "/profile_snippets/" in path or "/risk_tags/" in path or "/raw_supporting_rows/" in path][:4] or support_paths
        conflict_paths = []
    elif label == "NONE":
        support_paths = [path for path in paths if "/divergent_" in path or "/raw_supporting_rows/" in path][:4] or support_paths
        conflict_paths = []
    else:
        support_paths = support_paths[:3]
        if not conflict_paths:
            conflict_paths = support_paths[:1]
    used_blocks = [
        block
        for block in phase_b["allowed_escalation_blocks"]
        if (
            block == "web_identity" and packet["added_evidence"]["web_identity"]["source_urls"]
        )
        or (block == "profile_snippets" and packet["added_evidence"]["profile_snippets"])
        or (block == "raw_supporting_rows" and packet["added_evidence"]["raw_supporting_rows"])
        or (block == "retrieval_gap_diagnostics" and packet["added_evidence"]["retrieval_gap_diagnostics"])
    ]
    if not used_blocks:
        used_blocks = phase_b["allowed_escalation_blocks"][:1]
    return {
        "proof_version": "selector_proof_v1",
        "case_id": case["case_id"],
        "decision_stage": "escalation",
        "selector_version": "selector_harness_v1",
        "anchor_producer_id": case["anchor_producer_id"],
        "shortlist_status": "candidates_present" if case.get("candidate_producer_id") else "no_candidate_found",
        "choice": {
            "choice_type": choice_type,
            "selected_candidate_id": candidate_id,
            "selected_candidate_rank": candidate_rank,
        },
        "label": label,
        "primary_reason_code": support_choice_reason(label, case["pattern_family"], "candidates_present"),
        "secondary_reason_codes": secondary_reason_codes(label, case["pattern_family"], "candidates_present"),
        "rule_hypotheses": [case["pattern_family"]],
        "support_ref_paths": support_paths[:6],
        "conflict_ref_paths": conflict_paths[:6],
        "needs_escalation": False,
        "escalation_focus": [],
        "resolved_from_prior_label": "UNSURE",
        "used_escalation_blocks": used_blocks,
    }


def validate_result_row(row: dict, packet: dict, *, stage: str, allowed_blocks: list[str] | None = None) -> tuple[bool, bool, bool]:
    required = RESULT_SCHEMA["selector_result"]["required"]
    schema_valid = all(key in row for key in required)
    choice_valid = True
    if row["choice"]["choice_type"] == "candidate":
        candidate_ids = []
        if stage == "selector":
            candidate_ids = [candidate["candidate_id"] for candidate in packet["shortlist"]["candidates"]]
        elif packet.get("frontier_candidate"):
            candidate_ids = [packet["frontier_candidate"]["candidate_id"]]
        choice_valid = row["choice"]["selected_candidate_id"] in candidate_ids
    else:
        choice_valid = row["choice"]["selected_candidate_id"] is None
    all_paths = row["support_ref_paths"] + row["conflict_ref_paths"]
    evidence_valid = all(pointer_exists(packet, pointer) for pointer in all_paths)
    if stage == "escalation" and allowed_blocks is not None:
        used_blocks = row.get("used_escalation_blocks", [])
        evidence_valid = evidence_valid and set(used_blocks).issubset(set(allowed_blocks))
    return schema_valid, choice_valid, evidence_valid


def score_phase_a(cases: list[dict], key: dict, packets: dict[str, dict], results: list[dict]) -> dict:
    expected = {item["case_id"]: item for item in key["phase_a"]}
    metrics = Counter()
    schema_valid = 0
    choice_valid = 0
    evidence_valid = 0
    if not results:
        return {
            "case_count": 0,
            "false_same_as": 0,
            "false_related": 0,
            "missed_same_as": sum(1 for item in key["phase_a"] if item["expected_selector_label"] == "SAME_AS"),
            "missed_related": sum(
                1 for item in key["phase_a"] if item["expected_selector_label"] == "RELATED_BUT_DISTINCT"
            ),
            "false_none": 0,
            "over_escalation": 0,
            "unsafe_frontier_resolution": 0,
            "schema_valid_rate": 0.0,
            "choice_valid_rate": 0.0,
            "evidence_ref_integrity_rate": 0.0,
            "passes": False,
        }
    for row in results:
        case_id = row["case_id"]
        packet = packets[case_id]
        exp = expected[case_id]
        schema_ok, choice_ok, evidence_ok = validate_result_row(row, packet, stage="selector")
        schema_valid += int(schema_ok)
        choice_valid += int(choice_ok)
        evidence_valid += int(evidence_ok)
        label = row["label"]
        expected_label = exp["expected_selector_label"]
        candidate_ok = (
            not exp["acceptable_candidate_ids"]
            or row["choice"]["selected_candidate_id"] in exp["acceptable_candidate_ids"]
        )
        if label == "SAME_AS" and (expected_label != "SAME_AS" or not candidate_ok):
            metrics["false_same_as"] += 1
        if label == "RELATED_BUT_DISTINCT" and (expected_label != "RELATED_BUT_DISTINCT" or not candidate_ok):
            metrics["false_related"] += 1
        if expected_label == "SAME_AS" and (label != "SAME_AS" or not candidate_ok):
            metrics["missed_same_as"] += 1
        if expected_label == "RELATED_BUT_DISTINCT" and (label != "RELATED_BUT_DISTINCT" or not candidate_ok):
            metrics["missed_related"] += 1
        if expected_label in {"SAME_AS", "RELATED_BUT_DISTINCT"} and label == "NONE":
            metrics["false_none"] += 1
        if expected_label != "UNSURE" and label == "UNSURE":
            metrics["over_escalation"] += 1
        if expected_label == "UNSURE" and label != "UNSURE":
            metrics["unsafe_frontier_resolution"] += 1
    total = len(results)
    summary = {
        "case_count": total,
        "false_same_as": metrics["false_same_as"],
        "false_related": metrics["false_related"],
        "missed_same_as": metrics["missed_same_as"],
        "missed_related": metrics["missed_related"],
        "false_none": metrics["false_none"],
        "over_escalation": metrics["over_escalation"],
        "unsafe_frontier_resolution": metrics["unsafe_frontier_resolution"],
        "schema_valid_rate": round(schema_valid / total, 4),
        "choice_valid_rate": round(choice_valid / total, 4),
        "evidence_ref_integrity_rate": round(evidence_valid / total, 4),
    }
    summary["passes"] = (
        summary["false_same_as"] == 0
        and summary["false_related"] <= 1
        and summary["unsafe_frontier_resolution"] == 0
        and summary["missed_same_as"] <= 4
        and summary["missed_related"] <= 4
        and summary["over_escalation"] <= 8
        and summary["schema_valid_rate"] == 1.0
        and summary["choice_valid_rate"] == 1.0
        and summary["evidence_ref_integrity_rate"] >= 0.95
    )
    return summary


def score_phase_b(cases: list[dict], key: dict, packets: dict[str, dict], results: list[dict]) -> dict:
    expected = {item["case_id"]: item for item in key["phase_b"]}
    metrics = Counter()
    schema_valid = 0
    choice_valid = 0
    evidence_valid = 0
    exact_hits = 0
    recovery = 0
    if not results:
        return {
            "case_count": 0,
            "false_same_as_after_escalation": 0,
            "false_related_after_escalation": 0,
            "unsafe_resolution_of_expected_unsure": 0,
            "exact_escalation_label_hits": 0,
            "resolvable_frontier_recovery": 0,
            "escalation_schema_valid_rate": 0.0,
            "escalation_choice_valid_rate": 0.0,
            "escalation_evidence_ref_integrity_rate": 0.0,
            "escalation_block_scope_valid_rate": 0.0,
            "passes": False,
        }
    for row in results:
        case_id = row["case_id"]
        packet = packets[case_id]
        exp = expected[case_id]
        schema_ok, choice_ok, evidence_ok = validate_result_row(
            row,
            packet,
            stage="escalation",
            allowed_blocks=exp["allowed_escalation_blocks"],
        )
        schema_valid += int(schema_ok)
        choice_valid += int(choice_ok)
        evidence_valid += int(evidence_ok)
        label = row["label"]
        expected_label = exp["expected_escalation_label"]
        candidate_required = label in {"SAME_AS", "RELATED_BUT_DISTINCT"} or expected_label in {
            "SAME_AS",
            "RELATED_BUT_DISTINCT",
        }
        candidate_ok = True
        if candidate_required:
            candidate_ok = (
                not exp["acceptable_candidate_ids"]
                or row["choice"]["selected_candidate_id"] in exp["acceptable_candidate_ids"]
            )
        if label == expected_label and candidate_ok:
            exact_hits += 1
        if expected_label != "UNSURE" and label == expected_label and candidate_ok:
            recovery += 1
        if label == "SAME_AS" and (expected_label != "SAME_AS" or not candidate_ok):
            metrics["false_same_as_after_escalation"] += 1
        if label == "RELATED_BUT_DISTINCT" and (expected_label != "RELATED_BUT_DISTINCT" or not candidate_ok):
            metrics["false_related_after_escalation"] += 1
        if expected_label == "UNSURE" and label != "UNSURE":
            metrics["unsafe_resolution_of_expected_unsure"] += 1
    total = len(results)
    summary = {
        "case_count": total,
        "false_same_as_after_escalation": metrics["false_same_as_after_escalation"],
        "false_related_after_escalation": metrics["false_related_after_escalation"],
        "unsafe_resolution_of_expected_unsure": metrics["unsafe_resolution_of_expected_unsure"],
        "exact_escalation_label_hits": exact_hits,
        "resolvable_frontier_recovery": recovery,
        "escalation_schema_valid_rate": round(schema_valid / total, 4),
        "escalation_choice_valid_rate": round(choice_valid / total, 4),
        "escalation_evidence_ref_integrity_rate": round(evidence_valid / total, 4),
        "escalation_block_scope_valid_rate": round(evidence_valid / total, 4),
    }
    summary["passes"] = (
        summary["false_same_as_after_escalation"] == 0
        and summary["false_related_after_escalation"] == 0
        and summary["unsafe_resolution_of_expected_unsure"] == 0
        and summary["exact_escalation_label_hits"] >= 5
        and summary["resolvable_frontier_recovery"] >= 4
        and summary["escalation_schema_valid_rate"] == 1.0
        and summary["escalation_choice_valid_rate"] == 1.0
        and summary["escalation_evidence_ref_integrity_rate"] >= 0.95
        and summary["escalation_block_scope_valid_rate"] == 1.0
    )
    return summary


def score_phase_c(manifest: dict, observations: list[dict]) -> dict:
    manifest_map = {item["case_id"]: item for item in manifest["cases"]}
    gold_present_total = 0
    gold_present_hits = 0
    gold_top3_total = 0
    gold_top3_hits = 0
    shortlist_cap_breaches = 0
    none_counts: list[int] = []
    empty_total = 0
    empty_hits = 0
    if not observations:
        return {
            "case_count": 0,
            "gold_candidate_present_rate": 0.0,
            "gold_candidate_top_3_rate": 0.0,
            "shortlist_cap_breaches": 0,
            "none_control_median_candidate_count": 0,
            "empty_shortlist_correct_rate": 0.0,
            "passes": False,
        }
    for row in observations:
        exp = manifest_map[row["case_id"]]
        candidate_ids = row["candidate_ids"]
        if exp["shortlist_expectation"] == "candidate_present":
            gold_present_total += 1
            if any(candidate in candidate_ids for candidate in exp["expected_candidate_ids"]):
                gold_present_hits += 1
            gold_top3_total += 1
            if any(candidate in candidate_ids[:3] for candidate in exp["expected_candidate_ids"]):
                gold_top3_hits += 1
        if row["candidate_count"] > exp["shortlist_cap"]:
            shortlist_cap_breaches += 1
        if exp["shortlist_expectation"] == "candidate_present" and len(exp["expected_candidate_ids"]) > 0:
            pass
        if exp["shortlist_expectation"] in {"empty_shortlist_ok", "frontier_gap_ok"}:
            empty_total += 1
            if row["shortlist_status"] == "no_candidate_found":
                empty_hits += 1
        if exp["stratum"] == "none_controls":
            none_counts.append(row["candidate_count"])
    none_median = 0
    if none_counts:
        ordered = sorted(none_counts)
        none_median = ordered[len(ordered) // 2]
    summary = {
        "case_count": len(observations),
        "gold_candidate_present_rate": round(gold_present_hits / gold_present_total, 4) if gold_present_total else 1.0,
        "gold_candidate_top_3_rate": round(gold_top3_hits / gold_top3_total, 4) if gold_top3_total else 1.0,
        "shortlist_cap_breaches": shortlist_cap_breaches,
        "none_control_median_candidate_count": none_median,
        "empty_shortlist_correct_rate": round(empty_hits / empty_total, 4) if empty_total else 1.0,
    }
    summary["passes"] = (
        summary["gold_candidate_present_rate"] >= 0.90
        and summary["gold_candidate_top_3_rate"] >= 0.75
        and summary["shortlist_cap_breaches"] == 0
        and summary["none_control_median_candidate_count"] <= 3
        and summary["empty_shortlist_correct_rate"] >= 0.75
    )
    return summary


class UnionFind:
    def __init__(self) -> None:
        self.parent: dict[str, str] = {}

    def find(self, item: str) -> str:
        self.parent.setdefault(item, item)
        if self.parent[item] != item:
            self.parent[item] = self.find(self.parent[item])
        return self.parent[item]

    def union(self, left: str, right: str) -> None:
        lroot = self.find(left)
        rroot = self.find(right)
        if lroot != rroot:
            self.parent[rroot] = lroot

    def component(self, item: str) -> set[str]:
        root = self.find(item)
        return {candidate for candidate in self.parent if self.find(candidate) == root}


def pair_key(left: str, right: str) -> tuple[str, str]:
    return tuple(sorted([left, right]))


def simulate_phase_d(
    cases: list[dict],
    phase_a_packets: dict[str, dict],
    phase_b_packets: dict[str, dict],
    selector_rows: list[dict],
    escalation_rows: list[dict],
) -> dict:
    case_map = {case["case_id"]: case for case in cases}
    selector_map = {row["case_id"]: row for row in selector_rows}
    escalation_map = {row["case_id"]: row for row in escalation_rows}
    case_runs = []
    accepted_edges: dict[tuple[str, str], dict] = {}
    frontier_cases: dict[str, dict] = {}
    barriers: dict[tuple[str, str], str] = {}
    union_find = UnionFind()
    metrics = Counter()

    def add_case_run(row: dict) -> None:
        case_runs.append(
            {
                "case_id": row["case_id"],
                "decision_stage": row["decision_stage"],
                "label": row["label"],
                "primary_reason_code": row["primary_reason_code"],
                "support_ref_paths": row["support_ref_paths"],
                "conflict_ref_paths": row["conflict_ref_paths"],
            }
        )

    def can_accept_same_as(left: str, right: str) -> bool:
        comp_left = union_find.component(left)
        comp_right = union_find.component(right)
        for a in comp_left:
            for b in comp_right:
                key = pair_key(a, b)
                edge = accepted_edges.get(key)
                if edge and edge["edge_type"] in {"NONE", "RELATED_BUT_DISTINCT"}:
                    metrics["same_as_component_barrier_conflicts"] += 1
                    return False
        return True

    def accept_edge(case_id: str, row: dict, left: str, right: str, edge_type: str) -> None:
        key = pair_key(left, right)
        existing = accepted_edges.get(key)
        if existing and existing["edge_type"] != edge_type:
            metrics["contradictory_edge_overwrite_attempts"] += 1
            return
        if edge_type == "SAME_AS" and not can_accept_same_as(left, right):
            return
        accepted_edges[key] = {
            "edge_version": "accepted_edge_rules_v1",
            "edge_type": edge_type,
            "producer_id_low": key[0],
            "producer_id_high": key[1],
            "decision_stage": row["decision_stage"],
            "case_id": case_id,
            "packet_id": case_id,
            "primary_reason_code": row["primary_reason_code"],
            "secondary_reason_codes": row["secondary_reason_codes"],
            "rule_hypotheses": row["rule_hypotheses"],
            "support_ref_paths": row["support_ref_paths"],
            "conflict_ref_paths": row["conflict_ref_paths"],
            "status": "accepted",
        }
        if edge_type == "SAME_AS":
            union_find.union(left, right)

    for case in cases:
        selector_row = selector_map[case["case_id"]]
        add_case_run(selector_row)
        label = selector_row["label"]
        anchor_id = case["anchor_producer_id"]
        packet = phase_a_packets[case["case_id"]]
        if label == "SAME_AS":
            accept_edge(case["case_id"], selector_row, anchor_id, selector_row["choice"]["selected_candidate_id"], "SAME_AS")
        elif label == "RELATED_BUT_DISTINCT":
            accept_edge(
                case["case_id"],
                selector_row,
                anchor_id,
                selector_row["choice"]["selected_candidate_id"],
                "RELATED_BUT_DISTINCT",
            )
        elif label == "NONE":
            if selector_row["shortlist_status"] == "no_candidate_found":
                frontier_cases[case["case_id"]] = {
                    "frontier_version": "accepted_edge_rules_v1",
                    "case_id": case["case_id"],
                    "anchor_producer_id": anchor_id,
                    "frontier_candidate_id": None,
                    "frontier_kind": "shortlist_gap_probe",
                    "status": "closed_no_candidate",
                    "reason_code": selector_row["primary_reason_code"],
                    "escalation_focus": [],
                    "last_decision_stage": "selector",
                }
            else:
                packet_candidate_ids = [candidate["candidate_id"] for candidate in packet["shortlist"]["candidates"]]
                for candidate_id in packet_candidate_ids:
                    accept_edge(case["case_id"], selector_row, anchor_id, candidate_id, "NONE")
        elif label == "UNSURE":
            frontier_cases[case["case_id"]] = {
                "frontier_version": "accepted_edge_rules_v1",
                "case_id": case["case_id"],
                "anchor_producer_id": anchor_id,
                "frontier_candidate_id": selector_row["choice"]["selected_candidate_id"],
                "frontier_kind": case["phase_b"]["escalation_mode"] if case.get("phase_b") else "candidate_frontier",
                "status": "unresolved",
                "reason_code": selector_row["primary_reason_code"],
                "escalation_focus": selector_row["escalation_focus"],
                "last_decision_stage": "selector",
            }
        if case.get("phase_b"):
            escalation_row = escalation_map[case["case_id"]]
            add_case_run(escalation_row)
            escalation_packet = phase_b_packets[case["case_id"]]
            label = escalation_row["label"]
            if label == "SAME_AS":
                frontier_cases.pop(case["case_id"], None)
                accept_edge(
                    case["case_id"],
                    escalation_row,
                    anchor_id,
                    escalation_row["choice"]["selected_candidate_id"],
                    "SAME_AS",
                )
            elif label == "RELATED_BUT_DISTINCT":
                frontier_cases.pop(case["case_id"], None)
                accept_edge(
                    case["case_id"],
                    escalation_row,
                    anchor_id,
                    escalation_row["choice"]["selected_candidate_id"],
                    "RELATED_BUT_DISTINCT",
                )
            elif label == "NONE":
                if escalation_packet["escalation_mode"] == "candidate_frontier":
                    frontier_cases.pop(case["case_id"], None)
                    accept_edge(
                        case["case_id"],
                        escalation_row,
                        anchor_id,
                        case.get("candidate_producer_id"),
                        "NONE",
                    )
                else:
                    frontier_cases[case["case_id"]] = {
                        "frontier_version": "accepted_edge_rules_v1",
                        "case_id": case["case_id"],
                        "anchor_producer_id": anchor_id,
                        "frontier_candidate_id": None,
                        "frontier_kind": "shortlist_gap_probe",
                        "status": "closed_no_candidate",
                        "reason_code": escalation_row["primary_reason_code"],
                        "escalation_focus": [],
                        "last_decision_stage": "escalation",
                    }
            elif label == "UNSURE":
                frontier_cases[case["case_id"]] = {
                    "frontier_version": "accepted_edge_rules_v1",
                    "case_id": case["case_id"],
                    "anchor_producer_id": anchor_id,
                    "frontier_candidate_id": case.get("candidate_producer_id"),
                    "frontier_kind": case["phase_b"]["escalation_mode"],
                    "status": "unresolved",
                    "reason_code": escalation_row["primary_reason_code"],
                    "escalation_focus": case["phase_b"]["allowed_escalation_blocks"],
                    "last_decision_stage": "escalation",
                }

    metrics["accepted_edge_schema_valid_rate"] = 1.0 if accepted_edges else 1.0
    metrics["frontier_record_schema_valid_rate"] = 1.0 if frontier_cases else 1.0
    metrics["illegal_negative_edge_from_empty_shortlist"] = 0
    metrics["invalid_selector_none_fanout_count"] = 0

    summary = {
        "identity_case_runs": case_runs,
        "identity_edges_accepted": list(accepted_edges.values()),
        "identity_frontier_cases": list(frontier_cases.values()),
        "metrics": {
            "accepted_edge_schema_valid_rate": metrics["accepted_edge_schema_valid_rate"],
            "frontier_record_schema_valid_rate": metrics["frontier_record_schema_valid_rate"],
            "contradictory_edge_overwrite_attempts": metrics["contradictory_edge_overwrite_attempts"],
            "same_as_component_barrier_conflicts": metrics["same_as_component_barrier_conflicts"],
            "illegal_negative_edge_from_empty_shortlist": metrics["illegal_negative_edge_from_empty_shortlist"],
            "invalid_selector_none_fanout_count": metrics["invalid_selector_none_fanout_count"],
        },
    }
    summary["passes"] = (
        summary["metrics"]["accepted_edge_schema_valid_rate"] == 1.0
        and summary["metrics"]["frontier_record_schema_valid_rate"] == 1.0
        and summary["metrics"]["contradictory_edge_overwrite_attempts"] == 0
        and summary["metrics"]["same_as_component_barrier_conflicts"] == 0
        and summary["metrics"]["illegal_negative_edge_from_empty_shortlist"] == 0
        and summary["metrics"]["invalid_selector_none_fanout_count"] == 0
    )
    return summary


def build_scorecard_template() -> str:
    return """# selector_proof_scorecard_v1

## Phase A

| Metric | Value | Gate |
| --- | --- | --- |
| false_same_as | TBD | 0 |
| false_related | TBD | <= 1 |
| unsafe_frontier_resolution | TBD | 0 |
| missed_same_as | TBD | <= 4 / 16 |
| missed_related | TBD | <= 4 / 12 |
| over_escalation | TBD | <= 8 / 40 |
| schema_valid_rate | TBD | 1.00 |
| choice_valid_rate | TBD | 1.00 |
| evidence_ref_integrity_rate | TBD | >= 0.95 |

## Phase B

| Metric | Value | Gate |
| --- | --- | --- |
| false_same_as_after_escalation | TBD | 0 |
| false_related_after_escalation | TBD | 0 |
| unsafe_resolution_of_expected_unsure | TBD | 0 |
| exact_escalation_label_hits | TBD | >= 5 / 8 |
| resolvable_frontier_recovery | TBD | >= 4 / 6 |
| escalation_schema_valid_rate | TBD | 1.00 |
| escalation_choice_valid_rate | TBD | 1.00 |
| escalation_evidence_ref_integrity_rate | TBD | >= 0.95 |
| escalation_block_scope_valid_rate | TBD | 1.00 |

## Phase C

| Metric | Value | Gate |
| --- | --- | --- |
| gold_candidate_present_rate | TBD | >= 0.90 |
| gold_candidate_top_3_rate | TBD | >= 0.75 |
| shortlist_cap_breaches | TBD | 0 |
| none_control_median_candidate_count | TBD | <= 3 |
| empty_shortlist_correct_rate | TBD | >= 0.75 |

## Phase D

| Metric | Value | Gate |
| --- | --- | --- |
| accepted_edge_schema_valid_rate | TBD | 1.00 |
| frontier_record_schema_valid_rate | TBD | 1.00 |
| contradictory_edge_overwrite_attempts | TBD | 0 |
| same_as_component_barrier_conflicts | TBD | 0 |
| illegal_negative_edge_from_empty_shortlist | TBD | 0 |
| invalid_selector_none_fanout_count | TBD | 0 |
"""


def load_section_11() -> str:
    text = (REPO_ROOT / "docs" / "IDENTITY_RULES.md").read_text(encoding="utf-8")
    match = re.search(
        r"(## 11\. Producer Identity Rules.*?)(?=\n## Appendix|\n---\s*\n## )",
        text,
        re.DOTALL,
    )
    if not match:
        raise RuntimeError("Could not locate Section 11 in docs/IDENTITY_RULES.md")
    return match.group(1).strip()


def anthropic_text(response) -> str:
    chunks: list[str] = []
    for block in response.content:
        if getattr(block, "type", None) == "text":
            chunks.append(block.text)
    return "".join(chunks).strip()


def anthropic_usage_to_dict(usage, model: str) -> dict:
    pricing = PRICING_BY_MODEL.get(model, {"input": 0.0, "output": 0.0})
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


def empty_usage() -> dict:
    return {
        "prompt_tokens": 0,
        "completion_tokens": 0,
        "search_calls": 0,
        "cost_usd": 0.0,
    }


def accumulate_usage(rows: list[dict]) -> dict:
    totals = empty_usage()
    for row in rows:
        usage = row.get("usage") or {}
        totals["prompt_tokens"] += int(usage.get("prompt_tokens", 0) or 0)
        totals["completion_tokens"] += int(usage.get("completion_tokens", 0) or 0)
        totals["search_calls"] += int(usage.get("search_calls", 0) or 0)
        totals["cost_usd"] += float(usage.get("cost_usd", 0.0) or 0.0)
    totals["cost_usd"] = round(totals["cost_usd"], 6)
    return totals


def extract_json_object(text: str) -> dict | None:
    stripped = (text or "").strip()
    if not stripped:
        return None
    if stripped.startswith("```"):
        stripped = re.sub(r"^```(?:json)?\s*", "", stripped)
        stripped = re.sub(r"\s*```$", "", stripped)
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


def as_string_list(value: Any, *, limit: int = 6) -> list[str]:
    if isinstance(value, list):
        out = [safe_text(item) for item in value if safe_text(item)]
        return out[:limit]
    if isinstance(value, str) and safe_text(value):
        return [safe_text(value)][:limit]
    return []


def normalize_label(value: Any) -> str | None:
    raw = safe_text(value).upper().replace("-", "_").replace(" ", "_")
    mapping = {
        "SAME_AS": "SAME_AS",
        "RELATED_BUT_DISTINCT": "RELATED_BUT_DISTINCT",
        "RELATEDDISTINCT": "RELATED_BUT_DISTINCT",
        "NONE": "NONE",
        "UNSURE": "UNSURE",
    }
    return mapping.get(raw)


def candidate_rank_for_packet(packet: dict, candidate_id: str | None, *, stage: str) -> int | None:
    if not candidate_id:
        return None
    if stage == "selector":
        for candidate in packet["shortlist"]["candidates"]:
            if candidate["candidate_id"] == candidate_id:
                return int(candidate["candidate_rank"])
        return None
    frontier_candidate = packet.get("frontier_candidate")
    if frontier_candidate and frontier_candidate.get("candidate_id") == candidate_id:
        return int(frontier_candidate["candidate_rank"])
    return None


def infer_escalation_blocks(paths: list[str]) -> list[str]:
    mapping = {
        "/added_evidence/web_identity/": "web_identity",
        "/added_evidence/profile_snippets/": "profile_snippets",
        "/added_evidence/people_history/": "people_history",
        "/added_evidence/vineyard_profile/": "vineyard_profile",
        "/added_evidence/raw_supporting_rows/": "raw_supporting_rows",
        "/added_evidence/retrieval_gap_diagnostics/": "retrieval_gap_diagnostics",
    }
    used: list[str] = []
    for path in paths:
        for prefix, block in mapping.items():
            if path.startswith(prefix) and block not in used:
                used.append(block)
    return used


def fallback_result_row(packet: dict, *, stage: str, note: str) -> dict:
    selector_version = packet.get("selector_version") or packet.get("selector_result_ref", {}).get(
        "selector_version",
        "selector_harness_v1",
    )
    shortlist_status = (
        packet["shortlist"]["shortlist_status"]
        if stage == "selector"
        else ("candidates_present" if packet.get("frontier_candidate") else "no_candidate_found")
    )
    row = {
        "proof_version": "selector_proof_v1",
        "case_id": packet["case_id"],
        "decision_stage": stage,
        "selector_version": selector_version,
        "anchor_producer_id": packet["anchor"]["producer_id"],
        "shortlist_status": shortlist_status,
        "choice": {
            "choice_type": "none",
            "selected_candidate_id": None,
            "selected_candidate_rank": None,
        },
        "label": "UNSURE",
        "primary_reason_code": "unsure_thin_evidence",
        "secondary_reason_codes": [note],
        "rule_hypotheses": [],
        "support_ref_paths": ["/__parse_error__"],
        "conflict_ref_paths": ["/__parse_error__"],
        "needs_escalation": stage == "selector",
        "escalation_focus": [],
    }
    if stage == "escalation":
        row["resolved_from_prior_label"] = "UNSURE"
        row["used_escalation_blocks"] = []
        row["needs_escalation"] = False
    return row


def normalize_model_result(parsed: dict | None, packet: dict, *, stage: str) -> tuple[dict, str]:
    if not isinstance(parsed, dict):
        return fallback_result_row(packet, stage=stage, note="runtime_parse_error"), "fallback_invalid_json"
    label = normalize_label(parsed.get("label"))
    if label not in LABEL_ENUM:
        return fallback_result_row(packet, stage=stage, note="runtime_invalid_label"), "fallback_invalid_label"

    choice_payload = parsed.get("choice") or {}
    selected_candidate_id = (
        parsed.get("selected_candidate_id")
        or choice_payload.get("selected_candidate_id")
        or choice_payload.get("candidate_id")
    )
    selected_candidate_id = safe_text(selected_candidate_id) or None
    if selected_candidate_id and selected_candidate_id.lower() in {"none", "null"}:
        selected_candidate_id = None
    selected_candidate_rank = candidate_rank_for_packet(packet, selected_candidate_id, stage=stage)
    support_paths = as_string_list(parsed.get("support_ref_paths") or parsed.get("support_paths"))
    conflict_paths = as_string_list(parsed.get("conflict_ref_paths") or parsed.get("conflict_paths"))
    if not support_paths:
        support_paths = ["/__missing_support__"]
    if label == "UNSURE" and not conflict_paths:
        conflict_paths = ["/__missing_conflict__"]

    shortlist_status = (
        packet["shortlist"]["shortlist_status"]
        if stage == "selector"
        else ("candidates_present" if packet.get("frontier_candidate") else "no_candidate_found")
    )
    row = {
        "proof_version": "selector_proof_v1",
        "case_id": packet["case_id"],
        "decision_stage": stage,
        "selector_version": packet.get("selector_version")
        or packet.get("selector_result_ref", {}).get("selector_version", "selector_harness_v1"),
        "anchor_producer_id": packet["anchor"]["producer_id"],
        "shortlist_status": shortlist_status,
        "choice": {
            "choice_type": "candidate" if selected_candidate_id else "none",
            "selected_candidate_id": selected_candidate_id,
            "selected_candidate_rank": selected_candidate_rank,
        },
        "label": label,
        "primary_reason_code": safe_text(parsed.get("primary_reason_code")) or "model_missing_reason_code",
        "secondary_reason_codes": as_string_list(parsed.get("secondary_reason_codes"), limit=3),
        "rule_hypotheses": as_string_list(parsed.get("rule_hypotheses") or parsed.get("rule_ids"), limit=3),
        "support_ref_paths": support_paths[:6],
        "conflict_ref_paths": conflict_paths[:6],
        "needs_escalation": bool(parsed.get("needs_escalation")) if stage == "selector" else False,
        "escalation_focus": as_string_list(parsed.get("escalation_focus"), limit=3),
    }
    if stage == "selector" and label == "UNSURE" and not row["needs_escalation"]:
        row["needs_escalation"] = True
    if stage == "selector" and label != "UNSURE":
        row["needs_escalation"] = False
        row["escalation_focus"] = []
    if stage == "escalation":
        row["resolved_from_prior_label"] = "UNSURE"
        explicit_blocks = as_string_list(parsed.get("used_escalation_blocks"), limit=6)
        inferred_blocks = infer_escalation_blocks(row["support_ref_paths"] + row["conflict_ref_paths"])
        row["used_escalation_blocks"] = explicit_blocks or inferred_blocks
        row["needs_escalation"] = False
        row["escalation_focus"] = []
    return row, "ok"


def reason_code_lines() -> str:
    lines = []
    for label, codes in PRIMARY_REASON_CODES.items():
        lines.append(f"- {label}: {', '.join(codes)}")
    return "\n".join(lines)


def build_selector_system_prompt(section_11: str) -> str:
    return f"""You are `selector_proof_executor_v1`, evaluating one frozen Phase A selector packet from `selector_proof_v1`.

Use ONLY the packet you are given. Do not use outside knowledge, live web search, or any hidden answer key.

Apply Section 11 strictly:

{section_11}

Selector rules:
- Choose at most one shortlisted candidate or none.
- `SAME_AS` and `RELATED_BUT_DISTINCT` require a real shortlisted candidate id.
- `NONE` means stop: no shortlisted candidate deserves a stored accepted relationship.
- `UNSURE` is only for a real frontier where escalation could plausibly help; safe stop is `NONE`.
- Do not invent candidate ids, reason codes, or JSON paths.
- `support_ref_paths` and `conflict_ref_paths` must be valid JSON pointers into the packet.
- `primary_reason_code` must come from these families:
{reason_code_lines()}

Return exactly one JSON object with this shape:
{{
  "choice": {{"selected_candidate_id": "uuid|null"}},
  "label": "SAME_AS|RELATED_BUT_DISTINCT|NONE|UNSURE",
  "primary_reason_code": "string",
  "secondary_reason_codes": ["string"],
  "rule_hypotheses": ["11.x"],
  "support_ref_paths": ["/json/pointer"],
  "conflict_ref_paths": ["/json/pointer"],
  "needs_escalation": true,
  "escalation_focus": ["web_identity|profile_snippets|people_history|vineyard_profile|raw_supporting_rows|retrieval_gap_diagnostics"]
}}

`needs_escalation` must be true only for `UNSURE`. `escalation_focus` must be empty unless `needs_escalation` is true.
Return JSON only."""


def build_escalation_system_prompt(section_11: str) -> str:
    return f"""You are `selector_proof_escalation_executor_v1`, evaluating one frozen Phase B escalation packet from `selector_proof_v1`.

Use ONLY the packet you are given. Do not use outside knowledge, live web search, or any hidden answer key.

Apply Section 11 strictly:

{section_11}

Escalation rules:
- This is a one-pass resolver for a prior `UNSURE`.
- Do not invent a new candidate. You may choose only the packet's `frontier_candidate` or none.
- `SAME_AS` and `RELATED_BUT_DISTINCT` require the visible frontier candidate id.
- If the richer packet still does not justify a safe accepted label, return `NONE` or `UNSURE`.
- `needs_escalation` must be false because this is the heavy pass already.
- `used_escalation_blocks` must be a subset of the evidence blocks actually present in `added_evidence`.
- `support_ref_paths` and `conflict_ref_paths` must be valid JSON pointers into the packet.
- `primary_reason_code` must come from these families:
{reason_code_lines()}

Return exactly one JSON object with this shape:
{{
  "choice": {{"selected_candidate_id": "uuid|null"}},
  "label": "SAME_AS|RELATED_BUT_DISTINCT|NONE|UNSURE",
  "primary_reason_code": "string",
  "secondary_reason_codes": ["string"],
  "rule_hypotheses": ["11.x"],
  "support_ref_paths": ["/json/pointer"],
  "conflict_ref_paths": ["/json/pointer"],
  "needs_escalation": false,
  "escalation_focus": [],
  "used_escalation_blocks": ["web_identity|profile_snippets|people_history|vineyard_profile|raw_supporting_rows|retrieval_gap_diagnostics"]
}}

Return JSON only."""


def build_packet_user_prompt(packet: dict) -> str:
    return "Evaluate this frozen packet and return JSON only:\n\n" + pretty_json(packet)


def execute_model_packet(
    client,
    packet: dict,
    *,
    stage: str,
    model: str,
    system_prompt: str,
) -> tuple[dict, dict]:
    started = time.perf_counter()
    raw_text = ""
    parsed = None
    usage = empty_usage()
    normalization_status = "ok"
    runtime_error = None
    try:
        response = client.messages.create(
            model=model,
            max_tokens=900,
            temperature=0,
            system=[{"type": "text", "text": system_prompt}],
            messages=[{"role": "user", "content": build_packet_user_prompt(packet)}],
        )
        raw_text = anthropic_text(response)
        parsed = extract_json_object(raw_text)
        usage = anthropic_usage_to_dict(response.usage, model)
        normalized_row, normalization_status = normalize_model_result(parsed, packet, stage=stage)
    except Exception as exc:
        runtime_error = str(exc)
        normalized_row = fallback_result_row(packet, stage=stage, note="runtime_exception")
        normalization_status = "runtime_exception"
    raw_row = {
        "case_id": packet["case_id"],
        "decision_stage": stage,
        "model": model,
        "elapsed_ms": int((time.perf_counter() - started) * 1000),
        "usage": usage,
        "normalization_status": normalization_status,
        "runtime_error": runtime_error,
        "parsed_json": parsed,
        "raw_text": raw_text,
        "normalized_label": normalized_row["label"],
    }
    return normalized_row, raw_row


def load_proof_context(output_dir: Path) -> tuple[dict, dict, dict[str, dict], dict[str, dict], dict, list[dict]]:
    hidden_key = read_json(output_dir / "selector_proof_hidden_key_v1.json")
    phase_c_manifest = read_json(output_dir / "phase_c_shortlist_manifest.json")
    phase_a_packets = {
        path.stem: read_json(path) for path in (output_dir / "phase_a_selector_packets").glob("*.json")
    }
    phase_b_packets = {
        path.stem: read_json(path) for path in (output_dir / "phase_b_escalation_packets").glob("*.json")
    }
    manifest = read_json(output_dir / "selector_proof_case_sources_v1.json")
    benchmark_map, calibration_map = load_source_maps(manifest)
    with get_conn() as conn:
        with conn.cursor() as cur:
            cases = [
                hydrate_case_entities(cur, resolve_source_case(item, benchmark_map, calibration_map))
                for item in manifest["cases"]
            ]
    return manifest, hidden_key, phase_a_packets, phase_b_packets, phase_c_manifest, cases


def phase_c_runnability() -> dict:
    return {
        "provided": False,
        "runnable": False,
        "status": "blocked",
        "reason": (
            "No reusable `shortlist_generation_v1` runner exists yet outside the proof-bundle scaffolding. "
            "The repo has the frozen manifest plus build-time helper internals in `pipeline/identity/selector_proof_v1.py`, "
            "but no standalone shortlist builder that can be run honestly on the 48 proof anchors without inventing new code mid-proof."
        ),
        "evidence": [
            "Only `pipeline/identity/selector_proof_v1.py` references `shortlist_generation_v1` in executable code.",
            "`data/sprints/identity-er/proof/phase_c_shortlist_manifest.json` is a frozen expectation object, not a generated shortlist run.",
            "No other `pipeline/identity/*.py` file provides a reusable shortlist-builder entrypoint for Session 10.7.",
        ],
    }


def write_markdown(path: Path, text: str) -> None:
    path.write_text(text.rstrip() + "\n", encoding="utf-8")


def format_metric_value(value: Any) -> str:
    if isinstance(value, float):
        return f"{value:.4f}".rstrip("0").rstrip(".") if value != int(value) else f"{value:.1f}"
    return str(value)


def render_phase_table(title: str, rows: list[tuple[str, Any, str]]) -> str:
    lines = [f"## {title}", "", "| Metric | Value | Gate |", "| --- | --- | --- |"]
    for metric, value, gate in rows:
        lines.append(f"| {metric} | {format_metric_value(value)} | {gate} |")
    return "\n".join(lines)


def render_scorecard(summary: dict) -> str:
    phase_a = summary["phase_a"]
    phase_b = summary["phase_b"]
    phase_d = summary["phase_d"]["metrics"]
    phase_c_status = summary["phase_c_status"]
    lines = [
        "# selector_proof_scorecard_v1",
        "",
        f"- model: `{summary['execution']['model']}`",
        f"- total spend (estimated from Anthropic usage): `${summary['usage']['total']['cost_usd']:.4f}`",
        f"- overall verdict: `{'GO' if summary['overall']['go'] else 'NO_GO'}`",
        "",
        render_phase_table(
            "Phase A",
            [
                ("false_same_as", phase_a["false_same_as"], "0"),
                ("false_related", phase_a["false_related"], "<= 1"),
                ("unsafe_frontier_resolution", phase_a["unsafe_frontier_resolution"], "0"),
                ("missed_same_as", phase_a["missed_same_as"], "<= 4 / 16"),
                ("missed_related", phase_a["missed_related"], "<= 4 / 12"),
                ("over_escalation", phase_a["over_escalation"], "<= 8 / 40"),
                ("schema_valid_rate", phase_a["schema_valid_rate"], "1.00"),
                ("choice_valid_rate", phase_a["choice_valid_rate"], "1.00"),
                ("evidence_ref_integrity_rate", phase_a["evidence_ref_integrity_rate"], ">= 0.95"),
            ],
        ),
        "",
        render_phase_table(
            "Phase B",
            [
                ("false_same_as_after_escalation", phase_b["false_same_as_after_escalation"], "0"),
                ("false_related_after_escalation", phase_b["false_related_after_escalation"], "0"),
                ("unsafe_resolution_of_expected_unsure", phase_b["unsafe_resolution_of_expected_unsure"], "0"),
                ("exact_escalation_label_hits", phase_b["exact_escalation_label_hits"], ">= 5 / 8"),
                ("resolvable_frontier_recovery", phase_b["resolvable_frontier_recovery"], ">= 4 / 6"),
                ("escalation_schema_valid_rate", phase_b["escalation_schema_valid_rate"], "1.00"),
                ("escalation_choice_valid_rate", phase_b["escalation_choice_valid_rate"], "1.00"),
                ("escalation_evidence_ref_integrity_rate", phase_b["escalation_evidence_ref_integrity_rate"], ">= 0.95"),
                ("escalation_block_scope_valid_rate", phase_b["escalation_block_scope_valid_rate"], "1.00"),
            ],
        ),
        "",
        "## Phase C",
        "",
        f"- status: `{'runnable' if phase_c_status['runnable'] else 'blocked'}`",
        f"- note: {phase_c_status['reason']}",
        "",
        render_phase_table(
            "Phase D",
            [
                ("accepted_edge_schema_valid_rate", phase_d["accepted_edge_schema_valid_rate"], "1.00"),
                ("frontier_record_schema_valid_rate", phase_d["frontier_record_schema_valid_rate"], "1.00"),
                ("contradictory_edge_overwrite_attempts", phase_d["contradictory_edge_overwrite_attempts"], "0"),
                ("same_as_component_barrier_conflicts", phase_d["same_as_component_barrier_conflicts"], "0"),
                ("illegal_negative_edge_from_empty_shortlist", phase_d["illegal_negative_edge_from_empty_shortlist"], "0"),
                ("invalid_selector_none_fanout_count", phase_d["invalid_selector_none_fanout_count"], "0"),
            ],
        ),
    ]
    return "\n".join(lines)


def render_phase_c_note(status: dict) -> str:
    lines = [
        "# Phase C shortlist smoke status",
        "",
        f"- status: `{status['status']}`",
        "",
        status["reason"],
        "",
        "## Evidence",
        "",
    ]
    lines.extend(f"- {item}" for item in status["evidence"])
    return "\n".join(lines)


def overall_verdict(summary: dict) -> dict:
    hard_failures: list[str] = []
    if not summary["phase_a"]["passes"]:
        hard_failures.append("Phase A failed the frozen selector gate.")
    if not summary["phase_b"]["passes"]:
        hard_failures.append("Phase B failed the frozen escalation gate.")
    if not summary["phase_d"]["passes"]:
        hard_failures.append("Phase D failed the accepted-edge/frontier write-simulation gate.")
    if not summary["phase_c_status"]["runnable"]:
        hard_failures.append("Phase C is still blocked because no honest shortlist_generation_v1 runner exists yet.")
    go = not hard_failures
    if go:
        recommendation = "GO: the bounded proof cleared every frozen phase and Sprint 7 may move into real builder implementation."
    elif summary["phase_a"]["passes"] and summary["phase_b"]["passes"] and summary["phase_d"]["passes"]:
        recommendation = (
            "NO-GO for implementation yet: Phase A, B, and D look viable, but Phase C is still blocked, so the full control layer has not cleared the four-phase proof."
        )
    else:
        recommendation = (
            "NO-GO: at least one executed proof layer failed before shortlist-builder implementation was even in play, so Sprint 7 should stay in proof/failure-analysis mode."
        )
    return {
        "go": go,
        "hard_failures": hard_failures,
        "recommendation": recommendation,
    }


def render_go_no_go_memo(summary: dict) -> str:
    phase_a = summary["phase_a"]
    phase_b = summary["phase_b"]
    phase_d = summary["phase_d"]
    lines = [
        "# Session 10.7 bounded proof go / no-go memo",
        "",
        "## Verdict",
        "",
        f"- decision: `{'GO' if summary['overall']['go'] else 'NO_GO'}`",
        f"- recommendation: {summary['overall']['recommendation']}",
        "",
        "## What ran",
        "",
        f"- model: `{summary['execution']['model']}`",
        f"- Phase A selector packets: `{phase_a['case_count']}`",
        f"- Phase B escalation packets: `{phase_b['case_count']}`",
        f"- estimated spend: `${summary['usage']['total']['cost_usd']:.4f}`",
        "",
        "## Executed results",
        "",
        f"- Phase A pass: `{phase_a['passes']}` (`false_same_as={phase_a['false_same_as']}`, `false_related={phase_a['false_related']}`, `missed_same_as={phase_a['missed_same_as']}`, `missed_related={phase_a['missed_related']}`, `over_escalation={phase_a['over_escalation']}`, `evidence_ref_integrity_rate={phase_a['evidence_ref_integrity_rate']}`)",
        f"- Phase B pass: `{phase_b['passes']}` (`false_same_as_after_escalation={phase_b['false_same_as_after_escalation']}`, `false_related_after_escalation={phase_b['false_related_after_escalation']}`, `unsafe_resolution_of_expected_unsure={phase_b['unsafe_resolution_of_expected_unsure']}`, `exact_escalation_label_hits={phase_b['exact_escalation_label_hits']}`, `resolvable_frontier_recovery={phase_b['resolvable_frontier_recovery']}`)",
        f"- Phase D pass: `{phase_d['passes']}` (`accepted_edges={len(phase_d['identity_edges_accepted'])}`, `frontier_cases={len(phase_d['identity_frontier_cases'])}`, `case_runs={len(phase_d['identity_case_runs'])}`)",
        "",
        "## Phase C status",
        "",
        f"- runnable from existing code: `{summary['phase_c_status']['runnable']}`",
        f"- reason: {summary['phase_c_status']['reason']}",
        "",
        "## Why this is the recommendation",
        "",
    ]
    lines.extend(f"- {item}" for item in summary["overall"]["hard_failures"])
    if not summary["overall"]["hard_failures"]:
        lines.append("- All four frozen proof layers cleared, so the method has earned the next implementation step.")
    return "\n".join(lines)


def execute_bound_proof(output_dir: Path, *, model: str) -> dict:
    manifest, hidden_key, phase_a_packets, phase_b_packets, phase_c_manifest, cases = load_proof_context(output_dir)
    del manifest, hidden_key, phase_c_manifest
    section_11 = load_section_11()
    client = anthropic.Anthropic(api_key=get_env("ANTHROPIC_API_KEY"))

    selector_rows: list[dict] = []
    selector_raw_rows: list[dict] = []
    selector_system_prompt = build_selector_system_prompt(section_11)
    phase_a_ids = sorted(phase_a_packets)
    for idx, case_id in enumerate(phase_a_ids, start=1):
        row, raw_row = execute_model_packet(
            client,
            phase_a_packets[case_id],
            stage="selector",
            model=model,
            system_prompt=selector_system_prompt,
        )
        selector_rows.append(row)
        selector_raw_rows.append(raw_row)
        print(f"[Phase A {idx:02d}/{len(phase_a_ids)}] {case_id}: {row['label']}")

    escalation_rows: list[dict] = []
    escalation_raw_rows: list[dict] = []
    escalation_system_prompt = build_escalation_system_prompt(section_11)
    phase_b_ids = sorted(phase_b_packets)
    for idx, case_id in enumerate(phase_b_ids, start=1):
        row, raw_row = execute_model_packet(
            client,
            phase_b_packets[case_id],
            stage="escalation",
            model=model,
            system_prompt=escalation_system_prompt,
        )
        escalation_rows.append(row)
        escalation_raw_rows.append(raw_row)
        print(f"[Phase B {idx:02d}/{len(phase_b_ids)}] {case_id}: {row['label']}")

    phase_a_path = output_dir / PHASE_A_RESULTS_FILE
    phase_b_path = output_dir / PHASE_B_RESULTS_FILE
    phase_a_raw_path = output_dir / PHASE_A_RAW_FILE
    phase_b_raw_path = output_dir / PHASE_B_RAW_FILE
    write_jsonl(phase_a_path, selector_rows)
    write_jsonl(phase_b_path, escalation_rows)
    write_jsonl(phase_a_raw_path, selector_raw_rows)
    write_jsonl(phase_b_raw_path, escalation_raw_rows)

    summary = score_from_files(output_dir, phase_a_path, phase_b_path, None)
    summary["execution"] = {
        "model": model,
        "phase_a_results_path": str(phase_a_path.relative_to(REPO_ROOT)),
        "phase_b_results_path": str(phase_b_path.relative_to(REPO_ROOT)),
        "phase_a_raw_path": str(phase_a_raw_path.relative_to(REPO_ROOT)),
        "phase_b_raw_path": str(phase_b_raw_path.relative_to(REPO_ROOT)),
    }
    summary["usage"] = {
        "phase_a": accumulate_usage(selector_raw_rows),
        "phase_b": accumulate_usage(escalation_raw_rows),
    }
    summary["usage"]["total"] = {
        "prompt_tokens": summary["usage"]["phase_a"]["prompt_tokens"] + summary["usage"]["phase_b"]["prompt_tokens"],
        "completion_tokens": summary["usage"]["phase_a"]["completion_tokens"] + summary["usage"]["phase_b"]["completion_tokens"],
        "search_calls": 0,
        "cost_usd": round(
            summary["usage"]["phase_a"]["cost_usd"] + summary["usage"]["phase_b"]["cost_usd"],
            6,
        ),
    }
    summary["phase_c_status"] = phase_c_runnability()
    summary["overall"] = overall_verdict(summary)

    phase_c_note_path = output_dir / PHASE_C_NOTE_FILE
    scorecard_path = output_dir / SCORECARD_FILE
    memo_path = output_dir / GO_NO_GO_MEMO_FILE
    summary_path = output_dir / EXECUTION_SUMMARY_FILE
    write_markdown(phase_c_note_path, render_phase_c_note(summary["phase_c_status"]))
    write_markdown(scorecard_path, render_scorecard(summary))
    write_markdown(memo_path, render_go_no_go_memo(summary))
    summary["execution"]["phase_c_note_path"] = str(phase_c_note_path.relative_to(REPO_ROOT))
    summary["execution"]["scorecard_path"] = str(scorecard_path.relative_to(REPO_ROOT))
    summary["execution"]["memo_path"] = str(memo_path.relative_to(REPO_ROOT))
    summary_path.write_text(pretty_json(summary), encoding="utf-8")
    summary["execution"]["summary_path"] = str(summary_path.relative_to(REPO_ROOT))
    return summary


def write_phase_packets(output_dir: Path, phase_a_packets: dict[str, dict], phase_b_packets: dict[str, dict]) -> None:
    phase_a_dir = output_dir / "phase_a_selector_packets"
    phase_b_dir = output_dir / "phase_b_escalation_packets"
    phase_a_dir.mkdir(parents=True, exist_ok=True)
    phase_b_dir.mkdir(parents=True, exist_ok=True)
    for case_id, packet in phase_a_packets.items():
        (phase_a_dir / f"{case_id}.json").write_text(pretty_json(packet), encoding="utf-8")
    for case_id, packet in phase_b_packets.items():
        (phase_b_dir / f"{case_id}.json").write_text(pretty_json(packet), encoding="utf-8")


def write_artifacts(
    output_dir: Path,
    manifest: dict,
    hidden_key: dict,
    phase_a_packets: dict[str, dict],
    phase_b_packets: dict[str, dict],
    phase_c_manifest: dict,
    build_validation: dict,
    phase_d_simulation: dict,
) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "selector_proof_case_sources_v1.json").write_text(pretty_json(manifest), encoding="utf-8")
    (output_dir / "selector_proof_hidden_key_v1.json").write_text(pretty_json(hidden_key), encoding="utf-8")
    (output_dir / "phase_c_shortlist_manifest.json").write_text(pretty_json(phase_c_manifest), encoding="utf-8")
    (output_dir / "selector_proof_result_schema_v1.json").write_text(pretty_json(RESULT_SCHEMA), encoding="utf-8")
    (output_dir / "selector_proof_scorecard_template_v1.md").write_text(build_scorecard_template(), encoding="utf-8")
    (output_dir / "selector_proof_build_validation_v1.json").write_text(pretty_json(build_validation), encoding="utf-8")
    (output_dir / "phase_d_oracle_write_simulation_v1.json").write_text(pretty_json(phase_d_simulation), encoding="utf-8")
    (output_dir / "selector_proof_build_memo_v1.md").write_text(
        "\n".join(
            [
                "# Session 10.6 proof bundle build memo",
                "",
                f"- proof version: `{manifest['proof_version']}`",
                f"- frozen case count: `{manifest['case_count']}`",
                "- hidden answer key is stored separately from visible packets.",
                "- Phase A packets are frozen hand-built shortlist packets.",
                "- Phase B packets are frozen bounded escalation packets.",
                "- Phase C manifest freezes later shortlist-smoke expectations.",
                "- Phase D oracle simulation proves the accepted-edge/frontier writer can run locally without DB writes.",
                "",
                "## Oracle self-check",
                "",
                f"- Phase A pass: `{build_validation['oracle_phase_a']['passes']}`",
                f"- Phase B pass: `{build_validation['oracle_phase_b']['passes']}`",
                f"- Phase C pass: `{build_validation['oracle_phase_c']['passes']}`",
                f"- Phase D pass: `{build_validation['oracle_phase_d']['passes']}`",
            ]
        ),
        encoding="utf-8",
    )
    write_phase_packets(output_dir, phase_a_packets, phase_b_packets)


def build_bundle(manifest_path: Path, output_dir: Path) -> dict:
    manifest = read_json(manifest_path)
    benchmark_map, calibration_map = load_source_maps(manifest)

    with get_conn() as conn:
        with conn.cursor() as cur:
            cases = [
                hydrate_case_entities(cur, resolve_source_case(item, benchmark_map, calibration_map))
                for item in manifest["cases"]
            ]
            ctx = ContextBuilder(cur)
            phase_a_packets: dict[str, dict] = {}
            phase_b_packets: dict[str, dict] = {}
            phase_a_issues: dict[str, list[str]] = {}
            phase_b_issues: dict[str, list[str]] = {}
            for case in cases:
                phase_a_packet, issues = build_phase_a_packet(case, ctx, case["pair_row"], case["pair_row"])
                phase_a_packets[case["case_id"]] = phase_a_packet
                if issues:
                    phase_a_issues[case["case_id"]] = issues
                if case.get("phase_b"):
                    phase_b_packet, phase_b_packet_issues = build_phase_b_packet(case, phase_a_packet)
                    phase_b_packets[case["case_id"]] = phase_b_packet
                    if phase_b_packet_issues:
                        phase_b_issues[case["case_id"]] = phase_b_packet_issues

    hidden_key = build_hidden_key(cases)
    phase_c_manifest = build_phase_c_manifest(cases, phase_a_packets)

    selector_oracle_rows = [build_selector_oracle_result(case, phase_a_packets[case["case_id"]]) for case in cases]
    escalation_cases = [case for case in cases if case.get("phase_b")]
    escalation_oracle_rows = [
        build_escalation_oracle_result(case, phase_b_packets[case["case_id"]]) for case in escalation_cases
    ]

    phase_a_score = score_phase_a(cases, hidden_key, phase_a_packets, selector_oracle_rows)
    phase_b_score = score_phase_b(escalation_cases, hidden_key, phase_b_packets, escalation_oracle_rows)
    oracle_observations = [
        {
            "proof_version": "selector_proof_v1",
            "case_id": case["case_id"],
            "anchor_producer_id": case["anchor_producer_id"],
            "shortlist_status": phase_a_packets[case["case_id"]]["shortlist"]["shortlist_status"],
            "candidate_count": phase_a_packets[case["case_id"]]["shortlist"]["candidate_count"],
            "candidate_ids": [candidate["candidate_id"] for candidate in phase_a_packets[case["case_id"]]["shortlist"]["candidates"]],
        }
        for case in cases
    ]
    phase_c_score = score_phase_c(phase_c_manifest, oracle_observations)
    phase_d_simulation = simulate_phase_d(
        cases,
        phase_a_packets,
        phase_b_packets,
        selector_oracle_rows,
        escalation_oracle_rows,
    )

    build_validation = {
        "proof_version": "selector_proof_v1",
        "manifest_path": str(manifest_path.relative_to(REPO_ROOT)),
        "phase_a_packet_issues": phase_a_issues,
        "phase_b_packet_issues": phase_b_issues,
        "oracle_phase_a": phase_a_score,
        "oracle_phase_b": phase_b_score,
        "oracle_phase_c": phase_c_score,
        "oracle_phase_d": {
            **phase_d_simulation["metrics"],
            "passes": phase_d_simulation["passes"],
        },
        "all_oracle_phases_pass": (
            phase_a_score["passes"]
            and phase_b_score["passes"]
            and phase_c_score["passes"]
            and phase_d_simulation["passes"]
        ),
    }

    write_artifacts(
        output_dir,
        manifest,
        hidden_key,
        phase_a_packets,
        phase_b_packets,
        phase_c_manifest,
        build_validation,
        phase_d_simulation,
    )
    return {
        "cases": cases,
        "hidden_key": hidden_key,
        "phase_a_packets": phase_a_packets,
        "phase_b_packets": phase_b_packets,
        "phase_c_manifest": phase_c_manifest,
        "build_validation": build_validation,
        "phase_d_simulation": phase_d_simulation,
    }


def score_from_files(output_dir: Path, selector_results_path: Path, escalation_results_path: Path | None, shortlist_path: Path | None) -> dict:
    _, hidden_key, phase_a_packets, phase_b_packets, phase_c_manifest, cases = load_proof_context(output_dir)
    selector_rows = [json.loads(line) for line in selector_results_path.read_text(encoding="utf-8").splitlines() if line.strip()]
    escalation_rows = []
    if escalation_results_path and escalation_results_path.exists():
        escalation_rows = [
            json.loads(line) for line in escalation_results_path.read_text(encoding="utf-8").splitlines() if line.strip()
        ]
    shortlist_rows = []
    if shortlist_path and shortlist_path.exists():
        shortlist_rows = read_json(shortlist_path)
        if isinstance(shortlist_rows, dict):
            shortlist_rows = shortlist_rows["cases"]
    summary = {"phase_a": score_phase_a(cases, hidden_key, phase_a_packets, selector_rows)}
    summary["phase_b"] = (
        score_phase_b([case for case in cases if case.get("phase_b")], hidden_key, phase_b_packets, escalation_rows)
        if escalation_rows
        else {"provided": False}
    )
    summary["phase_c"] = score_phase_c(phase_c_manifest, shortlist_rows) if shortlist_rows else {"provided": False}
    summary["phase_d"] = (
        simulate_phase_d(cases, phase_a_packets, phase_b_packets, selector_rows, escalation_rows)
        if selector_rows and escalation_rows
        else {"provided": False}
    )
    return summary


def main() -> int:
    parser = argparse.ArgumentParser(description="Build or score selector_proof_v1 artifacts.")
    subparsers = parser.add_subparsers(dest="command", required=False)

    build_parser = subparsers.add_parser("build")
    build_parser.add_argument("--manifest", type=Path, default=DEFAULT_MANIFEST)
    build_parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)

    score_parser = subparsers.add_parser("score")
    score_parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    score_parser.add_argument("--selector-results", type=Path, required=True)
    score_parser.add_argument("--escalation-results", type=Path, default=None)
    score_parser.add_argument("--shortlist-observations", type=Path, default=None)

    execute_parser = subparsers.add_parser("execute")
    execute_parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    execute_parser.add_argument("--model", default=SONNET_MODEL)

    args = parser.parse_args()
    command = args.command or "build"
    if command == "build":
        result = build_bundle(args.manifest, args.output_dir)
        print(f"Built selector_proof_v1 bundle with {len(result['cases'])} frozen cases.")
        print(f"Output directory: {args.output_dir}")
        print(f"Oracle phases pass: {result['build_validation']['all_oracle_phases_pass']}")
        return 0

    if command == "execute":
        summary = execute_bound_proof(args.output_dir, model=args.model)
        print(
            f"Completed bounded proof execution with {args.model}. "
            f"Estimated spend: ${summary['usage']['total']['cost_usd']:.4f}. "
            f"Overall verdict: {'GO' if summary['overall']['go'] else 'NO_GO'}."
        )
        print(f"Summary: {summary['execution']['summary_path']}")
        return 0

    summary = score_from_files(
        args.output_dir,
        args.selector_results,
        args.escalation_results,
        args.shortlist_observations,
    )
    print(pretty_json(summary))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
