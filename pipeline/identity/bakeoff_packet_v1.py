"""
Session 5 - Build evidence_packet_v1 rows from benchmark_v1 cases.

Writes two JSONL artifacts:
1. Stored packet rows with envelope.benchmark_overlay preserved for scoring joins.
2. Model-visible packet rows with all hidden benchmark fields stripped.

The packet builder is intentionally deterministic. Missing retrieval or survivor
details are surfaced in completeness / retrieval_gaps instead of being hidden.

Run:
    python -m pipeline.identity.bakeoff_packet_v1

    python -m pipeline.identity.bakeoff_packet_v1 --limit 8
"""

from __future__ import annotations

import argparse
import json
import re
import unicodedata
from collections import Counter
from copy import deepcopy
from datetime import datetime
from pathlib import Path
from urllib.parse import urlparse

from pipeline.lib.db import get_conn


REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_BENCHMARK = REPO_ROOT / "data" / "sprints" / "dedup" / "benchmark_v1.json"
DEFAULT_OUTPUT_DIR = REPO_ROOT / "data" / "sprints" / "dedup" / "bakeoff_v1" / "packets"
HIDDEN_PACKET_KEYS = {
    "benchmark_overlay",
    "expected_verdict",
    "historical_failure_mode",
    "source_of_truth",
    "source_artifact",
    "rationale",
    "pattern_cluster",
    "stratum",
    "producer_name_a",
    "producer_name_b",
    "country_a",
    "country_b",
}
GENERIC_WRAPPER_TOKENS = {
    "and",
    "cellar",
    "cellars",
    "co",
    "company",
    "domaine",
    "domain",
    "estate",
    "estates",
    "et",
    "family",
    "fils",
    "freres",
    "llc",
    "sa",
    "sarl",
    "sons",
    "vineyard",
    "vineyards",
    "vignerons",
    "vins",
    "weingut",
    "winery",
    "wines",
}
COMMON_PLACE_TOKENS = {
    "aop",
    "ava",
    "barolo",
    "bordeaux",
    "bourgogne",
    "burgundy",
    "cabernet",
    "chablis",
    "champagne",
    "chianti",
    "grand",
    "grand cru",
    "premier",
    "premier cru",
    "reserve",
    "reserva",
    "rioja",
    "sauvignon",
    "shiraz",
}


def normalize_text(value: str | None) -> str:
    if not value:
        return ""
    ascii_text = (
        unicodedata.normalize("NFKD", value)
        .encode("ascii", "ignore")
        .decode("ascii")
        .lower()
    )
    return re.sub(r"[^a-z0-9]+", " ", ascii_text).strip()


def tokenize(value: str | None) -> list[str]:
    return [tok for tok in normalize_text(value).split() if tok]


def name_alignment_ok(expected: str, actual: str) -> bool:
    left = normalize_text(expected)
    right = normalize_text(actual)
    if not left or not right:
        return False
    if left == right or left in right or right in left:
        return True
    left_tokens = set(left.split())
    right_tokens = set(right.split())
    overlap = left_tokens & right_tokens
    return bool(overlap) and len(overlap) >= min(len(left_tokens), len(right_tokens))


def canonical_json_dumps(value: object) -> str:
    return json.dumps(value, ensure_ascii=True, sort_keys=True)


def write_jsonl(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="\n") as handle:
        for row in rows:
            handle.write(canonical_json_dumps(row))
            handle.write("\n")


def load_benchmark_cases(path: Path) -> tuple[str, list[dict]]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    return payload["benchmark_id"], payload["cases"]


def fetch_pair_row(cur, pair_id: int) -> dict | None:
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
        return None
    return {
        "id": row[0],
        "producer_id_a": row[1],
        "producer_id_b": row[2],
        "name_a": row[3],
        "name_b": row[4],
        "country": row[5],
        "similarity": float(row[6] or 0.0),
        "wines_a": row[7] or 0,
        "wines_b": row[8] or 0,
        "signals": row[9] or {},
    }


def fetch_producers(cur, producer_ids: list[str]) -> dict[str, dict]:
    cur.execute(
        """
        SELECT p.id::text,
               p.name,
               p.name_normalized,
               c.iso_code,
               c.name,
               r.name,
               a.name,
               p.website_url,
               p.year_established,
               p.producer_type,
               p.parent_producer_id::text,
               p.deleted_at,
               p.metadata,
               p.created_at
        FROM producers p
        LEFT JOIN countries c ON c.id = p.country_id
        LEFT JOIN regions r ON r.id = p.region_id
        LEFT JOIN appellations a ON a.id = p.appellation_id
        WHERE p.id = ANY(%s::uuid[])
        """,
        (producer_ids,),
    )
    producers: dict[str, dict] = {}
    for row in cur.fetchall():
        producers[row[0]] = {
            "producer_id": row[0],
            "name": row[1],
            "name_normalized": row[2],
            "country_code": row[3],
            "country_name": row[4],
            "region_name": row[5],
            "appellation_name": row[6],
            "website_url": row[7],
            "year_established": row[8],
            "producer_type": row[9],
            "parent_producer_id": row[10],
            "deleted_at": row[11].isoformat() if row[11] else None,
            "metadata": row[12] or {},
            "created_at": row[13].isoformat() if row[13] else None,
        }
    return producers


def fetch_catalog_context(cur, producer_ids: list[str]) -> dict[str, dict]:
    cur.execute(
        """
        WITH wine_base AS (
          SELECT w.id,
                 w.producer_id::text AS producer_id,
                 COALESCE(NULLIF(w.display_name, ''), NULLIF(w.name, ''), '[unnamed wine]') AS display_name,
                 COALESCE(NULLIF(w.name_normalized, ''), '') AS name_normalized,
                 a.name AS appellation_name,
                 r.name AS region_name,
                 c.iso_code AS country_code
          FROM wines w
          LEFT JOIN appellations a ON a.id = w.appellation_id
          LEFT JOIN regions r ON r.id = w.region_id
          LEFT JOIN countries c ON c.id = w.country_id
          WHERE w.producer_id = ANY(%s::uuid[])
            AND w.deleted_at IS NULL
        ),
        lwin_ids AS (
          SELECT DISTINCT entity_id::text AS wine_id
          FROM external_ids
          WHERE entity_type = 'wine' AND system = 'lwin_7'
        ),
        priced AS (
          SELECT DISTINCT wv.wine_id::text AS wine_id
          FROM wine_vintages wv
          JOIN wine_vintage_prices wvp ON wvp.wine_vintage_id = wv.id
        ),
        scored AS (
          SELECT DISTINCT wv.wine_id::text AS wine_id
          FROM wine_vintages wv
          JOIN wine_vintage_scores wvs ON wvs.wine_vintage_id = wv.id
        )
        SELECT wb.producer_id,
               wb.id::text AS wine_id,
               wb.display_name,
               wb.name_normalized,
               wb.appellation_name,
               wb.region_name,
               wb.country_code,
               (l.wine_id IS NOT NULL) AS has_lwin,
               (pr.wine_id IS NOT NULL) AS has_price,
               (sc.wine_id IS NOT NULL) AS has_score
        FROM wine_base wb
        LEFT JOIN lwin_ids l ON l.wine_id = wb.id::text
        LEFT JOIN priced pr ON pr.wine_id = wb.id::text
        LEFT JOIN scored sc ON sc.wine_id = wb.id::text
        ORDER BY wb.producer_id, length(wb.display_name) DESC, wb.display_name
        """,
        (producer_ids,),
    )
    context: dict[str, dict] = {}
    for row in cur.fetchall():
        producer_id = row[0]
        info = context.setdefault(
            producer_id,
            {
                "wine_count": 0,
                "wines_with_lwin": 0,
                "wines_with_prices": 0,
                "wines_with_scores": 0,
                "wines": [],
            },
        )
        info["wine_count"] += 1
        info["wines_with_lwin"] += int(bool(row[7]))
        info["wines_with_prices"] += int(bool(row[8]))
        info["wines_with_scores"] += int(bool(row[9]))
        info["wines"].append(
            {
                "wine_id": row[1],
                "display_name": row[2],
                "name_normalized": row[3] or "",
                "appellation_name": row[4],
                "region_name": row[5],
                "country_code": row[6],
                "has_lwin": bool(row[7]),
                "has_price": bool(row[8]),
                "has_score": bool(row[9]),
            }
        )
    for producer_id in producer_ids:
        context.setdefault(
            producer_id,
            {
                "wine_count": 0,
                "wines_with_lwin": 0,
                "wines_with_prices": 0,
                "wines_with_scores": 0,
                "wines": [],
            },
        )
    return context


def dominant_places(wines: list[dict], limit: int = 3) -> list[str]:
    counts: Counter[str] = Counter()
    for wine in wines:
        place = wine["appellation_name"] or wine["region_name"]
        if place:
            counts[place] += 1
    return [place for place, _ in counts.most_common(limit)]


def representative_wines(wines: list[dict], limit: int = 5) -> list[str]:
    return [wine["display_name"] for wine in wines[:limit]]


def build_catalog_summary(producer: dict, catalog: dict) -> dict:
    sparsity_flags: list[str] = []
    if catalog["wine_count"] <= 2:
        sparsity_flags.append("thin_catalog")
    if catalog["wines_with_lwin"] == 0:
        sparsity_flags.append("no_external_ids")
    if catalog["wines_with_prices"] == 0 and catalog["wines_with_scores"] == 0:
        sparsity_flags.append("no_prices_or_scores")
    if catalog["wine_count"] == 0:
        sparsity_flags.append("empty_row")
    return {
        "wine_count": catalog["wine_count"],
        "wines_with_lwin": catalog["wines_with_lwin"],
        "wines_with_prices": catalog["wines_with_prices"],
        "wines_with_scores": catalog["wines_with_scores"],
        "representative_wines": representative_wines(catalog["wines"]),
        "dominant_places": dominant_places(catalog["wines"]),
        "sparsity_flags": sparsity_flags,
    }


def detect_containment(name_a: str, name_b: str) -> str:
    norm_a = normalize_text(name_a)
    norm_b = normalize_text(name_b)
    if norm_a and norm_a in norm_b and norm_a != norm_b:
        return "a_in_b"
    if norm_b and norm_b in norm_a and norm_a != norm_b:
        return "b_in_a"
    return "none"


def shared_core_tokens(name_a: str, name_b: str) -> list[str]:
    toks_a = set(tokenize(name_a))
    toks_b = set(tokenize(name_b))
    shared = toks_a & toks_b
    return sorted(tok for tok in shared if tok not in GENERIC_WRAPPER_TOKENS)


def wrapper_token_diff(name_a: str, name_b: str) -> tuple[list[str], list[str]]:
    toks_a = set(tokenize(name_a))
    toks_b = set(tokenize(name_b))
    only_a = sorted(tok for tok in toks_a - toks_b if tok in GENERIC_WRAPPER_TOKENS)
    only_b = sorted(tok for tok in toks_b - toks_a if tok in GENERIC_WRAPPER_TOKENS)
    return only_a, only_b


def find_exact_overlap(side_a: dict, side_b: dict) -> tuple[int, list[str], list[str]]:
    names_a = {wine["name_normalized"] for wine in side_a["wines"] if wine["name_normalized"]}
    names_b = {wine["name_normalized"] for wine in side_b["wines"] if wine["name_normalized"]}
    exact_names = sorted(names_a & names_b)
    place_a = {wine["appellation_name"] or wine["region_name"] for wine in side_a["wines"] if wine["appellation_name"] or wine["region_name"]}
    place_b = {wine["appellation_name"] or wine["region_name"] for wine in side_b["wines"] if wine["appellation_name"] or wine["region_name"]}
    shared_places = sorted(place_a & place_b)
    examples: list[str] = []
    for place in shared_places[:2]:
        examples.append(f"Both sides bottle {place}")
    for name in exact_names[:1]:
        examples.append(f"Both sides carry a shared cuvee key '{name}'")
    return len(exact_names), examples[:3], exact_names[:3]


def portfolio_shape_comment(side_a: dict, side_b: dict, anchor_examples: list[str]) -> str:
    count_a = side_a["wine_count"]
    count_b = side_b["wine_count"]
    smaller = min(count_a, count_b)
    larger = max(count_a, count_b)
    if smaller == 0:
        return "One side is an empty catalog shell, so packet evidence is thin."
    if anchor_examples and smaller * 4 <= larger:
        return "The smaller side looks like a thin subset of the larger side's place/style footprint."
    if anchor_examples:
        return "Both sides show overlapping place and portfolio anchors."
    return "The two local catalogs do not expose obvious shared anchors from DB evidence alone."


def has_shared_surname_split_risk(producer_a: dict, producer_b: dict) -> bool:
    toks_a = tokenize(producer_a["name"])
    toks_b = tokenize(producer_b["name"])
    if not toks_a or not toks_b:
        return False
    surname_a = toks_a[-1]
    surname_b = toks_b[-1]
    if surname_a != surname_b:
        return False
    region_diff = producer_a.get("region_name") != producer_b.get("region_name")
    country_diff = producer_a.get("country_code") != producer_b.get("country_code")
    return region_diff or country_diff or len(toks_a) != len(toks_b)


def derive_candidate_family(pair_row: dict, producer_a: dict, producer_b: dict, anchor_examples: list[str]) -> str:
    signals = pair_row["signals"] or {}
    if producer_a["country_code"] != producer_b["country_code"]:
        return "cross_country_same_brand"
    if "s10_shared_rare_wine" in signals:
        return "rare_wine_anchor"
    if "s8_catalog_overlap" in signals or anchor_examples:
        return "catalog_coherence"
    if "s2_trigram" in signals or "s9_substring" in signals:
        return "same_country_lexical_alias"
    return "mixed"


def derive_rule_paths(pair_row: dict, producer_a: dict, producer_b: dict) -> list[str]:
    paths: list[str] = []
    same_country = producer_a["country_code"] == producer_b["country_code"]
    containment = detect_containment(producer_a["name"], producer_b["name"])
    signals = pair_row["signals"] or {}

    if "s6_ttb_permit" in signals:
        paths.append("11.4.j")
    if not same_country:
        paths.extend(["11.4.n", "11.4.m"])
    if containment != "none":
        paths.extend(["11.4.h", "11.4.f"])
    if has_shared_surname_split_risk(producer_a, producer_b):
        paths.append("11.4.m")
    if not paths and pair_row["similarity"] >= 0.6:
        paths.append("11.4.h")
    if not paths:
        paths.append("11.1")

    seen: set[str] = set()
    ordered: list[str] = []
    for path in paths:
        if path not in seen:
            ordered.append(path)
            seen.add(path)
    return ordered[:3]


def build_support_and_contradictions(
    pair_row: dict,
    producer_a: dict,
    producer_b: dict,
    side_a_catalog: dict,
    side_b_catalog: dict,
    anchor_examples: list[str],
) -> tuple[list[dict], list[dict]]:
    support: list[dict] = []
    contradictions: list[dict] = []
    same_country = producer_a["country_code"] == producer_b["country_code"]
    containment = detect_containment(producer_a["name"], producer_b["name"])
    shared_tokens = shared_core_tokens(producer_a["name"], producer_b["name"])
    only_a, only_b = wrapper_token_diff(producer_a["name"], producer_b["name"])

    if containment != "none" and (only_a or only_b or pair_row["similarity"] >= 0.5):
        support.append(
            {
                "code": "lexical_short_full_form",
                "strength": "high",
                "summary": "Name containment plus wrapper-token differences point to a short-form versus fuller on-label producer form.",
            }
        )
    elif pair_row["similarity"] >= 0.92:
        support.append(
            {
                "code": "lexical_near_exact",
                "strength": "high",
                "summary": "The producer names are near-exact after normalization.",
            }
        )

    if same_country and anchor_examples:
        support.append(
            {
                "code": "catalog_subset_match",
                "strength": "medium",
                "summary": "The local catalog shows overlapping place or cuvee anchors that fit one producer footprint.",
            }
        )

    if not same_country and normalize_text(producer_a["name"]) == normalize_text(producer_b["name"]):
        support.append(
            {
                "code": "cross_country_name_match",
                "strength": "medium",
                "summary": "The same normalized producer name appears across countries, which can signal a global brand split.",
            }
        )

    if "s6_ttb_permit" in (pair_row["signals"] or {}):
        support.append(
            {
                "code": "shared_ttb_permit",
                "strength": "medium",
                "summary": "The pair shares a TTB BW permit signal, which is strong identity or facility evidence but not merge proof by itself.",
            }
        )

    if not same_country and normalize_text(producer_a["name"]) != normalize_text(producer_b["name"]):
        contradictions.append(
            {
                "code": "country_conflict",
                "severity": "high",
                "summary": "The two rows live in different countries without a deterministic same-brand link from local evidence alone.",
            }
        )

    if has_shared_surname_split_risk(producer_a, producer_b):
        contradictions.append(
            {
                "code": "shared_surname_split_risk",
                "severity": "high",
                "summary": "The rows share a surname-style token but diverge in geography or family form, which is a known split pattern.",
            }
        )

    if abs(side_a_catalog["wine_count"] - side_b_catalog["wine_count"]) >= 10:
        contradictions.append(
            {
                "code": "catalog_asymmetry",
                "severity": "low",
                "summary": "One side carries a much larger local catalog, so overlap can be suggestive without being exhaustive.",
            }
        )

    if not producer_a["website_url"] and not producer_b["website_url"]:
        contradictions.append(
            {
                "code": "sparse_web",
                "severity": "low",
                "summary": "Neither side has an official website URL on the producer row, so retrieval evidence is thin.",
            }
        )

    if not support and same_country and shared_tokens:
        support.append(
            {
                "code": "shared_core_token",
                "strength": "low",
                "summary": "The rows share a distinctive producer token but need stronger evidence to resolve confidently.",
            }
        )

    return support[:4], contradictions[:4]


def build_official_hits(producer_key: str, producer: dict) -> list[dict]:
    website_url = producer.get("website_url")
    if not website_url:
        return []
    parsed = urlparse(website_url)
    domain = parsed.netloc or website_url
    return [
        {
            "ref_id": f"official_{producer_key}",
            "subject": producer_key,
            "domain": domain,
            "url": website_url,
            "page_title": f"{producer['name']} official website",
            "claim_summary": "Official website URL is present on the current producer row.",
            "supports": "NEUTRAL",
            "retrieved_at": datetime.now().date().isoformat(),
        }
    ]


def build_retrieval_section(producer_a: dict, producer_b: dict) -> tuple[dict, str]:
    official_hits = build_official_hits("side_a", producer_a) + build_official_hits("side_b", producer_b)
    retrieval_gaps: list[str] = []
    if not official_hits:
        retrieval_gaps.append("No official website URL is present on either producer row.")
        completeness = "missing"
    else:
        retrieval_gaps.append("Official-domain URL presence is captured, but page-level claim extraction is not populated yet.")
        completeness = "partial"
    return (
        {
            "official_domain_hits": official_hits[:4],
            "secondary_hits": [],
            "retrieval_gaps": retrieval_gaps,
        },
        completeness,
    )


def compare_survivor(preferred_a: dict, preferred_b: dict, catalog_a: dict, catalog_b: dict) -> list[dict]:
    items: list[tuple[int, str, list[str]]] = []

    def score(producer: dict, catalog: dict, other: dict) -> tuple[int, list[str]]:
        points = 0
        reasons: list[str] = []
        containment = detect_containment(preferred_a["name"], preferred_b["name"])
        if containment == "a_in_b" and producer["producer_id"] == preferred_b["producer_id"]:
            points += 30
            reasons.append("full on-label form beats shorthand")
        if containment == "b_in_a" and producer["producer_id"] == preferred_a["producer_id"]:
            points += 30
            reasons.append("full on-label form beats shorthand")

        metadata_fields = [
            producer.get("website_url"),
            producer.get("year_established"),
            producer.get("producer_type"),
            producer.get("region_name"),
            producer.get("country_name"),
        ]
        metadata_score = sum(1 for field in metadata_fields if field)
        points += metadata_score * 3
        if metadata_score:
            reasons.append("more complete producer metadata")

        if catalog["wine_count"] > 0 and catalog["wine_count"] >= other["wine_count"]:
            points += 2
            reasons.append("larger attached wine catalog")
        if catalog["wines_with_lwin"] > 0 and catalog["wines_with_lwin"] >= other["wines_with_lwin"]:
            points += 1
            reasons.append("stronger LWIN linkage")
        if producer.get("created_at") and other.get("created_at") and producer["created_at"] <= other["created_at"]:
            points += 1
            reasons.append("older canonical row as final tie-break")
        return points, reasons[:3]

    score_a, reasons_a = score(preferred_a, catalog_a, catalog_b)
    score_b, reasons_b = score(preferred_b, catalog_b, catalog_a)
    items.append((score_a, preferred_a["producer_id"], reasons_a))
    items.append((score_b, preferred_b["producer_id"], reasons_b))
    items.sort(key=lambda item: item[0], reverse=True)
    return [
        {
            "rank": idx + 1,
            "producer_id": producer_id,
            "name": preferred_a["name"] if producer_id == preferred_a["producer_id"] else preferred_b["name"],
            "why": reasons or ["tie-break required"],
        }
        for idx, (_, producer_id, reasons) in enumerate(items)
    ]


def build_survivor_section(producer_a: dict, producer_b: dict, catalog_a: dict, catalog_b: dict) -> tuple[dict, str]:
    ordered = compare_survivor(producer_a, producer_b, catalog_a, catalog_b)
    winner = ordered[0] if ordered else None
    confidence = "low"
    if winner and len(ordered) == 2:
        winner_reasons = len(winner["why"])
        confidence = "high" if winner_reasons >= 2 else "medium"
    alias_to_preserve = None
    if winner:
        loser_name = producer_b["name"] if winner["producer_id"] == producer_a["producer_id"] else producer_a["name"]
        alias_to_preserve = loser_name
    return (
        {
            "candidate_order": ordered,
            "recommended_survivor_producer_id": winner["producer_id"] if winner else None,
            "alias_to_preserve": alias_to_preserve,
            "survivor_confidence": confidence,
            "only_apply_if_verdict": "MERGE",
        },
        "complete" if winner else "partial",
    )


def validate_case(case: dict, pair_row: dict, producer_a: dict, producer_b: dict) -> list[str]:
    issues: list[str] = []
    if pair_row["producer_id_a"] != producer_a["producer_id"] or pair_row["producer_id_b"] != producer_b["producer_id"]:
        issues.append("pair row producer ids do not match resolved producer rows")
    if not name_alignment_ok(case["producer_name_a"], producer_a["name"]):
        issues.append(
            f"benchmark producer_name_a '{case['producer_name_a']}' no longer aligns with current '{producer_a['name']}'"
        )
    if not name_alignment_ok(case["producer_name_b"], producer_b["name"]):
        issues.append(
            f"benchmark producer_name_b '{case['producer_name_b']}' no longer aligns with current '{producer_b['name']}'"
        )
    if case["country_a"] != (producer_a["country_code"] or ""):
        issues.append(
            f"benchmark country_a '{case['country_a']}' != current '{producer_a['country_code']}'"
        )
    if case["country_b"] != (producer_b["country_code"] or ""):
        issues.append(
            f"benchmark country_b '{case['country_b']}' != current '{producer_b['country_code']}'"
        )
    return issues


def build_packet(case: dict, pair_row: dict, producer_a: dict, producer_b: dict, catalog_context: dict) -> dict:
    side_a_catalog = catalog_context[producer_a["producer_id"]]
    side_b_catalog = catalog_context[producer_b["producer_id"]]
    exact_overlap_count, anchor_examples, rare_anchor_wines = find_exact_overlap(side_a_catalog, side_b_catalog)
    support_signals, contradiction_flags = build_support_and_contradictions(
        pair_row,
        producer_a,
        producer_b,
        side_a_catalog,
        side_b_catalog,
        anchor_examples,
    )
    external_evidence, retrieval_completeness = build_retrieval_section(producer_a, producer_b)
    survivor_if_merge, survivor_completeness = build_survivor_section(
        producer_a,
        producer_b,
        side_a_catalog,
        side_b_catalog,
    )
    only_a_wrappers, only_b_wrappers = wrapper_token_diff(producer_a["name"], producer_b["name"])
    pair_tier = case.get("source_pair_tier") or "unknown"

    packet = {
        "packet_version": "v1",
        "packet_id": f"producer_pair_{case['pair_id']}_v1",
        "envelope": {
            "pair_id": case["pair_id"],
            "producer_id_a": producer_a["producer_id"],
            "producer_id_b": producer_b["producer_id"],
            "pair_tier": pair_tier,
            "candidate_family": derive_candidate_family(pair_row, producer_a, producer_b, anchor_examples),
            "source_methods": [f"blocking:{key}" for key in sorted((pair_row["signals"] or {}).keys())],
            "generated_at": datetime.now().astimezone().isoformat(timespec="seconds"),
            "data_cutoff_at": datetime.now().astimezone().isoformat(timespec="seconds"),
            "completeness": {
                "local_catalog": "complete",
                "retrieval": retrieval_completeness,
                "survivor_calc": survivor_completeness,
            },
            "benchmark_overlay": {
                "benchmark_case_id": case["case_id"],
                "expected_verdict": case["expected_verdict"],
                "source_of_truth": case["source_of_truth"],
                "producer_name_a": case["producer_name_a"],
                "producer_name_b": case["producer_name_b"],
                "country_a": case["country_a"],
                "country_b": case["country_b"],
                "stratum": case["stratum"],
                "source_pair_tier": case["source_pair_tier"],
                "pattern_cluster": case["pattern_cluster"],
                "historical_failure_mode": case["historical_failure_mode"],
                "source_artifact": case["source_artifact"],
                "rationale": case["rationale"],
            },
        },
        "evidence": {
            "pair": {
                "display_name": f"{producer_a['name']} <-> {producer_b['name']}",
                "names": {
                    "a": producer_a["name"],
                    "b": producer_b["name"],
                },
                "normalized_names": {
                    "a": producer_a["name_normalized"] or normalize_text(producer_a["name"]),
                    "b": producer_b["name_normalized"] or normalize_text(producer_b["name"]),
                },
                "country_pair": [producer_a["country_code"], producer_b["country_code"]],
                "rule_paths_to_check": derive_rule_paths(pair_row, producer_a, producer_b),
                "why_this_pair_exists": [
                    f"candidate_family={derive_candidate_family(pair_row, producer_a, producer_b, anchor_examples)}",
                    f"blocking_signals={', '.join(sorted((pair_row['signals'] or {}).keys())) or 'none'}",
                    f"trigram similarity {pair_row['similarity']:.3f}",
                ],
            },
            "side_a": {
                "producer_id": producer_a["producer_id"],
                "name": producer_a["name"],
                "name_normalized": producer_a["name_normalized"] or normalize_text(producer_a["name"]),
                "producer_snapshot": {
                    "country": producer_a["country_name"],
                    "region": producer_a["region_name"],
                    "appellation": producer_a["appellation_name"],
                    "website_url": producer_a["website_url"],
                    "year_established": producer_a["year_established"],
                    "producer_type": producer_a["producer_type"],
                    "parent_producer_id": producer_a["parent_producer_id"],
                    "deleted_at": producer_a["deleted_at"],
                },
                "catalog_summary": build_catalog_summary(producer_a, side_a_catalog),
            },
            "side_b": {
                "producer_id": producer_b["producer_id"],
                "name": producer_b["name"],
                "name_normalized": producer_b["name_normalized"] or normalize_text(producer_b["name"]),
                "producer_snapshot": {
                    "country": producer_b["country_name"],
                    "region": producer_b["region_name"],
                    "appellation": producer_b["appellation_name"],
                    "website_url": producer_b["website_url"],
                    "year_established": producer_b["year_established"],
                    "producer_type": producer_b["producer_type"],
                    "parent_producer_id": producer_b["parent_producer_id"],
                    "deleted_at": producer_b["deleted_at"],
                },
                "catalog_summary": build_catalog_summary(producer_b, side_b_catalog),
            },
            "comparison": {
                "lexical": {
                    "trigram_similarity": round(pair_row["similarity"], 4),
                    "containment": detect_containment(producer_a["name"], producer_b["name"]),
                    "shared_core_tokens": shared_core_tokens(producer_a["name"], producer_b["name"]),
                    "wrapper_tokens_only_on_a": only_a_wrappers,
                    "wrapper_tokens_only_on_b": only_b_wrappers,
                },
                "geography": {
                    "same_country": producer_a["country_code"] == producer_b["country_code"],
                    "same_region": producer_a["region_name"] == producer_b["region_name"],
                    "same_appellation": producer_a["appellation_name"] == producer_b["appellation_name"],
                    "conflict_notes": [
                        note
                        for note in [
                            None
                            if producer_a["country_code"] == producer_b["country_code"]
                            else "country differs",
                            None
                            if producer_a["region_name"] == producer_b["region_name"]
                            else "region differs",
                            None
                            if producer_a["appellation_name"] == producer_b["appellation_name"]
                            else "appellation differs",
                        ]
                        if note
                    ],
                },
                "catalog": {
                    "exact_overlap_count": exact_overlap_count,
                    "anchor_overlap_examples": anchor_examples,
                    "rare_anchor_wines": rare_anchor_wines,
                    "portfolio_shape_comment": portfolio_shape_comment(side_a_catalog, side_b_catalog, anchor_examples),
                },
                "support_signals": support_signals,
                "contradiction_flags": contradiction_flags,
            },
            "external_evidence": external_evidence,
            "survivor_if_merge": survivor_if_merge,
        },
    }
    return packet


def validate_packet_shape(packet: dict) -> list[str]:
    issues: list[str] = []
    required_top = {"packet_version", "packet_id", "envelope", "evidence"}
    missing_top = sorted(required_top - set(packet))
    if missing_top:
        issues.append(f"missing top-level keys: {missing_top}")
    envelope = packet.get("envelope") or {}
    evidence = packet.get("evidence") or {}
    for key in ("pair_id", "producer_id_a", "producer_id_b", "pair_tier", "candidate_family", "source_methods", "generated_at", "data_cutoff_at", "completeness"):
        if key not in envelope:
            issues.append(f"missing envelope.{key}")
    for key in ("pair", "side_a", "side_b", "comparison", "external_evidence", "survivor_if_merge"):
        if key not in evidence:
            issues.append(f"missing evidence.{key}")
    if "benchmark_overlay" not in envelope:
        issues.append("missing envelope.benchmark_overlay")
    return issues


def strip_hidden_overlay(packet: dict) -> dict:
    visible = deepcopy(packet)
    visible.get("envelope", {}).pop("benchmark_overlay", None)
    return visible


def walk_keys(value: object, keys: set[str] | None = None) -> set[str]:
    if keys is None:
        keys = set()
    if isinstance(value, dict):
        for key, item in value.items():
            keys.add(key)
            walk_keys(item, keys)
    elif isinstance(value, list):
        for item in value:
            walk_keys(item, keys)
    return keys


def validate_visible_packet(visible_packet: dict) -> list[str]:
    packet_keys = walk_keys(visible_packet)
    leaked = sorted(HIDDEN_PACKET_KEYS & packet_keys)
    return [f"hidden key leaked into visible packet: {key}" for key in leaked]


def build_packets(benchmark_path: Path, limit: int | None = None) -> tuple[str, list[dict], list[dict], list[dict]]:
    benchmark_id, cases = load_benchmark_cases(benchmark_path)
    if limit is not None:
        cases = cases[:limit]

    full_packets: list[dict] = []
    visible_packets: list[dict] = []
    validations: list[dict] = []

    conn = get_conn()
    try:
        with conn.cursor() as cur:
            for case in cases:
                pair_row = fetch_pair_row(cur, case["pair_id"])
                if not pair_row:
                    raise RuntimeError(f"packet_build_error: pair_id {case['pair_id']} does not resolve in producer_dedup_pairs")

                producers = fetch_producers(cur, [pair_row["producer_id_a"], pair_row["producer_id_b"]])
                producer_a = producers.get(pair_row["producer_id_a"])
                producer_b = producers.get(pair_row["producer_id_b"])
                if not producer_a or not producer_b:
                    raise RuntimeError(
                        f"packet_build_error: pair_id {case['pair_id']} could not resolve both producer rows"
                    )

                case_issues = validate_case(case, pair_row, producer_a, producer_b)
                if case_issues:
                    raise RuntimeError(
                        f"packet_build_error: case_id {case['case_id']} failed validation: {'; '.join(case_issues)}"
                    )

                catalog_context = fetch_catalog_context(cur, [producer_a["producer_id"], producer_b["producer_id"]])
                packet = build_packet(case, pair_row, producer_a, producer_b, catalog_context)
                shape_issues = validate_packet_shape(packet)
                if shape_issues:
                    raise RuntimeError(
                        f"packet_build_error: case_id {case['case_id']} built invalid packet: {'; '.join(shape_issues)}"
                    )

                visible_packet = strip_hidden_overlay(packet)
                visible_issues = validate_visible_packet(visible_packet)
                if visible_issues:
                    raise RuntimeError(
                        f"packet_build_error: case_id {case['case_id']} leaked hidden fields: {'; '.join(visible_issues)}"
                    )

                full_packets.append(packet)
                visible_packets.append(visible_packet)
                validations.append(
                    {
                        "benchmark_id": benchmark_id,
                        "case_id": case["case_id"],
                        "packet_id": packet["packet_id"],
                        "pair_id": case["pair_id"],
                        "packet_valid": True,
                        "visible_packet_valid": True,
                        "hidden_field_leaks": 0,
                    }
                )
    finally:
        conn.close()

    return benchmark_id, full_packets, visible_packets, validations


def main() -> int:
    parser = argparse.ArgumentParser(description="Build evidence_packet_v1 rows from benchmark_v1")
    parser.add_argument("--benchmark", type=Path, default=DEFAULT_BENCHMARK)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--limit", type=int, default=None)
    args = parser.parse_args()

    benchmark_id, full_packets, visible_packets, validations = build_packets(
        benchmark_path=args.benchmark,
        limit=args.limit,
    )

    full_path = args.output_dir / "benchmark_v1_packets_full.jsonl"
    visible_path = args.output_dir / "benchmark_v1_packets_visible.jsonl"
    validation_path = args.output_dir / "benchmark_v1_packet_validation.json"

    write_jsonl(full_path, full_packets)
    write_jsonl(visible_path, visible_packets)
    validation_path.write_text(
        canonical_json_dumps(
            {
                "benchmark_id": benchmark_id,
                "packet_count": len(full_packets),
                "visible_packet_count": len(visible_packets),
                "hidden_field_leaks": sum(item["hidden_field_leaks"] for item in validations),
                "cases": validations,
            }
        ),
        encoding="utf-8",
    )

    print(f"Built {len(full_packets)} stored packets -> {full_path}")
    print(f"Built {len(visible_packets)} visible packets -> {visible_path}")
    print(f"Validation report -> {validation_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
