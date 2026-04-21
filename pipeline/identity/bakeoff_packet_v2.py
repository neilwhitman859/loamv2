"""
Session 8 - Build evidence_packet_v2 rows from benchmark_v1 cases.

Key v2 changes versus v1:
1. Flat citeable `evidence_refs[]` ledger.
2. Real official-domain retrieval via Serper at packet-build time.
3. Explicit unresolved official-domain refs when resolution fails.
4. First-class risk refs for shared-surname split, holdco/product-tier, and
   country-conflict cases.

Writes two JSONL artifacts:
1. Stored packet rows with envelope.benchmark_overlay preserved for scoring joins.
2. Model-visible packet rows with all hidden benchmark fields stripped.

Run:
    python -m pipeline.identity.bakeoff_packet_v2
    python -m pipeline.identity.bakeoff_packet_v2 --limit 8
"""

from __future__ import annotations

import argparse
import json
import re
from copy import deepcopy
from datetime import datetime
from pathlib import Path
from urllib.parse import urlparse

from pipeline.identity.bakeoff_packet_v1 import (
    COMMON_PLACE_TOKENS,
    DEFAULT_BENCHMARK,
    GENERIC_WRAPPER_TOKENS,
    HIDDEN_PACKET_KEYS,
    build_catalog_summary,
    build_survivor_section,
    canonical_json_dumps,
    compare_survivor,
    derive_candidate_family,
    derive_rule_paths,
    detect_containment,
    fetch_catalog_context,
    fetch_pair_row,
    fetch_producers,
    find_exact_overlap,
    load_benchmark_cases,
    name_alignment_ok,
    normalize_text,
    portfolio_shape_comment,
    shared_core_tokens,
    tokenize,
    validate_case,
    walk_keys,
    wrapper_token_diff,
    write_jsonl,
)
from pipeline.lib.db import get_conn
from pipeline.lib.serper import search


REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_OUTPUT_DIR = REPO_ROOT / "data" / "sprints" / "dedup" / "bakeoff_v2" / "packets"
ARTICLE_TOKENS = {
    "a",
    "al",
    "aux",
    "da",
    "das",
    "de",
    "dei",
    "del",
    "della",
    "der",
    "des",
    "di",
    "du",
    "el",
    "il",
    "la",
    "las",
    "le",
    "les",
    "los",
    "san",
    "santa",
    "the",
    "van",
    "von",
    "y",
}
OFFICIAL_BLOCKLIST = {
    "amazon.com",
    "apple.com",
    "facebook.com",
    "instagram.com",
    "linkedin.com",
    "vivino.com",
    "wikipedia.org",
    "wine-searcher.com",
    "wine.com",
    "x.com",
    "youtube.com",
}
MULTIPART_TLDS = {
    "co.nz",
    "co.uk",
    "com.au",
    "com.br",
    "com.mx",
    "com.tr",
}
HOLDCO_RISK_TOKENS = COMMON_PLACE_TOKENS | {
    "baron",
    "barons",
    "brand",
    "collection",
    "cuvee",
    "family",
    "label",
    "line",
    "pauillac",
    "private",
    "reserve",
    "special",
    "speciale",
    "selection",
    "series",
}
OWNERSHIP_RISK_PHRASES = {
    "acquired",
    "acquisition",
    "controlled by",
    "estate of",
    "founded by",
    "owned by",
    "operated by",
    "operator of",
    "part of",
    "portfolio",
    "project of",
    "sister brand",
}


def now_iso() -> str:
    return datetime.now().astimezone().isoformat(timespec="seconds")


def domain_host(url: str | None) -> str | None:
    if not url:
        return None
    parsed = urlparse(url if "://" in url else f"https://{url}")
    host = (parsed.netloc or parsed.path or "").lower().strip()
    host = host.split("/")[0]
    if host.startswith("www."):
        host = host[4:]
    return host or None


def root_domain(host: str | None) -> str | None:
    if not host:
        return None
    parts = host.split(".")
    if len(parts) >= 3 and ".".join(parts[-2:]) in MULTIPART_TLDS:
        return ".".join(parts[-3:])
    if len(parts) >= 2:
        return ".".join(parts[-2:])
    return host


def country_gl(iso_code: str | None) -> str | None:
    if not iso_code:
        return None
    value = iso_code.lower()
    return value if len(value) == 2 else None


def meaningful_name_tokens(name: str | None) -> set[str]:
    tokens = set(tokenize(name))
    return {
        token
        for token in tokens
        if token not in GENERIC_WRAPPER_TOKENS
        and token not in ARTICLE_TOKENS
        and len(token) >= 3
    }


def ref_entry(
    ref_id: str,
    ref_type: str,
    stance: str,
    summary: str,
    *,
    subject: str | None = None,
    detail: dict | None = None,
) -> dict:
    entry = {
        "ref_id": ref_id,
        "ref_type": ref_type,
        "stance": stance,
        "summary": summary,
    }
    if subject:
        entry["subject"] = subject
    if detail:
        entry["detail"] = detail
    return entry


def choose_official_domain(producer: dict, primary_result) -> tuple[str | None, str | None]:
    website_domain = root_domain(domain_host(producer.get("website_url")))
    if website_domain and website_domain not in OFFICIAL_BLOCKLIST:
        return website_domain, "producer_row.website_url"

    kg = primary_result.knowledge_graph or {}
    kg_domain = root_domain(domain_host(kg.get("website")))
    kg_title = str(kg.get("title") or "")
    if (
        kg_domain
        and kg_domain not in OFFICIAL_BLOCKLIST
        and (
            domain_matches_producer_name(kg_domain, producer["name"])
            or name_alignment_ok(producer["name"], kg_title)
        )
    ):
        return kg_domain, "serper.knowledge_graph.website"
    return None, None


def result_claim(text: str | None, fallback: str) -> str:
    cleaned = re.sub(r"\s+", " ", (text or "")).strip()
    if not cleaned:
        return fallback
    return cleaned[:280]


def build_hit_ref(ref_id: str, subject: str, item: dict, domain: str, source: str) -> dict:
    claim = result_claim(item.get("snippet") or item.get("description"), "Official-domain search result.")
    return ref_entry(
        ref_id,
        "retrieval",
        "support",
        claim,
        subject=subject,
        detail={
            "domain": domain,
            "source": source,
            "title": item.get("title"),
            "url": item.get("link") or item.get("website"),
        },
    )


def build_secondary_ref(ref_id: str, subject: str, item: dict) -> dict:
    claim = result_claim(item.get("snippet"), "Secondary search result.")
    return ref_entry(
        ref_id,
        "retrieval",
        "neutral",
        claim,
        subject=subject,
        detail={
            "domain": root_domain(domain_host(item.get("link"))),
            "title": item.get("title"),
            "url": item.get("link"),
        },
    )


def domain_matches_producer_name(domain: str | None, producer_name: str) -> bool:
    host_text = normalize_text((domain or "").replace(".", " "))
    if not host_text:
        return False
    return any(token in host_text for token in meaningful_name_tokens(producer_name))


def hit_haystack(hit: dict) -> str:
    detail = hit.get("detail", {}) if isinstance(hit.get("detail"), dict) else {}
    return normalize_text(
        " ".join(
            [
                str(hit.get("summary") or ""),
                str(detail.get("title") or ""),
                str(detail.get("url") or ""),
            ]
        )
    )


def full_name_phrase_present(text: str, producer_name: str) -> bool:
    phrase = normalize_text(producer_name)
    return bool(phrase) and phrase in text


def official_hits_mention_name(retrieval: dict, producer_name: str) -> bool:
    return any(full_name_phrase_present(hit_haystack(hit), producer_name) for hit in retrieval["official_hits"])


def shared_domain_brand_identity_aligned(
    producer_a: dict,
    producer_b: dict,
    retrieval_a: dict,
    retrieval_b: dict,
) -> bool:
    if retrieval_a["resolved_domain"] != retrieval_b["resolved_domain"]:
        return False
    if not name_alignment_ok(producer_a["name"], producer_b["name"]):
        return False
    return official_hits_mention_name(retrieval_a, producer_a["name"]) and official_hits_mention_name(
        retrieval_b, producer_b["name"]
    )


def find_owner_operator_identity_risk_ref(
    producer_a: dict,
    producer_b: dict,
    retrieval_a: dict,
    retrieval_b: dict,
) -> dict | None:
    for retrieval, other_name in (
        (retrieval_a, producer_b["name"]),
        (retrieval_b, producer_a["name"]),
    ):
        for hit in retrieval["official_hits"] + retrieval["secondary_hits"]:
            haystack = hit_haystack(hit)
            if not full_name_phrase_present(haystack, other_name):
                continue
            if any(phrase in haystack for phrase in OWNERSHIP_RISK_PHRASES):
                return ref_entry(
                    "risk_owner_or_operator_not_identity",
                    "risk",
                    "risk",
                    "Ownership, operator, or acquisition language links the two names, but that does not prove same on-label identity.",
                    detail={"support_ref_id": hit["ref_id"]},
                )
    return None


def retrieve_official_domain(producer_key: str, producer: dict) -> dict:
    subject = f"side_{producer_key}"
    queries: list[str] = []
    search_count = 0
    gl = country_gl(producer.get("country_code"))
    primary_query = f'"{producer["name"]}" wine producer official'
    primary = search(primary_query, num=5, country=gl)
    queries.append(primary_query)
    search_count += 1

    official_domain, resolution_source = choose_official_domain(producer, primary)
    official_hits: list[dict] = []
    secondary_hits: list[dict] = []

    if official_domain:
        seen_urls: set[str] = set()
        kg = primary.knowledge_graph or {}
        kg_host = root_domain(domain_host(kg.get("website")))
        if kg and kg_host == official_domain:
            official_hits.append(
                build_hit_ref(
                    f"official_{producer_key}_1",
                    subject,
                    {
                        "title": kg.get("title"),
                        "description": kg.get("description"),
                        "website": kg.get("website"),
                    },
                    official_domain,
                    "knowledge_graph",
                )
            )
            seen_urls.add(kg.get("website") or "")

        site_query = f'site:{official_domain} "{producer["name"]}"'
        site_result = search(site_query, num=5, country=gl)
        queries.append(site_query)
        search_count += 1

        for item in site_result.organic:
            link = item.get("link") or ""
            if root_domain(domain_host(link)) != official_domain:
                continue
            if link in seen_urls:
                continue
            ref_id = f"official_{producer_key}_{len(official_hits) + 1}"
            official_hits.append(build_hit_ref(ref_id, subject, item, official_domain, "site_query"))
            seen_urls.add(link)
            if len(official_hits) >= 2:
                break

        for item in primary.organic:
            if len(secondary_hits) >= 2:
                break
            host = root_domain(domain_host(item.get("link")))
            if not host or host == official_domain:
                continue
            ref_id = f"secondary_{producer_key}_{len(secondary_hits) + 1}"
            secondary_hits.append(build_secondary_ref(ref_id, subject, item))
    else:
        for item in primary.organic[:2]:
            ref_id = f"secondary_{producer_key}_{len(secondary_hits) + 1}"
            secondary_hits.append(build_secondary_ref(ref_id, subject, item))

    unresolved_ref = None
    if not official_hits:
        reason = primary.error or "no official domain resolved from producer row or search results"
        unresolved_ref = ref_entry(
            f"official_unresolved_{producer_key}",
            "retrieval",
            "risk",
            f"Official-domain continuity could not be resolved for {producer['name']}: {reason}.",
            subject=subject,
            detail={
                "resolution_source": resolution_source,
                "resolved_domain": official_domain,
            },
        )

    return {
        "official_hits": official_hits,
        "secondary_hits": secondary_hits,
        "unresolved_ref": unresolved_ref,
        "resolved_domain": official_domain,
        "resolution_source": resolution_source,
        "queries": queries,
        "search_count": search_count,
        "primary_error": primary.error,
    }


def first_party_name_variation_tokens(name_a: str, name_b: str) -> set[str]:
    tokens_a = meaningful_name_tokens(name_a)
    tokens_b = meaningful_name_tokens(name_b)
    return tokens_a ^ tokens_b


def has_shared_surname_split_risk_v2(producer_a: dict, producer_b: dict) -> bool:
    toks_a = tokenize(producer_a["name"])
    toks_b = tokenize(producer_b["name"])
    if not toks_a or not toks_b:
        return False
    if toks_a[-1] != toks_b[-1]:
        return False
    extra = first_party_name_variation_tokens(producer_a["name"], producer_b["name"])
    country_diff = producer_a.get("country_code") != producer_b.get("country_code")
    region_diff = producer_a.get("region_name") != producer_b.get("region_name")
    return bool(extra) and (country_diff or region_diff or len(extra) >= 1)


def has_holdco_or_product_tier_risk(
    producer_a: dict,
    producer_b: dict,
    side_a_catalog: dict,
    side_b_catalog: dict,
) -> bool:
    containment = detect_containment(producer_a["name"], producer_b["name"])
    if containment == "none":
        return False
    toks_a = meaningful_name_tokens(producer_a["name"])
    toks_b = meaningful_name_tokens(producer_b["name"])
    extras = (toks_a ^ toks_b) - ARTICLE_TOKENS
    if not extras:
        return False
    place_tokens = meaningful_name_tokens(" ".join(side_a_catalog["dominant_places"] + side_b_catalog["dominant_places"]))
    risky = extras & (HOLDCO_RISK_TOKENS | place_tokens)
    if risky:
        return True
    exact_overlap_count = min(side_a_catalog["wine_count"], side_b_catalog["wine_count"])
    return bool(extras) and abs(side_a_catalog["wine_count"] - side_b_catalog["wine_count"]) >= 10 and exact_overlap_count == 0


def build_continuity_refs(
    producer_a: dict,
    producer_b: dict,
    retrieval_a: dict,
    retrieval_b: dict,
) -> list[dict]:
    refs: list[dict] = []
    domain_a = retrieval_a["resolved_domain"]
    domain_b = retrieval_b["resolved_domain"]
    if domain_a and domain_b and domain_a == domain_b:
        refs.append(
            ref_entry(
                "soft_continuity_hint_shared_domain",
                "retrieval",
                "support",
                f"Both sides resolve to the same producer-owned domain `{domain_a}`, but shared domain alone does not prove same brand identity.",
                detail={"domain": domain_a},
            )
        )
        if shared_domain_brand_identity_aligned(producer_a, producer_b, retrieval_a, retrieval_b):
            refs.append(
                ref_entry(
                    "hard_official_continuity_shared_domain",
                    "retrieval",
                    "support",
                    f"Both sides resolve to `{domain_a}` and the page-level brand identity aligns with the producer names on both sides.",
                    detail={"domain": domain_a},
                )
            )
    for retrieval, other_name, producer_key in (
        (retrieval_a, producer_b["name"], "a"),
        (retrieval_b, producer_a["name"], "b"),
    ):
        other_tokens = meaningful_name_tokens(other_name)
        for hit in retrieval["official_hits"]:
            haystack = hit_haystack(hit)
            if full_name_phrase_present(haystack, other_name):
                refs.append(
                    ref_entry(
                        f"hard_official_continuity_alias_{producer_key}",
                        "retrieval",
                        "support",
                        f"Hard-official retrieval for one side directly mentions the full other on-label form `{other_name}`.",
                        detail={"support_ref_id": hit["ref_id"]},
                    )
                )
                break
            if other_tokens and any(token in haystack for token in other_tokens):
                refs.append(
                    ref_entry(
                        f"soft_continuity_hint_alias_{producer_key}",
                        "retrieval",
                        "support",
                        f"Retrieval for one side cross-mentions tokens from `{other_name}`, but not as exact alias proof.",
                        detail={"support_ref_id": hit["ref_id"]},
                    )
                )
                break
    return refs


def build_evidence_refs(
    pair_row: dict,
    producer_a: dict,
    producer_b: dict,
    side_a_catalog: dict,
    side_b_catalog: dict,
    anchor_examples: list[str],
    exact_overlap_count: int,
    retrieval_a: dict,
    retrieval_b: dict,
) -> tuple[list[dict], dict]:
    refs: list[dict] = []
    containment = detect_containment(producer_a["name"], producer_b["name"])
    shared_tokens = shared_core_tokens(producer_a["name"], producer_b["name"])
    only_a_wrappers, only_b_wrappers = wrapper_token_diff(producer_a["name"], producer_b["name"])
    same_country = producer_a["country_code"] == producer_b["country_code"]
    same_region = producer_a["region_name"] == producer_b["region_name"]
    same_appellation = producer_a["appellation_name"] == producer_b["appellation_name"]
    trigram = round(pair_row["similarity"], 4)

    if containment != "none":
        refs.append(
            ref_entry(
                "lex_contains",
                "lexical",
                "support",
                f"Name containment is present ({containment}) between `{producer_a['name']}` and `{producer_b['name']}`.",
                detail={"containment": containment},
            )
        )
    if trigram >= 0.92:
        refs.append(
            ref_entry(
                "lex_near_exact",
                "lexical",
                "support",
                f"Normalized trigram similarity is {trigram:.4f}, which is near-exact.",
                detail={"trigram_similarity": trigram},
            )
        )
    if shared_tokens:
        refs.append(
            ref_entry(
                "lex_shared_core_tokens",
                "lexical",
                "support",
                f"The rows share distinctive producer tokens: {', '.join(shared_tokens[:6])}.",
                detail={"shared_core_tokens": shared_tokens[:6]},
            )
        )

    if same_country:
        refs.append(ref_entry("geo_same_country", "geography", "support", "Both rows resolve to the same country."))
    else:
        refs.append(
            ref_entry(
                "geo_country_conflict",
                "geography",
                "risk",
                f"Country conflict: `{producer_a['country_code']}` versus `{producer_b['country_code']}`.",
            )
        )
    if same_region and producer_a["region_name"]:
        refs.append(
            ref_entry(
                "geo_same_region",
                "geography",
                "support",
                f"Both rows resolve to the same region `{producer_a['region_name']}`.",
            )
        )
    if same_appellation and producer_a["appellation_name"]:
        refs.append(
            ref_entry(
                "geo_same_appellation",
                "geography",
                "support",
                f"Both rows resolve to the same appellation `{producer_a['appellation_name']}`.",
            )
        )

    if exact_overlap_count > 0:
        refs.append(
            ref_entry(
                "catalog_exact_overlap",
                "catalog",
                "support",
                f"The two local catalogs share {exact_overlap_count} normalized wine-name overlap(s).",
                detail={"exact_overlap_count": exact_overlap_count},
            )
        )
    if anchor_examples:
        refs.append(
            ref_entry(
                "catalog_subset_match",
                "catalog",
                "support",
                "The local catalog shows overlapping place or cuvee anchors that fit one producer footprint.",
                detail={"anchor_examples": anchor_examples[:3]},
            )
        )
    if side_a_catalog["wine_count"] != side_b_catalog["wine_count"]:
        refs.append(
            ref_entry(
                "catalog_asymmetry",
                "catalog",
                "neutral",
                f"Catalog sizes differ ({side_a_catalog['wine_count']} versus {side_b_catalog['wine_count']}).",
            )
        )
    refs.append(
        ref_entry(
            "catalog_portfolio_shape",
            "catalog",
            "neutral",
            portfolio_shape_comment(side_a_catalog, side_b_catalog, anchor_examples),
        )
    )

    shared_surname_risk = has_shared_surname_split_risk_v2(producer_a, producer_b)
    if shared_surname_risk:
        refs.append(
            ref_entry(
                "risk_shared_surname_split",
                "risk",
                "risk",
                "Shared-surname family split risk is present and requires official continuity evidence before merge.",
            )
        )
    holdco_risk = has_holdco_or_product_tier_risk(producer_a, producer_b, side_a_catalog, side_b_catalog)
    if holdco_risk:
        refs.append(
            ref_entry(
                "risk_holdco_or_product_tier",
                "risk",
                "risk",
                "One name looks like a parent-family, holdco, or product-tier extension of the other.",
            )
        )
    owner_operator_risk = find_owner_operator_identity_risk_ref(producer_a, producer_b, retrieval_a, retrieval_b)
    if owner_operator_risk:
        refs.append(owner_operator_risk)

    refs.extend(retrieval_a["official_hits"])
    refs.extend(retrieval_b["official_hits"])
    refs.extend(retrieval_a["secondary_hits"])
    refs.extend(retrieval_b["secondary_hits"])
    if retrieval_a["unresolved_ref"]:
        refs.append(retrieval_a["unresolved_ref"])
    if retrieval_b["unresolved_ref"]:
        refs.append(retrieval_b["unresolved_ref"])

    continuity_refs = build_continuity_refs(producer_a, producer_b, retrieval_a, retrieval_b)
    refs.extend(continuity_refs)

    if retrieval_a["unresolved_ref"] or retrieval_b["unresolved_ref"]:
        refs.append(
            ref_entry(
                "risk_sparse_official_evidence",
                "risk",
                "risk",
                "Official-domain retrieval is unresolved or sparse on at least one side.",
            )
        )

    refs_by_id = {entry["ref_id"]: entry for entry in refs}
    ordered = [refs_by_id[key] for key in sorted(refs_by_id)]
    flags = {
        "shared_surname_split": shared_surname_risk,
        "holdco_or_product_tier": holdco_risk,
        "owner_or_operator_not_identity": bool(owner_operator_risk),
        "country_conflict": not same_country,
        "has_hard_official_continuity": any(
            entry["ref_id"].startswith("hard_official_continuity_") for entry in ordered
        ),
        "has_soft_continuity_hint": any(
            entry["ref_id"].startswith("soft_continuity_hint_") for entry in ordered
        ),
    }
    return ordered, flags


def build_packet(case: dict, pair_row: dict, producer_a: dict, producer_b: dict, catalog_context: dict) -> dict:
    side_a_catalog = catalog_context[producer_a["producer_id"]]
    side_b_catalog = catalog_context[producer_b["producer_id"]]
    side_a_summary = build_catalog_summary(producer_a, side_a_catalog)
    side_b_summary = build_catalog_summary(producer_b, side_b_catalog)
    exact_overlap_count, anchor_examples, rare_anchor_wines = find_exact_overlap(side_a_catalog, side_b_catalog)

    retrieval_a = retrieve_official_domain("a", producer_a)
    retrieval_b = retrieve_official_domain("b", producer_b)
    evidence_refs, risk_flags = build_evidence_refs(
        pair_row,
        producer_a,
        producer_b,
        side_a_summary,
        side_b_summary,
        anchor_examples,
        exact_overlap_count,
        retrieval_a,
        retrieval_b,
    )
    survivor_if_merge, survivor_completeness = build_survivor_section(
        producer_a,
        producer_b,
        side_a_catalog,
        side_b_catalog,
    )
    only_a_wrappers, only_b_wrappers = wrapper_token_diff(producer_a["name"], producer_b["name"])
    generated_at = now_iso()
    retrieval_complete = "complete"
    if retrieval_a["primary_error"] or retrieval_b["primary_error"]:
        retrieval_complete = "partial"

    packet = {
        "packet_version": "v2",
        "packet_id": f"producer_pair_{case['pair_id']}_v2",
        "envelope": {
            "pair_id": case["pair_id"],
            "producer_id_a": producer_a["producer_id"],
            "producer_id_b": producer_b["producer_id"],
            "pair_tier": case.get("source_pair_tier") or "unknown",
            "candidate_family": derive_candidate_family(pair_row, producer_a, producer_b, anchor_examples),
            "source_methods": [f"blocking:{key}" for key in sorted((pair_row["signals"] or {}).keys())],
            "generated_at": generated_at,
            "data_cutoff_at": generated_at,
            "completeness": {
                "local_catalog": "complete",
                "retrieval": retrieval_complete,
                "survivor_calc": survivor_completeness,
            },
            "allowed_ref_ids": [entry["ref_id"] for entry in evidence_refs],
            "retrieval_search_calls": retrieval_a["search_count"] + retrieval_b["search_count"],
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
                "names": {"a": producer_a["name"], "b": producer_b["name"]},
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
                "catalog_summary": side_a_summary,
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
                "catalog_summary": side_b_summary,
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
                },
                "catalog": {
                    "exact_overlap_count": exact_overlap_count,
                    "anchor_overlap_examples": anchor_examples,
                    "rare_anchor_wines": rare_anchor_wines,
                    "portfolio_shape_comment": portfolio_shape_comment(side_a_catalog, side_b_catalog, anchor_examples),
                },
                "risk_flags": risk_flags,
            },
            "external_evidence": {
                "official_domain_resolution": {
                    "side_a": {
                        "resolved_domain": retrieval_a["resolved_domain"],
                        "resolution_source": retrieval_a["resolution_source"],
                        "queries": retrieval_a["queries"],
                    },
                    "side_b": {
                        "resolved_domain": retrieval_b["resolved_domain"],
                        "resolution_source": retrieval_b["resolution_source"],
                        "queries": retrieval_b["queries"],
                    },
                },
            },
            "survivor_if_merge": survivor_if_merge,
        },
        "evidence_refs": evidence_refs,
    }
    return packet


def validate_packet_shape(packet: dict) -> list[str]:
    issues: list[str] = []
    required_top = {"packet_version", "packet_id", "envelope", "evidence", "evidence_refs"}
    missing_top = sorted(required_top - set(packet))
    if missing_top:
        issues.append(f"missing top-level keys: {missing_top}")
    envelope = packet.get("envelope") or {}
    evidence = packet.get("evidence") or {}
    for key in (
        "pair_id",
        "producer_id_a",
        "producer_id_b",
        "pair_tier",
        "candidate_family",
        "source_methods",
        "generated_at",
        "data_cutoff_at",
        "completeness",
        "allowed_ref_ids",
    ):
        if key not in envelope:
            issues.append(f"missing envelope.{key}")
    for key in ("pair", "side_a", "side_b", "comparison", "external_evidence", "survivor_if_merge"):
        if key not in evidence:
            issues.append(f"missing evidence.{key}")
    if "benchmark_overlay" not in envelope:
        issues.append("missing envelope.benchmark_overlay")
    refs = packet.get("evidence_refs") or []
    ref_ids = [entry.get("ref_id") for entry in refs if isinstance(entry, dict)]
    if not refs:
        issues.append("missing evidence_refs entries")
    if sorted(ref_ids) != sorted(envelope.get("allowed_ref_ids") or []):
        issues.append("evidence_refs ref ids do not match envelope.allowed_ref_ids")
    return issues


def strip_hidden_overlay(packet: dict) -> dict:
    visible = deepcopy(packet)
    visible.get("envelope", {}).pop("benchmark_overlay", None)
    return visible


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
                        "retrieval_search_calls": packet["envelope"]["retrieval_search_calls"],
                    }
                )
    finally:
        conn.close()

    return benchmark_id, full_packets, visible_packets, validations


def main() -> int:
    parser = argparse.ArgumentParser(description="Build evidence_packet_v2 rows from benchmark_v1")
    parser.add_argument("--benchmark", type=Path, default=DEFAULT_BENCHMARK)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--limit", type=int, default=None)
    args = parser.parse_args()

    benchmark_id, full_packets, visible_packets, validations = build_packets(
        benchmark_path=args.benchmark,
        limit=args.limit,
    )

    full_path = args.output_dir / "benchmark_v1_packets_full_v2.jsonl"
    visible_path = args.output_dir / "benchmark_v1_packets_visible_v2.jsonl"
    validation_path = args.output_dir / "benchmark_v1_packet_validation_v2.json"

    write_jsonl(full_path, full_packets)
    write_jsonl(visible_path, visible_packets)
    validation_path.write_text(
        canonical_json_dumps(
            {
                "benchmark_id": benchmark_id,
                "packet_count": len(full_packets),
                "visible_packet_count": len(visible_packets),
                "hidden_field_leaks": sum(item["hidden_field_leaks"] for item in validations),
                "retrieval_search_calls": sum(item["retrieval_search_calls"] for item in validations),
                "cases": validations,
            }
        ),
        encoding="utf-8",
    )

    print(f"Built {len(full_packets)} stored v2 packets -> {full_path}")
    print(f"Built {len(visible_packets)} visible v2 packets -> {visible_path}")
    print(f"Validation report -> {validation_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
