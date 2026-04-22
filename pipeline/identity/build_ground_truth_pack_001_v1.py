"""Build Sprint 7 ground-truth Pack 001 from live read-only data.

Pack 001 intentionally mixes two kinds of records:
1. net-new scoreable pairs backed by primary-source evidence, and
2. explicit seed-repair / quarantine records for challenged benchmark truths.

Outputs:
- data/sprints/identity-er/ground_truth_pack_001_v1.jsonl
- data/sprints/identity-er/ground_truth_pack_001_summary_v1.md
"""

from __future__ import annotations

import json
import re
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from pipeline.lib.db import get_conn


REPO_ROOT = Path(__file__).resolve().parents[2]
OUTPUT_DIR = REPO_ROOT / "data" / "sprints" / "identity-er"
PACK_OUTPUT_PATH = OUTPUT_DIR / "ground_truth_pack_001_v1.jsonl"
SUMMARY_OUTPUT_PATH = OUTPUT_DIR / "ground_truth_pack_001_summary_v1.md"
SEED_PAIRS_PATH = OUTPUT_DIR / "ground_truth_seed_pairs_v1.jsonl"

PACK_VERSION = "ground_truth_pack_001_v1"
SCOREABLE_LABELS = {"SAME_AS", "RELATED_BUT_DISTINCT", "NONE"}
QUARANTINE_LABEL = "QUARANTINED_DISPUTE"
TARGET_SCOREABLE_TOTAL = 1000
TARGET_LABEL_COUNTS = {
    "SAME_AS": 300,
    "RELATED_BUT_DISTINCT": 200,
    "NONE": 500,
}

GENERIC_NAME_TOKENS = {
    "the",
    "and",
    "chateau",
    "cellar",
    "cellars",
    "co",
    "company",
    "domaine",
    "estate",
    "estates",
    "vineyard",
    "vineyards",
    "wine",
    "wines",
    "winery",
}


@dataclass(frozen=True)
class CaseSpec:
    name_a: str
    name_b: str
    label: str
    family_tag: str
    pattern_cluster: str
    record_mode: str
    evidence_kind: str
    rationale: str
    source_urls: tuple[str, ...] = field(default_factory=tuple)
    source_artifact: str | None = None
    source_note: str | None = None
    optional: bool = True


def uses_ttb_evidence(spec: CaseSpec) -> bool:
    return spec.evidence_kind in {"ttb_same_as", "ttb_none", "brand_literal_ttb"}


def read_jsonl(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            rows.append(json.loads(line))
    return rows


def write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    with path.open("w", encoding="utf-8", newline="\n") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=True, sort_keys=True))
            handle.write("\n")


def normalize_name(value: str) -> str:
    tokens = re.findall(r"[a-z0-9]+", value.lower())
    tokens = [token for token in tokens if token not in GENERIC_NAME_TOKENS]
    return " ".join(tokens)


def pair_key(producer_id_a: str, producer_id_b: str) -> str:
    left, right = sorted((producer_id_a, producer_id_b))
    return f"{left}::{right}"


def derive_tier(wine_count_a: int, wine_count_b: int) -> str:
    high = max(wine_count_a, wine_count_b)
    if high >= 25:
        return "core"
    if high >= 8:
        return "mid"
    return "tail"


def build_seed_index() -> set[str]:
    keys: set[str] = set()
    for row in read_jsonl(SEED_PAIRS_PATH):
        producer_id_a = row.get("producer_id_a")
        producer_id_b = row.get("producer_id_b")
        if producer_id_a and producer_id_b:
            keys.add(pair_key(str(producer_id_a), str(producer_id_b)))
    return keys


def fetch_live_counts() -> dict[str, int]:
    queries = {
        "producers": "select count(*) from producers",
        "active_wines": "select count(*) from wines where deleted_at is null",
        "wine_vintages": "select count(*) from wine_vintages",
        "producer_dedup_pairs": "select count(*) from producer_dedup_pairs",
        "blank_verdict_source": (
            "select count(*) from producer_dedup_pairs "
            "where coalesce(verdict_source, '') = ''"
        ),
    }
    conn = get_conn()
    cur = conn.cursor()
    counts: dict[str, int] = {}
    for key, sql in queries.items():
        cur.execute(sql)
        counts[key] = int(cur.fetchone()[0])
    cur.close()
    conn.close()
    return counts


def fetch_producer_snapshots(names: list[str]) -> dict[str, dict[str, Any]]:
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        with base as (
          select
            p.id,
            p.name,
            p.website_url,
            c.name as country_name,
            r.name as region_name,
            a.name as appellation_name
          from producers p
          left join countries c on c.id = p.country_id
          left join regions r on r.id = p.region_id
          left join appellations a on a.id = p.appellation_id
          where p.name = any(%s)
        ),
        wine_counts as (
          select producer_id, count(*) as wine_count
          from wines
          where deleted_at is null
            and producer_id in (select id from base)
          group by producer_id
        ),
        sample_wines as (
          select
            producer_id,
            array_agg(name order by name) as wines
          from (
            select distinct producer_id, name
            from wines
            where deleted_at is null
              and producer_id in (select id from base)
          ) dedup
          group by producer_id
        )
        select
          b.id,
          b.name,
          b.website_url,
          b.country_name,
          b.region_name,
          b.appellation_name,
          coalesce(wc.wine_count, 0) as wine_count,
          coalesce(sw.wines[1:5], array[]::text[]) as sample_wines
        from base b
        left join wine_counts wc on wc.producer_id = b.id
        left join sample_wines sw on sw.producer_id = b.id
        """,
        (names,),
    )
    rows = cur.fetchall()
    cur.close()
    conn.close()

    result: dict[str, dict[str, Any]] = {}
    for (
        producer_id,
        name,
        website_url,
        country_name,
        region_name,
        appellation_name,
        wine_count,
        sample_wines,
    ) in rows:
        result[str(name)] = {
            "id": str(producer_id),
            "name": name,
            "website_url": website_url,
            "country_name": country_name,
            "region_name": region_name,
            "appellation_name": appellation_name,
            "wine_count": int(wine_count or 0),
            "sample_wines": list(sample_wines or []),
        }
    return result


def fetch_pair_rows(producer_id_a: str, producer_id_b: str) -> list[dict[str, Any]]:
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        select
          id,
          name_a,
          name_b,
          country,
          similarity,
          wines_a,
          wines_b,
          method_name,
          verdict
        from producer_dedup_pairs
        where least(producer_id_a, producer_id_b) = least(%s::uuid, %s::uuid)
          and greatest(producer_id_a, producer_id_b) = greatest(%s::uuid, %s::uuid)
        order by id
        """,
        (producer_id_a, producer_id_b, producer_id_a, producer_id_b),
    )
    rows = cur.fetchall()
    cur.close()
    conn.close()

    result: list[dict[str, Any]] = []
    for (
        pair_id,
        name_a,
        name_b,
        country,
        similarity,
        wines_a,
        wines_b,
        method_name,
        verdict,
    ) in rows:
        result.append(
            {
                "pair_id": int(pair_id),
                "name_a": name_a,
                "name_b": name_b,
                "country": country,
                "similarity": float(similarity) if similarity is not None else None,
                "wines_a": int(wines_a or 0),
                "wines_b": int(wines_b or 0),
                "method_name": method_name,
                "verdict": verdict,
            }
        )
    return result


def fetch_exact_overlap_count(producer_id_a: str, producer_id_b: str) -> int:
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        select count(distinct wa.name)
        from wines wa
        join wines wb
          on wb.name = wa.name
         and wb.producer_id <> wa.producer_id
        where wa.deleted_at is null
          and wb.deleted_at is null
          and least(wa.producer_id, wb.producer_id) = least(%s::uuid, %s::uuid)
          and greatest(wa.producer_id, wb.producer_id) = greatest(%s::uuid, %s::uuid)
        """,
        (producer_id_a, producer_id_b, producer_id_a, producer_id_b),
    )
    count = int(cur.fetchone()[0] or 0)
    cur.close()
    conn.close()
    return count


def fetch_ttb_rows_for_producers(producer_ids: list[str]) -> list[dict[str, Any]]:
    if not producer_ids:
        return []
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        select
          canonical_producer_id,
          coalesce(nullif(permit_no,''), nullif(permit_number,''), nullif(serial_number,'')) as permit_key,
          brand_name,
          applicant_name,
          applicant_dba,
          count(*) as row_count
        from source_ttb_colas
        where canonical_producer_id = any(%s::uuid[])
          and coalesce(nullif(permit_no,''), nullif(permit_number,''), nullif(serial_number,'')) is not null
        group by 1,2,3,4,5
        """,
        (producer_ids,),
    )
    rows = cur.fetchall()
    cur.close()
    conn.close()
    result: list[dict[str, Any]] = []
    for producer_id, permit_key, brand_name, applicant_name, applicant_dba, row_count in rows:
        result.append(
            {
                "producer_id": str(producer_id),
                "permit_key": permit_key,
                "brand_name": brand_name,
                "applicant_name": applicant_name,
                "applicant_dba": applicant_dba,
                "row_count": int(row_count or 0),
            }
        )
    return result


def fetch_ttb_rows_for_permits(permit_keys: list[str]) -> list[dict[str, Any]]:
    if not permit_keys:
        return []
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        select
          coalesce(nullif(permit_no,''), nullif(permit_number,''), nullif(serial_number,'')) as permit_key,
          canonical_producer_id,
          count(*) as row_count
        from source_ttb_colas
        where coalesce(nullif(permit_no,''), nullif(permit_number,''), nullif(serial_number,'')) = any(%s)
          and canonical_producer_id is not null
        group by 1,2
        """,
        (permit_keys,),
    )
    rows = cur.fetchall()
    cur.close()
    conn.close()
    result: list[dict[str, Any]] = []
    for permit_key, producer_id, row_count in rows:
        result.append(
            {
                "permit_key": permit_key,
                "producer_id": str(producer_id),
                "row_count": int(row_count or 0),
            }
        )
    return result


def build_ttb_cache(specs: list[CaseSpec], producer_lookup: dict[str, dict[str, Any]]) -> dict[str, Any]:
    producer_ids = sorted(
        {
            producer_lookup[spec.name_a]["id"]
            for spec in specs
            if uses_ttb_evidence(spec) and spec.name_a in producer_lookup and spec.name_b in producer_lookup
        }
        | {
            producer_lookup[spec.name_b]["id"]
            for spec in specs
            if uses_ttb_evidence(spec) and spec.name_a in producer_lookup and spec.name_b in producer_lookup
        }
    )
    producer_rows = fetch_ttb_rows_for_producers(producer_ids)
    by_producer: dict[str, list[dict[str, Any]]] = defaultdict(list)
    permits_by_producer: dict[str, set[str]] = defaultdict(set)
    for row in producer_rows:
        by_producer[row["producer_id"]].append(row)
        permits_by_producer[row["producer_id"]].add(str(row["permit_key"]))

    all_shared_permits: set[str] = set()
    for spec in specs:
        if not uses_ttb_evidence(spec):
            continue
        if spec.name_a not in producer_lookup or spec.name_b not in producer_lookup:
            continue
        producer_id_a = producer_lookup[spec.name_a]["id"]
        producer_id_b = producer_lookup[spec.name_b]["id"]
        all_shared_permits.update(permits_by_producer[producer_id_a] & permits_by_producer[producer_id_b])

    permit_rows = fetch_ttb_rows_for_permits(sorted(all_shared_permits))
    permit_producer_counts: dict[str, dict[str, int]] = defaultdict(dict)
    for row in permit_rows:
        permit_producer_counts[row["permit_key"]][row["producer_id"]] = row["row_count"]

    return {
        "by_producer": by_producer,
        "permits_by_producer": permits_by_producer,
        "permit_producer_counts": permit_producer_counts,
    }


def summarize_ttb_for_producer(rows: list[dict[str, Any]], shared_permits: set[str]) -> dict[str, Any]:
    shared_rows = [row for row in rows if not shared_permits or row["permit_key"] in shared_permits]
    brand_counts: Counter[str] = Counter()
    applicant_counts: Counter[str] = Counter()
    dba_counts: Counter[str] = Counter()
    for row in shared_rows:
        if row["brand_name"]:
            brand_counts[str(row["brand_name"])] += row["row_count"]
        if row["applicant_name"]:
            applicant_counts[str(row["applicant_name"])] += row["row_count"]
        if row["applicant_dba"]:
            dba_counts[str(row["applicant_dba"])] += row["row_count"]
    return {
        "brand_names": [name for name, _ in brand_counts.most_common(4)],
        "applicant_names": [name for name, _ in applicant_counts.most_common(3)],
        "applicant_dbas": [name for name, _ in dba_counts.most_common(2)],
    }


def build_case_record(
    spec: CaseSpec,
    seed_keys: set[str],
    producer_lookup: dict[str, dict[str, Any]],
    ttb_cache: dict[str, Any],
) -> tuple[dict[str, Any] | None, str | None]:
    producer_a = producer_lookup.get(spec.name_a)
    producer_b = producer_lookup.get(spec.name_b)
    if producer_a is None or producer_b is None:
        return None, "missing_producer"

    pair_rows = fetch_pair_rows(producer_a["id"], producer_b["id"])
    if not pair_rows:
        return None, "missing_pair_rows"

    pair_rep = pair_rows[0]
    overlap_count = fetch_exact_overlap_count(producer_a["id"], producer_b["id"])
    normalized_name_a = normalize_name(spec.name_a)
    normalized_name_b = normalize_name(spec.name_b)
    shared_token_count = len(set(normalized_name_a.split()) & set(normalized_name_b.split()))

    shared_permits: set[str] = set()
    ttb_context: dict[str, Any] | None = None
    if uses_ttb_evidence(spec):
        permits_by_producer = ttb_cache["permits_by_producer"]
        shared_permits = set(permits_by_producer[producer_a["id"]]) & set(permits_by_producer[producer_b["id"]])
        if spec.evidence_kind in {"ttb_same_as", "ttb_none"} and not shared_permits:
            return None, "missing_shared_ttb_permit"

        side_a_rows = ttb_cache["by_producer"][producer_a["id"]]
        side_b_rows = ttb_cache["by_producer"][producer_b["id"]]
        permit_counts = ttb_cache["permit_producer_counts"]
        ttb_context = {
            "shared_permits": sorted(shared_permits),
            "producer_a": summarize_ttb_for_producer(side_a_rows, shared_permits),
            "producer_b": summarize_ttb_for_producer(side_b_rows, shared_permits),
            "permit_roster": {
                permit_key: {
                    producer_id: row_count
                    for producer_id, row_count in sorted(permit_counts[permit_key].items())
                }
                for permit_key in sorted(shared_permits)
            },
        }

        if spec.evidence_kind == "ttb_same_as":
            if normalized_name_a != normalized_name_b:
                return None, "ttb_same_as_name_mismatch"
            if overlap_count <= 0:
                return None, "ttb_same_as_no_exact_overlap"
        elif spec.evidence_kind == "ttb_none":
            if overlap_count != 0:
                return None, "ttb_none_has_overlap"
            if normalized_name_a == normalized_name_b:
                return None, "ttb_none_name_collision"
            if shared_token_count > 0:
                return None, "ttb_none_token_overlap"

    current_pair_key = pair_key(producer_a["id"], producer_b["id"])
    seed_member = current_pair_key in seed_keys
    scoreable_pair = spec.label in SCOREABLE_LABELS
    net_new_scoreable_pair = scoreable_pair and not seed_member
    duplicate_pair_ids = [row["pair_id"] for row in pair_rows]

    record = {
        "pack_version": PACK_VERSION,
        "record_key": f"{PACK_VERSION}:{producer_a['id']}::{producer_b['id']}",
        "record_mode": spec.record_mode,
        "normalized_truth_label": spec.label,
        "scoreable_pair": scoreable_pair,
        "net_new_scoreable_pair": net_new_scoreable_pair,
        "seed_member": seed_member,
        "family_tag": spec.family_tag,
        "pattern_cluster": spec.pattern_cluster,
        "tier_tag": derive_tier(producer_a["wine_count"], producer_b["wine_count"]),
        "country_tag": pair_rep["country"],
        "evidence_kind": spec.evidence_kind,
        "representative_pair_id": pair_rep["pair_id"],
        "duplicate_pair_ids": duplicate_pair_ids,
        "duplicate_pair_row_count": len(duplicate_pair_ids),
        "producer_id_a": producer_a["id"],
        "producer_id_b": producer_b["id"],
        "name_a": spec.name_a,
        "name_b": spec.name_b,
        "producer_a_country": producer_a["country_name"],
        "producer_b_country": producer_b["country_name"],
        "producer_a_region": producer_a["region_name"],
        "producer_b_region": producer_b["region_name"],
        "producer_a_appellation": producer_a["appellation_name"],
        "producer_b_appellation": producer_b["appellation_name"],
        "producer_a_website_url": producer_a["website_url"],
        "producer_b_website_url": producer_b["website_url"],
        "producer_a_wine_count": producer_a["wine_count"],
        "producer_b_wine_count": producer_b["wine_count"],
        "producer_a_sample_wines": producer_a["sample_wines"],
        "producer_b_sample_wines": producer_b["sample_wines"],
        "current_pair_similarity": round(pair_rep["similarity"], 4) if pair_rep["similarity"] is not None else None,
        "current_pair_wines_a": pair_rep["wines_a"],
        "current_pair_wines_b": pair_rep["wines_b"],
        "exact_wine_name_overlap_count": overlap_count,
        "name_a_normalized": normalized_name_a,
        "name_b_normalized": normalized_name_b,
        "source_urls": list(spec.source_urls),
        "source_artifact": spec.source_artifact,
        "source_note": spec.source_note,
        "rationale": spec.rationale,
        "ttb_context": ttb_context,
    }
    return record, None


def render_summary(
    rows: list[dict[str, Any]],
    skipped: list[dict[str, str]],
    live_counts: dict[str, int],
    seed_scoreable_counts: Counter[str],
) -> str:
    label_counts = Counter(row["normalized_truth_label"] for row in rows)
    net_new_rows = [row for row in rows if row["net_new_scoreable_pair"]]
    repair_rows = [row for row in rows if row["record_mode"] == "seed_repair"]
    quarantine_rows = [row for row in rows if row["normalized_truth_label"] == QUARANTINE_LABEL]

    new_label_counts = Counter(row["normalized_truth_label"] for row in net_new_rows)
    family_counts = Counter(row["family_tag"] for row in rows)
    tier_counts = Counter(row["tier_tag"] for row in rows)
    country_counts = Counter(row["country_tag"] for row in rows)
    evidence_counts = Counter(row["evidence_kind"] for row in rows)

    updated_label_counts = Counter(seed_scoreable_counts)
    updated_label_counts.update(
        row["normalized_truth_label"] for row in net_new_rows if row["normalized_truth_label"] in SCOREABLE_LABELS
    )
    updated_total = sum(updated_label_counts.values())

    label_gap_lines = []
    for label in ("SAME_AS", "RELATED_BUT_DISTINCT", "NONE"):
        current = updated_label_counts.get(label, 0)
        target = TARGET_LABEL_COUNTS[label]
        remaining = max(target - current, 0)
        label_gap_lines.append(f"- `{label}`: `{current}` current after Pack 001, need `+{remaining}` to reach `{target}`")

    family_lines = [f"- `{family}`: `{count}`" for family, count in sorted(family_counts.items())]
    tier_lines = [f"- `{tier}`: `{count}`" for tier, count in sorted(tier_counts.items())]
    country_lines = [f"- `{country}`: `{count}`" for country, count in country_counts.most_common(12)]
    evidence_lines = [f"- `{kind}`: `{count}`" for kind, count in sorted(evidence_counts.items())]
    skipped_lines = [
        f"- `{item['name_a']}` / `{item['name_b']}`: `{item['reason']}`"
        for item in skipped[:20]
    ]
    quarantine_lines = [
        f"- `{row['name_a']}` / `{row['name_b']}`"
        for row in quarantine_rows
    ]

    return "\n".join(
        [
            "# ground_truth_pack_001_v1 summary",
            "",
            "Pack 001 targets the first audited expansion pass toward Sprint 7's `1,000` scoreable pair goal.",
            "It mixes net-new scoreable pairs with explicit benchmark-truth repair / quarantine records.",
            "",
            "## Live corpus snapshot",
            "",
            f"- producers: `{live_counts['producers']}`",
            f"- active wines: `{live_counts['active_wines']}`",
            f"- wine_vintages: `{live_counts['wine_vintages']}`",
            f"- producer_dedup_pairs: `{live_counts['producer_dedup_pairs']}`",
            f"- blank verdict_source rows: `{live_counts['blank_verdict_source']}`",
            "",
            "## Pack inventory",
            "",
            f"- manifest requests: `{len(CASE_SPECS)}` candidate records",
            f"- built records: `{len(rows)}`",
            f"- net-new scoreable additions: `{len(net_new_rows)}`",
            f"- seed repair / reaffirm records: `{len(repair_rows)}`",
            f"- quarantined disputes: `{len(quarantine_rows)}`",
            f"- skipped candidate records: `{len(skipped)}`",
            "",
            "## Overall label mix",
            "",
            *[f"- `{label}`: `{count}`" for label, count in sorted(label_counts.items())],
            "",
            "## Net-new scoreable additions by label",
            "",
            *[f"- `{label}`: `{count}`" for label, count in sorted(new_label_counts.items())],
            "",
            "## Gap to 1,000 after Pack 001 additions",
            "",
            f"- total scoreable pairs after Pack 001 additions: `{updated_total}`",
            f"- remaining gap to `1,000`: `+{max(TARGET_SCOREABLE_TOTAL - updated_total, 0)}`",
            *label_gap_lines,
            "",
            "## Family mix",
            "",
            *family_lines,
            "",
            "## Tier mix",
            "",
            *tier_lines,
            "",
            "## Country mix",
            "",
            *country_lines,
            "",
            "## Evidence mix",
            "",
            *evidence_lines,
            "",
            "## Quarantined disputes",
            "",
            *(quarantine_lines or ["- none"]),
            "",
            "## Skipped candidates (first 20)",
            "",
            *(skipped_lines or ["- none"]),
            "",
            "## Notes",
            "",
            "- Pair identity is keyed on producer IDs, not raw `producer_dedup_pairs.id`, because the live table contains duplicate method-stage rows for the same producer pair.",
            "- `ttb_same_as` records require shared official TTB permit evidence plus exact wine-name overlap and normalized near-identical brand names.",
            "- `ttb_none` records require shared official TTB permit evidence plus zero exact wine-name overlap and distinct normalized brand names.",
            "- `tier_tag` for new Pack 001 records uses current live wine-count exposure buckets: `core` >= 25 wines on either side, `mid` >= 8, otherwise `tail`.",
            "- Pack 001 stays well below the aspirational `150`-record target because disputed-history cases were quarantined and several candidate pairs failed the evidence bar on re-check.",
        ]
    ) + "\n"


def make_ttb_same_as_case(name_a: str, name_b: str) -> CaseSpec:
    return CaseSpec(
        name_a=name_a,
        name_b=name_b,
        label="SAME_AS",
        family_tag="ttb_variant_same_as",
        pattern_cluster="11.4.j",
        record_mode="new_pair",
        evidence_kind="ttb_same_as",
        rationale=(
            "Official TTB rows place both renderings on the same basic permit and the names collapse to the same"
            " brand identity once generic winery terms are stripped; treat this as one brand-on-label producer, not a"
            " facility-only adjacency."
        ),
        source_artifact="source_ttb_colas",
        source_note="Shared TTB permit plus overlapping catalog and near-identical brand rendering.",
    )


def make_ttb_none_case(name_a: str, name_b: str) -> CaseSpec:
    return CaseSpec(
        name_a=name_a,
        name_b=name_b,
        label="NONE",
        family_tag="ttb_custom_crush_none",
        pattern_cluster="11.4.j",
        record_mode="new_pair",
        evidence_kind="ttb_none",
        rationale=(
            "Official TTB rows put both brands under the same permit, but shared permit is only a facility clue here."
            " The brand identities stay distinct and the catalogs do not overlap, so this remains a non-merge"
            " precision trap."
        ),
        source_artifact="source_ttb_colas",
        source_note="Shared TTB permit custom-crush / host-facility trap.",
    )


TTB_SAME_AS_CASES = [
    make_ttb_same_as_case("Aaron", "Aaron Wines"),
    make_ttb_same_as_case("Bedrock", "Bedrock Wine Co."),
    make_ttb_same_as_case("Boedecker", "Boedecker Cellars"),
    make_ttb_same_as_case("Cakebread", "Cakebread Cellars"),
    make_ttb_same_as_case("Carlson", "Carlson Vineyards"),
    make_ttb_same_as_case("Caymus", "Caymus Vineyards"),
    make_ttb_same_as_case("Chandon", "Domaine Chandon"),
    make_ttb_same_as_case("Copain", "Copain Wines"),
    make_ttb_same_as_case("Dominus", "Dominus Estate"),
    make_ttb_same_as_case("Duckhorn", "Duckhorn Vineyards"),
    make_ttb_same_as_case("Ecole No 41", "L'Ecole No 41"),
    make_ttb_same_as_case("Flowers", "Flowers Vineyards"),
    make_ttb_same_as_case("Hall", "Hall Wines"),
    make_ttb_same_as_case("Harlan", "Harlan Estate"),
    make_ttb_same_as_case("Hartwell", "Hartwell Vineyards"),
    make_ttb_same_as_case("Justin", "Justin Vineyard"),
    make_ttb_same_as_case("King Family Vineyard", "King Family Vineyards"),
    make_ttb_same_as_case("Lewis", "Lewis Cellars"),
    make_ttb_same_as_case("Marimar", "Marimar Estate"),
    make_ttb_same_as_case("Ojai", "The Ojai Vineyard"),
    make_ttb_same_as_case("Peay", "Peay Vineyards"),
    make_ttb_same_as_case("Pine Ridge", "Pine Ridge Vineyards"),
    make_ttb_same_as_case("Realm", "Realm Cellars"),
    make_ttb_same_as_case("Ridge", "Ridge Vineyards"),
    make_ttb_same_as_case("Rombauer", "Rombauer Vineyards"),
    make_ttb_same_as_case("Rodney Strong", "Rodney Strong Vineyards"),
    make_ttb_same_as_case("Robert Mondavi", "Robert Mondavi Winery"),
    make_ttb_same_as_case("Shafer", "Shafer Vineyards"),
    make_ttb_same_as_case("Silver Oak", "Silver Oak Cellars"),
    make_ttb_same_as_case("St. Jean", "Chateau St. Jean"),
    make_ttb_same_as_case("Ste. Michelle", "Chateau Ste. Michelle"),
    make_ttb_same_as_case("The Farm", "The Farm Winery"),
    make_ttb_same_as_case("White Rose", "White Rose Estate"),
]


TTB_NONE_CASES = [
    make_ttb_none_case("Clarksburg Wine Company", "Three Wine Company"),
    make_ttb_none_case("Clarksburg Wine Company", "Brownstone"),
    make_ttb_none_case("Clarksburg Wine Company", "Due Vigne"),
    make_ttb_none_case("Clarksburg Wine Company", "Miro"),
    make_ttb_none_case("Midnight Cellars", "Clavo Cellars"),
    make_ttb_none_case("Horton Vineyards", "Fox Meadow Vineyards"),
    make_ttb_none_case("Horton Vineyards", "Waterford"),
    make_ttb_none_case("Dunham Cellars", "Tulpen Cellars"),
    make_ttb_none_case("Dunham Cellars", "Aluve"),
    make_ttb_none_case("Dunham Cellars", "Elephant Seven"),
    make_ttb_none_case("Dunham Cellars", "Pursued by bear"),
    make_ttb_none_case("Dunham Cellars", "Sinclair Estate Vineyards"),
    make_ttb_none_case("Dunham Cellars", "Double Canyon"),
    make_ttb_none_case("Dunham Cellars", "DAMA Wines"),
    make_ttb_none_case("Zerba Cellars", "Tulpen Cellars"),
    make_ttb_none_case("Grape Creek Vineyards", "Heath Vineyards"),
    make_ttb_none_case("Grape Creek Vineyards", "Julien Fayard"),
    make_ttb_none_case("Grape Creek Vineyards", "Mirabeau"),
    make_ttb_none_case("Turnbull", "Clif Family Winery"),
    make_ttb_none_case("Turnbull", "Guarachi Family Wines"),
    make_ttb_none_case("Turnbull", "DeSante"),
    make_ttb_none_case("Turnbull", "Houndstooth"),
    make_ttb_none_case("Turnbull", "Shypoke"),
    make_ttb_none_case("Clif Family Winery", "Guarachi Family Wines"),
]


WEB_RBD_CASES = [
    CaseSpec(
        name_a="CrossBarn by Paul Hobbs",
        name_b="Paul Hobbs",
        label="RELATED_BUT_DISTINCT",
        family_tag="subbrand_by_parent",
        pattern_cluster="11.4.s",
        record_mode="new_pair",
        evidence_kind="official_web",
        rationale=(
            "CrossBarn is presented by the winery itself as a separate label that Paul Hobbs established after launching"
            " his namesake winery; related umbrella, distinct on-label identity."
        ),
        source_urls=("https://www.crossbarn.com/story/",),
    ),
    CaseSpec(
        name_a="Woodbridge by Robert Mondavi",
        name_b="Robert Mondavi",
        label="RELATED_BUT_DISTINCT",
        family_tag="subbrand_by_parent",
        pattern_cluster="11.4.s",
        record_mode="new_pair",
        evidence_kind="brand_literal_ttb",
        rationale=(
            "The child label explicitly carries the parent surname in its own on-label identity. Treat it as related but"
            " distinct rather than a facility-only coincidence."
        ),
        source_artifact="source_ttb_colas",
        source_note="Child label literally carries `by Robert Mondavi` branding in official TTB records.",
    ),
    CaseSpec(
        name_a="Twenty Acres by Bogle",
        name_b="Bogle",
        label="RELATED_BUT_DISTINCT",
        family_tag="subbrand_by_parent",
        pattern_cluster="11.4.s",
        record_mode="new_pair",
        evidence_kind="official_web",
        rationale=(
            "Bogle launched Twenty Acres as a named line under the Bogle family umbrella; related producer family,"
            " distinct label identity."
        ),
        source_urls=(
            "https://boglewinery.com/collection/twenty-acres/",
            "https://boglewinery.com/wine/twenty-acres-chardonnay/",
        ),
    ),
    CaseSpec(
        name_a="La Storia by Trentadue",
        name_b="Trentadue",
        label="RELATED_BUT_DISTINCT",
        family_tag="subbrand_collection",
        pattern_cluster="11.4.s",
        record_mode="new_pair",
        evidence_kind="official_web",
        rationale=(
            "Trentadue's own site separates Trentadue Estate from the La Storia Collection, which is marketed as a"
            " distinct collection under the family winery."
        ),
        source_urls=(
            "https://trentadue.com/",
            "https://trentadue.com/media/32567/2022-la-storia-cabernet-sauvignon.pdf",
        ),
    ),
    CaseSpec(
        name_a="Domaine Carneros by Taittinger",
        name_b="Taittinger",
        label="RELATED_BUT_DISTINCT",
        family_tag="foreign_domain_under_parent_house",
        pattern_cluster="11.4.s",
        record_mode="new_pair",
        evidence_kind="official_web",
        rationale=(
            "Taittinger describes Domaine Carneros as the California estate it created in partnership, which makes it"
            " related to but distinct from the Champagne parent house."
        ),
        source_urls=(
            "https://www.taittinger.com/en/our-domaines",
            "https://www.domainecarneros.com/our-story",
        ),
    ),
    CaseSpec(
        name_a="Artisan by Murdoch Hill",
        name_b="Murdoch Hill",
        label="RELATED_BUT_DISTINCT",
        family_tag="subbrand_collection",
        pattern_cluster="11.4.s",
        record_mode="new_pair",
        evidence_kind="official_web",
        rationale=(
            "Murdoch Hill treats Artisan as a named series within the family winery's range, so this is a structured"
            " sub-line rather than a merge."
        ),
        source_urls=("https://www.murdochhill.com.au/pages/about",),
    ),
    CaseSpec(
        name_a="Marius by Michel Chapoutier",
        name_b="M. Chapoutier",
        label="RELATED_BUT_DISTINCT",
        family_tag="subbrand_collection",
        pattern_cluster="11.4.s",
        record_mode="new_pair",
        evidence_kind="official_web",
        rationale=(
            "Chapoutier presents Marius as its own maison / line rooted in Marius Chapoutier's legacy, which makes it"
            " related to the parent house but not the same on-label producer identity."
        ),
        source_urls=("https://www.chapoutier.com/en/our-wines/our-domains-and-maisons/mariusbymichelchapoutier/",),
    ),
    CaseSpec(
        name_a="Sohm & Kracher",
        name_b="Kracher",
        label="RELATED_BUT_DISTINCT",
        family_tag="collaboration_label",
        pattern_cluster="11.4.o",
        record_mode="new_pair",
        evidence_kind="official_web",
        rationale=(
            "Kracher explicitly presents Sohm & Kracher as a collaboration with Aldo Sohm, so the collaboration label"
            " should stay separate from the base Kracher producer."
        ),
        source_urls=(
            "https://www.kracher.at/en/news_press/news/newsdetail/2023-gruener-veltliner-lion-sohm-kracher",
            "https://shop.kracher.at/en/sohm-kracher",
        ),
    ),
    CaseSpec(
        name_a="Carneros by Taittinger",
        name_b="Taittinger",
        label="RELATED_BUT_DISTINCT",
        family_tag="foreign_domain_under_parent_house",
        pattern_cluster="11.4.s",
        record_mode="new_pair",
        evidence_kind="official_web",
        rationale=(
            "The Carneros/Taittinger identity is presented as a California estate created by the Taittinger house;"
            " related parent lineage, distinct estate label."
        ),
        source_urls=(
            "https://www.taittinger.com/en/our-domaines",
            "https://www.domainecarneros.com/our-story",
        ),
    ),
]


SEED_REPAIR_CASES = [
    CaseSpec(
        name_a="Stadt Krems",
        name_b="Krems",
        label="SAME_AS",
        family_tag="challenge_place_alias",
        pattern_cluster="11.4.h",
        record_mode="seed_repair",
        evidence_kind="official_web",
        rationale=(
            "Fresh official winery material still supports the original merge: the winery identifies itself as Weingut"
            " Stadt Krems while selling a wine literally labeled `KREMS`."
        ),
        source_urls=(
            "https://www.weingutstadtkrems.at/weingut",
            "https://www.weingutstadtkrems.at/_files/ugd/c17f2a_e250f2b48e4a403abdf3497809b77247.pdf",
        ),
        source_artifact="data/sprints/identity-er/method_bakeoff/session10_9_web_validation_ledger_v1.md",
    ),
    CaseSpec(
        name_a="Tenuta Brunelli",
        name_b="Brunelli",
        label="NONE",
        family_tag="challenge_place_alias_negative",
        pattern_cluster="11.4.h",
        record_mode="seed_repair",
        evidence_kind="official_web",
        rationale=(
            "Fresh official sources still support distinct identities: Brunelli is the Valpolicella / Amarone producer,"
            " while Tenuta Brunelli / Martoccia is the Montalcino estate."
        ),
        source_urls=(
            "https://www.brunelliwine.com/en/",
            "https://www.poderemartoccia.it/",
        ),
        source_artifact="data/sprints/identity-er/method_bakeoff/session10_9_web_validation_ledger_v1.md",
    ),
    CaseSpec(
        name_a="de la Gaffeliere",
        name_b="Canon la Gaffeliere",
        label="NONE",
        family_tag="challenge_maison_alias_negative",
        pattern_cluster="11.4.m",
        record_mode="seed_repair",
        evidence_kind="official_web",
        rationale=(
            "Fresh official Saint-Emilion material still shows separate estates, owners, and identities for La"
            " Gaffeliere and Canon la Gaffeliere."
        ),
        source_urls=(
            "https://vins-saint-emilion.com/en/castle/chateau-la-gaffeliere-2/",
            "https://vins-saint-emilion.com/en/castle/chateau-canon-la-gaffeliere-4/",
        ),
        source_artifact="data/sprints/identity-er/method_bakeoff/session10_9_web_validation_ledger_v1.md",
    ),
    CaseSpec(
        name_a="Ardhuy Cabotte",
        name_b="de la Cabotte",
        label=QUARANTINE_LABEL,
        family_tag="challenge_maison_alias_positive",
        pattern_cluster="11.4.h",
        record_mode="seed_repair",
        evidence_kind="mixed_web_trade_registry",
        rationale=(
            "This benchmark-positive merge no longer clears the trust bar. The official d'Ardhuy and Domaine la Cabotte"
            " material points to the Rhone estate, while separate Burgundy evidence points toward a different Maison la"
            " Cabotte line. Quarantine rather than force a scoreable label."
        ),
        source_urls=(
            "https://www.ardhuy.com/en/histoire-domaine/gabriel-liogier-d-ardhuy",
            "https://www.cabotte.com/historique/",
            "https://www.soilairselection.com/maison-la-cabotte",
        ),
        source_artifact="data/sprints/identity-er/method_bakeoff/session10_9_web_validation_ledger_v1.md",
    ),
    CaseSpec(
        name_a="Protheau & Fils",
        name_b="Jean-Francois Protheau",
        label=QUARANTINE_LABEL,
        family_tag="challenge_fils_person_alias_positive",
        pattern_cluster="11.4.h",
        record_mode="seed_repair",
        evidence_kind="mixed_web_trade_registry",
        rationale=(
            "The continuity story still looks plausible but not cleanly proven from primary producer material. Keep it"
            " out of the scoreable pool until the relationship is documented more clearly."
        ),
        source_urls=(
            "https://www.pappers.fr/entreprise/domaine-jean-francois-protheau-385363791",
        ),
        source_artifact="data/sprints/identity-er/method_bakeoff/session10_9_web_validation_ledger_v1.md",
    ),
    CaseSpec(
        name_a="Fery-Meunier",
        name_b="Jean Fery & Fils",
        label=QUARANTINE_LABEL,
        family_tag="challenge_fils_person_alias_negative",
        pattern_cluster="11.4.m",
        record_mode="seed_repair",
        evidence_kind="mixed_web_trade_registry",
        rationale=(
            "Fresh evidence shows shared address / family continuity signals strong enough that the clean `NONE` story is"
            " no longer trustworthy. Quarantine rather than preserve a brittle negative."
        ),
        source_urls=(
            "https://www.fery-vin.fr/",
            "https://www.larvf.com/maison-fery-meunier%2C10572%2C405554.asp",
            "https://www.pappers.fr/entreprise/maison-fery-meunier-399867001",
        ),
        source_artifact="data/sprints/identity-er/method_bakeoff/session10_9_web_validation_ledger_v1.md",
    ),
]


CASE_SPECS = TTB_SAME_AS_CASES + TTB_NONE_CASES + WEB_RBD_CASES + SEED_REPAIR_CASES


def main() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    all_names = sorted({spec.name_a for spec in CASE_SPECS} | {spec.name_b for spec in CASE_SPECS})
    producer_lookup = fetch_producer_snapshots(all_names)
    seed_keys = build_seed_index()
    ttb_cache = build_ttb_cache(CASE_SPECS, producer_lookup)

    rows: list[dict[str, Any]] = []
    skipped: list[dict[str, str]] = []
    for spec in CASE_SPECS:
        record, reason = build_case_record(spec, seed_keys, producer_lookup, ttb_cache)
        if record is None:
            skipped.append({"name_a": spec.name_a, "name_b": spec.name_b, "reason": str(reason)})
            continue
        rows.append(record)

    rows.sort(key=lambda row: (row["record_mode"], row["family_tag"], row["country_tag"], row["name_a"], row["name_b"]))
    write_jsonl(PACK_OUTPUT_PATH, rows)

    live_counts = fetch_live_counts()
    seed_scoreable_counts = Counter(
        row["normalized_truth_label"]
        for row in read_jsonl(SEED_PAIRS_PATH)
        if row.get("scoreable_pair")
    )
    SUMMARY_OUTPUT_PATH.write_text(
        render_summary(rows, skipped, live_counts, seed_scoreable_counts),
        encoding="utf-8",
    )

    print(str(PACK_OUTPUT_PATH))
    print(str(SUMMARY_OUTPUT_PATH))


if __name__ == "__main__":
    main()
