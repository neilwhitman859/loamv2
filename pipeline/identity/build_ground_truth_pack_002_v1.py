"""Build Sprint 7 ground-truth Pack 002 from live read-only data.

Pack 002 extends the audited truth base with:
1. cross-country SAME_AS merchant / house-brand continuity,
2. non-FR RELATED_BUT_DISTINCT parent / collaboration families, and
3. non-FR NONE precision traps backed by shared official TTB permits.

Outputs:
- data/sprints/identity-er/ground_truth_pack_002_v1.jsonl
- data/sprints/identity-er/ground_truth_pack_002_summary_v1.md
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
PACK_OUTPUT_PATH = OUTPUT_DIR / "ground_truth_pack_002_v1.jsonl"
SUMMARY_OUTPUT_PATH = OUTPUT_DIR / "ground_truth_pack_002_summary_v1.md"
SEED_PAIRS_PATH = OUTPUT_DIR / "ground_truth_seed_pairs_v1.jsonl"
PACK_001_PATH = OUTPUT_DIR / "ground_truth_pack_001_v1.jsonl"

PACK_VERSION = "ground_truth_pack_002_v1"
PACK_BASELINE_VERSION = "ground_truth_pack_001_v1"
SCOREABLE_LABELS = {"SAME_AS", "RELATED_BUT_DISTINCT", "NONE"}
TARGET_SCOREABLE_TOTAL = 1000
TARGET_LABEL_COUNTS = {
    "SAME_AS": 300,
    "RELATED_BUT_DISTINCT": 200,
    "NONE": 500,
}

GENERIC_NAME_TOKENS = {
    "the",
    "and",
    "by",
    "bros",
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
    producer_id_a: str
    producer_id_b: str
    representative_pair_id: int
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
    require_distinct_countries: bool = False
    require_normalized_name_match: bool = False
    require_normalized_name_distinct: bool = False
    require_zero_overlap: bool = False
    require_shared_ttb_permit: bool = False
    require_zero_shared_tokens: bool = False


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


def fetch_producer_snapshots_by_ids(producer_ids: list[str]) -> dict[str, dict[str, Any]]:
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
          where p.id = any(%s::uuid[])
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
        (producer_ids,),
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
        result[str(producer_id)] = {
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


def build_ttb_cache(specs: list[CaseSpec]) -> dict[str, Any]:
    producer_ids = sorted(
        {
            spec.producer_id_a
            for spec in specs
            if spec.require_shared_ttb_permit
        }
        | {
            spec.producer_id_b
            for spec in specs
            if spec.require_shared_ttb_permit
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
        if not spec.require_shared_ttb_permit:
            continue
        all_shared_permits.update(
            permits_by_producer[spec.producer_id_a] & permits_by_producer[spec.producer_id_b]
        )

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


def build_truth_index(rows: list[dict[str, Any]]) -> set[str]:
    keys: set[str] = set()
    for row in rows:
        producer_id_a = row.get("producer_id_a")
        producer_id_b = row.get("producer_id_b")
        if producer_id_a and producer_id_b:
            keys.add(pair_key(str(producer_id_a), str(producer_id_b)))
    return keys


def build_pack_001_scoreable_counts() -> Counter[str]:
    counts: Counter[str] = Counter()
    for row in read_jsonl(PACK_001_PATH):
        if row.get("net_new_scoreable_pair") and row.get("normalized_truth_label") in SCOREABLE_LABELS:
            counts[str(row["normalized_truth_label"])] += 1
    return counts


def build_case_record(
    spec: CaseSpec,
    seed_keys: set[str],
    pack_001_keys: set[str],
    producer_lookup: dict[str, dict[str, Any]],
    ttb_cache: dict[str, Any],
) -> tuple[dict[str, Any] | None, str | None]:
    producer_a = producer_lookup.get(spec.producer_id_a)
    producer_b = producer_lookup.get(spec.producer_id_b)
    if producer_a is None or producer_b is None:
        return None, "missing_producer"
    if producer_a["name"] != spec.name_a or producer_b["name"] != spec.name_b:
        return None, "producer_name_mismatch"

    pair_rows = fetch_pair_rows(spec.producer_id_a, spec.producer_id_b)
    if not pair_rows:
        return None, "missing_pair_rows"

    pair_rep = next((row for row in pair_rows if row["pair_id"] == spec.representative_pair_id), None)
    if pair_rep is None:
        return None, "missing_representative_pair_id"

    overlap_count = fetch_exact_overlap_count(spec.producer_id_a, spec.producer_id_b)
    normalized_name_a = normalize_name(spec.name_a)
    normalized_name_b = normalize_name(spec.name_b)
    shared_token_count = len(set(normalized_name_a.split()) & set(normalized_name_b.split()))

    if spec.require_distinct_countries and producer_a["country_name"] == producer_b["country_name"]:
        return None, "same_country_pair"
    if spec.require_normalized_name_match and normalized_name_a != normalized_name_b:
        return None, "normalized_name_mismatch"
    if spec.require_normalized_name_distinct and normalized_name_a == normalized_name_b:
        return None, "normalized_name_collision"
    if spec.require_zero_overlap and overlap_count != 0:
        return None, "unexpected_exact_wine_overlap"
    if spec.require_zero_shared_tokens and shared_token_count != 0:
        return None, "shared_name_tokens"

    shared_permits: set[str] = set()
    ttb_context: dict[str, Any] | None = None
    if spec.require_shared_ttb_permit:
        permits_by_producer = ttb_cache["permits_by_producer"]
        shared_permits = set(permits_by_producer[spec.producer_id_a]) & set(permits_by_producer[spec.producer_id_b])
        if not shared_permits:
            return None, "missing_shared_ttb_permit"

        side_a_rows = ttb_cache["by_producer"][spec.producer_id_a]
        side_b_rows = ttb_cache["by_producer"][spec.producer_id_b]
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

    current_pair_key = pair_key(spec.producer_id_a, spec.producer_id_b)
    seed_member = current_pair_key in seed_keys
    pack_001_member = current_pair_key in pack_001_keys
    prior_truth_member = seed_member or pack_001_member
    scoreable_pair = spec.label in SCOREABLE_LABELS
    net_new_scoreable_pair = scoreable_pair and not prior_truth_member
    duplicate_pair_ids = [row["pair_id"] for row in pair_rows]

    record = {
        "pack_version": PACK_VERSION,
        "record_key": f"{PACK_VERSION}:{spec.producer_id_a}::{spec.producer_id_b}",
        "record_mode": spec.record_mode,
        "normalized_truth_label": spec.label,
        "scoreable_pair": scoreable_pair,
        "net_new_scoreable_pair": net_new_scoreable_pair,
        "seed_member": seed_member,
        "pack_001_member": pack_001_member,
        "prior_truth_member": prior_truth_member,
        "family_tag": spec.family_tag,
        "pattern_cluster": spec.pattern_cluster,
        "tier_tag": derive_tier(producer_a["wine_count"], producer_b["wine_count"]),
        "country_tag": pair_rep["country"],
        "evidence_kind": spec.evidence_kind,
        "representative_pair_id": pair_rep["pair_id"],
        "duplicate_pair_ids": duplicate_pair_ids,
        "duplicate_pair_row_count": len(duplicate_pair_ids),
        "producer_id_a": spec.producer_id_a,
        "producer_id_b": spec.producer_id_b,
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
    baseline_scoreable_counts: Counter[str],
) -> str:
    label_counts = Counter(row["normalized_truth_label"] for row in rows)
    net_new_rows = [row for row in rows if row["net_new_scoreable_pair"]]
    prior_truth_rows = [row for row in rows if row["prior_truth_member"]]

    new_label_counts = Counter(row["normalized_truth_label"] for row in net_new_rows)
    family_counts = Counter(row["family_tag"] for row in rows)
    tier_counts = Counter(row["tier_tag"] for row in rows)
    country_counts = Counter(row["country_tag"] for row in rows)
    evidence_counts = Counter(row["evidence_kind"] for row in rows)

    updated_label_counts = Counter(baseline_scoreable_counts)
    updated_label_counts.update(
        row["normalized_truth_label"] for row in net_new_rows if row["normalized_truth_label"] in SCOREABLE_LABELS
    )
    updated_total = sum(updated_label_counts.values())
    baseline_total = sum(baseline_scoreable_counts.values())

    label_gap_lines = []
    for label in ("SAME_AS", "RELATED_BUT_DISTINCT", "NONE"):
        current = updated_label_counts.get(label, 0)
        target = TARGET_LABEL_COUNTS[label]
        remaining = max(target - current, 0)
        label_gap_lines.append(f"- `{label}`: `{current}` current after Pack 002, need `+{remaining}` to reach `{target}`")

    family_lines = [f"- `{family}`: `{count}`" for family, count in sorted(family_counts.items())]
    tier_lines = [f"- `{tier}`: `{count}`" for tier, count in sorted(tier_counts.items())]
    country_lines = [f"- `{country}`: `{count}`" for country, count in country_counts.most_common(12)]
    evidence_lines = [f"- `{kind}`: `{count}`" for kind, count in sorted(evidence_counts.items())]
    skipped_lines = [
        f"- `{item['name_a']}` / `{item['name_b']}`: `{item['reason']}`"
        for item in skipped[:20]
    ]

    return "\n".join(
        [
            "# ground_truth_pack_002_v1 summary",
            "",
            "Pack 002 extends the audited Sprint 7 truth base with cross-country SAME_AS continuity,",
            "non-FR RELATED_BUT_DISTINCT families, and non-FR shared-permit NONE traps.",
            "",
            "## Live corpus snapshot",
            "",
            f"- producers: `{live_counts['producers']}`",
            f"- active wines: `{live_counts['active_wines']}`",
            f"- wine_vintages: `{live_counts['wine_vintages']}`",
            f"- producer_dedup_pairs: `{live_counts['producer_dedup_pairs']}`",
            f"- blank verdict_source rows: `{live_counts['blank_verdict_source']}`",
            "",
            "## Baseline before Pack 002",
            "",
            f"- scoreable pairs after {PACK_BASELINE_VERSION}: `{baseline_total}`",
            *[
                f"- `{label}` baseline after {PACK_BASELINE_VERSION}: `{baseline_scoreable_counts.get(label, 0)}`"
                for label in ("SAME_AS", "RELATED_BUT_DISTINCT", "NONE")
            ],
            "",
            "## Pack inventory",
            "",
            f"- manifest requests: `{len(CASE_SPECS)}` candidate records",
            f"- built records: `{len(rows)}`",
            f"- net-new scoreable additions: `{len(net_new_rows)}`",
            f"- prior-truth overlaps carried forward: `{len(prior_truth_rows)}`",
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
            "## Gap to 1,000 after Pack 002 additions",
            "",
            f"- total scoreable pairs after Pack 002 additions: `{updated_total}`",
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
            "## Skipped candidates (first 20)",
            "",
            *(skipped_lines or ["- none"]),
            "",
            "## Notes",
            "",
            "- Pair identity stays keyed on producer IDs, not raw `producer_dedup_pairs.id`, because the live table still contains duplicate method-stage rows per producer pair.",
            "- Pack 002 switches the builder to explicit producer IDs because Pack 001's name-keyed lookup would collapse cross-country same-name merchant-brand cases.",
            "- `official_web_cross_country_same_brand` cases require matching normalized producer names across distinct countries.",
            "- `ttb_none` cases require shared official TTB permit evidence plus zero exact wine-name overlap and distinct normalized brand identities.",
            "- Pack 002 keeps Pack 001 labels fixed; this pack only adds new audited cases that still clear the strict evidence bar.",
        ]
    ) + "\n"


def same_as_case(
    *,
    producer_id_a: str,
    producer_id_b: str,
    representative_pair_id: int,
    name: str,
    source_urls: tuple[str, ...],
    source_note: str,
    rationale: str,
) -> CaseSpec:
    return CaseSpec(
        producer_id_a=producer_id_a,
        producer_id_b=producer_id_b,
        representative_pair_id=representative_pair_id,
        name_a=name,
        name_b=name,
        label="SAME_AS",
        family_tag="global_brand_cross_country",
        pattern_cluster="11.4.n",
        record_mode="new_pair",
        evidence_kind="official_web_cross_country_same_brand",
        rationale=rationale,
        source_urls=source_urls,
        source_note=source_note,
        require_distinct_countries=True,
        require_normalized_name_match=True,
    )


def rbd_case(
    *,
    producer_id_a: str,
    producer_id_b: str,
    representative_pair_id: int,
    name_a: str,
    name_b: str,
    family_tag: str,
    pattern_cluster: str,
    source_urls: tuple[str, ...],
    source_note: str,
    rationale: str,
) -> CaseSpec:
    return CaseSpec(
        producer_id_a=producer_id_a,
        producer_id_b=producer_id_b,
        representative_pair_id=representative_pair_id,
        name_a=name_a,
        name_b=name_b,
        label="RELATED_BUT_DISTINCT",
        family_tag=family_tag,
        pattern_cluster=pattern_cluster,
        record_mode="new_pair",
        evidence_kind="official_web",
        rationale=rationale,
        source_urls=source_urls,
        source_note=source_note,
    )


def ttb_none_case(
    *,
    producer_id_a: str,
    producer_id_b: str,
    representative_pair_id: int,
    name_a: str,
    name_b: str,
) -> CaseSpec:
    return CaseSpec(
        producer_id_a=producer_id_a,
        producer_id_b=producer_id_b,
        representative_pair_id=representative_pair_id,
        name_a=name_a,
        name_b=name_b,
        label="NONE",
        family_tag="ttb_shared_permit_none",
        pattern_cluster="11.4.j",
        record_mode="new_pair",
        evidence_kind="ttb_none",
        rationale=(
            "Official TTB rows put both brands under the same permit, but the shared permit is only a host-facility clue"
            " here. The brand identities remain distinct and the catalogs do not overlap, so this should stay a"
            " non-merge precision trap."
        ),
        source_artifact="source_ttb_colas",
        source_note="Shared official TTB permit with zero exact wine-name overlap and distinct brand identity.",
        require_normalized_name_distinct=True,
        require_zero_overlap=True,
        require_shared_ttb_permit=True,
        require_zero_shared_tokens=True,
    )


CASE_SPECS = [
    same_as_case(
        producer_id_a="2ab1618e-74ff-4ba2-a498-c255fd29e8b0",
        producer_id_b="ad52f8d3-a42f-4158-82a8-81fe7f72aaca",
        representative_pair_id=140079,
        name="Penfolds",
        source_urls=(
            "https://us.penfolds.com/collections/2025-collection",
            "https://us.penfolds.com/en-us/vineyards/regions.html",
        ),
        source_note="Official Penfolds material presents one house style spanning Australia, the USA, France, and China.",
        rationale=(
            "Penfolds explicitly presents these country-specific releases as one global Penfolds house, so the"
            " cross-country producer rows should count as the same producer identity rather than parallel lookalikes."
        ),
    ),
    same_as_case(
        producer_id_a="2ab1618e-74ff-4ba2-a498-c255fd29e8b0",
        producer_id_b="9c11e370-8567-4225-91d3-827f72746cc6",
        representative_pair_id=139165,
        name="Penfolds",
        source_urls=(
            "https://us.penfolds.com/collections/2025-collection",
            "https://us.penfolds.com/en-us/vineyards/regions.html",
        ),
        source_note="Official Penfolds material presents one house style spanning Australia, the USA, France, and China.",
        rationale=(
            "Penfolds explicitly presents these country-specific releases as one global Penfolds house, so the"
            " cross-country producer rows should count as the same producer identity rather than parallel lookalikes."
        ),
    ),
    same_as_case(
        producer_id_a="2ab1618e-74ff-4ba2-a498-c255fd29e8b0",
        producer_id_b="53a640c5-b1c5-45d6-b819-6c1cfc8215b9",
        representative_pair_id=136766,
        name="Penfolds",
        source_urls=(
            "https://us.penfolds.com/collections/2025-collection",
            "https://us.penfolds.com/en-us/vineyards/regions.html",
        ),
        source_note="Official Penfolds material presents one house style spanning Australia, the USA, France, and China.",
        rationale=(
            "Penfolds explicitly presents these country-specific releases as one global Penfolds house, so the"
            " cross-country producer rows should count as the same producer identity rather than parallel lookalikes."
        ),
    ),
    same_as_case(
        producer_id_a="84b3409b-82b7-423c-a1cf-1168b5b30093",
        producer_id_b="985c7848-4597-4894-9a99-14bd6ab73d44",
        representative_pair_id=139024,
        name="90+ Cellars",
        source_urls=(
            "https://www.ninetypluscellars.com/collections/90-wines",
            "https://www.ninetypluscellars.com/products/lot-23-old-vine-malbec-mendoza-argentina",
        ),
        source_note="Official 90+ Cellars material presents Argentina releases as part of the same 90+ Cellars house portfolio.",
        rationale=(
            "90+ Cellars sells these wines under one merchant-house identity across countries, so cross-country producer"
            " rows with the same name should resolve to one producer truth rather than separate country houses."
        ),
    ),
    same_as_case(
        producer_id_a="5eb22f03-9f04-4532-889c-4ea9ef2ddebe",
        producer_id_b="985c7848-4597-4894-9a99-14bd6ab73d44",
        representative_pair_id=139028,
        name="90+ Cellars",
        source_urls=(
            "https://www.ninetypluscellars.com/collections/classic-wines",
            "https://www.ninetypluscellars.com/products/lot-21-french-fusion-red-languedoc-france",
        ),
        source_note="Official 90+ Cellars material presents France releases as part of the same 90+ Cellars house portfolio.",
        rationale=(
            "90+ Cellars sells these wines under one merchant-house identity across countries, so cross-country producer"
            " rows with the same name should resolve to one producer truth rather than separate country houses."
        ),
    ),
    same_as_case(
        producer_id_a="985c7848-4597-4894-9a99-14bd6ab73d44",
        producer_id_b="bec97812-d6dc-49bb-85dd-a60469b0f4af",
        representative_pair_id=140819,
        name="90+ Cellars",
        source_urls=(
            "https://www.ninetypluscellars.com/collections/classic-wines",
            "https://www.ninetypluscellars.com/products/lot-197-prosecco-rose",
        ),
        source_note="Official 90+ Cellars material presents Italy releases as part of the same 90+ Cellars house portfolio.",
        rationale=(
            "90+ Cellars sells these wines under one merchant-house identity across countries, so cross-country producer"
            " rows with the same name should resolve to one producer truth rather than separate country houses."
        ),
    ),
    same_as_case(
        producer_id_a="d7653909-e259-4cd0-b542-e9685915cfcd",
        producer_id_b="f815d7ce-43d5-4ccf-8392-08bd7c84a534",
        representative_pair_id=143722,
        name="Scout & Cellar",
        source_urls=(
            "https://scoutandcellar.com/products/nv-scout-wild-red-blend-california-750ml",
            "https://scoutandcellar.com/pages/etnico",
        ),
        source_note="Official Scout & Cellar material presents both California and Chile wines inside the same Scout & Cellar house portfolio.",
        rationale=(
            "Scout & Cellar is acting as the same merchant-house producer identity across these country rows, so the"
            " same-name cross-country entries should be merged in truth rather than split by sourcing country."
        ),
    ),
    same_as_case(
        producer_id_a="e8c04d48-0dce-403e-a4a9-f2ac4f8c8b16",
        producer_id_b="f815d7ce-43d5-4ccf-8392-08bd7c84a534",
        representative_pair_id=143720,
        name="Scout & Cellar",
        source_urls=(
            "https://scoutandcellar.com/products/nv-scout-wild-red-blend-california-750ml",
            "https://scoutandcellar.com/products/the-sparkling-set",
        ),
        source_note="Official Scout & Cellar material presents both California and Italy wines inside the same Scout & Cellar house portfolio.",
        rationale=(
            "Scout & Cellar is acting as the same merchant-house producer identity across these country rows, so the"
            " same-name cross-country entries should be merged in truth rather than split by sourcing country."
        ),
    ),
    same_as_case(
        producer_id_a="4bc9b792-91ca-43f0-adbf-d15557ea43d5",
        producer_id_b="aaf53c55-368f-4fd2-aaec-d1b9385401a3",
        representative_pair_id=139957,
        name="Berry Bros. & Rudd",
        source_urls=(
            "https://www.bbr.com/our-own-selection",
            "https://www.bbr.com/own-selection-wine",
        ),
        source_note="Berry Bros. & Rudd's official own-selection pages present one merchant house spanning France, Spain, Italy, and Portugal.",
        rationale=(
            "Berry Bros. & Rudd's own-selection wines are explicitly a single merchant-house line produced across several"
            " countries, so same-name country rows should count as one producer identity."
        ),
    ),
    same_as_case(
        producer_id_a="07f9d1e7-ffeb-4a88-9d1e-c6e4b4e6c4a2",
        producer_id_b="4bc9b792-91ca-43f0-adbf-d15557ea43d5",
        representative_pair_id=136595,
        name="Berry Bros. & Rudd",
        source_urls=(
            "https://www.bbr.com/our-own-selection",
            "https://us.bbr.com/collections/our-own-selection-wine-from-italy",
        ),
        source_note="Berry Bros. & Rudd's official own-selection pages present one merchant house spanning France, Spain, Italy, and Portugal.",
        rationale=(
            "Berry Bros. & Rudd's own-selection wines are explicitly a single merchant-house line produced across several"
            " countries, so same-name country rows should count as one producer identity."
        ),
    ),
    same_as_case(
        producer_id_a="07e62539-ae2c-4402-9c33-0c0e7ac90abc",
        producer_id_b="4bc9b792-91ca-43f0-adbf-d15557ea43d5",
        representative_pair_id=136597,
        name="Berry Bros. & Rudd",
        source_urls=(
            "https://www.bbr.com/our-own-selection",
            "https://www.bbr.com/products-10008010902-berry-bros-and-rudd-william-pickering-tawny-port-by-quinta-do-noval",
        ),
        source_note="Berry Bros. & Rudd's official own-selection pages present one merchant house spanning France, Spain, Italy, and Portugal.",
        rationale=(
            "Berry Bros. & Rudd's own-selection wines are explicitly a single merchant-house line produced across several"
            " countries, so same-name country rows should count as one producer identity."
        ),
    ),
    rbd_case(
        producer_id_a="64ae18ce-1034-44bb-b853-5d8a59971d18",
        producer_id_b="d82ae251-ea5c-4f22-a804-cdfdb9307bdc",
        representative_pair_id=63478,
        name_a="Bee Tree by Sugrue",
        name_b="Sugrue",
        family_tag="estate_under_parent_house",
        pattern_cluster="11.4.s",
        source_urls=("https://www.sugruesouthdowns.com/pages/about",),
        source_note="Sugrue's official site describes Bee Tree as the vineyard Sugrue South Downs purchased as its first vineyard in 2023.",
        rationale=(
            "Bee Tree is presented by Sugrue as a named vineyard / label inside the Sugrue house rather than a synonym"
            " for the base producer, so the relationship is related but distinct."
        ),
    ),
    rbd_case(
        producer_id_a="89e35aa8-6dbc-4e91-a022-dc54471c661e",
        producer_id_b="bb50f553-ab5d-4b0a-9775-ee0ba9a639d7",
        representative_pair_id=161860,
        name_a="Tement",
        name_b="Ciringa (Tement)",
        family_tag="cross_border_estate_under_parent_house",
        pattern_cluster="11.4.s",
        source_urls=(
            "https://www.tement.at/en/",
            "https://www.tement.at/wp-content/uploads/2023/08/press-text_short_V2_E.pdf",
        ),
        source_note="Official Tement material describes Domaine Ciringa as the Slovenian estate created from the Tement family's vineyards over the border.",
        rationale=(
            "Tement presents Ciringa as its Slovenian estate / sibling domain, which makes it lineage-related but still"
            " a separate on-label identity rather than a clean merge."
        ),
    ),
    rbd_case(
        producer_id_a="2ab1618e-74ff-4ba2-a498-c255fd29e8b0",
        producer_id_b="b8851b21-01d0-4a5e-bfa3-a92ae583ee28",
        representative_pair_id=161793,
        name_a="Penfolds",
        name_b="Penfolds & Domaine de la Chapelle",
        family_tag="cross_house_collaboration_label",
        pattern_cluster="11.4.o",
        source_urls=(
            "https://www.penfolds.com/en-us/wines/limited-editions/grange-lachapelle.html",
            "https://us.penfolds.com/blogs/news-collaborations/grange-x-la-chapelle",
        ),
        source_note="Official Penfolds material presents Grange La Chapelle as a joint Penfolds / La Chapelle collaboration release.",
        rationale=(
            "Penfolds & Domaine de la Chapelle is an explicit collaboration label joining two houses, so it should stay"
            " distinct from the base Penfolds producer while remaining clearly related."
        ),
    ),
    rbd_case(
        producer_id_a="13ec571b-5cfa-4908-ad67-b312b39ae368",
        producer_id_b="801e4586-1f3a-4758-bb9a-92125cf8be90",
        representative_pair_id=138057,
        name_a="Kracher",
        name_b="Liliac & Kracher",
        family_tag="cross_house_collaboration_label",
        pattern_cluster="11.4.o",
        source_urls=("https://www.kracher.at/en/about_us/kracher_friends/",),
        source_note="Official Kracher material presents Liliac under the Kracher & Friends collaboration umbrella.",
        rationale=(
            "Liliac & Kracher is presented as a collaboration between Kracher and the Romanian house Liliac, so it"
            " belongs in RELATED_BUT_DISTINCT rather than collapsing into Kracher."
        ),
    ),
    rbd_case(
        producer_id_a="1af5edf8-e962-4938-8d87-452ad2d7e537",
        producer_id_b="6a58f016-040f-4e81-9eee-160a0a35ce2d",
        representative_pair_id=160415,
        name_a="Antinori",
        name_b="Ste. Michelle & Antinori",
        family_tag="joint_venture_label",
        pattern_cluster="11.4.o",
        source_urls=(
            "https://www.antinori.it/en/tenuta/estates-world/col-solare-estate/",
            "https://colsolare.com/product/2018-col-solare-cabernet-sauvignon/",
        ),
        source_note="Official Antinori and Col Solare material presents Col Solare as the Antinori / Chateau Ste. Michelle partnership.",
        rationale=(
            "Ste. Michelle & Antinori is an explicit joint venture identity, which should remain distinct from the base"
            " Antinori producer while staying in the related-family bucket."
        ),
    ),
    ttb_none_case(
        producer_id_a="0caea022-da5b-4b93-a464-664f506bf089",
        producer_id_b="e599a36b-d84a-4eee-9796-8d84bba18f38",
        representative_pair_id=120693,
        name_a="Midnight Cellars",
        name_b="Tobin James",
    ),
    ttb_none_case(
        producer_id_a="0caea022-da5b-4b93-a464-664f506bf089",
        producer_id_b="d3a61865-aa31-4ea3-abe4-0ed722171822",
        representative_pair_id=119742,
        name_a="Midnight Cellars",
        name_b="Calcareous Vineyard",
    ),
    ttb_none_case(
        producer_id_a="c380f5d1-3662-4b8a-b76d-89d2d6913238",
        producer_id_b="d8e088e4-fe49-40f4-809a-1bcd22462fff",
        representative_pair_id=120165,
        name_a="Vina Robles",
        name_b="The Language of Yes",
    ),
    ttb_none_case(
        producer_id_a="892a9076-b798-4d03-8d9a-1bfb87ef046f",
        producer_id_b="c380f5d1-3662-4b8a-b76d-89d2d6913238",
        representative_pair_id=119028,
        name_a="J Dusi",
        name_b="Vina Robles",
    ),
    ttb_none_case(
        producer_id_a="156c6f7c-0677-4716-a7a1-a4bd38fb0a6f",
        producer_id_b="84adb2e9-b30b-45c0-b8af-aabdf131d6a9",
        representative_pair_id=116557,
        name_a="Turnbull",
        name_b="Premiere Napa Valley",
    ),
]


def main() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    producer_lookup = fetch_producer_snapshots_by_ids(
        sorted({spec.producer_id_a for spec in CASE_SPECS} | {spec.producer_id_b for spec in CASE_SPECS})
    )
    seed_keys = build_truth_index(read_jsonl(SEED_PAIRS_PATH))
    pack_001_rows = read_jsonl(PACK_001_PATH)
    pack_001_keys = build_truth_index(pack_001_rows)
    ttb_cache = build_ttb_cache(CASE_SPECS)

    rows: list[dict[str, Any]] = []
    skipped: list[dict[str, str]] = []
    for spec in CASE_SPECS:
        record, reason = build_case_record(spec, seed_keys, pack_001_keys, producer_lookup, ttb_cache)
        if record is None:
            skipped.append({"name_a": spec.name_a, "name_b": spec.name_b, "reason": str(reason)})
            continue
        rows.append(record)

    rows.sort(key=lambda row: (row["record_mode"], row["family_tag"], row["country_tag"], row["name_a"], row["name_b"]))
    write_jsonl(PACK_OUTPUT_PATH, rows)

    live_counts = fetch_live_counts()
    baseline_scoreable_counts = Counter(
        row["normalized_truth_label"]
        for row in read_jsonl(SEED_PAIRS_PATH)
        if row.get("scoreable_pair")
    )
    baseline_scoreable_counts.update(build_pack_001_scoreable_counts())
    SUMMARY_OUTPUT_PATH.write_text(
        render_summary(rows, skipped, live_counts, baseline_scoreable_counts),
        encoding="utf-8",
    )

    print(str(PACK_OUTPUT_PATH))
    print(str(SUMMARY_OUTPUT_PATH))


if __name__ == "__main__":
    main()
