"""Session 10.9 - build live read-only case snapshots for web validation.

This script records a small set of producer-pair snapshots so the web-grounded
validation memo has durable local context from the live canonical tables.
"""

from __future__ import annotations

import json
import os
import re
from pathlib import Path

import psycopg2
from psycopg2.extras import RealDictCursor


REPO_ROOT = Path(__file__).resolve().parents[2]
OUTPUT_PATH = (
    REPO_ROOT
    / "data"
    / "sprints"
    / "identity-er"
    / "method_bakeoff"
    / "session10_9_case_snapshots_v1.json"
)

CASE_SPECS = [
    {
        "validation_case_id": "place_positive_stadt_krems",
        "family": "place_alias",
        "pair_kind": "benchmark_positive_exact",
        "left_name": "Stadt Krems",
        "right_name": "Krems",
    },
    {
        "validation_case_id": "place_negative_tenuta_brunelli",
        "family": "place_alias",
        "pair_kind": "outside_benchmark_negative_analogue",
        "left_name": "Tenuta Brunelli",
        "right_name": "Brunelli",
    },
    {
        "validation_case_id": "maison_positive_ardhuy_cabotte",
        "family": "maison_alias",
        "pair_kind": "benchmark_positive_exact",
        "left_name": "Ardhuy Cabotte",
        "right_name": "de la Cabotte",
    },
    {
        "validation_case_id": "maison_negative_gaffeliere",
        "family": "maison_alias",
        "pair_kind": "outside_benchmark_negative_analogue",
        "left_name": "de la Gaffeliere",
        "right_name": "Canon la Gaffeliere",
    },
    {
        "validation_case_id": "fils_positive_protheau",
        "family": "fils_person_alias",
        "pair_kind": "benchmark_positive_exact",
        "left_name": "Protheau & Fils",
        "right_name": "Jean-Francois Protheau",
    },
    {
        "validation_case_id": "fils_negative_jean_boillot",
        "family": "fils_person_alias",
        "pair_kind": "benchmark_negative_exact",
        "left_name": "Jean Boillot & Fils",
        "right_name": "Jean-Marc Boillot",
    },
    {
        "validation_case_id": "fils_negative_fery_meunier",
        "family": "fils_person_alias",
        "pair_kind": "outside_benchmark_negative_analogue",
        "left_name": "Fery-Meunier",
        "right_name": "Jean Fery & Fils",
    },
    {
        "validation_case_id": "fils_negative_grivelet",
        "family": "fils_person_alias",
        "pair_kind": "outside_benchmark_negative_analogue",
        "left_name": "Grivelet Pere & Fils",
        "right_name": "Grivelet-Cusset",
    },
]

STOPWORDS = {"de", "la", "le", "les", "et", "fils", "pere", "and"}


def load_database_url() -> str:
    env_path = REPO_ROOT / ".env"
    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        if key == "DATABASE_URL":
            return value
    raise RuntimeError("DATABASE_URL not found in .env")


def tokenize(value: str) -> list[str]:
    return re.findall(r"[A-Za-z']+", value.lower())


def lexical_summary(left_name: str, right_name: str) -> dict:
    left_tokens = tokenize(left_name)
    right_tokens = tokenize(right_name)
    left_core = [token for token in left_tokens if token not in STOPWORDS]
    right_core = [token for token in right_tokens if token not in STOPWORDS]
    shared_core = sorted(set(left_core) & set(right_core))
    return {
        "left_tokens": left_tokens,
        "right_tokens": right_tokens,
        "shared_core_tokens": shared_core,
        "left_contains_right": right_name.lower() in left_name.lower(),
        "right_contains_left": left_name.lower() in right_name.lower(),
    }


def fetch_side(cursor: RealDictCursor, name: str) -> dict:
    cursor.execute(
        """
        with producer as (
          select
            p.id,
            p.name,
            p.website_url,
            c.name as country,
            r.name as region,
            a.name as appellation
          from producers p
          left join countries c on c.id = p.country_id
          left join regions r on r.id = p.region_id
          left join appellations a on a.id = p.appellation_id
          where p.name = %s
        ),
        reps as (
          select
            w.producer_id,
            array_agg(w.name order by w.name) as wines
          from wines w
          where w.deleted_at is null
            and w.producer_id in (select id from producer)
          group by w.producer_id
        ),
        counts as (
          select
            w.producer_id,
            count(*) as wine_count
          from wines w
          where w.deleted_at is null
            and w.producer_id in (select id from producer)
          group by w.producer_id
        )
        select
          producer.*,
          coalesce(counts.wine_count, 0) as wine_count,
          coalesce((reps.wines)[1:5], array[]::text[]) as representative_wines
        from producer
        left join counts on counts.producer_id = producer.id
        left join reps on reps.producer_id = producer.id
        """,
        (name,),
    )
    row = cursor.fetchone()
    if row is None:
        raise RuntimeError(f"Producer not found: {name}")
    return dict(row)


def fetch_overlap_count(cursor: RealDictCursor, left_name: str, right_name: str) -> int:
    cursor.execute(
        """
        select count(*)
        from wines wa
        join producers pa on pa.id = wa.producer_id
        join wines wb on wb.name = wa.name
        join producers pb on pb.id = wb.producer_id
        where pa.name = %s
          and pb.name = %s
          and wa.deleted_at is null
          and wb.deleted_at is null
        """,
        (left_name, right_name),
    )
    return int(cursor.fetchone()["count"])


def build_snapshots() -> list[dict]:
    database_url = load_database_url()
    with psycopg2.connect(database_url) as connection:
        with connection.cursor(cursor_factory=RealDictCursor) as cursor:
            snapshots: list[dict] = []
            for spec in CASE_SPECS:
                left = fetch_side(cursor, spec["left_name"])
                right = fetch_side(cursor, spec["right_name"])
                snapshots.append(
                    {
                        **spec,
                        "left": left,
                        "right": right,
                        "comparison": {
                            "same_country": left["country"] == right["country"],
                            "same_region": left["region"] == right["region"],
                            "same_appellation": left["appellation"] == right["appellation"],
                            "exact_overlap_count": fetch_overlap_count(
                                cursor,
                                spec["left_name"],
                                spec["right_name"],
                            ),
                            "lexical": lexical_summary(spec["left_name"], spec["right_name"]),
                        },
                    }
                )
            return snapshots


def main() -> None:
    payload = {
        "generated_from": "live read-only canonical tables",
        "case_count": len(CASE_SPECS),
        "cases": build_snapshots(),
    }
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_PATH.write_text(json.dumps(payload, ensure_ascii=True, indent=2) + "\n", encoding="utf-8")
    print(str(OUTPUT_PATH))


if __name__ == "__main__":
    main()
