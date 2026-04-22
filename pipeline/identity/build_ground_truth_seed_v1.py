"""
Build Sprint 7 ground-truth seed artifacts from the validated Sprint 6 ledgers.

Outputs:
1. pairwise seed records that still map to the live `producer_dedup_pairs` table,
2. singleton sanity records that are useful context but do not count toward the
   scoreable pair target, and
3. a markdown summary with current counts and gap-to-target math.
"""

from __future__ import annotations

import json
from collections import Counter
from pathlib import Path
from typing import Any

from pipeline.lib.db import get_conn


REPO_ROOT = Path(__file__).resolve().parents[2]
LEDGER_PATH = (
    REPO_ROOT
    / "data"
    / "sprints"
    / "dedup"
    / "execution_bundle"
    / "verdict_ledger.jsonl"
)
BENCHMARK_PATH = REPO_ROOT / "data" / "sprints" / "dedup" / "benchmark_v1.json"
OUTPUT_DIR = REPO_ROOT / "data" / "sprints" / "identity-er"
PAIRS_OUTPUT_PATH = OUTPUT_DIR / "ground_truth_seed_pairs_v1.jsonl"
SINGLETONS_OUTPUT_PATH = OUTPUT_DIR / "ground_truth_seed_singletons_v1.jsonl"
SUMMARY_OUTPUT_PATH = OUTPUT_DIR / "ground_truth_seed_summary_v1.md"

SEED_VERSION = "ground_truth_seed_v1"
SCOREABLE_LABELS = {"SAME_AS", "RELATED_BUT_DISTINCT", "NONE"}
LABEL_MAP = {
    "MERGE": "SAME_AS",
    "PARENT_CHILD": "RELATED_BUT_DISTINCT",
    "SKIP": "NONE",
    "DEFERRED_SPRINT_7": "DEFERRED",
    "KEEP_AS_IS": "KEEP_AS_IS",
}

TARGET_LABEL_COUNTS = {
    "SAME_AS": 300,
    "RELATED_BUT_DISTINCT": 200,
    "NONE": 500,
}


def read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


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


def normalize_truth_label(value: str | None) -> str | None:
    if value is None:
        return None
    return LABEL_MAP.get(value, value)


def load_benchmark_by_pair_id() -> dict[int, dict[str, Any]]:
    raw = read_json(BENCHMARK_PATH)
    cases = raw.get("cases") if isinstance(raw, dict) else raw
    if cases is None:
        cases = []
    benchmark_by_pair_id: dict[int, dict[str, Any]] = {}
    for case in cases:
        pair_id = case.get("pair_id")
        if pair_id is None:
            continue
        benchmark_by_pair_id[int(pair_id)] = case
    return benchmark_by_pair_id


def fetch_pair_snapshots(pair_ids: list[int]) -> dict[int, dict[str, Any]]:
    if not pair_ids:
        return {}
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
            verdict,
            method_name,
            confidence,
            signals
        from producer_dedup_pairs
        where id = any(%s)
        """,
        (pair_ids,),
    )
    rows = cur.fetchall()
    cur.close()
    conn.close()

    snapshot_by_id: dict[int, dict[str, Any]] = {}
    for (
        pair_id,
        name_a,
        name_b,
        country,
        similarity,
        wines_a,
        wines_b,
        verdict,
        method_name,
        confidence,
        signals,
    ) in rows:
        snapshot_by_id[int(pair_id)] = {
            "pair_id": int(pair_id),
            "name_a": name_a,
            "name_b": name_b,
            "country": country,
            "similarity": float(similarity) if similarity is not None else None,
            "wines_a": wines_a,
            "wines_b": wines_b,
            "verdict": verdict,
            "method_name": method_name,
            "confidence": float(confidence) if confidence is not None else None,
            "signal_keys": sorted((signals or {}).keys()),
        }
    return snapshot_by_id


def fetch_live_counts() -> dict[str, int]:
    queries = {
        "producers": "select count(*) from producers",
        "wines": "select count(*) from wines",
        "wine_vintages": "select count(*) from wine_vintages",
        "producer_dedup_pairs": "select count(*) from producer_dedup_pairs",
        "unlabeled_producer_dedup_pairs": (
            "select count(*) from producer_dedup_pairs where verdict is null"
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


def build_pair_record(
    ledger_row: dict[str, Any],
    benchmark_case: dict[str, Any] | None,
    snapshot: dict[str, Any] | None,
) -> dict[str, Any]:
    final_verdict = ledger_row.get("final_verdict")
    normalized_truth_label = normalize_truth_label(final_verdict)
    pair_id = int(ledger_row["pair_id"])
    return {
        "seed_version": SEED_VERSION,
        "record_kind": "pair",
        "seed_key": f"pair:{pair_id}",
        "pair_id": pair_id,
        "ledger_key": ledger_row.get("ledger_key"),
        "producer_id_a": ledger_row.get("producer_id_a"),
        "producer_id_b": ledger_row.get("producer_id_b"),
        "name_a": ledger_row.get("name_a"),
        "name_b": ledger_row.get("name_b"),
        "country": ledger_row.get("country"),
        "tier": ledger_row.get("tier"),
        "pattern_cluster": ledger_row.get("pattern_cluster"),
        "source_artifact": "data/sprints/dedup/execution_bundle/verdict_ledger.jsonl",
        "source_truth_label": final_verdict,
        "normalized_truth_label": normalized_truth_label,
        "scoreable_pair": normalized_truth_label in SCOREABLE_LABELS,
        "source_reasoning": ledger_row.get("override_reasoning")
        or ledger_row.get("original_reasoning"),
        "benchmark_v1_member": benchmark_case is not None,
        "benchmark_v1_case_id": benchmark_case.get("case_id") if benchmark_case else None,
        "benchmark_v1_expected_verdict": (
            benchmark_case.get("expected_verdict") if benchmark_case else None
        ),
        "benchmark_v1_stratum": benchmark_case.get("stratum") if benchmark_case else None,
        "current_pair_exists": snapshot is not None,
        "current_pair_similarity": (
            round(snapshot["similarity"], 4)
            if snapshot and snapshot["similarity"] is not None
            else None
        ),
        "current_pair_wines_a": snapshot.get("wines_a") if snapshot else None,
        "current_pair_wines_b": snapshot.get("wines_b") if snapshot else None,
        "current_pair_verdict": snapshot.get("verdict") if snapshot else None,
        "current_pair_method_name": snapshot.get("method_name") if snapshot else None,
        "current_pair_confidence": (
            round(snapshot["confidence"], 4)
            if snapshot and snapshot["confidence"] is not None
            else None
        ),
        "current_pair_signal_keys": snapshot.get("signal_keys") if snapshot else [],
    }


def build_singleton_record(ledger_row: dict[str, Any]) -> dict[str, Any]:
    final_verdict = ledger_row.get("final_verdict")
    normalized_truth_label = normalize_truth_label(final_verdict)
    return {
        "seed_version": SEED_VERSION,
        "record_kind": "singleton",
        "seed_key": ledger_row.get("ledger_key"),
        "pair_id": None,
        "ledger_key": ledger_row.get("ledger_key"),
        "producer_id_a": ledger_row.get("producer_id_a"),
        "producer_id_b": ledger_row.get("producer_id_b"),
        "name_a": ledger_row.get("name_a"),
        "name_b": ledger_row.get("name_b"),
        "country": ledger_row.get("country"),
        "tier": ledger_row.get("tier"),
        "pattern_cluster": ledger_row.get("pattern_cluster"),
        "source_artifact": "data/sprints/dedup/execution_bundle/verdict_ledger.jsonl",
        "source_truth_label": final_verdict,
        "normalized_truth_label": normalized_truth_label,
        "scoreable_pair": False,
        "source_reasoning": ledger_row.get("override_reasoning")
        or ledger_row.get("original_reasoning"),
    }


def render_summary(
    pair_rows: list[dict[str, Any]],
    singleton_rows: list[dict[str, Any]],
    live_counts: dict[str, int],
) -> str:
    pair_label_counts = Counter(row["normalized_truth_label"] for row in pair_rows)
    scoreable_label_counts = Counter(
        row["normalized_truth_label"] for row in pair_rows if row["scoreable_pair"]
    )
    tier_counts = Counter(row["tier"] for row in pair_rows)
    country_counts = Counter(row["country"] for row in pair_rows)
    benchmark_overlap = sum(1 for row in pair_rows if row["benchmark_v1_member"])
    scoreable_pair_count = sum(1 for row in pair_rows if row["scoreable_pair"])
    scoreable_gap = 1000 - scoreable_pair_count

    label_gap_lines: list[str] = []
    for label, target in TARGET_LABEL_COUNTS.items():
        current = scoreable_label_counts.get(label, 0)
        gap = target - current
        label_gap_lines.append(f"- `{label}`: {current} current, need `+{gap}` to reach `{target}`")

    top_country_lines = [
        f"- `{country}`: `{count}`"
        for country, count in country_counts.most_common(10)
    ]
    tier_lines = [f"- `{tier}`: `{count}`" for tier, count in sorted(tier_counts.items())]

    return "\n".join(
        [
            f"# {SEED_VERSION} summary",
            "",
            "Generated from the validated Sprint 6 execution ledger plus the frozen `benchmark_v1` subset.",
            "",
            "## Live corpus snapshot",
            "",
            f"- producers: `{live_counts['producers']}`",
            f"- wines: `{live_counts['wines']}`",
            f"- wine_vintages: `{live_counts['wine_vintages']}`",
            f"- producer_dedup_pairs: `{live_counts['producer_dedup_pairs']}`",
            f"- unlabeled producer_dedup_pairs: `{live_counts['unlabeled_producer_dedup_pairs']}`",
            "",
            "## Seed inventory",
            "",
            f"- pair records: `{len(pair_rows)}`",
            f"- scoreable pair records: `{scoreable_pair_count}`",
            f"- singleton sanity records: `{len(singleton_rows)}`",
            f"- frozen benchmark overlap: `{benchmark_overlap}`",
            "",
            "## Pair label mix",
            "",
            *[f"- `{label}`: `{count}`" for label, count in sorted(pair_label_counts.items())],
            "",
            "## Scoreable gap to 1,000 pairs",
            "",
            f"- total scoreable gap: `+{scoreable_gap}`",
            *label_gap_lines,
            "",
            "## Tier mix",
            "",
            *tier_lines,
            "",
            "## Top countries in current pair seed",
            "",
            *top_country_lines,
            "",
            "## Notes",
            "",
            "- `DEFERRED` records stay in the ledger but do not count toward the scoreable target.",
            "- Singleton `KEEP_AS_IS` records are useful sanity checks for producer-card correctness, not pairwise scoring.",
            "- The live `producer_dedup_pairs` table is not treated as truth because `verdict_source` is still blank on all rows.",
        ]
    )


def main() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    ledger_rows = read_jsonl(LEDGER_PATH)
    benchmark_by_pair_id = load_benchmark_by_pair_id()

    pair_ids = sorted(
        int(row["pair_id"]) for row in ledger_rows if row.get("pair_id") is not None
    )
    pair_snapshots = fetch_pair_snapshots(pair_ids)

    pair_rows: list[dict[str, Any]] = []
    singleton_rows: list[dict[str, Any]] = []

    for ledger_row in ledger_rows:
        pair_id = ledger_row.get("pair_id")
        if pair_id is None:
            singleton_rows.append(build_singleton_record(ledger_row))
            continue
        benchmark_case = benchmark_by_pair_id.get(int(pair_id))
        snapshot = pair_snapshots.get(int(pair_id))
        pair_rows.append(build_pair_record(ledger_row, benchmark_case, snapshot))

    live_counts = fetch_live_counts()

    write_jsonl(PAIRS_OUTPUT_PATH, pair_rows)
    write_jsonl(SINGLETONS_OUTPUT_PATH, singleton_rows)
    SUMMARY_OUTPUT_PATH.write_text(
        render_summary(pair_rows, singleton_rows, live_counts) + "\n",
        encoding="utf-8",
    )

    print(f"Wrote {len(pair_rows)} pair rows to {PAIRS_OUTPUT_PATH}")
    print(f"Wrote {len(singleton_rows)} singleton rows to {SINGLETONS_OUTPUT_PATH}")
    print(f"Wrote summary to {SUMMARY_OUTPUT_PATH}")


if __name__ == "__main__":
    main()
