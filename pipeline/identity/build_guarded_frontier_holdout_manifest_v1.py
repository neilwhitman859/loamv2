"""Session 10.8 - build a fresh guarded-frontier holdout manifest.

The broad method bakeoff found a current best candidate on the frozen
`benchmark_v1`, but same-benchmark reruns are not enough to trust it. This
script builds a deterministic confirmation slice from the frozen Sprint 6
Chrome-validated verdict ledger, using only `MERGE` / `SKIP` pairwise cases
that are outside `benchmark_v1`.

Output:
1. JSON manifest shaped like a benchmark file so packet builders can consume it.
2. Markdown summary explaining the selection buckets and chosen cases.

This is a confirmation artifact, not a new production gate. The frozen gate
remains `benchmark_v1`.
"""

from __future__ import annotations

import json
from collections import Counter
from dataclasses import dataclass
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
BENCHMARK_PATH = REPO_ROOT / "data" / "sprints" / "dedup" / "benchmark_v1.json"
VERDICT_LEDGER_PATH = (
    REPO_ROOT / "data" / "sprints" / "dedup" / "execution_bundle" / "verdict_ledger.jsonl"
)
OUTPUT_JSON = (
    REPO_ROOT
    / "data"
    / "sprints"
    / "identity-er"
    / "method_bakeoff"
    / "session10_8_guarded_frontier_holdout_manifest_v1.json"
)
OUTPUT_MD = (
    REPO_ROOT
    / "data"
    / "sprints"
    / "identity-er"
    / "method_bakeoff"
    / "session10_8_guarded_frontier_holdout_manifest_v1.md"
)
SOURCE_ARTIFACT = "data/sprints/dedup/execution_bundle/verdict_ledger.jsonl"
SOURCE_OF_TRUTH = "sprint6_execution_bundle_outside_benchmark"


def canonical_json_dumps(value: object) -> str:
    return json.dumps(value, ensure_ascii=True, indent=2, sort_keys=False) + "\n"


def load_jsonl(path: Path) -> list[dict]:
    return [
        json.loads(line)
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]


def normalize_name(value: str | None) -> str:
    if not value:
        return ""
    return " ".join(value.lower().split())


@dataclass(frozen=True)
class BucketSpec:
    bucket_id: str
    label: str
    verdict: str
    tiers: tuple[str, ...]
    pattern_clusters: tuple[str, ...]
    count: int
    stratum: str
    rationale_note: str
    require_distinct_names: bool = True


SELECTION_BUCKETS: tuple[BucketSpec, ...] = (
    BucketSpec(
        bucket_id="shared_surname_skip_core",
        label="Core shared-surname skip controls",
        verdict="SKIP",
        tiers=("core",),
        pattern_clusters=("11.4.m",),
        count=4,
        stratum="fresh_shared_surname_skip",
        rationale_note="Stress the exact frontier family the guarded method treats as ambiguity-sensitive.",
    ),
    BucketSpec(
        bucket_id="shared_surname_skip_mid",
        label="Mid shared-surname skip controls",
        verdict="SKIP",
        tiers=("mid",),
        pattern_clusters=("11.4.m",),
        count=4,
        stratum="fresh_shared_surname_skip",
        rationale_note="Make sure the frontier discipline is not only surviving on core cases.",
    ),
    BucketSpec(
        bucket_id="related_holdco_skip_core",
        label="Core related / holdco skip controls",
        verdict="SKIP",
        tiers=("core",),
        pattern_clusters=("11.4.g", "11.4.j"),
        count=3,
        stratum="fresh_related_holdco_skip",
        rationale_note="Probe whether the candidate stays conservative on adjacent-brand and holdco traps.",
    ),
    BucketSpec(
        bucket_id="related_holdco_skip_mid",
        label="Mid related / holdco skip controls",
        verdict="SKIP",
        tiers=("mid",),
        pattern_clusters=("11.4.g", "11.4.j"),
        count=3,
        stratum="fresh_related_holdco_skip",
        rationale_note="Extend the same trap family away from the core slice.",
    ),
    BucketSpec(
        bucket_id="alias_merge_mid",
        label="Mid alias / short-full merge recoveries",
        verdict="MERGE",
        tiers=("mid",),
        pattern_clusters=("11.4.h",),
        count=6,
        stratum="fresh_alias_merge",
        rationale_note="Check whether the candidate can still recover genuine merges outside the frozen benchmark.",
    ),
    BucketSpec(
        bucket_id="misc_positive_merge",
        label="Generational and merchant-prefix merge recoveries",
        verdict="MERGE",
        tiers=("mid", "tail"),
        pattern_clusters=("11.4.f", "11.4.p"),
        count=4,
        stratum="fresh_other_merge",
        rationale_note="Add positive shapes that are not just simple short-full aliases.",
    ),
)


def benchmark_pair_ids() -> set[int]:
    payload = json.loads(BENCHMARK_PATH.read_text(encoding="utf-8"))
    return {int(case["pair_id"]) for case in payload["cases"]}


def outside_benchmark_rows() -> list[dict]:
    bench_ids = benchmark_pair_ids()
    rows = []
    for row in load_jsonl(VERDICT_LEDGER_PATH):
        pair_id = row.get("pair_id")
        if pair_id is None:
            continue
        if row.get("final_verdict") not in {"MERGE", "SKIP"}:
            continue
        if int(pair_id) in bench_ids:
            continue
        rows.append(row)
    return rows


def distinct_name_row(row: dict) -> bool:
    if not row.get("name_a") or not row.get("name_b"):
        return False
    return normalize_name(row["name_a"]) != normalize_name(row["name_b"])


def sort_key(row: dict) -> tuple:
    tier_rank = {"core": 0, "mid": 1, "tail": 2}
    return (
        tier_rank.get(str(row.get("tier")), 99),
        str(row.get("pattern_cluster") or ""),
        int(row["pair_id"]),
    )


def select_rows(rows: list[dict]) -> tuple[list[dict], list[dict]]:
    chosen: list[dict] = []
    ledger: list[dict] = []
    seen_pair_ids: set[int] = set()
    for spec in SELECTION_BUCKETS:
        bucket_rows = [
            row
            for row in rows
            if row.get("final_verdict") == spec.verdict
            and str(row.get("tier")) in spec.tiers
            and str(row.get("pattern_cluster") or "") in spec.pattern_clusters
            and int(row["pair_id"]) not in seen_pair_ids
            and (not spec.require_distinct_names or distinct_name_row(row))
        ]
        bucket_rows.sort(key=sort_key)
        picked = bucket_rows[: spec.count]
        if len(picked) != spec.count:
            raise RuntimeError(
                f"Bucket {spec.bucket_id} needed {spec.count} rows but only found {len(picked)}."
            )
        for index, row in enumerate(picked, start=1):
            pair_id = int(row["pair_id"])
            seen_pair_ids.add(pair_id)
            tagged = dict(row)
            tagged["selection_bucket_id"] = spec.bucket_id
            tagged["selection_bucket_label"] = spec.label
            tagged["selection_bucket_index"] = index
            tagged["selection_stratum"] = spec.stratum
            tagged["selection_note"] = spec.rationale_note
            chosen.append(tagged)
        ledger.append(
            {
                "bucket_id": spec.bucket_id,
                "label": spec.label,
                "verdict": spec.verdict,
                "tiers": list(spec.tiers),
                "pattern_clusters": list(spec.pattern_clusters),
                "count": spec.count,
                "pair_ids": [int(row["pair_id"]) for row in picked],
                "note": spec.rationale_note,
            }
        )
    return chosen, ledger


def case_id_for(row: dict) -> str:
    return f"{row['selection_bucket_id']}_{int(row['pair_id'])}"


def final_rationale(row: dict) -> str:
    return (
        str(row.get("override_reasoning") or "").strip()
        or str(row.get("original_reasoning") or "").strip()
    )


def build_manifest(chosen_rows: list[dict], selection_ledger: list[dict]) -> dict:
    cases: list[dict] = []
    for row in chosen_rows:
        cases.append(
            {
                "case_id": case_id_for(row),
                "pair_id": int(row["pair_id"]),
                "producer_name_a": row["name_a"],
                "producer_name_b": row["name_b"],
                "country_a": row.get("country") or "",
                "country_b": row.get("country") or "",
                "stratum": row["selection_stratum"],
                "source_pair_tier": row["tier"],
                "expected_verdict": row["final_verdict"],
                "pattern_cluster": row["pattern_cluster"],
                "historical_failure_mode": None,
                "rationale": final_rationale(row),
                "source_of_truth": SOURCE_OF_TRUTH,
                "source_artifact": SOURCE_ARTIFACT,
                "selection_bucket_id": row["selection_bucket_id"],
                "selection_bucket_label": row["selection_bucket_label"],
                "selection_note": row["selection_note"],
            }
        )

    verdict_counts = Counter(case["expected_verdict"] for case in cases)
    tier_counts = Counter(case["source_pair_tier"] for case in cases)
    stratum_counts = Counter(case["stratum"] for case in cases)
    pattern_counts = Counter(case["pattern_cluster"] for case in cases)

    return {
        "benchmark_id": "producer_dedup_guarded_frontier_holdout_v1",
        "created_on": "2026-04-21",
        "task_scope": "Fresh confirmation slice for Session 10.8 guarded frontier candidate",
        "selection_policy": (
            "Frozen Sprint 6 Chrome-validated MERGE/SKIP ledger rows with real pair_ids, "
            "excluding all benchmark_v1 pair_ids. Deterministic bucket selection by verdict, "
            "tier, pattern cluster, and ascending pair_id."
        ),
        "notes": [
            "This manifest is for confirmation only. The frozen production gate remains benchmark_v1.",
            "The holdout intentionally over-samples shared-surname skip traps and alias-style merge recoveries because those are the current candidate's live risk surfaces.",
            "Counts are bounded to keep the follow-on proof cheap.",
        ],
        "case_count": len(cases),
        "selection_buckets": selection_ledger,
        "summary": {
            "verdict_counts": dict(verdict_counts),
            "tier_counts": dict(tier_counts),
            "stratum_counts": dict(stratum_counts),
            "pattern_cluster_counts": dict(pattern_counts),
        },
        "cases": cases,
    }


def render_markdown(manifest: dict) -> str:
    lines: list[str] = []
    lines.append("# Session 10.8 - guarded frontier holdout manifest")
    lines.append("")
    lines.append(f"- Benchmark id: `{manifest['benchmark_id']}`")
    lines.append(f"- Case count: `{manifest['case_count']}`")
    lines.append("- Purpose: fresh confirmation slice outside `benchmark_v1` for the current guarded-frontier candidate.")
    lines.append("")
    lines.append("## Why This Exists")
    lines.append("")
    lines.append(
        "The guarded frontier candidate already clears the frozen production gate on `benchmark_v1`, but same-benchmark reruns are not enough to trust it. This manifest defines a deterministic outside-benchmark slice from the frozen Sprint 6 execution ledger so the next confirmation run can use fresh labeled pairs without moving the gateposts."
    )
    lines.append("")
    lines.append("## Summary")
    lines.append("")
    summary = manifest["summary"]
    lines.append(
        f"- Verdicts: MERGE `{summary['verdict_counts'].get('MERGE', 0)}`, SKIP `{summary['verdict_counts'].get('SKIP', 0)}`."
    )
    lines.append(
        f"- Tiers: core `{summary['tier_counts'].get('core', 0)}`, mid `{summary['tier_counts'].get('mid', 0)}`, tail `{summary['tier_counts'].get('tail', 0)}`."
    )
    lines.append("")
    lines.append("## Selection Buckets")
    lines.append("")
    lines.append("| Bucket | Verdict | Tiers | Patterns | Count | Pair IDs |")
    lines.append("|---|---|---|---|---:|---|")
    for bucket in manifest["selection_buckets"]:
        lines.append(
            f"| `{bucket['bucket_id']}` | {bucket['verdict']} | {', '.join(bucket['tiers'])} | "
            f"{', '.join(bucket['pattern_clusters'])} | {bucket['count']} | "
            f"{', '.join(str(pair_id) for pair_id in bucket['pair_ids'])} |"
        )
    lines.append("")
    lines.append("## Cases")
    lines.append("")
    lines.append("| Case ID | Pair | Verdict | Tier | Pattern | Names |")
    lines.append("|---|---:|---|---|---|---|")
    for case in manifest["cases"]:
        lines.append(
            f"| `{case['case_id']}` | {case['pair_id']} | {case['expected_verdict']} | {case['source_pair_tier']} | "
            f"`{case['pattern_cluster']}` | {case['producer_name_a']} / {case['producer_name_b']} |"
        )
    return "\n".join(lines).rstrip() + "\n"


def main() -> int:
    rows = outside_benchmark_rows()
    chosen, selection_ledger = select_rows(rows)
    manifest = build_manifest(chosen, selection_ledger)
    OUTPUT_JSON.write_text(canonical_json_dumps(manifest), encoding="utf-8")
    OUTPUT_MD.write_text(render_markdown(manifest), encoding="utf-8")
    print(f"Wrote {OUTPUT_JSON}")
    print(f"Wrote {OUTPUT_MD}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
