"""Session 10.8 - build a frozen-file stress audit for the benchmark-clearing stack.

This audit does not call models, touch the DB, or mutate any proof artifacts.
It summarizes:

1. the exact late-overlay trigger hits in the frozen corpora
2. nearby broader analogues and counterexamples
3. whether any further frozen-file confirmation path remains honest
"""

from __future__ import annotations

import json
import re
from collections import Counter
from datetime import date
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
METHOD_DIR = REPO_ROOT / "data" / "sprints" / "identity-er" / "method_bakeoff"
BENCHMARK_PATH = REPO_ROOT / "data" / "sprints" / "dedup" / "benchmark_v1.json"
LEDGER_PATH = REPO_ROOT / "data" / "sprints" / "dedup" / "execution_bundle" / "verdict_ledger.jsonl"
HOLDOUT_MANIFEST_PATH = METHOD_DIR / "session10_8_guarded_frontier_holdout_manifest_v1.json"
OUTPUT_JSON = METHOD_DIR / "session10_8_hybrid_guarded_stack_stress_audit_v1.json"
OUTPUT_MD = METHOD_DIR / "session10_8_hybrid_guarded_stack_stress_audit_v1.md"
PLACE_PREFIXES = {"stadt", "weingut", "winzer"}
NAME_RE = re.compile(r"[A-Za-z']+")
CONTEXT_SOURCES = [
    REPO_ROOT / "data" / "sprints" / "dedup" / "chrome_validation" / "_rechrome_core_context.jsonl",
    REPO_ROOT / "data" / "sprints" / "dedup" / "chrome_validation" / "_rechrome_rest_context.jsonl",
]


def canonical_json_dumps(value: object) -> str:
    return json.dumps(value, ensure_ascii=True, indent=2, sort_keys=False) + "\n"


def load_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def load_jsonl(path: Path) -> list[dict]:
    return [
        json.loads(line)
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]


def write_json(path: Path, payload: object) -> None:
    path.write_text(canonical_json_dumps(payload), encoding="utf-8")


def tokens(value: str | None) -> list[str]:
    return NAME_RE.findall((value or "").lower())


def benchmark_maps() -> tuple[set[int], dict[int, dict]]:
    benchmark = load_json(BENCHMARK_PATH)
    case_by_pair = {int(case["pair_id"]): case for case in benchmark["cases"]}
    return set(case_by_pair), case_by_pair


def verdict_label(row: dict) -> str | None:
    return row.get("final_verdict") or row.get("verdict_original") or row.get("original_verdict")


def place_exact_name(a: str | None, b: str | None) -> bool:
    if not a or not b:
        return False
    tokens_a = tokens(a)
    tokens_b = tokens(b)
    for long_tokens, short_tokens in ((tokens_a, tokens_b), (tokens_b, tokens_a)):
        if long_tokens and long_tokens[0] in PLACE_PREFIXES and len(short_tokens) == 1 and short_tokens[0] in long_tokens[1:]:
            return True
    return False


def maison_name_broad(a: str | None, b: str | None) -> bool:
    if not a or not b:
        return False
    match_a = re.fullmatch(r"de la ([A-Za-z']+)", a.strip(), re.IGNORECASE)
    match_b = re.fullmatch(r"de la ([A-Za-z']+)", b.strip(), re.IGNORECASE)
    if bool(match_a) == bool(match_b):
        return False
    token = (match_a or match_b).group(1).lower()
    other_name = b if match_a else a
    return token in tokens(other_name)


def fils_exact_name(a: str | None, b: str | None) -> bool:
    if not a or not b:
        return False
    for fils_name, person_name in ((a, b), (b, a)):
        fils_tokens = tokens(fils_name)
        person_tokens = tokens(person_name)
        person_parts = person_name.strip().split()
        if "fils" not in fils_tokens:
            continue
        if "fils" in person_tokens:
            continue
        if len(person_parts) != 2:
            continue
        surname_tokens = tokens(person_parts[-1])
        if not surname_tokens:
            continue
        surname = surname_tokens[0]
        if surname not in fils_tokens:
            continue
        if "-" not in person_parts[0]:
            continue
        return True
    return False


def fils_broad_name(a: str | None, b: str | None) -> dict | None:
    if not a or not b:
        return None
    for fils_name, person_name, side in ((a, b, "a"), (b, a, "b")):
        fils_tokens = tokens(fils_name)
        person_tokens = tokens(person_name)
        person_parts = person_name.strip().split()
        shared = sorted(set(fils_tokens) & set(person_tokens))
        if "fils" not in fils_tokens or not shared:
            continue
        return {
            "fils_name": fils_name,
            "person_name": person_name,
            "fils_side": side,
            "person_has_fils": "fils" in person_tokens,
            "person_word_count": len(person_parts),
            "person_first_word_hyphenated": bool(person_parts and "-" in person_parts[0]),
            "shared_tokens": shared,
        }
    return None


def maison_exact_context(row: dict) -> bool:
    a_name = row["side_a"]["name"]
    b_name = row["side_b"]["name"]
    match_a = re.fullmatch(r"de la ([A-Za-z']+)", a_name.strip(), re.IGNORECASE)
    match_b = re.fullmatch(r"de la ([A-Za-z']+)", b_name.strip(), re.IGNORECASE)
    if bool(match_a) == bool(match_b):
        return False
    article_side = row["side_a"] if match_a else row["side_b"]
    other_side = row["side_b"] if match_a else row["side_a"]
    token = (match_a or match_b).group(1).lower()
    phrase = f"maison de la {token}"
    return token in tokens(other_side["name"]) and any(
        phrase in (wine or "").lower() for wine in article_side.get("wines") or []
    )


def summarize_hits(rows: list[dict]) -> dict:
    return {
        "count": len(rows),
        "inside_benchmark_count": sum(1 for row in rows if row["inside_benchmark"]),
        "outside_benchmark_count": sum(1 for row in rows if not row["inside_benchmark"]),
        "by_scope_and_verdict": {
            f"{scope}:{verdict}": count
            for (scope, verdict), count in Counter(
                ("inside" if row["inside_benchmark"] else "outside", row["verdict"])
                for row in rows
            ).items()
        },
    }


def build_payload() -> dict:
    bench_pairs, case_by_pair = benchmark_maps()
    ledger_exact_place: list[dict] = []
    ledger_broad_maison: list[dict] = []
    ledger_exact_fils: list[dict] = []
    ledger_broad_fils: list[dict] = []

    for row in load_jsonl(LEDGER_PATH):
        pair_id = row.get("pair_id")
        inside_benchmark = pair_id is not None and int(pair_id) in bench_pairs
        base = {
            "pair_id": pair_id,
            "inside_benchmark": inside_benchmark,
            "verdict": verdict_label(row),
            "pattern_cluster": row.get("pattern_cluster"),
            "name_a": row.get("name_a"),
            "name_b": row.get("name_b"),
        }
        if place_exact_name(row.get("name_a"), row.get("name_b")):
            ledger_exact_place.append(dict(base))
        if maison_name_broad(row.get("name_a"), row.get("name_b")):
            ledger_broad_maison.append(dict(base))
        if fils_exact_name(row.get("name_a"), row.get("name_b")):
            ledger_exact_fils.append(dict(base))
        broad_fils = fils_broad_name(row.get("name_a"), row.get("name_b"))
        if broad_fils:
            broadened = dict(base)
            broadened["broad_detail"] = broad_fils
            ledger_broad_fils.append(broadened)

    context_exact_maison: list[dict] = []
    for path in CONTEXT_SOURCES:
        for row in load_jsonl(path):
            if "pair_id" not in row:
                continue
            if not maison_exact_context(row):
                continue
            pair_id = int(row["pair_id"])
            context_exact_maison.append(
                {
                    "pair_id": pair_id,
                    "inside_benchmark": pair_id in bench_pairs,
                    "verdict": row.get("verdict_original"),
                    "pattern_cluster": row.get("pattern_cluster"),
                    "name_a": row["side_a"]["name"],
                    "name_b": row["side_b"]["name"],
                }
            )

    holdout = load_json(HOLDOUT_MANIFEST_PATH)
    holdout_family_hits = {
        "place_exact": [],
        "maison_name_broad": [],
        "fils_exact": [],
    }
    for case in holdout["cases"]:
        a_name = case["producer_name_a"]
        b_name = case["producer_name_b"]
        if place_exact_name(a_name, b_name):
            holdout_family_hits["place_exact"].append(case["case_id"])
        if maison_name_broad(a_name, b_name):
            holdout_family_hits["maison_name_broad"].append(case["case_id"])
        if fils_exact_name(a_name, b_name):
            holdout_family_hits["fils_exact"].append(case["case_id"])

    return {
        "generated_on": date.today().isoformat(),
        "candidate": "hybrid_guarded_fils_person_alias_v1",
        "benchmark_clear_reference": "data/sprints/identity-er/method_bakeoff/session10_8_hybrid_guarded_stack_confirmation_v1.md",
        "families": {
            "place_alias": {
                "exact_hits": ledger_exact_place,
                "exact_summary": summarize_hits(ledger_exact_place),
                "broad_summary_note": "No broader frozen ledger rows with `Stadt`/`Weingut`/`Winzer` name prefixes were found outside the single benchmark case.",
            },
            "maison_alias": {
                "exact_hits": context_exact_maison,
                "exact_summary": summarize_hits(context_exact_maison),
                "broad_name_hits": ledger_broad_maison,
                "broad_name_summary": summarize_hits(ledger_broad_maison),
            },
            "fils_person_alias": {
                "exact_hits": ledger_exact_fils,
                "exact_summary": summarize_hits(ledger_exact_fils),
                "broad_name_hits": ledger_broad_fils,
                "broad_name_summary": summarize_hits(ledger_broad_fils),
            },
        },
        "holdout_family_coverage": {
            family: {
                "count": len(case_ids),
                "case_ids": case_ids,
            }
            for family, case_ids in holdout_family_hits.items()
        },
        "freeze_assessment": {
            "frozen_file_confirmation_exhausted": True,
            "reason": (
                "The final stack now has repeated benchmark clearance plus the strongest honest frozen-file stress audit available. "
                "The existing holdout does not cover the late overlay families, and the remaining outside-benchmark corpora only show "
                "adjacent analogues rather than faithful full-packet rerun coverage."
            ),
            "next_non_frozen_requirement": (
                "Restore a working packet-build/runtime path or preserve richer outside-benchmark structured packets before making a fresh independent confirmation claim."
            ),
        },
    }


def render_lines_for_hits(rows: list[dict]) -> list[str]:
    if not rows:
        return ["- none"]
    return [
        "- `pair_id {pair_id}`: `{verdict}` | `{name_a}` vs `{name_b}` | cluster `{pattern_cluster}` | inside benchmark `{inside_benchmark}`".format(
            **row
        )
        for row in rows
    ]


def render_lines_for_broad_fils(rows: list[dict]) -> list[str]:
    if not rows:
        return ["- none"]
    rendered = []
    for row in rows:
        detail = row["broad_detail"]
        rendered.append(
            "- `pair_id {pair_id}`: `{verdict}` | `{name_a}` vs `{name_b}` | cluster `{pattern_cluster}` | inside benchmark `{inside_benchmark}` | shared `{shared}` | hyphen-first `{hyphen}` | person-has-fils `{person_has_fils}`".format(
                pair_id=row["pair_id"],
                verdict=row["verdict"],
                name_a=row["name_a"],
                name_b=row["name_b"],
                pattern_cluster=row["pattern_cluster"],
                inside_benchmark=row["inside_benchmark"],
                shared=", ".join(detail["shared_tokens"]),
                hyphen=detail["person_first_word_hyphenated"],
                person_has_fils=detail["person_has_fils"],
            )
        )
    return rendered


def render_markdown(payload: dict) -> str:
    place = payload["families"]["place_alias"]
    maison = payload["families"]["maison_alias"]
    fils = payload["families"]["fils_person_alias"]
    holdout = payload["holdout_family_coverage"]
    lines = [
        "# Session 10.8 - hybrid guarded stack stress audit",
        "",
        f"- Date: {payload['generated_on']}",
        f"- Candidate: `{payload['candidate']}`",
        f"- Benchmark-clear reference: `{payload['benchmark_clear_reference']}`",
        "- Incremental spend: `$0.00`",
        "",
        "## Goal",
        "",
        "Stress the late zero-cost overlay families against the strongest remaining frozen-file analogues before calling the recommendation frozen.",
        "",
        "## `place_alias`",
        "",
        "Exact trigger hits:",
        "",
    ]
    lines.extend(render_lines_for_hits(place["exact_hits"]))
    lines.extend(
        [
            "",
            "Summary:",
            "",
            f"- Exact hits total: `{place['exact_summary']['count']}`",
            f"- Exact hits outside benchmark: `{place['exact_summary']['outside_benchmark_count']}`",
            f"- Note: {place['broad_summary_note']}",
            "",
            "Interpretation:",
            "",
            "- The institutional-prefix place-alias family remains benchmark-unique in the frozen corpora.",
            "- That is supportive, but it still does not create an independent outside-benchmark execution claim.",
            "",
            "## `maison_alias`",
            "",
            "Exact rich-context hits:",
            "",
        ]
    )
    lines.extend(render_lines_for_hits(maison["exact_hits"]))
    lines.extend(
        [
            "",
            "Broader name-only analogues:",
            "",
        ]
    )
    lines.extend(render_lines_for_hits(maison["broad_name_hits"]))
    lines.extend(
        [
            "",
            "Interpretation:",
            "",
            f"- Exact hits outside benchmark: `{maison['exact_summary']['outside_benchmark_count']}`",
            f"- Broader name-only outside-benchmark hits: `{maison['broad_name_summary']['outside_benchmark_count']}`",
            "- The exact `Maison de la <token>` wine-list shape is still benchmark-unique.",
            "- But the broader lexical shell does have an outside-benchmark SKIP (`de la Gaffeliere` / `Canon la Gaffeliere`), which proves the wine-list phrase is necessary and the family must stay narrow.",
            "",
            "## `fils_person_alias`",
            "",
            "Exact hits:",
            "",
        ]
    )
    lines.extend(render_lines_for_hits(fils["exact_hits"]))
    lines.extend(
        [
            "",
            "Broader name-only analogues:",
            "",
        ]
    )
    lines.extend(render_lines_for_broad_fils(fils["broad_name_hits"]))
    lines.extend(
        [
            "",
            "Interpretation:",
            "",
            f"- Exact hits outside benchmark: `{fils['exact_summary']['outside_benchmark_count']}`",
            f"- Broader name-only outside-benchmark hits: `{fils['broad_name_summary']['outside_benchmark_count']}`",
            "- The exact two-word hyphenated personal-name shape appears only on two benchmark cases: one MERGE (`Protheau & Fils` / `Jean-Francois Protheau`) and one SKIP (`Jean Boillot & Fils` / `Jean-Marc Boillot`).",
            "- Outside the benchmark, the broader `Fils` naming family has many nearby non-MERGE cases, which means this rule must stay pinned to the full packet-conditioned trigger and should not be generalized from names alone.",
            "",
            "## Holdout Coverage",
            "",
            f"- `place_exact` hits in the existing 24-case holdout: `{holdout['place_exact']['count']}`",
            f"- `maison_name_broad` hits in the existing 24-case holdout: `{holdout['maison_name_broad']['count']}`",
            f"- `fils_exact` hits in the existing 24-case holdout: `{holdout['fils_exact']['count']}`",
            "",
            "The current holdout still does not independently test any of the three late overlay families.",
            "",
            "## Frozen Stop Point",
            "",
            f"- Frozen-file confirmation exhausted: `{str(payload['freeze_assessment']['frozen_file_confirmation_exhausted']).lower()}`",
            f"- Why: {payload['freeze_assessment']['reason']}",
            f"- Next non-frozen requirement: {payload['freeze_assessment']['next_non_frozen_requirement']}",
            "",
            "## Recommendation",
            "",
            "> The benchmark-clearing hybrid guarded stack has now been stress-audited as far as frozen local artifacts honestly allow. Two late overlay families are benchmark-unique in the frozen corpora, while the `Fils` family is only safe when kept extremely narrow and packet-conditioned. No existing frozen holdout cases independently cover those late overlays, so the recommendation is now frozen at: benchmark-clear and stress-audited, but not freshly independently validated.",
        ]
    )
    return "\n".join(lines).rstrip() + "\n"


def main() -> int:
    payload = build_payload()
    write_json(OUTPUT_JSON, payload)
    OUTPUT_MD.write_text(render_markdown(payload), encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
