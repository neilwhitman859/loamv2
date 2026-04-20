"""B6.6 final verdict ledger builder.

Consolidates three verdict sources into a single frozen ledger:

1. Original Chrome verdicts (yellow, core, mid, tail verdict JSONLs from B6.5a)
2. Manual B6.6 validations (4 pairs: 62908, 54026, 43596, 57771)
3. Subagent B6.6 re-Chrome (Core 66 pairs + Mid/Tail/Yellow 127 pairs)

Also applies canonical-row redirect logic: when the Chrome rename target matches
an existing producer name in the DB, redirect the MERGE survivor to that
existing row (preserves the larger wine portfolio, prevents duplicate creation).

Output: data/sprints/dedup/execution_bundle/verdict_ledger.jsonl
Also writes: data/sprints/dedup/execution_bundle/verdict_ledger_summary.md
"""
from __future__ import annotations

import json
import sys
import unicodedata
from collections import Counter
from pathlib import Path

from pipeline.lib.db import get_conn

REPO = Path(__file__).resolve().parents[1]
CHROME = REPO / "data/sprints/dedup/chrome_validation"
BUNDLE = REPO / "data/sprints/dedup/execution_bundle"
BUNDLE.mkdir(parents=True, exist_ok=True)

LEDGER_OUT = BUNDLE / "verdict_ledger.jsonl"
SUMMARY_OUT = BUNDLE / "verdict_ledger_summary.md"

# Manual B6.6 validations from this session
MANUAL_OVERRIDES = {
    "core#62908": {
        "action": "FLIP_TO_SKIP",
        "verdict_new": "SKIP",
        "reasoning": "Chrome re-validation: 'Beausejour' is an extremely common French château name spanning ≥10 distinct estates (Fronsac, Puisseguin-SE ×2, Montagne-SE, Saint-Estèphe, Pomerol, Chinon, Touraine, Crozes-Hermitage). 'Croix de Beausejour' is Duffau-Lagarrosse's second wine, not Bécot's. The losing row wines span 6+ unrelated estates.",
        "chrome_url": "https://www.wine-searcher.com/find/beausejour",
        "chrome_evidence": "Wine-Searcher lists Ch. Beau-Séjour Bécot + Ch. Beausejour Duffau-Lagarrosse + Ch. Haut-Beausejour + ≥7 other distinct Beausejour estates",
        "sprint7_flag": "beausejour_row_needs_per_wine_split"
    },
    "mid#54026": {
        "action": "FLIP_TO_SKIP",
        "verdict_new": "SKIP",
        "reasoning": "Chrome re-validation: 'Boisson' row is Domaine Boisson Cairanne (Rhône) + Château Boisson (Bordeaux). Anne Boisson is Meursault, Burgundy — one of three Meursault Boisson domaines (§11.4.m sibling split with Pierre Boisson and Boisson-Vadot). Zero overlap between rows.",
        "chrome_url": "https://pleasurewine.com/en/brand/82-domaine-anne-boisson",
        "chrome_evidence": "Anne Boisson daughter of Bernard Boisson-Vadot, 1.5ha of 8.5ha family Meursault estate; sibling Pierre Boisson runs a separate 3.5ha portion."
    },
    "mid#43596": {
        "action": "FLIP_DIRECTION",
        "verdict_new": "MERGE",
        "new_survivor_name": "Comtesse de Cherisey",
        "reasoning": "Chrome re-validation: same estate, flip direction. Jasper Morris confirms 'The domaine formerly known as Martelet de Cherisey is now officially the Domaine Comtesse de Cherisey.' Comtesse is current canonical name (the 9-wine row); Martelet is the historical form. Chrome originally chose Martelet as survivor — backwards.",
        "chrome_url": "https://insideburgundy.com/overview/domaine-comtesse-de-cherisey",
        "chrome_evidence": "'The domaine formerly known as Martelet de Cherisey is now officially the Domaine Comtesse de Cherisey but is subtitled Hélène et Laurent Martelet.'"
    },
    "core#57771": {
        "action": "FLIP_TO_SKIP",
        "verdict_new": "SKIP",
        "reasoning": "Chrome re-validation: 'Alex et Benoit Moreau' is a single-wine cuvée of Domaine Bernard Moreau et Fils (father's Chassagne estate, separate 11w DB row), not a collab producer. Alex and Benoît are sibling winemakers with distinct DB rows (§11.4.m). Making the cuvée a child of Alex alone is factually wrong — it's equally Benoît's, and structurally a wine of Bernard Moreau et Fils per §11.4.e.",
        "chrome_url": "https://www.ploc.co/observintoire/vins/domaine-bernard-moreau-et-fils-fleurie-alex-et-benoit-moreau-2019-rouge-2364d",
        "chrome_evidence": "PLOC lists the Fleurie as 'Domaine Bernard Moreau et Fils Fleurie Alex et Benoit Moreau' — the cuvée name on a Bernard Moreau bottling.",
        "sprint7_flag": "moreau_family_5_row_cleanup"
    },
}


def normalize_name(s: str | None) -> str:
    if not s:
        return ""
    s = unicodedata.normalize("NFKD", s)
    s = "".join(c for c in s if not unicodedata.combining(c))
    return " ".join(s.lower().strip().split())


def load_original_verdicts():
    """Load all 493 B6.5a Chrome-validated verdicts, tagged with ledger_key."""
    verdicts = []
    for fname, tier in (("yellow_verdicts.jsonl", "yellow"), ("core_verdicts.jsonl", "core"),
                        ("mid_verdicts.jsonl", "mid"), ("tail_verdicts.jsonl", "tail")):
        path = CHROME / fname
        with path.open(encoding="utf-8") as f:
            for line in f:
                if not line.strip():
                    continue
                v = json.loads(line)
                v["_tier"] = tier
                if tier == "yellow":
                    v["_ledger_key"] = f"yellow#{v.get('idx')}"
                else:
                    v["_ledger_key"] = f"{tier}#{v.get('pair_id')}"
                verdicts.append(v)
    return verdicts


def load_rechrome_overrides():
    """Load B6.6 subagent re-Chrome overrides. Returns dict of ledger_key → override dict."""
    overrides = {}

    # Core re-Chrome
    core_path = CHROME / "_rechrome_core_verdicts.jsonl"
    if core_path.exists():
        with core_path.open(encoding="utf-8") as f:
            for line in f:
                if not line.strip():
                    continue
                v = json.loads(line)
                pid = v.get("pair_id")
                if pid is not None:
                    overrides[f"core#{pid}"] = v

    # Mid+Tail+Yellow re-Chrome
    rest_path = CHROME / "_rechrome_rest_verdicts.jsonl"
    if rest_path.exists():
        with rest_path.open(encoding="utf-8") as f:
            for line in f:
                if not line.strip():
                    continue
                v = json.loads(line)
                key = v.get("ledger_key")
                if key:
                    overrides[key] = v

    return overrides


def resolve_producer_ids_from_pair(conn, pair_id: int):
    with conn.cursor() as cur:
        cur.execute(
            "SELECT name_a, name_b, producer_id_a::text, producer_id_b::text, country "
            "FROM producer_dedup_pairs WHERE id = %s",
            (pair_id,),
        )
        row = cur.fetchone()
        if not row:
            return None
        return {"name_a": row[0], "name_b": row[1], "pid_a": row[2], "pid_b": row[3], "country": row[4]}


def load_all_producers(conn) -> list[dict]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT id::text, name, country_id::text,
                   (SELECT COUNT(*) FROM wines w WHERE w.producer_id = p.id) AS wines
            FROM producers p WHERE p.deleted_at IS NULL
            """
        )
        return [{"id": r[0], "name": r[1], "country_id": r[2], "wines": r[3]} for r in cur.fetchall()]


def canonical_row_lookup(name: str, country_id: str | None, producers: list[dict],
                         exclude_ids: set[str] | None = None) -> dict | None:
    """Find an existing producer row matching `name` (case-insensitive, accent-folded) in the same country."""
    if not name:
        return None
    t = normalize_name(name)
    exclude_ids = exclude_ids or set()
    matches = []
    for p in producers:
        if p["id"] in exclude_ids:
            continue
        if country_id and p["country_id"] != country_id:
            continue
        if normalize_name(p["name"]) == t:
            matches.append(p)
    if not matches:
        return None
    # Prefer the row with more wines
    matches.sort(key=lambda p: -p["wines"])
    return matches[0]


def build_ledger_entry(v: dict, override: dict | None, conn, all_producers: list[dict]) -> dict:
    """Build a single ledger entry from one original verdict + optional override."""
    ledger_key = v["_ledger_key"]
    tier = v["_tier"]
    pair_id = v.get("pair_id")
    pair_row = resolve_producer_ids_from_pair(conn, pair_id) if pair_id else None

    entry = {
        "ledger_key": ledger_key,
        "tier": tier,
        "pair_id": pair_id,
        "pattern_cluster": v.get("pattern_cluster"),
        "original_verdict": v.get("verdict"),
        "original_survivor_name": v.get("survivor_name"),
        "original_parent_name": v.get("parent_name"),
        "original_reasoning": (v.get("reasoning") or "")[:400],
        "name_a": pair_row.get("name_a") if pair_row else v.get("name"),
        "name_b": pair_row.get("name_b") if pair_row else v.get("sibling_name"),
        "country": pair_row.get("country") if pair_row else v.get("country"),
    }

    # Apply overrides (manual first, then subagent)
    applied_override = None
    if ledger_key in MANUAL_OVERRIDES:
        applied_override = {"source": "manual_b6_6", **MANUAL_OVERRIDES[ledger_key]}
    elif override:
        applied_override = {"source": "subagent_rechrome", **override}

    if applied_override:
        entry["override_source"] = applied_override["source"]
        entry["override_action"] = applied_override.get("action")
        entry["override_reasoning"] = applied_override.get("reasoning", "")[:400]
        entry["override_chrome_url"] = applied_override.get("chrome_url")
        entry["override_chrome_evidence"] = (applied_override.get("chrome_evidence") or "")[:200]
        if applied_override.get("sprint7_flag"):
            entry["sprint7_flag"] = applied_override["sprint7_flag"]

        action = applied_override.get("action", "KEEP")
        if action == "KEEP":
            entry["final_verdict"] = entry["original_verdict"]
            entry["final_survivor_name"] = entry["original_survivor_name"]
            entry["final_parent_name"] = entry["original_parent_name"]
        elif action == "FLIP_TO_SKIP":
            entry["final_verdict"] = "SKIP"
        elif action == "FLIP_DIRECTION":
            entry["final_verdict"] = entry["original_verdict"]
            entry["final_survivor_name"] = applied_override.get("new_survivor_name") or entry["original_survivor_name"]
            entry["final_parent_name"] = applied_override.get("new_parent_name") or entry["original_parent_name"]
        elif action == "FLIP_TO_MERGE":
            entry["final_verdict"] = "MERGE"
            entry["final_survivor_name"] = applied_override.get("new_survivor_name") or entry["original_survivor_name"]
        elif action == "FLIP_TO_PC":
            entry["final_verdict"] = "PARENT_CHILD"
            entry["final_parent_name"] = applied_override.get("new_parent_name") or entry["original_parent_name"]
        elif action == "NEEDS_HUMAN_REVIEW":
            entry["final_verdict"] = "DEFERRED_SPRINT_7"
        else:
            entry["final_verdict"] = entry["original_verdict"]
    else:
        entry["final_verdict"] = entry["original_verdict"]
        entry["final_survivor_name"] = entry["original_survivor_name"]
        entry["final_parent_name"] = entry["original_parent_name"]

    # Resolve producer_ids
    if pair_row:
        entry["producer_id_a"] = pair_row["pid_a"]
        entry["producer_id_b"] = pair_row["pid_b"]
    else:
        # Yellow tier: pick the counterpart based on verdict type
        pid = v.get("producer_id")
        pid = pid if str(pid or "").count("-") == 4 else None
        entry["producer_id_a"] = pid
        if v.get("verdict") == "PARENT_CHILD":
            # Child = sibling_id (different from yellow subject's producer_id which equals parent_id)
            entry["producer_id_b"] = v.get("sibling_id") or v.get("sibling_producer_id")
        else:
            # MERGE: target or source
            entry["producer_id_b"] = v.get("merge_target_id") or v.get("merge_source_id") or v.get("sibling_id")
        # Fallback: for yellow PC where producer_id is non-UUID (e.g. yellow#3 "dalla_valle"),
        # look up by name in the producer set.
        if not entry["producer_id_a"] and v.get("name"):
            m = canonical_row_lookup(v["name"], None, all_producers)
            if m:
                entry["producer_id_a"] = m["id"]
        if not entry["producer_id_b"] and v.get("sibling_name"):
            exclude = {entry["producer_id_a"]} if entry["producer_id_a"] else set()
            m = canonical_row_lookup(v["sibling_name"], None, all_producers, exclude_ids=exclude)
            if m:
                entry["producer_id_b"] = m["id"]

    # Canonical row lookup for MERGE/PARENT_CHILD
    final_verdict = entry.get("final_verdict")
    canonical_target_name = None
    if final_verdict == "MERGE":
        canonical_target_name = entry.get("final_survivor_name")
    elif final_verdict == "PARENT_CHILD":
        canonical_target_name = entry.get("final_parent_name")

    if canonical_target_name and pair_row:
        pid_a, pid_b = pair_row["pid_a"], pair_row["pid_b"]
        exclude = {pid_a, pid_b} if pid_a and pid_b else set()
        # Look up country_id from one of the pair producers
        country_id = None
        for p in all_producers:
            if p["id"] in (pid_a, pid_b):
                country_id = p["country_id"]
                break
        canonical = canonical_row_lookup(canonical_target_name, country_id, all_producers, exclude_ids=exclude)
        if canonical:
            entry["canonical_redirect_id"] = canonical["id"]
            entry["canonical_redirect_name"] = canonical["name"]
            entry["canonical_redirect_wines"] = canonical["wines"]

    # Yellow: carry unflagged_additional_merges
    if tier == "yellow" and v.get("unflagged_additional_merges"):
        entry["yellow_additional_merges"] = v["unflagged_additional_merges"]

    return entry


def write_summary(entries: list[dict]):
    by_final = Counter(e["final_verdict"] for e in entries)
    by_tier = Counter((e["tier"], e["final_verdict"]) for e in entries)
    overridden = sum(1 for e in entries if e.get("override_source"))
    redirected = sum(1 for e in entries if e.get("canonical_redirect_id"))
    sprint7 = sum(1 for e in entries if e.get("sprint7_flag") or e.get("final_verdict") == "DEFERRED_SPRINT_7")

    lines = []
    lines.append("# B6.6 Final Verdict Ledger — Summary")
    lines.append("")
    lines.append(f"Total verdicts: **{len(entries)}**")
    lines.append("")
    lines.append("## Final verdict distribution")
    lines.append("")
    for v in ("MERGE", "PARENT_CHILD", "SKIP", "KEEP_AS_IS", "DEFERRED_SPRINT_7"):
        lines.append(f"- {v}: **{by_final.get(v, 0)}**")
    lines.append("")
    lines.append(f"## Overrides applied: {overridden}")
    lines.append(f"## Canonical-row redirects: {redirected}")
    lines.append(f"## Deferred to Sprint 7: {sprint7}")
    lines.append("")
    lines.append("## Per-tier breakdown")
    lines.append("")
    lines.append("| Tier | MERGE | PC | SKIP | KEEP_AS_IS | DEFERRED | Total |")
    lines.append("|---|---|---|---|---|---|---|")
    for tier in ("yellow", "core", "mid", "tail"):
        counts = {v: 0 for v in ("MERGE", "PARENT_CHILD", "SKIP", "KEEP_AS_IS", "DEFERRED_SPRINT_7")}
        for (t, v), n in by_tier.items():
            if t == tier:
                counts[v] = counts.get(v, 0) + n
        total = sum(counts.values())
        lines.append(f"| {tier} | {counts['MERGE']} | {counts['PARENT_CHILD']} | {counts['SKIP']} | {counts['KEEP_AS_IS']} | {counts['DEFERRED_SPRINT_7']} | {total} |")
    lines.append("")

    SUMMARY_OUT.write_text("\n".join(lines), encoding="utf-8")
    print(f"Summary written: {SUMMARY_OUT}")


def main():
    originals = load_original_verdicts()
    overrides = load_rechrome_overrides()
    print(f"Loaded {len(originals)} original verdicts and {len(overrides)} re-Chrome overrides")
    print(f"Manual B6.6 overrides: {len(MANUAL_OVERRIDES)}")

    conn = get_conn()
    try:
        all_producers = load_all_producers(conn)
        print(f"Loaded {len(all_producers)} active producers for canonical-row lookup")

        entries = []
        for v in originals:
            key = v["_ledger_key"]
            override = overrides.get(key)
            entry = build_ledger_entry(v, override, conn, all_producers)
            entries.append(entry)
    finally:
        conn.close()

    # Write ledger
    with LEDGER_OUT.open("w", encoding="utf-8") as f:
        for e in entries:
            f.write(json.dumps(e, ensure_ascii=False) + "\n")
    print(f"Ledger written: {LEDGER_OUT}  ({len(entries)} entries)")

    write_summary(entries)


if __name__ == "__main__":
    sys.exit(main() or 0)
