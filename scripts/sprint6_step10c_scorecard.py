"""
Sprint 6 Step 10c — pre-execution scorecard.

Reads the four Chrome-validated verdict JSONL files, resolves each pair to
canonical producer_id_a/b via `producer_dedup_pairs`, determines survivor /
parent / child producer IDs, and writes a human-readable scorecard to:

    data/sprints/dedup/chrome_validation/step10c_pre_scorecard.md

Does NOT mutate the DB. User signs off on the scorecard before execution.

Usage:
    python scripts/sprint6_step10c_scorecard.py
"""

from __future__ import annotations

import json
import sys
import unicodedata
from collections import Counter, defaultdict
from pathlib import Path

from pipeline.lib.db import get_conn

REPO_ROOT = Path(__file__).resolve().parents[1]
VERDICT_DIR = REPO_ROOT / "data/sprints/dedup/chrome_validation"
OUT_PATH = VERDICT_DIR / "step10c_pre_scorecard.md"

VERDICT_FILES = [
    ("yellow", VERDICT_DIR / "yellow_verdicts.jsonl"),
    ("core", VERDICT_DIR / "core_verdicts.jsonl"),
    ("mid", VERDICT_DIR / "mid_verdicts.jsonl"),
    ("tail", VERDICT_DIR / "tail_verdicts.jsonl"),
]


def load_verdicts() -> list[dict]:
    out = []
    for tier, path in VERDICT_FILES:
        with path.open(encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                v = json.loads(line)
                v["_tier"] = tier
                out.append(v)
    return out


def normalize_name(s: str | None) -> str:
    """Fold accents + lowercase + collapse whitespace for name matching."""
    if not s:
        return ""
    s = unicodedata.normalize("NFKD", s)
    s = "".join(c for c in s if not unicodedata.combining(c))
    s = s.lower().strip()
    return " ".join(s.split())


def pick_row_by_name(pair_row: dict, target_name: str) -> str | None:
    """Given a producer_dedup_pairs row and a target name, return the producer_id that matches."""
    if not target_name:
        return None
    t = normalize_name(target_name)
    na = normalize_name(pair_row.get("name_a") or "")
    nb = normalize_name(pair_row.get("name_b") or "")
    if t == na:
        return pair_row.get("producer_id_a")
    if t == nb:
        return pair_row.get("producer_id_b")
    # Looser: prefix / substring either way
    if t and na and (t in na or na in t):
        return pair_row.get("producer_id_a")
    if t and nb and (t in nb or nb in t):
        return pair_row.get("producer_id_b")
    return None


def resolve_pair_ids(conn, pair_ids: list[int]) -> dict[int, dict]:
    """Batch-fetch producer_dedup_pairs rows by id."""
    out = {}
    with conn.cursor() as cur:
        # Split into chunks to avoid oversized queries
        chunk = 500
        for i in range(0, len(pair_ids), chunk):
            batch = pair_ids[i:i + chunk]
            cur.execute(
                "SELECT id, name_a, name_b, producer_id_a, producer_id_b, country "
                "FROM producer_dedup_pairs WHERE id = ANY(%s)",
                (batch,),
            )
            for row in cur.fetchall():
                out[row[0]] = {
                    "pair_id": row[0],
                    "name_a": row[1],
                    "name_b": row[2],
                    "producer_id_a": str(row[3]) if row[3] else None,
                    "producer_id_b": str(row[4]) if row[4] else None,
                    "country": row[5],
                }
    return out


def fetch_producer_details(conn, producer_ids: set[str]) -> dict[str, dict]:
    """Fetch name, created_at, metadata-completeness signals, wine count per producer."""
    if not producer_ids:
        return {}
    pids = list(producer_ids)
    out: dict[str, dict] = {}
    with conn.cursor() as cur:
        # Producer rows
        cur.execute(
            """
            SELECT p.id::text,
                   p.name,
                   p.slug,
                   p.country_id,
                   p.region_id,
                   p.website_url,
                   p.year_established,
                   p.hectares_under_vine,
                   p.total_production_cases,
                   p.latitude,
                   p.longitude,
                   p.description,
                   p.parent_producer_id::text,
                   p.deleted_at,
                   p.created_at
            FROM producers p
            WHERE p.id = ANY(%s::uuid[])
            """,
            (pids,),
        )
        for row in cur.fetchall():
            pid = row[0]
            metadata_fields = [row[3], row[4], row[5], row[6], row[7], row[8], row[9], row[10], row[11]]
            out[pid] = {
                "id": pid,
                "name": row[1],
                "slug": row[2],
                "parent_producer_id": row[12],
                "deleted_at": row[13],
                "created_at": row[14],
                "metadata_count": sum(1 for v in metadata_fields if v is not None),
                "has_website": row[5] is not None,
                "wine_count": 0,
                "lwin_count": 0,
            }
        # Wine counts
        cur.execute(
            "SELECT producer_id::text, COUNT(*) FROM wines "
            "WHERE producer_id = ANY(%s::uuid[]) GROUP BY producer_id",
            (pids,),
        )
        for pid, cnt in cur.fetchall():
            if pid in out:
                out[pid]["wine_count"] = int(cnt)
        # LWIN external_ids (polymorphic: entity_type='wine' + join wines.producer_id)
        cur.execute(
            """
            SELECT w.producer_id::text, COUNT(*) FROM external_ids e
            JOIN wines w ON w.id = e.entity_id AND e.entity_type = 'wine'
            WHERE w.producer_id = ANY(%s::uuid[])
              AND lower(e.system) LIKE 'lwin%%'
            GROUP BY w.producer_id
            """,
            (pids,),
        )
        for pid, cnt in cur.fetchall():
            if pid in out:
                out[pid]["lwin_count"] = int(cnt)
    return out


def fk_surface_snapshot(conn, producer_ids: set[str]) -> dict[str, dict[str, int]]:
    """For each producer_id, count rows across every FK-referencing table."""
    if not producer_ids:
        return {}
    pids = list(producer_ids)
    # Enumerate FK columns pointing at producers.id
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT tc.table_name, kcu.column_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
              ON tc.constraint_name = kcu.constraint_name
              AND tc.table_schema = kcu.table_schema
            JOIN information_schema.constraint_column_usage ccu
              ON ccu.constraint_name = tc.constraint_name
              AND ccu.table_schema = tc.table_schema
            WHERE tc.constraint_type = 'FOREIGN KEY'
              AND ccu.table_name = 'producers'
              AND ccu.column_name = 'id'
              AND tc.table_schema = 'public'
            """,
        )
        fks = cur.fetchall()
    # We skip self-ref producers.parent_producer_id from FK surface (handled separately)
    # and skip producer_merge_history.survivor_producer_id (historical record).
    skip = {
        ("producers", "parent_producer_id"),
        ("producer_merge_history", "survivor_producer_id"),
        ("producer_dedup_pairs", "producer_id_a"),
        ("producer_dedup_pairs", "producer_id_b"),
    }
    fks = [(t, c) for (t, c) in fks if (t, c) not in skip]
    counts: dict[str, dict[str, int]] = {pid: {} for pid in pids}
    with conn.cursor() as cur:
        for table, col in fks:
            cur.execute(
                f"SELECT {col}::text, COUNT(*) FROM public.{table} "
                f"WHERE {col} = ANY(%s::uuid[]) GROUP BY {col}",
                (pids,),
            )
            for pid, cnt in cur.fetchall():
                key = f"{table}.{col}"
                counts[pid][key] = counts[pid].get(key, 0) + int(cnt)
    return counts


def resolve_survivor(
    verdict: dict,
    pair_row: dict,
    details: dict[str, dict],
) -> tuple[str | None, str | None, list[str]]:
    """Return (survivor_id, loser_id, flags) for a MERGE verdict."""
    flags = []
    pid_a = pair_row.get("producer_id_a")
    pid_b = pair_row.get("producer_id_b")
    if not pid_a or not pid_b:
        flags.append(f"missing_producer_id(pid_a={pid_a}, pid_b={pid_b})")
        return None, None, flags

    survivor_name = (verdict.get("survivor_name") or "").strip()
    if survivor_name:
        sid = pick_row_by_name(pair_row, survivor_name)
        if sid:
            loser = pid_b if sid == pid_a else pid_a
            return sid, loser, flags
        # survivor_name is a third canonical form (e.g. "Fabien Coche" for pair
        # "Coche Boulicault" vs "Coche Bouillot"). Fall through to §11.6 heuristic
        # to pick survivor row; execute step renames the chosen row to survivor_name.
        flags.append(f"rename_on_merge('{survivor_name}')")

    # Heuristic fallback per §11.6
    a = details.get(pid_a, {})
    b = details.get(pid_b, {})
    # 1. Accent preservation / label form — approximate by preferring the name with non-ASCII chars
    name_a = a.get("name") or ""
    name_b = b.get("name") or ""
    non_ascii_a = any(ord(c) > 127 for c in name_a)
    non_ascii_b = any(ord(c) > 127 for c in name_b)
    if non_ascii_a != non_ascii_b:
        return (pid_a, pid_b, flags) if non_ascii_a else (pid_b, pid_a, flags)
    # 3. Metadata completeness
    if a.get("metadata_count", 0) != b.get("metadata_count", 0):
        return (pid_a, pid_b, flags) if a["metadata_count"] > b["metadata_count"] else (pid_b, pid_a, flags)
    # 4. Wine count
    if a.get("wine_count", 0) != b.get("wine_count", 0):
        return (pid_a, pid_b, flags) if a["wine_count"] > b["wine_count"] else (pid_b, pid_a, flags)
    # 5. LWIN presence
    if a.get("lwin_count", 0) != b.get("lwin_count", 0):
        return (pid_a, pid_b, flags) if a["lwin_count"] > b["lwin_count"] else (pid_b, pid_a, flags)
    # 6. Older row wins
    if a.get("created_at") and b.get("created_at"):
        return (pid_a, pid_b, flags) if a["created_at"] <= b["created_at"] else (pid_b, pid_a, flags)
    # Last resort: alpha
    return (pid_a, pid_b, flags) if pid_a < pid_b else (pid_b, pid_a, flags)


UUID_RE = __import__("re").compile(r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$")


def _uuid_or_none(x) -> str | None:
    if isinstance(x, str) and UUID_RE.match(x):
        return x
    return None


def extract_yellow_merge(v: dict, details: dict[str, dict]) -> tuple[str | None, str | None, list[str]]:
    """Yellow tier MERGE extraction. Return (survivor_id, loser_id, flags)."""
    flags: list[str] = []
    # Variant: explicit primary+sibling
    pid_primary = _uuid_or_none(v.get("producer_id_primary"))
    pid_sibling = _uuid_or_none(v.get("producer_id_sibling"))
    surv_id = _uuid_or_none(v.get("survivor_id"))
    if pid_primary and pid_sibling:
        if surv_id and surv_id in (pid_primary, pid_sibling):
            loser = pid_sibling if surv_id == pid_primary else pid_primary
            return surv_id, loser, flags
        # Use name to pick
        survivor_name = (v.get("survivor_name") or "").strip()
        if survivor_name:
            t = normalize_name(survivor_name)
            np = normalize_name(details.get(pid_primary, {}).get("name") or v.get("name") or "")
            ns = normalize_name(details.get(pid_sibling, {}).get("name") or v.get("sibling_name") or "")
            if t == np or t in np or np in t:
                return pid_primary, pid_sibling, flags
            if t == ns or t in ns or ns in t:
                return pid_sibling, pid_primary, flags
        flags.append("yellow_primary_sibling_unresolved")
        return None, None, flags

    merge_target = _uuid_or_none(v.get("merge_target_id"))
    merge_source = _uuid_or_none(v.get("merge_source_id"))
    pid = _uuid_or_none(v.get("producer_id"))

    # Variant: merge_target_id (yellow subject is loser, target is survivor)
    # But skip if merge_target == producer_id (self-pointing — subject is actually the survivor)
    if merge_target and pid and merge_target != pid:
        return merge_target, pid, flags

    # Variant: merge_source_id (yellow subject is survivor, source is loser)
    if merge_source and pid and merge_source != pid:
        return pid, merge_source, flags

    # No-op case: yellow#62 Chateau Ausone — survivor_name equals subject's name, no target/source
    if pid and not merge_target and not merge_source:
        target_name = (v.get("merge_target_name") or v.get("survivor_name") or "").strip()
        if target_name:
            flags.append(f"yellow_merge_target_by_name_only('{target_name}')")
        else:
            flags.append("yellow_merge_target_missing")
        return None, None, flags

    if not pid:
        flags.append(f"yellow_producer_id_not_uuid('{v.get('producer_id')}')")
    return None, None, flags


def extract_yellow_pc(v: dict) -> tuple[str | None, str | None, list[str]]:
    """Yellow tier PARENT_CHILD extraction. Return (parent_id, child_id, flags)."""
    flags: list[str] = []
    parent = _uuid_or_none(v.get("parent_id"))
    pid = _uuid_or_none(v.get("producer_id"))
    sibling = _uuid_or_none(v.get("sibling_id")) or _uuid_or_none(v.get("sibling_producer_id"))
    if parent and (pid or sibling):
        # Child is the non-parent side
        if parent == pid and sibling:
            return parent, sibling, flags
        if parent == sibling and pid:
            return parent, pid, flags
        # Parent is somewhere else? derive child = yellow subject
        if pid and parent != pid:
            return parent, pid, flags
    if not parent:
        flags.append("yellow_parent_id_missing_or_invalid")
    if not (pid or sibling):
        flags.append("yellow_no_child_side_resolvable")
    return None, None, flags


def resolve_parent_child(
    verdict: dict,
    pair_row: dict,
    details: dict[str, dict] | None = None,
) -> tuple[str | None, str | None, list[str]]:
    """Return (parent_id, child_id, flags) for a PARENT_CHILD verdict."""
    flags = []
    pid_a = pair_row.get("producer_id_a")
    pid_b = pair_row.get("producer_id_b")
    if not pid_a or not pid_b:
        flags.append(f"missing_producer_id(pid_a={pid_a}, pid_b={pid_b})")
        return None, None, flags

    parent_name = (verdict.get("parent_name") or "").strip()
    if not parent_name:
        signals = verdict.get("signals") or {}
        hint = signals.get("parent_producer_id")
        if hint and hint in (pid_a, pid_b):
            parent = hint
            child = pid_b if parent == pid_a else pid_a
            return parent, child, flags
        flags.append("parent_name_missing")
        return None, None, flags

    parent = pick_row_by_name(pair_row, parent_name)
    if parent:
        child = pid_b if parent == pid_a else pid_a
        return parent, child, flags

    # parent_name is a third canonical form. Pick the row with more wines
    # as the parent (per §11.6 wine-count tie-break) and rename it during execute.
    flags.append(f"rename_on_parent_link('{parent_name}')")
    if details is not None:
        a = details.get(pid_a, {})
        b = details.get(pid_b, {})
        wa = a.get("wine_count", 0)
        wb = b.get("wine_count", 0)
        if wa != wb:
            parent = pid_a if wa > wb else pid_b
        elif a.get("metadata_count", 0) != b.get("metadata_count", 0):
            parent = pid_a if a["metadata_count"] > b["metadata_count"] else pid_b
        else:
            parent = pid_a  # deterministic fallback
        child = pid_b if parent == pid_a else pid_a
        return parent, child, flags
    return None, None, flags


def resolve_chains(merges: list[dict]) -> tuple[dict[str, str], list[str]]:
    """Union-find across MERGE verdicts → each loser maps to its terminal survivor.

    Returns (loser_id → terminal_survivor_id, chain_warnings).
    """
    # Build direct survivor map
    direct: dict[str, str] = {}
    for m in merges:
        s = m["survivor_id"]
        l = m["loser_id"]
        if not s or not l:
            continue
        direct[l] = s

    def terminal(pid: str, seen: set[str]) -> str:
        if pid not in direct:
            return pid
        if pid in seen:
            return pid  # cycle — shouldn't happen but defend
        seen.add(pid)
        return terminal(direct[pid], seen)

    resolved = {l: terminal(s, {l}) for l, s in direct.items()}

    warnings = []
    seen = set()
    for l, s_direct in direct.items():
        s_final = resolved[l]
        if s_direct != s_final and (l, s_final) not in seen:
            warnings.append(f"chain: {l} → {s_direct} → ... → {s_final}  (resolved to terminal)")
            seen.add((l, s_final))
        # Self-pointing bug detection
        if l == s_direct:
            warnings.append(f"self_merge(bug): {l} → itself")
    return resolved, warnings


def build_scorecard(conn) -> None:
    verdicts = load_verdicts()
    by_verdict = Counter(v["verdict"] for v in verdicts)
    by_cluster = Counter(v.get("pattern_cluster") for v in verdicts)
    by_tier = Counter(v["_tier"] for v in verdicts)

    pair_ids = [int(v["pair_id"]) for v in verdicts if v.get("pair_id") is not None]
    print(f"Loaded {len(verdicts)} verdicts across 4 tiers")
    print(f"Resolving {len(pair_ids)} pair_ids from producer_dedup_pairs...")
    pair_rows = resolve_pair_ids(conn, pair_ids)

    all_producer_ids: set[str] = set()
    for pr in pair_rows.values():
        if pr["producer_id_a"]:
            all_producer_ids.add(pr["producer_id_a"])
        if pr["producer_id_b"]:
            all_producer_ids.add(pr["producer_id_b"])
    # Yellow-tier producer UUIDs
    for v in verdicts:
        if v["_tier"] != "yellow":
            continue
        for key in ("producer_id", "producer_id_primary", "producer_id_sibling",
                    "merge_target_id", "merge_source_id", "survivor_id",
                    "parent_id", "sibling_id", "sibling_producer_id",
                    "unflagged_additional_merge_id"):
            u = _uuid_or_none(v.get(key))
            if u:
                all_producer_ids.add(u)
    print(f"Fetching details for {len(all_producer_ids)} distinct producers...")
    details = fetch_producer_details(conn, all_producer_ids)
    print("Computing FK surface impact counts...")
    fk_counts = fk_surface_snapshot(conn, all_producer_ids)

    merges: list[dict] = []
    parent_children: list[dict] = []
    skip_flags: list[str] = []
    cross_pair_flags: list[str] = []

    for v in verdicts:
        verdict = v["verdict"]
        tier = v["_tier"]
        if tier == "yellow":
            if verdict == "MERGE":
                s, l, flags = extract_yellow_merge(v, details)
                name_a = v.get("name") or ""
                name_b = v.get("sibling_name") or v.get("merge_target_name") or v.get("merge_source_name") or ""
                merges.append({
                    "pair_id": f"yellow#{v.get('idx')}",
                    "tier": tier,
                    "name_a": name_a,
                    "name_b": name_b,
                    "pattern_cluster": v.get("pattern_cluster"),
                    "survivor_id": s,
                    "loser_id": l,
                    "flags": flags,
                    "verdict_row": v,
                    "pair_row": None,
                })
                # Handle optional unflagged_additional_merge_id as a second MERGE
                extra = _uuid_or_none(v.get("unflagged_additional_merge_id"))
                if extra and s:
                    merges.append({
                        "pair_id": f"yellow#{v.get('idx')}#extra",
                        "tier": tier,
                        "name_a": v.get("unflagged_additional_name") or "",
                        "name_b": name_a,
                        "pattern_cluster": v.get("pattern_cluster"),
                        "survivor_id": s,
                        "loser_id": extra,
                        "flags": ["yellow_extra_merge_from_unflagged_additional_merge_id"],
                        "verdict_row": v,
                        "pair_row": None,
                    })
            elif verdict == "PARENT_CHILD":
                parent, child, flags = extract_yellow_pc(v)
                parent_children.append({
                    "pair_id": f"yellow#{v.get('idx')}",
                    "tier": tier,
                    "name_a": v.get("name") or "",
                    "name_b": v.get("sibling_name") or "",
                    "pattern_cluster": v.get("pattern_cluster"),
                    "parent_id": parent,
                    "child_id": child,
                    "flags": flags,
                    "verdict_row": v,
                    "pair_row": None,
                })
            # SKIP / KEEP_AS_IS: no action
            continue

        # Pair-based tiers (core, mid, tail)
        pair_id = int(v["pair_id"]) if v.get("pair_id") is not None else None
        pair_row = pair_rows.get(pair_id)
        if pair_row is None:
            if verdict in ("MERGE", "PARENT_CHILD"):
                skip_flags.append(f"pair_id {pair_id} ({v.get('name_a')} vs {v.get('name_b')}): not found in producer_dedup_pairs")
            continue
        if verdict == "MERGE":
            s, l, flags = resolve_survivor(v, pair_row, details)
            merges.append({
                "pair_id": pair_id,
                "tier": tier,
                "name_a": pair_row["name_a"],
                "name_b": pair_row["name_b"],
                "pattern_cluster": v.get("pattern_cluster"),
                "survivor_id": s,
                "loser_id": l,
                "flags": flags,
                "verdict_row": v,
                "pair_row": pair_row,
            })
        elif verdict == "PARENT_CHILD":
            parent, child, flags = resolve_parent_child(v, pair_row, details)
            parent_children.append({
                "pair_id": pair_id,
                "tier": tier,
                "name_a": pair_row["name_a"],
                "name_b": pair_row["name_b"],
                "pattern_cluster": v.get("pattern_cluster"),
                "parent_id": parent,
                "child_id": child,
                "flags": flags,
                "verdict_row": v,
                "pair_row": pair_row,
            })
        # SKIP and KEEP_AS_IS: no action

    resolved_chains, chain_warnings = resolve_chains(merges)
    # Rewrite merges to use terminal survivors for downstream analytics
    for m in merges:
        if m["loser_id"] and m["loser_id"] in resolved_chains:
            terminal = resolved_chains[m["loser_id"]]
            if terminal != m["survivor_id"]:
                m["survivor_id"] = terminal

    # Build scorecard
    lines: list[str] = []
    add = lines.append

    add("# B6.6 Step 10c — Pre-execution Scorecard")
    add("")
    add(f"Generated for {len(verdicts)} Chrome-validated verdicts.")
    add("")
    add("## Summary")
    add("")
    add(f"- Verdict distribution: {dict(by_verdict)}")
    add(f"- Tier distribution: {dict(by_tier)}")
    add(f"- MERGE pairs to apply: **{len(merges)}**")
    add(f"- PARENT_CHILD pairs to apply: **{len(parent_children)}**")
    add(f"- SKIP / KEEP_AS_IS (no-op): **{by_verdict.get('SKIP', 0) + by_verdict.get('KEEP_AS_IS', 0)}**")
    add("")

    # Aggregate wines affected
    loser_ids = {m["loser_id"] for m in merges if m["loser_id"]}
    wines_affected = sum(details.get(pid, {}).get("wine_count", 0) for pid in loser_ids)
    add(f"- Total wines to be re-pointed (sum of loser wine_counts): **{wines_affected:,}**")
    add(f"- Producers to be soft-deleted (§11.6): **{len(loser_ids)}**")
    add(f"- Producers to have parent_producer_id set: **{len({pc['child_id'] for pc in parent_children if pc['child_id']})}**")
    add("")

    # Flags section — classify by severity
    flagged_merges = [m for m in merges if m["flags"]]
    flagged_pcs = [pc for pc in parent_children if pc["flags"]]

    def is_blocking(fl_list: list[str]) -> bool:
        for f in fl_list:
            if f.startswith("rename_on_merge(") or f.startswith("rename_on_parent_link("):
                continue
            if f == "yellow_extra_merge_from_unflagged_additional_merge_id":
                continue
            return True
        return False

    blocking_merges = [m for m in flagged_merges if is_blocking(m["flags"])]
    blocking_pcs = [pc for pc in flagged_pcs if is_blocking(pc["flags"])]
    total_flags = len(flagged_merges) + len(flagged_pcs) + len(skip_flags) + len(chain_warnings)

    add("## Flag severity summary")
    add("")
    add(f"- **Blocking flags (need user decision):** {len(blocking_merges) + len(blocking_pcs)}")
    add(f"- **Soft flags (rename-on-merge / rename-on-parent-link, execute auto-handles):** {len(flagged_merges) + len(flagged_pcs) - len(blocking_merges) - len(blocking_pcs)}")
    add(f"- **Chain-merge warnings (resolved by union-find, auto-handles):** {len(chain_warnings)}")
    add("")

    if blocking_merges or blocking_pcs:
        add("### Blocking flags")
        add("")
        for m in blocking_merges:
            add(f"- **MERGE pair {m['pair_id']}** ({m['name_a']} vs {m['name_b']}): {', '.join(m['flags'])}")
        for pc in blocking_pcs:
            add(f"- **PC pair {pc['pair_id']}** ({pc['name_a']} vs {pc['name_b']}): {', '.join(pc['flags'])}")
        add("")
        add("**Proposed handling for blocking items:**")
        add("- `yellow_merge_target_by_name_only(...)`: no actual merge partner in DB — these are rename-only operations. Options: (a) rename the subject producer to the target name (no row absorbed), or (b) skip. Default: **skip** during execute, flag for Sprint 7 revisit.")
        add("- `yellow_parent_id_missing_or_invalid` / `yellow_no_child_side_resolvable`: data integrity issue (e.g. string placeholder instead of UUID). Default: **skip** during execute, flag for Sprint 7 revisit.")
        add("")

    add("## Flags")
    add("")
    if total_flags == 0:
        add("**None.** All pairs resolved cleanly.")
    else:
        add(f"**{total_flags} flags requiring review.**")
        add("")
        if flagged_merges:
            add(f"### MERGE resolution flags ({len(flagged_merges)})")
            add("")
            for m in flagged_merges:
                add(f"- pair {m['pair_id']} ({m['name_a']} vs {m['name_b']}): {', '.join(m['flags'])}")
            add("")
        if flagged_pcs:
            add(f"### PARENT_CHILD resolution flags ({len(flagged_pcs)})")
            add("")
            for pc in flagged_pcs:
                add(f"- pair {pc['pair_id']} ({pc['name_a']} vs {pc['name_b']}): {', '.join(pc['flags'])}")
            add("")
        if skip_flags:
            add(f"### Pair-row lookup flags ({len(skip_flags)})")
            add("")
            for s in skip_flags:
                add(f"- {s}")
            add("")
        if chain_warnings:
            add(f"### Chain-merge warnings ({len(chain_warnings)})")
            add("")
            add("The executor will resolve these to terminal survivors via union-find.")
            add("")
            for w in chain_warnings:
                add(f"- {w}")
            add("")

    # Pattern-cluster breakdown
    add("## Pattern-cluster breakdown (all verdicts)")
    add("")
    add("| Cluster | MERGE | PC | SKIP | KEEP_AS_IS | Total |")
    add("|---|---|---|---|---|---|")
    cluster_detail: dict[str | None, Counter] = defaultdict(Counter)
    for v in verdicts:
        cluster_detail[v.get("pattern_cluster")][v["verdict"]] += 1
    for cluster in sorted(cluster_detail.keys(), key=lambda k: (k is None, str(k))):
        c = cluster_detail[cluster]
        total = sum(c.values())
        label = cluster if cluster is not None else "(none)"
        add(f"| {label} | {c.get('MERGE', 0)} | {c.get('PARENT_CHILD', 0)} | {c.get('SKIP', 0)} | {c.get('KEEP_AS_IS', 0)} | {total} |")
    add("")

    # Top 20 largest merges
    ranked = sorted(
        (m for m in merges if m["loser_id"]),
        key=lambda m: details.get(m["loser_id"], {}).get("wine_count", 0),
        reverse=True,
    )[:20]
    add("## Top 20 largest MERGEs (by loser wine count — manual sanity-check list)")
    add("")
    add("| pair_id | tier | cluster | loser → survivor | loser wines | survivor wines | combined |")
    add("|---|---|---|---|---|---|---|")
    for m in ranked:
        l = details.get(m["loser_id"], {})
        s = details.get(m["survivor_id"], {})
        loser_name = l.get("name") or m["name_a"]
        surv_name = s.get("name") or m["name_b"]
        lw = l.get("wine_count", 0)
        sw = s.get("wine_count", 0)
        add(f"| {m['pair_id']} | {m['tier']} | {m['pattern_cluster']} | {loser_name} → {surv_name} | {lw} | {sw} | {lw + sw} |")
    add("")

    # Top 20 largest PCs
    pc_ranked = sorted(
        (pc for pc in parent_children if pc["parent_id"] and pc["child_id"]),
        key=lambda pc: details.get(pc["parent_id"], {}).get("wine_count", 0),
        reverse=True,
    )[:20]
    add("## Top 20 largest PARENT_CHILD assignments (by parent wine count)")
    add("")
    add("| pair_id | tier | cluster | child → parent | child wines | parent wines |")
    add("|---|---|---|---|---|---|")
    for pc in pc_ranked:
        p = details.get(pc["parent_id"], {})
        c = details.get(pc["child_id"], {})
        add(f"| {pc['pair_id']} | {pc['tier']} | {pc['pattern_cluster']} | {c.get('name', pc['name_a'])} → {p.get('name', pc['name_b'])} | {c.get('wine_count', 0)} | {p.get('wine_count', 0)} |")
    add("")

    # FK surface summary
    add("## FK surface impact")
    add("")
    add("Sum of rows across FK-referencing tables, aggregated over all loser producers:")
    add("")
    agg: Counter = Counter()
    for loser in loser_ids:
        for k, v in fk_counts.get(loser, {}).items():
            agg[k] += v
    add("| Table.Column | Rows to re-point |")
    add("|---|---|")
    for k, v in sorted(agg.items(), key=lambda kv: -kv[1]):
        add(f"| {k} | {v:,} |")
    add("")

    # Append per-tier breakdown
    add("## Per-tier verdict breakdown")
    add("")
    add("| Tier | MERGE | PC | SKIP | KEEP_AS_IS | Total |")
    add("|---|---|---|---|---|---|")
    tier_detail: dict[str, Counter] = defaultdict(Counter)
    for v in verdicts:
        tier_detail[v["_tier"]][v["verdict"]] += 1
    for tier in ("yellow", "core", "mid", "tail"):
        c = tier_detail.get(tier, Counter())
        total = sum(c.values())
        add(f"| {tier} | {c.get('MERGE', 0)} | {c.get('PARENT_CHILD', 0)} | {c.get('SKIP', 0)} | {c.get('KEEP_AS_IS', 0)} | {total} |")
    add("")

    add("---")
    add("")
    add("**Next step:** user review + signoff. If clean, run `scripts/sprint6_step10_execute.py --execute`.")
    add("")

    OUT_PATH.write_text("\n".join(lines), encoding="utf-8")
    print(f"\nScorecard written to {OUT_PATH}")
    print(f"  MERGE: {len(merges)}  (flagged: {len(flagged_merges)})")
    print(f"  PC:    {len(parent_children)}  (flagged: {len(flagged_pcs)})")
    print(f"  Chain warnings: {len(chain_warnings)}")
    print(f"  Pair-lookup flags: {len(skip_flags)}")


def main() -> int:
    conn = get_conn()
    try:
        build_scorecard(conn)
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
