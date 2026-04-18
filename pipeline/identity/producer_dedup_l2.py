"""
B6.4 Phase D — L2 Haiku rich-prompt producer-pair classifier.

Reads pairs (from DB or calibration_set.json), batches 5/call, sends richer
context than L1 to Haiku 4.5, writes results to producer_dedup_pairs with
method_name='l2_haiku_rich'.

Difference from L1:
- Full TTB fingerprint (no per-field cap; all BW permits, all addresses,
  all brand names, all permittees shown)
- Up to 20 wines per producer (was 5 at L1)
- Producer metadata (year_established, region, country_id) shown
- External_ids (not just LWIN count)
- Producer aliases history
- parent_producer_id chain
- Batch size 5/call (was 10/call at L1) — richer prompt, lower throughput

Run:
    # dry-run
    python -m pipeline.identity.producer_dedup_l2 --dry-run --limit 5

    # run on calibration set pairs (for Phase D1 calibration)
    python -m pipeline.identity.producer_dedup_l2 --calibration --execute

    # run on production escalated set (Phase D2, after threshold set)
    python -m pipeline.identity.producer_dedup_l2 --execute --budget 60 \
        --l1-conf-max 0.92 --l1-verdicts UNCERTAIN,MERGE,PARENT_CHILD,SKIP
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from threading import Lock

import anthropic

from pipeline.lib.db import get_conn, get_env
from pipeline.lib.models import HAIKU_MODEL


CALIBRATION_PATH = Path(__file__).resolve().parents[2] / "data" / "sprints" / "dedup" / "calibration_set.json"
IDENTITY_RULES_PATH = Path(__file__).resolve().parents[2] / "docs" / "IDENTITY_RULES.md"


PRICING = {
    "input":           0.80,
    "output":          4.00,
    "cache_write":     1.00,
    "cache_write_1h":  1.60,
    "cache_read":      0.08,
}


def load_section_11() -> str:
    text = IDENTITY_RULES_PATH.read_text(encoding="utf-8")
    m = re.search(
        r"(## 11\. Producer Identity Rules.*?)(?=\n## Appendix|\n---\s*\n## )",
        text, re.DOTALL
    )
    if not m:
        raise RuntimeError("Could not locate Section 11")
    return m.group(1).strip()


OUTPUT_SCHEMA = """OUTPUT FORMAT — return ONLY a JSON array, no markdown fences, no preamble:

[
  {
    "pair_id": <integer>,
    "verdict": "MERGE" | "PARENT_CHILD" | "SKIP" | "UNCERTAIN",
    "confidence": <float 0.0-1.0>,
    "reasoning": "<2-3 sentences citing specific evidence>"
  },
  ...
]
"""


L2_INSTRUCTIONS = """You are the L2 rich-context classifier. Each pair comes with MORE signal than
the L1 prompt had: full TTB fingerprint (every BW permit + permittee + address +
brand list), up to 20 wines per producer, external IDs, and producer metadata.

Tune confidence carefully:
- If multiple pieces of new context converge on the same verdict → confidence >= 0.90
- If the new context resolves an L1 ambiguity → confidence 0.88-0.95
- If context is still mixed (true borderline) → confidence 0.75-0.85; prefer UNCERTAIN only at <0.75
- Consider the brand-on-label rule strictly. Shared TTB permit does NOT imply MERGE
  if brand names on COLAs are different (that's PARENT_CHILD). Substring/similarity
  alone without corroborating TTB/LWIN does NOT imply MERGE — could be commune
  overlap or family-name collision.
"""


FEW_SHOT = """Examples:

Example 1 — MERGE resolved by shared BW permit + single brand on COLAs:
Input: Producer A "Ridge Vineyards" (US, 110 wines) TTB {BW-CA-4488, permittee RIDGE VINEYARDS 17100 MONTE BELLO RD, brands [RIDGE,RIDGE VINEYARDS,MONTE BELLO], colas=85};
       Producer B "Ridge" (US, 2 wines) TTB {BW-CA-4488, permittee RIDGE VINEYARDS same address, brands [RIDGE VINEYARDS], colas=3}. signals s2_trigram+s9_substring+s6_ttb_permit.
Output: {"verdict":"MERGE","confidence":0.98,"reasoning":"Identical BW permit BW-CA-4488 + identical permittee name + identical address + overlapping brand list (RIDGE, RIDGE VINEYARDS). Producer B is a B6.2 LWIN-import variant of A."}

Example 2 — PARENT_CHILD: shared BW permit but distinct brand portfolios:
Input: Producer A "Silver Oak" (US, 4 wines) TTB {BW-CA-5455, permittee SILVER OAK CELLARS, brands [SILVER OAK,SILVER OAK CELLARS]};
       Producer B "Twomey" (US, 13 wines) TTB {BW-CA-5455, permittee SILVER OAK CELLARS, brands [TWOMEY,TWOMEY CELLARS]}. signals s6_ttb_permit.
Output: {"verdict":"PARENT_CHILD","confidence":0.93,"reasoning":"Shared BW permit BW-CA-5455 + shared permittee (Duncan family), but COLAs show entirely distinct brand names (SILVER OAK vs TWOMEY). Classic sister-winery pattern per §11.2 — same facility, different labels."}

Example 3 — SKIP despite shared BW permit (importer/custom crush pattern):
Input: Producer A "Errazuriz" (CL, 35 wines); Producer B "Caliterra" (CL, 22 wines). TTB signal: same US importer address on COLAs (both filed by a third party). wines differ entirely.
Output: {"verdict":"SKIP","confidence":0.92,"reasoning":"Two independent Chilean producers that happen to share a US import/distribution entity on TTB filings. Brand names entirely distinct on labels; no shared wine catalog; distinct estates. §11.4.j applies — shared permit does not imply same producer when it's an importer pattern."}

Example 4 — MERGE despite no TTB overlap (non-US, shared LWIN):
Input: Producer A "Domaine Leflaive" (FR, 40 wines, 35 LWIN-linked); Producer B "Leflaive" (FR, 3 wines, 3 LWIN-linked). signals s2_trigram+s9_substring+s5_shared_wine_lwin. Shared wines: "Puligny-Montrachet 1er Cru Les Pucelles", "Bâtard-Montrachet Grand Cru".
Output: {"verdict":"MERGE","confidence":0.96,"reasoning":"Shared LWIN_7s on Domaine Leflaive's iconic Grand Cru and 1er Cru wines (Pucelles, Bâtard-Montrachet). Producer B is a short-form spelling variant imported from a different catalog; both refer to the same White Burgundy house in Puligny-Montrachet."}

Example 5 — SKIP: commune-overlap trap, high trigram, no supporting signals:
Input: Producer A "Château Latour" (FR, 12 wines, 10 LWIN-linked Pauillac); Producer B "Château Latour-Martillac" (FR, 8 wines, 6 LWIN-linked Pessac-Léognan Graves). signals s2_trigram 0.75.
Output: {"verdict":"SKIP","confidence":0.98,"reasoning":"Two Bordeaux estates in different sub-regions — 1st Growth Pauillac vs Pessac-Léognan Graves classified growth. Commune/vineyard-name collision only (§11.4.k); wine catalogs and LWIN-linked appellations are entirely distinct."}
"""


def build_static_preamble(section_11: str) -> str:
    return f"""You are the L2 rich-context classifier for producer-pair duplicates. You
receive the same kind of output schema as the L1 classifier but with a richer
per-pair context block. Apply IDENTITY_RULES Section 11 strictly.

{section_11}

{OUTPUT_SCHEMA}

{L2_INSTRUCTIONS}

{FEW_SHOT}
"""


# ── Context loaders ──────────────────────────────────────────────

def load_ttb_fingerprints(cur, producer_ids: list) -> dict:
    if not producer_ids:
        return {}
    placeholders = ",".join(["%s"] * len(producer_ids))
    cur.execute(f"""
        SELECT canonical_producer_id,
               COALESCE(permit_no, permit_number) AS permit,
               applicant_name, applicant_address, applicant_city, applicant_state,
               brand_name, fanciful_name
        FROM source_ttb_colas
        WHERE canonical_producer_id IN ({placeholders})
          AND (permit_no IS NOT NULL OR permit_number IS NOT NULL)
        LIMIT 50000
    """, producer_ids)
    per = {}
    for pid, permit, nm, addr, city, state, brand, fanciful in cur.fetchall():
        d = per.setdefault(pid, {
            "bw_permits": set(),
            "permittees": set(),
            "addresses": set(),
            "brand_names": set(),
            "fanciful_names": set(),
            "cola_count": 0,
        })
        d["cola_count"] += 1
        if permit and (permit.startswith("BW-") or re.match(r"^[A-Z]{2,3}-BW", permit)):
            d["bw_permits"].add(permit)
        if nm:
            d["permittees"].add(nm)
        if addr:
            full = ", ".join(x for x in [addr, city, state] if x)
            if full:
                d["addresses"].add(full)
        if brand:
            d["brand_names"].add(brand.upper())
        if fanciful:
            d["fanciful_names"].add(fanciful.upper())
    # Serialize as sorted lists (full, no cap — richer than L1)
    result = {}
    for pid, d in per.items():
        result[pid] = {
            "bw_permits": sorted(d["bw_permits"]),
            "permittees": sorted(d["permittees"]),
            "addresses": sorted(d["addresses"]),
            "brand_names": sorted(d["brand_names"]),
            "fanciful_names": sorted(d["fanciful_names"]),
            "cola_count": d["cola_count"],
        }
    return result


def load_wine_catalog(cur, producer_ids: list, cap_per_producer: int = 20) -> dict:
    if not producer_ids:
        return {}
    placeholders = ",".join(["%s"] * len(producer_ids))
    cur.execute(f"""
        WITH ranked AS (
          SELECT w.producer_id, w.display_name, w.name,
                 ROW_NUMBER() OVER (
                   PARTITION BY w.producer_id
                   ORDER BY
                     (CASE WHEN w.display_name IS NOT NULL THEN 0 ELSE 1 END) ASC,
                     length(COALESCE(w.display_name, w.name, '')) DESC
                 ) AS rn
          FROM wines w
          WHERE w.producer_id IN ({placeholders}) AND w.deleted_at IS NULL
        )
        SELECT producer_id, COALESCE(display_name, name) AS nm
        FROM ranked
        WHERE rn <= %s
    """, producer_ids + [cap_per_producer])
    per = {}
    for pid, nm in cur.fetchall():
        per.setdefault(pid, []).append(nm)
    return per


def load_producer_meta(cur, producer_ids: list) -> dict:
    if not producer_ids:
        return {}
    placeholders = ",".join(["%s"] * len(producer_ids))
    cur.execute(f"""
        SELECT p.id, p.metadata, p.region_id, p.year_established,
               p.country_id, p.parent_producer_id,
               r.name AS region_name, c.iso_code AS country_code
        FROM producers p
        LEFT JOIN regions r ON r.id = p.region_id
        LEFT JOIN countries c ON c.id = p.country_id
        WHERE p.id IN ({placeholders})
    """, producer_ids)
    per = {}
    for pid, md, rid, year, cid, parent_id, rname, ccode in cur.fetchall():
        website = None
        if md and isinstance(md, dict):
            website = md.get("website") or md.get("url") or md.get("homepage")
        per[pid] = {
            "website": website,
            "region_name": rname,
            "country_code": ccode,
            "year_established": year,
            "parent_producer_id": str(parent_id) if parent_id else None,
        }
    return per


def load_external_ids(cur, producer_ids: list) -> dict:
    if not producer_ids:
        return {}
    placeholders = ",".join(["%s"] * len(producer_ids))
    cur.execute(f"""
        SELECT entity_id, system, external_id
        FROM external_ids
        WHERE entity_type='producer' AND entity_id IN ({placeholders})
    """, producer_ids)
    per = {}
    for pid, system, ext in cur.fetchall():
        per.setdefault(str(pid), {}).setdefault(system, []).append(ext)
    # Cap per-system to 5
    for pid, systems in per.items():
        for s in systems:
            systems[s] = systems[s][:5]
    return per


def load_aliases(cur, producer_ids: list) -> dict:
    if not producer_ids:
        return {}
    placeholders = ",".join(["%s"] * len(producer_ids))
    # Guard against missing table
    cur.execute(f"""
        SELECT tablename FROM pg_tables WHERE tablename='producer_aliases'
    """)
    if not cur.fetchone():
        return {}
    cur.execute(f"""
        SELECT producer_id, name, alias_type, source
        FROM producer_aliases
        WHERE producer_id IN ({placeholders})
        LIMIT 200
    """, producer_ids)
    per = {}
    for pid, alias, atype, src in cur.fetchall():
        per.setdefault(str(pid), []).append({"alias": alias, "type": atype, "source": src})
    return per


# ── Per-pair formatter ──────────────────────────────────────────

def format_pair_context(pair: dict, ttb: dict, wines: dict, meta: dict,
                         ext_ids: dict, aliases: dict) -> str:
    pid = pair["pair_id"]
    a = pair["producer_id_a"]
    b = pair["producer_id_b"]

    def ttb_blob(t):
        if not t:
            return "  (no TTB)"
        parts = []
        if t.get("bw_permits"):
            parts.append(f"    BW permits ({len(t['bw_permits'])}): {t['bw_permits']}")
        if t.get("permittees"):
            parts.append(f"    Permittees ({len(t['permittees'])}): {t['permittees']}")
        if t.get("addresses"):
            parts.append(f"    Addresses ({len(t['addresses'])}): {t['addresses']}")
        if t.get("brand_names"):
            parts.append(f"    Brand names on COLAs ({len(t['brand_names'])}): {t['brand_names']}")
        if t.get("fanciful_names"):
            parts.append(f"    Fanciful names ({len(t['fanciful_names'])}): {t['fanciful_names']}")
        parts.append(f"    COLA count: {t['cola_count']}")
        return "\n".join(parts)

    def meta_blob(m):
        if not m:
            return "  (no metadata)"
        bits = [
            f"    country: {m.get('country_code')}",
            f"    region: {m.get('region_name') or '-'}",
            f"    website: {m.get('website') or '-'}",
            f"    year_established: {m.get('year_established') or '-'}",
        ]
        if m.get("parent_producer_id"):
            bits.append(f"    parent_producer_id: {m['parent_producer_id']}")
        return "\n".join(bits)

    def ext_blob(e):
        if not e:
            return "  (no external_ids)"
        return "    " + json.dumps(e, ensure_ascii=False)

    def alias_blob(al):
        if not al:
            return "  (no aliases)"
        return "\n".join(f"    - {x['alias']!r} ({x['type']}/{x['source']})" for x in al[:10])

    wines_a = wines.get(a, [])
    wines_b = wines.get(b, [])

    return f"""--- PAIR {pid} ---
Country: {pair['country']} | L1 verdict: {pair.get('l1_verdict')} / conf {pair.get('l1_confidence')}
L1 reasoning: {pair.get('l1_reasoning')}

Producer A ({pair['name_a']!r}):
  wines={pair['wines_a']} ({len(wines_a)} shown)
  Top wines: {wines_a[:20]}
  TTB:
{ttb_blob(ttb.get(a, {}))}
  Meta:
{meta_blob(meta.get(a, {}))}
  External IDs:
{ext_blob(ext_ids.get(a, {}))}
  Aliases:
{alias_blob(aliases.get(a, []))}

Producer B ({pair['name_b']!r}):
  wines={pair['wines_b']} ({len(wines_b)} shown)
  Top wines: {wines_b[:20]}
  TTB:
{ttb_blob(ttb.get(b, {}))}
  Meta:
{meta_blob(meta.get(b, {}))}
  External IDs:
{ext_blob(ext_ids.get(b, {}))}
  Aliases:
{alias_blob(aliases.get(b, []))}

Blocking signals: {json.dumps(pair.get('signals') or {}, default=str)}
"""


# ── API call ─────────────────────────────────────────────────────

def call_l2(client, static_preamble: str, batch_block: str,
            dry_run: bool = False) -> tuple[list, dict]:
    if dry_run:
        return [], {
            "input_tokens": 0, "output_tokens": 0,
            "cache_read_tokens": 0, "cache_write_tokens": 0,
        }
    system = [{
        "type": "text",
        "text": static_preamble,
        "cache_control": {"type": "ephemeral"},
    }]
    n = len(re.findall(r"--- PAIR", batch_block))
    user = f"Classify the {n} pairs below. Return ONLY a JSON array as specified.\n\n{batch_block}"
    resp = client.messages.create(
        model=HAIKU_MODEL,
        max_tokens=3500,
        system=system,
        messages=[{"role": "user", "content": user}],
    )
    raw = resp.content[0].text.strip()
    if raw.startswith("```"):
        raw = raw.split("\n", 1)[1] if "\n" in raw else raw[3:]
        if raw.endswith("```"):
            raw = raw[:-3].strip()
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        m = re.search(r"\[\s*\{.*\}\s*\]", raw, re.DOTALL)
        if m:
            parsed = json.loads(m.group(0))
        else:
            raise RuntimeError(f"L2 JSON parse failed. First 400 chars: {raw[:400]}")
    u = resp.usage
    return parsed, {
        "input_tokens": u.input_tokens,
        "output_tokens": u.output_tokens,
        "cache_read_tokens": getattr(u, "cache_read_input_tokens", 0) or 0,
        "cache_write_tokens": getattr(u, "cache_creation_input_tokens", 0) or 0,
    }


def write_l2(cur, pair_verdicts, method_name: str = "l2_haiku_rich"):
    for pair, verdict, cost_cents in pair_verdicts:
        cur.execute("""
          INSERT INTO producer_dedup_pairs
            (producer_id_a, producer_id_b, name_a, name_b, country, similarity,
             wines_a, wines_b, method_name, verdict, confidence, reasoning,
             cost_cents, signals, created_at, updated_at)
          VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s,
                  %s, %s, %s, %s, %s, now(), now())
          ON CONFLICT (producer_id_a, producer_id_b, method_name)
          DO UPDATE SET
            verdict = EXCLUDED.verdict,
            confidence = EXCLUDED.confidence,
            reasoning = EXCLUDED.reasoning,
            cost_cents = EXCLUDED.cost_cents,
            signals = EXCLUDED.signals,
            updated_at = now()
        """, (
            pair["producer_id_a"], pair["producer_id_b"],
            pair["name_a"], pair["name_b"], pair["country"], pair["similarity"],
            pair["wines_a"], pair["wines_b"],
            method_name,
            verdict.get("verdict"),
            verdict.get("confidence"),
            verdict.get("reasoning"),
            cost_cents,
            json.dumps({"l1_verdict": pair.get("l1_verdict"),
                        "l1_confidence": pair.get("l1_confidence"),
                        "blocking_signals": pair.get("signals"),
                        "l2_model": HAIKU_MODEL}, default=str),
        ))


def compute_cost_cents(usage):
    in_t = usage["input_tokens"]
    out_t = usage["output_tokens"]
    cache_r = usage.get("cache_read_tokens", 0)
    cache_w = usage.get("cache_write_tokens", 0)
    regular_input = max(0, in_t - cache_r - cache_w)
    cost = (
        regular_input * PRICING["input"] / 1_000_000
        + out_t        * PRICING["output"] / 1_000_000
        + cache_r      * PRICING["cache_read"] / 1_000_000
        + cache_w      * PRICING["cache_write"] / 1_000_000
    )
    return cost * 100


# ── Pair sourcing ────────────────────────────────────────────────

def _row_to_pair(row) -> dict:
    return {
        "pair_id": row[0],
        "producer_id_a": str(row[1]),
        "producer_id_b": str(row[2]),
        "name_a": row[3],
        "name_b": row[4],
        "country": row[5],
        "similarity": float(row[6]) if row[6] is not None else None,
        "wines_a": row[7],
        "wines_b": row[8],
        "signals": row[9],
        "l1_verdict": row[10],
        "l1_confidence": float(row[11]) if row[11] is not None else None,
        "l1_reasoning": row[12],
    }


def load_pairs(cur, args, method_name: str = "l2_haiku_rich") -> list[dict]:
    """Determine the pair set based on mode."""
    if args.pair_ids_file or args.calibration:
        if args.pair_ids_file:
            pair_ids = json.loads(Path(args.pair_ids_file).read_text(encoding="utf-8"))
        else:
            cal = json.loads(CALIBRATION_PATH.read_text(encoding="utf-8"))
            pair_ids = [p["pair_id"] for tier, pairs in cal["tiers"].items() for p in pairs]
        if not pair_ids:
            return []
        placeholders = ",".join(["%s"] * len(pair_ids))
        cur.execute(f"""
            SELECT b.id, b.producer_id_a, b.producer_id_b, b.name_a, b.name_b,
                   b.country, b.similarity, b.wines_a, b.wines_b, b.signals,
                   l.verdict, l.confidence, l.reasoning
            FROM producer_dedup_pairs b
            LEFT JOIN producer_dedup_pairs l
              ON l.producer_id_a = b.producer_id_a
             AND l.producer_id_b = b.producer_id_b
             AND l.method_name = 'l1_haiku_batch'
            WHERE b.method_name = 'blocking' AND b.id IN ({placeholders})
        """, pair_ids)
        pairs = [_row_to_pair(r) for r in cur.fetchall()]
        if args.resume:
            existing = set()
            cur.execute("""
                SELECT producer_id_a, producer_id_b
                FROM producer_dedup_pairs
                WHERE method_name=%s
            """, (method_name,))
            for a, b in cur.fetchall():
                existing.add((str(a), str(b)))
            pairs = [p for p in pairs if (p["producer_id_a"], p["producer_id_b"]) not in existing]
        return pairs[: args.limit] if args.limit else pairs

    # Production mode: filter by L1 verdict + confidence
    verdicts = args.l1_verdicts.split(",") if args.l1_verdicts else []
    conditions = []
    params = []
    if verdicts:
        conditions.append(f"l.verdict IN ({','.join(['%s'] * len(verdicts))})")
        params.extend(verdicts)
    if args.l1_conf_max is not None:
        conditions.append("(l.confidence < %s OR l.verdict = 'UNCERTAIN')")
        params.append(args.l1_conf_max)
    if args.l1_conf_min is not None:
        conditions.append("l.confidence >= %s")
        params.append(args.l1_conf_min)
    if args.resume:
        conditions.append("""NOT EXISTS (
            SELECT 1 FROM producer_dedup_pairs ddp2
            WHERE ddp2.producer_id_a = l.producer_id_a
              AND ddp2.producer_id_b = l.producer_id_b
              AND ddp2.method_name = %s
        )""")
        params.append(method_name)
    where = " AND ".join(conditions) if conditions else "1=1"

    cur.execute(f"""
        SELECT b.id, b.producer_id_a, b.producer_id_b, b.name_a, b.name_b,
               b.country, b.similarity, b.wines_a, b.wines_b, b.signals,
               l.verdict, l.confidence, l.reasoning
        FROM producer_dedup_pairs b
        JOIN producer_dedup_pairs l
          ON l.producer_id_a = b.producer_id_a
         AND l.producer_id_b = b.producer_id_b
         AND l.method_name = 'l1_haiku_batch'
        WHERE b.method_name = 'blocking' AND {where}
        ORDER BY b.id
        LIMIT %s
    """, (*params, args.limit or 10**9))
    return [_row_to_pair(r) for r in cur.fetchall()]


# ── Main ─────────────────────────────────────────────────────────

def main() -> int:
    if os.name == "nt":
        import io
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
        sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

    ap = argparse.ArgumentParser()
    ap.add_argument("--calibration", action="store_true",
                    help="run on pairs in calibration_set.json")
    ap.add_argument("--execute", action="store_true")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--limit", type=int, default=None)
    ap.add_argument("--budget", type=float, default=2.0)
    ap.add_argument("--batch-size", type=int, default=5)
    ap.add_argument("--workers", type=int, default=1)
    ap.add_argument("--l1-verdicts", default=None,
                    help="comma-sep L1 verdicts to include (prod mode)")
    ap.add_argument("--l1-conf-max", type=float, default=None,
                    help="include pairs where L1 conf < this (prod mode)")
    ap.add_argument("--l1-conf-min", type=float, default=None)
    ap.add_argument("--resume", action="store_true", default=True)
    ap.add_argument("--no-resume", dest="resume", action="store_false")
    ap.add_argument("--print-sample", type=int, default=5)
    ap.add_argument("--pair-ids-file", default=None,
                    help="JSON file with flat list of pair_ids to classify")
    ap.add_argument("--method-name", default="l2_haiku_rich",
                    help="override method_name for produced rows")
    args = ap.parse_args()

    if not (args.execute or args.dry_run):
        print("Specify --execute or --dry-run", file=sys.stderr)
        return 2

    section_11 = load_section_11()
    preamble = build_static_preamble(section_11)
    print(f"Preamble: {len(preamble)} chars (~{len(preamble)//4} tokens est.)")
    print(f"method_name: {args.method_name}")

    conn = get_conn()
    conn.autocommit = False
    cur = conn.cursor()

    pairs = load_pairs(cur, args, args.method_name)
    mode = "pair_ids_file" if args.pair_ids_file else ("calibration" if args.calibration else "production")
    print(f"Sampled {len(pairs)} pairs (mode={mode})")
    if not pairs:
        cur.close(); conn.close()
        return 0

    all_pids = list({p["producer_id_a"] for p in pairs} | {p["producer_id_b"] for p in pairs})
    print(f"Loading context for {len(all_pids)} distinct producers...")
    t0 = time.time()
    ttb = load_ttb_fingerprints(cur, all_pids)
    wines = load_wine_catalog(cur, all_pids, cap_per_producer=20)
    meta = load_producer_meta(cur, all_pids)
    ext_ids = load_external_ids(cur, all_pids)
    aliases = load_aliases(cur, all_pids)
    print(f"  TTB={len(ttb)} wines={len(wines)} meta={len(meta)} extIds={len(ext_ids)} aliases={len(aliases)} / {time.time()-t0:.1f}s")

    client = anthropic.Anthropic(api_key=get_env("ANTHROPIC_API_KEY"))

    if args.dry_run:
        batch = pairs[: args.batch_size]
        blocks = "\n".join(
            format_pair_context(p, ttb, wines, meta, ext_ids, aliases) for p in batch
        )
        print("\n--- PREAMBLE HEAD ---")
        print(preamble[:1500])
        print("\n--- USER MESSAGE ---")
        print(blocks[:5000])
        return 0

    total_cost_cents = 0.0
    total_calls = 0
    verdict_counts = {"MERGE": 0, "PARENT_CHILD": 0, "SKIP": 0, "UNCERTAIN": 0, "UNKNOWN": 0}
    conf_sums = {"MERGE": 0.0, "PARENT_CHILD": 0.0, "SKIP": 0.0, "UNCERTAIN": 0.0}
    sample_results = []

    batches = [pairs[i:i+args.batch_size] for i in range(0, len(pairs), args.batch_size)]
    state_lock = Lock()
    abort = {"a": False}

    def process(idx: int, batch: list):
        if abort["a"]:
            return idx, batch, [], {}, 0.0, None, "abort"
        blocks = "\n".join(
            format_pair_context(p, ttb, wines, meta, ext_ids, aliases) for p in batch
        )
        try:
            verdicts, usage = call_l2(client, preamble, blocks)
            cost = compute_cost_cents(usage)
            return idx, batch, verdicts, usage, cost, blocks, None
        except Exception as e:
            return idx, batch, [], {}, 0.0, blocks, str(e)[:300]

    t_start = time.time()
    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        futures = {ex.submit(process, i, b): i for i, b in enumerate(batches)}
        for fut in as_completed(futures):
            idx, batch, verdicts, usage, cost, blocks, err = fut.result()
            if abort["a"]:
                continue
            if err:
                print(f"  batch {idx+1}: ERROR: {err}", flush=True)
                continue
            with state_lock:
                total_cost_cents += cost
                total_calls += 1
                if total_cost_cents / 100 > args.budget:
                    print(f"\n!! Budget exceeded. Aborting.", flush=True)
                    abort["a"] = True

            verdicts_by_id = {int(v["pair_id"]): v for v in verdicts if "pair_id" in v}
            for_write = []
            for p in batch:
                v = verdicts_by_id.get(p["pair_id"])
                if not v:
                    v = {"verdict": "UNCERTAIN", "confidence": 0.0,
                         "reasoning": "MISSING in model output"}
                vk = v.get("verdict", "UNKNOWN")
                with state_lock:
                    if vk not in verdict_counts:
                        verdict_counts["UNKNOWN"] += 1
                    else:
                        verdict_counts[vk] += 1
                        try:
                            conf_sums[vk] += float(v.get("confidence") or 0)
                        except (TypeError, ValueError):
                            pass
                per_pair_cost = cost / max(1, len(batch))
                for_write.append((p, v, per_pair_cost))
                with state_lock:
                    if len(sample_results) < args.print_sample:
                        sample_results.append((p, v))

            if args.execute:
                with state_lock:
                    write_l2(cur, for_write, args.method_name)
                    conn.commit()

            processed = sum(verdict_counts.values())
            elapsed = time.time() - t_start
            eta = (elapsed / max(1, processed)) * (len(pairs) - processed)
            print(f"  b{idx+1:>4}/{len(batches)}: {len(batch)} pairs "
                  f"in={usage.get('input_tokens',0):>5} "
                  f"(cr={usage.get('cache_read_tokens',0):>5} cw={usage.get('cache_write_tokens',0):>4}) "
                  f"out={usage.get('output_tokens',0):>4} | "
                  f"{cost:.2f}c | processed={processed}/{len(pairs)} | "
                  f"${total_cost_cents/100:.2f} | eta={eta/60:.0f}m", flush=True)

    print("\n" + "=" * 60)
    print("L2 RESULTS")
    print("=" * 60)
    print(f"Pairs processed: {sum(verdict_counts.values())}")
    print(f"Total cost:      ${total_cost_cents/100:.3f}")
    if sum(verdict_counts.values()):
        print(f"Avg cost/pair:   {total_cost_cents/sum(verdict_counts.values()):.3f}c")
    for v, n in verdict_counts.items():
        if n:
            avg_c = conf_sums.get(v, 0) / n if v in conf_sums else 0
            print(f"  {v:<15} {n:>5}  (conf {avg_c:.2f})")

    # Sample reasoning
    for p, v in sample_results:
        print(f"\n  [{p['pair_id']}] {p['name_a']!r:<35}/{p['name_b']!r:<35}")
        print(f"     verdict: {v.get('verdict')}, conf: {v.get('confidence')}")
        print(f"     reasoning: {(v.get('reasoning') or '')[:200]}")

    cur.close()
    conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
