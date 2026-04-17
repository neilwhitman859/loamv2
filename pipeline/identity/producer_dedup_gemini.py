"""
B6.4 Phase C+F — Gemini 3 Flash Preview cross-check (basic or rich).

Classifies producer-pair duplicates via Google Gemini 3 Flash Preview through
OpenRouter. Designed to provide a SECOND-OPINION cross-check on Haiku's L1/L2
output — Haiku is cautious (low FPR, higher FNR) and Gemini is more aggressive
(higher FPR, lower FNR). Joint agreement collapses errors.

Two modes:
  --mode basic   — L1.5 equivalent, short prompt matching L1 structure.
                   Writes method_name='l1_gemini_basic_calibration' or 'l1_gemini_basic'.
  --mode rich    — L2.5 equivalent, rich prompt matching L2 structure.
                   Writes method_name='l2_gemini_rich_calibration' or 'l2_gemini_rich'.

Run on calibration set (batches against pair_ids in calibration_set.json):
    python -m pipeline.identity.producer_dedup_gemini --mode basic --calibration --execute

Run on production (filter by L1 verdict/conf band):
    python -m pipeline.identity.producer_dedup_gemini --mode basic --execute \
        --l1-verdicts MERGE,PARENT_CHILD,SKIP --l1-conf-min 0.92 --l1-conf-max 0.97
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

import requests

from pipeline.lib.db import get_conn, get_env


GEMINI_MODEL = "google/gemini-3-flash-preview"
OPENROUTER_URL = "https://openrouter.ai/api/v1/chat/completions"

IDENTITY_RULES_PATH = Path(__file__).resolve().parents[2] / "docs" / "IDENTITY_RULES.md"
CALIBRATION_PATH = Path(__file__).resolve().parents[2] / "data" / "sprints" / "dedup" / "calibration_set.json"


PRICING = {
    "input":   0.15,   # $/MTok
    "output":  0.60,   # $/MTok
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
  {"pair_id": <int>, "verdict": "MERGE"|"PARENT_CHILD"|"SKIP"|"UNCERTAIN",
   "confidence": <float 0.0-1.0>, "reasoning": "<1-3 sentences>"},
  ...
]
"""


# ── Basic prompt (L1.5 equivalent) ───────────────────────────────

BASIC_FEW_SHOT = """Examples:

Example 1 — MERGE (multi-strategy agreement):
Pair {id:1, A:"Ridge Vineyards" (US, 110 wines), B:"Ridge" (US, 2 wines), signals s2+s9+s6}
-> {"pair_id":1,"verdict":"MERGE","confidence":0.97,"reasoning":"Shared BW permit, substring containment, and trigram similarity — B is LWIN-import variant of A."}

Example 2 — SKIP (famous distinct-producers):
Pair {id:2, A:"Stag's Leap Wine Cellars" (US, 49), B:"Stags' Leap Winery" (US, 26), signals s2 0.72}
-> {"pair_id":2,"verdict":"SKIP","confidence":0.97,"reasoning":"Two distinct historical producers in the Stag's Leap District, Napa. Apostrophe-position genuinely different brand identities."}

Example 3 — PARENT_CHILD (shared permit, different brands):
Pair {id:3, A:"Silver Oak" (4 wines), B:"Twomey" (13 wines), signals s6_ttb BW-CA-5455}
-> {"pair_id":3,"verdict":"PARENT_CHILD","confidence":0.92,"reasoning":"Same Duncan-family facility (BW-CA-5455) but distinct label brands producing entirely different wine portfolios. Classic sister-winery pattern."}
"""


BASIC_INSTRUCTIONS = """You are the L1.5 basic-prompt cross-check classifier. Apply IDENTITY_RULES
§11 strictly. You are being used to provide a SECOND OPINION on Haiku's L1
classification — do NOT look at or defer to any other model's verdict.

Emit confidence >=0.90 only when you are near-certain. Use UNCERTAIN when
evidence is genuinely mixed, not as a convenience bucket.
"""


def build_basic_preamble(section_11: str) -> str:
    return f"""You are a producer-pair dedup classifier for a wine database.

{section_11}

{OUTPUT_SCHEMA}

{BASIC_INSTRUCTIONS}

{BASIC_FEW_SHOT}
"""


# ── Rich prompt (L2.5 equivalent) reuses L2's FEW_SHOT via import ──

def build_rich_preamble(section_11: str) -> str:
    # Import lazily to avoid circular deps
    from pipeline.identity.producer_dedup_l2 import FEW_SHOT as L2_FEW_SHOT, L2_INSTRUCTIONS
    return f"""You are a producer-pair dedup classifier for a wine database. You receive
RICH context per pair (full TTB fingerprint, up to 20 wines, external IDs,
producer metadata). Apply IDENTITY_RULES Section 11 strictly. You are the
SECOND-OPINION model cross-checking Haiku's L2 classification — do NOT defer
to any other model's verdict.

{section_11}

{OUTPUT_SCHEMA}

{L2_INSTRUCTIONS}

{L2_FEW_SHOT}
"""


# ── Pair context loaders ──────────────────────────────────────────

def load_ttb_fingerprints(cur, producer_ids, rich: bool = False) -> dict:
    if not producer_ids:
        return {}
    placeholders = ",".join(["%s"] * len(producer_ids))
    cur.execute(f"""
        SELECT canonical_producer_id,
               COALESCE(permit_no, permit_number) AS permit,
               applicant_name, applicant_address, applicant_city, applicant_state,
               brand_name
        FROM source_ttb_colas
        WHERE canonical_producer_id IN ({placeholders})
          AND (permit_no IS NOT NULL OR permit_number IS NOT NULL)
        LIMIT 50000
    """, producer_ids)
    per = {}
    for pid, permit, nm, addr, city, state, brand in cur.fetchall():
        d = per.setdefault(pid, {"bw_permits": set(), "permittees": set(),
                                 "addresses": set(), "brand_names": set(),
                                 "cola_count": 0})
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

    cap_permit = None if rich else 5
    cap_permittee = None if rich else 3
    cap_addr = None if rich else 3
    cap_brand = None if rich else 10
    result = {}
    for pid, d in per.items():
        result[pid] = {
            "bw_permits": sorted(d["bw_permits"])[:cap_permit] if cap_permit else sorted(d["bw_permits"]),
            "permittees": sorted(d["permittees"])[:cap_permittee] if cap_permittee else sorted(d["permittees"]),
            "addresses": sorted(d["addresses"])[:cap_addr] if cap_addr else sorted(d["addresses"]),
            "brand_names": sorted(d["brand_names"])[:cap_brand] if cap_brand else sorted(d["brand_names"]),
            "cola_count": d["cola_count"],
        }
    return result


def load_wine_samples(cur, producer_ids, cap: int = 5) -> dict:
    if not producer_ids:
        return {}
    placeholders = ",".join(["%s"] * len(producer_ids))
    cur.execute(f"""
        WITH ranked AS (
          SELECT producer_id, COALESCE(display_name, name) AS nm,
                 ROW_NUMBER() OVER (
                   PARTITION BY producer_id
                   ORDER BY length(COALESCE(display_name, name, '')) DESC
                 ) AS rn
          FROM wines
          WHERE producer_id IN ({placeholders}) AND deleted_at IS NULL
        )
        SELECT producer_id, nm FROM ranked WHERE rn <= %s
    """, producer_ids + [cap])
    per = {}
    for pid, nm in cur.fetchall():
        per.setdefault(pid, []).append(nm)
    return per


def load_lwin_counts(cur, producer_ids) -> dict:
    if not producer_ids:
        return {}
    placeholders = ",".join(["%s"] * len(producer_ids))
    cur.execute(f"""
        SELECT w.producer_id, COUNT(DISTINCT ei.external_id)
        FROM wines w
        JOIN external_ids ei
          ON ei.entity_type='wine' AND ei.entity_id=w.id AND ei.system='lwin_7'
        WHERE w.producer_id IN ({placeholders}) AND w.deleted_at IS NULL
        GROUP BY w.producer_id
    """, producer_ids)
    return {pid: n for pid, n in cur.fetchall()}


def load_producer_meta(cur, producer_ids) -> dict:
    if not producer_ids:
        return {}
    placeholders = ",".join(["%s"] * len(producer_ids))
    cur.execute(f"""
        SELECT p.id, p.metadata, p.year_established, p.parent_producer_id,
               r.name AS region_name, c.iso_code AS country_code
        FROM producers p
        LEFT JOIN regions r ON r.id=p.region_id
        LEFT JOIN countries c ON c.id=p.country_id
        WHERE p.id IN ({placeholders})
    """, producer_ids)
    per = {}
    for pid, md, year, parent_id, rname, ccode in cur.fetchall():
        site = None
        if md and isinstance(md, dict):
            site = md.get("website") or md.get("url") or md.get("homepage")
        per[pid] = {
            "website": site,
            "region_name": rname,
            "country_code": ccode,
            "year_established": year,
            "parent_producer_id": str(parent_id) if parent_id else None,
        }
    return per


def load_external_ids(cur, producer_ids) -> dict:
    if not producer_ids:
        return {}
    placeholders = ",".join(["%s"] * len(producer_ids))
    cur.execute(f"""
        SELECT entity_id, system, external_id
        FROM external_ids
        WHERE entity_type='producer' AND entity_id IN ({placeholders})
    """, producer_ids)
    per = {}
    for pid, s, e in cur.fetchall():
        per.setdefault(str(pid), {}).setdefault(s, []).append(e)
    for pid, sys_map in per.items():
        for s in sys_map:
            sys_map[s] = sys_map[s][:5]
    return per


# ── Pair formatter ────────────────────────────────────────────────

def format_pair_basic(pair: dict, ttb: dict, wines: dict, lwin: dict) -> str:
    pid = pair["pair_id"]
    a = pair["producer_id_a"]
    b = pair["producer_id_b"]
    wa = wines.get(a, [])[:5]
    wb = wines.get(b, [])[:5]
    ta = ttb.get(a, {})
    tb = ttb.get(b, {})
    la = lwin.get(a, 0)
    lb = lwin.get(b, 0)

    def ttb_blob(t):
        if not t:
            return "(none)"
        p = []
        if t.get("bw_permits"): p.append(f"BW: {t['bw_permits']}")
        if t.get("permittees"): p.append(f"Permittees: {t['permittees']}")
        if t.get("brand_names"): p.append(f"Brands: {t['brand_names']}")
        p.append(f"cola_count: {t.get('cola_count', 0)}")
        return "; ".join(p)

    return f"""--- PAIR {pid} ---
A: {pair['name_a']!r} [{pair['country']}] wines={pair['wines_a']} ({la} LWIN) top_wines={wa}
A TTB: {ttb_blob(ta)}
B: {pair['name_b']!r} [{pair['country']}] wines={pair['wines_b']} ({lb} LWIN) top_wines={wb}
B TTB: {ttb_blob(tb)}
signals: {json.dumps(pair.get('signals') or {}, default=str)}
"""


def format_pair_rich(pair: dict, ttb: dict, wines: dict, meta: dict,
                      ext_ids: dict) -> str:
    from pipeline.identity.producer_dedup_l2 import format_pair_context as l2_format
    # Approximate L2 format without aliases
    return l2_format(pair, ttb, wines, meta, ext_ids, {})


# ── OpenRouter call ───────────────────────────────────────────────

def call_gemini(preamble: str, batch_block: str, api_key: str, model: str,
                dry_run: bool = False, timeout: int = 120) -> tuple[list, dict, str]:
    if dry_run:
        return [], {}, None
    n = len(re.findall(r"--- PAIR", batch_block))
    user = f"Classify the {n} pairs below. Return ONLY a JSON array as specified.\n\n{batch_block}"
    payload = {
        "model": model,
        "messages": [
            {"role": "system", "content": preamble},
            {"role": "user", "content": user},
        ],
        "max_tokens": 3500,
        "temperature": 0.0,
    }
    r = requests.post(
        OPENROUTER_URL,
        headers={"Authorization": f"Bearer {api_key}",
                  "Content-Type": "application/json",
                  "HTTP-Referer": "https://loam.onrender.com",
                  "X-Title": "Loam B6.4 producer dedup"},
        json=payload,
        timeout=timeout,
    )
    r.raise_for_status()
    j = r.json()
    raw = j["choices"][0]["message"]["content"].strip()
    # Handle fenced code
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
            raise RuntimeError(f"Gemini JSON parse failed. First 400 chars: {raw[:400]}")
    usage = j.get("usage", {})
    return parsed, usage, raw


def compute_cost_cents(usage: dict) -> float:
    in_t = usage.get("prompt_tokens", 0)
    out_t = usage.get("completion_tokens", 0)
    cost = in_t * PRICING["input"] / 1_000_000 + out_t * PRICING["output"] / 1_000_000
    return cost * 100


# ── Pair sourcing + DB write ──────────────────────────────────────

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


def load_pairs(cur, args, method_name: str) -> list[dict]:
    if args.calibration:
        cal = json.loads(CALIBRATION_PATH.read_text(encoding="utf-8"))
        pair_ids = [p["pair_id"] for tier, pairs in cal["tiers"].items() for p in pairs]
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
            WHERE b.method_name='blocking' AND b.id IN ({placeholders})
        """, pair_ids)
        pairs = [_row_to_pair(r) for r in cur.fetchall()]
    else:
        verdicts = args.l1_verdicts.split(",") if args.l1_verdicts else []
        conds, params = [], []
        if verdicts:
            conds.append(f"l.verdict IN ({','.join(['%s'] * len(verdicts))})")
            params.extend(verdicts)
        if args.l1_conf_min is not None:
            conds.append("l.confidence >= %s")
            params.append(args.l1_conf_min)
        if args.l1_conf_max is not None:
            conds.append("l.confidence < %s")
            params.append(args.l1_conf_max)
        where = " AND ".join(conds) if conds else "1=1"
        cur.execute(f"""
            SELECT b.id, b.producer_id_a, b.producer_id_b, b.name_a, b.name_b,
                   b.country, b.similarity, b.wines_a, b.wines_b, b.signals,
                   l.verdict, l.confidence, l.reasoning
            FROM producer_dedup_pairs b
            JOIN producer_dedup_pairs l
              ON l.producer_id_a = b.producer_id_a
             AND l.producer_id_b = b.producer_id_b
             AND l.method_name='l1_haiku_batch'
            WHERE b.method_name='blocking' AND {where}
            ORDER BY b.id
            LIMIT %s
        """, (*params, args.limit or 10**9))
        pairs = [_row_to_pair(r) for r in cur.fetchall()]

    if args.resume:
        cur.execute("""
            SELECT producer_id_a, producer_id_b FROM producer_dedup_pairs
            WHERE method_name=%s
        """, (method_name,))
        done = {(str(a), str(b)) for a, b in cur.fetchall()}
        pairs = [p for p in pairs if (p["producer_id_a"], p["producer_id_b"]) not in done]
    return pairs[: args.limit] if args.limit else pairs


def write_results(cur, pair_verdicts, method_name: str):
    for pair, v, cost_cents in pair_verdicts:
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
            v.get("verdict"), v.get("confidence"), v.get("reasoning"),
            cost_cents,
            json.dumps({"l1_verdict": pair.get("l1_verdict"),
                        "l1_confidence": pair.get("l1_confidence"),
                        "blocking_signals": pair.get("signals"),
                        "gemini_model": GEMINI_MODEL}, default=str),
        ))


def main() -> int:
    if os.name == "nt":
        import io
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
        sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", choices=["basic", "rich"], required=True)
    ap.add_argument("--calibration", action="store_true")
    ap.add_argument("--execute", action="store_true")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--limit", type=int, default=None)
    ap.add_argument("--budget", type=float, default=3.0)
    ap.add_argument("--batch-size", type=int, default=5)
    ap.add_argument("--workers", type=int, default=4)
    ap.add_argument("--l1-verdicts", default=None)
    ap.add_argument("--l1-conf-min", type=float, default=None)
    ap.add_argument("--l1-conf-max", type=float, default=None)
    ap.add_argument("--resume", action="store_true", default=True)
    ap.add_argument("--no-resume", dest="resume", action="store_false")
    ap.add_argument("--method-name", default=None,
                    help="override default method_name; useful for distinguishing calibration vs production")
    args = ap.parse_args()

    if not (args.execute or args.dry_run):
        print("Specify --execute or --dry-run", file=sys.stderr)
        return 2

    # Method name selection
    if args.method_name:
        method_name = args.method_name
    elif args.mode == "basic":
        method_name = "l1_gemini_basic_calibration" if args.calibration else "l1_gemini_basic"
    else:
        method_name = "l2_gemini_rich_calibration" if args.calibration else "l2_gemini_rich"
    print(f"Mode: {args.mode} | method_name: {method_name}")

    section_11 = load_section_11()
    if args.mode == "basic":
        preamble = build_basic_preamble(section_11)
    else:
        preamble = build_rich_preamble(section_11)
    print(f"Preamble: {len(preamble)} chars")

    conn = get_conn()
    conn.autocommit = False
    cur = conn.cursor()

    pairs = load_pairs(cur, args, method_name)
    print(f"Target {len(pairs)} pairs")
    if not pairs:
        cur.close(); conn.close()
        return 0

    all_pids = list({p["producer_id_a"] for p in pairs} | {p["producer_id_b"] for p in pairs})
    rich = args.mode == "rich"
    print(f"Loading context for {len(all_pids)} producers...")
    t0 = time.time()
    ttb = load_ttb_fingerprints(cur, all_pids, rich=rich)
    wines = load_wine_samples(cur, all_pids, cap=20 if rich else 5)
    lwin = load_lwin_counts(cur, all_pids)
    meta = load_producer_meta(cur, all_pids) if rich else {}
    ext_ids = load_external_ids(cur, all_pids) if rich else {}
    print(f"  loaded / {time.time()-t0:.1f}s")

    api_key = get_env("OPENROUTER_API_KEY")

    if args.dry_run:
        batch = pairs[: args.batch_size]
        if args.mode == "basic":
            blocks = "\n".join(format_pair_basic(p, ttb, wines, lwin) for p in batch)
        else:
            blocks = "\n".join(format_pair_rich(p, ttb, wines, meta, ext_ids) for p in batch)
        print("\n--- PREAMBLE HEAD ---")
        print(preamble[:1200])
        print("\n--- USER ---")
        print(blocks[:5000])
        return 0

    total_cost_cents = 0.0
    total_calls = 0
    verdict_counts = {"MERGE": 0, "PARENT_CHILD": 0, "SKIP": 0, "UNCERTAIN": 0, "UNKNOWN": 0}
    conf_sums = {"MERGE": 0.0, "PARENT_CHILD": 0.0, "SKIP": 0.0, "UNCERTAIN": 0.0}

    batches = [pairs[i:i+args.batch_size] for i in range(0, len(pairs), args.batch_size)]
    state_lock = Lock()
    abort = {"a": False}
    sample_results = []

    def process(idx: int, batch: list):
        if abort["a"]:
            return idx, batch, [], {}, 0.0, "abort"
        if args.mode == "basic":
            blocks = "\n".join(format_pair_basic(p, ttb, wines, lwin) for p in batch)
        else:
            blocks = "\n".join(format_pair_rich(p, ttb, wines, meta, ext_ids) for p in batch)
        try:
            verdicts, usage, _ = call_gemini(preamble, blocks, api_key, GEMINI_MODEL)
            cost = compute_cost_cents(usage)
            return idx, batch, verdicts, usage, cost, None
        except Exception as e:
            return idx, batch, [], {}, 0.0, str(e)[:300]

    t_start = time.time()
    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        futures = {ex.submit(process, i, b): i for i, b in enumerate(batches)}
        for fut in as_completed(futures):
            idx, batch, verdicts, usage, cost, err = fut.result()
            if abort["a"]:
                continue
            if err and err != "abort":
                print(f"  b{idx+1}: ERR {err}", flush=True)
                continue
            with state_lock:
                total_cost_cents += cost
                total_calls += 1
                if total_cost_cents / 100 > args.budget:
                    print(f"\n!! Budget {args.budget} exceeded. Aborting.", flush=True)
                    abort["a"] = True

            verdicts_by_id = {int(v["pair_id"]): v for v in verdicts if "pair_id" in v}
            for_write = []
            for p in batch:
                v = verdicts_by_id.get(p["pair_id"])
                if not v:
                    v = {"verdict": "UNCERTAIN", "confidence": 0.0,
                         "reasoning": "MISSING in Gemini output"}
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
                for_write.append((p, v, cost / max(1, len(batch))))
                with state_lock:
                    if len(sample_results) < 5:
                        sample_results.append((p, v))

            if args.execute:
                with state_lock:
                    write_results(cur, for_write, method_name)
                    conn.commit()

            processed = sum(verdict_counts.values())
            elapsed = time.time() - t_start
            eta = (elapsed / max(1, processed)) * (len(pairs) - processed)
            print(f"  b{idx+1:>4}/{len(batches)}: {len(batch)} pairs "
                  f"in={usage.get('prompt_tokens',0)} out={usage.get('completion_tokens',0)} | "
                  f"{cost:.2f}c | processed={processed}/{len(pairs)} | "
                  f"${total_cost_cents/100:.2f} | eta={eta/60:.0f}m", flush=True)

    print("\n" + "=" * 60)
    print(f"GEMINI {args.mode.upper()} RESULTS")
    print("=" * 60)
    print(f"Pairs processed: {sum(verdict_counts.values())}")
    print(f"Total cost:      ${total_cost_cents/100:.3f}")
    if sum(verdict_counts.values()):
        print(f"Avg cost/pair:   {total_cost_cents/sum(verdict_counts.values()):.3f}c")
    for v, n in verdict_counts.items():
        if n:
            avg_c = conf_sums.get(v, 0) / n if v in conf_sums else 0
            print(f"  {v:<15} {n:>5}  (conf {avg_c:.2f})")

    for p, v in sample_results:
        print(f"\n  [{p['pair_id']}] {p['name_a']!r:<35}/{p['name_b']!r:<35}")
        print(f"     verdict: {v.get('verdict')}, conf: {v.get('confidence')}")
        print(f"     reasoning: {(v.get('reasoning') or '')[:200]}")

    cur.close(); conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
