"""Haiku 4.5 rich + Serper-grounded producer-pair classifier.

For each pair, pre-fetches 2 Serper searches (one per producer name) and
injects the web snippets into the rich L2 prompt. Much cheaper than L3
Sonnet + Anthropic web_search (~$0.005/pair vs $0.147/pair).

Run:
    python -m pipeline.identity.producer_dedup_l2_web \
        --pair-ids-file data/sprints/dedup/<ids>.json \
        --method-name l2_haiku_rich_web \
        --execute --budget 10 --workers 6
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
from pipeline.lib.serper import producer_context, credits_used
from pipeline.identity.producer_dedup_l2 import (
    load_section_11,
    load_ttb_fingerprints,
    load_wine_catalog,
    load_producer_meta,
    load_external_ids,
    load_aliases,
    format_pair_context,
    OUTPUT_SCHEMA,
    FEW_SHOT,
    PRICING,
    compute_cost_cents,
)


CALIBRATION_PATH = Path(__file__).resolve().parents[2] / "data" / "sprints" / "dedup" / "calibration_set.json"


L2_WEB_INSTRUCTIONS = """You are the L2 rich-context classifier WITH web-search grounding. Each pair
comes with the full L2 context (TTB, wines, metadata) PLUS fresh Google search
snippets for both producer names. Apply IDENTITY_RULES §11 strictly.

Use the web evidence aggressively:
- If the two producers show DIFFERENT official websites → strong SKIP signal
  (same brand → one website, usually)
- If the web results converge on the same Wikipedia entry or entity → strong MERGE signal
- If web results disambiguate patterns like "X" vs "X & Y" (collaboration),
  "Winery X" vs "Winery Y (shared surname)" — use them to SKIP correctly
- Merchant/importer patterns: if A is a wine merchant page (BBR, Kermit Lynch,
  Berry Bros) and B is an estate website, they are SEPARATE producers even
  when the merchant name bundles the estate. SKIP.
- Négociant bottlings of the same source (Hospices de Beaune pattern):
  different négociant → SKIP; same estate with spelling variant → MERGE.

Tune confidence:
- Web evidence converges with cross-family verdict → confidence >=0.95
- Web evidence resolves a prior ambiguity → confidence 0.88-0.95
- Web evidence is thin or conflicting → 0.75-0.85; UNCERTAIN at <0.75
"""


def build_preamble(section_11: str) -> str:
    return f"""You are the L2 rich+web classifier for producer-pair duplicates. Apply
IDENTITY_RULES Section 11 strictly.

{section_11}

{OUTPUT_SCHEMA}

{L2_WEB_INSTRUCTIONS}

{FEW_SHOT}
"""


def fetch_web_context(name_a: str, name_b: str, country: str) -> str:
    """Fetch Serper snippets for both producers. Returns text block for prompt."""
    country_a = country_b = country.lower() if country and len(country) == 2 else None
    ra = producer_context(name_a, country=country_a, num=4)
    rb = producer_context(name_b, country=country_b, num=4)
    block = ["WEB EVIDENCE ─────"]
    block.append(f"A ({name_a!r}):")
    if ra.error:
        block.append(f"  [serper error: {ra.error}]")
    else:
        block.append(ra.as_snippet_block(max_organic=3, max_snippet_chars=180))
    block.append(f"\nB ({name_b!r}):")
    if rb.error:
        block.append(f"  [serper error: {rb.error}]")
    else:
        block.append(rb.as_snippet_block(max_organic=3, max_snippet_chars=180))
    block.append("─────")
    return "\n".join(block)


def _row_to_pair(row) -> dict:
    return {
        "pair_id": row[0],
        "producer_id_a": str(row[1]),
        "producer_id_b": str(row[2]),
        "name_a": row[3], "name_b": row[4], "country": row[5],
        "similarity": float(row[6]) if row[6] is not None else None,
        "wines_a": row[7], "wines_b": row[8], "signals": row[9],
        "l1_verdict": row[10],
        "l1_confidence": float(row[11]) if row[11] is not None else None,
        "l1_reasoning": row[12],
    }


def load_pairs(cur, args, method_name: str) -> list[dict]:
    if args.pair_ids_file:
        pair_ids = json.loads(Path(args.pair_ids_file).read_text(encoding="utf-8"))
    elif args.calibration:
        cal = json.loads(CALIBRATION_PATH.read_text(encoding="utf-8"))
        pair_ids = [p["pair_id"] for _, pairs in cal["tiers"].items() for p in pairs]
    else:
        print("Specify --pair-ids-file or --calibration", file=sys.stderr)
        sys.exit(2)

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
            FROM producer_dedup_pairs WHERE method_name=%s
        """, (method_name,))
        for a, b in cur.fetchall():
            existing.add((str(a), str(b)))
        pairs = [p for p in pairs if (p["producer_id_a"], p["producer_id_b"]) not in existing]
    return pairs[:args.limit] if args.limit else pairs


def call_haiku(client, preamble: str, user_block: str, max_retries: int = 3):
    for attempt in range(max_retries):
        try:
            resp = client.messages.create(
                model=HAIKU_MODEL,
                max_tokens=2000,
                system=[{"type": "text", "text": preamble,
                         "cache_control": {"type": "ephemeral", "ttl": "1h"}}],
                messages=[{"role": "user", "content": user_block}],
                temperature=0.0,
            )
            break
        except Exception as e:
            if attempt == max_retries - 1:
                raise
            time.sleep(2 ** attempt)
    raw = resp.content[0].text.strip()
    if raw.startswith("```"):
        raw = raw.split("\n", 1)[1] if "\n" in raw else raw[3:]
        if raw.endswith("```"): raw = raw[:-3].strip()
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        m = re.search(r"\{.*\}", raw, re.DOTALL)
        if not m:
            raise RuntimeError(f"Haiku JSON parse failed: {raw[:300]}")
        parsed = json.loads(m.group(0))
    if isinstance(parsed, list):
        parsed = parsed[0] if parsed else {"verdict": "UNCERTAIN", "confidence": 0.0}
    u = resp.usage
    usage = {
        "input_tokens": u.input_tokens, "output_tokens": u.output_tokens,
        "cache_read_tokens": getattr(u, "cache_read_input_tokens", 0) or 0,
        "cache_write_tokens": getattr(u, "cache_creation_input_tokens", 0) or 0,
    }
    return parsed, usage


def write_row(cur, pair, verdict, usage, web_cost_cents, method_name: str):
    llm_cost = compute_cost_cents(usage)
    total_cost_cents = llm_cost + web_cost_cents
    cur.execute("""
      INSERT INTO producer_dedup_pairs
        (producer_id_a, producer_id_b, name_a, name_b, country, similarity,
         wines_a, wines_b, method_name, verdict, confidence, reasoning,
         cost_cents, signals, web_evidence, created_at, updated_at)
      VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s, now(), now())
      ON CONFLICT (producer_id_a, producer_id_b, method_name) DO UPDATE SET
        verdict=EXCLUDED.verdict, confidence=EXCLUDED.confidence,
        reasoning=EXCLUDED.reasoning, cost_cents=EXCLUDED.cost_cents,
        web_evidence=EXCLUDED.web_evidence, updated_at=now()
    """, (
        pair["producer_id_a"], pair["producer_id_b"],
        pair["name_a"], pair["name_b"], pair["country"], pair["similarity"],
        pair["wines_a"], pair["wines_b"],
        method_name,
        verdict.get("verdict"), verdict.get("confidence"), verdict.get("reasoning"),
        total_cost_cents,
        json.dumps({"l1_verdict": pair.get("l1_verdict"),
                    "l1_confidence": pair.get("l1_confidence"),
                    "blocking_signals": pair.get("signals"),
                    "l2_web_model": HAIKU_MODEL,
                    "llm_cost_cents": llm_cost,
                    "web_cost_cents": web_cost_cents}, default=str),
        json.dumps({"tier": "serper_prefetch"}),
    ))


def main() -> int:
    if os.name == "nt":
        import io
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
        sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

    ap = argparse.ArgumentParser()
    ap.add_argument("--pair-ids-file", default=None)
    ap.add_argument("--calibration", action="store_true")
    ap.add_argument("--method-name", default="l2_haiku_rich_web")
    ap.add_argument("--execute", action="store_true")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--limit", type=int, default=None)
    ap.add_argument("--budget", type=float, default=10.0)
    ap.add_argument("--workers", type=int, default=4)
    ap.add_argument("--resume", action="store_true", default=True)
    ap.add_argument("--no-resume", dest="resume", action="store_false")
    args = ap.parse_args()

    if not (args.execute or args.dry_run):
        print("Specify --execute or --dry-run", file=sys.stderr); return 2

    section_11 = load_section_11()
    preamble = build_preamble(section_11)
    print(f"Preamble: {len(preamble)} chars | method_name: {args.method_name}")

    conn = get_conn(); conn.autocommit = False
    cur = conn.cursor()
    pairs = load_pairs(cur, args, args.method_name)
    print(f"Target {len(pairs)} pairs")
    if not pairs:
        cur.close(); conn.close(); return 0

    all_pids = list({p["producer_id_a"] for p in pairs} | {p["producer_id_b"] for p in pairs})
    print(f"Loading context for {len(all_pids)} producers...")
    t0 = time.time()
    ttb = load_ttb_fingerprints(cur, all_pids)
    wines = load_wine_catalog(cur, all_pids, cap_per_producer=20)
    meta = load_producer_meta(cur, all_pids)
    ext_ids = load_external_ids(cur, all_pids)
    aliases = load_aliases(cur, all_pids)
    print(f"  loaded in {time.time()-t0:.1f}s")

    client = anthropic.Anthropic(api_key=get_env("ANTHROPIC_API_KEY"))
    state_lock = Lock()
    abort = {"a": False}
    total_cents = 0.0
    verdict_counts = {"MERGE": 0, "PARENT_CHILD": 0, "SKIP": 0, "UNCERTAIN": 0, "UNKNOWN": 0}
    sample = []

    def process(pair):
        if abort["a"]: return pair, None, 0, "abort"
        ctx = format_pair_context(pair, ttb, wines, meta, ext_ids, aliases)
        web = fetch_web_context(pair["name_a"], pair["name_b"], pair.get("country") or "")
        user_block = f"Classify this producer pair for possible dedup.\n\n{ctx}\n\n{web}\n\nReturn ONLY a JSON object with keys: verdict, confidence, reasoning."
        try:
            v, usage = call_haiku(client, preamble, user_block)
            # Web cost = 2 queries × 0.1¢ (at $1/1K starter tier)
            web_cost_cents = 0.2
            return pair, (v, usage, web_cost_cents), 0, None
        except Exception as e:
            return pair, None, 0, str(e)[:200]

    t_start = time.time()
    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        futures = {ex.submit(process, p): p for p in pairs}
        for fut in as_completed(futures):
            pair, result, _, err = fut.result()
            if abort["a"]: continue
            if err:
                print(f"  ERR {pair['pair_id']}: {err}", flush=True)
                continue
            v, usage, web_cost_cents = result
            llm_cost_cents = compute_cost_cents(usage)
            pair_cost_cents = llm_cost_cents + web_cost_cents
            with state_lock:
                total_cents += pair_cost_cents
                if total_cents / 100 > args.budget:
                    print(f"!! Budget {args.budget} exceeded. Aborting.", flush=True)
                    abort["a"] = True
                vk = v.get("verdict", "UNKNOWN")
                verdict_counts[vk] = verdict_counts.get(vk, 0) + 1
                if len(sample) < 6:
                    sample.append((pair, v))
            if args.execute:
                with state_lock:
                    write_row(cur, pair, v, usage, web_cost_cents, args.method_name)
                    conn.commit()
            done = sum(verdict_counts.values())
            if done % 10 == 0:
                elapsed = time.time()-t_start
                eta = (elapsed/done)*(len(pairs)-done) if done else 0
                print(f"  {done}/{len(pairs)} | {v.get('verdict'):14} {v.get('confidence',0):.2f} | "
                      f"llm={llm_cost_cents:.2f}c web={web_cost_cents:.2f}c | "
                      f"${total_cents/100:.2f} total | eta={eta/60:.0f}m", flush=True)

    print("\n" + "="*60)
    print(f"L2 HAIKU + SERPER RESULTS")
    print("="*60)
    print(f"Pairs: {sum(verdict_counts.values())}")
    print(f"Serper credits used: {credits_used()}")
    print(f"Total cost: ${total_cents/100:.3f}")
    if sum(verdict_counts.values()):
        print(f"Avg/pair: {total_cents/sum(verdict_counts.values()):.2f}c")
    for k, n in verdict_counts.items():
        if n: print(f"  {k:<15} {n}")
    for p, v in sample:
        print(f"\n  [{p['pair_id']}] {p['name_a']!r} / {p['name_b']!r}")
        print(f"    verdict: {v.get('verdict')}, conf: {v.get('confidence')}")
        print(f"    reason: {(v.get('reasoning') or '')[:250]}")

    cur.close(); conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
