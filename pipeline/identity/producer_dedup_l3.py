"""
B6.4 Phase G — L3 Sonnet 4.6 + Anthropic native web_search_20250305.

Rigor-tier classifier. Runs on pairs escalated from L2 (UNCERTAIN or low
confidence after rich prompt) + L1.5/L2.5 Gemini disagreements. Each pair
gets its own web searches; not batched.

Writes to producer_dedup_pairs with method_name='l3_sonnet_web' (or
'l3_sonnet_web_calibration' when --calibration).

Run (calibration):
    python -m pipeline.identity.producer_dedup_l3 --calibration --execute \
        --budget 10 --max-searches 3

Run (production, from DB filter):
    python -m pipeline.identity.producer_dedup_l3 --execute --budget 45 \
        --l2-verdicts UNCERTAIN,MERGE,PARENT_CHILD --l2-conf-max 0.90

Run ablation test (no web):
    python -m pipeline.identity.producer_dedup_l3 --calibration --execute \
        --limit 50 --no-web --method-name l3_sonnet_noweb_ablation
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
from pipeline.lib.models import SONNET_MODEL


IDENTITY_RULES_PATH = Path(__file__).resolve().parents[2] / "docs" / "IDENTITY_RULES.md"
CALIBRATION_PATH = Path(__file__).resolve().parents[2] / "data" / "sprints" / "dedup" / "calibration_set.json"


PRICING = {
    "input":       3.00,
    "output":     15.00,
    "cache_read":  0.30,
    "cache_write": 3.75,
    "web_search": 10.00,  # $/1K queries
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


L3_OUTPUT_SCHEMA = """OUTPUT FORMAT — at the end of your response, emit a single JSON object (not an
array), no markdown fences:

{
  "verdict":    "MERGE" | "PARENT_CHILD" | "SKIP" | "UNCERTAIN",
  "confidence": <float 0.0-1.0>,
  "reasoning":  "<2-5 sentences citing the specific web evidence you used>",
  "web_evidence": [
    {"url": "<URL>", "title": "<page title>", "why": "<what it proved>"},
    ...
  ]
}

The JSON object MUST be the last thing in your response.
"""


L3_INSTRUCTIONS = """You are the L3 rigor-tier classifier. You have access to web_search (or
not — see note below). Use up to 3 searches per pair to confirm or contradict
the L1/L2 verdicts we show. Prefer producer-site + Wikipedia + Wine-Searcher +
Liv-ex/LWIN record.

Confidence guidance:
- If web evidence unambiguously confirms same entity → 0.95+ MERGE
- If evidence clearly distinguishes two entities → 0.92+ SKIP
- If docs point to sister-brand / ownership relation without label consolidation → PARENT_CHILD 0.88+
- If web searches surface too little on both → UNCERTAIN 0.55-0.70 (do not over-confidently SKIP)

You may DISAGREE with L1 and L2. If you do, explain why in reasoning.
Always cite evidence via web_evidence. If --no-web is in effect you must
classify on DB signals alone; lower your confidence accordingly (max 0.85).
"""


def build_preamble(section_11: str, use_web: bool) -> str:
    web_note = (
        "Use the web_search tool freely; prefer 2-3 high-quality searches over 5 shallow ones."
        if use_web
        else "The web_search tool is DISABLED for this call — classify on DB signals only."
    )
    return f"""You are the L3 rigor-tier oracle for producer-pair dedup in a wine database.

{section_11}

{L3_OUTPUT_SCHEMA}

{L3_INSTRUCTIONS}

{web_note}
"""


def parse_response(content_blocks):
    text = ""
    evidence = []
    n_searches = 0
    for b in content_blocks:
        t = getattr(b, "type", None)
        if t == "text":
            text += b.text
        elif t == "web_search_tool_result":
            for r in (b.content or []):
                if getattr(r, "type", None) == "web_search_result":
                    evidence.append({
                        "url": getattr(r, "url", None),
                        "title": getattr(r, "title", None),
                    })
        elif t == "server_tool_use":
            n_searches += 1
    if not text.strip():
        return {"verdict": "UNCERTAIN", "confidence": 0.0,
                "reasoning": "No text response"}, evidence, n_searches
    # Find last JSON object
    obj = None
    for m in reversed(list(re.finditer(r"\{(?:[^{}]|\{[^{}]*\})*\}", text, re.DOTALL))):
        try:
            parsed = json.loads(m.group(0))
            if "verdict" in parsed:
                obj = parsed
                break
        except json.JSONDecodeError:
            continue
    if not obj:
        return {"verdict": "UNCERTAIN", "confidence": 0.0,
                "reasoning": f"No JSON parsed from response. First 400: {text[:400]}"}, evidence, n_searches
    return obj, evidence, n_searches


def compute_cost_cents(usage, n_searches):
    if not usage:
        return 0.0
    in_t = usage.input_tokens
    out_t = usage.output_tokens
    cache_r = getattr(usage, "cache_read_input_tokens", 0) or 0
    cache_w = getattr(usage, "cache_creation_input_tokens", 0) or 0
    reg = max(0, in_t - cache_r - cache_w)
    cost = (reg * PRICING["input"] / 1_000_000
            + out_t * PRICING["output"] / 1_000_000
            + cache_r * PRICING["cache_read"] / 1_000_000
            + cache_w * PRICING["cache_write"] / 1_000_000
            + n_searches * PRICING["web_search"] / 1000)
    return cost * 100


# ── Context loaders (same as oracle) ──────────────────────────────

def load_context(cur, pair):
    from pipeline.identity.calibration_oracle import load_context as lc
    return lc(cur, pair)


def format_prompt(pair, ctx):
    from pipeline.identity.calibration_oracle import format_pair_prompt
    # Ensure 'tier' key exists (calibration_oracle.format_pair_prompt requires it)
    pair = dict(pair)
    pair.setdefault("tier", "production")
    return format_pair_prompt(pair, ctx)


def call_l3(client, preamble, pair_prompt, max_searches=3, use_web=True):
    tools = []
    if use_web:
        tools = [{
            "type": "web_search_20250305",
            "name": "web_search",
            "max_uses": max_searches,
        }]
    resp = client.messages.create(
        model=SONNET_MODEL,
        max_tokens=4000,
        system=[{
            "type": "text",
            "text": preamble,
            "cache_control": {"type": "ephemeral"},
        }],
        tools=tools if tools else anthropic.NOT_GIVEN,
        messages=[{"role": "user", "content": pair_prompt}],
    )
    obj, evidence, n_searches = parse_response(resp.content)
    return obj, evidence, resp.usage, n_searches


def write_l3(cur, pair, verdict, evidence, cost_cents, method_name: str):
    cur.execute("""
      INSERT INTO producer_dedup_pairs
        (producer_id_a, producer_id_b, name_a, name_b, country, similarity,
         wines_a, wines_b, method_name, verdict, confidence, reasoning,
         cost_cents, signals, web_evidence, created_at, updated_at)
      VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s,
              %s, %s, %s, %s, %s, %s, now(), now())
      ON CONFLICT (producer_id_a, producer_id_b, method_name)
      DO UPDATE SET
        verdict = EXCLUDED.verdict,
        confidence = EXCLUDED.confidence,
        reasoning = EXCLUDED.reasoning,
        cost_cents = EXCLUDED.cost_cents,
        signals = EXCLUDED.signals,
        web_evidence = EXCLUDED.web_evidence,
        updated_at = now()
    """, (
        pair["producer_id_a"], pair["producer_id_b"],
        pair["name_a"], pair["name_b"], pair["country"], pair["similarity"],
        pair["wines_a"], pair["wines_b"],
        method_name,
        verdict.get("verdict"), verdict.get("confidence"), verdict.get("reasoning"),
        cost_cents,
        json.dumps({"l1_verdict": pair.get("l1_verdict"),
                    "l1_confidence": pair.get("l1_confidence"),
                    "l2_verdict": pair.get("l2_verdict"),
                    "l2_confidence": pair.get("l2_confidence"),
                    "blocking_signals": pair.get("signals"),
                    "l3_model": SONNET_MODEL}, default=str),
        json.dumps({"evidence": evidence,
                    "web_evidence_from_model": verdict.get("web_evidence") or []}, default=str),
    ))


# ── Pair sourcing ────────────────────────────────────────────────

def _row_to_pair(row):
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
        "l2_verdict": row[13] if len(row) > 13 else None,
        "l2_confidence": float(row[14]) if len(row) > 14 and row[14] is not None else None,
    }


def load_pairs(cur, args, method_name):
    if args.calibration:
        cal = json.loads(CALIBRATION_PATH.read_text(encoding="utf-8"))
        pair_ids = [p["pair_id"] for tier, pairs in cal["tiers"].items() for p in pairs]
        placeholders = ",".join(["%s"] * len(pair_ids))
        cur.execute(f"""
            SELECT b.id, b.producer_id_a, b.producer_id_b, b.name_a, b.name_b,
                   b.country, b.similarity, b.wines_a, b.wines_b, b.signals,
                   l.verdict, l.confidence, l.reasoning,
                   l2.verdict, l2.confidence
            FROM producer_dedup_pairs b
            LEFT JOIN producer_dedup_pairs l
              ON l.producer_id_a = b.producer_id_a
             AND l.producer_id_b = b.producer_id_b
             AND l.method_name='l1_haiku_batch'
            LEFT JOIN producer_dedup_pairs l2
              ON l2.producer_id_a = b.producer_id_a
             AND l2.producer_id_b = b.producer_id_b
             AND l2.method_name='l2_haiku_rich'
            WHERE b.method_name='blocking' AND b.id IN ({placeholders})
        """, pair_ids)
        pairs = [_row_to_pair(r) for r in cur.fetchall()]
    else:
        # Production: filter on L2 verdict/conf
        verdicts = args.l2_verdicts.split(",") if args.l2_verdicts else []
        conds, params = [], []
        if verdicts:
            conds.append(f"l2.verdict IN ({','.join(['%s']*len(verdicts))})")
            params.extend(verdicts)
        if args.l2_conf_max is not None:
            conds.append("l2.confidence < %s")
            params.append(args.l2_conf_max)
        where = " AND ".join(conds) if conds else "1=1"
        cur.execute(f"""
            SELECT b.id, b.producer_id_a, b.producer_id_b, b.name_a, b.name_b,
                   b.country, b.similarity, b.wines_a, b.wines_b, b.signals,
                   l.verdict, l.confidence, l.reasoning,
                   l2.verdict, l2.confidence
            FROM producer_dedup_pairs b
            JOIN producer_dedup_pairs l
              ON l.producer_id_a = b.producer_id_a
             AND l.producer_id_b = b.producer_id_b
             AND l.method_name='l1_haiku_batch'
            JOIN producer_dedup_pairs l2
              ON l2.producer_id_a = b.producer_id_a
             AND l2.producer_id_b = b.producer_id_b
             AND l2.method_name='l2_haiku_rich'
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


def main() -> int:
    if os.name == "nt":
        import io
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
        sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

    ap = argparse.ArgumentParser()
    ap.add_argument("--calibration", action="store_true")
    ap.add_argument("--execute", action="store_true")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--limit", type=int, default=None)
    ap.add_argument("--budget", type=float, default=10.0)
    ap.add_argument("--max-searches", type=int, default=3)
    ap.add_argument("--workers", type=int, default=2)
    ap.add_argument("--no-web", action="store_true", help="disable web_search tool (ablation)")
    ap.add_argument("--l2-verdicts", default=None)
    ap.add_argument("--l2-conf-max", type=float, default=None)
    ap.add_argument("--resume", action="store_true", default=True)
    ap.add_argument("--no-resume", dest="resume", action="store_false")
    ap.add_argument("--method-name", default=None)
    args = ap.parse_args()

    if not (args.execute or args.dry_run):
        print("Specify --execute or --dry-run", file=sys.stderr); return 2

    use_web = not args.no_web

    method_name = args.method_name or ("l3_sonnet_web" if use_web else "l3_sonnet_noweb")
    if args.calibration:
        method_name = method_name + "_calibration"
    print(f"method_name: {method_name} | web: {use_web} | budget ${args.budget}")

    section_11 = load_section_11()
    preamble = build_preamble(section_11, use_web=use_web)
    print(f"Preamble: {len(preamble)} chars")

    conn = get_conn()
    conn.autocommit = False
    cur = conn.cursor()

    pairs = load_pairs(cur, args, method_name)
    print(f"Target {len(pairs)} pairs")
    if not pairs:
        cur.close(); conn.close(); return 0

    client = anthropic.Anthropic(api_key=get_env("ANTHROPIC_API_KEY"))
    total_cost_cents = 0.0
    completed = 0
    state_lock = Lock()
    abort = {"a": False}
    verdict_counts = {"MERGE": 0, "PARENT_CHILD": 0, "SKIP": 0, "UNCERTAIN": 0, "UNKNOWN": 0}

    def process_one(pair):
        if abort["a"]:
            return pair, {"verdict": "UNCERTAIN"}, [], 0.0, "abort"
        local_conn = get_conn(); local_cur = local_conn.cursor()
        try:
            ctx = load_context(local_cur, pair)
            prompt = format_prompt(pair, ctx)
            try:
                obj, evidence, usage, n_searches = call_l3(
                    client, preamble, prompt,
                    max_searches=args.max_searches, use_web=use_web
                )
            except Exception as e:
                return pair, {"verdict": "UNCERTAIN", "confidence": 0.0,
                              "reasoning": f"API ERROR: {str(e)[:200]}"}, [], 0.0, str(e)[:300]
            cost = compute_cost_cents(usage, n_searches)
            return pair, obj, evidence, cost, None
        finally:
            local_cur.close(); local_conn.close()

    t_start = time.time()
    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        futures = {ex.submit(process_one, p): p for p in pairs}
        for fut in as_completed(futures):
            pair, verdict, evidence, cost, err = fut.result()
            if abort["a"]:
                continue
            with state_lock:
                total_cost_cents += cost
                completed += 1
                if total_cost_cents / 100 > args.budget:
                    print(f"\n!! Budget exceeded. Aborting.", flush=True)
                    abort["a"] = True
                vk = verdict.get("verdict", "UNKNOWN")
                if vk in verdict_counts:
                    verdict_counts[vk] += 1
                else:
                    verdict_counts["UNKNOWN"] += 1

            if args.execute and not err:
                with state_lock:
                    write_l3(cur, pair, verdict, evidence, cost, method_name)
                    conn.commit()

            elapsed = time.time() - t_start
            eta = (elapsed / max(1, completed)) * (len(pairs) - completed)
            status = verdict.get("verdict", "?")
            conf = verdict.get("confidence", "?")
            print(f"  [{completed}/{len(pairs)}] {pair['name_a'][:26]!r:<28}/{pair['name_b'][:26]!r:<28} "
                  f"-> {status:<14} {conf} | {cost:.2f}c | ${total_cost_cents/100:.2f} | eta={eta/60:.0f}m"
                  + (f" | ERR {err[:80]}" if err else ""),
                  flush=True)

    print("\n" + "=" * 60)
    print("L3 RESULTS")
    print("=" * 60)
    print(f"Pairs processed: {completed}")
    print(f"Total cost:      ${total_cost_cents/100:.3f}")
    if completed:
        print(f"Avg cost/pair:   {total_cost_cents/completed:.3f}c")
    for v, n in verdict_counts.items():
        if n:
            print(f"  {v:<15} {n}")

    cur.close(); conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
