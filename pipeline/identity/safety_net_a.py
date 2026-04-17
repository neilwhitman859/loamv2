"""
B6.4 Phase I — Safety Net A: unblocked spot-check.

Pull 200 random producer-pairs NOT in the blocking candidate list. Run them
through the cross-model (L1 + L1.5) ladder. If any come back MERGE at
confidence >0.85, blocking missed them — add a rule or loosen a threshold.

Strategy: random sample of same-country pairs where neither pair is in
producer_dedup_pairs with method_name='blocking'. Run L1 Haiku + Gemini
basic on them. Report any MERGEs with names + reasoning.

Run:
    python -m pipeline.identity.safety_net_a --execute --budget 1 --limit 200
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
import requests

from pipeline.lib.db import get_conn, get_env
from pipeline.lib.models import HAIKU_MODEL


GEMINI_MODEL = "google/gemini-3-flash-preview"
OPENROUTER_URL = "https://openrouter.ai/api/v1/chat/completions"

IDENTITY_RULES_PATH = Path(__file__).resolve().parents[2] / "docs" / "IDENTITY_RULES.md"
OUT_PATH = Path(__file__).resolve().parents[2] / "data" / "sprints" / "dedup" / "safety_net_a.md"


def load_section_11() -> str:
    text = IDENTITY_RULES_PATH.read_text(encoding="utf-8")
    m = re.search(
        r"(## 11\. Producer Identity Rules.*?)(?=\n## Appendix|\n---\s*\n## )",
        text, re.DOTALL
    )
    if not m:
        raise RuntimeError("Could not locate Section 11")
    return m.group(1).strip()


def sample_unblocked_pairs(cur, n: int) -> list[dict]:
    """Pull random unblocked same-country producer pairs.

    Strategy: sample a subset of producers (with wines, with country), then
    pair them at random within same country, filter out blocked pairs.
    Avoids blowing up on 33K×33K cross product.
    """
    # Step 1: get a random subset of active producers with wines + country
    cur.execute("""
        SELECT p.id, p.name, p.name_normalized, p.country_id,
               (SELECT COUNT(*) FROM wines WHERE producer_id=p.id AND deleted_at IS NULL) AS wines
        FROM producers p
        WHERE p.deleted_at IS NULL AND p.country_id IS NOT NULL
          AND EXISTS (SELECT 1 FROM wines WHERE producer_id=p.id AND deleted_at IS NULL)
        ORDER BY random()
        LIMIT 2000
    """)
    producers_sample = [dict(id=str(r[0]), name=r[1], norm=r[2], country_id=str(r[3]) if r[3] else None,
                              wines=r[4]) for r in cur.fetchall()]
    # Step 2: bucket by country
    by_country = {}
    for p in producers_sample:
        if p["country_id"]:
            by_country.setdefault(p["country_id"], []).append(p)

    # Step 3: generate random same-country pairs
    import random
    candidates = []
    for cid, group in by_country.items():
        if len(group) < 2:
            continue
        # Sample up to 40 pairs per country
        for _ in range(min(40, len(group) * (len(group) - 1) // 2)):
            a, b = random.sample(group, 2)
            if a["id"] < b["id"]:
                candidates.append((a, b))
            else:
                candidates.append((b, a))

    # Step 4: filter out blocked pairs
    random.shuffle(candidates)
    pairs_out = []
    for a, b in candidates:
        if len(pairs_out) >= n * 2:  # buffer
            break
        cur.execute("""
            SELECT 1 FROM producer_dedup_pairs
            WHERE method_name='blocking'
              AND ((producer_id_a=%s AND producer_id_b=%s)
                OR (producer_id_a=%s AND producer_id_b=%s))
            LIMIT 1
        """, (a["id"], b["id"], b["id"], a["id"]))
        if cur.fetchone():
            continue
        # trigram similarity
        cur.execute("SELECT similarity(%s, %s)", (a["norm"], b["norm"]))
        sim = float(cur.fetchone()[0])
        pairs_out.append((a, b, sim))
        if len(pairs_out) >= n:
            break
    pairs = []
    for a, b, sim in pairs_out[:n]:
        pairs.append({
            "pair_id": f"SN-{a['id'][:8]}-{b['id'][:8]}",
            "producer_id_a": a["id"],
            "producer_id_b": b["id"],
            "name_a": a["name"],
            "name_b": b["name"],
            "country": "?",
            "country_id": a["country_id"],
            "wines_a": a["wines"],
            "wines_b": b["wines"],
            "similarity": sim,
            "signals": {"safety_net_a": True, "trigram_sim": sim},
        })
    return pairs


def load_wine_samples(cur, producer_ids: list) -> dict:
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
        SELECT producer_id, nm FROM ranked WHERE rn <= 5
    """, producer_ids)
    per = {}
    for pid, nm in cur.fetchall():
        per.setdefault(pid, []).append(nm)
    return per


def format_pair(pair, wines):
    wa = wines.get(pair["producer_id_a"], [])
    wb = wines.get(pair["producer_id_b"], [])
    return f"""--- PAIR {pair['pair_id']} ---
A: {pair['name_a']!r} (country_id={pair.get('country_id')}, wines={pair['wines_a']}) top: {wa}
B: {pair['name_b']!r} (country_id={pair.get('country_id')}, wines={pair['wines_b']}) top: {wb}
trigram_similarity: {pair['similarity']}
signals: this pair was NOT flagged by blocking — running as a safety-net check.
"""


def build_preamble(section_11: str) -> str:
    return f"""You are the safety-net cross-check classifier. You're classifying producer pairs
that blocking did NOT flag. Return ONLY a JSON array (no markdown):

[
  {{"pair_id": <string>, "verdict": "MERGE"|"PARENT_CHILD"|"SKIP"|"UNCERTAIN",
    "confidence": <float>, "reasoning": "<1-2 sentences>"}},
  ...
]

Apply IDENTITY_RULES §11 strictly. If you see a MERGE at confidence >0.85,
it means blocking missed a duplicate — we need to know.

{section_11}
"""


def call_haiku(client, preamble, batch_block):
    n = len(re.findall(r"--- PAIR", batch_block))
    user = f"Classify the {n} pairs below. Return ONLY a JSON array.\n\n{batch_block}"
    resp = client.messages.create(
        model=HAIKU_MODEL, max_tokens=2500,
        system=[{"type": "text", "text": preamble, "cache_control": {"type": "ephemeral"}}],
        messages=[{"role": "user", "content": user}],
    )
    raw = resp.content[0].text.strip()
    if raw.startswith("```"):
        raw = raw.split("\n", 1)[1] if "\n" in raw else raw[3:]
        if raw.endswith("```"):
            raw = raw[:-3].strip()
    try:
        return json.loads(raw), resp.usage
    except json.JSONDecodeError:
        m = re.search(r"\[\s*\{.*\}\s*\]", raw, re.DOTALL)
        return (json.loads(m.group(0)) if m else []), resp.usage


def call_gemini(preamble, batch_block, api_key):
    n = len(re.findall(r"--- PAIR", batch_block))
    user = f"Classify the {n} pairs below. Return ONLY a JSON array.\n\n{batch_block}"
    r = requests.post(
        OPENROUTER_URL,
        headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json",
                 "HTTP-Referer": "https://loam.onrender.com",
                 "X-Title": "Loam B6.4 safety net"},
        json={"model": GEMINI_MODEL, "max_tokens": 2500, "temperature": 0.0,
              "messages": [
                  {"role": "system", "content": preamble},
                  {"role": "user", "content": user},
              ]},
        timeout=120,
    )
    r.raise_for_status()
    raw = r.json()["choices"][0]["message"]["content"].strip()
    if raw.startswith("```"):
        raw = raw.split("\n", 1)[1] if "\n" in raw else raw[3:]
        if raw.endswith("```"):
            raw = raw[:-3].strip()
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        m = re.search(r"\[\s*\{.*\}\s*\]", raw, re.DOTALL)
        return json.loads(m.group(0)) if m else []


def main():
    if os.name == "nt":
        import io
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

    ap = argparse.ArgumentParser()
    ap.add_argument("--execute", action="store_true")
    ap.add_argument("--limit", type=int, default=200)
    ap.add_argument("--budget", type=float, default=1.0)
    ap.add_argument("--batch-size", type=int, default=10)
    args = ap.parse_args()

    if not args.execute:
        print("--execute required"); return 2

    conn = get_conn()
    cur = conn.cursor()

    print(f"Sampling {args.limit} unblocked pairs...")
    pairs = sample_unblocked_pairs(cur, args.limit)
    print(f"  got {len(pairs)} pairs")
    if not pairs:
        return 0

    all_pids = list({p["producer_id_a"] for p in pairs} | {p["producer_id_b"] for p in pairs})
    wines = load_wine_samples(cur, all_pids)
    print(f"  wines loaded for {len(wines)} producers")

    section_11 = load_section_11()
    preamble = build_preamble(section_11)

    anth = anthropic.Anthropic(api_key=get_env("ANTHROPIC_API_KEY"))
    gkey = get_env("OPENROUTER_API_KEY")

    # Run Haiku + Gemini on each batch
    batches = [pairs[i:i+args.batch_size] for i in range(0, len(pairs), args.batch_size)]
    results_haiku = {}
    results_gemini = {}
    total_cost_cents = 0.0

    print(f"Running Haiku + Gemini on {len(batches)} batches of {args.batch_size}...")
    for i, batch in enumerate(batches):
        block = "\n".join(format_pair(p, wines) for p in batch)
        try:
            h_verdicts, h_usage = call_haiku(anth, preamble, block)
            cache_r = getattr(h_usage, "cache_read_input_tokens", 0) or 0
            cache_w = getattr(h_usage, "cache_creation_input_tokens", 0) or 0
            reg = max(0, h_usage.input_tokens - cache_r - cache_w)
            hc = reg*0.80/1e6 + h_usage.output_tokens*4.0/1e6 + cache_r*0.08/1e6 + cache_w*1.0/1e6
            total_cost_cents += hc * 100
            for v in h_verdicts:
                if "pair_id" in v:
                    results_haiku[str(v["pair_id"])] = v
        except Exception as e:
            print(f"  haiku batch {i}: ERR {e}")

        try:
            g_verdicts = call_gemini(preamble, block, gkey)
            total_cost_cents += 0.02  # approx
            for v in g_verdicts:
                if "pair_id" in v:
                    results_gemini[str(v["pair_id"])] = v
        except Exception as e:
            print(f"  gemini batch {i}: ERR {e}")

        if total_cost_cents / 100 > args.budget:
            print(f"  Budget exceeded, stopping at batch {i+1}")
            break

        if (i+1) % 5 == 0:
            print(f"  batch {i+1}/{len(batches)} | ${total_cost_cents/100:.2f}", flush=True)

    # Assemble report
    rows = []
    for p in pairs:
        pid = p["pair_id"]
        h = results_haiku.get(pid, {})
        g = results_gemini.get(pid, {})
        rows.append({
            "pair_id": pid,
            "name_a": p["name_a"],
            "name_b": p["name_b"],
            "country_id": p.get("country_id"),
            "wines_a": p["wines_a"],
            "wines_b": p["wines_b"],
            "sim": p["similarity"],
            "haiku_verdict": h.get("verdict"),
            "haiku_conf": h.get("confidence"),
            "gemini_verdict": g.get("verdict"),
            "gemini_conf": g.get("confidence"),
            "haiku_reasoning": h.get("reasoning"),
            "gemini_reasoning": g.get("reasoning"),
        })

    # Flag any MERGE with conf>0.85 from either model
    flagged = [r for r in rows
               if (r["haiku_verdict"] == "MERGE" and (r["haiku_conf"] or 0) > 0.85)
               or (r["gemini_verdict"] == "MERGE" and (r["gemini_conf"] or 0) > 0.85)]

    lines = ["# Safety Net A — Unblocked Spot-Check Results", ""]
    lines.append(f"Sampled {len(rows)} producer pairs NOT in blocking candidate list.")
    lines.append(f"Total cost: ${total_cost_cents/100:.3f}")
    lines.append(f"Haiku labels: {len(results_haiku)} | Gemini labels: {len(results_gemini)}")
    lines.append("")
    lines.append(f"## Flagged (MERGE at conf > 0.85 from either model): {len(flagged)}")
    lines.append("")
    if flagged:
        lines.append("| name_a | name_b | country | Haiku | Gemini | sim |")
        lines.append("|---|---|---|---|---|---|")
        for r in flagged:
            h = f"{r['haiku_verdict']}@{r['haiku_conf']}" if r['haiku_verdict'] else "-"
            g = f"{r['gemini_verdict']}@{r['gemini_conf']}" if r['gemini_verdict'] else "-"
            lines.append(f"| {r['name_a']!r} | {r['name_b']!r} | {r['country_id']} | {h} | {g} | {r['sim']:.2f} |")
        lines.append("")
        lines.append("## Detailed reasoning per flagged pair")
        lines.append("")
        for r in flagged:
            lines.append(f"### {r['name_a']!r} / {r['name_b']!r}")
            lines.append(f"- Haiku: {r['haiku_verdict']} @ {r['haiku_conf']}")
            lines.append(f"  - {r.get('haiku_reasoning', '')}")
            lines.append(f"- Gemini: {r['gemini_verdict']} @ {r['gemini_conf']}")
            lines.append(f"  - {r.get('gemini_reasoning', '')}")
            lines.append("")
    else:
        lines.append("**No unblocked pairs flagged as MERGE.** Blocking recall is solid for sampled cases.")
        lines.append("")

    lines.append("## Verdict distribution")
    from collections import Counter
    h_dist = Counter(r["haiku_verdict"] for r in rows if r["haiku_verdict"])
    g_dist = Counter(r["gemini_verdict"] for r in rows if r["gemini_verdict"])
    lines.append(f"- Haiku: {dict(h_dist)}")
    lines.append(f"- Gemini: {dict(g_dist)}")

    OUT_PATH.write_text("\n".join(lines), encoding="utf-8")
    print(f"\n{'=' * 60}")
    print(f"Wrote {OUT_PATH}")
    print(f"Total cost: ${total_cost_cents/100:.3f}")
    print(f"Flagged: {len(flagged)} pairs")

    cur.close(); conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
